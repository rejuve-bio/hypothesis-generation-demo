import json
import os
import re
from typing import List

import openai
import pandas as pd
import scipy.spatial
from llama_index.core.llms import ChatMessage
from llama_index.llms.anthropic import Anthropic
from llama_index.llms.openai import OpenAI
from loguru import logger
from pydantic import BaseModel

_DEFAULT_GO_LLM_KEY = "ollama"
_DEFAULT_GO_LLM_MODEL = "gemma4"
_DEFAULT_OPENAI_GO_LLM_MODEL = "gpt-4o"
_DEFAULT_LLM_MAX_CANDIDATES = 250
_DEFAULT_LLM_MAX_OUTPUT_TOKENS = 4096


def _make_go_llm_client() -> openai.OpenAI:
    """OpenAI-compatible client for the local GO ranking model (e.g. Ollama).

    There is no baked-in default host: the local LLM endpoint is specific to
    each environment. GO_LLM_URL must be set whenever GO_LLM_BACKEND=local
    (the default); this raises immediately with a clear message otherwise,
    rather than silently pointing at a fixed personal/lab host.
    """
    url = os.getenv("GO_LLM_URL", "").strip()
    if not url:
        raise RuntimeError(
            "GO_LLM_URL is not set. GO_LLM_BACKEND=local requires the URL of an "
            "OpenAI-compatible local LLM endpoint (e.g. Ollama) for this "
            "environment. Set GO_LLM_URL, or set GO_LLM_BACKEND=openai to use "
            "OpenAI instead."
        )
    base = url.rstrip("/")
    if not base.endswith("/v1"):
        base = f"{base}/v1"
    return openai.OpenAI(
        api_key=os.getenv("GO_LLM_KEY", _DEFAULT_GO_LLM_KEY),
        base_url=base,
    )


def _strip_json_fence(raw: str) -> str:
    text = raw.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[-1]
        if text.endswith("```"):
            text = text.rsplit("```", 1)[0]
    return text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()


def _cell_str(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value != value:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _resolve_llm_batch_size(max_candidates: int) -> int:
    """Per-request batch size for LLM GO ranking (context window chunk)."""
    if max_candidates > 0:
        return max_candidates
    env_raw = os.getenv("GO_LLM_MAX_CANDIDATES", str(_DEFAULT_LLM_MAX_CANDIDATES))
    try:
        env_cap = int(env_raw)
    except ValueError:
        env_cap = _DEFAULT_LLM_MAX_CANDIDATES
    return env_cap if env_cap > 0 else _DEFAULT_LLM_MAX_CANDIDATES


def _resolve_llm_candidate_cap(max_candidates: int, pool_size: int) -> int:
    """Cap LLM candidate pool to fit model context; 0 means use env/default auto-cap."""
    batch_size = _resolve_llm_batch_size(max_candidates)
    cap = min(batch_size, pool_size)
    if cap < pool_size:
        logger.info(
            f"LLM GO ranking: using top {cap} candidates by adj p-value "
            f"(pool has {pool_size} terms; set GO_LLM_MAX_CANDIDATES to override)"
        )
    return cap


def _go_llm_system_prompt(k: int) -> str:
    return (
        "You are an expert in GWAS functional follow-up and Gene Ontology (GO) enrichment analysis. "
        f"From the candidate GO biological process terms provided, select exactly {k} terms.\n\n"
        "CRITICAL — quality of the LAST-ranked terms matters as much as the first:\n"
        "- Every one of the k terms must be a genuine, defensible enrichment result. Do NOT fill out "
        "the list with weak or statistically null terms just to reach k. A term with adjusted p-value "
        "not meaningfully below 1 (e.g. p > 0.1) has NO real enrichment signal in this analysis and "
        "must not be selected UNLESS every other candidate is equally or more null — never prefer a "
        "statistically null term over an available term with real signal (even modest, e.g. p < 0.05), "
        "no matter how thematically appealing the null term's name sounds.\n"
        "- If you are tempted to justify a low-ranked pick purely by the causal gene's textbook/canonical "
        "function (e.g. 'this gene is broadly known for X') rather than by this specific enrichment's "
        "statistics, that is a warning sign — check whether a candidate with stronger adjusted p-value "
        "was available instead and prefer it.\n"
        "- Rank ALL k terms by the same standard: adjusted p-value strength combined with mechanistic "
        "plausibility for the phenotype. Do not relax this standard for ranks toward the bottom of the "
        "list.\n"
        "- If fewer than k candidates are well-justified, still return k terms, but fill remaining slots "
        "with the next-highest adjusted p-value candidates from the list rather than arbitrary or "
        "thematically-associated-but-statistically-unsupported ones.\n\n"
        "Other rules:\n"
        "- Choose ONLY from the candidate list (use the exact go_id from the list).\n"
        "- Prioritize terms whose biology is mechanistically plausible for the stated GWAS phenotype.\n"
        "- When a causal gene is provided, treat it as supporting context only. Prioritize phenotype "
        "fit and this enrichment's actual statistics over repeating the gene's canonical functions "
        "unless those functions are also statistically well-supported here.\n"
        "- Deprioritize generic housekeeping processes (e.g. RNA polymerase II transcription, ribosome "
        "biogenesis, generic cell cycle) when more specific, phenotype-linked processes with comparable "
        "statistical support are available.\n"
        "\nTERM SPECIFICITY — treat this as the primary ordering rule:\n"
        "Candidates state how many genes they annotate. A term annotating more than 500 genes "
        "(e.g. 'regulation of gene expression', 'regulation of DNA-templated transcription') is a "
        "generic truism: it is enriched in almost any gene network and says nothing specific about "
        "this gene. A term annotating fewer than 100 genes names a concrete mechanism.\n"
        f"- At least half of your {k} selections must annotate fewer than 100 genes, whenever that "
        "many such candidates exist with adjusted p < 0.05.\n"
        "- Never rank a >500-gene term above a <100-gene term that is also significant "
        "(adj p < 0.05), however many orders of magnitude smaller the broad term's p-value is. A tiny "
        "p-value on a huge term reflects its breadth, not its biological informativeness.\n"
        "- Broad terms belong at the bottom of the list as context, never at the top.\n"
        "- Prefer a diverse set of distinct mechanisms over multiple near-duplicate or tightly "
        "hierarchical sibling terms, but never sacrifice statistical support for diversity alone.\n\n"
        'Return JSON only: {"terms": [{"rank": 1, "go_id": "GO:...", "name": "...", "reason": "..."}]}. '
        "The \"reason\" for each term must cite its adjusted p-value or score, not just its thematic fit."
    )

def _llm_batched_ranking_enabled() -> bool:
    return os.getenv("GO_LLM_BATCHED", "true").strip().lower() in {"1", "true", "yes"}


def _go_llm_candidate_preamble(
    k: int,
    candidate_count: int,
    *,
    prefiltered: bool,
) -> str:
    if prefiltered:
        return (
            f"Select the {k} most relevant GO biological processes from the {candidate_count} "
            "candidates below. These were shortlisted from the full enrichment output by combining "
            "the strongest enrichment signals with semantic similarity to the phenotype "
            "(higher score = better).\n"
        )
    return (
        f"Select the {k} GO biological processes most relevant to this phenotype from the "
        f"candidate list below ({candidate_count} terms, sorted by enrichment p-value).\n"
    )


def split_text(text: str, n=100, character=" ") -> List[str]:
    """Split the text every ``n``-th occurrence of ``character``"""
    text = text.split(character)
    return [character.join(text[i : i + n]).strip() for i in range(0, len(text), n)]

def split_documents(documents: dict) -> dict:
    """Split documents into passages"""
    titles, texts = [], []
    for title, text in zip(documents["title"], documents["text"]):
        if text is not None:
            for passage in split_text(text):
                titles.append(title if title is not None else "")
                texts.append(passage)
    return {"title": titles, "text": texts}

class GoTerm(BaseModel):
    rank: int
    name: str
    reason: str

class Response(BaseModel):
    terms: List[GoTerm]

class LLM:

    def __init__(self, llm="gpt4", temperature=0.0):

        
        self.temperature = temperature
        if llm == "gpt4":
            #Check that the openai key is available
            try:
                openai_api_key = os.getenv("OPENAI_API_KEY")
                openai.api_key = openai_api_key
                self.llm = OpenAI(api_key=openai_api_key, temperature=temperature, model="gpt-4-0613")
                # self.llm = OpenAI(api_key=openai_api_key, temperature=temperature, model="gpt-35-turbo-0613")
            except KeyError:
                raise ValueError("Please set the OPENAI_API_KEY environment variable")
        elif llm == "claude":
            # Check that Anthropic key is available
            try:
                anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
                self.llm = Anthropic(api_key=anthropic_api_key, temperature=temperature, model="claude-3-5-sonnet-20240620")
            except KeyError:
                raise ValueError("Please set the ANTHROPIC_API_KEY environment variable")
    
    
    def predict_casual_gene(self, phenotype, genes, 
                            prev_gene = None, rule=None):
        """
        Given a variant, a list of candidate genes and a phenotype, query the LLM to predict the causal gene
        """
        genes = sorted(genes)
        genes_fmt = []
        for gene in genes:
            genes_fmt.append("{" + gene + "}")
            
        genes_str = ",".join(genes_fmt)
        if rule is None:
            system_prompt = """You are an expert in biology and genetics.
                            Your task is to identify likely causal genes within a locus for a given GWAS phenotype based on literature evidence.

                            From the list, provide the likely causal gene (matching one of the given genes), confidence (0: very unsure to 1: very confident), and a brief reason (50 words or less) for your choice.

                            Return your response in JSON format, excluding the GWAS phenotype name and gene list in the locus. JSON keys should be ‘causal_gene’,‘confidence’,‘reason’.
                            Don't add any additional information to the response.

                        """
        else:
            assert prev_gene is not None, "Previous gene must be provided when rule is provided"
            system_prompt = f"""You are an expert in biology and genetics.
                            Your task is to identify likely causal genes within a locus for a given GWAS phenotype based on literature evidence.

                            From the list, provide the likely causal gene (matching one of the given genes), confidence (0: very unsure to 1: very confident), and a brief reason (50 words or less) for your choice.
                            
                            You previously identified {prev_gene} as a causal gene. Your prediction couldn't be verified by the following prolog rule:
                            
                            {rule}
                            
                            Make sure your prediction is consistent with the rule.
                            Return your response in JSON format, excluding the GWAS phenotype name and gene list in the locus. JSON keys should be ‘causal_gene’,‘confidence’,‘reason’.
                            Don't add any additional information to the response.
                        """
            
        
        # print(f"Systen Prompt: {system_prompt}")
        query = f"GWAS Phenotype: {phenotype}\nGenes: {genes_str}"
        print(f"Query: {query}")
        messages = [
                        ChatMessage(role="system", content=system_prompt),
                        ChatMessage(role="user", content=query),
                    ]
        response = self.llm.chat(messages).message.content
        print(f"LLM Response: {response}")
        try:
            response = json.loads(response)
        except:
            # retry
            response = self.llm.chat(messages)
            response = json.loads(response.message.content)
        return response
        
    def get_relevant_go(self, phentoype, enrich_tbl, 
                        k=10):
        """
        Given a phenotype, a sequence variant and an enrichment analysis table, get the top k relevant GO terms relevant to the phenotype by prompting the LLM using RAG
        :param phentoype: GWAS Phenotype/Trait
        :param variant: Sequence Variant
        :param enrich_tbl: Table containing the over-presentation test expected columns are ID, Term, Desc, Adjusted P-val
        :return: dict obj containing the k relevant GO terms, their p-val and the reason why the LLM thinks they are relevant to the phenotype
        """
        # tmp_file = tempfile.NamedTemporaryFile("w+")
        # df.drop(columns=["ID", "Adjusted P-value", "Genes"], inplace=True)
        # df.to_csv(tmp_file, index=False)
        
        #Embed the GO terms and their descriptions.
        res = self._retrieve_top_k_go_terms(phentoype, enrich_tbl, k)
        
        return res

    def _embed_dataset(self, batch):
        combined_text = []
        for title, text in zip(batch['title'], batch['text']):
            combined_text.append(' [SEP] '.join([title, text]))

        return {"embeddings" : self.embed_model.encode(combined_text)}

    def _retrieve_top_k_go_terms(self, query, data, k):
        """
        Given a query and a dataset containing document embeddings, retrieve the top k most relevant documents using a metric (e.g MIPS)
        :param query: The query to use for retrieval
        :param dataset: The embedded documents
        :param k: Number of documents to retrieve
        :return:
        """
        # Validate that data is not empty
        if data is None or len(data) == 0:
            print("Warning: Empty enrichment table provided to LLM")
            return []
        
        texts = []
        for _, row in data.iterrows():
            term = _cell_str(row["Term"])
            desc = _cell_str(row.get("Desc", ""))
            if not desc or desc in {"NA", "GO"}:
                desc = term
            if not term:
                continue
            texts.append(f"{term} [SEP] {desc}")
        
        # Validate that we have texts to embed
        if not texts:
            print("Warning: No texts to embed after processing enrichment table")
            return []
        
        data = data.copy()  # Avoid SettingWithCopyWarning
        client = openai.Client()
        embeddings =  client.embeddings.create(input = texts, model="text-embedding-3-small").data
        data["embeddings"] = [emb.embedding for emb in embeddings]
        query_embedding = client.embeddings.create(input = [query], model="text-embedding-3-small").data[0].embedding
        data["similarity"] = data.embeddings.apply(lambda x: 1 - scipy.spatial.distance.cosine(x, query_embedding))
        res = data.sort_values("similarity", ascending=False).head(k)
        print("these are response: ", type(res))     
        # subset_go = {"ID": [], "Name": [], "Rank": [],  "Genes": [], "Adjusted P-value": []}
        # i = 1
        # for _, row in res.iterrows():
        #     go_id, name, rank, pval, genes = row["ID"], row["Term"], i, row["Adjusted P-value"], row["Genes"]
        #     subset_go["ID"].append(go_id.strip())
        #     subset_go["Name"].append(name.strip())
        #     subset_go["Rank"].append(rank)
        #     subset_go["Genes"].append(genes)
        #     subset_go["Adjusted P-value"].append(pval)
        #     i += 1
        # return subset_go
        subset_go = []
        i = 1
        for _, row in res.iterrows():
            go_entry = {
                "id": row["ID"].strip(),
                "name": row["Term"].strip(),
                "genes": row["Genes"].split(';'),
                "p": row["Adjusted P-value"],
                "rank": i
            }
            subset_go.append(go_entry)
            i += 1

        return subset_go

    def _rank_relevant_go_by_llm_once(
        self,
        phenotype: str,
        candidates,
        k: int,
        causal_gene: str | None,
        llm_model: str,
        client: openai.OpenAI,
        llm_backend: str,
        prefiltered: bool = False,
    ) -> list[dict]:
        if "score" in candidates.columns:
            candidates = candidates.sort_values("score", ascending=False)
        elif "Adjusted P-value" in candidates.columns:
            candidates = candidates.sort_values("Adjusted P-value", ascending=True)

        id_lookup = {str(row["ID"]).strip(): row for _, row in candidates.iterrows()}
        name_lookup = {
            _cell_str(row["Term"]).lower(): row for _, row in candidates.iterrows()
        }

        compact = len(candidates) > 120
        show_score = prefiltered and "score" in candidates.columns
        lines = []
        for _, row in candidates.iterrows():
            genes_raw = str(row.get("Genes", ""))
            genes = [g.strip() for g in genes_raw.replace(",", ";").split(";") if g.strip()]
            gene_preview = "; ".join(genes[:3 if compact else 8])
            term = _cell_str(row["Term"])
            desc = _cell_str(row.get("Desc", ""))
            if not desc or desc in {"NA", "GO"}:
                desc = term
            score_bit = ""
            if show_score:
                score_bit = f" | score={float(row['score']):.3f}"
            size_bit = ""
            size = pd.to_numeric(row.get("Term Size"), errors="coerce")
            if pd.notna(size):
                size_bit = f" | annotates {int(size)} genes"
            if compact:
                lines.append(
                    f"- {row['ID']} | {term} | adj_p={float(row['Adjusted P-value']):.2e}"
                    f"{score_bit}{size_bit} | genes: {gene_preview}"
                )
            else:
                lines.append(
                    f"- {row['ID']} | {term} | adj_p={float(row['Adjusted P-value']):.2e}"
                    f"{score_bit}{size_bit} | genes: {gene_preview} | desc: {desc[:120]}"
                )

        gene_line = f"Causal gene at locus: {causal_gene}.\n" if causal_gene else ""
        user_prompt = (
            f"GWAS phenotype: {phenotype.strip()}\n"
            f"{gene_line}\n"
            f"{_go_llm_candidate_preamble(k, len(candidates), prefiltered=prefiltered)}\n"
            + "\n".join(lines)
        )
        system_prompt = _go_llm_system_prompt(k)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        try:
            max_tokens = int(
                os.getenv("GO_LLM_MAX_OUTPUT_TOKENS", str(_DEFAULT_LLM_MAX_OUTPUT_TOKENS))
            )
            response = client.chat.completions.create(
                model=llm_model,
                temperature=0.0,
                max_tokens=max_tokens,
                messages=messages,
            )
            raw = response.choices[0].message.content or ""
        except Exception as exc:
            logger.exception("LLM GO ranking call failed")
            raise RuntimeError(f"GO term ranking service error: {exc}") from exc

        raw = _strip_json_fence(raw)
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            response = client.chat.completions.create(
                model=llm_model,
                temperature=0.0,
                max_tokens=max_tokens,
                messages=messages,
            )
            raw = _strip_json_fence(response.choices[0].message.content or "")
            parsed = json.loads(raw)

        results = []
        for item in parsed.get("terms", [])[:k]:
            go_id = str(item.get("go_id", "")).strip()
            name = str(item.get("name", "")).strip()
            row = id_lookup.get(go_id)
            if row is None and name:
                row = name_lookup.get(name.lower())
            if row is None:
                logger.warning(f"LLM picked unknown GO term: {go_id} / {name}")
                continue
            genes_raw = str(row.get("Genes", ""))
            genes = [g.strip() for g in genes_raw.replace(",", ";").split(";") if g.strip()]
            results.append(
                {
                    "id": str(row["ID"]).strip(),
                    "name": str(row["Term"]).strip(),
                    "genes": genes,
                    "p": float(row["Adjusted P-value"]),
                    "rank": int(item.get("rank", len(results) + 1)),
                    "reason": str(item.get("reason", "")).strip(),
                    "llm_candidate_pool": len(candidates),
                    "llm_model": llm_model,
                    "llm_backend": llm_backend,
                }
            )

        results.sort(key=lambda x: x["rank"])
        for idx, entry in enumerate(results, start=1):
            entry["rank"] = idx
        return results[:k]

    def rank_relevant_go_by_llm(
        self,
        phenotype: str,
        enrich_tbl,
        k: int = 10,
        causal_gene: str | None = None,
        max_candidates: int = 0,
        model: str | None = None,
        backend: str | None = None,
        prefiltered: bool = False,
    ) -> list[dict]:
        """Ask an LLM to pick the top-k GO terms most relevant to the phenotype."""
        if enrich_tbl is None or len(enrich_tbl) == 0:
            return []

        data = enrich_tbl.copy()
        if not prefiltered and "Adjusted P-value" in data.columns:
            data = data.sort_values("Adjusted P-value", ascending=True)
        batch_size = _resolve_llm_batch_size(max_candidates)

        llm_backend = (backend or os.getenv("GO_LLM_BACKEND", "local")).strip().lower()
        if llm_backend == "openai":
            llm_model = model or os.getenv(
                "GO_OPENAI_LLM_MODEL", _DEFAULT_OPENAI_GO_LLM_MODEL
            )
            client = openai.Client()
        else:
            llm_model = model or os.getenv("GO_LLM_MODEL", _DEFAULT_GO_LLM_MODEL)
            client = _make_go_llm_client()

        use_batches = (
            not prefiltered
            and _llm_batched_ranking_enabled()
            and len(data) > batch_size
        )
        if not use_batches:
            candidates = data
            if not prefiltered:
                candidates = data.head(_resolve_llm_candidate_cap(max_candidates, len(data)))
            return self._rank_relevant_go_by_llm_once(
                phenotype,
                candidates,
                k,
                causal_gene,
                llm_model,
                client,
                llm_backend,
                prefiltered=prefiltered,
            )

        n_batches = (len(data) + batch_size - 1) // batch_size
        logger.info(
            f"LLM GO ranking: batched map-reduce over {len(data)} terms "
            f"in {n_batches} batches of up to {batch_size}"
        )

        winner_ids: list[str] = []
        for batch_idx, start in enumerate(range(0, len(data), batch_size), start=1):
            chunk = data.iloc[start : start + batch_size]
            logger.info(
                f"LLM GO ranking: batch {batch_idx}/{n_batches} "
                f"({len(chunk)} terms, enrichment ranks {start + 1}-{start + len(chunk)})"
            )
            batch_results = self._rank_relevant_go_by_llm_once(
                phenotype,
                chunk,
                k,
                causal_gene,
                llm_model,
                client,
                llm_backend,
                prefiltered=False,
            )
            for entry in batch_results:
                go_id = entry["id"]
                if go_id not in winner_ids:
                    winner_ids.append(go_id)

        if not winner_ids:
            return []

        id_set = set(winner_ids)
        rerank_pool = data[data["ID"].astype(str).str.strip().isin(id_set)]
        if len(rerank_pool) <= k:
            results = []
            for go_id in winner_ids:
                row = data[data["ID"].astype(str).str.strip() == go_id].iloc[0]
                genes_raw = str(row.get("Genes", ""))
                genes = [g.strip() for g in genes_raw.replace(",", ";").split(";") if g.strip()]
                results.append(
                    {
                        "id": go_id,
                        "name": str(row["Term"]).strip(),
                        "genes": genes,
                        "p": float(row["Adjusted P-value"]),
                        "rank": len(results) + 1,
                        "reason": "",
                        "llm_candidate_pool": len(data),
                        "llm_model": llm_model,
                        "llm_backend": llm_backend,
                        "llm_batches": n_batches,
                    }
                )
            return results[:k]

        logger.info(
            f"LLM GO ranking: final rerank over {len(rerank_pool)} batch winners"
        )
        final = self._rank_relevant_go_by_llm_once(
            phenotype,
            rerank_pool,
            k,
            causal_gene,
            llm_model,
            client,
            llm_backend,
            prefiltered=False,
        )
        for entry in final:
            entry["llm_candidate_pool"] = len(data)
            entry["llm_batches"] = n_batches
        return final

    def get_structured_response(self, response, enrich_table):
        """
        Use outlines to generate a structured response to a prompt
        :param prompt: Prompt to use
        :param enrich_table: Enrichment table
        :return:
        """
        # model = models.openai("gpt-4-0163", api_key=openai.api_key)
        # generator = outlines.generate.json(model, Response)
        # # rng = torch.Generator(device="cuda")
        # # rng.manual_seed(42)
        # response = generator(prompt)
        subset_go = {"ID": [], "Name": [], "Rank": [],   "Reason": [], "Genes": [], "Adjusted P-value": []}

        for res in response.terms:
            row = enrich_table[enrich_table["Term"].str.contains(res.name, case=False)]
            if len(row) == 0:
                print(f"Couldn't find {res['Name']}")
                continue
            elif len(row) > 1:
                row = row.head(1)
            go_id, name, rank, reason, pval, genes = row["ID"].iloc[0], row["Term"].iloc[0], res.rank, \
                res.reason, row["Adjusted P-value"].iloc[0], row["Genes"].iloc[0]
            if go_id not in subset_go["ID"]:
                subset_go["ID"].append(go_id)
                subset_go["Name"].append(name)
                subset_go["Rank"].append(rank)
                subset_go["Reason"].append(reason)
                subset_go["Genes"].append(genes)
                subset_go["Adjusted P-value"].append(pval)

        return subset_go
    
    def summarize_graph(self, graph):
        system_prompt = f"""You are an expert in biology and genetics. You have been provided with a graph provides a hypothesis for the connection of a SNP to a phenotype in terms of genes and Go terms. 
                   Your task is to summarize the graph in 150 words or less. Return your response in JSON format with the key 'summary'. Don't add any additional information to the response."""
        
        query = f"Graph: {graph}"
        messages = [
                        ChatMessage(role="system", content=system_prompt),
                        ChatMessage(role="user", content=query),
                    ]

        response = self.llm.chat(messages).message.content
        print(f"LLM Response: {response}")
        try:
            response = json.loads(response)
            return response["summary"]
        except:
            response = self.llm.chat(messages).message.content #retry
            response = json.loads(response)
            return response["summary"]
        
    
    def chat(self, query, graph):
        """
        Given a graph as a context, chat with the LLM
        """
        
        system_prompt = f"""You are an expert in biology and genetics. 
        Use the provided graph, which describes a potential hypothesis as to why a SNP is causally related to a phenotype, as a context and answer the query. Your answer should be 100 words or less.
        
        Return your response in JSON format. JSON key should be `response`. Don't add any additional information to the response."""
                     
        query = f"Graph: {graph}\nQuery: {query}"
        messages = [
                        ChatMessage(role="system", content=system_prompt),
                        ChatMessage(role="user", content=query),
                    ]
        response = self.llm.chat(messages).message.content
        print(f"LLM Response: {response}")
        try:
            response = json.loads(response)
            return response["response"]
        except:
            response = self.llm.chat(messages).message.content
            response = json.loads(response)
            return response["response"]
