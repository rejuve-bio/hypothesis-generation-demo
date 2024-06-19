import json
import tempfile
from typing import List

import faiss
import torch
from datasets import load_dataset
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModelForCausalLM
from pydantic import BaseModel
import outlines
from outlines.models import Transformers

torch.set_grad_enabled(False)
device = "cuda" if torch.cuda.is_available() else "cpu"

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

    def __init__(self, llm="llama3",  embedding_model="w601sxs/b1ade-embed-kd",
                 prompt_template=None, temperature=1.0,
                 embed_dim=768, hf_token=None):
        """
        :param llm: The language model to use. It has to be a model name that's accepted by the Transformers library
        :param embedding_model: The name of the hugging face model to use for embedding
        :param temperature: The model temperature parameter
        :param embed_dim: The dimension of the embeddings
        :param hf_token: The hugging face token to use (if needed)
        """
        self.embed_dim = embed_dim
        self.embed_model = SentenceTransformer(embedding_model, truncate_dim=self.embed_dim)
        self.temperature = temperature
        self.model = AutoModelForCausalLM.from_pretrained(llm, token=hf_token)
        self.tokenizer = AutoTokenizer.from_pretrained(llm, token=hf_token)

    def get_relevant_go(self, phentoype, variant, enrich_tbl, k=10):
        """
        Given a phenotype, a sequence variant and an enrichment analysis table, get the top k relevant GO terms relevant to the phenotype by prompting the LLM using RAG
        :param phentoype: GWAS Phenotype/Trait
        :param variant: Sequence Variant
        :param enrich_tbl: Table containing the over-presentation test expected columns are ID, Term, Desc, Adjusted P-val
        :return: dict obj containing the k relevant GO terms, their p-val and the reason why the LLM thinks they are relevant to the phenotype
        """
        tmp_file = tempfile.NamedTemporaryFile("w+")
        df = enrich_tbl.copy()
        df.drop(columns=["ID", "Adjusted P-value"], inplace=True)
        df.to_csv(tmp_file, index=False)
        #Embed the GO terms and their descriptions.
        dataset = load_dataset("csv", data_files=[tmp_file.name],
                                split="train",
                               delimiter=",", column_names=["Term", "Desc"])
        dataset = dataset.map(split_documents, batched=True)
        dataset = dataset.map(self._embed_dataset, batched=True)
        index = faiss.IndexHNSWFlat(self.embed_dim, 128, faiss.METRIC_INNER_PRODUCT)
        dataset.add_faiss_index("embeddings", custom_index=index)
        query = f"A biologist is studying the causal relationship between SNP {variant} and {phentoype}. Select {k} most relevant GO terms that are most likely to explain {phentoype}."
        context = self._retrieve_top_k_documents(query, dataset, k)
        prompt = f"""
            <s>[INST] <<SYS>>
            You are an AI assistant helping biologists understand the mechanism of action of genomic mutation and how it brings about a phenotype .You should explain each of your response. Don't write introductions or conclusions.
            <</SYS>>
            Here's a context
            {context}
            
            ---------------------
            Query: {query}
            Answer: \
            """

        llm_response = self.get_structured_response(prompt, enrich_tbl)
        return llm_response

    def _embed_dataset(self, batch):
        combined_text = []
        for title, text in zip(batch['title'], batch['text']):
            combined_text.append(' [SEP] '.join([title, text]))

        return {"embeddings" : self.embed_model.encode(combined_text)}

    def _retrieve_top_k_documents(self, query, dataset, k):
        """
        Given a query and a dataset containing document embeddings, retrieve the top k most relevant documents using a metric (e.g MIPS)
        :param query: The query to use for retrieval
        :param dataset: The embedded documents
        :param k: Number of documents to retrieve
        :return:
        """
        embedded_query = self.embed_model.encode(query)
        _, retrieved_docs = dataset.get_nearset_examples("embeddings", embedded_query, k)
        return retrieved_docs

    def get_structured_response(self, prompt, enrich_table):
        """
        Use outlines to generate a structured response to a prompt
        :param prompt: Prompt to use
        :param enrich_table: Enrichment table
        :return:
        """
        model = Transformers(self.model, self.tokenizer)
        generator = outlines.generate.json(model, Response)
        # rng = torch.Generator(device="cuda")
        # rng.manual_seed(42)
        response = generator(prompt)
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