import json
from typing import List

import scipy.spatial

from pydantic import BaseModel
from llama_index.core.llms import ChatMessage
from llama_index.llms.openai import OpenAI
from llama_index.llms.anthropic import Anthropic
import openai
import scipy
import os

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
        texts = []
        for _, row in data.iterrows():
            term, desc = row["Term"].strip(), row["Desc"].strip()
            texts.append(f"{term} [SEP] {desc}")
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
