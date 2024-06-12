import os
import logging
import sys
import pandas as pd
from llama_index.core import VectorStoreIndex
from llama_index.core import PromptTemplate
from IPython.display import Markdown, display
from llama_index.llms.ollama import Ollama
from llama_index.llms.openai import OpenAI
import openai
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core import Settings
from pathlib import Path
from llama_index.readers.file import CSVReader
import tempfile
import json
import re

regex = re.compile("\[\s?(\{.*?\})+\]")

class LLM:

    def __init__(self, llm="llama3",  embedding_model="w601sxs/b1ade-embed-kd",
                 prompt_template=None, temperature=1.0, request_timeout=120.0, 
                 ollama_host="localhost", ollama_port="11434"):
        """
        :param llm: The language model to use. Currently, either LLama3 or OpenAI GPT-4
        :param embedding_model: The name of the hugging face model to use for embedding
        :param temperature: Temperature parameter for LLama3
        :param request_timeout: Request parameter for LLama3
        :param ollama_host: Hostname for Ollama
        :param ollama_port: Port for Ollama
        """

        if llm == "llama3":
            ollama_api_url = f"http://{ollama_host}:{ollama_port}"
            self.llm = Ollama(model="llama3", request_timeout=request_timeout,
                              temperature=temperature, base_url=ollama_api_url)
        elif llm.startswith("gpt"):
            openai.api_key = os.environ["OPENAI_API_KEY"]
            self.llm = OpenAI(model=llm)

        else:
            raise ValueError(f"Unknown Large Language Model - {llm}. Currently supportered are llama3, "
                             f"gpt-3.5-turbo,  gpt-4")

        self.embed_model = HuggingFaceEmbedding(model_name=embedding_model) #TODO add option to use OpenAI embedding

        if prompt_template is None:
            qa_prompt_tmpl_str = """\
            Context information is below.
            ---------------------
            <s>[INST] <<SYS>>
            You are an AI assistant helping biologists understand the mechanism of action of genomic mutation and how it brings about a phenotype .You should explain each of your response. Don't write introductions or conclusions.
            <</SYS>>
            Here's a context
            {context_str}
            
            ---------------------
            Query: {query_str}
            Answer: \
            """
            self.prompt_template = PromptTemplate(qa_prompt_tmpl_str)

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
        Settings.embed_model = self.embed_model
        reader = CSVReader(concat_rows=False) #Embed each row separately
        docs = reader.load_data(file=Path(tmp_file.name))
        index = VectorStoreIndex.from_documents(docs)

        query_engine = index.as_query_engine(similarity_top_k=k, llm=self.llm,
                                             text_qa_template=self.prompt_template)

        query_str = f"A biologist is studying the causal relationship between SNP {variant} and {phentoype}. Select {k} most relevant GO terms that are most likely to explain {phentoype}. Your response should be a parseable json that includes two fields 'Name' for name of the term and 'Reason' for the reason of your answer. Don't include introduction and conclusion remarks. Your response should be in the following format" + "[{'Name': 'GO_1', 'Reason': 'Reason Here'}]. Don't include statements such as 'Here is the response', 'The answer is', etc"

        llm_response = query_engine.query(query_str)
        result = llm_response.response
        print(f"LLM Response: {result}")
        parsed_res = self.parse_llm_response(result, enrich_tbl)
        return parsed_res

    def parse_llm_response(self, response, enrich_table):
        # s = ''.join(response.split("\n"))
        # matches = regex.findall(s)
        # s = ''.join(matches)
        # s = f"[{s}]"
        response = json.loads(response)
        subset_go = {"ID": [], "Name": [], "Reason": [], "Genes": [], "Adjusted P-value": []}

        for res in response:
            row = enrich_table[enrich_table["Term"].str.contains(res["Name"], case=False)]
            if len(row) == 0:
                print(f"Couldn't find {res['Name']}")
                continue
            elif len(row) > 1:
                row = row.head(1)
            go_id, name, reason, pval, genes = row["ID"].iloc[0], row["Term"].iloc[0], \
                res["Reason"], row["Adjusted P-value"].iloc[0], row["Genes"].iloc[0]
            if go_id not in subset_go["ID"]:
                subset_go["ID"].append(go_id)
                subset_go["Name"].append(name)
                subset_go["Reason"].append(reason)
                subset_go["Genes"].append(genes)
                subset_go["Adjusted P-value"].append(pval)

        return subset_go