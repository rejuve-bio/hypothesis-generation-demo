import tempfile
from typing import List, Optional, Tuple

import faiss
import outlines
import torch
from datasets import load_dataset, Dataset
from outlines.models import Transformers
from pydantic import BaseModel, Field
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModel

# torch.set_grad_enabled(False)
device = "cuda" if torch.cuda.is_available() else "cpu"

class SemanticSearch:

    def __init__(self, embedding_model="mixedbread-ai/mxbai-embed-large-v1",
                 hf_token=None):
        """
        :param embedding_model: The name of the hugging face model to use for embedding
        """

        self.embed_model = SentenceTransformer(embedding_model, token=hf_token)

    def get_relevant_go(self, phentoype, enrich_tbl, k=10):
        """
        Given a phenotype, a sequence variant and an enrichment analysis table, get the top k relevant GO terms relevant to the phenotype by prompting the LLM using RAG
        :param variant: Sequence Variant
        :param enrich_tbl: Table containing the over-presentation test expected columns are ID, Term, Desc, Adjusted P-val
        :return: dict obj containing the k relevant GO terms, their p-val and the reason why the LLM thinks they are relevant to the phenotype
        """
        df = enrich_tbl.copy()
        df.drop(columns=["ID", "Adjusted P-value", "Genes"], inplace=True)
        dataset = Dataset.from_pandas(df)
        dataset = dataset.map(
            lambda x: {"text": x["Term"] + "\n" + x["Desc"]}
        )
        dataset = dataset.map(
            lambda x: {"embeddings": self.embed_model.encode(x["text"])}
        )
        dataset.add_faiss_index(column="embeddings")
        # query = f"A biologist is studying the causal relationship between SNP {variant} and {phentoype}."
        embedded_query = self.embed_model.encode(phentoype)
        scores, docs = dataset.get_nearest_examples("embeddings", embedded_query, k)
        subset_go = {"ID": [], "Name": [], "Genes": [], "Adjusted P-value": []}
        for doc in docs["Term"]:
            print(f"Document: {doc}")
            row = enrich_tbl[enrich_tbl["Term"].str.contains(doc, case=False)]
            if len(row) == 0:
                print(f"Couldn't find {doc}")
                continue
            elif len(row) > 1:
                row = row.head(1)
            go_id, name, pval, genes = row["ID"].iloc[0], row["Term"].iloc[0], \
                row["Adjusted P-value"].iloc[0], row["Genes"].iloc[0]
            if go_id not in subset_go["ID"]:
                subset_go["ID"].append(go_id)
                subset_go["Name"].append(name)
                # subset_go["Rank"].append(rank)
                subset_go["Genes"].append(genes)
                subset_go["Adjusted P-value"].append(pval)

        return subset_go