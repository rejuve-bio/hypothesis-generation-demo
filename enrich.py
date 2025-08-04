from collections import namedtuple
from typing import NamedTuple, List
import pickle
import gseapy as gp
from config import Config

class Enrich:

    def __init__(self, ensembl_hgnc_map_path, hgnc_ensembl_map_path,
                 go_map_path):

        self.ensembl_hgnc_map = pickle.load(open(ensembl_hgnc_map_path, "rb"))
        self.hgnc_ensembl_map = pickle.load(open(hgnc_ensembl_map_path, "rb"))
        self.go_map = pickle.load(open(go_map_path, "rb"))

    def get_hgnc_syms(self, ensg_ids):
        hgnc_symbols = []
        for g in ensg_ids:
            sym = self.ensembl_hgnc_map.get(g.upper(), None)
            if sym is not None:
                hgnc_symbols.append(sym)

        return hgnc_symbols

    def get_ensembl_ids(self, hgnc_syms):
        ensembl_ids = []
        for g in hgnc_syms:
            ensembl_id = self.hgnc_ensembl_map.get(g.upper(), None)
            if ensembl_id is not None:
                ensembl_ids.append(ensembl_id.lower())
            else:
                print(f"Couldn't find ensembl id for {g.upper()}")

        return ensembl_ids

    def get_coexpression_net(self, relevant_gene):
        """
        Given a gene, tissue and cell_type, return the top correlated genes.
        :param gene: Gene ID
        :param tissue: Tissue name
        :param cell_type: Cell Type
        :return: List of genes
        """
        #TODO: Implement this using CellxGene API
        config = Config.from_env()
        brown_preadipocytes_top_corr_genes = pickle.load(open(f"{config.data_dir}/brown_preadipocytes_irx3_corr_top_500_genes.pkl", "rb"))
        return brown_preadipocytes_top_corr_genes


    def run(self, relevant_gene):
        """
        Given a gene, return the enriched GO terms based on its co-expression network.
        """
        library = "GO_Biological_Process_2023"
        organism = "Human"

        config = Config.from_env()
        background_genes = pickle.load(open(f"{config.data_dir}/brown_preadipocytes_irx3_corr_background_genes.pkl", "rb"))
        gene_list = self.get_coexpression_net(relevant_gene)
        # gene_list = self.get_hgnc_syms(gene_list)
        # background_genes = self.get_hgnc_syms(background_genes) #TODO uncomment when working with CellxGene
        print(f"Relevant Gene: {relevant_gene}")
        print(f"Gene list: {gene_list[:5]}")
        res = gp.enrichr(gene_list=gene_list,
                                      gene_sets=library,
                                      background=background_genes,
                                      organism=organism,
                                      outdir=None).results
        res.drop("Gene_set", axis=1, inplace=True)
        res.insert(1, "ID", res["Term"].apply(
            lambda x: x.split("(")[1].split(")")[0]))
        res["Term"] = res["Term"].apply(lambda x: x.split("(")[0])
        res = res[res["Adjusted P-value"] < 0.05]
        desc = []
        for _, row in res.iterrows():
            go_id = row["ID"]
            go_name = row["Term"]
            try:
                go_desc = self.go_map[go_id]["desc"]
                desc.append(go_desc)
            except KeyError:
                print(f"Couldn't find term {go_id}, {go_name}")
                desc.append("NA")

        res["Desc"] = desc
        res.drop(res.columns.difference(["ID", "Term", "Desc", "Adjusted P-value", "Genes"]), inplace=True, axis=1)
        return res