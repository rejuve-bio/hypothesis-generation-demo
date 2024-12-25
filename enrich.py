from collections import namedtuple
from typing import NamedTuple, List
import pickle
import gseapy as gp
import cellxgene_census
import pandas as pd
import numpy as np
from scipy.stats import pearsonr

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

    # def get_coexpression_net(self, relevant_gene):
    #     """
    #     Given a gene, tissue and cell_type, return the top correlated genes.
    #     :param gene: Gene ID
    #     :param tissue: Tissue name
    #     :param cell_type: Cell Type
    #     :return: List of genes
    #     """
    #     #TODO: Implement this using CellxGene API
    #     brown_preadipocytes_top_corr_genes = pickle.load(open(f"./data/brown_preadipocytes_irx3_corr_top_500_genes.pkl", "rb"))
    #     return brown_preadipocytes_top_corr_genes
    
    def get_coexpression_matrix(self, gene, tissue, cell_type, k=500):
            with cellxgene_census.open_soma() as census:
                adata = cellxgene_census.get_anndata(
                    census=census,
                    organism="Homo sapiens",
                    obs_value_filter=f"cell_type == '{cell_type}'", # use obs_value_filter=f"tissue_general == '{tissue}'" if you want to filter with tissue type
                    # obs_value_filter=f"tissue_general == '{tissue}'",
                    column_names={"obs": ["assay", "cell_type", "tissue", "tissue_general", "suspension_type", "disease"]},
                )
                gene_expression_sum = np.array((adata.X > 0).sum(axis=0)).flatten()
                adata_filtered = adata[:, gene_expression_sum > 0]
                genes = adata_filtered.var['feature_id']

        
                df_expression = pd.DataFrame(adata_filtered.X.toarray(), columns=genes)
                
                if gene in df_expression.columns:
                    non_zero_samples = df_expression[df_expression[gene] > 0]
                    total_samples = df_expression.shape[0]
                    non_zero_sample_count = non_zero_samples.shape[0]
                    non_zero_percentage = (non_zero_sample_count / total_samples) * 100
                    print(f"Total samples: {total_samples}")
                    print(f"Samples with non-zero expression for '{gene}': {non_zero_sample_count} ({non_zero_percentage:.2f}%)")

                    correlations = {}
                    for g in non_zero_samples.columns:
                        if g != gene:
                            corr, p_value = pearsonr(non_zero_samples[gene], non_zero_samples[g])
                            if p_value < 0.05:
                                correlations[g] = corr
                                
                  
                    sorted_correlations = sorted(correlations.items(), key=lambda x: x[1], reverse=True)
                    top_positive = sorted_correlations[:k]
                    top_negative = sorted_correlations[-k:]

                    return top_positive, top_negative, genes
                    
                else:
                    print(f"Gene of interest '{gene}' not found in the dataset.")
                    return [], []
                

    def run(self, relevant_gene):
        """
        Given a gene, return the enriched GO terms based on its co-expression network.
        """
        library = "GO_Biological_Process_2023"
        organism = "Human"

        #gene_of_interest = 'ENSG00000140718' #FTO
        #gene_of_interest= relevant_gene
        gene_of_interest = 'ENSG00000177508' #IRX3
        
        tissue_type = 'Adipose'
        cell_type = 'preadipocyte'
        top_positive, top_negative, all_genes = self.get_coexpression_matrix(gene_of_interest, tissue_type, cell_type, k=500)
        print("top_positive", top_positive[:10])

        top_positive_hgnc = [(self.ensembl_hgnc_map.get(gene, gene), corr) for gene, corr in top_positive]
        print("top_positive_hgnc", top_positive_hgnc[:10])
        top_negative_hgnc = [(self.ensembl_hgnc_map.get(gene, gene), corr) for gene, corr in top_negative]
        all_genes_hgnc = [self.ensembl_hgnc_map.get(gene, gene) for gene in all_genes]
        print("all_genes_hgnc", all_genes_hgnc[:10])
       
        print(all_genes, "gene_list")
        
      
       
        res = gp.enrichr(gene_list=[gene[0] for gene in top_positive_hgnc],
                                gene_sets=library,
                                background=all_genes_hgnc,
                                organism=organism,
                                outdir=None).results
        
        res.to_csv("res_results_test_00.csv")
        
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
        res.to_csv("enrichment_results.csv", index=False)

        return  res

