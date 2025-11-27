from collections import namedtuple
from typing import List, Optional
import pickle
import gseapy as gp
from config import Config
from config import create_dependencies
from gene_expression_tasks import get_coexpression_matrix_for_tissue
import pandas as pd


class Enrich:

    def __init__(self, ensembl_hgnc_map_path, hgnc_ensembl_map_path,
                 go_map_path):

        with open(ensembl_hgnc_map_path, "rb") as f:
            self.ensembl_hgnc_map = pickle.load(f)
        with open(hgnc_ensembl_map_path, "rb") as f:
            self.hgnc_ensembl_map = pickle.load(f)
        with open(go_map_path, "rb") as f:
            self.go_map = pickle.load(f)
        
        self.config = Config.from_env()

    def _load_fallback_coexpression_data(self) -> List[str]:
        """Load hardcoded brown preadipocytes coexpression data as fallback."""
        fallback_path = f"{self.config.data_dir}/brown_preadipocytes_irx3_corr_top_500_genes.pkl"
        with open(fallback_path, "rb") as f:
            return pickle.load(f)
    
    def _load_fallback_background_data(self) -> List[str]:
        """Load hardcoded brown preadipocytes background genes as fallback."""
        fallback_path = f"{self.config.data_dir}/brown_preadipocytes_irx3_corr_background_genes.pkl"
        with open(fallback_path, "rb") as f:
            return pickle.load(f)
    
    def _get_tissue_uberon_id(self, user_id: str, project_id: str, tissue_name: str) -> Optional[str]:
        """
        Retrieve UBERON ID from database for a given tissue name.
        """
        deps = create_dependencies(self.config)
        gene_expression = deps['gene_expression']
        
        # Retrieve tissue mapping from database
        tissue_mapping = gene_expression.get_tissue_mapping(user_id, project_id, tissue_name)
        
        if not tissue_mapping:
            return None
        
        # Extract UBERON ID from database record
        tissue_uberon_id = (tissue_mapping.get('cellxgene_descendant_uberon_id') or 
                           tissue_mapping.get('cellxgene_parent_uberon_id'))
        
        return tissue_uberon_id

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

    def get_coexpression_net(self, relevant_gene, tissue_name=None, cell_type=None, k=500, user_id=None, project_id=None):
        """
        Given a gene, tissue and cell_type, return the top correlated genes using CellxGene API.
        """
        if not tissue_name:
            return self._load_fallback_coexpression_data()
        
        if not user_id or not project_id:
            print(f"Warning: user_id and project_id required for tissue-specific analysis, falling back to hardcoded data")
            return self._load_fallback_coexpression_data()
        
        try:
            # Get UBERON ID from database
            tissue_uberon_id = self._get_tissue_uberon_id(user_id, project_id, tissue_name)
            
            if not tissue_uberon_id:
                print(f"Warning: No UBERON ID found for tissue '{tissue_name}', falling back to hardcoded data")
                return self._load_fallback_coexpression_data()
            
            print(f"Using tissue mapping from database: {tissue_name} -> {tissue_uberon_id}")
            
            # Run tissue-specific coexpression analysis using UBERON ID
            top_positive_tuples, top_negative_tuples, all_genes = get_coexpression_matrix_for_tissue(
                relevant_gene, tissue_uberon_id, cell_type, k=k
            )
            
            # Extract gene symbols from tuples
            if top_positive_tuples and isinstance(top_positive_tuples[0], tuple):
                return [gene_data[0] for gene_data in top_positive_tuples]
            else:
                return top_positive_tuples
            
        except Exception as e:
            print(f"Error running CellxGene coexpression analysis: {e}")
            return self._load_fallback_coexpression_data()


    def _get_tissue_specific_background(self, relevant_gene: str, user_id: str, 
                                        project_id: str, tissue_name: str) -> Optional[List[str]]:
        """
        Get tissue-specific background genes for enrichment analysis.
        :return: List of background genes or None if failed
        """
        try:
            tissue_uberon_id = self._get_tissue_uberon_id(user_id, project_id, tissue_name)
            
            if not tissue_uberon_id:
                raise ValueError(f"No UBERON ID found for tissue '{tissue_name}'")
            
            print(f"Using tissue mapping from database: {tissue_name} -> {tissue_uberon_id}")
            
            # Get all genes from the tissue analysis (this is our background)
            _, _, all_tissue_genes = get_coexpression_matrix_for_tissue(
                relevant_gene, tissue_uberon_id, None, k=500
            )
            
            # Convert Ensembl IDs to HGNC symbols for background
            background_genes = self.get_hgnc_syms(all_tissue_genes)
            print(f"Running tissue-specific enrichment for {relevant_gene} in {tissue_name} ({tissue_uberon_id})")
            print(f"Using tissue-specific background: {len(background_genes)} genes from CellxGene analysis")
            
            return background_genes
            
        except Exception as e:
            print(f"Failed to get tissue-specific background genes: {e}")
            print("Falling back to enrichr default background")
            return None 
    def _process_enrichment_results(self, res: pd.DataFrame) -> pd.DataFrame:
        """
        Process and filter enrichment results from gseapy.
        """
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
        res.drop(res.columns.difference(["ID", "Term", "Desc", "Adjusted P-value", "Genes"]), 
                inplace=True, axis=1)
        
        return res

    def run(self, relevant_gene, tissue_name=None, user_id=None, project_id=None):
        """
        Given a gene, return the enriched GO terms based on its co-expression network.
        """
        library = "GO_Biological_Process_2023"
        organism = "Human"
        
        # Get coexpressed genes
        gene_list = self.get_coexpression_net(relevant_gene, tissue_name, user_id=user_id, project_id=project_id)
        
        # Determine background genes
        if tissue_name and gene_list and user_id and project_id:
            background_genes = self._get_tissue_specific_background(
                relevant_gene, user_id, project_id, tissue_name
            )
        else:
            background_genes = self._load_fallback_background_data()
            print(f"Running standard enrichment for {relevant_gene}")
        
        print(f"Relevant Gene: {relevant_gene}")
        print(f"Gene list: {gene_list[:5] if gene_list else []}")
        print(f"Total coexpressed genes: {len(gene_list) if gene_list else 0}")
        
        if not gene_list:
            print("No coexpressed genes found, returning empty results")
            return pd.DataFrame(columns=["ID", "Term", "Desc", "Adjusted P-value", "Genes"])
        
        # Run enrichment
        res = gp.enrichr(gene_list=gene_list,
                         gene_sets=library,
                         background=background_genes,
                         organism=organism,
                         outdir=None).results
        
        return self._process_enrichment_results(res)