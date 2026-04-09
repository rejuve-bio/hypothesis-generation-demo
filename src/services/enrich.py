from collections import namedtuple
from typing import List, Optional
import pickle
import gseapy as gp
from src.config import Config
from src.config import create_dependencies
from gene_expression_tasks import get_coexpression_matrix_for_tissue
import pandas as pd
from loguru import logger


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
    
    def get_tissue_uberon_id(self, user_id: str, project_id: str, tissue_name: str) -> Optional[str]:
        """
        Retrieve UBERON ID from database for a given tissue name (public, for flow use).
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
                logger.warning(f"Couldn't find ensembl id for {g.upper()}")

        return ensembl_ids

    def get_coexpression_net(self, relevant_gene, tissue_name=None, cell_type=None, k=500, user_id=None, project_id=None, coexpression_data=None):
        """
        Given a gene, tissue and cell_type, return the top correlated genes using CellxGene API.
        If coexpression_data is provided (top_positive_tuples, top_negative_tuples, all_genes), use it
        instead of computing (allows Dask-offloaded pre-computation).
        """
        if coexpression_data is not None:
            top_positive_tuples, top_negative_tuples, all_genes = coexpression_data
        elif not tissue_name:
            return self._load_fallback_coexpression_data()
        elif not user_id or not project_id:
            logger.warning(f"user_id and project_id required for tissue-specific analysis, falling back to hardcoded data")
            return self._load_fallback_coexpression_data()
        else:
            try:
                # Get UBERON ID from database
                tissue_uberon_id = self.get_tissue_uberon_id(user_id, project_id, tissue_name)
                
                if not tissue_uberon_id:
                    logger.warning(f"No UBERON ID found for tissue '{tissue_name}', falling back to hardcoded data")
                    return self._load_fallback_coexpression_data()
                
                logger.info(f"Using tissue mapping from database: {tissue_name} -> {tissue_uberon_id}")
                
                # Run tissue-specific coexpression analysis using UBERON ID (inline - not Dask)
                top_positive_tuples, top_negative_tuples, all_genes = get_coexpression_matrix_for_tissue(
                    relevant_gene, tissue_uberon_id, cell_type, k=k
                )
            except Exception as e:
                logger.error(f"Error running CellxGene coexpression analysis: {e}")
                return self._load_fallback_coexpression_data()
        
        # Extract gene symbols from tuples
        if top_positive_tuples and isinstance(top_positive_tuples[0], tuple):
            top_positive_genes = [gene_data[0] for gene_data in top_positive_tuples]
        else:
            top_positive_genes = top_positive_tuples
        
        # Return both top genes and all genes for background
        return top_positive_genes, all_genes


    def _process_enrichment_results(self, res: pd.DataFrame) -> pd.DataFrame:
        """
        Process and filter enrichment results from gseapy.
        """
        res = res.copy()  # Avoid SettingWithCopyWarning
        res.drop("Gene_set", axis=1, inplace=True)
        res.insert(1, "ID", res["Term"].apply(
            lambda x: x.split("(")[1].split(")")[0]))
        res["Term"] = res["Term"].apply(lambda x: x.split("(")[0])
        res = res[res["Adjusted P-value"] < 0.05].copy()
        desc = []
        for _, row in res.iterrows():
            go_id = row["ID"]
            go_name = row["Term"]
            try:
                go_desc = self.go_map[go_id]["desc"]
                desc.append(go_desc)
            except KeyError:
                logger.warning(f"Couldn't find term {go_id}, {go_name} in go_map")
                desc.append("NA")
        res["Desc"] = desc
        res = res[["ID", "Term", "Desc", "Adjusted P-value", "Genes"]].copy()        
        return res

    def run(self, relevant_gene, tissue_name=None, user_id=None, project_id=None, coexpression_data=None):
        """
        Given a gene, return the enriched GO terms based on its co-expression network.
        If coexpression_data is provided (from Dask task), use it instead of computing.
        """
        library = "GO_Biological_Process_2023"
        organism = "Human"
        
        # Get coexpressed genes (or use pre-computed from Dask)
        coexpression_result = self.get_coexpression_net(
            relevant_gene, tissue_name, user_id=user_id, project_id=project_id,
            coexpression_data=coexpression_data
        )
        
        # Handle different return types (tuple for tissue-specific, list for fallback)
        if isinstance(coexpression_result, tuple):
            gene_list_ensembl, all_tissue_genes = coexpression_result
            # Convert Ensembl IDs to HGNC symbols for both gene list and background
            gene_list = self.get_hgnc_syms(gene_list_ensembl)
            
            # Using background size (5000 genes) instead of all tissue genes
            max_background_size = 5000
            if len(all_tissue_genes) > max_background_size:
                logger.info(f"Limiting background from {len(all_tissue_genes)} to {max_background_size} genes for better enrichment signal")
                background_genes_ensembl = all_tissue_genes[:max_background_size]
            else:
                background_genes_ensembl = all_tissue_genes
            
            background_genes = self.get_hgnc_syms(background_genes_ensembl)
            logger.info(f"Running tissue-specific enrichment for {relevant_gene} in {tissue_name}")
            logger.info(f"Using tissue-specific background: {len(background_genes)} genes from CellxGene analysis")
            logger.info(f"Converted {len(gene_list_ensembl)} Ensembl IDs to {len(gene_list)} HGNC symbols")
        else:
            # Fallback case - no tissue specified or fallback data used (already HGNC symbols)
            gene_list = coexpression_result
            background_genes = self._load_fallback_background_data()
            logger.info(f"Running standard enrichment for {relevant_gene}")
        
        logger.info(f"Relevant Gene: {relevant_gene}")
        logger.info(f"Gene list sample: {gene_list[:5] if gene_list else []}")
        logger.info(f"Total coexpressed genes: {len(gene_list) if gene_list else 0}")
        
        if not gene_list:
            logger.warning("No coexpressed genes found, returning empty results")
            return pd.DataFrame(columns=["ID", "Term", "Desc", "Adjusted P-value", "Genes"])
        
        res = gp.enrichr(gene_list=gene_list,
                         gene_sets=library,
                         background=background_genes,
                         organism=organism,
                         outdir=None).results
        
        return self._process_enrichment_results(res)