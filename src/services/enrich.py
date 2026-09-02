import copy
import pickle
import re
from typing import List, Optional

import gseapy as gp
import pandas as pd
from loguru import logger

from src.config import Config
from src.catlas_census_mapping import CatlasMappingError
from src.tasks.gene_expression import get_coexpression_matrix_for_tissue

_ENSG_RE = re.compile(r"^ENSG\d+$", re.IGNORECASE)


class EnrichrAPIUnavailableError(RuntimeError):
    """Raised when all attempts to call the Enrichr API fail."""


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
    
    @staticmethod
    def is_ensembl_id(gene: str) -> bool:
        return bool(gene and _ENSG_RE.match(str(gene).strip()))

    @staticmethod
    def _normalize_gene_token(gene: str) -> str:
        return str(gene).strip().strip("'\"")

    def to_symbol(self, gene: str) -> str:
        """Resolve an Ensembl ID or gene name to an HGNC symbol (uppercase)."""
        if not gene:
            return gene
        token = self._normalize_gene_token(gene)
        if self.is_ensembl_id(token):
            symbol = self.ensembl_hgnc_map.get(token.upper())
            if symbol:
                return symbol.upper()
            return token.upper()
        return token.upper()

    def to_ensembl_id(self, gene: str) -> Optional[str]:
        """Resolve a gene symbol or Ensembl ID to a lowercase Ensembl ID."""
        if not gene:
            return None
        token = self._normalize_gene_token(gene)
        if self.is_ensembl_id(token):
            return token.lower()
        ensembl_id = self.hgnc_ensembl_map.get(token.upper())
        return ensembl_id.lower() if ensembl_id else None

    def annotate_graph_gene_names(self, graph: dict) -> dict:
        """Return a graph copy with gene node names set to HGNC symbols (ids unchanged)."""
        resolved = copy.deepcopy(graph)
        for node in resolved.get("nodes", []):
            if node.get("type") != "gene":
                continue
            node["name"] = self.to_symbol(node.get("name") or node.get("id", ""))
        return resolved

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

    def get_coexpression_net(self, relevant_gene, tissue_name=None, k=500, coexpression_data=None):
        """
        Return top correlated genes for a gene using CellxGene.
        """
        if coexpression_data is not None:
            top_positive_tuples, top_negative_tuples, all_genes = coexpression_data
        elif not tissue_name:
            return self._load_fallback_coexpression_data()
        else:
            try:
                logger.info(f"[Enrich] Inline coexpression query for '{relevant_gene}' in '{tissue_name}'")
                top_positive_tuples, top_negative_tuples, all_genes = get_coexpression_matrix_for_tissue.fn(
                    relevant_gene, tissue_name, k=k
                )
            except CatlasMappingError:
                raise
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

    def _run_enrichr_with_retry(
        self,
        *,
        gene_list,
        gene_sets,
        background,
        organism,
        outdir=None,
    ) -> pd.DataFrame:
        """Call Enrichr with progressively smaller backgrounds."""
        background_sizes = []
        for limit in (5000, 2500, 1000):
            size = min(len(background), limit)
            if size not in background_sizes:
                background_sizes.append(size)

        last_error = None
        for background_size in background_sizes:
            try:
                logger.info(
                    f"Calling Enrichr with background size {background_size}"
                )
                return gp.enrichr(
                    gene_list=gene_list,
                    gene_sets=gene_sets,
                    background=background[:background_size],
                    organism=organism,
                    outdir=outdir,
                ).results
            except Exception as exc:
                last_error = exc
                logger.warning(
                    f"Enrichr call failed with background size {background_size}: {exc}"
                )

        raise EnrichrAPIUnavailableError(
            "Enrichr API remained unavailable after retrying with background sizes "
            f"{background_sizes}"
        ) from last_error

    def run(self, relevant_gene, tissue_name=None, coexpression_data=None):
        """
        Given a gene, return the enriched GO terms based on its co-expression network.
        If coexpression_data is provided (from Dask task), use it instead of computing.
        """
        library = "GO_Biological_Process_2023"
        organism = "human"
        causal_gene_symbol = self.to_symbol(relevant_gene)
        ensembl_gene = self.to_ensembl_id(relevant_gene)
        if ensembl_gene is None:
            ensembl_gene = relevant_gene
            logger.warning(
                f"Could not map '{relevant_gene}' to Ensembl ID; "
                "coexpression queries may fail"
            )

        coexpression_result = self.get_coexpression_net(
            ensembl_gene, tissue_name, coexpression_data=coexpression_data
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
            logger.info(
                f"Running tissue-specific enrichment for {causal_gene_symbol} "
                f"in {tissue_name}"
            )
            logger.info(f"Using tissue-specific background: {len(background_genes)} genes from CellxGene analysis")
            logger.info(f"Converted {len(gene_list_ensembl)} Ensembl IDs to {len(gene_list)} HGNC symbols")
        else:
            # Fallback case - no tissue specified or fallback data used (already HGNC symbols)
            gene_list = coexpression_result
            background_genes = self._load_fallback_background_data()
            logger.info(f"Running standard enrichment for {causal_gene_symbol}")
        
        logger.info(f"Relevant Gene: {causal_gene_symbol}")
        logger.info(f"Gene list sample: {gene_list[:5] if gene_list else []}")
        logger.info(f"Total coexpressed genes: {len(gene_list) if gene_list else 0}")
        
        if not gene_list:
            logger.warning("No coexpressed genes found, returning empty results")
            return pd.DataFrame(columns=["ID", "Term", "Desc", "Adjusted P-value", "Genes"])
        
        res = self._run_enrichr_with_retry(
            gene_list=gene_list,
            gene_sets=library,
            background=background_genes,
            organism=organism,
            outdir=None,
        )
        
        return self._process_enrichment_results(res)
