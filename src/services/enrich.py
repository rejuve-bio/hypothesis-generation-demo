import copy
import pickle
import re
from typing import Optional

import gseapy as gp
import pandas as pd
from loguru import logger

from src.config import DEFAULT_P_VALUE_THRESHOLD, Config
from src.tasks.gene_expression import get_coexpression_matrix_for_tissue

_ENSG_RE = re.compile(r"^ENSG\d+$", re.IGNORECASE)


class EnrichrAPIUnavailableError(RuntimeError):
    """Raised when all attempts to call the Enrichr API fail."""

    def __init__(self, message: str, *, variant: str | None = None) -> None:
        super().__init__(message)
        self.variant = variant

    def as_detail(self) -> dict[str, str]:
        detail = {"error_type": "enrichr_service_unavailable", "message": str(self)}
        if self.variant:
            detail["variant"] = self.variant
        return detail


class NoEnrichmentFoundError(ValueError):
    """Enrichment found nothing for the gene; ``reason`` says which step came up empty."""

    def __init__(
        self,
        message: str,
        *,
        reason: str,
        gene: str | None = None,
        tissue: str | None = None,
        variant: str | None = None,
        min_adjusted_p: float | None = None,
        max_adjusted_p: float | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.gene = gene
        self.tissue = tissue
        self.variant = variant
        self.min_adjusted_p = min_adjusted_p
        self.max_adjusted_p = max_adjusted_p

    def as_detail(self) -> dict[str, str]:
        detail = {
            "error_type": "no_enrichment_found",
            "reason": self.reason,
            "message": str(self),
        }
        for key in ("gene", "tissue", "variant"):
            value = getattr(self, key)
            if value:
                detail[key] = value
        for key in ("min_adjusted_p", "max_adjusted_p"):
            value = getattr(self, key)
            if value is not None:
                detail[key] = value
        return detail


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
            raise NoEnrichmentFoundError(
                f"Cannot build a co-expression network for {relevant_gene}: no tissue "
                "was given. Enrichment needs a tissue to compute co-expression in.",
                reason="no_tissue",
                gene=relevant_gene,
            )
        else:
            logger.info(f"[Enrich] Inline coexpression query for '{relevant_gene}' in '{tissue_name}'")
            top_positive_tuples, top_negative_tuples, all_genes = get_coexpression_matrix_for_tissue.fn(
                relevant_gene, tissue_name, k=k
            )
        
        # Extract gene symbols from tuples
        if top_positive_tuples and isinstance(top_positive_tuples[0], tuple):
            top_positive_genes = [gene_data[0] for gene_data in top_positive_tuples]
        else:
            top_positive_genes = top_positive_tuples
        
        # Return both top genes and all genes for background
        return top_positive_genes, all_genes


    def _process_enrichment_results(
        self, res: pd.DataFrame, p_value_threshold: float = DEFAULT_P_VALUE_THRESHOLD
    ) -> pd.DataFrame:
        """
        Process and filter enrichment results from gseapy.
        """
        res = res.copy()  # Avoid SettingWithCopyWarning
        res.drop("Gene_set", axis=1, inplace=True)
        res.insert(1, "ID", res["Term"].apply(
            lambda x: x.split("(")[1].split(")")[0]))
        res["Term"] = res["Term"].apply(lambda x: x.split("(")[0])
        res = res[res["Adjusted P-value"] < p_value_threshold].copy()
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

    def run(
        self,
        relevant_gene,
        tissue_name=None,
        coexpression_data=None,
        p_value_threshold=DEFAULT_P_VALUE_THRESHOLD,
    ):
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

        gene_list_ensembl, all_tissue_genes = self.get_coexpression_net(
            ensembl_gene, tissue_name, coexpression_data=coexpression_data
        )

        # Convert Ensembl IDs to HGNC symbols for both gene list and background
        gene_list = self.get_hgnc_syms(gene_list_ensembl)
        if not gene_list:
            raise self._no_coexpressed_genes_error(
                causal_gene_symbol, ensembl_gene, tissue_name,
                gene_list_ensembl, all_tissue_genes,
            )

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
        logger.info(f"Gene list sample: {gene_list[:5]}")

        res = self._run_enrichr_with_retry(
            gene_list=gene_list,
            gene_sets=library,
            background=background_genes,
            organism=organism,
            outdir=None,
        )

        enriched = self._process_enrichment_results(res, p_value_threshold)
        if enriched.empty:
            min_adjusted_p = max_adjusted_p = None
            p_range = ""
            if not res.empty:
                most = res.loc[res["Adjusted P-value"].idxmin()]
                least = res.loc[res["Adjusted P-value"].idxmax()]
                min_adjusted_p = float(most["Adjusted P-value"])
                max_adjusted_p = float(least["Adjusted P-value"])
                p_range = (
                    f" Adjusted p-values range from {min_adjusted_p:.3g} (most significant: "
                    f"'{most['Term'].split('(')[0].strip()}') to {max_adjusted_p:.3g} "
                    f"(least significant: '{least['Term'].split('(')[0].strip()}'); "
                    "set p_value_threshold within this range to include terms."
                )
            raise NoEnrichmentFoundError(
                f"{len(gene_list)} genes are co-expressed with {causal_gene_symbol} in "
                f"'{tissue_name}', but no GO Biological Process term is significantly "
                f"enriched among them (adjusted p < {p_value_threshold:g}).{p_range} "
                "Selecting a different tissue may also give a result.",
                reason="no_significant_go_terms",
                gene=causal_gene_symbol,
                tissue=tissue_name,
                min_adjusted_p=min_adjusted_p,
                max_adjusted_p=max_adjusted_p,
            )
        return enriched

    @staticmethod
    def _no_coexpressed_genes_error(
        gene_symbol, ensembl_gene, tissue_name, gene_list_ensembl, all_tissue_genes
    ) -> NoEnrichmentFoundError:
        """Build the error explaining why co-expression returned no genes."""
        if not all_tissue_genes:
            reason = "tissue_data_unavailable"
            message = (
                f"No CellxGene expression data could be loaded for '{tissue_name}', "
                f"so no co-expression network could be built for {gene_symbol}."
            )
        elif str(ensembl_gene).upper() not in {str(g).upper() for g in all_tissue_genes}:
            reason = "gene_not_in_tissue_data"
            message = (
                f"{gene_symbol} is not measured in the CellxGene expression data for "
                f"'{tissue_name}', so it has no co-expression network there."
            )
        elif gene_list_ensembl:
            reason = "coexpressed_genes_unmapped"
            message = (
                f"{len(gene_list_ensembl)} genes are co-expressed with {gene_symbol} in "
                f"'{tissue_name}', but none of them could be mapped to an HGNC gene "
                "symbol, which GO enrichment needs."
            )
        else:
            reason = "no_coexpressed_genes"
            message = (
                f"No genes are significantly co-expressed with {gene_symbol} in "
                f"'{tissue_name}'. It may be expressed in too few cells of that tissue "
                "to measure co-expression."
            )
        return NoEnrichmentFoundError(
            f"{message} Selecting a different tissue may give a result.",
            reason=reason,
            gene=gene_symbol,
            tissue=tissue_name,
        )
