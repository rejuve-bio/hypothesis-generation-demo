import copy
import os
import json
import pickle
import re
import threading
from typing import Optional

import gseapy as gp
import pandas as pd
from loguru import logger

from src.config import Config, significance_floor
from src.tasks.gene_expression import get_coexpression_matrix_for_tissue


_ENSG_RE = re.compile(r"^ENSG\d+$", re.IGNORECASE)

# Enrichr returns all-NaN p-values for genome-scale backgrounds (~60k genes).
_MAX_BACKGROUND_SIZE = 20000


_GO_ID_IN_TERM_RE = re.compile(r"\((GO:\d+)\)")
# GO names can contain their own parentheses, so anchor to the trailing id.
_GO_ID_SUFFIX_RE = re.compile(r"^(?P<name>.*?)\s*\((?P<go_id>GO:\d+)\)\s*$")


def split_term_and_go_id(term: str) -> tuple[str, str]:
    """Split "<name> (GO:0001234)" into its name and id."""
    text = str(term).strip()
    match = _GO_ID_SUFFIX_RE.match(text)
    if match:
        return match.group("name").strip(), match.group("go_id")
    found = _GO_ID_IN_TERM_RE.findall(text)
    if found:
        go_id = found[-1]
        return text.replace(f"({go_id})", "").strip(), go_id
    return text, text
_GO_TERM_SIZES: dict | None = None
_GO_TERM_SIZES_LOCK = threading.Lock()
# Distinct genes in GO_Biological_Process_2023 (Human).
_GO_CORPUS_GENES_DEFAULT = 14697


def _parse_term_size(overlap: pd.Series) -> pd.Series:
    """Extract the GO term's total gene count from Enrichr's "k/n" Overlap."""
    return pd.to_numeric(
        overlap.astype(str).str.split("/").str[-1], errors="coerce"
    )


def go_term_sizes(library: str = "GO_Biological_Process_2023") -> dict[str, int]:
    """Map GO id -> number of genes annotated to it, from the Enrichr library.

    The background-corrected endpoint omits the Overlap column, so term size
    is not recoverable from the results themselves.
    """
    global _GO_TERM_SIZES
    if _GO_TERM_SIZES is None:
        with _GO_TERM_SIZES_LOCK:
            if _GO_TERM_SIZES is None:
                record = _load_go_term_sizes(library)
                if record["sizes"]:
                    _GO_TERM_SIZES = record
                else:
                    return {}
    return _GO_TERM_SIZES["sizes"]


def go_corpus_gene_count(library: str = "GO_Biological_Process_2023") -> int:
    """Distinct genes annotated anywhere in the library.

    The denominator for a term's information content.
    """
    go_term_sizes(library)
    return (_GO_TERM_SIZES or {}).get("corpus_genes") or _GO_CORPUS_GENES_DEFAULT


def _load_go_term_sizes(library: str) -> dict[str, int]:
    cache_path = os.getenv("GO_TERM_SIZES_CACHE", "data/go_term_sizes.json")
    try:
        with open(cache_path) as fh:
            cached = json.load(fh)
        if isinstance(cached, dict) and "sizes" in cached:
            return {
                "sizes": {k: int(v) for k, v in cached["sizes"].items()},
                "corpus_genes": int(cached.get("corpus_genes") or 0) or None,
            }
        # pre-corpus-count cache: sizes only
        return {"sizes": {k: int(v) for k, v in cached.items()}, "corpus_genes": None}
    except (OSError, ValueError, TypeError):
        pass

    try:
        sizes: dict[str, int] = {}
        corpus: set[str] = set()
        for term, genes in gp.get_library(name=library, organism="Human").items():
            corpus.update(str(g).strip().upper() for g in genes)
            match = _GO_ID_IN_TERM_RE.search(term)
            if match:
                sizes[match.group(1)] = len(genes)
        if not sizes:
            raise ValueError(f"no GO ids parsed from library {library}")
        record = {"sizes": sizes, "corpus_genes": len(corpus)}
        try:
            os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
            with open(cache_path, "w") as fh:
                json.dump(record, fh)
        except OSError as exc:
            logger.warning(f"Could not cache GO term sizes to {cache_path}: {exc}")
        logger.info(
            f"Loaded {len(sizes)} GO term sizes from {library} "
            f"({len(corpus)} distinct genes)"
        )
        return record
    except Exception as exc:
        logger.warning(
            f"Could not load GO term sizes from {library}: {exc}. "
            "Ranking will fall back to size-agnostic scoring."
        )
        return {"sizes": {}, "corpus_genes": None}


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

    def _process_enrichment_results(self, res: pd.DataFrame) -> pd.DataFrame:
        """Split the GO id out of each term, attach its description and size."""
        res = res.copy()  # Avoid SettingWithCopyWarning
        res.drop("Gene_set", axis=1, inplace=True)
        split = res["Term"].apply(split_term_and_go_id)
        res.insert(1, "ID", [go_id for _, go_id in split])
        res["Term"] = [name for name, _ in split]
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

        if "Overlap" in res.columns:
            res["Term Size"] = _parse_term_size(res["Overlap"])
        else:
            sizes = go_term_sizes()
            res["Term Size"] = (
                res["ID"].map(sizes).astype("Float64") if sizes else pd.NA
            )
            # A term Enrichr scores may be absent from the published library
            # we read sizes from. Unmatched terms score as the broadest.
            if sizes is not None and len(res):
                matched = int(pd.to_numeric(res["Term Size"], errors="coerce").notna().sum())
                if matched < len(res) * 0.9:
                    logger.warning(
                        f"GO term sizes matched only {matched}/{len(res)} enriched "
                        f"terms; specificity weighting is degraded. The Enrichr "
                        f"library version may have diverged from the cached sizes."
                    )
        return res[
            ["ID", "Term", "Desc", "Adjusted P-value", "Genes", "Term Size"]
        ].copy()

    def _run_enrichr_with_retry(
        self,
        *,
        gene_list,
        gene_sets,
        background,
        organism,
        outdir=None,
    ) -> pd.DataFrame:
        """Call Enrichr, shrinking the background only if a call fails.

        Rungs stop at 2000; the backgroundenrich endpoint returns a non-JSON
        body below roughly that size, so smaller rungs can never succeed.
        """
        background_sizes = []
        for limit in (len(background), 5000, 2000):
            size = min(len(background), limit)
            if size >= 2000 and size not in background_sizes:
                background_sizes.append(size)
        if not background_sizes:
            background_sizes = [len(background)]

        last_error = None
        for background_size in background_sizes:
            try:
                logger.info(
                    f"Calling Enrichr with background size {background_size}"
                )
                # The query is unioned back in: it sits at the tail of the
                # background, so a head slice would drop exactly the genes the
                # caller added to stop Enrichr discarding them.
                trimmed = list(
                    dict.fromkeys(list(background[:background_size]) + list(gene_list))
                )
                return gp.enrichr(
                    gene_list=gene_list,
                    gene_sets=gene_sets,
                    background=trimmed,
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

    def _run_enrichr(self, relevant_gene, tissue_name=None, coexpression_data=None):
        """Raw enrichr output for a gene's co-expression network."""
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

        max_background_size = int(
            os.getenv("GO_MAX_BACKGROUND_SIZE", str(_MAX_BACKGROUND_SIZE))
        )
        if len(all_tissue_genes) > max_background_size:
            logger.info(
                f"Limiting background from {len(all_tissue_genes)} to "
                f"{max_background_size} genes"
            )
            background_genes_ensembl = all_tissue_genes[:max_background_size]
        else:
            background_genes_ensembl = all_tissue_genes
        # Enrichr drops query genes absent from the background.
        background_genes_ensembl = list(
            dict.fromkeys(list(background_genes_ensembl) + list(gene_list_ensembl))
        )

        background_genes = self.get_hgnc_syms(background_genes_ensembl)
        missing = len(set(gene_list) - set(background_genes))
        logger.info(
            f"Running tissue-specific enrichment for {causal_gene_symbol} "
            f"in {tissue_name}"
        )
        logger.info(
            f"Background: {len(background_genes)} tissue-expressed genes "
            f"(query genes not covered: {missing})"
        )
        logger.info(f"Converted {len(gene_list_ensembl)} Ensembl IDs to {len(gene_list)} HGNC symbols")
        logger.info(f"Gene list sample: {gene_list[:5]}")

        res = self._run_enrichr_with_retry(
            gene_list=gene_list,
            gene_sets=library,
            background=background_genes,
            organism=organism,
            outdir=None,
        )

        return res, causal_gene_symbol, gene_list

    def run(
        self, relevant_gene, tissue_name=None, coexpression_data=None, p_value_threshold=None
    ):
        """Significantly enriched GO terms for a gene's co-expression network."""
        res, causal_gene_symbol, gene_list = self._run_enrichr(
            relevant_gene, tissue_name, coexpression_data
        )
        all_terms = self._process_enrichment_results(res)
        floor = significance_floor(p_value_threshold)
        pvalues = pd.to_numeric(all_terms["Adjusted P-value"], errors="coerce")
        enriched = all_terms[pvalues < floor].copy()
        if enriched.empty:
            min_adjusted_p = max_adjusted_p = None
            p_range = ""
            if pvalues.notna().any():
                most = all_terms.loc[pvalues.idxmin()]
                least = all_terms.loc[pvalues.idxmax()]
                min_adjusted_p = float(pvalues.min())
                max_adjusted_p = float(pvalues.max())
                p_range = (
                    f" Adjusted p-values range from {min_adjusted_p:.3g} (most significant: "
                    f"'{most['Term']}') to {max_adjusted_p:.3g} (least significant: "
                    f"'{least['Term']}'); set p_value_threshold within this range to "
                    "include terms."
                )
            raise NoEnrichmentFoundError(
                f"{len(gene_list)} genes are co-expressed with {causal_gene_symbol} in "
                f"'{tissue_name}', but no GO Biological Process term is significantly "
                f"enriched among them (adjusted p < {floor:g}).{p_range} Selecting a "
                "different tissue may also give a result.",
                reason="no_significant_go_terms",
                gene=causal_gene_symbol,
                tissue=tissue_name,
                min_adjusted_p=min_adjusted_p,
                max_adjusted_p=max_adjusted_p,
            )
        logger.info(
            f"Enrichr returned {len(all_terms)} GO terms; "
            f"{len(enriched)} pass adj p < {floor:g}"
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
