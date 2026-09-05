import json
import os
import re
import subprocess
import threading
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import cellxgene_census
import numpy as np
import pandas as pd
import tiledbsoma as soma
from loguru import logger
from prefect import task
from scipy import sparse
from scipy.stats import pearsonr
from statsmodels.stats.multitest import fdrcorrection

from src.config import Config
from src.utils import get_deps
from src.tasks.ldsc_sumstats import harmonized_to_ldsc_sumstats_zhang
from src.catlas_census_mapping import (
    CatlasMappingError,
    ResolvedCensusCellFilter,
    _escape_soma_string_literal,
    resolve_ldsc_for_census,
)

CENSUS_VERSION = "2024-07-01"

# In-process memoization on top of the disk cache below, so repeated calls
# within the same worker process (e.g. several genes queried against the
# same tissue in one flow run) don't even hit the filesystem twice.
_TISSUE_CONTEXT_CACHE: dict = {}
_TISSUE_CONTEXT_LOCK = threading.Lock()


@dataclass
class _TissueCoexpressionContext:
    """Everything needed to answer coexpression queries for a tissue, built
    from a single CellxGene Census download and reused across every gene
    queried against that tissue (in-process, and on disk across runs)."""

    cell_type: str
    obs_joinids: np.ndarray
    cell_sums: np.ndarray
    genes: list          # feature_ids of the top ~15k highly-expressed genes
    gene_index: dict      # feature_id -> column index into `matrix`
    all_genes_list: list  # feature_ids of every gene in the Census dataset
    matrix: sparse.csr_matrix  # raw counts, shape (len(obs_joinids), len(genes))


def _census_obs_axis_query_for_resolved(
    experiment,
    resolved: ResolvedCensusCellFilter,
):
    """Try cell_type labels then CL ids until a query returns > 0 cells."""
    if resolved.skip_coexpression:
        logger.info(
            f"[Census] skip coexpression for ldsc={resolved.ldsc_name!r} "
            f"reason={resolved.skip_reason} ({resolved.source})"
        )
        return None

    for lab in resolved.cell_type_labels:
        esc = _escape_soma_string_literal(lab)
        vf = f"cell_type == '{esc}'"
        try:
            aq = experiment.axis_query(
                measurement_name="RNA",
                obs_query=soma.AxisQuery(value_filter=vf),
            )
            n = len(aq.obs_joinids())
            if n > 0:
                logger.info(
                    f"[Census] ldsc={resolved.ldsc_name!r} matched n={n} via cell_type={lab!r} "
                    f"({resolved.source})"
                )
                return aq
        except Exception as e:
            logger.warning(f"[Census] cell_type filter failed {vf!r}: {e}")

    for cl in resolved.cl_ids:
        esc = _escape_soma_string_literal(cl)
        vf = f"cell_type_ontology_term_id == '{esc}'"
        try:
            aq = experiment.axis_query(
                measurement_name="RNA",
                obs_query=soma.AxisQuery(value_filter=vf),
            )
            n = len(aq.obs_joinids())
            if n > 0:
                logger.info(
                    f"[Census] ldsc={resolved.ldsc_name!r} matched n={n} via {cl} ({resolved.source})"
                )
                return aq
        except Exception as e:
            logger.warning(f"[Census] CL id filter failed {vf!r}: {e}")

    logger.warning(
        f"[Census] no obs for ldsc={resolved.ldsc_name!r} ({resolved.source}) "
        f"labels={resolved.cell_type_labels} cl_ids={resolved.cl_ids}"
    )
    return None


@task(log_prints=True)
def setup_ldsc_environment(base_dir):
    """LDSC is already installed in Docker, just return the path"""
    return "/opt/ldsc"

@task(log_prints=True)
def run_ldsc_analysis(ldsc_dir, gwas_file, output_prefix):
    """ cell-type LDSC: harmonized SSF -> sumstats (HM3 + strand filter), then --h2-cts."""
    config = Config.from_env()
    repo_root = os.path.abspath(config.repo_root)

    output_dir = os.path.dirname(output_prefix)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    gwas_file = os.path.abspath(gwas_file)
    output_prefix = os.path.abspath(output_prefix)
    output_dir = os.path.dirname(output_prefix)

    ldsc_work_dir = os.path.join(output_dir, "ldsc_analysis")
    os.makedirs(ldsc_work_dir, exist_ok=True)
    sumstats_path = os.path.join(ldsc_work_dir, "zhang.sumstats.gz")

    w_hm3 = config.resolve_ldsc_path(config.ldsc_w_hm3_snplist)
    if not os.path.isfile(w_hm3):
        raise FileNotFoundError(f"LDSC w_hm3.snplist not found: {w_hm3}")
    harmonized_to_ldsc_sumstats_zhang(gwas_file, sumstats_path, w_hm3)

    cts_path = config.resolve_ldsc_path(config.ldsc_cts_file)
    if not os.path.isfile(cts_path):
        raise FileNotFoundError(f"LDSC CTS file not found: {cts_path}")

    # Validate all CTS entries have their ldscore files — fail clearly if any are missing
    missing_cts = []
    with open(cts_path) as fh:
        for raw in fh:
            line = raw.rstrip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            prefix = parts[1]
            abs_prefix = prefix if os.path.isabs(prefix) else os.path.join(repo_root, prefix)
            probe = f"{abs_prefix}1.l2.ldscore.gz"
            if not os.path.isfile(probe):
                missing_cts.append(f"{parts[0]} → {probe}")

    if missing_cts:
        raise FileNotFoundError(
            f"[LDSC] {len(missing_cts)} CTS entries are missing ldscore files:\n"
            + "\n".join(missing_cts[:20])
            + ("\n..." if len(missing_cts) > 20 else "")
        )

    ref_ld_chr = config.get_ldsc_ref_ld_chr()
    w_ld = config.ldsc_w_ld_prefix

    cmd = [
        "/usr/local/bin/ldsc",
        "--h2-cts",
        sumstats_path,
        "--ref-ld-chr",
        ref_ld_chr,
        "--ref-ld-chr-cts",
        cts_path,
        "--w-ld-chr",
        w_ld,
        "--out",
        output_prefix,
    ]
    logger.info(f"[LDSC] cwd={repo_root} ref_ld_chr={ref_ld_chr}")
    logger.info(f"[LDSC] CTS={cts_path}")
    logger.info(f"[LDSC] cmd={' '.join(cmd)}")

    ldsc_tail = deque(maxlen=250)
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
        cwd=repo_root,
    )
    while True:
        line = process.stdout.readline()
        if line == "" and process.poll() is not None:
            break
        if line:
            stripped = line.rstrip()
            ldsc_tail.append(stripped)
            logger.info(stripped)

    rc = process.poll()
    if rc != 0:
        tail_txt = "\n".join(ldsc_tail)
        logger.error(
            f"[LDSC] command failed rc={rc}\n"
            f"[LDSC] cmd={' '.join(cmd)}\n"
            f"[LDSC] --- output (last {len(ldsc_tail)} lines) ---\n{tail_txt}"
        )
        # Keep exception message bounded for Prefect UI
        snippet = tail_txt[-3500:] if len(tail_txt) > 3500 else tail_txt
        raise RuntimeError(
            f"LDSC --h2-cts exited with code {rc}. Check logs for full output. "
            f"Last lines from ldsc.py:\n{snippet}"
        )

    if os.path.isfile(sumstats_path):
        try:
            os.remove(sumstats_path)
            logger.info(f"[LDSC] Removed intermediate: {sumstats_path}")
        except OSError as e:
            logger.warning(f"[LDSC] Could not remove {sumstats_path}: {e}")

    logger.info("LDSC Zhang --h2-cts completed successfully")
    return True


@task(log_prints=True)
def process_ldsc_results(results_dir, output_prefix, top_n=10):
    """Process LDSC results"""
    result_file = Path(results_dir) / f"{output_prefix}.cell_type_results.txt"
    
    if not result_file.exists():
        raise FileNotFoundError(f"LDSC results file not found: {result_file}")
    
    logger.info(f"Reading LDSC results from: {result_file}")
    df = pd.read_csv(result_file, sep="\t")
    p_col = "Coefficient_P_value" if "Coefficient_P_value" in df.columns else df.columns[3]
    coef_col = "Coefficient" if "Coefficient" in df.columns else df.columns[1]

    try:
        _, df["FDR"] = fdrcorrection(df[p_col].fillna(1).astype(float).values)
    except Exception as e:
        logger.warning(f"[LDSC] statsmodels FDR failed ({e}), skipping FDR column")
        df["FDR"] = np.nan

    ranked_path = Path(results_dir) / f"{output_prefix}_ranked.csv"
    df.sort_values(p_col).to_csv(ranked_path, index=False)
    logger.info(f"[LDSC] Ranked results saved to {ranked_path}")

    df_filtered = df[df[p_col] < 0.01]
    df_sorted = df_filtered.sort_values(by=coef_col, ascending=False)
    top_tissues_df = df_sorted.head(top_n)

    top_tissues = [
        re.sub(r"_\(", "_", name).replace(")", "") 
        for name in top_tissues_df['Name'].tolist()
    ]

    logger.info(
        f"Processed {len(df)} cell types, {len(df_filtered)} p<0.01, returning top {len(top_tissues)}"
    )
    return top_tissues, top_tissues_df.to_dict("records")


@task(log_prints=True)
def map_tissues_to_cellxgene(top_tissues):
    """Map LDSC cell-type names to Catlas / Census metadata for storage and queries."""
    config = Config.from_env()
    results = {}
    for ldsc_name in top_tissues:
        try:
            resolved = resolve_ldsc_for_census(
                ldsc_name,
                repo_root=config.repo_root,
                mapping_json_rel=config.catlas_celltype_cl_mapping_json,
                catlas_aliases_rel=config.catlas_abc_aliases_tsv,
            )
        except CatlasMappingError as e:
            # Defensive backstop: resolve() itself no longer raises for
            # match_method=="no_match", but a genuinely-missing mapping entry
            # (no key in the mapping JSON at all) still does — don't let one
            # bad/missing entry abort mapping for the rest of the batch.
            logger.warning(f"[Mapping] {ldsc_name!r} → skip (unresolvable: {e})")
            resolved = ResolvedCensusCellFilter(
                ldsc_name=ldsc_name, skip_coexpression=True, skip_reason=str(e),
            )
        results[ldsc_name] = {
            "cell_type": ldsc_name,
            "census_mapping_source": resolved.source,
            "census_cell_type_labels": resolved.cell_type_labels,
            "census_cl_ids": resolved.cl_ids,
            "census_skip_coexpression": resolved.skip_coexpression,
            "census_skip_reason": resolved.skip_reason,
        }
        if resolved.skip_coexpression:
            logger.info(f"[Mapping] {ldsc_name!r} → skip ({resolved.skip_reason})")
        else:
            logger.info(
                f"[Mapping] {ldsc_name!r} → census_labels={resolved.cell_type_labels} "
                f"cl_ids={resolved.cl_ids} ({resolved.source})"
            )
    return results


def _tissue_cache_paths(cell_type: str, config) -> dict:
    """Where the (cells x genes) raw-count matrix for a tissue is cached on
    disk, keyed by cell type + Census version."""
    cache_dir = Path(getattr(config, "census_cache_dir", None) or os.path.join(config.data_dir, "census_cache"))
    cache_dir.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^A-Za-z0-9_.-]", "_", cell_type)
    key = f"{safe_name}__{CENSUS_VERSION}"
    return {
        "matrix": cache_dir / f"{key}.matrix.npz",
        "meta": cache_dir / f"{key}.meta.json",
        "obs": cache_dir / f"{key}.obs_joinids.npy",
        "cellsums": cache_dir / f"{key}.cell_sums.npy",
    }


def _load_tissue_context_from_disk(cell_type: str, config) -> Optional[_TissueCoexpressionContext]:
    paths = _tissue_cache_paths(cell_type, config)
    if not all(p.exists() for p in paths.values()):
        return None
    try:
        meta = json.loads(paths["meta"].read_text())
        obs_joinids = np.load(paths["obs"])
        cell_sums = np.load(paths["cellsums"])
        matrix = sparse.load_npz(paths["matrix"])
        genes = meta["genes"]
        return _TissueCoexpressionContext(
            cell_type=cell_type,
            obs_joinids=obs_joinids,
            cell_sums=cell_sums,
            genes=genes,
            gene_index={g: i for i, g in enumerate(genes)},
            all_genes_list=meta["all_genes_list"],
            matrix=matrix,
        )
    except Exception as e:
        logger.warning(
            f"[Census cache] Failed to load cached matrix for {cell_type!r}: {e}. "
            "Will re-download from Census."
        )
        return None


def _save_tissue_context_to_disk(context: _TissueCoexpressionContext, config) -> None:
    paths = _tissue_cache_paths(context.cell_type, config)
    try:
        sparse.save_npz(paths["matrix"], context.matrix)
        np.save(paths["obs"], context.obs_joinids)
        np.save(paths["cellsums"], context.cell_sums)
        paths["meta"].write_text(json.dumps({
            "genes": context.genes,
            "all_genes_list": context.all_genes_list,
        }))
        logger.info(f"[Census cache] Cached coexpression matrix for {context.cell_type!r} at {paths['matrix']}")
    except Exception as e:
        logger.warning(f"[Census cache] Failed to persist cache for {context.cell_type!r}: {e}")


def _build_tissue_context_from_census(
    cell_type: str, resolved: ResolvedCensusCellFilter
) -> Optional[_TissueCoexpressionContext]:
    """The expensive part: open Census, select cells for the tissue, filter
    to the highly-expressed genes, and download the raw-count matrix for
    every (cell, gene) pair once. This is the "co-expression matrix
    downloading step" — after this runs once per tissue, every subsequent
    gene queried against the same tissue reuses the cached result instead of
    re-downloading it from Census.
    """
    with cellxgene_census.open_soma(census_version=CENSUS_VERSION) as census:
        experiment = census["census_data"]["homo_sapiens"]

        axis_query = _census_obs_axis_query_for_resolved(experiment, resolved)
        if axis_query is None:
            return None

        obs_joinids = axis_query.obs_joinids().to_numpy()
        logger.info(f"Found {len(obs_joinids)} cells for ldsc cell type '{cell_type}'")

        if len(obs_joinids) > 100000:
            obs_joinids = obs_joinids[:100000]
        n = len(obs_joinids)

        if n == 0:
            logger.warning(f"No cells in join set for ldsc cell type '{cell_type}'")
            return None

        logger.info("Getting library sizes from obs metadata...")
        obs_df = experiment.obs.read(
            coords=(obs_joinids.tolist(),),
            column_names=["soma_joinid", "n_measured_vars"],
        ).concat().to_pandas().set_index("soma_joinid")
        cell_sums = obs_df.loc[obs_joinids, "n_measured_vars"].values.astype(np.float32)
        cell_sums = np.where(cell_sums > 0, cell_sums, 1.0)
        logger.info(f"Loaded library sizes for {n} cells (mean: {np.mean(cell_sums):.0f} counts/cell)")

        logger.info("Loading gene metadata and filtering to highly expressed genes...")
        var_df = experiment.ms["RNA"].var.read(
            column_names=["soma_joinid", "feature_id", "feature_name", "n_measured_obs"]
        ).concat().to_pandas()

        min_cells = max(10, int(n * 0.01))
        var_df_filtered = var_df[var_df["n_measured_obs"] >= min_cells].copy()
        var_df_filtered = var_df_filtered.nlargest(15000, "n_measured_obs").set_index("feature_id")

        genes_filtered = var_df_filtered.index.tolist()
        all_genes_list = var_df.set_index("feature_id").index.tolist()
        gene_joinids = var_df_filtered["soma_joinid"].values
        logger.info(f"Filtered from {len(all_genes_list)} to {len(genes_filtered)} highly expressed genes")

        # The actual matrix download: raw counts for every selected cell x
        # every filtered gene, in one shot — this is what used to be
        # re-fetched from Census on every single gene call.
        logger.info(
            f"[Census] Downloading raw expression matrix for {n} cells x "
            f"{len(genes_filtered)} genes (tissue={cell_type!r})..."
        )
        obs_idx_map = {int(j): i for i, j in enumerate(obs_joinids)}
        gene_idx_map = {int(j): i for i, j in enumerate(gene_joinids)}
        matrix = sparse.lil_matrix((n, len(genes_filtered)), dtype=np.float32)

        table_iter = experiment.ms["RNA"].X["raw"].read(
            (obs_joinids.tolist(), gene_joinids.tolist())
        ).tables()
        for table in table_iter:
            obs_jids = table["soma_dim_0"].to_numpy()
            gene_jids = table["soma_dim_1"].to_numpy()
            values = table["soma_data"].to_numpy()

            rows = np.fromiter((obs_idx_map.get(int(j), -1) for j in obs_jids), dtype=np.int64, count=len(obs_jids))
            cols = np.fromiter((gene_idx_map.get(int(j), -1) for j in gene_jids), dtype=np.int64, count=len(gene_jids))
            valid = (rows >= 0) & (cols >= 0)
            if np.any(valid):
                matrix[rows[valid], cols[valid]] = values[valid]

        matrix = matrix.tocsr()
        logger.info(
            f"[Census] Matrix download complete: {matrix.shape[0]} cells x "
            f"{matrix.shape[1]} genes, {matrix.nnz} non-zero entries"
        )

        return _TissueCoexpressionContext(
            cell_type=cell_type,
            obs_joinids=obs_joinids,
            cell_sums=cell_sums,
            genes=genes_filtered,
            gene_index={g: i for i, g in enumerate(genes_filtered)},
            all_genes_list=all_genes_list,
            matrix=matrix,
        )


def _fetch_single_gene_counts(context: _TissueCoexpressionContext, gene: str):
    """Rare path: the gene of interest wasn't among the cached top-expressed
    genes for this tissue. Fetch just its counts column directly instead of
    re-downloading (or invalidating) the shared cached matrix."""
    with cellxgene_census.open_soma(census_version=CENSUS_VERSION) as census:
        experiment = census["census_data"]["homo_sapiens"]
        esc = _escape_soma_string_literal(gene)
        row = experiment.ms["RNA"].var.read(
            column_names=["soma_joinid", "feature_id"],
            value_filter=f"feature_id == '{esc}'",
        ).concat().to_pandas()
        if row.empty:
            return None
        gene_joinid = int(row.iloc[0]["soma_joinid"])

        counts = np.zeros(len(context.obs_joinids), dtype=np.float32)
        obs_idx_map = {int(j): i for i, j in enumerate(context.obs_joinids)}
        table_iter = experiment.ms["RNA"].X["raw"].read(
            (context.obs_joinids.tolist(), [gene_joinid])
        ).tables()
        for table in table_iter:
            obs_jids = table["soma_dim_0"].to_numpy()
            values = table["soma_data"].to_numpy()
            for obs_jid, value in zip(obs_jids, values):
                idx = obs_idx_map.get(int(obs_jid))
                if idx is not None:
                    counts[idx] = value
        return counts


def _get_or_build_tissue_context(cell_type: str, config) -> Optional[_TissueCoexpressionContext]:
    """Return the cached coexpression context for a tissue, building and
    persisting it on first use. Checks the in-process cache, then the disk
    cache, and only falls through to a real Census download as a last
    resort."""
    with _TISSUE_CONTEXT_LOCK:
        if cell_type in _TISSUE_CONTEXT_CACHE:
            return _TISSUE_CONTEXT_CACHE[cell_type]

    disk_context = _load_tissue_context_from_disk(cell_type, config)
    if disk_context is not None:
        logger.info(
            f"[Census cache] Using cached coexpression matrix for {cell_type!r} "
            "— skipping Census download entirely"
        )
        with _TISSUE_CONTEXT_LOCK:
            _TISSUE_CONTEXT_CACHE[cell_type] = disk_context
        return disk_context

    resolved = resolve_ldsc_for_census(
        cell_type,
        repo_root=config.repo_root,
        mapping_json_rel=config.catlas_celltype_cl_mapping_json,
        catlas_aliases_rel=config.catlas_abc_aliases_tsv,
    )
    logger.info(
        f"[Census cache] No cache for {cell_type!r} (source={resolved.source}, "
        f"skip={resolved.skip_coexpression}) — checking whether a download is needed"
    )
    if resolved.skip_coexpression:
        with _TISSUE_CONTEXT_LOCK:
            _TISSUE_CONTEXT_CACHE[cell_type] = None
        return None

    context = _build_tissue_context_from_census(cell_type, resolved)
    with _TISSUE_CONTEXT_LOCK:
        _TISSUE_CONTEXT_CACHE[cell_type] = context
    if context is not None:
        _save_tissue_context_to_disk(context, config)
    return context


@task(log_prints=True)
def get_coexpression_matrix_for_tissue(gene, cell_type, k=500, batch_size=1000):
    """Query CellxGene census for co-expressed genes in the given cell type.

    ``cell_type`` is the LDSC / CTS name (e.g. ``Atrial_Cardiomyocyte``). Catlas TSVs
    resolve it to Census ``cell_type`` strings and/or CL ids.
    """
    config = Config.from_env()
    resolved = resolve_ldsc_for_census(
        cell_type,
        repo_root=config.repo_root,
        mapping_json_rel=config.catlas_celltype_cl_mapping_json,
        catlas_aliases_rel=config.catlas_abc_aliases_tsv,
    )
    log_label = cell_type.replace("_", " ").lower()
    logger.info(
        f"Starting coexpression for gene '{gene}' | ldsc={cell_type!r} "
        f"(passthrough_label={log_label!r}) source={resolved.source} "
        f"skip={resolved.skip_coexpression}"
    )

    with cellxgene_census.open_soma(census_version="2024-07-01") as census:
        experiment = census["census_data"]["homo_sapiens"]

        axis_query = _census_obs_axis_query_for_resolved(experiment, resolved)
        if axis_query is None:
            return [], [], []

        obs_joinids = axis_query.obs_joinids().to_numpy()
        logger.info(f"Found {len(obs_joinids)} cells for ldsc cell type '{cell_type}'")

        if len(obs_joinids) > 100000:
            obs_joinids = obs_joinids[:100000] 
            n = 100000
        else:
            n = len(obs_joinids)
            
        if n == 0:
            logger.warning(f"No cells in join set for ldsc cell type '{cell_type}'")
            return [], [], []

        # Get library sizes from obs metadata
        logger.info("Getting library sizes from obs metadata...")
        obs_df = experiment.obs.read(
            coords=(obs_joinids.tolist(),),
            column_names=["soma_joinid", "n_measured_vars"]
        ).concat().to_pandas()
        
        # Create mapping from joinid to library size
        obs_df = obs_df.set_index("soma_joinid")
        cell_sums = obs_df.loc[obs_joinids, "n_measured_vars"].values.astype(np.float32)
        
        # Avoid division by zero
        cell_sums = np.where(cell_sums > 0, cell_sums, 1.0)
        logger.info(f"Loaded library sizes for {n} cells (mean: {np.mean(cell_sums):.0f} counts/cell)")

        # Pre-filter to highly variable genes
        logger.info("Loading gene metadata and filtering to highly expressed genes...")
        var_df = experiment.ms["RNA"].var.read(
            column_names=["soma_joinid", "feature_id", "feature_name", "n_measured_obs"]
        ).concat().to_pandas()
        
        # Filter genes: expressed in at least 1% of cells (1000 cells for 100k sample)
        min_cells = max(10, int(n * 0.01))
        var_df_filtered = var_df[var_df["n_measured_obs"] >= min_cells].copy()
        
        # Sort by number of cells expressing (keep top ~15k genes)
        var_df_filtered = var_df_filtered.nlargest(15000, "n_measured_obs")
        var_df_filtered = var_df_filtered.set_index("feature_id")
        
        genes_filtered = var_df_filtered.index.tolist()
        all_genes_list = var_df.set_index("feature_id").index.tolist()
        
        logger.info(f"Filtered from {len(all_genes_list)} to {len(genes_filtered)} highly expressed genes")
        
        # CellxGene uses uppercase Ensembl IDs - convert input to uppercase
        gene = gene.upper()
        
        if gene not in all_genes_list:
            logger.warning(f"Gene of interest '{gene}' not found in dataset")
            return [], [], all_genes_list
        
        # Make sure gene of interest is in filtered set
        if gene not in genes_filtered:
            logger.info(f"Gene of interest not in filtered set, adding it")
            genes_filtered.append(gene)
            # var_df uses a SOMA row index, not feature_id — select by column
            gene_info = var_df[var_df["feature_id"] == gene]
            if gene_info.empty:
                logger.warning(
                    f"Gene '{gene}' was in feature list but row metadata missing; skipping coexpression"
                )
                return [], [], all_genes_list
            var_df_filtered = pd.concat([var_df_filtered, gene_info.set_index("feature_id")])
        
        logger.info(f"Found gene '{gene}' in dataset")
        
        # Use filtered genes for correlation analysis
        var_df = var_df_filtered

        gene_joinid = var_df.loc[gene]["soma_joinid"]

        # Get gene expression from raw counts  
        gene_table_iter = experiment.ms["RNA"].X["raw"].read((obs_joinids.tolist(), [gene_joinid])).tables()
        gene_expr = np.zeros(n, dtype=np.float32)
        gene_joinid_to_idx = {jid: idx for idx, jid in enumerate(obs_joinids)}
        
        for batch in gene_table_iter:
            obs_jids = batch["soma_dim_0"].to_numpy()
            values = batch["soma_data"].to_numpy()
            
            for obs_jid, value in zip(obs_jids, values):
                if obs_jid in gene_joinid_to_idx:
                    idx = gene_joinid_to_idx[obs_jid]
                    gene_expr[idx] = value
        
        # Normalize by library size (CPM-like: counts per 10k) then log1p
        gene_expr = np.log1p((gene_expr / cell_sums) * 1e4)

        # Filter cells with non-zero expression
        nonzero_mask = gene_expr > 0
        if np.sum(nonzero_mask) < 10:
            logger.warning(
                f"Too few cells with non-zero expression for gene '{gene}' in "
                f"ldsc cell type '{cell_type}'"
            )
            return [], [], all_genes_list

        sub_joinids = obs_joinids[nonzero_mask]
        gene_expr_sub = gene_expr[nonzero_mask]
        cell_sums_sub = cell_sums[nonzero_mask]  # Subset library sizes too
        
        n_sub = len(sub_joinids)
        logger.info(f"Gene expressed in {n_sub} cells, computing correlations...")

        # Get other genes (exclude gene of interest)
        genes = var_df.index.tolist()
        mask = np.array([g != gene for g in genes])
        other_gene_joinids = var_df.loc[mask, "soma_joinid"].values
        other_genes = np.array(genes)[mask]

        # Pre-compute cell index mapping
        sub_joinid_to_idx = {jid: idx for idx, jid in enumerate(sub_joinids)}

        # Vectorized correlation computation
        logger.info(f"Reading expression for {len(other_genes)} genes...")
        all_table_iter = experiment.ms["RNA"].X["raw"].read(
            (sub_joinids.tolist(), other_gene_joinids.tolist())
        ).tables()
        
        # Build full expression matrix
        all_expr = np.zeros((n_sub, len(other_genes)), dtype=np.float32)
        gene_jid_to_idx = {jid: idx for idx, jid in enumerate(other_gene_joinids)}
        
        for table in all_table_iter:
            obs_jids = table["soma_dim_0"].to_numpy()
            gene_jids = table["soma_dim_1"].to_numpy()
            values = table["soma_data"].to_numpy()
            
            for obs_jid, gene_jid, value in zip(obs_jids, gene_jids, values):
                if obs_jid in sub_joinid_to_idx and gene_jid in gene_jid_to_idx:
                    obs_idx = sub_joinid_to_idx[obs_jid]
                    gene_idx = gene_jid_to_idx[gene_jid]
                    all_expr[obs_idx, gene_idx] = value
        
        logger.info("Normalizing expression matrix...")
        all_expr = (all_expr / cell_sums_sub[:, np.newaxis]) * 1e4
        all_expr = np.log1p(all_expr)
        
        logger.info("Computing correlations (vectorized pre-filtering)...")
        
        # Filter genes with sufficient expression (at least 10 cells)
        gene_counts = np.sum(all_expr > 0, axis=0)
        valid_genes = gene_counts >= 10
        
        if np.sum(valid_genes) == 0:
            logger.warning("No genes with sufficient expression for correlation")
            return [], [], all_genes_list
        
        all_expr_filtered = all_expr[:, valid_genes]
        other_genes_filtered = other_genes[valid_genes]
        
        # Standardize for correlation
        gene_expr_centered = gene_expr_sub - np.mean(gene_expr_sub)
        other_expr_centered = all_expr_filtered - np.mean(all_expr_filtered, axis=0)
        
        gene_std = np.std(gene_expr_sub)
        other_std = np.std(all_expr_filtered, axis=0)
        
        # Avoid division by zero
        valid_std = (gene_std > 1e-10) & (other_std > 1e-10)
        
        correlations_vec = np.zeros(len(other_genes_filtered))
        if gene_std > 1e-10:
            correlations_vec[valid_std] = np.dot(gene_expr_centered, other_expr_centered[:, valid_std]) / (
                n_sub * gene_std * other_std[valid_std]
            )
        
        # Get top candidates (top 2000 by absolute correlation for efficiency)
        top_n = min(2000, len(correlations_vec))
        top_indices = np.argsort(np.abs(correlations_vec))[-top_n:]
        
        logger.info(f"Pre-filtered to top {len(top_indices)} candidates, running scipy.stats.pearsonr...")
        
        correlations = {}
        for idx in top_indices:
            gene_symbol = other_genes_filtered[idx]
            other_expr = all_expr_filtered[:, idx]
            
            if np.sum(other_expr > 0) >= 10:
                try:
                    corr, p_value = pearsonr(gene_expr_sub, other_expr)
                    if p_value <= 0.05 and not np.isnan(corr):
                        correlations[gene_symbol] = corr
                except Exception:
                    continue
        
        logger.info(f"Found {len(correlations)} significant correlations (p <= 0.05)")

        # Sort by correlation and get top k
        sorted_correlations = sorted(correlations.items(), key=lambda x: x[1], reverse=True)
        
        top_positive = sorted_correlations[:k]
        top_negative = sorted_correlations[-k:]
        
        return top_positive, top_negative, all_genes_list

@task(log_prints=True)
def run_combined_ldsc_tissue_analysis(munged_file, output_dir, project_id, user_id):
    """Combined LDSC + tissue analysis as part of main analysis pipeline"""
    analysis_run_id = None

    deps = get_deps()
    gene_expression = deps["gene_expression"]

    try:
        logger.info("[PIPELINE] Starting combined LDSC + tissue analysis")
        
        # Create work directory for LDSC
        ldsc_work_dir = f"{output_dir}/ldsc_analysis"
        os.makedirs(ldsc_work_dir, exist_ok=True)
        
        # Create analysis run record
        analysis_run_id = gene_expression.create_gene_expression_run(
            gwas_file=munged_file,
            gene_of_interest="project_analysis",  # Indicates project-level analysis
            project_id=project_id,
            user_id=user_id
        )
        
        # Update status to running
        gene_expression.update_gene_expression_run_status(analysis_run_id, 'running')
        
        # Step 1: Setup LDSC environment
        logger.info("[PIPELINE] Setting up LDSC environment...")
        ldsc_dir = setup_ldsc_environment(ldsc_work_dir)
        
        # Step 2: Run LDSC analysis
        logger.info("[PIPELINE] Running LDSC heritability analysis...")
        output_prefix = f"{output_dir}/ldsc_project_analysis"
        ldsc_success = run_ldsc_analysis(ldsc_dir, munged_file, output_prefix)
        
        if not ldsc_success:
            raise RuntimeError("LDSC analysis failed")
        
        # Step 3: Process LDSC results
        logger.info("[PIPELINE] Processing LDSC results...")
        top_tissues, ldsc_results_data = process_ldsc_results(
            output_dir, "ldsc_project_analysis", 10
        )

        storage = deps.get("storage")
        if storage and user_id and project_id:
            for suffix in (".cell_type_results.txt", "_ranked.csv"):
                path = f"{output_prefix}{suffix}"
                if os.path.isfile(path):
                    key = f"ldsc/{user_id}/{project_id}/{os.path.basename(path)}"
                    if storage.upload_file(path, key):
                        logger.info(f"[LDSC] Uploaded to MinIO: {key}")
        
        # Step 4: Tissue mapping analysis
        logger.info("[PIPELINE] Running tissue mapping analysis...")

        # Map cell-type labels for CellxGene queries
        ontology_mapping_results = map_tissues_to_cellxgene(top_tissues)

        # Exclude tissues with no Census mapping from what gets saved as LDSC
        # results — the tissue picker (get_ldsc_results_for_project,
        # format='selection') sources its list solely from ldsc_results_collection,
        # so unmapped tissues must not end up there even though LDSC computed
        # valid numbers for them.
        mapped_ldsc_results_data = [
            r for r in ldsc_results_data
            if not ontology_mapping_results.get(r.get('Name', ''), {}).get('census_skip_coexpression')
        ]
        skipped_n = len(ldsc_results_data) - len(mapped_ldsc_results_data)
        if skipped_n:
            logger.info(f"[PIPELINE] Excluding {skipped_n} unmapped tissue(s) from saved LDSC results")

        # Step 5: Save comprehensive results to database
        gene_expression.save_ldsc_results(analysis_run_id, mapped_ldsc_results_data)

        # Save tissue mappings (full set, including skipped, for debugging visibility)
        gene_expression.save_tissue_mappings(analysis_run_id, ontology_mapping_results)

        # Update status to completed
        gene_expression.update_gene_expression_run_status(analysis_run_id, 'ldsc_tissue_completed')

        significant_count = len([t for t in mapped_ldsc_results_data if t.get('Coefficient_P_value', 1) < 0.05])

        logger.info(f"[PIPELINE] LDSC + tissue analysis completed successfully!, Analyzed {len(mapped_ldsc_results_data)} tissues, found {significant_count} significant")

        return {
            "success": True,
            "analysis_run_id": analysis_run_id,
            "tissues_analyzed": len(mapped_ldsc_results_data),
            "significant_tissues": significant_count,
            "top_tissues": mapped_ldsc_results_data[:10]
        }
        
    except CatlasMappingError as e:
        logger.error(
            f"[PIPELINE] LDSC tissue mapping failed for {e.ldsc_name!r}: {e}"
        )
        if analysis_run_id:
            gene_expression.update_gene_expression_run_status(analysis_run_id, 'failed')
        raise
    except Exception as e:
        logger.error(f"[PIPELINE] Combined LDSC + tissue analysis failed: {str(e)}")
        if analysis_run_id:
            gene_expression.update_gene_expression_run_status(analysis_run_id, 'failed')
        raise