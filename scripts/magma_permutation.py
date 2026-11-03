#!/usr/bin/env python3
"""
Phenotype-permutation null for MAGMA gene and gene-set analysis.

Follows Moskvina et al. 2009 (Mol Psychiatry 14:252-260), which used 1000
genome-wide permutations to obtain (a) empirical per-gene p-values and (b) the
null distribution of the NUMBER of genes surpassing nominal thresholds.

NOTE ON SCOPE: Moskvina et al. performed gene-wide analysis only. They did not
run a gene-set analysis; they mention only in passing that the approach is
applicable to "groups of genes with related functions". The gene-set portion
below is therefore an extension, not a reproduction of their method.

WHAT IS PERMUTED
    Only the phenotype VALUES are shuffled across lines. Genotypes, the gene
    annotation and the gene-set definitions are held fixed. This breaks the
    genotype-phenotype relationship while preserving LD, gene size, SNP density
    and the overlap structure between gene sets -- all of which are exactly the
    nuisance features an analytic null gets wrong.

WHAT IS REPRODUCED
    The permutation reruns the SAME MAGMA commands as the observed analysis
    (raw-genotype mode, same annotation file, same flags). If the observed run
    used covariates, they must be used here too, or the null will not match.
    The observed pipeline in this project passes only `--covar chrX-use-sex=0`,
    which is a chrX handling modifier and NOT a covariate file, so no covariates
    are included below. Change COVAR_ARGS if that is not what you intend.

OUTPUTS
    1. results/magma_perm_counts.tsv
         per-permutation counts of genes / gene sets below each threshold,
         plus the minimum p-value seen in that permutation
    2. results/magma_perm_gene_empirical_p.tsv
         per-gene empirical p-value (Moskvina's method 1)
    3. printed summary
         observed vs null mean/SD/max, empirical p, normal-approximation p,
         and the empirical genome-wide significance threshold

USAGE
    Set N_PERM = 1 and time it before scaling up. Then run under tmux/nohup.
"""

import shutil
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import norm

# ----------------------------------------------------------------------
# Configuration -- paths match the project layout
# ----------------------------------------------------------------------
BASE_DIR = Path("/mnt/hdd_1/rediet/fly-ldsc")
MAGMA_BIN = BASE_DIR / "tools" / "magma" / "magma"
MAGMA_DIR = BASE_DIR / "data" / "magma"

MAGMA_BFILE = MAGMA_DIR / "merged_qc"
PHENO_FILE = BASE_DIR / "data" / "gwas" / "lifespan_female.pheno"
PHENO_NAME = "S18_1537_F"

# Use the SAME annotation / gene-set files as the observed run you are testing.
# Switch to the non-windowed versions if that is the analysis you want a null for.
ANNOT_FILE = MAGMA_DIR / "dgrp_lifespan_female_w5.genes.annot"
GENE_SETS_FILE = MAGMA_DIR / "gene_sets.txt"

# Observed results, for comparison
OBS_GENES_OUT = MAGMA_DIR / "dgrp_lifespan_female_gene_w5.genes.out"
OBS_GSA_OUT = MAGMA_DIR / "dgrp_lifespan_female_geneset_w5.gsa.out"

# Flags from the observed run. chrX-use-sex=0 is a modifier, not a covar file.
COVAR_ARGS = ["--covar", "chrX-use-sex=0"]

PERM_DIR = MAGMA_DIR / "permutations"
RESULTS_DIR = BASE_DIR / "results"
COUNTS_FILE = RESULTS_DIR / "magma_perm_counts.tsv"
GENE_EMP_FILE = RESULTS_DIR / "magma_perm_gene_empirical_p.tsv"

N_PERM = 100
SEED = 42
THRESHOLDS = [0.05, 0.01, 0.001]
KEEP_INTERMEDIATE = False
COMPUTE_PER_GENE_EMPIRICAL = True   # Moskvina method 1; adds no real cost


# ----------------------------------------------------------------------
def read_magma_table(path, colname="P"):
    """Read a MAGMA .genes.out or .gsa.out, skipping '#' comment lines."""
    df = pd.read_csv(path, sep=r"\s+", comment="#")
    if colname not in df.columns:
        raise RuntimeError(f"{path.name}: no '{colname}' column. Got {list(df.columns)}")
    return df


def write_permuted_pheno(pheno, rng, out_path):
    """Shuffle phenotype VALUES only; FID/IID order and genotypes stay fixed."""
    perm = pheno.copy()
    perm[PHENO_NAME] = rng.permutation(perm[PHENO_NAME].to_numpy())
    perm.to_csv(out_path, sep="\t", index=False)
    return out_path


def run_magma_gene(pheno_path, out_prefix):
    cmd = [
        str(MAGMA_BIN),
        "--bfile", str(MAGMA_BFILE),
        "--pheno", f"file={pheno_path}", f"use={PHENO_NAME}",
        *COVAR_ARGS,
        "--gene-annot", str(ANNOT_FILE),
        "--out", str(out_prefix),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    genes_out = Path(str(out_prefix) + ".genes.out")
    if not genes_out.exists():
        raise RuntimeError(f"MAGMA gene step failed:\n{r.stdout[-800:]}\n{r.stderr[-800:]}")
    return genes_out, Path(str(out_prefix) + ".genes.raw")


def run_magma_geneset(genes_raw, out_prefix):
    cmd = [
        str(MAGMA_BIN),
        "--gene-results", str(genes_raw),
        "--set-annot", str(GENE_SETS_FILE),
        "--out", str(out_prefix),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    gsa_out = Path(str(out_prefix) + ".gsa.out")
    if not gsa_out.exists():
        raise RuntimeError(f"MAGMA gene-set step failed:\n{r.stdout[-800:]}\n{r.stderr[-800:]}")
    return gsa_out


def summarise(label, observed, null_counts, n_done):
    """Empirical and normal-approximation p-values, as in Moskvina Tables 2-3."""
    print(f"\n{label}")
    print(f"{'thresh':>8} {'obs':>7} {'null mean':>10} {'null sd':>9} "
          f"{'null max':>9} {'ratio':>7} {'emp p':>10} {'calc p':>11}")
    rows = []
    for t in THRESHOLDS:
        col = np.asarray(null_counts[t], dtype=float)
        obs = observed[t]
        mean, sd = col.mean(), col.std(ddof=1) if len(col) > 1 else 0.0
        n_ge = int((col >= obs).sum())
        emp_p = max(n_ge / n_done, 1.0 / n_done)
        emp_str = f"{emp_p:.4g}" if n_ge > 0 else f"<{1/n_done:.4g}"
        calc_p = norm.sf((obs - mean) / sd) if sd > 0 else float("nan")
        ratio = obs / mean if mean > 0 else float("nan")
        print(f"{t:>8} {obs:>7} {mean:>10.2f} {sd:>9.2f} {int(col.max()):>9} "
              f"{ratio:>7.2f} {emp_str:>10} {calc_p:>11.3g}")
        rows.append({"threshold": t, "observed": obs, "null_mean": mean,
                     "null_sd": sd, "null_max": int(col.max()), "ratio": ratio,
                     "empirical_p": emp_p, "normal_approx_p": calc_p})
    return rows


# ----------------------------------------------------------------------
def main():
    for p in [MAGMA_BIN, ANNOT_FILE, GENE_SETS_FILE, PHENO_FILE, OBS_GENES_OUT]:
        if not Path(p).exists():
            sys.exit(f"Missing required file: {p}")

    PERM_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # ---- observed results ------------------------------------------------
    obs_genes = read_magma_table(OBS_GENES_OUT)
    obs_gene_counts = {t: int((obs_genes["P"] < t).sum()) for t in THRESHOLDS}
    obs_gene_min_p = float(obs_genes["P"].min())
    n_genes = len(obs_genes)

    print(f"Observed genes tested: {n_genes}")
    print(f"Observed gene counts:  " +
          "  ".join(f"P<{t}: {obs_gene_counts[t]}" for t in THRESHOLDS))
    print(f"Observed min gene P:   {obs_gene_min_p:.3g}")

    have_sets = OBS_GSA_OUT.exists()
    if have_sets:
        obs_sets = read_magma_table(OBS_GSA_OUT)
        obs_set_counts = {t: int((obs_sets["P"] < t).sum()) for t in THRESHOLDS}
        n_sets = len(obs_sets)
        print(f"\nObserved gene sets tested: {n_sets}")
        print(f"Observed set counts:  " +
              "  ".join(f"P<{t}: {obs_set_counts[t]}" for t in THRESHOLDS))
    else:
        print(f"\nNo observed .gsa.out at {OBS_GSA_OUT}; skipping gene-set null.")
        obs_set_counts, n_sets = None, 0

    # gene order is fixed across runs; used for per-gene empirical p-values
    gene_index = {g: i for i, g in enumerate(obs_genes["GENE"].to_numpy())}
    obs_p_vec = obs_genes["P"].to_numpy()
    ge_counter = np.zeros(n_genes, dtype=np.int64)
    n_perm_for_gene = 0

    pheno = pd.read_csv(PHENO_FILE, sep=r"\s+")
    if PHENO_NAME not in pheno.columns:
        sys.exit(f"{PHENO_NAME} not in {PHENO_FILE}; got {list(pheno.columns)}")
    print(f"\nPhenotype lines: {len(pheno)}")

    rng = np.random.default_rng(SEED)
    null_gene = {t: [] for t in THRESHOLDS}
    null_set = {t: [] for t in THRESHOLDS}
    min_ps, rows = [], []
    t_start = time.time()

    # ---- permutation loop ------------------------------------------------
    for i in range(1, N_PERM + 1):
        work = PERM_DIR / f"perm_{i:04d}"
        work.mkdir(parents=True, exist_ok=True)
        t0 = time.time()

        try:
            pheno_path = write_permuted_pheno(pheno, rng, work / "perm.pheno")
            genes_out, genes_raw = run_magma_gene(pheno_path, work / "perm_gene")

            pg = read_magma_table(genes_out)
            gcounts = {t: int((pg["P"] < t).sum()) for t in THRESHOLDS}
            for t in THRESHOLDS:
                null_gene[t].append(gcounts[t])
            min_p = float(pg["P"].min())
            min_ps.append(min_p)

            if COMPUTE_PER_GENE_EMPIRICAL:
                idx = np.array([gene_index.get(g, -1) for g in pg["GENE"].to_numpy()])
                keep = idx >= 0
                ge_counter[idx[keep]] += (pg["P"].to_numpy()[keep] <= obs_p_vec[idx[keep]])
                n_perm_for_gene += 1

            row = {"perm": i, "min_gene_p": min_p,
                   **{f"genes_p{t}": gcounts[t] for t in THRESHOLDS}}

            if have_sets:
                gsa_out = run_magma_geneset(genes_raw, work / "perm_set")
                ps = read_magma_table(gsa_out)
                scounts = {t: int((ps["P"] < t).sum()) for t in THRESHOLDS}
                for t in THRESHOLDS:
                    null_set[t].append(scounts[t])
                row.update({f"sets_p{t}": scounts[t] for t in THRESHOLDS})

            rows.append(row)
            pd.DataFrame(rows).to_csv(COUNTS_FILE, sep="\t", index=False)

            msg = "  ".join(f"g_p{t}: {gcounts[t]}" for t in THRESHOLDS)
            if have_sets:
                msg += "   " + "  ".join(f"s_p{t}: {scounts[t]}" for t in THRESHOLDS)
            print(f"[{i}/{N_PERM}] {msg}   ({(time.time()-t0)/60:.1f} min)", flush=True)

        except Exception as e:
            print(f"[{i}/{N_PERM}] FAILED: {e}", flush=True)
        finally:
            if not KEEP_INTERMEDIATE:
                shutil.rmtree(work, ignore_errors=True)

    n_done = len(rows)
    if n_done == 0:
        sys.exit("No permutations completed.")

    # ---- summary ---------------------------------------------------------
    print(f"\n{'='*78}")
    print(f"{n_done} permutations in {(time.time()-t_start)/60:.1f} min")
    print(f"{'='*78}")

    gene_rows = summarise("GENE-LEVEL excess (Moskvina Table 2 equivalent)",
                          obs_gene_counts, null_gene, n_done)
    if have_sets:
        set_rows = summarise("GENE-SET excess (extension; not in Moskvina et al.)",
                             obs_set_counts, null_set, n_done)

    # empirical genome-wide threshold from the distribution of per-run minima
    mp = np.sort(np.asarray(min_ps))
    thr05 = np.quantile(mp, 0.05)
    print(f"\nEmpirical genome-wide significance threshold")
    print(f"  5th percentile of per-permutation minimum gene P : {thr05:.3g}")
    print(f"  Bonferroni equivalent (0.05/{n_genes})            : {0.05/n_genes:.3g}")
    print(f"  observed minimum gene P                           : {obs_gene_min_p:.3g}")
    n_below = int((mp <= obs_gene_min_p).sum())
    print(f"  permutations with a minimum P <= observed         : {n_below}/{n_done} "
          f"(empirical FWER p = {max(n_below/n_done, 1/n_done):.4g})")

    if COMPUTE_PER_GENE_EMPIRICAL and n_perm_for_gene > 0:
        emp = (ge_counter + 1) / (n_perm_for_gene + 1)   # add-one, avoids p=0
        out = pd.DataFrame({
            "GENE": obs_genes["GENE"].to_numpy(),
            "P_MAGMA": obs_p_vec,
            "P_EMPIRICAL": emp,
            "N_PERM": n_perm_for_gene,
        }).sort_values("P_EMPIRICAL")
        out.to_csv(GENE_EMP_FILE, sep="\t", index=False)
        print(f"\nPer-gene empirical p-values -> {GENE_EMP_FILE}")
        print(out.head(15).to_string(index=False))
        print(f"\n(resolution limit with {n_perm_for_gene} permutations: "
              f"{1/(n_perm_for_gene+1):.3g})")

    pd.DataFrame(gene_rows).to_csv(
        RESULTS_DIR / "magma_perm_gene_summary.tsv", sep="\t", index=False)
    if have_sets:
        pd.DataFrame(set_rows).to_csv(
            RESULTS_DIR / "magma_perm_set_summary.tsv", sep="\t", index=False)

    print(f"\nPer-permutation counts -> {COUNTS_FILE}")


if __name__ == "__main__":
    main()