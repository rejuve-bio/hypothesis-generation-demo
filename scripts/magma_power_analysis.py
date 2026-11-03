#!/usr/bin/env python3

import subprocess
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd


BASE_DIR = Path("/mnt/hdd_1/rediet/fly-ldsc")

MAGMA_BIN = BASE_DIR / "tools" / "magma" / "magma"

BFILE = BASE_DIR / "data" / "gwas" / "tmp" / "merged_qc"
GENE_ANNOT = BASE_DIR / "data" / "magma" / "dgrp_lifespan_female.genes.annot"
GENE_SETS = BASE_DIR / "data" / "magma" / "gene_sets.txt"
PHENO_FILE = BASE_DIR / "data" / "gwas" / "lifespan_female.pheno"

OUTPUT_DIR = BASE_DIR / "data" / "magma" / "power"

GENE_RESULTS = OUTPUT_DIR / "gene_power_results.tsv"
GENE_SUMMARY = OUTPUT_DIR / "gene_power_summary.tsv"

GENESET_RESULTS = OUTPUT_DIR / "geneset_power_results.tsv"
GENESET_SUMMARY = OUTPUT_DIR / "geneset_power_summary.tsv"

N_SIMULATIONS = 50
RANDOM_SEED = 20260821

EFFECT_SIZES = [
    0.2,
    0.3,
    0.5,
    0.75,
    1.0,
]

GENE_ALPHA = 0.05 / 19312
GENESET_ALPHA = 0.05 / 167


def read_fam():
    fam_file = BFILE.with_suffix(".fam")

    fam = pd.read_csv(
        fam_file,
        sep=r"\s+",
        header=None,
        names=["FID", "IID", "PAT", "MAT", "SEX", "PHENO"],
        dtype={"FID": str, "IID": str},
    )

    return fam


def read_phenotype():
    pheno = pd.read_csv(
        PHENO_FILE,
        sep=r"\s+",
        engine="python",
    )

    values = pd.to_numeric(
        pheno["S18_1537_F"],
        errors="coerce",
    ).dropna()

    return values.mean(), values.std(ddof=1)


def load_gene_sets():
    gene_sets = {}

    with open(GENE_SETS) as fh:
        for line in fh:
            fields = line.rstrip("\n").split("\t")

            if len(fields) < 2:
                continue

            name = fields[0]
            genes = [x for x in fields[1:] if x]

            if genes:
                gene_sets[name] = genes

    return gene_sets


def write_pheno(path, fam, phenotype):
    out = pd.DataFrame(
        {
            "FID": fam["FID"],
            "IID": fam["IID"],
            "SIM": phenotype,
        }
    )

    out.to_csv(
        path,
        sep="\t",
        index=False,
    )


def run_magma_gene(pheno_file, output_prefix):
    subprocess.run(
        [
            str(MAGMA_BIN),
            "--bfile",
            str(BFILE),
            "--pheno",
            f"file={pheno_file}",
            "use=SIM",
            "--covar",
            "chrX-use-sex=0",
            "--gene-annot",
            str(GENE_ANNOT),
            "--out",
            str(output_prefix),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )


def run_magma_geneset(gene_raw, output_prefix):
    subprocess.run(
        [
            str(MAGMA_BIN),
            "--gene-results",
            str(gene_raw),
            "--set-annot",
            str(GENE_SETS),
            "--out",
            str(output_prefix),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )


def read_gene_results(path):
    return pd.read_csv(
        path,
        sep=r"\s+",
        comment="#",
        engine="python",
    )


def read_geneset_results(path):
    return pd.read_csv(
        path,
        sep=r"\s+",
        comment="#",
        engine="python",
    )


def build_gene_score(bim_file, target_gene):
    gene_annot = pd.read_csv(
        GENE_ANNOT,
        sep=r"\s+",
        comment="#",
        engine="python",
    )

    row = gene_annot[
        gene_annot["GENE"] == target_gene
    ]

    if row.empty:
        return None

    gene_row = row.iloc[0]

    chrom = str(gene_row["CHR"])
    start = int(gene_row["START"])
    stop = int(gene_row["STOP"])

    bim = pd.read_csv(
        bim_file,
        sep=r"\s+",
        header=None,
        names=["CHR", "SNP", "CM", "BP", "A1", "A2"],
        dtype={"CHR": str},
    )

    snps = bim[
        (bim["CHR"] == chrom)
        & (bim["BP"] >= start)
        & (bim["BP"] <= stop)
    ]["SNP"].tolist()

    return snps


def extract_genotypes_for_snps(snps, fam):
    if not snps:
        return None

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)

        snp_file = tmp / "snps.txt"
        snp_file.write_text("\n".join(snps) + "\n")

        prefix = tmp / "subset"

        subprocess.run(
            [
                "plink",
                "--bfile",
                str(BFILE),
                "--extract",
                str(snp_file),
                "--allow-extra-chr",
                "--recode",
                "A",
                "--out",
                str(prefix),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )

        raw = pd.read_csv(
            prefix.with_suffix(".raw"),
            sep=r"\s+",
            engine="python",
        )

        dosage_columns = [
            c for c in raw.columns
            if c not in {"FID", "IID", "PAT", "MAT", "SEX", "PHENOTYPE"}
        ]

        if not dosage_columns:
            return None

        genotype = raw[dosage_columns].sum(axis=1).to_numpy(dtype=float)

    return genotype


def build_set_scores(gene_sets, bim_file, fam):
    gene_annot = pd.read_csv(
        GENE_ANNOT,
        sep=r"\s+",
        comment="#",
        engine="python",
    )

    gene_regions = {}

    for _, row in gene_annot.iterrows():
        gene_regions[row["GENE"]] = (
            str(row["CHR"]),
            int(row["START"]),
            int(row["STOP"]),
        )

    bim = pd.read_csv(
        bim_file,
        sep=r"\s+",
        header=None,
        names=["CHR", "SNP", "CM", "BP", "A1", "A2"],
        dtype={"CHR": str},
    )

    set_scores = {}

    for set_name, genes in gene_sets.items():
        regions = [
            gene_regions[g]
            for g in genes
            if g in gene_regions
        ]

        if not regions:
            continue

        snps = []

        for chrom, start, stop in regions:
            matches = bim[
                (bim["CHR"] == chrom)
                & (bim["BP"] >= start)
                & (bim["BP"] <= stop)
            ]

            snps.extend(matches["SNP"].tolist())

        snps = list(dict.fromkeys(snps))

        if not snps:
            continue

        genotype = extract_genotypes_for_snps(
            snps,
            fam,
        )

        if genotype is None:
            continue

        genotype = genotype.astype(float)

        sd = genotype.std(ddof=1)

        if sd == 0 or np.isnan(sd):
            continue

        set_scores[set_name] = (
            genotype - genotype.mean()
        ) / sd

    return set_scores


def standardize(x):
    x = np.asarray(x, dtype=float)

    sd = x.std(ddof=1)

    if sd == 0:
        return np.zeros_like(x)

    return (x - x.mean()) / sd


def simulate_phenotype(score, effect, phenotype_mean, phenotype_sd, rng):
    score = standardize(score)

    residual_sd = phenotype_sd * np.sqrt(
        max(1.0 - effect**2, 0.01)
    )

    phenotype = (
        phenotype_mean
        + phenotype_sd * effect * score
        + rng.normal(
            0,
            residual_sd,
            len(score),
        )
    )

    return phenotype


def run_gene_power():
    fam = read_fam()
    phenotype_mean, phenotype_sd = read_phenotype()

    bim_file = BFILE.with_suffix(".bim")

    gene_annot = pd.read_csv(
        GENE_ANNOT,
        sep=r"\s+",
        comment="#",
        engine="python",
    )

    genes = gene_annot["GENE"].tolist()

    rng = np.random.default_rng(RANDOM_SEED)

    target_genes = rng.choice(
        genes,
        size=min(100, len(genes)),
        replace=False,
    )

    rows = []

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)

        for effect in EFFECT_SIZES:
            detected = 0
            valid = 0

            for simulation in range(N_SIMULATIONS):
                target_gene = rng.choice(target_genes)

                snps = build_gene_score(
                    bim_file,
                    target_gene,
                )

                if not snps:
                    continue

                score = extract_genotypes_for_snps(
                    snps,
                    fam,
                )

                if score is None:
                    continue

                phenotype = simulate_phenotype(
                    score,
                    effect,
                    phenotype_mean,
                    phenotype_sd,
                    rng,
                )

                pheno_file = tmp / f"gene_{effect}_{simulation}.pheno"
                prefix = tmp / f"gene_{effect}_{simulation}"

                write_pheno(
                    pheno_file,
                    fam,
                    phenotype,
                )

                run_magma_gene(
                    pheno_file,
                    prefix,
                )

                result = read_gene_results(
                    prefix.with_suffix(".genes.out")
                )

                target = result[
                    result["GENE"] == target_gene
                ]

                if target.empty:
                    continue

                valid += 1

                p = float(target.iloc[0]["P"])

                if p <= GENE_ALPHA:
                    detected += 1

                rows.append(
                    {
                        "effect": effect,
                        "simulation": simulation + 1,
                        "target_gene": target_gene,
                        "p": p,
                        "significant": int(p <= GENE_ALPHA),
                    }
                )

            power = (
                detected / valid
                if valid > 0
                else np.nan
            )

            rows.append(
                {
                    "effect": effect,
                    "simulation": "SUMMARY",
                    "target_gene": "",
                    "p": "",
                    "significant": power,
                }
            )

    results = pd.DataFrame(rows)

    summary = (
        results[
            results["simulation"] == "SUMMARY"
        ][
            ["effect", "significant"]
        ]
        .rename(
            columns={
                "significant": "power"
            }
        )
    )

    return results, summary


def run_geneset_power():
    fam = read_fam()
    phenotype_mean, phenotype_sd = read_phenotype()

    gene_sets = load_gene_sets()

    rng = np.random.default_rng(
        RANDOM_SEED + 1
    )

    bim_file = BFILE.with_suffix(".bim")

    set_scores = build_set_scores(
        gene_sets,
        bim_file,
        fam,
    )

    rows = []

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)

        for effect in EFFECT_SIZES:
            detection = {
                name: 0
                for name in set_scores
            }

            valid = 0

            for simulation in range(N_SIMULATIONS):
                if not set_scores:
                    continue

                target_set = rng.choice(
                    list(set_scores.keys())
                )

                score = set_scores[target_set]

                phenotype = simulate_phenotype(
                    score,
                    effect,
                    phenotype_mean,
                    phenotype_sd,
                    rng,
                )

                pheno_file = tmp / f"set_{effect}_{simulation}.pheno"
                gene_prefix = tmp / f"set_{effect}_{simulation}_gene"
                set_prefix = tmp / f"set_{effect}_{simulation}_geneset"

                write_pheno(
                    pheno_file,
                    fam,
                    phenotype,
                )

                run_magma_gene(
                    pheno_file,
                    gene_prefix,
                )

                run_magma_geneset(
                    gene_prefix.with_suffix(".genes.raw"),
                    set_prefix,
                )

                result = read_geneset_results(
                    set_prefix.with_suffix(".gsa.out")
                )

                target = result[
                    result["FULL_NAME"] == target_set
                ]

                if target.empty:
                    target = result[
                        result["VARIABLE"] == target_set
                    ]

                if target.empty:
                    continue

                valid += 1

                p = float(target.iloc[0]["P"])

                if p <= GENESET_ALPHA:
                    detection[target_set] += 1

                rows.append(
                    {
                        "effect": effect,
                        "simulation": simulation + 1,
                        "target_gene_set": target_set,
                        "p": p,
                        "significant": int(
                            p <= GENESET_ALPHA
                        ),
                    }
                )

            power = (
                sum(detection.values()) / valid
                if valid > 0
                else np.nan
            )

            rows.append(
                {
                    "effect": effect,
                    "simulation": "SUMMARY",
                    "target_gene_set": "",
                    "p": "",
                    "significant": power,
                }
            )

    results = pd.DataFrame(rows)

    summary = (
        results[
            results["simulation"] == "SUMMARY"
        ][
            ["effect", "significant"]
        ]
        .rename(
            columns={
                "significant": "power"
            }
        )
    )

    return results, summary


def main():
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("Running MAGMA gene power analysis...")
    gene_results, gene_summary = run_gene_power()

    gene_results.to_csv(
        GENE_RESULTS,
        sep="\t",
        index=False,
    )

    gene_summary.to_csv(
        GENE_SUMMARY,
        sep="\t",
        index=False,
    )

    print("\nGene power:")
    print(gene_summary.to_string(index=False))

    print("\nRunning MAGMA gene-set power analysis...")
    geneset_results, geneset_summary = run_geneset_power()

    geneset_results.to_csv(
        GENESET_RESULTS,
        sep="\t",
        index=False,
    )

    geneset_summary.to_csv(
        GENESET_SUMMARY,
        sep="\t",
        index=False,
    )

    print("\nGene-set power:")
    print(geneset_summary.to_string(index=False))

    print(f"\nGene results: {GENE_RESULTS}")
    print(f"Gene summary: {GENE_SUMMARY}")
    print(f"Gene-set results: {GENESET_RESULTS}")
    print(f"Gene-set summary: {GENESET_SUMMARY}")


if __name__ == "__main__":
    main()