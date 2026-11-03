#!/usr/bin/env python3

from pathlib import Path
import math
import subprocess
import tempfile

import numpy as np
import pandas as pd
from scipy.stats import nct, t


BASE_DIR = Path("/mnt/hdd_1/rediet/fly-ldsc")

BFILE = BASE_DIR / "data" / "magma" / "merged_qc"
PHENO_FILE = BASE_DIR / "data" / "gwas" / "lifespan_female.pheno"
GWAS_FILE = BASE_DIR / "data" / "gwas" / "lifespan_female_gwas.tsv"

OUTPUT_DIR = BASE_DIR / "data" / "magma" / "power"

RESULTS_FILE = OUTPUT_DIR / "lifespan_genotype_power.tsv"
SUMMARY_FILE = OUTPUT_DIR / "lifespan_genotype_power_summary.tsv"

PHENO_COLUMN = "S18_1537_F"

ALPHA = 0.05

HYPOTHETICAL_DIFFERENCES = [
    2.0,
    5.0,
    10.0,
    15.0,
]


def read_phenotype():
    pheno = pd.read_csv(
        PHENO_FILE,
        sep=r"\s+",
        engine="python",
    )

    required = {"FID", "IID", PHENO_COLUMN}

    missing = required - set(pheno.columns)

    if missing:
        raise ValueError(
            f"Missing phenotype columns: {sorted(missing)}"
        )

    pheno = pheno[
        ["FID", "IID", PHENO_COLUMN]
    ].copy()

    pheno[PHENO_COLUMN] = pd.to_numeric(
        pheno[PHENO_COLUMN],
        errors="coerce",
    )

    pheno = pheno.dropna(
        subset=[PHENO_COLUMN]
    )

    pheno["FID"] = pheno["FID"].astype(str)
    pheno["IID"] = pheno["IID"].astype(str)

    return pheno


def read_gwas():
    gwas = pd.read_csv(
        GWAS_FILE,
        sep=r"\s+",
        engine="python",
    )

    required = {"SNP", "P"}

    missing = required - set(gwas.columns)

    if missing:
        raise ValueError(
            f"Missing GWAS columns: {sorted(missing)}"
        )

    gwas["P"] = pd.to_numeric(
        gwas["P"],
        errors="coerce",
    )

    gwas = gwas.dropna(
        subset=["P"]
    )

    gwas = gwas.sort_values(
        "P"
    )

    return gwas


def get_most_associated_snp(gwas):
    row = gwas.iloc[0]

    return {
        "SNP": str(row["SNP"]),
        "P": float(row["P"]),
        "CHR": str(row["CHR"]) if "CHR" in row else "",
        "POS": int(row["POS"]) if "POS" in row else "",
        "A1": str(row["A1"]) if "A1" in row else "",
        "BETA": float(row["BETA"]) if "BETA" in row else np.nan,
        "SE": float(row["SE"]) if "SE" in row else np.nan,
        "N": int(row["N"]) if "N" in row else np.nan,
    }


def extract_genotype(snp):
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)

        snp_file = tmp / "snp.txt"

        snp_file.write_text(
            f"{snp}\n"
        )

        prefix = tmp / "subset"

        subprocess.run(
            [
                "plink",
                "--bfile",
                str(BFILE),
                "--extract",
                str(snp_file),
                "--recode",
                "A",
                "--out",
                str(prefix),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        raw_file = prefix.with_suffix(
            ".raw"
        )

        if not raw_file.exists():
            return None

        raw = pd.read_csv(
            raw_file,
            sep=r"\s+",
            engine="python",
        )

        dosage_columns = [
            c
            for c in raw.columns
            if c not in {
                "FID",
                "IID",
                "PAT",
                "MAT",
                "SEX",
                "PHENOTYPE",
            }
        ]

        if not dosage_columns:
            return None

        genotype = raw[
            dosage_columns[0]
        ]

        result = pd.DataFrame(
            {
                "FID": raw["FID"].astype(str),
                "IID": raw["IID"].astype(str),
                "GENOTYPE": pd.to_numeric(
                    genotype,
                    errors="coerce",
                ),
            }
        )

        return result


def calculate_pooled_sd(
    var1,
    var2,
    n1,
    n2,
):
    pooled_variance = (
        ((n1 - 1) * var1)
        + ((n2 - 1) * var2)
    ) / (
        n1 + n2 - 2
    )

    return math.sqrt(
        pooled_variance
    )


def calculate_cohens_d(
    mean1,
    mean2,
    var1,
    var2,
    n1,
    n2,
):
    pooled_sd = calculate_pooled_sd(
        var1,
        var2,
        n1,
        n2,
    )

    if pooled_sd == 0:
        return np.nan

    return abs(
        mean1 - mean2
    ) / pooled_sd


def calculate_power(
    d,
    n1,
    n2,
    alpha=0.05,
):
    if (
        not np.isfinite(d)
        or d <= 0
        or n1 < 2
        or n2 < 2
    ):
        return np.nan

    df = n1 + n2 - 2

    ncp = (
        d
        * math.sqrt(
            (n1 * n2)
            / (n1 + n2)
        )
    )

    critical = t.ppf(
        1 - alpha / 2,
        df,
    )

    upper = 1 - nct.cdf(
        critical,
        df,
        ncp,
    )

    lower = nct.cdf(
        -critical,
        df,
        ncp,
    )

    return upper + lower


def analyze_observed_snp(
    snp_info,
    phenotype,
):
    genotype = extract_genotype(
        snp_info["SNP"]
    )

    if genotype is None:
        raise RuntimeError(
            "PLINK did not return genotype data for the SNP."
        )

    data = phenotype.merge(
        genotype,
        on=["FID", "IID"],
        how="inner",
    )

    data = data.dropna(
        subset=[
            PHENO_COLUMN,
            "GENOTYPE",
        ]
    )

    counts = (
        data["GENOTYPE"]
        .value_counts()
        .sort_index()
    )

    available = [
        genotype
        for genotype, count in counts.items()
        if count >= 2
    ]

    if len(available) < 2:
        raise RuntimeError(
            "Fewer than two genotype groups have at least two observations."
        )

    if 0 in available and 2 in available:
        genotype1 = 0
        genotype2 = 2
    else:
        genotype1 = available[0]
        genotype2 = available[-1]

    group1 = data[
        data["GENOTYPE"] == genotype1
    ][PHENO_COLUMN]

    group2 = data[
        data["GENOTYPE"] == genotype2
    ][PHENO_COLUMN]

    n1 = len(group1)
    n2 = len(group2)

    mean1 = group1.mean()
    mean2 = group2.mean()

    var1 = group1.var(
        ddof=1
    )

    var2 = group2.var(
        ddof=1
    )

    sd1 = group1.std(
        ddof=1
    )

    sd2 = group2.std(
        ddof=1
    )

    pooled_sd = calculate_pooled_sd(
        var1,
        var2,
        n1,
        n2,
    )

    mean_difference = abs(
        mean1 - mean2
    )

    cohens_d = calculate_cohens_d(
        mean1,
        mean2,
        var1,
        var2,
        n1,
        n2,
    )

    power = calculate_power(
        cohens_d,
        n1,
        n2,
        ALPHA,
    )

    return {
        "SNP": snp_info["SNP"],
        "CHR": snp_info["CHR"],
        "POS": snp_info["POS"],
        "GWAS_P": snp_info["P"],
        "GWAS_BETA": snp_info["BETA"],
        "GWAS_SE": snp_info["SE"],
        "GWAS_N": snp_info["N"],
        "GENOTYPE_1": genotype1,
        "GENOTYPE_2": genotype2,
        "N1": n1,
        "N2": n2,
        "MEAN_1": mean1,
        "MEAN_2": mean2,
        "SD_1": sd1,
        "SD_2": sd2,
        "VAR_1": var1,
        "VAR_2": var2,
        "POOLED_SD": pooled_sd,
        "MEAN_DIFFERENCE": mean_difference,
        "COHENS_D": cohens_d,
        "POWER": power,
    }


def calculate_hypothetical_power(
    phenotype,
):
    n = len(phenotype)

    phenotype_sd = phenotype[
        PHENO_COLUMN
    ].std(
        ddof=1
    )

    n1 = n // 2
    n2 = n - n1

    rows = []

    for difference in HYPOTHETICAL_DIFFERENCES:

        d = (
            difference
            / phenotype_sd
        )

        power = calculate_power(
            d,
            n1,
            n2,
            ALPHA,
        )

        rows.append(
            {
                "EFFECT_DIFFERENCE_DAYS": difference,
                "TOTAL_N": n,
                "N1": n1,
                "N2": n2,
                "PHENOTYPE_SD": phenotype_sd,
                "COHENS_D": d,
                "POWER": power,
            }
        )

    return pd.DataFrame(
        rows
    )


def main():
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    print(
        "Loading phenotype..."
    )

    phenotype = read_phenotype()

    print(
        f"Phenotype samples: {len(phenotype)}"
    )

    print(
        "Loading GWAS results..."
    )

    gwas = read_gwas()

    snp_info = get_most_associated_snp(
        gwas
    )

    print(
        "\nMost associated SNP:"
    )

    print(
        f"SNP: {snp_info['SNP']}"
    )

    print(
        f"P-value: {snp_info['P']:.6g}"
    )

    print(
        f"CHR: {snp_info['CHR']}"
    )

    print(
        f"POS: {snp_info['POS']}"
    )

    print(
        "\nCalculating genotype-specific effect..."
    )

    observed = analyze_observed_snp(
        snp_info,
        phenotype,
    )

    observed_df = pd.DataFrame(
        [observed]
    )

    observed_df.to_csv(
        RESULTS_FILE,
        sep="\t",
        index=False,
    )

    hypothetical = (
        calculate_hypothetical_power(
            phenotype
        )
    )

    hypothetical.to_csv(
        SUMMARY_FILE,
        sep="\t",
        index=False,
    )

    print(
        "\nObserved SNP:"
    )

    print(
        observed_df.to_string(
            index=False
        )
    )

    print(
        "\nHypothetical effect-size power:"
    )

    print(
        hypothetical.to_string(
            index=False
        )
    )

    print(
        f"\nResults: {RESULTS_FILE}"
    )

    print(
        f"Summary: {SUMMARY_FILE}"
    )


if __name__ == "__main__":
    main()