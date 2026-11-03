#!/usr/bin/env python3

import subprocess
from pathlib import Path
import math

import pandas as pd
from scipy.stats import nct, t


BASE_DIR = Path("/mnt/hdd_1/rediet/fly-ldsc")

GENOTYPE_PREFIX = BASE_DIR / "data" / "gwas" / "tmp" / "merged_qc"
KEEP_FILE = BASE_DIR / "data" / "gwas" / "tmp" / "female_lifespan.keep"
SUBSET_PREFIX = BASE_DIR / "data" / "gwas" / "tmp" / "female_lifespan_genotypes"
FREQ_PREFIX = BASE_DIR / "data" / "gwas" / "tmp" / "female_lifespan_freq"

PHENO_FILE = BASE_DIR / "data" / "gwas" / "lifespan_female.pheno"

HOMOZYGOTE_POWER_FILE = (
    BASE_DIR / "data" / "gwas" / "tmp" / "female_lifespan_power_homozygote.tsv"
)
HOMOZYGOTE_SUMMARY_FILE = (
    BASE_DIR / "data" / "gwas" / "tmp" / "female_lifespan_power_homozygote_summary.tsv"
)

ADDITIVE_POWER_FILE = (
    BASE_DIR / "data" / "gwas" / "tmp" / "female_lifespan_power_additive.tsv"
)
ADDITIVE_SUMMARY_FILE = (
    BASE_DIR / "data" / "gwas" / "tmp" / "female_lifespan_power_additive_summary.tsv"
)

ALPHA = 2.28e-8
POWER_THRESHOLD = 0.80

MAF_VALUES = [
    0.01,
    0.02,
    0.05,
    0.10,
    0.20,
    0.30,
    0.40,
    0.50,
]

EFFECT_SIZES = [
    1.0,
    2.0,
    5.0,
    7.5,
    10.0,
    15.0,
    20.0,
]


def run_plink_subset():
    subset_bed = SUBSET_PREFIX.with_suffix(".bed")
    subset_bim = SUBSET_PREFIX.with_suffix(".bim")
    subset_fam = SUBSET_PREFIX.with_suffix(".fam")

    if subset_bed.exists() and subset_bim.exists() and subset_fam.exists():
        return

    subprocess.run(
        [
            "plink",
            "--bfile",
            str(GENOTYPE_PREFIX),
            "--keep",
            str(KEEP_FILE),
            "--allow-extra-chr",
            "--make-bed",
            "--out",
            str(SUBSET_PREFIX),
        ],
        check=True,
    )


def run_plink_frequency():
    freq_file = FREQ_PREFIX.with_suffix(".frq")

    if freq_file.exists():
        return

    subprocess.run(
        [
            "plink",
            "--bfile",
            str(SUBSET_PREFIX),
            "--allow-extra-chr",
            "--freq",
            "--out",
            str(FREQ_PREFIX),
        ],
        check=True,
    )


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

    return len(values), values.mean(), values.std(ddof=1)


def count_samples(path):
    with open(path) as f:
        return sum(1 for _ in f)


def calculate_power(n1, n2, effect, sd, alpha):
    if n1 <= 1 or n2 <= 1:
        return float("nan")

    d = effect / sd
    df = n1 + n2 - 2

    ncp = d / math.sqrt((1.0 / n1) + (1.0 / n2))

    critical = t.ppf(
        1.0 - alpha / 2.0,
        df,
    )

    power_upper = nct.sf(
        critical,
        df,
        ncp,
    )

    power_lower = nct.cdf(
        -critical,
        df,
        ncp,
    )

    if math.isnan(power_lower):
        power_lower = 0.0

    return power_upper + power_lower


def calculate_additive_power(N, maf, effect, sd, alpha):
    if N <= 2 or maf <= 0 or maf >= 1:
        return float("nan")

    genotype_variance = 2.0 * maf * (1.0 - maf)

    ncp = (
        effect
        * math.sqrt(N * genotype_variance)
        / sd
    )

    df = N - 2

    critical = t.ppf(
        1.0 - alpha / 2.0,
        df,
    )

    power_upper = nct.sf(
        critical,
        df,
        ncp,
    )

    power_lower = nct.cdf(
        -critical,
        df,
        ncp,
    )

    if math.isnan(power_lower):
        power_lower = 0.0

    return power_upper + power_lower


def calculate_homozygote_power_table(N, sd):
    rows = []

    for maf in MAF_VALUES:
        n_minor_homozygotes = N * maf**2
        n_common_homozygotes = N * (1.0 - maf)**2

        for effect in EFFECT_SIZES:
            power = calculate_power(
                n_common_homozygotes,
                n_minor_homozygotes,
                effect,
                sd,
                ALPHA,
            )

            rows.append(
                {
                    "N": N,
                    "MAF": maf,
                    "effect_days": effect,
                    "sd_lifespan": sd,
                    "n_common_homozygotes": n_common_homozygotes,
                    "n_minor_homozygotes": n_minor_homozygotes,
                    "alpha": ALPHA,
                    "power": power,
                }
            )

    return pd.DataFrame(rows)


def calculate_additive_power_table(N, sd, observed_mafs):
    rows = []

    for maf in observed_mafs:
        for effect in EFFECT_SIZES:
            power = calculate_additive_power(
                N,
                maf,
                effect,
                sd,
                ALPHA,
            )

            rows.append(
                {
                    "N": N,
                    "MAF": maf,
                    "effect_days": effect,
                    "sd_lifespan": sd,
                    "genotype_variance": 2.0 * maf * (1.0 - maf),
                    "alpha": ALPHA,
                    "power": power,
                }
            )

    return pd.DataFrame(rows)


def calculate_summary(power_df):
    rows = []

    observed_min_maf = power_df["MAF"].min()
    observed_max_maf = power_df["MAF"].max()

    for effect in EFFECT_SIZES:
        subset = (
            power_df[
                power_df["effect_days"] == effect
            ]
            .dropna(subset=["power"])
            .sort_values("MAF")
        )

        if subset.empty:
            rows.append(
                {
                    "effect_days": effect,
                    "observed_min_MAF": observed_min_maf,
                    "observed_max_MAF": observed_max_maf,
                    "minimum_observed_MAF_for_80pct_power": float("nan"),
                    "maximum_power": float("nan"),
                    "MAF_at_maximum_power": float("nan"),
                }
            )
            continue

        maximum_power = subset["power"].max()

        max_power_maf = subset.loc[
            subset["power"].idxmax(),
            "MAF",
        ]

        sufficient = subset[
            subset["power"] >= POWER_THRESHOLD
        ]

        if len(sufficient) > 0:
            minimum_maf = sufficient.iloc[0]["MAF"]
        else:
            minimum_maf = float("nan")

        rows.append(
            {
                "effect_days": effect,
                "observed_min_MAF": observed_min_maf,
                "observed_max_MAF": observed_max_maf,
                "minimum_observed_MAF_for_80pct_power": minimum_maf,
                "maximum_power": maximum_power,
                "MAF_at_maximum_power": max_power_maf,
            }
        )

    return pd.DataFrame(rows)


def verify_inputs():
    genotype_fam = SUBSET_PREFIX.with_suffix(".fam")

    keep_count = count_samples(KEEP_FILE)

    genotype_count = count_samples(
        GENOTYPE_PREFIX.with_suffix(".fam")
    )

    subset_count = count_samples(genotype_fam)

    phenotype_count, mean_lifespan, sd_lifespan = read_phenotype()

    freq = pd.read_csv(
        FREQ_PREFIX.with_suffix(".frq"),
        sep=r"\s+",
        dtype={"CHR": str},
    )

    valid_maf = pd.to_numeric(
        freq["MAF"],
        errors="coerce",
    ).dropna()

    valid_maf = valid_maf[
        (valid_maf > 0) &
        (valid_maf < 0.5)
    ]

    observed_mafs = sorted(
        valid_maf.unique().tolist()
    )

    print("Verification")
    print(f"Genotype samples: {genotype_count}")
    print(f"Phenotype samples: {phenotype_count}")
    print(f"Keep-file samples: {keep_count}")
    print(f"Subset samples: {subset_count}")
    print(f"Mean lifespan: {mean_lifespan:.4f}")
    print(f"SD lifespan: {sd_lifespan:.6f}")
    print(f"Variants: {len(valid_maf)}")
    print(f"Min MAF: {valid_maf.min():.6f}")
    print(f"Median MAF: {valid_maf.median():.6f}")
    print(f"Max MAF: {valid_maf.max():.6f}")

    if phenotype_count != keep_count:
        raise ValueError(
            f"Phenotype count ({phenotype_count}) does not match "
            f"keep-file count ({keep_count})."
        )

    if phenotype_count != subset_count:
        raise ValueError(
            f"Phenotype count ({phenotype_count}) does not match "
            f"genotype subset count ({subset_count})."
        )

    return (
        phenotype_count,
        mean_lifespan,
        sd_lifespan,
        observed_mafs,
    )


def main():
    run_plink_subset()
    run_plink_frequency()

    (
        N,
        mean_lifespan,
        sd_lifespan,
        observed_mafs,
    ) = verify_inputs()

    homozygote_power_df = calculate_homozygote_power_table(
        N,
        sd_lifespan,
    )

    homozygote_summary_df = calculate_summary(
        homozygote_power_df,
    )

    additive_power_df = calculate_additive_power_table(
        N,
        sd_lifespan,
        observed_mafs,
    )

    additive_summary_df = calculate_summary(
        additive_power_df,
    )

    homozygote_power_df.to_csv(
        HOMOZYGOTE_POWER_FILE,
        sep="\t",
        index=False,
        float_format="%.10g",
    )

    homozygote_summary_df.to_csv(
        HOMOZYGOTE_SUMMARY_FILE,
        sep="\t",
        index=False,
        float_format="%.10g",
    )

    additive_power_df.to_csv(
        ADDITIVE_POWER_FILE,
        sep="\t",
        index=False,
        float_format="%.10g",
    )

    additive_summary_df.to_csv(
        ADDITIVE_SUMMARY_FILE,
        sep="\t",
        index=False,
        float_format="%.10g",
    )

    print(f"Homozygote power results: {HOMOZYGOTE_POWER_FILE}")
    print(f"Homozygote power summary: {HOMOZYGOTE_SUMMARY_FILE}")
    print(f"Additive power results: {ADDITIVE_POWER_FILE}")
    print(f"Additive power summary: {ADDITIVE_SUMMARY_FILE}")


if __name__ == "__main__":
    main()