#!/usr/bin/env python3

# %% [markdown]
# # Female Lifespan Fine-Mapping Pipeline

# %%
import argparse
import shlex
import shutil
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd


# %% [markdown]
# ## Shared Configuration

# %%
BASE_DIR = Path(__file__).resolve().parents[1]
FLY_CHROMS = ("2L", "2R", "3L", "3R", "4", "X")

PHENO_NAME = "S18_1537_F"
GLM_DIR = BASE_DIR / "data" / "gwas" / "tmp"

MERGED_QC_SOURCE = GLM_DIR / "merged_qc"

OUT_DIR = BASE_DIR / "data" / "finemap" / "female"
SIG_SNP_FILE = OUT_DIR / "female_significant_snps.tsv"
COJO_INPUT_FILE = OUT_DIR / "female_cojo_input.txt"
COJO_OUT_PREFIX = OUT_DIR / "cojo" / "female_lifespan_cojo"
REGIONS_DIR = OUT_DIR / "regions"
SUSIE_WORK_DIR = OUT_DIR / "susie"
SUSIE_RESULTS_DIR = OUT_DIR / "susie_results"

CHROM_MAP = {"2L": "1", "2R": "2", "3L": "3", "3R": "4", "4": "5", "X": "23", "23": "23"}
COJO_CHROM_MAP = {1: "2L", 2: "2R", 3: "3L", 4: "3R", 5: "4", 23: "X"}
LD_REF_BFILE = OUT_DIR / "bfile" / "merged_qc_numeric"

WINDOW_BP = 100_000

SIG_P_THRESHOLD = 1e-5
MIN_MAF = 0.05
MIN_N = 100
SUSIE_COVERAGE = 0.95
SUSIE_MAX_EFFECTS = 10

GCTA_BIN = "gcta64"
PLINK_BIN = "plink"

GWAS_COLUMN_MAP = {
    "#CHROM": "CHR",
    "ID": "SNP",
    "A1_FREQ": "freq",
    "BETA": "b",
    "SE": "se",
    "P": "p",
    "OBS_CT": "N",
    "OMITTED": "A2",
}
GWAS_COLUMNS = ["CHR", "POS", "SNP", "A1", "A2", "freq", "b", "se", "p", "N"]
NUMERIC_COLUMNS = ["POS", "freq", "b", "se", "p", "N"]
COJO_COLUMNS = ["SNP", "A1", "A2", "freq", "b", "se", "p", "N"]
COJO_RESULT_COLUMNS = ["Chr", "SNP", "bp", "b", "p", "bJ", "pJ"]
COJO_RESULT_NUMERIC_COLUMNS = ["Chr", "bp", "b", "p", "bJ", "pJ"]
SUSIE_SUMMARY_COLUMNS = ["SNP", "A1", "A2", "b", "se", "p", "N"]
SUSIE_NUMERIC_COLUMNS = ["b", "se", "p", "N"]


# %% [markdown]
# ## Stage 1 — GCTA-COJO Signal Selection

# %%
def merge_gwas_sumstats() -> pd.DataFrame:
    frames = []
    for chrom in FLY_CHROMS:
        input_file = GLM_DIR / f"lifespan_{chrom}.{PHENO_NAME}.glm.linear"
        if not input_file.is_file():
            raise FileNotFoundError(f"Missing female GWAS output for {chrom}: {input_file}")

        chromosome_gwas = pd.read_csv(input_file, sep="\t", low_memory=False)
        required_columns = set(GWAS_COLUMN_MAP) | {"POS", "A1"}
        missing_columns = sorted(required_columns - set(chromosome_gwas.columns))
        if missing_columns:
            raise ValueError(f"{input_file} is missing columns: {', '.join(missing_columns)}")

        chromosome_gwas = chromosome_gwas.rename(columns=GWAS_COLUMN_MAP)
        frames.append(chromosome_gwas[GWAS_COLUMNS])

    gwas = pd.concat(frames, ignore_index=True)
    for column in NUMERIC_COLUMNS:
        gwas[column] = pd.to_numeric(gwas[column], errors="coerce")

    row_count = len(gwas)
    gwas = gwas.dropna(subset=NUMERIC_COLUMNS).copy()
    gwas["CHR"] = gwas["CHR"].astype(str)
    dropped_count = row_count - len(gwas)

    print(f"Merged female GWAS: {len(gwas):,} SNPs across {len(FLY_CHROMS)} chromosomes")
    if dropped_count:
        print(f"Dropped {dropped_count:,} rows with missing or invalid numeric values")
    return gwas


def filter_significant_snps(gwas: pd.DataFrame) -> pd.DataFrame:
    maf = np.minimum(gwas["freq"], 1 - gwas["freq"])
    valid = (
        gwas[["SNP", "A1", "A2"]].notna().all(axis=1)
        & gwas["freq"].between(0, 1)
        & gwas["p"].between(0, 1)
        & (gwas["se"] > 0)
        & (gwas["N"] >= MIN_N)
    )
    eligible = gwas.loc[valid & (maf >= MIN_MAF)].copy()
    significant = eligible.loc[eligible["p"] <= SIG_P_THRESHOLD].sort_values("p")

    if significant.empty:
        raise ValueError(
            f"No SNPs passed MAF >= {MIN_MAF:g}, N >= {MIN_N}, "
            f"and P <= {SIG_P_THRESHOLD:g}"
        )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    significant.to_csv(SIG_SNP_FILE, sep="\t", index=False)

    genome_wide_count = int((eligible["p"] <= 5e-8).sum())
    print(f"Eligible SNPs after MAF and sample-size filters: {len(eligible):,}")
    print(f"SNPs at P <= 5e-8: {genome_wide_count:,}")
    print(f"SNPs at P <= {SIG_P_THRESHOLD:g}: {len(significant):,}")
    print(f"Saved significant SNPs: {SIG_SNP_FILE}")
    return significant


def write_cojo_input(significant_snps: pd.DataFrame) -> None:
    duplicate_snps = significant_snps.loc[
        significant_snps["SNP"].duplicated(keep=False), "SNP"
    ].unique()
    if len(duplicate_snps):
        duplicate_list = ", ".join(map(str, duplicate_snps[:5]))
        raise ValueError(f"COJO input contains duplicate SNP IDs: {duplicate_list}")

    cojo = significant_snps[COJO_COLUMNS].copy()
    rounded_sample_size = np.rint(cojo["N"])
    if not np.allclose(cojo["N"], rounded_sample_size):
        raise ValueError("COJO sample sizes must be whole numbers")

    cojo["N"] = rounded_sample_size.astype(int)
    COJO_INPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    cojo.to_csv(COJO_INPUT_FILE, sep=" ", index=False)
    print(f"Saved COJO input: {COJO_INPUT_FILE} ({len(cojo):,} SNPs)")


def prepare_cojo_bfile() -> None:
    source_files = [MERGED_QC_SOURCE.with_suffix(ext) for ext in (".bed", ".bim", ".fam")]
    missing_files = [path for path in source_files if not path.is_file()]
    if missing_files:
        missing_list = "\n".join(f"  {path}" for path in missing_files)
        raise FileNotFoundError(f"Missing PLINK LD-reference files:\n{missing_list}")

    LD_REF_BFILE.parent.mkdir(parents=True, exist_ok=True)
    for extension in (".bed", ".fam"):
        source = MERGED_QC_SOURCE.with_suffix(extension).resolve()
        destination = LD_REF_BFILE.with_suffix(extension)
        if destination.is_symlink() and destination.resolve() != source:
            destination.unlink()
        elif destination.exists() and not destination.is_symlink():
            raise FileExistsError(f"Refusing to replace existing file: {destination}")
        if not destination.exists():
            destination.symlink_to(source)

    source_bim = MERGED_QC_SOURCE.with_suffix(".bim")
    target_bim = LD_REF_BFILE.with_suffix(".bim")
    temporary_bim = target_bim.with_suffix(".bim.tmp")
    variant_count = 0

    try:
        with source_bim.open(encoding="utf-8") as input_file, temporary_bim.open(
            "w", encoding="utf-8"
        ) as output_file:
            for line_number, line in enumerate(input_file, start=1):
                fields = line.split()
                if len(fields) != 6:
                    raise ValueError(
                        f"Malformed BIM row {line_number} in {source_bim}: "
                        f"expected 6 fields, found {len(fields)}"
                    )

                chrom, snp, cm, pos, allele_1, allele_2 = fields
                if chrom not in CHROM_MAP:
                    raise ValueError(f"Unsupported chromosome {chrom!r} in {source_bim}")

                output_file.write(
                    f"{CHROM_MAP[chrom]}\t{snp}\t{cm}\t{pos}\t{allele_1}\t{allele_2}\n"
                )
                variant_count += 1

        temporary_bim.replace(target_bim)
    finally:
        temporary_bim.unlink(missing_ok=True)

    print(f"Prepared COJO LD reference: {LD_REF_BFILE} ({variant_count:,} SNPs)")


def run_cojo(gcta_bin: str = GCTA_BIN, force: bool = False) -> None:
    jma_file = COJO_OUT_PREFIX.with_suffix(".jma.cojo")
    if jma_file.is_file() and not force:
        print(f"Using existing COJO result: {jma_file}")
        return

    gcta_command = str(Path(gcta_bin).expanduser())
    gcta_executable = shutil.which(gcta_command)
    if gcta_executable is None:
        raise FileNotFoundError(
            f"Could not execute {gcta_bin!r}; add gcta64 to PATH or pass --gcta-bin"
        )

    required_inputs = [
        COJO_INPUT_FILE,
        LD_REF_BFILE.with_suffix(".bed"),
        LD_REF_BFILE.with_suffix(".bim"),
        LD_REF_BFILE.with_suffix(".fam"),
    ]
    missing_inputs = [path for path in required_inputs if not path.is_file()]
    if missing_inputs:
        missing_list = "\n".join(f"  {path}" for path in missing_inputs)
        raise FileNotFoundError(f"Missing COJO input files:\n{missing_list}")

    COJO_OUT_PREFIX.parent.mkdir(parents=True, exist_ok=True)
    command = [
        gcta_executable,
        "--bfile",
        str(LD_REF_BFILE),
        "--maf",
        str(MIN_MAF),
        "--cojo-file",
        str(COJO_INPUT_FILE),
        "--cojo-slct",
        "--cojo-p",
        str(SIG_P_THRESHOLD),
        "--out",
        str(COJO_OUT_PREFIX),
    ]
    if force:
        jma_file.unlink(missing_ok=True)
    print(f"Running: {shlex.join(command)}")
    subprocess.run(command, check=True)

    if not jma_file.is_file():
        raise RuntimeError(f"GCTA finished without creating the expected result: {jma_file}")
    print(f"Saved COJO result: {jma_file}")


def load_cojo_signals() -> pd.DataFrame:
    jma_file = COJO_OUT_PREFIX.with_suffix(".jma.cojo")
    if not jma_file.is_file():
        raise FileNotFoundError(f"Missing COJO result: {jma_file}")

    signals = pd.read_csv(jma_file, sep=r"\s+")
    missing_columns = sorted(set(COJO_RESULT_COLUMNS) - set(signals.columns))
    if missing_columns:
        raise ValueError(f"{jma_file} is missing columns: {', '.join(missing_columns)}")
    if signals.empty:
        raise ValueError(f"COJO selected no independent signals in {jma_file}")

    for column in COJO_RESULT_NUMERIC_COLUMNS:
        signals[column] = pd.to_numeric(signals[column], errors="coerce")
    if signals[COJO_RESULT_NUMERIC_COLUMNS].isna().any(axis=None):
        raise ValueError(f"COJO result contains missing or invalid numeric values: {jma_file}")
    if not signals["p"].between(0, 1).all() or not signals["pJ"].between(0, 1).all():
        raise ValueError(f"COJO result contains P-values outside [0, 1]: {jma_file}")

    rounded_positions = np.rint(signals["bp"])
    if not np.allclose(signals["bp"], rounded_positions) or (rounded_positions < 1).any():
        raise ValueError(f"COJO result contains invalid base-pair positions: {jma_file}")
    signals["bp"] = rounded_positions.astype(int)

    rounded_chromosomes = np.rint(signals["Chr"])
    if not np.allclose(signals["Chr"], rounded_chromosomes):
        raise ValueError(f"COJO result contains invalid chromosome values: {jma_file}")
    signals["Chr"] = rounded_chromosomes.astype(int)
    unsupported_chromosomes = sorted(set(signals["Chr"]) - set(COJO_CHROM_MAP))
    if unsupported_chromosomes:
        chromosome_list = ", ".join(map(str, unsupported_chromosomes))
        raise ValueError(f"COJO result contains unsupported chromosomes: {chromosome_list}")

    duplicate_snps = signals.loc[signals["SNP"].duplicated(keep=False), "SNP"].unique()
    if len(duplicate_snps):
        duplicate_list = ", ".join(map(str, duplicate_snps[:5]))
        raise ValueError(f"COJO result contains duplicate SNP IDs: {duplicate_list}")

    signals = signals.sort_values("pJ").reset_index(drop=True)
    print(f"\n{len(signals)} independent signal(s) selected by COJO:")
    print(signals[COJO_RESULT_COLUMNS].to_string(index=False))
    return signals


def extract_regions(gwas: pd.DataFrame, signals: pd.DataFrame) -> None:
    REGIONS_DIR.mkdir(parents=True, exist_ok=True)
    loci = []
    coordinates = set()

    for signal in signals.itertuples(index=False):
        chrom = COJO_CHROM_MAP[signal.Chr]
        pos = int(signal.bp)
        coordinate = (chrom, pos)
        if coordinate in coordinates:
            raise ValueError(f"COJO result contains duplicate locus coordinate: {chrom}:{pos}")
        coordinates.add(coordinate)

        region = gwas.loc[
            (gwas["CHR"] == chrom)
            & gwas["POS"].between(max(1, pos - WINDOW_BP), pos + WINDOW_BP)
        ].sort_values(["POS", "SNP"])
        if region.empty:
            raise ValueError(f"No GWAS SNPs found within {WINDOW_BP:,} bp of {chrom}:{pos}")

        out_file = REGIONS_DIR / f"chr{chrom}_pos{pos}_snps.tsv"
        loci.append((signal.SNP, chrom, pos, region, out_file))

    expected_files = {locus[4] for locus in loci}
    for stale_file in set(REGIONS_DIR.glob("chr*_pos*_snps.tsv")) - expected_files:
        stale_file.unlink()
        print(f"Removed stale region: {stale_file}")

    for snp, chrom, pos, region, out_file in loci:
        temporary_file = out_file.with_suffix(".tsv.tmp")
        try:
            region.to_csv(temporary_file, sep="\t", index=False)
            temporary_file.replace(out_file)
        finally:
            temporary_file.unlink(missing_ok=True)
        snp_label = "SNP" if len(region) == 1 else "SNPs"
        print(f"{snp}: {len(region):,} {snp_label} within +/-{WINDOW_BP:,} bp -> {out_file}")


# %% [markdown]
# ## Stage 2 — SuSiE-RSS Fine-Mapping

# %%
def find_region_files() -> list[Path]:
    region_files = sorted(
        path for path in REGIONS_DIR.glob("chr*_pos*_snps.tsv") if path.is_file()
    )
    if not region_files:
        raise FileNotFoundError(f"No fine-mapping region files found in {REGIONS_DIR}")
    return region_files


def region_label(region_file: Path) -> str:
    suffix = "_snps"
    if not region_file.stem.endswith(suffix):
        raise ValueError(f"Invalid fine-mapping region filename: {region_file.name}")
    return region_file.stem.removesuffix(suffix)


def load_region_summary(region_file: Path) -> pd.DataFrame:
    region = pd.read_csv(region_file, sep="\t", low_memory=False)
    missing_columns = sorted(set(SUSIE_SUMMARY_COLUMNS) - set(region.columns))
    if missing_columns:
        raise ValueError(f"{region_file} is missing columns: {', '.join(missing_columns)}")
    if region.empty:
        raise ValueError(f"Fine-mapping region is empty: {region_file}")

    region = region.copy()
    invalid_snp = region["SNP"].isna() | region["SNP"].astype(str).str.contains(r"\s|^$")
    if invalid_snp.any():
        raise ValueError(f"Fine-mapping region contains invalid SNP IDs: {region_file}")
    region["SNP"] = region["SNP"].astype(str)

    alleles = region[["A1", "A2"]]
    invalid_alleles = alleles.isna().any(axis=1) | alleles.astype(str).apply(
        lambda column: column.str.contains(r"\s|^$")
    ).any(axis=1)
    if invalid_alleles.any():
        raise ValueError(f"Fine-mapping region contains invalid alleles: {region_file}")
    region[["A1", "A2"]] = alleles.astype(str)
    if (region["A1"] == region["A2"]).any():
        raise ValueError(f"Fine-mapping region contains identical A1 and A2 alleles: {region_file}")

    for column in SUSIE_NUMERIC_COLUMNS:
        region[column] = pd.to_numeric(region[column], errors="coerce")
    if not np.isfinite(region[SUSIE_NUMERIC_COLUMNS].to_numpy(dtype=float)).all():
        raise ValueError(f"Fine-mapping region contains invalid numeric values: {region_file}")
    if (region["se"] <= 0).any():
        raise ValueError(f"Fine-mapping region contains non-positive standard errors: {region_file}")
    if not region["p"].between(0, 1).all():
        raise ValueError(f"Fine-mapping region contains P-values outside [0, 1]: {region_file}")

    rounded_sample_size = np.rint(region["N"])
    if not np.allclose(region["N"], rounded_sample_size) or (rounded_sample_size <= 0).any():
        raise ValueError(f"Fine-mapping region contains invalid sample sizes: {region_file}")
    region["N"] = rounded_sample_size.astype(int)

    duplicate_snps = region.loc[region["SNP"].duplicated(keep=False), "SNP"].unique()
    if len(duplicate_snps):
        duplicate_list = ", ".join(map(str, duplicate_snps[:5]))
        raise ValueError(f"Fine-mapping region contains duplicate SNP IDs: {duplicate_list}")
    return region


def write_region_snplist(region_file: Path) -> Path:
    region = load_region_summary(region_file)
    label = region_label(region_file)
    SUSIE_WORK_DIR.mkdir(parents=True, exist_ok=True)
    snplist_file = SUSIE_WORK_DIR / f"{label}.snplist"
    temporary_file = snplist_file.with_suffix(".snplist.tmp")

    try:
        snp_text = "\n".join(region["SNP"].astype(str)) + "\n"
        temporary_file.write_text(snp_text, encoding="utf-8")
        temporary_file.replace(snplist_file)
    finally:
        temporary_file.unlink(missing_ok=True)

    print(f"Saved PLINK SNP list: {snplist_file} ({len(region):,} SNPs)")
    return snplist_file


def extract_region_bfile(
    region_file: Path, plink_bin: str = PLINK_BIN, force: bool = False
) -> Path:
    label = region_label(region_file)
    output_prefix = SUSIE_WORK_DIR / label
    output_files = [output_prefix.with_suffix(ext) for ext in (".bed", ".bim", ".fam")]
    if all(path.is_file() for path in output_files) and not force:
        print(f"Using existing locus genotype files: {output_prefix}")
        return output_prefix

    reference_files = [LD_REF_BFILE.with_suffix(ext) for ext in (".bed", ".bim", ".fam")]
    missing_reference = [path for path in reference_files if not path.is_file()]
    if missing_reference:
        missing_list = "\n".join(f"  {path}" for path in missing_reference)
        raise FileNotFoundError(f"Missing PLINK LD-reference files:\n{missing_list}")

    plink_command = str(Path(plink_bin).expanduser())
    plink_executable = shutil.which(plink_command)
    if plink_executable is None:
        raise FileNotFoundError(
            f"Could not execute {plink_bin!r}; add plink to PATH or pass --plink-bin"
        )

    snplist_file = write_region_snplist(region_file)
    for output_file in output_files:
        output_file.unlink(missing_ok=True)

    command = [
        plink_executable,
        "--bfile",
        str(LD_REF_BFILE),
        "--extract",
        str(snplist_file),
        "--make-bed",
        "--out",
        str(output_prefix),
    ]
    print(f"Running: {shlex.join(command)}")
    subprocess.run(command, check=True)

    missing_output = [path for path in output_files if not path.is_file()]
    if missing_output:
        missing_list = "\n".join(f"  {path}" for path in missing_output)
        raise RuntimeError(f"PLINK did not create the expected locus files:\n{missing_list}")
    if output_prefix.with_suffix(".bim").stat().st_size == 0:
        raise ValueError(f"PLINK extracted no variants for locus {label}")

    print(f"Saved locus genotype files: {output_prefix}")
    return output_prefix


def compute_region_ld(
    bfile_prefix: Path, plink_bin: str = PLINK_BIN, force: bool = False
) -> Path:
    genotype_files = [bfile_prefix.with_suffix(ext) for ext in (".bed", ".bim", ".fam")]
    missing_genotypes = [path for path in genotype_files if not path.is_file()]
    if missing_genotypes:
        missing_list = "\n".join(f"  {path}" for path in missing_genotypes)
        raise FileNotFoundError(f"Missing locus genotype files:\n{missing_list}")

    ld_file = bfile_prefix.with_suffix(".ld")
    if ld_file.is_file() and ld_file.stat().st_size > 0 and not force:
        print(f"Using existing locus LD matrix: {ld_file}")
        return ld_file

    plink_command = str(Path(plink_bin).expanduser())
    plink_executable = shutil.which(plink_command)
    if plink_executable is None:
        raise FileNotFoundError(
            f"Could not execute {plink_bin!r}; add plink to PATH or pass --plink-bin"
        )

    ld_file.unlink(missing_ok=True)
    command = [
        plink_executable,
        "--bfile",
        str(bfile_prefix),
        "--r",
        "square",
        "--out",
        str(bfile_prefix),
    ]
    print(f"Running: {shlex.join(command)}")
    subprocess.run(command, check=True)

    if not ld_file.is_file() or ld_file.stat().st_size == 0:
        raise RuntimeError(f"PLINK did not create a nonempty LD matrix: {ld_file}")

    print(f"Saved locus LD matrix: {ld_file}")
    return ld_file


# %% [markdown]
# ## Pipeline Command-Line Interface

# %%
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Select independent female lifespan GWAS signals for fine-mapping."
    )
    parser.add_argument(
        "--gcta-bin",
        default=GCTA_BIN,
        help="GCTA executable name or path (default: gcta64)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run GCTA-COJO even when its result file already exists",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    print("\n[1/7] Loading chromosome-level GWAS results")
    gwas = merge_gwas_sumstats()

    print("\n[2/7] Filtering COJO-eligible SNPs")
    significant_snps = filter_significant_snps(gwas)

    print("\n[3/7] Writing GCTA-COJO summary statistics")
    write_cojo_input(significant_snps)

    print("\n[4/7] Preparing the PLINK LD reference")
    prepare_cojo_bfile()

    print("\n[5/7] Selecting independent signals with GCTA-COJO")
    run_cojo(gcta_bin=args.gcta_bin, force=args.force)

    print("\n[6/7] Loading independent COJO signals")
    signals = load_cojo_signals()

    print("\n[7/7] Extracting fine-mapping regions")
    extract_regions(gwas, signals)


if __name__ == "__main__":
    main()
