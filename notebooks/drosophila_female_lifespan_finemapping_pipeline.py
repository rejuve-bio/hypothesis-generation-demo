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


def load_aligned_region_data(
    region_file: Path, bfile_prefix: Path, ld_file: Path
) -> tuple[pd.DataFrame, np.ndarray]:
    region = load_region_summary(region_file)
    bim_file = bfile_prefix.with_suffix(".bim")
    if not bim_file.is_file():
        raise FileNotFoundError(f"Missing locus BIM file: {bim_file}")
    if not ld_file.is_file():
        raise FileNotFoundError(f"Missing locus LD matrix: {ld_file}")

    bim = pd.read_csv(
        bim_file,
        sep=r"\s+",
        header=None,
        names=["CHR", "SNP", "CM", "POS", "LD_A1", "LD_A2"],
    )
    if bim.empty:
        raise ValueError(f"Locus BIM file is empty: {bim_file}")
    duplicate_bim_snps = bim.loc[bim["SNP"].duplicated(keep=False), "SNP"].unique()
    if len(duplicate_bim_snps):
        duplicate_list = ", ".join(map(str, duplicate_bim_snps[:5]))
        raise ValueError(f"Locus BIM contains duplicate SNP IDs: {duplicate_list}")

    ld = np.loadtxt(ld_file, dtype=float, ndmin=2)
    expected_shape = (len(bim), len(bim))
    if ld.shape != expected_shape:
        raise ValueError(
            f"LD matrix shape {ld.shape} does not match {len(bim)} BIM variants: {ld_file}"
        )

    aligned = bim.merge(region, on="SNP", how="left", validate="one_to_one", indicator=True)
    missing_summary = aligned.loc[aligned["_merge"] != "both", "SNP"].tolist()
    if missing_summary:
        missing_list = ", ".join(map(str, missing_summary[:5]))
        raise ValueError(f"BIM variants are missing summary statistics: {missing_list}")
    aligned = aligned.drop(columns="_merge")

    missing_reference_count = int((~region["SNP"].isin(bim["SNP"])).sum())
    if missing_reference_count:
        print(f"Excluded {missing_reference_count:,} region SNPs absent from the LD reference")

    summary_a1 = aligned["A1"].str.upper()
    summary_a2 = aligned["A2"].str.upper()
    ld_a1 = aligned["LD_A1"].astype(str).str.upper()
    ld_a2 = aligned["LD_A2"].astype(str).str.upper()
    matching = (summary_a1 == ld_a1) & (summary_a2 == ld_a2)
    swapped = (summary_a1 == ld_a2) & (summary_a2 == ld_a1)
    incompatible = ~(matching | swapped)
    if incompatible.any():
        incompatible_snps = ", ".join(aligned.loc[incompatible, "SNP"].astype(str).head(5))
        raise ValueError(f"Summary statistics and BIM alleles are incompatible: {incompatible_snps}")

    aligned.loc[swapped, "b"] = -aligned.loc[swapped, "b"]
    aligned["A1"] = aligned["LD_A1"].astype(str)
    aligned["A2"] = aligned["LD_A2"].astype(str)
    aligned = aligned.drop(columns=["LD_A1", "LD_A2"])

    finite_variants = np.isfinite(ld).all(axis=0) & np.isfinite(ld).all(axis=1)
    if not finite_variants.all():
        removed_count = int((~finite_variants).sum())
        aligned = aligned.loc[finite_variants].reset_index(drop=True)
        ld = ld[np.ix_(finite_variants, finite_variants)]
        print(f"Excluded {removed_count:,} variants with non-finite LD values")

    if len(aligned) < 2:
        raise ValueError(f"Fewer than two usable variants remain for locus {region_label(region_file)}")
    if not np.allclose(ld, ld.T, atol=1e-8):
        raise ValueError(f"LD matrix is not symmetric: {ld_file}")
    if not np.allclose(np.diag(ld), 1.0, atol=1e-6):
        raise ValueError(f"LD matrix diagonal is not one: {ld_file}")
    if (np.abs(ld) > 1 + 1e-8).any():
        raise ValueError(f"LD matrix contains correlations outside [-1, 1]: {ld_file}")

    return aligned, ld


def load_susie_runtime():
    try:
        import rpy2.robjects as ro
        from rpy2.robjects.packages import importr
    except ImportError as error:
        raise RuntimeError("Stage 2 requires rpy2 and an accessible R installation") from error

    try:
        susie_r = importr("susieR")
    except Exception as error:
        raise RuntimeError("Stage 2 requires the R package susieR") from error
    return ro, susie_r


def run_susie_rss(
    aligned: pd.DataFrame, ld: np.ndarray, runtime=None
) -> tuple[np.ndarray, np.ndarray]:
    if len(aligned) != ld.shape[0] or ld.shape[0] != ld.shape[1]:
        raise ValueError("SuSiE summary statistics and LD matrix dimensions do not match")
    if len(aligned) < 2:
        raise ValueError("SuSiE requires at least two aligned variants")

    ro, susie_r = runtime or load_susie_runtime()
    sample_size = int(np.rint(aligned["N"].median()))
    bhat = ro.FloatVector(aligned["b"].to_numpy(dtype=float))
    shat = ro.FloatVector(aligned["se"].to_numpy(dtype=float))
    r_matrix = ro.r["matrix"](
        ro.FloatVector(ld.flatten(order="F")),
        nrow=ld.shape[0],
    )

    fit = susie_r.susie_rss(
        bhat=bhat,
        shat=shat,
        R=r_matrix,
        n=sample_size,
        L=SUSIE_MAX_EFFECTS,
        estimate_residual_variance=True,
        verbose=False,
    )
    pip = np.asarray(fit.rx2("pip"), dtype=float)
    if pip.shape != (len(aligned),) or not np.isfinite(pip).all():
        raise ValueError("SuSiE returned invalid posterior inclusion probabilities")
    if ((pip < 0) | (pip > 1)).any():
        raise ValueError("SuSiE returned posterior inclusion probabilities outside [0, 1]")

    credible_sets = np.full(len(aligned), np.nan)
    cs_result = susie_r.susie_get_cs(fit, coverage=SUSIE_COVERAGE, Xcorr=r_matrix)
    cs_list = cs_result.rx2("cs")
    if cs_list is not ro.NULL:
        cs_names = [] if cs_list.names is ro.NULL else [str(name) for name in cs_list.names]
        for cs_number, cs_name in enumerate(cs_names, start=1):
            member_indices = np.asarray(cs_list.rx2(cs_name), dtype=int) - 1
            if ((member_indices < 0) | (member_indices >= len(aligned))).any():
                raise ValueError("SuSiE returned an invalid credible-set index")
            if np.isfinite(credible_sets[member_indices]).any():
                raise ValueError("SuSiE returned overlapping credible sets")
            credible_sets[member_indices] = cs_number

    return pip, credible_sets


def finemap_region(
    region_file: Path,
    plink_bin: str = PLINK_BIN,
    force: bool = False,
    runtime=None,
) -> Path:
    label = region_label(region_file)
    output_file = SUSIE_RESULTS_DIR / f"{label}_susie.tsv"
    if output_file.is_file() and output_file.stat().st_size > 0 and not force:
        print(f"Using existing SuSiE result: {output_file}")
        return output_file

    bfile_prefix = extract_region_bfile(region_file, plink_bin=plink_bin, force=force)
    ld_file = compute_region_ld(bfile_prefix, plink_bin=plink_bin, force=force)
    aligned, ld = load_aligned_region_data(region_file, bfile_prefix, ld_file)
    pip, credible_sets = run_susie_rss(aligned, ld, runtime=runtime)

    result = aligned.copy()
    result["PIP"] = pip
    result["CS"] = pd.array(credible_sets, dtype="Int64")
    result = result.sort_values(["PIP", "SNP"], ascending=[False, True]).reset_index(drop=True)

    SUSIE_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    temporary_file = output_file.with_suffix(".tsv.tmp")
    try:
        result.to_csv(temporary_file, sep="\t", index=False)
        temporary_file.replace(output_file)
    finally:
        temporary_file.unlink(missing_ok=True)

    credible_set_count = result["CS"].nunique(dropna=True)
    credible_set_label = "credible set" if credible_set_count == 1 else "credible sets"
    top_variant = result.iloc[0]
    print(
        f"{label}: {len(result):,} SNPs, {credible_set_count} {credible_set_label}, "
        f"top PIP {top_variant['SNP']} = {top_variant['PIP']:.3f}"
    )
    print(f"Saved SuSiE result: {output_file}")
    return output_file


def run_susie_finemapping(
    plink_bin: str = PLINK_BIN, force: bool = False, runtime=None
) -> list[Path]:
    region_files = find_region_files()
    output_files = []
    shared_runtime = runtime

    for region_file in region_files:
        expected_output = SUSIE_RESULTS_DIR / f"{region_label(region_file)}_susie.tsv"
        needs_inference = force or not expected_output.is_file() or expected_output.stat().st_size == 0
        if needs_inference and shared_runtime is None:
            shared_runtime = load_susie_runtime()
        output_files.append(
            finemap_region(
                region_file,
                plink_bin=plink_bin,
                force=force,
                runtime=shared_runtime,
            )
        )

    expected_outputs = set(output_files)
    for stale_file in set(SUSIE_RESULTS_DIR.glob("*_susie.tsv")) - expected_outputs:
        stale_file.unlink()
        print(f"Removed stale SuSiE result: {stale_file}")

    return output_files


# %% [markdown]
# ## Pipeline Command-Line Interface

# %%
def run_cojo_stage(gcta_bin: str = GCTA_BIN, force: bool = False) -> None:
    print("\n[1/7] Loading chromosome-level GWAS results")
    gwas = merge_gwas_sumstats()

    print("\n[2/7] Filtering COJO-eligible SNPs")
    significant_snps = filter_significant_snps(gwas)

    print("\n[3/7] Writing GCTA-COJO summary statistics")
    write_cojo_input(significant_snps)

    print("\n[4/7] Preparing the PLINK LD reference")
    prepare_cojo_bfile()

    print("\n[5/7] Selecting independent signals with GCTA-COJO")
    run_cojo(gcta_bin=gcta_bin, force=force)

    print("\n[6/7] Loading independent COJO signals")
    signals = load_cojo_signals()

    print("\n[7/7] Extracting fine-mapping regions")
    extract_regions(gwas, signals)


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
    run_cojo_stage(gcta_bin=args.gcta_bin, force=args.force)


if __name__ == "__main__":
    main()
