#!/usr/bin/env python3
"""DGRP female lifespan GWAS and fine-mapping pipeline.
"""

import argparse
import filecmp
import gzip
import hashlib
import platform
import re
import shlex
import shutil
import subprocess
import tarfile
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_BASE_DIR = Path(__file__).resolve().parents[1]
FLY_CHROMS = ("2L", "2R", "3L", "3R", "4", "X")

PHENO_NAME = "S18_1537_F"
N_PCS = 4

CHROM_MAP = {"2L": "1", "2R": "2", "3L": "3", "3R": "4", "4": "5", "X": "23", "23": "23"}
COJO_CHROM_MAP = {1: "2L", 2: "2R", 3: "3L", 4: "3R", 5: "4", 23: "X"}
SOURCE_CHROM_MAP = {"2L": "chr2L", "2R": "chr2R", "3L": "chr3L", "3R": "chr3R", "4": "4", "X": "23"}


PHENOTYPE_URL = "https://dgrpool.epfl.ch/studies/18/get_file?name=summary.tsv"
PHENOTYPE_SHA256 = "efb9a7f2dcd46ab24a01ff41876b4340a1450de90cb35d211b3f0c07bd68e9aa"
GENOTYPE_URL_PREFIX = "https://zenodo.org/records/837947/files/dgrp2_dm6_dbSNP.vcf"
GENOTYPE_MD5 = {
    ".bed": "6e3b0c5b2c186c3a6dec99c20c24e3f4",
    ".bim": "18ba9e60111dd6398d160f9bba89023c",
    ".fam": "d5cbeec02da5e40a89f3c3a9ab8f15cf",
}
GTF_URL = (
    "https://ftp.ebi.ac.uk/ensemblgenomes/pub/metazoa/release-62/gtf/"
    "drosophila_melanogaster/Drosophila_melanogaster.BDGP6.54.62.chr.gtf.gz"
)
GTF_SHA256 = "39e943ea25fbe46a6ec3fc28742e7bbf5f6c5e6de470785597bee3662e80730e"


ENHANCER_ATLAS_DIR = Path("/mnt/hdd_2/biocypher-kg/input/enhancer_atlas/dm")


def configure_paths(base_dir=None, glm_dir=None, pheno=None, qc_dir=None,
                    eigenvec=None, bfile=None, out_dir=None, gtf=None,
                    enhancer_dir=None):
    """Set every input/output path global, deriving defaults from base_dir.
    Called once at import with defaults, and again from main() with any CLI
    overrides so the pipeline can run against inputs in any directory."""
    global BASE_DIR, GLM_DIR, PHENO_FILE, QC_GENOTYPE_DIR, EIGENVEC_FILE
    global MERGED_QC_SOURCE, OUT_DIR, SIG_SNP_FILE, COJO_INPUT_FILE, COJO_OUT_PREFIX
    global REGIONS_DIR, SUSIE_WORK_DIR, SUSIE_RESULTS_DIR, LD_REF_BFILE
    global GTF_FILE, GENE_MAPPING_FILE, ENHANCER_LOCAL_DIR, ENHANCER_MAPPING_FILE
    global REFERENCE_DIR, RAW_GENOTYPE_PREFIX, RAW_PHENO_FILE, ANALYSIS_LINES_FILE

    BASE_DIR = Path(base_dir).expanduser() if base_dir else DEFAULT_BASE_DIR
    REFERENCE_DIR = BASE_DIR / "data" / "reference"
    RAW_GENOTYPE_PREFIX = REFERENCE_DIR / "dgrp2_dm6_dbSNP.vcf"
    RAW_PHENO_FILE = BASE_DIR / "data" / "gwas" / "lifespan_female_raw.tsv.gz"
    GLM_DIR = Path(glm_dir).expanduser() if glm_dir else BASE_DIR / "data" / "gwas" / "tmp"
    PHENO_FILE = Path(pheno).expanduser() if pheno else BASE_DIR / "data" / "gwas" / "lifespan_female.pheno"
    QC_GENOTYPE_DIR = Path(qc_dir).expanduser() if qc_dir else GLM_DIR / "qc"
    EIGENVEC_FILE = Path(eigenvec).expanduser() if eigenvec else GLM_DIR / "dgrp_pca.eigenvec"
    MERGED_QC_SOURCE = Path(bfile).expanduser() if bfile else GLM_DIR / "merged_qc"
    ANALYSIS_LINES_FILE = GLM_DIR / "analysis_lines.keep"

    OUT_DIR = Path(out_dir).expanduser() if out_dir else BASE_DIR / "data" / "finemap" / "female"
    SIG_SNP_FILE = OUT_DIR / "female_significant_snps.tsv"
    COJO_INPUT_FILE = OUT_DIR / "female_cojo_input.txt"
    COJO_OUT_PREFIX = OUT_DIR / "cojo" / "female_lifespan_cojo"
    REGIONS_DIR = OUT_DIR / "regions"
    SUSIE_WORK_DIR = OUT_DIR / "susie"
    SUSIE_RESULTS_DIR = OUT_DIR / "susie_results"
    LD_REF_BFILE = OUT_DIR / "bfile" / "merged_qc_numeric"

    GTF_FILE = Path(gtf).expanduser() if gtf else BASE_DIR / "data" / "genes" / "Drosophila_melanogaster.BDGP6.54.62.chr.gtf.gz"
    GENE_MAPPING_FILE = OUT_DIR / "female_finemap_gene_mapping.tsv"
    ENHANCER_LOCAL_DIR = Path(enhancer_dir).expanduser() if enhancer_dir else BASE_DIR / "data" / "enhancers" / "dm"
    ENHANCER_MAPPING_FILE = OUT_DIR / "female_finemap_enhancer_overlap.tsv"


configure_paths()

WINDOW_BP = 100_000

SIG_P_THRESHOLD = 1e-5
MIN_MAF = 0.05
MIN_N = 100
SUSIE_COVERAGE = 0.95
SUSIE_MAX_EFFECTS = 10

GCTA_BIN = "gcta64"
PLINK_BIN = "plink"
PLINK2_BIN = "plink2"

GCTA_VERSION = "1.94.1"
GCTA_PACKAGE_URL = (
    "https://conda.anaconda.org/bioconda/linux-64/"
    "gcta-1.94.1-h9ee0642_0.tar.bz2"
)
GCTA_PACKAGE_SHA256 = (
    "8be7f419e57de1453422cb512d073388aa000672a28149af81121043d0cebbee"
)
GCTA_ARCHIVE_MEMBER = f"bin/gcta-{GCTA_VERSION}"
GCTA_LOCAL_BIN = BASE_DIR / "tools" / "gcta" / GCTA_VERSION / "gcta64"

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


def install_gcta(destination: Path = GCTA_LOCAL_BIN) -> Path:
    machine = platform.machine().lower()
    if platform.system() != "Linux" or machine not in {"x86_64", "amd64"}:
        raise RuntimeError(
            "Automatic GCTA installation supports Linux x86_64 only; "
            "install GCTA manually and pass its path with --gcta-bin"
        )

    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)

    try:
        with tempfile.TemporaryDirectory(
            prefix="gcta-download-", dir=destination.parent
        ) as temporary_directory:
            archive = Path(temporary_directory) / "gcta.tar.bz2"
            print(f"Downloading GCTA {GCTA_VERSION} from {GCTA_PACKAGE_URL}")
            urllib.request.urlretrieve(GCTA_PACKAGE_URL, archive)

            digest = hashlib.sha256(archive.read_bytes()).hexdigest()
            if digest != GCTA_PACKAGE_SHA256:
                raise RuntimeError(
                    "Downloaded GCTA package failed SHA-256 verification: "
                    f"expected {GCTA_PACKAGE_SHA256}, found {digest}"
                )

            with tarfile.open(archive, mode="r:bz2") as package:
                executable = package.extractfile(GCTA_ARCHIVE_MEMBER)
                if executable is None:
                    raise RuntimeError(
                        f"GCTA package does not contain {GCTA_ARCHIVE_MEMBER}"
                    )
                temporary_executable = Path(temporary_directory) / "gcta64"
                with executable, temporary_executable.open("wb") as output_file:
                    shutil.copyfileobj(executable, output_file)

            if temporary_executable.stat().st_size == 0:
                raise RuntimeError("Downloaded GCTA executable is empty")
            with temporary_executable.open("rb") as executable_file:
                if executable_file.read(4) != b"\x7fELF":
                    raise RuntimeError(
                        "Downloaded GCTA executable is not a Linux ELF binary"
                    )

            temporary_executable.chmod(0o755)
            temporary_executable.replace(destination)
    except (OSError, tarfile.TarError, urllib.error.URLError) as error:
        raise RuntimeError(
            f"Could not install GCTA {GCTA_VERSION} in {destination.parent}: {error}"
        ) from error

    print(f"Installed GCTA {GCTA_VERSION}: {destination}")
    return destination


def resolve_gcta_binary(gcta_bin: str = GCTA_BIN) -> str:
    if gcta_bin != GCTA_BIN:
        requested = str(Path(gcta_bin).expanduser())
        executable = shutil.which(requested)
        if executable is None:
            raise FileNotFoundError(
                f"Could not execute the requested GCTA binary: {gcta_bin}"
            )
        return executable

    local_executable = shutil.which(str(GCTA_LOCAL_BIN))
    if local_executable is not None:
        return local_executable

    return str(install_gcta())


def merge_gwas_sumstats() -> pd.DataFrame:
    frames = []
    for chrom in FLY_CHROMS:
        input_file = GLM_DIR / f"lifespan_female_{chrom}.{PHENO_NAME}.glm.linear"
        if not input_file.is_file():
            raise FileNotFoundError(f"Missing female GWAS output for {chrom}: {input_file}")

        chromosome_gwas = pd.read_csv(input_file, sep="\t", low_memory=False)
        required_columns = set(GWAS_COLUMN_MAP) | {"POS", "A1", "TEST"}
        missing_columns = sorted(required_columns - set(chromosome_gwas.columns))
        if missing_columns:
            raise ValueError(f"{input_file} is missing columns: {', '.join(missing_columns)}")

        non_additive_count = int(chromosome_gwas["TEST"].ne("ADD").sum())
        if non_additive_count:
            print(f"Excluded {non_additive_count:,} non-additive tests from {input_file}")
        chromosome_gwas = chromosome_gwas.loc[chromosome_gwas["TEST"].eq("ADD")]
        if chromosome_gwas.empty:
            raise ValueError(f"No additive association results found in {input_file}")
        observed_chromosomes = set(chromosome_gwas["#CHROM"].astype(str))
        if observed_chromosomes != {chrom}:
            raise ValueError(
                f"Unexpected chromosome labels in {input_file}: "
                f"expected {chrom}, found {sorted(observed_chromosomes)}"
            )
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


def save_gwas_sumstats() -> Path:
    output_file = GLM_DIR / "lifespan_female_gwas_sumstats.tsv"
    temporary_file = output_file.with_suffix(".tsv.tmp")
    gwas = merge_gwas_sumstats()
    valid = (
        np.isfinite(gwas[NUMERIC_COLUMNS].to_numpy(dtype=float)).all(axis=1)
        & gwas[["SNP", "A1", "A2"]].notna().all(axis=1).to_numpy()
        & (gwas["A1"] != gwas["A2"]).to_numpy()
        & gwas["freq"].between(0, 1).to_numpy()
        & gwas["p"].between(0, 1).to_numpy()
        & (gwas["POS"] > 0).to_numpy()
        & (gwas["se"] > 0).to_numpy()
        & (gwas["N"] > 0).to_numpy()
    )
    invalid_count = int((~valid).sum())
    gwas = gwas.loc[valid]
    if gwas.empty:
        raise ValueError("No valid female GWAS summary statistics remain after quality checks")
    try:
        gwas.to_csv(temporary_file, sep="\t", index=False)
        temporary_file.replace(output_file)
    finally:
        temporary_file.unlink(missing_ok=True)
    if invalid_count:
        print(f"Excluded {invalid_count:,} invalid GWAS row{'s' if invalid_count != 1 else ''}")
    print(f"Saved {len(gwas):,} female GWAS summary statistics: {output_file}")
    return output_file


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
    temporary_file = COJO_INPUT_FILE.with_name(COJO_INPUT_FILE.name + ".tmp")
    try:
        cojo.to_csv(temporary_file, sep=" ", index=False)
        if not COJO_INPUT_FILE.is_file() or not filecmp.cmp(
            temporary_file, COJO_INPUT_FILE, shallow=False
        ):
            temporary_file.replace(COJO_INPUT_FILE)
    finally:
        temporary_file.unlink(missing_ok=True)
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

        if not target_bim.is_file() or not filecmp.cmp(
            temporary_bim, target_bim, shallow=False
        ):
            temporary_bim.replace(target_bim)
    finally:
        temporary_bim.unlink(missing_ok=True)

    print(f"Prepared COJO LD reference: {LD_REF_BFILE} ({variant_count:,} SNPs)")


def run_cojo(gcta_bin: str = GCTA_BIN, force: bool = False) -> None:
    jma_file = COJO_OUT_PREFIX.with_suffix(".jma.cojo")
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
    if jma_file.is_file() and not force:
        latest_input = max(path.stat().st_mtime_ns for path in required_inputs)
        if jma_file.stat().st_mtime_ns > latest_input:
            print(f"Using existing COJO result: {jma_file}")
            return
        print(f"Rerunning COJO because an input is newer than {jma_file}")

    gcta_executable = resolve_gcta_binary(gcta_bin)
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


def summarize_susie_results(output_files: list[Path]) -> None:
    total_credible_sets = 0
    total_credible_variants = 0
    print("\nSuSiE credible-set summary")

    for output_file in output_files:
        result = pd.read_csv(output_file, sep="\t", low_memory=False)
        required_columns = {"SNP", "PIP", "CS"}
        missing_columns = sorted(required_columns - set(result.columns))
        if missing_columns:
            raise ValueError(
                f"{output_file} is missing columns: {', '.join(missing_columns)}"
            )

        result["PIP"] = pd.to_numeric(result["PIP"], errors="coerce")
        result["CS"] = pd.to_numeric(result["CS"], errors="coerce")
        if result["PIP"].isna().any() or not result["PIP"].between(0, 1).all():
            raise ValueError(f"{output_file} contains invalid PIP values")

        credible_variants = result.loc[result["CS"].notna()].copy()
        if not credible_variants.empty:
            rounded_sets = np.rint(credible_variants["CS"])
            if (
                not np.allclose(credible_variants["CS"], rounded_sets)
                or (rounded_sets < 1).any()
            ):
                raise ValueError(f"{output_file} contains invalid credible-set values")
            credible_variants["CS"] = rounded_sets.astype(int)

        locus = output_file.stem.removesuffix("_susie")
        credible_set_count = credible_variants["CS"].nunique()
        print(f"{locus}: {credible_set_count} credible set(s)")

        credible_variants = credible_variants.sort_values(
            ["CS", "PIP", "SNP"], ascending=[True, False, True]
        )
        for variant in credible_variants.itertuples(index=False):
            print(f"  CS {variant.CS}: {variant.SNP}, PIP = {variant.PIP:.6f}")

        total_credible_sets += credible_set_count
        total_credible_variants += len(credible_variants)

    print(f"Total credible sets: {total_credible_sets}")
    print(f"Total credible-set variants: {total_credible_variants}")


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

    summarize_susie_results(output_files)
    return output_files


GENE_ID_PATTERN = re.compile(r'gene_id "([^"]+)"')
GENE_NAME_PATTERN = re.compile(r'gene_name "([^"]+)"')
GENE_BIOTYPE_PATTERN = re.compile(r'gene_biotype "([^"]+)"')
GENE_MAPPING_COLUMNS = [
    "locus", "SNP", "CHR", "POS", "PIP", "CS",
    "gene_id", "gene_name", "gene_biotype", "relation", "distance_bp",
]


def _extract_gtf_attribute(pattern: re.Pattern, attributes: str, default: str = "") -> str:
    match = pattern.search(attributes)
    return match.group(1) if match else default


def load_gene_annotations(gtf_file: Path | None = None) -> pd.DataFrame:
    gtf_file = gtf_file or GTF_FILE
    if not gtf_file.is_file():
        raise FileNotFoundError(f"Missing gene annotation GTF: {gtf_file}")

    rows = []
    with gzip.open(gtf_file, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.startswith("#"):
                continue
            fields = line.rstrip("\n").split("\t")
            if len(fields) != 9 or fields[2] != "gene":
                continue
            attributes = fields[8]
            gene_id = _extract_gtf_attribute(GENE_ID_PATTERN, attributes)
            rows.append(
                {
                    "CHR": fields[0],
                    "start": int(fields[3]),
                    "end": int(fields[4]),
                    "strand": fields[6],
                    "gene_id": gene_id,
                    "gene_name": _extract_gtf_attribute(GENE_NAME_PATTERN, attributes, gene_id),
                    "gene_biotype": _extract_gtf_attribute(GENE_BIOTYPE_PATTERN, attributes),
                }
            )

    genes = pd.DataFrame(rows)
    if genes.empty:
        raise ValueError(f"No gene features parsed from {gtf_file}")
    print(f"Loaded {len(genes):,} genes from {gtf_file.name}")
    return genes


def select_credible_set_variants(output_files: list[Path]) -> pd.DataFrame:
    frames = []
    for output_file in output_files:
        result = pd.read_csv(output_file, sep="\t", low_memory=False)
        required_columns = {"SNP", "POS_x", "PIP", "CS"}
        missing_columns = sorted(required_columns - set(result.columns))
        if missing_columns:
            raise ValueError(f"{output_file} is missing columns: {', '.join(missing_columns)}")
        result["locus"] = output_file.stem.removesuffix("_susie")
        frames.append(result)

    variants = pd.concat(frames, ignore_index=True)
    variants["PIP"] = pd.to_numeric(variants["PIP"], errors="coerce")
    variants["CS"] = pd.to_numeric(variants["CS"], errors="coerce")

    credible = variants.loc[variants["CS"].notna()].copy()
    credible = credible.sort_values(
        ["locus", "CS", "PIP"], ascending=[True, True, False]
    ).reset_index(drop=True)

    credible_set_count = credible.groupby("locus")["CS"].nunique().sum()
    print(
        f"Selected {len(credible):,} credible-set variants "
        f"across {credible['locus'].nunique()} loci ({credible_set_count} credible sets)"
    )
    return credible


def map_variant_to_gene(genes: pd.DataFrame, chrom: str, pos: int) -> pd.DataFrame:
    on_chrom = genes.loc[genes["CHR"] == chrom]
    if on_chrom.empty:
        raise ValueError(f"No genes annotated on chromosome {chrom}")

    overlapping = on_chrom.loc[(on_chrom["start"] <= pos) & (on_chrom["end"] >= pos)]
    if not overlapping.empty:
        mapped = overlapping.copy()
        mapped["distance_bp"] = 0
        mapped["relation"] = "overlapping"
        return mapped

    distance = np.maximum(pos - on_chrom["end"], on_chrom["start"] - pos)
    nearest_index = distance.idxmin()
    mapped = on_chrom.loc[[nearest_index]].copy()
    mapped["distance_bp"] = int(distance.loc[nearest_index])
    mapped["relation"] = "nearest"
    return mapped


def map_finemap_genes(output_files: list[Path]) -> pd.DataFrame:
    variants = select_credible_set_variants(output_files)
    if variants.empty:
        print("No credible-set variants to map to genes")
        mapping = pd.DataFrame(columns=GENE_MAPPING_COLUMNS)
    else:
        genes = load_gene_annotations()
        rows = []
        for variant in variants.itertuples(index=False):
            chrom = str(variant.SNP).split("_", maxsplit=1)[0]
            pos = int(variant.POS_x)
            for gene in map_variant_to_gene(genes, chrom, pos).itertuples(index=False):
                rows.append(
                    {
                        "locus": variant.locus,
                        "SNP": variant.SNP,
                        "CHR": chrom,
                        "POS": pos,
                        "PIP": variant.PIP,
                        "CS": variant.CS,
                        "gene_id": gene.gene_id,
                        "gene_name": gene.gene_name,
                        "gene_biotype": gene.gene_biotype,
                        "relation": gene.relation,
                        "distance_bp": gene.distance_bp,
                    }
                )
        mapping = pd.DataFrame(rows, columns=GENE_MAPPING_COLUMNS)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    temporary_file = GENE_MAPPING_FILE.with_suffix(".tsv.tmp")
    try:
        mapping.to_csv(temporary_file, sep="\t", index=False)
        temporary_file.replace(GENE_MAPPING_FILE)
    finally:
        temporary_file.unlink(missing_ok=True)

    print("\nFine-mapped variant -> gene mapping")
    print(mapping.to_string(index=False))
    print(f"Saved gene mapping: {GENE_MAPPING_FILE}")
    return mapping


def run_gene_mapping_stage() -> pd.DataFrame:
    output_files = sorted(
        path for path in SUSIE_RESULTS_DIR.glob("*_susie.tsv") if path.is_file()
    )
    if not output_files:
        raise FileNotFoundError(f"No SuSiE result files found in {SUSIE_RESULTS_DIR}")
    return map_finemap_genes(output_files)


ENHANCER_MAPPING_COLUMNS = [
    "locus", "SNP", "CHR", "POS", "PIP", "CS",
    "enhancer_tissue", "enh_start", "enh_end", "enh_score",
]


def resolve_enhancer_dir() -> Path | None:
    for candidate in (ENHANCER_LOCAL_DIR, ENHANCER_ATLAS_DIR):
        if candidate is not None and candidate.is_dir() and any(candidate.glob("*.bed")):
            return candidate
    return None


def load_enhancer_atlas(enhancer_dir: Path) -> dict[str, pd.DataFrame]:
    atlas = {}
    for bed_file in sorted(enhancer_dir.glob("*.bed")):
        enhancers = pd.read_csv(
            bed_file,
            sep="\t",
            header=None,
            names=["chrom", "start", "end", "score"],
            low_memory=False,
        )
        enhancers["start"] = pd.to_numeric(enhancers["start"], errors="coerce")
        enhancers["end"] = pd.to_numeric(enhancers["end"], errors="coerce")
        enhancers = enhancers.dropna(subset=["start", "end"])
        enhancers[["start", "end"]] = enhancers[["start", "end"]].astype(int)
        atlas[bed_file.stem] = enhancers
    if not atlas:
        raise ValueError(f"No enhancer BED files parsed from {enhancer_dir}")
    print(f"Loaded {len(atlas)} enhancer tissue tracks from {enhancer_dir}")
    return atlas


def map_variant_to_enhancers(
    atlas: dict[str, pd.DataFrame], chrom: str, pos: int
) -> list[dict]:
    ucsc_chrom = chrom if chrom.startswith("chr") else f"chr{chrom}"
    hits = []
    for tissue, enhancers in atlas.items():
        overlapping = enhancers.loc[
            (enhancers["chrom"] == ucsc_chrom)
            & (enhancers["start"] <= pos)
            & (enhancers["end"] >= pos)
        ]
        for enhancer in overlapping.itertuples(index=False):
            hits.append(
                {
                    "enhancer_tissue": tissue,
                    "enh_start": int(enhancer.start),
                    "enh_end": int(enhancer.end),
                    "enh_score": round(float(enhancer.score), 4),
                }
            )
    return hits


def map_finemap_enhancers(output_files: list[Path], enhancer_dir: Path) -> pd.DataFrame:
    variants = select_credible_set_variants(output_files)
    if variants.empty:
        print("No credible-set variants to overlap with enhancers")
        return pd.DataFrame(columns=ENHANCER_MAPPING_COLUMNS)

    atlas = load_enhancer_atlas(enhancer_dir)
    rows = []
    for variant in variants.itertuples(index=False):
        chrom = str(variant.SNP).split("_", maxsplit=1)[0]
        pos = int(variant.POS_x)
        hits = map_variant_to_enhancers(atlas, chrom, pos)
        base = {
            "locus": variant.locus,
            "SNP": variant.SNP,
            "CHR": chrom,
            "POS": pos,
            "PIP": variant.PIP,
            "CS": variant.CS,
        }
        if hits:
            for hit in hits:
                rows.append({**base, **hit})
        else:
            rows.append(
                {
                    **base,
                    "enhancer_tissue": "-NONE-",
                    "enh_start": pd.NA,
                    "enh_end": pd.NA,
                    "enh_score": pd.NA,
                }
            )

    mapping = pd.DataFrame(rows, columns=ENHANCER_MAPPING_COLUMNS)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    temporary_file = ENHANCER_MAPPING_FILE.with_suffix(".tsv.tmp")
    try:
        mapping.to_csv(temporary_file, sep="\t", index=False)
        temporary_file.replace(ENHANCER_MAPPING_FILE)
    finally:
        temporary_file.unlink(missing_ok=True)

    overlap_count = int((mapping["enhancer_tissue"] != "-NONE-").sum())
    variants_in_enhancers = mapping.loc[
        mapping["enhancer_tissue"] != "-NONE-", "SNP"
    ].nunique()
    print("\nFine-mapped variant -> enhancer overlap")
    print(mapping.to_string(index=False))
    print(
        f"{variants_in_enhancers} of {len(variants)} variants overlap an enhancer "
        f"({overlap_count} variant-tissue overlaps)"
    )
    print(f"Saved enhancer overlap: {ENHANCER_MAPPING_FILE}")
    return mapping


def run_enhancer_mapping_stage() -> pd.DataFrame | None:
    output_files = sorted(
        path for path in SUSIE_RESULTS_DIR.glob("*_susie.tsv") if path.is_file()
    )
    if not output_files:
        raise FileNotFoundError(f"No SuSiE result files found in {SUSIE_RESULTS_DIR}")

    enhancer_dir = resolve_enhancer_dir()
    if enhancer_dir is None:
        raise FileNotFoundError(
            f"No EnhancerAtlas BED files found in {ENHANCER_LOCAL_DIR} or "
            f"{ENHANCER_ATLAS_DIR}; pass --enhancer-dir on another server"
        )
    return map_finemap_enhancers(output_files, enhancer_dir)


def _file_digest(path: Path, algorithm: str) -> str:
    digest = hashlib.new(algorithm)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _download_verified(url: str, destination: Path, digest: str, algorithm: str) -> None:
    """Cache a source file only after its published/local checksum matches."""
    if destination.is_file():
        if _file_digest(destination, algorithm) != digest:
            raise ValueError(f"Cached source has the wrong {algorithm} checksum: {destination}")
        print(f"Using verified source: {destination}")
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix=destination.name + ".", suffix=".tmp",
                                     dir=destination.parent, delete=False) as temporary:
        temporary_path = Path(temporary.name)
        try:
            print(f"Downloading {url} -> {destination}", flush=True)
            with urllib.request.urlopen(url, timeout=120) as response:
                shutil.copyfileobj(response, temporary, length=1024 * 1024)
        except BaseException:
            temporary_path.unlink(missing_ok=True)
            raise
    try:
        actual = _file_digest(temporary_path, algorithm)
        if actual != digest:
            raise ValueError(f"{algorithm} mismatch for {url}: expected {digest}, got {actual}")
        temporary_path.replace(destination)
    finally:
        temporary_path.unlink(missing_ok=True)


def _verify_phenotype_source(path: Path) -> None:
    digest = hashlib.sha256()
    with gzip.open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != PHENOTYPE_SHA256:
        raise ValueError(f"Female lifespan source changed or is corrupt: {path}")


def _plink_trio_exists(prefix: Path) -> bool:
    return all(Path(f"{prefix}{extension}").is_file() for extension in (".bed", ".bim", ".fam"))


def _run_checked(command: list[str]) -> None:
    print("Running:", shlex.join(command), flush=True)
    subprocess.run(command, check=True)


def prepare_phenotype(force: bool = False) -> None:
    """Convert Ivanov female mean lifespan to PLINK phenotype and keep files."""
    if PHENO_FILE.is_file() and ANALYSIS_LINES_FILE.is_file() and not force:
        print(f"Using prepared phenotype: {PHENO_FILE}")
        return
    if not RAW_PHENO_FILE.is_file():
        RAW_PHENO_FILE.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(prefix="female_lifespan.", suffix=".tmp",
                                         dir=RAW_PHENO_FILE.parent, delete=False) as temporary:
            temporary_path = Path(temporary.name)
            try:
                with urllib.request.urlopen(PHENOTYPE_URL, timeout=120) as response:
                    shutil.copyfileobj(response, temporary)
            except BaseException:
                temporary_path.unlink(missing_ok=True)
                raise
        try:
            _verify_phenotype_source(temporary_path)
            temporary_path.replace(RAW_PHENO_FILE)
        finally:
            temporary_path.unlink(missing_ok=True)
    _verify_phenotype_source(RAW_PHENO_FILE)
    source = pd.read_csv(RAW_PHENO_FILE, sep="\t", compression="gzip")
    required = {"DGRP", "sex", "mn_Lifespan"}
    if not required.issubset(source.columns):
        raise ValueError(f"Phenotype source lacks columns: {required - set(source.columns)}")
    female = source.loc[source["sex"].eq("F"), ["DGRP", "mn_Lifespan"]].copy()
    female["IID"] = female["DGRP"].str.extract(r"^DGRP_(\d+)$", expand=False)
    female[PHENO_NAME] = pd.to_numeric(female["mn_Lifespan"], errors="coerce")
    if female["IID"].isna().any() or female[PHENO_NAME].isna().any():
        raise ValueError("Malformed female lifespan line IDs or missing mean lifespan")
    female["IID"] = female["IID"].astype(int).astype(str)
    if female["IID"].duplicated().any():
        raise ValueError("Duplicate DGRP female line IDs")
    fam = pd.read_csv(Path(f"{RAW_GENOTYPE_PREFIX}.fam"), sep=r"\s+", header=None,
                      usecols=[0, 1], names=["FID", "IID"], dtype=str)
    available = set(zip(fam["FID"], fam["IID"]))
    female.insert(1, "FID", "line")
    female = female.loc[[key in available for key in zip(female["FID"], female["IID"])]]
    if len(female) != 197:
        raise ValueError(f"Expected 197 female lines with genotypes; found {len(female)}")
    PHENO_FILE.parent.mkdir(parents=True, exist_ok=True)
    ANALYSIS_LINES_FILE.parent.mkdir(parents=True, exist_ok=True)
    phenotype = female[["FID", "IID", PHENO_NAME]]
    for destination, frame, header in (
        (PHENO_FILE, phenotype, True),
        (ANALYSIS_LINES_FILE, phenotype[["FID", "IID"]], False),
    ):
        temporary = destination.with_name(destination.name + ".tmp")
        frame.to_csv(temporary, sep="\t", header=header, index=False)
        temporary.replace(destination)
    print(f"Prepared {len(phenotype)} female lines: {PHENO_FILE}")


def prepare_genotypes(plink2_bin: str, force: bool = False) -> None:
    """Split the Zenodo dm6 PLINK panel into chromosome arms."""
    if all(_plink_trio_exists(REFERENCE_DIR / f"DGRP.{chrom}") for chrom in FLY_CHROMS) and not force:
        print(f"Using chromosome-arm genotypes in {REFERENCE_DIR}")
        return
    for extension, digest in GENOTYPE_MD5.items():
        _download_verified(GENOTYPE_URL_PREFIX + extension + "?download=1",
                           Path(f"{RAW_GENOTYPE_PREFIX}{extension}"), digest, "md5")
    for chrom in FLY_CHROMS:
        prefix = REFERENCE_DIR / f"DGRP.{chrom}"
        if _plink_trio_exists(prefix) and not force:
            continue
        source_chrom = SOURCE_CHROM_MAP[chrom]
        _run_checked([plink2_bin, "--bfile", str(RAW_GENOTYPE_PREFIX), "--chr", source_chrom,
                      "--allow-extra-chr", "--make-bed", "--out", str(prefix)])
        if not _plink_trio_exists(prefix):
            raise RuntimeError(f"PLINK did not create genotype files for {chrom}: {prefix}")
        bim = Path(f"{prefix}.bim")
        temporary_bim = Path(f"{bim}.tmp")
        try:
            with bim.open() as source, temporary_bim.open("w") as destination:
                for line in source:
                    source_label, rest = line.split("\t", 1)
                    if source_label not in (source_chrom, chrom):
                        raise ValueError(f"Unexpected chromosome {source_label} in {bim}")
                    destination.write(f"{chrom}\t{rest}")
            temporary_bim.replace(bim)
        finally:
            temporary_bim.unlink(missing_ok=True)


def prepare_qc_and_pca(plink_bin: str, plink2_bin: str, force: bool = False) -> None:
    """Match 197 lines, QC each arm, merge, prune LD, and calculate 10 PCs."""
    QC_GENOTYPE_DIR.mkdir(parents=True, exist_ok=True)
    for chrom in FLY_CHROMS:
        output = QC_GENOTYPE_DIR / chrom
        if _plink_trio_exists(output) and not force:
            continue
        _run_checked([plink2_bin, "--bfile", str(REFERENCE_DIR / f"DGRP.{chrom}"),
                      "--keep", str(ANALYSIS_LINES_FILE), "--maf", "0.01",
                      "--geno", "0.05", "--allow-extra-chr", "--make-bed",
                      "--out", str(output)])
        if not _plink_trio_exists(output):
            raise RuntimeError(f"PLINK did not create QC files for {chrom}: {output}")
    if not _plink_trio_exists(MERGED_QC_SOURCE) or force:
        merge_list = GLM_DIR / "pca_merge_list.txt"
        merge_list.parent.mkdir(parents=True, exist_ok=True)
        merge_list.write_text("".join(
            f"{QC_GENOTYPE_DIR / chrom}.bed {QC_GENOTYPE_DIR / chrom}.bim "
            f"{QC_GENOTYPE_DIR / chrom}.fam\n" for chrom in FLY_CHROMS[1:]
        ))
        _run_checked([plink_bin, "--bfile", str(QC_GENOTYPE_DIR / FLY_CHROMS[0]),
                      "--merge-list", str(merge_list), "--allow-extra-chr", "--make-bed",
                      "--out", str(MERGED_QC_SOURCE)])
        if not _plink_trio_exists(MERGED_QC_SOURCE):
            raise RuntimeError(f"PLINK did not create merged reference: {MERGED_QC_SOURCE}")
    prune_prefix = GLM_DIR / "pca_prune"
    prune_file = prune_prefix.with_suffix(".prune.in")
    if not prune_file.is_file() or force:
        _run_checked([plink2_bin, "--bfile", str(MERGED_QC_SOURCE), "--allow-extra-chr",
                      "--indep-pairwise", "200kb", "1", "0.2", "--out", str(prune_prefix)])
    if not EIGENVEC_FILE.is_file() or force:
        pca_prefix = GLM_DIR / "dgrp_pca"
        _run_checked([plink2_bin, "--bfile", str(MERGED_QC_SOURCE),
                      "--extract", str(prune_file), "--pca", "10",
                      "--out", str(pca_prefix)])
        generated = pca_prefix.with_suffix(".eigenvec")
        if generated != EIGENVEC_FILE:
            shutil.copy2(generated, EIGENVEC_FILE)
    if not EIGENVEC_FILE.is_file():
        raise RuntimeError(f"PLINK did not create PCA covariates: {EIGENVEC_FILE}")
    print(f"QC and PCA complete: {EIGENVEC_FILE}")


def run_prepare_stage(plink_bin: str = PLINK_BIN, plink2_bin: str = PLINK2_BIN,
                      force: bool = False) -> None:
    plink = shutil.which(str(Path(plink_bin).expanduser()))
    plink2 = shutil.which(str(Path(plink2_bin).expanduser()))
    if not plink or not plink2:
        raise FileNotFoundError("Preparation requires PLINK 1.9 and PLINK 2.0; "
                                "pass --plink-bin and --plink2-bin if needed")
    prepare_genotypes(plink2, force=force)
    prepare_phenotype(force=force)
    prepare_qc_and_pca(plink, plink2, force=force)
    if not GTF_FILE.is_file():
        _download_verified(GTF_URL, GTF_FILE, GTF_SHA256, "sha256")


def run_phenotype_plots_stage() -> list[Path]:
    """Plot the prepared line means and reported per-line fly summaries."""
    if not PHENO_FILE.is_file() or not RAW_PHENO_FILE.is_file():
        raise FileNotFoundError("Phenotype inputs are missing; run --stage prepare first")

    from matplotlib.figure import Figure

    phenotype = pd.read_csv(PHENO_FILE, sep=r"\s+")
    if not {"IID", PHENO_NAME}.issubset(phenotype.columns):
        raise ValueError(f"Missing IID or {PHENO_NAME} in {PHENO_FILE}")
    line_ids = pd.to_numeric(phenotype["IID"], errors="raise").astype(int)
    lifespan = pd.to_numeric(phenotype[PHENO_NAME], errors="raise").to_numpy()
    if len(lifespan) < 2 or not np.isfinite(lifespan).all():
        raise ValueError("At least two finite line-level lifespan values are required")
    if line_ids.duplicated().any():
        raise ValueError("Duplicate DGRP line IDs in prepared phenotype")

    plot_dir = OUT_DIR / "phenotype_plots"
    plot_dir.mkdir(parents=True, exist_ok=True)
    distribution_file = plot_dir / "lifespan_distribution.png"
    figure = Figure(figsize=(8, 5), dpi=180)
    ax = figure.subplots()
    ax.hist(lifespan, bins="auto", color="#4978a5", edgecolor="white")
    ax.axvline(np.mean(lifespan), color="#a84332", linewidth=2,
               label=f"Mean: {np.mean(lifespan):.1f} days")
    ax.axvline(np.median(lifespan), color="#345a35", linestyle="--", linewidth=2,
               label=f"Median: {np.median(lifespan):.1f} days")
    ax.set(title=f"Female lifespan across {len(lifespan)} DGRP lines",
           xlabel="Mean lifespan per line (days)", ylabel="Number of lines")
    ax.text(0.98, 0.96,
            f"Between-line sample variance: {np.var(lifespan, ddof=1):.1f} days²",
            transform=ax.transAxes, ha="right", va="top",
            bbox={"facecolor": "white", "alpha": 0.9, "edgecolor": "none"})
    ax.legend(loc="upper left")
    figure.tight_layout()
    figure.savefig(distribution_file)

    source = pd.read_csv(RAW_PHENO_FILE, sep="\t", compression="gzip")
    required = {"DGRP", "sex", "NumberOfFlies", "sd_Lifespan"}
    if not required.issubset(source.columns):
        raise ValueError(f"Missing source columns: {required - set(source.columns)}")
    source = source.loc[source["sex"].eq("F")].copy()
    source["IID"] = pd.to_numeric(
        source["DGRP"].str.extract(r"^DGRP_(\d+)$", expand=False), errors="raise"
    ).astype(int)
    if source["IID"].duplicated().any():
        raise ValueError("Duplicate DGRP line IDs in source phenotype")
    source = source.set_index("IID").reindex(line_ids)
    if source["DGRP"].isna().any():
        raise ValueError("Prepared phenotype contains lines absent from the source table")

    counts = pd.to_numeric(source["NumberOfFlies"], errors="coerce").dropna()
    standard_deviation = pd.to_numeric(source["sd_Lifespan"], errors="coerce").dropna()
    if counts.empty or standard_deviation.empty:
        raise ValueError("No reported fly counts or lifespan standard deviations")
    if (counts <= 0).any() or (standard_deviation < 0).any():
        raise ValueError("Invalid fly count or lifespan standard deviation")
    variance = standard_deviation.pow(2)

    sample_file = plot_dir / "fly_counts_and_variance.png"
    figure = Figure(figsize=(11, 4.8), dpi=180)
    count_ax, variance_ax = figure.subplots(1, 2)
    bins = np.arange(int(counts.min()), int(counts.max()) + 2) - 0.5
    count_ax.hist(counts, bins=bins, color="#4978a5", edgecolor="white")
    count_ax.axvline(counts.median(), color="#a84332", linestyle="--",
                     label=f"Median: {counts.median():.0f} flies")
    count_ax.set(title=f"Fly counts reported for {len(counts)}/{len(line_ids)} lines",
                 xlabel="Flies measured per line", ylabel="Number of lines")
    count_ax.legend()
    variance_ax.hist(variance, bins="auto", color="#6d9a75", edgecolor="white")
    variance_ax.axvline(variance.median(), color="#a84332", linestyle="--",
                        label=f"Median: {variance.median():.1f} days²")
    variance_ax.set(title=f"Within-line variance reported for {len(variance)}/{len(line_ids)} lines",
                    xlabel="Within-line lifespan variance (days²)", ylabel="Number of lines")
    variance_ax.legend()
    figure.text(0.5, 0.01,
                "The source reports summaries per line; individual fly ages are unavailable.",
                ha="center", fontsize=9)
    figure.tight_layout(rect=(0, 0.04, 1, 1))
    figure.savefig(sample_file)

    outputs = [distribution_file, sample_file]
    for output in outputs:
        print(f"Saved phenotype plot: {output}")
    return outputs


def validate_gwas_sample_alignment() -> int:
    phenotype = pd.read_csv(PHENO_FILE, sep=r"\s+", dtype=str)
    covariates = pd.read_csv(EIGENVEC_FILE, sep=r"\s+", dtype=str)
    covariates = covariates.rename(columns={"#FID": "FID"})
    pc_names = [f"PC{index}" for index in range(1, N_PCS + 1)]
    for label, frame, required in (
        ("phenotype", phenotype, {"FID", "IID", PHENO_NAME}),
        ("PCA covariates", covariates, {"FID", "IID", *pc_names}),
    ):
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"{label} is missing columns: {sorted(missing)}")
        if frame.empty or frame[["FID", "IID"]].isna().any().any():
            raise ValueError(f"{label} has missing sample identifiers")
        if frame.duplicated(["FID", "IID"]).any():
            raise ValueError(f"{label} has duplicate sample identifiers")

    phenotype_values = pd.to_numeric(phenotype[PHENO_NAME], errors="coerce")
    pc_values = covariates[pc_names].apply(pd.to_numeric, errors="coerce")
    if not np.isfinite(phenotype_values.to_numpy(dtype=float)).all():
        raise ValueError("Female lifespan phenotype has nonfinite values")
    if not np.isfinite(pc_values.to_numpy(dtype=float)).all():
        raise ValueError("PCA covariates have nonfinite PC1-PC4 values")

    phenotype_ids = pd.MultiIndex.from_frame(phenotype[["FID", "IID"]])
    covariate_ids = pd.MultiIndex.from_frame(covariates[["FID", "IID"]])
    missing_ids = phenotype_ids.difference(covariate_ids)
    if len(missing_ids):
        raise ValueError(f"{len(missing_ids)} phenotype lines are missing PCA covariates")
    print(f"Validated phenotype and PC1-PC{N_PCS} for {len(phenotype)} matching lines")
    return len(phenotype)


def validate_gwas_genotype_samples(bfile: Path, chrom: str) -> None:
    phenotype = pd.read_csv(PHENO_FILE, sep=r"\s+", usecols=["FID", "IID"], dtype=str)
    genotype = pd.read_csv(
        bfile.with_suffix(".fam"), sep=r"\s+", header=None,
        usecols=[0, 1], names=["FID", "IID"], dtype=str,
    )
    if genotype.empty or genotype[["FID", "IID"]].isna().any().any():
        raise ValueError(f"{chrom} genotype sample identifiers are missing")
    if genotype.duplicated(["FID", "IID"]).any():
        raise ValueError(f"{chrom} genotype sample identifiers are duplicated")
    phenotype_ids = pd.MultiIndex.from_frame(phenotype)
    genotype_ids = pd.MultiIndex.from_frame(genotype)
    missing = phenotype_ids.difference(genotype_ids)
    extra = genotype_ids.difference(phenotype_ids)
    if len(missing) or len(extra):
        raise ValueError(
            f"{chrom} QC genotypes do not match phenotype lines: "
            f"{len(missing)} missing, {len(extra)} extra"
        )


def gwas_output_is_current(output_file: Path, bfile: Path) -> bool:
    if not output_file.is_file():
        return False
    inputs = (PHENO_FILE, EIGENVEC_FILE, *(bfile.with_suffix(ext) for ext in (".bed", ".bim", ".fam")))
    return output_file.stat().st_mtime_ns >= max(path.stat().st_mtime_ns for path in inputs)


def run_gwas_stage(plink2_bin: str = PLINK2_BIN, force: bool = False) -> None:
    plink2_command = str(Path(plink2_bin).expanduser())
    plink2_executable = shutil.which(plink2_command)
    if plink2_executable is None:
        raise FileNotFoundError(
            f"Could not execute {plink2_bin!r}; add plink2 to PATH or pass --plink2-bin"
        )
    if not PHENO_FILE.is_file():
        raise FileNotFoundError(f"Missing phenotype file: {PHENO_FILE}")
    if not EIGENVEC_FILE.is_file():
        raise FileNotFoundError(f"Missing PCA eigenvector file: {EIGENVEC_FILE}")

    validate_gwas_sample_alignment()
    covar_names = f"PC1-PC{N_PCS}"
    print(
        f"Running per-chromosome GWAS with the first {N_PCS} principal components "
        f"(PC1-PC{N_PCS}) as covariates"
    )

    for chrom in FLY_CHROMS:
        bfile = QC_GENOTYPE_DIR / chrom
        output_prefix = GLM_DIR / f"lifespan_female_{chrom}"
        output_file = GLM_DIR / f"lifespan_female_{chrom}.{PHENO_NAME}.glm.linear"
        missing = [bfile.with_suffix(ext) for ext in (".bed", ".bim", ".fam")
                   if not bfile.with_suffix(ext).is_file()]
        if missing:
            missing_list = "\n".join(f"    {path}" for path in missing)
            raise FileNotFoundError(f"Missing QC'd genotype files for {chrom}:\n{missing_list}")
        validate_gwas_genotype_samples(bfile, chrom)
        if gwas_output_is_current(output_file, bfile) and not force:
            print(f"  {chrom}: using existing GWAS output")
            continue
        if output_file.is_file() and not force:
            print(f"  {chrom}: rerunning GWAS because an input is newer than the output")

        command = [
            plink2_executable,
            "--bfile", str(bfile),
            "--pheno", str(PHENO_FILE),
            "--pheno-name", PHENO_NAME,
            "--covar", str(EIGENVEC_FILE),
            "--covar-name", covar_names,
            "--glm", "hide-covar",
            "--out", str(output_prefix),
            "--no-psam-pheno",
            "--allow-extra-chr",
        ]
        print("Running:", shlex.join(command), flush=True)
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL)

        if not output_file.is_file():
            raise RuntimeError(f"plink2 did not create the expected GWAS output: {output_file}")
        n_snps = sum(1 for _ in open(output_file)) - 1
        print(f"  {chrom}: {n_snps:,} SNPs tested")

    save_gwas_sumstats()
    print(f"GWAS complete: {N_PCS}-PC association results in {GLM_DIR}")


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
        description="Run the Drosophila female lifespan fine-mapping pipeline."
    )
    parser.add_argument(
        "--stage",
        choices=("prepare", "plots", "gwas", "cojo", "susie", "genes", "enhancers", "all"),
        default="all",
        help="Pipeline stage to run (default: all)",
    )
    parser.add_argument(
        "--gcta-bin",
        default=GCTA_BIN,
        help="GCTA executable name or path (default: gcta64)",
    )
    parser.add_argument(
        "--plink-bin",
        default=PLINK_BIN,
        help="PLINK executable name or path (default: plink)",
    )
    parser.add_argument(
        "--plink2-bin",
        default=PLINK2_BIN,
        help="PLINK2 executable name or path (default: plink2)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate cached outputs for the selected stage or stages",
    )
    parser.add_argument("--base-dir", help="Project root; all default input/output paths derive from it")
    parser.add_argument("--glm-dir", help="Directory holding GWAS/PLINK working files (default: <base>/data/gwas/tmp)")
    parser.add_argument("--pheno", help="Phenotype file (default: <base>/data/gwas/lifespan_female.pheno)")
    parser.add_argument("--qc-dir", help="Directory of per-chromosome QC'd PLINK genotypes (default: <glm-dir>/qc)")
    parser.add_argument("--eigenvec", help="PCA eigenvector file (default: <glm-dir>/dgrp_pca.eigenvec)")
    parser.add_argument("--bfile", help="Merged QC'd PLINK prefix for the LD reference (default: <glm-dir>/merged_qc)")
    parser.add_argument("--gtf", help="Gene annotation GTF (default: <base>/data/genes/...BDGP6.54.62.chr.gtf.gz)")
    parser.add_argument("--enhancer-dir", help="EnhancerAtlas BED directory (default: <base>/data/enhancers/dm, then BioCypher fly tracks)")
    parser.add_argument("--out-dir", help="Output directory for results (default: <base>/data/finemap/female)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    configure_paths(
        base_dir=args.base_dir,
        glm_dir=args.glm_dir,
        pheno=args.pheno,
        qc_dir=args.qc_dir,
        eigenvec=args.eigenvec,
        bfile=args.bfile,
        out_dir=args.out_dir,
        gtf=args.gtf,
        enhancer_dir=args.enhancer_dir,
    )
    if args.stage in ("prepare", "gwas", "all"):
        run_prepare_stage(plink_bin=args.plink_bin, plink2_bin=args.plink2_bin,
                          force=args.force)
    if args.stage in ("plots", "all"):
        run_phenotype_plots_stage()
    if args.stage in ("gwas", "all"):
        run_gwas_stage(plink2_bin=args.plink2_bin, force=args.force)
    if args.stage in ("cojo", "all"):
        run_cojo_stage(gcta_bin=args.gcta_bin, force=args.force)
    if args.stage in ("susie", "all"):
        run_susie_finemapping(plink_bin=args.plink_bin, force=args.force)
    if args.stage in ("genes", "all"):
        run_gene_mapping_stage()
    if args.stage in ("enhancers", "all"):
        run_enhancer_mapping_stage()


if __name__ == "__main__":
    main()
