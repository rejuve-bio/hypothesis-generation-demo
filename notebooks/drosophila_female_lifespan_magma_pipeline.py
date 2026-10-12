#!/usr/bin/env python3

import argparse
import errno
import gzip
import math
import os
from pathlib import Path
import re
import shutil
import subprocess


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SUMSTATS = BASE_DIR / "data" / "gwas" / "tmp" / "lifespan_female_gwas_sumstats.tsv"
DEFAULT_GTF = BASE_DIR / "data" / "genes" / "Drosophila_melanogaster.BDGP6.54.62.chr.gtf.gz"
DEFAULT_BFILE = BASE_DIR / "data" / "gwas" / "tmp" / "merged_qc"
DEFAULT_MAGMA_BIN = BASE_DIR / "tools" / "magma" / "magma"
DEFAULT_OUT_DIR = BASE_DIR / "results" / "magma_female"
CHROM_MAP = {"2L": "1", "2R": "2", "3L": "3", "3R": "4", "4": "5", "X": "X", "23": "X"}
GENE_ID_PATTERN = re.compile(r'gene_id "([^"]+)"')


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run MAGMA gene and gene-set analysis for DGRP female lifespan."
    )
    parser.add_argument("--sumstats", type=Path, default=DEFAULT_SUMSTATS)
    parser.add_argument("--gtf", type=Path, default=DEFAULT_GTF)
    parser.add_argument("--bfile", type=Path, default=DEFAULT_BFILE)
    parser.add_argument("--magma-bin", type=Path, default=DEFAULT_MAGMA_BIN)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    return parser.parse_args(argv)


def validate_inputs(args: argparse.Namespace) -> None:
    required_files = [
        args.sumstats,
        args.gtf,
        args.magma_bin,
        *(Path(f"{args.bfile}{extension}") for extension in (".bed", ".bim", ".fam")),
    ]
    missing = [path for path in required_files if not path.is_file()]
    if missing:
        raise FileNotFoundError(
            "Missing MAGMA inputs:\n" + "\n".join(str(path) for path in missing)
        )
    if not os.access(args.magma_bin, os.X_OK):
        raise PermissionError(f"MAGMA binary is not executable: {args.magma_bin}")

    opener = gzip.open if args.sumstats.suffix == ".gz" else open
    with opener(args.sumstats, "rt", encoding="utf-8") as source:
        columns = source.readline().split()
        required_columns = {"CHR", "POS", "SNP", "p", "N"}
        missing_columns = required_columns - set(columns)
        if missing_columns:
            raise ValueError(
                f"GWAS summary statistics are missing columns: {sorted(missing_columns)}"
            )
        if not any(line.strip() for line in source):
            raise ValueError(f"GWAS summary statistics contain no variants: {args.sumstats}")

    print(f"Validated MAGMA inputs: {args.sumstats}")


def write_gene_locations(gtf_path: Path, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(f"{output_path.name}.tmp")
    count = 0
    try:
        opener = gzip.open if gtf_path.suffix == ".gz" else open
        with opener(gtf_path, "rt", encoding="utf-8") as source, temporary_path.open(
            "w", encoding="utf-8"
        ) as destination:
            for line in source:
                if line.startswith("#"):
                    continue
                fields = line.rstrip("\n").split("\t")
                if len(fields) != 9 or fields[2] != "gene":
                    continue
                chrom = fields[0].removeprefix("chr")
                if chrom not in CHROM_MAP:
                    continue
                match = GENE_ID_PATTERN.search(fields[8])
                if match is None:
                    raise ValueError(f"Gene record has no gene_id: {line.rstrip()}")
                start, end = int(fields[3]), int(fields[4])
                if start < 1 or end < start or fields[6] not in {"+", "-"}:
                    raise ValueError(f"Invalid gene coordinates or strand: {line.rstrip()}")
                destination.write(
                    f"{match.group(1)}\t{CHROM_MAP[chrom]}\t{start}\t{end}\t{fields[6]}\n"
                )
                count += 1
        if count == 0:
            raise ValueError(f"No Drosophila gene records found in {gtf_path}")
        temporary_path.replace(output_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    print(f"Wrote {count} gene locations: {output_path}")
    return output_path


def write_snp_locations(bfile: Path, output_path: Path) -> Path:
    bim_path = Path(f"{bfile}.bim")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(f"{output_path.name}.tmp")
    count = 0
    try:
        with bim_path.open("r", encoding="utf-8") as source, temporary_path.open(
            "w", encoding="utf-8"
        ) as destination:
            for line_number, line in enumerate(source, start=1):
                fields = line.split()
                if len(fields) != 6:
                    raise ValueError(f"Invalid BIM record at {bim_path}:{line_number}")
                chrom = fields[0].removeprefix("chr")
                chrom = CHROM_MAP.get(chrom, chrom)
                if chrom not in CHROM_MAP.values():
                    raise ValueError(f"Unsupported chromosome at {bim_path}:{line_number}")
                position = int(fields[3])
                if position < 1 or fields[1] in {"", "."}:
                    raise ValueError(f"Invalid SNP ID or position at {bim_path}:{line_number}")
                destination.write(f"{fields[1]}\t{chrom}\t{position}\n")
                count += 1
        if count == 0:
            raise ValueError(f"No SNP records found in {bim_path}")
        temporary_path.replace(output_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    print(f"Wrote {count} SNP locations: {output_path}")
    return output_path


def write_magma_pvalues(sumstats_path: Path, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(f"{output_path.name}.tmp")
    count = 0
    try:
        opener = gzip.open if sumstats_path.suffix == ".gz" else open
        with opener(sumstats_path, "rt", encoding="utf-8") as source, temporary_path.open(
            "w", encoding="utf-8"
        ) as destination:
            columns = source.readline().split()
            missing = {"SNP", "p", "N"} - set(columns)
            if missing:
                raise ValueError(f"GWAS statistics are missing columns: {sorted(missing)}")
            indices = [columns.index(name) for name in ("SNP", "p", "N")]
            destination.write("SNP\tP\tN\n")
            for line_number, line in enumerate(source, start=2):
                if not line.strip():
                    continue
                fields = line.split()
                if len(fields) != len(columns):
                    raise ValueError(f"Invalid GWAS row at {sumstats_path}:{line_number}")
                snp, raw_p, raw_n = (fields[index] for index in indices)
                p_value = float(raw_p)
                sample_size = float(raw_n)
                if (
                    snp == "."
                    or not math.isfinite(p_value)
                    or not 0 <= p_value <= 1
                    or not math.isfinite(sample_size)
                    or sample_size <= 0
                    or not sample_size.is_integer()
                ):
                    raise ValueError(f"Invalid SNP, p-value, or N at {sumstats_path}:{line_number}")
                destination.write(f"{snp}\t{max(p_value, 1e-300):.17g}\t{int(sample_size)}\n")
                count += 1
        if count == 0:
            raise ValueError(f"No GWAS variants found in {sumstats_path}")
        temporary_path.replace(output_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    print(f"Wrote {count} MAGMA p-values: {output_path}")
    return output_path


def run_annotation(
    magma_bin: Path, snp_locations: Path, gene_locations: Path, output_prefix: Path
) -> Path:
    command = [
        str(magma_bin),
        "--annotate",
        "nonhuman",
        "--snp-loc",
        str(snp_locations),
        "--gene-loc",
        str(gene_locations),
        "--out",
        str(output_prefix),
    ]
    print("Running MAGMA SNP-to-gene annotation", flush=True)
    subprocess.run(command, check=True)
    output_path = Path(f"{output_prefix}.genes.annot")
    if not output_path.is_file() or output_path.stat().st_size == 0:
        raise RuntimeError(f"MAGMA did not produce a nonempty annotation: {output_path}")
    print(f"Saved MAGMA gene annotation: {output_path}")
    return output_path


def prepare_magma_bfile(source_prefix: Path, output_prefix: Path) -> Path:
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    for extension in (".bed", ".fam"):
        source = Path(f"{source_prefix}{extension}")
        destination = Path(f"{output_prefix}{extension}")
        temporary = Path(f"{destination}.tmp")
        try:
            temporary.unlink(missing_ok=True)
            try:
                os.link(source, temporary)
            except OSError as exc:
                if exc.errno != errno.EXDEV:
                    raise
                shutil.copy2(source, temporary)
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)

    source_bim = Path(f"{source_prefix}.bim")
    destination_bim = Path(f"{output_prefix}.bim")
    temporary_bim = Path(f"{destination_bim}.tmp")
    count = 0
    try:
        with source_bim.open("r", encoding="utf-8") as source, temporary_bim.open(
            "w", encoding="utf-8"
        ) as destination:
            for line_number, line in enumerate(source, start=1):
                fields = line.split()
                if len(fields) != 6:
                    raise ValueError(f"Invalid BIM record at {source_bim}:{line_number}")
                chromosome = fields[0].removeprefix("chr")
                chromosome = CHROM_MAP.get(chromosome, chromosome)
                if chromosome not in CHROM_MAP.values():
                    raise ValueError(f"Unsupported chromosome at {source_bim}:{line_number}")
                destination.write("\t".join((chromosome, *fields[1:])) + "\n")
                count += 1
        if count == 0:
            raise ValueError(f"No SNP records found in {source_bim}")
        temporary_bim.replace(destination_bim)
    finally:
        temporary_bim.unlink(missing_ok=True)
    print(f"Prepared MAGMA PLINK reference with {count} SNPs: {output_prefix}")
    return output_prefix


def run_gene_analysis(
    magma_bin: Path,
    bfile: Path,
    pvalues: Path,
    annotation: Path,
    output_prefix: Path,
) -> Path:
    command = [
        str(magma_bin),
        "--bfile",
        str(bfile),
        "--pval",
        str(pvalues),
        "ncol=N",
        "--gene-annot",
        str(annotation),
        "--out",
        str(output_prefix),
    ]
    print("Running MAGMA gene-based analysis", flush=True)
    subprocess.run(command, check=True)
    output_path = Path(f"{output_prefix}.genes.raw")
    if not output_path.is_file() or output_path.stat().st_size == 0:
        raise RuntimeError(f"MAGMA did not produce nonempty gene results: {output_path}")
    print(f"Saved MAGMA gene results: {output_path}")
    return output_path


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    validate_inputs(args)
    gene_locations = write_gene_locations(args.gtf, args.out_dir / "gene_loc.txt")
    snp_locations = write_snp_locations(args.bfile, args.out_dir / "snp_loc.txt")
    pvalues = write_magma_pvalues(args.sumstats, args.out_dir / "lifespan_female.magma.p.txt")
    annotation = run_annotation(
        args.magma_bin,
        snp_locations,
        gene_locations,
        args.out_dir / "dgrp_lifespan_female",
    )
    bfile = prepare_magma_bfile(args.bfile, args.out_dir / "merged_qc_magma")
    run_gene_analysis(
        args.magma_bin,
        bfile,
        pvalues,
        annotation,
        args.out_dir / "dgrp_lifespan_female_gene",
    )


if __name__ == "__main__":
    main()
