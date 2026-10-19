#!/usr/bin/env python3
# Interactive: marimo edit notebooks/drosophila_female_lifespan_magma_pipeline.py
# Command line: python notebooks/drosophila_female_lifespan_magma_pipeline.py --stage validate
import marimo

__generated_with = "0.17.8"
app = marimo.App(width="medium")


with app.setup(hide_code=True):
    import marimo as mo
    import argparse
    from collections import defaultdict
    import csv
    import errno
    import gzip
    import hashlib
    import json
    import math
    import os
    from pathlib import Path
    import platform
    import re
    import shutil
    import subprocess
    import sys
    import tempfile
    import urllib.error
    import urllib.request
    import zipfile


    BASE_DIR = Path(__file__).resolve().parents[1]
    DEFAULT_SUMSTATS = BASE_DIR / "data" / "gwas" / "tmp" / "lifespan_female_gwas_sumstats.tsv"
    DEFAULT_GTF = BASE_DIR / "data" / "genes" / "Drosophila_melanogaster.BDGP6.54.62.chr.gtf.gz"
    DEFAULT_BFILE = BASE_DIR / "data" / "gwas" / "tmp" / "merged_qc"
    DEFAULT_MAGMA_BIN = BASE_DIR / "tools" / "magma" / "magma"
    DEFAULT_OUT_DIR = BASE_DIR / "results" / "magma_female"
    MAGMA_VERSION = "1.10"
    MAGMA_ARCHIVE_URL = "https://vu.data.surf.nl/index.php/s/lxDgt2dNdNr6DYt/download"
    MAGMA_ARCHIVE_SHA256 = "d8b20778b773f47b4fb0f1020baefebc92b11ee65946e708a618d494d6819e39"
    GO_GAF_URL = "https://current.geneontology.org/annotations/gaf/DROME-mod.gaf.gz"
    GO_OBO_URL = "https://current.geneontology.org/ontology/go-basic.obo"
    CHROM_MAP = {"2L": "1", "2R": "2", "3L": "3", "3R": "4", "4": "5", "X": "X", "23": "X"}
    BFILE_EXTENSIONS = (".bed", ".bim", ".fam")
    GENE_ID_PATTERN = re.compile(r'gene_id "([^"]+)"')
    GENE_NAME_PATTERN = re.compile(r'gene_name "([^"]+)"')
    GENE_BIOTYPE_PATTERN = re.compile(r'gene_biotype "([^"]+)"')


    def default_magma_bin() -> Path:
        if DEFAULT_MAGMA_BIN.is_file() and os.access(DEFAULT_MAGMA_BIN, os.X_OK):
            return DEFAULT_MAGMA_BIN
        path_binary = shutil.which("magma")
        return Path(path_binary) if path_binary is not None else DEFAULT_MAGMA_BIN


    def install_magma(destination: Path = DEFAULT_MAGMA_BIN) -> Path:
        if platform.system() != "Linux" or platform.machine().lower() not in {"x86_64", "amd64"}:
            raise RuntimeError(
                "Automatic MAGMA installation supports Linux x86_64 only; "
                "pass a compatible executable with --magma-bin"
            )
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            with tempfile.TemporaryDirectory(prefix="magma-download-", dir=destination.parent) as temp:
                archive = Path(temp) / "magma.zip"
                digest = hashlib.sha256()
                with urllib.request.urlopen(MAGMA_ARCHIVE_URL, timeout=120) as response, archive.open(
                    "wb"
                ) as output:
                    for chunk in iter(lambda: response.read(1024 * 1024), b""):
                        output.write(chunk)
                        digest.update(chunk)
                if digest.hexdigest() != MAGMA_ARCHIVE_SHA256:
                    raise RuntimeError(
                        "Downloaded MAGMA archive failed SHA-256 verification: "
                        f"expected {MAGMA_ARCHIVE_SHA256}, found {digest.hexdigest()}"
                    )
                with zipfile.ZipFile(archive) as package, package.open("magma") as executable:
                    temporary_binary = Path(temp) / "magma"
                    with temporary_binary.open("wb") as output:
                        shutil.copyfileobj(executable, output)
                with temporary_binary.open("rb") as executable:
                    if executable.read(4) != b"\x7fELF":
                        raise RuntimeError("Downloaded MAGMA executable is not a Linux ELF binary")
                temporary_binary.chmod(0o755)
                temporary_binary.replace(destination)
        except (OSError, urllib.error.URLError, zipfile.BadZipFile, KeyError) as error:
            raise RuntimeError(
                f"Could not install MAGMA {MAGMA_VERSION}; pass a local executable with --magma-bin: "
                f"{error}"
            ) from error
        print(f"Installed MAGMA {MAGMA_VERSION}: {destination}")
        return destination


    def resolve_magma_binary(requested: Path) -> Path:
        if requested.is_file() and os.access(requested, os.X_OK):
            return requested
        if requested == DEFAULT_MAGMA_BIN and not requested.exists():
            return install_magma(requested)
        raise FileNotFoundError(f"MAGMA executable is missing or not executable: {requested}")


    def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
        parser = argparse.ArgumentParser(
            description="Run MAGMA gene and gene-set analysis for DGRP female lifespan."
        )
        parser.add_argument("--sumstats", type=Path, default=DEFAULT_SUMSTATS)
        parser.add_argument("--gtf", type=Path, default=DEFAULT_GTF)
        parser.add_argument("--bfile", type=Path, default=DEFAULT_BFILE)
        parser.add_argument("--magma-bin", type=Path, default=default_magma_bin())
        parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
        parser.add_argument(
            "--force", action="store_true", help="Rebuild MAGMA outputs regardless of file timestamps"
        )
        parser.add_argument(
            "--stage",
            choices=(
                "all", "inputs", "install", "validate", "prepare", "annotate", "genes", "sets", "report"
            ),
            default="all",
        )
        gene_set_source = parser.add_mutually_exclusive_group()
        gene_set_source.add_argument("--gene-sets", type=Path)
        gene_set_source.add_argument("--go-gaf", type=Path)
        gene_set_source.add_argument("--download-go", action="store_true")
        parser.add_argument("--go-obo", type=Path)
        args = parser.parse_args(argv)
        if args.go_gaf is not None and args.go_obo is None:
            parser.error("--go-gaf requires --go-obo")
        if args.go_obo is not None and args.go_gaf is None and args.stage != "report":
            parser.error("--go-gaf and --go-obo must be provided together")
        return args


    def validate_inputs(args: argparse.Namespace) -> None:
        required_files = [
            args.sumstats,
            args.gtf,
            *bfile_files(args.bfile),
        ]
        if args.gene_sets is not None:
            required_files.append(args.gene_sets)
        if args.go_gaf is not None:
            required_files.extend((args.go_gaf, args.go_obo))
        require_files(required_files)
        if args.stage != "prepare":
            args.magma_bin = resolve_magma_binary(args.magma_bin)

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


    def require_files(paths: list[Path]) -> None:
        missing = [path for path in paths if not path.is_file() or path.stat().st_size == 0]
        if missing:
            raise FileNotFoundError(
                "Missing or empty MAGMA inputs:\n" + "\n".join(str(path) for path in missing)
            )


    def bfile_files(prefix: Path) -> list[Path]:
        return [Path(f"{prefix}{extension}") for extension in BFILE_EXTENSIONS]


    def validate_individual_inputs(
        bfile: Path, phenotype: Path, trait: str, covariates: Path,
        covariate_names: list[str],
    ) -> dict:
        """Validate individual data without silently dropping phenotype samples."""
        require_files([*bfile_files(bfile), phenotype, covariates])
        if not trait or trait in {"FID", "IID"}:
            raise ValueError("Choose a phenotype column other than FID or IID")
        if (
            not covariate_names
            or len(set(covariate_names)) != len(covariate_names)
            or any(not name or name in {"FID", "IID"} for name in covariate_names)
        ):
            raise ValueError("Choose unique, nonempty covariate columns other than FID or IID")

        def read_samples(path: Path, value_columns: list[str] | None) -> dict:
            samples = {}
            with path.open(encoding="utf-8") as source:
                if value_columns is not None:
                    header = source.readline().split()
                    header = ["FID" if name == "#FID" else name for name in header]
                    if len(set(header)) != len(header):
                        raise ValueError(f"Duplicate column names in {path}")
                    missing = {"FID", "IID", *value_columns} - set(header)
                    if missing:
                        raise ValueError(f"Missing columns in {path}: {sorted(missing)}")
                else:
                    header = ["FID", "IID", "father", "mother", "sex", "phenotype"]
                for line_number, line in enumerate(source, start=2 if value_columns is not None else 1):
                    if not line.strip():
                        continue
                    fields = line.split()
                    if len(fields) != len(header):
                        raise ValueError(f"Invalid column count at {path}:{line_number}")
                    row = dict(zip(header, fields))
                    key = (row["FID"], row["IID"])
                    if any(value.upper() in {"", "NA", "NAN", ".", "-9"} for value in key):
                        raise ValueError(f"Missing sample ID at {path}:{line_number}")
                    if key in samples:
                        raise ValueError(f"Duplicate sample {key} in {path}")
                    values = []
                    for column in value_columns or []:
                        try:
                            value = float(row[column])
                        except ValueError as error:
                            raise ValueError(f"Nonnumeric {column} at {path}:{line_number}") from error
                        if not math.isfinite(value) or value == -9:
                            raise ValueError(f"Missing or nonfinite {column} at {path}:{line_number}")
                        values.append(value)
                    samples[key] = values
            if not samples:
                raise ValueError(f"No samples found in {path}")
            return samples

        genotype_rows = read_samples(Path(f"{bfile}.fam"), None)
        phenotype_rows = read_samples(phenotype, [trait])
        covariate_rows = read_samples(covariates, covariate_names)
        for label, rows in (("genotypes", genotype_rows), ("covariates", covariate_rows)):
            missing_ids = phenotype_rows.keys() - rows.keys()
            if missing_ids:
                raise ValueError(
                    f"{len(missing_ids)} phenotype samples missing from {label}; "
                    f"examples: {sorted(missing_ids)[:5]}"
                )
        # Preserve genotype order for downstream exports; do not rely on input row order.
        matched_ids = [key for key in genotype_rows if key in phenotype_rows]
        if len(matched_ids) <= len(covariate_names) + 2:
            raise ValueError("Too few matched samples for covariates and gene regression")
        if len({phenotype_rows[key][0] for key in matched_ids}) < 2:
            raise ValueError("The matched phenotype is constant")
        for index, name in enumerate(covariate_names):
            if len({covariate_rows[key][index] for key in matched_ids}) < 2:
                raise ValueError(f"Covariate {name} is constant in the matched samples")
        return {
            "sample_ids": matched_ids,
            "phenotype_values": [phenotype_rows[key][0] for key in matched_ids],
            "covariate_values": [covariate_rows[key] for key in matched_ids],
            "covariate_names": list(covariate_names),
            "genotype_samples": len(genotype_rows),
            "phenotype_samples": len(phenotype_rows),
            "covariate_samples": len(covariate_rows),
            "matched_samples": len(matched_ids),
            "genotype_samples_without_phenotype": len(genotype_rows.keys() - phenotype_rows.keys()),
        }


    def prepare_individual_files(
        bfile: Path, phenotype: Path, trait: str, covariates: Path,
        covariate_names: list[str], output_dir: Path,
    ) -> dict:
        """Revalidate sources and export matched MAGMA phenotype/covariate tables."""
        matched = validate_individual_inputs(bfile, phenotype, trait, covariates, covariate_names)
        paths = {
            "phenotype": output_dir / "individual.phenotype.tsv",
            "covariates": output_dir / "individual.covariates.tsv",
        }
        sources = {path.resolve() for path in [*bfile_files(bfile), phenotype, covariates]}
        if any(path.resolve() in sources for path in paths.values()):
            raise ValueError("Prepared output paths must not overwrite source inputs")
        output_dir.mkdir(parents=True, exist_ok=True)
        # Stage both complete tables before replacing any previous exports.
        with tempfile.TemporaryDirectory(prefix="individual-inputs-", dir=output_dir) as temp:
            staged = {key: Path(temp) / path.name for key, path in paths.items()}
            with staged["phenotype"].open("w", encoding="utf-8", newline="") as pheno_file, staged[
                "covariates"
            ].open("w", encoding="utf-8", newline="") as covar_file:
                pheno_writer = csv.writer(pheno_file, delimiter="\t", lineterminator="\n")
                covar_writer = csv.writer(covar_file, delimiter="\t", lineterminator="\n")
                pheno_writer.writerow(["FID", "IID", trait])
                covar_writer.writerow(["FID", "IID", *covariate_names])
                for key, value, covar_values in zip(
                    matched["sample_ids"], matched["phenotype_values"], matched["covariate_values"]
                ):
                    pheno_writer.writerow([*key, format(value, ".17g")])
                    covar_writer.writerow([*key, *(format(value, ".17g") for value in covar_values)])
            for key, path in paths.items():
                staged[key].replace(path)
        return {**paths, "matched_samples": matched["matched_samples"]}


    def outputs_current(outputs: list[Path], inputs: list[Path], log: Path | None = None) -> bool:
        if any(not path.is_file() or path.stat().st_size == 0 for path in (*outputs, *inputs)):
            return False
        if log is not None:
            if not log.is_file() or "End time is" not in log.read_text(encoding="utf-8"):
                return False
            outputs = [*outputs, log]
        return min(path.stat().st_mtime_ns for path in outputs) >= max(
            path.stat().st_mtime_ns for path in inputs
        )


    def ensure_gwas_inputs(args: argparse.Namespace) -> None:
        required = [
            args.sumstats,
            args.gtf,
            *bfile_files(args.bfile),
        ]
        if all(path.is_file() and path.stat().st_size > 0 for path in required):
            return
        if (args.sumstats, args.gtf, args.bfile) != (
            DEFAULT_SUMSTATS,
            DEFAULT_GTF,
            DEFAULT_BFILE,
        ):
            require_files(required)
        female_pipeline = BASE_DIR / "notebooks" / "drosophila_female_lifespan_finemapping_pipeline.py"
        require_files([female_pipeline])
        print("Generating female lifespan GWAS inputs for MAGMA", flush=True)
        subprocess.run([sys.executable, str(female_pipeline), "--stage", "gwas"], check=True)
        require_files(required)


    def write_gene_locations(gtf_path: Path, output_path: Path, force: bool = False) -> Path:
        if not force and outputs_current([output_path], [gtf_path]):
            print(f"Using existing gene locations: {output_path}")
            return output_path
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


    def load_gene_metadata(gtf_path: Path) -> dict[str, tuple[str, str]]:
        metadata = {}
        opener = gzip.open if gtf_path.suffix == ".gz" else open
        with opener(gtf_path, "rt", encoding="utf-8") as source:
            for line in source:
                if line.startswith("#"):
                    continue
                fields = line.rstrip("\n").split("\t")
                if len(fields) != 9 or fields[2] != "gene":
                    continue
                match = GENE_ID_PATTERN.search(fields[8])
                if match is None:
                    continue
                gene_id = match.group(1)
                name_match = GENE_NAME_PATTERN.search(fields[8])
                biotype_match = GENE_BIOTYPE_PATTERN.search(fields[8])
                metadata[gene_id] = (
                    name_match.group(1) if name_match else gene_id,
                    biotype_match.group(1) if biotype_match else "",
                )
        return metadata


    def write_snp_locations(bfile: Path, output_path: Path, force: bool = False) -> Path:
        bim_path = Path(f"{bfile}.bim")
        if not force and outputs_current([output_path], [bim_path]):
            print(f"Using existing SNP locations: {output_path}")
            return output_path
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


    def write_magma_pvalues(sumstats_path: Path, output_path: Path, force: bool = False) -> Path:
        if not force and outputs_current([output_path], [sumstats_path]):
            print(f"Using existing MAGMA p-values: {output_path}")
            return output_path
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


    def prepare_magma_bfile(source_prefix: Path, output_prefix: Path, force: bool = False) -> Path:
        if not force and all(
            outputs_current(
                [Path(f"{output_prefix}{extension}")],
                [Path(f"{source_prefix}{extension}")],
            )
            for extension in BFILE_EXTENSIONS
        ):
            print(f"Using existing MAGMA PLINK reference: {output_prefix}")
            return output_prefix
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


    def run_individual_analysis(
        magma_bin: Path, bfile: Path, gtf: Path, phenotype: Path, trait: str,
        covariates: Path, covariate_names: list[str], output_dir: Path,
    ) -> dict:
        """Annotate fly genes and run principal-components regression in MAGMA."""
        require_files([magma_bin, gtf, *bfile_files(bfile), phenotype, covariates])
        if not os.access(magma_bin, os.X_OK):
            raise PermissionError(f"MAGMA binary is not executable: {magma_bin}")
        # Reserve generated names so preparation cannot replace any source input.
        source_paths = {path.resolve() for path in [magma_bin, gtf, phenotype, covariates, *bfile_files(bfile)]}
        if any(path.parent == output_dir.resolve() for path in source_paths):
            raise ValueError("Use an output directory separate from the source input directories")
        prepared = prepare_individual_files(
            bfile, phenotype, trait, covariates, covariate_names, output_dir
        )
        gene_locations = write_gene_locations(gtf, output_dir / "gene_locations.tsv", force=True)
        snp_locations = write_snp_locations(bfile, output_dir / "snp_locations.tsv", force=True)
        prepared_bfile = prepare_magma_bfile(bfile, output_dir / "merged_qc_magma", force=True)
        annotation = run_annotation(
            magma_bin, snp_locations, gene_locations, output_dir / "dgrp_lifespan_female"
        )
        output_prefix = output_dir / "dgrp_lifespan_female_individual_gene"
        command = [
            str(magma_bin), "--bfile", str(prepared_bfile),
            "--gene-annot", str(annotation),
            "--pheno", f"file={prepared['phenotype']}", "use=1",
            # DGRP observations are strain means; unknown FAM sex is not a covariate.
            "--covar", f"file={prepared['covariates']}", "chrX-use-sex=0",
            "--gene-model", "linreg", "--out", str(output_prefix),
        ]
        print("Running individual-level MAGMA:", " ".join(command), flush=True)
        subprocess.run(command, check=True)
        gene_results = Path(f"{output_prefix}.genes.raw")
        gene_table = Path(f"{output_prefix}.genes.out")
        require_files([gene_results, gene_table])
        summary = summarize_magma_results(
            gene_table, "GENE", output_dir / "gene_results_bonferroni.tsv",
            gene_metadata=load_gene_metadata(gtf),
            significant_output_path=output_dir / "gene_results_significant.tsv",
        )
        return {
            "annotation": annotation, "gene_results": gene_results,
            "gene_table": gene_table, "summary": summary,
            "matched_samples": prepared["matched_samples"],
        }


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


    def validate_gene_sets(set_file: Path, gene_locations: Path) -> int:
        with gene_locations.open("r", encoding="utf-8") as source:
            known_genes = {line.split()[0] for line in source if line.strip()}
        set_count = 0
        matched_genes = set()
        seen_sets = set()
        with set_file.open("r", encoding="utf-8") as source:
            for line_number, line in enumerate(source, start=1):
                fields = line.split()
                if len(fields) < 2 or fields[0].startswith("#"):
                    raise ValueError(f"Invalid MAGMA gene set at {set_file}:{line_number}")
                if fields[0] in seen_sets:
                    raise ValueError(f"Duplicate MAGMA gene set name: {fields[0]}")
                seen_sets.add(fields[0])
                matched_genes.update(known_genes.intersection(fields[1:]))
                set_count += 1
        if set_count == 0 or not matched_genes:
            raise ValueError(f"Gene sets contain no IDs matching the MAGMA gene locations: {set_file}")
        print(f"Validated {set_count} gene sets with {len(matched_genes)} matching genes")
        return set_count


    def load_go_parents(obo_path: Path) -> tuple[dict[str, set[str]], dict[str, str]]:
        parents = {}
        namespaces = {}
        term_id = None
        namespace = None
        term_parents = set()
        obsolete = False
        in_term = False

        def save_term() -> None:
            if in_term and term_id and namespace and not obsolete:
                parents[term_id] = term_parents.copy()
                namespaces[term_id] = namespace

        with obo_path.open("r", encoding="utf-8") as source:
            for line in source:
                line = line.strip()
                if line.startswith("["):
                    save_term()
                    in_term = line == "[Term]"
                    term_id = None
                    namespace = None
                    term_parents = set()
                    obsolete = False
                elif in_term and line.startswith("id: GO:"):
                    term_id = line[4:]
                elif in_term and line.startswith("namespace: "):
                    namespace = line[11:]
                elif in_term and line == "is_obsolete: true":
                    obsolete = True
                elif in_term and line.startswith("is_a: GO:"):
                    term_parents.add(line[6:].split()[0])
                elif in_term and line.startswith("relationship: part_of GO:"):
                    term_parents.add(line.split()[2])
        save_term()
        if not parents:
            raise ValueError(f"No GO terms found in {obo_path}")
        return parents, namespaces


    def load_go_names(obo_path: Path) -> dict[str, str]:
        names = {}
        term_id = None
        name = None
        obsolete = False
        in_term = False

        def save_term() -> None:
            if in_term and term_id and name and not obsolete:
                names[term_id] = name

        with obo_path.open("r", encoding="utf-8") as source:
            for line in source:
                line = line.strip()
                if line.startswith("["):
                    save_term()
                    in_term = line == "[Term]"
                    term_id = None
                    name = None
                    obsolete = False
                elif in_term and line.startswith("id: GO:"):
                    term_id = line[4:]
                elif in_term and line.startswith("name: "):
                    name = line[6:]
                elif in_term and line == "is_obsolete: true":
                    obsolete = True
        save_term()
        return names


    def build_go_gene_sets(
        gaf_path: Path,
        obo_path: Path,
        gene_locations: Path,
        output_path: Path,
        min_size: int = 10,
        max_size: int = 500,
    ) -> Path:
        parents, namespaces = load_go_parents(obo_path)
        with gene_locations.open("r", encoding="utf-8") as source:
            known_genes = {line.split()[0] for line in source if line.strip()}
        ancestor_cache = {}

        def ancestors(term: str) -> set[str]:
            if term not in ancestor_cache:
                ancestor_cache[term] = set()
                for parent in parents.get(term, ()):
                    if parent in parents:
                        ancestor_cache[term].add(parent)
                        ancestor_cache[term].update(ancestors(parent))
            return ancestor_cache[term]

        gene_sets = defaultdict(set)
        opener = gzip.open if gaf_path.suffix == ".gz" else open
        with opener(gaf_path, "rt", encoding="utf-8") as source:
            for line_number, line in enumerate(source, start=1):
                if line.startswith("!"):
                    continue
                fields = line.rstrip("\n").split("\t")
                if len(fields) < 9:
                    raise ValueError(f"Invalid GAF record at {gaf_path}:{line_number}")
                gene_id, qualifier, term, evidence, aspect = (
                    fields[1], fields[3], fields[4], fields[6], fields[8]
                )
                if (
                    gene_id not in known_genes
                    or aspect != "P"
                    or evidence == "ND"
                    or "NOT" in qualifier.split("|")
                    or namespaces.get(term) != "biological_process"
                ):
                    continue
                for related in {term, *ancestors(term)}:
                    if namespaces.get(related) == "biological_process":
                        gene_sets[related].add(gene_id)

        retained = {
            term: genes for term, genes in gene_sets.items()
            if min_size <= len(genes) <= max_size
        }
        if not retained:
            raise ValueError(f"No GO biological-process sets of size {min_size}-{max_size}")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = output_path.with_name(f"{output_path.name}.tmp")
        try:
            with temporary_path.open("w", encoding="utf-8") as destination:
                for term, genes in sorted(retained.items()):
                    destination.write("\t".join((term, *sorted(genes))) + "\n")
            temporary_path.replace(output_path)
        finally:
            temporary_path.unlink(missing_ok=True)
        print(f"Wrote {len(retained)} GO biological-process gene sets: {output_path}")
        return output_path


    def download_go_inputs(output_dir: Path) -> tuple[Path, Path]:
        output_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = output_dir / "source_manifest.json"
        previous_manifest = (
            json.loads(manifest_path.read_text(encoding="utf-8"))
            if manifest_path.is_file()
            else {}
        )
        files = (
            (GO_GAF_URL, output_dir / "DROME-mod.gaf.gz"),
            (GO_OBO_URL, output_dir / "go-basic.obo"),
        )
        manifest = {}
        for url, destination in files:
            cached = destination.is_file() and destination.stat().st_size > 0
            if not cached:
                temporary = destination.with_name(f"{destination.name}.tmp")
                try:
                    print(f"Downloading GO input: {url}", flush=True)
                    request = urllib.request.Request(
                        url, headers={"User-Agent": "DrosophilaLifespanMAGMA/1.0"}
                    )
                    with urllib.request.urlopen(request, timeout=120) as response, temporary.open(
                        "wb"
                    ) as output:
                        shutil.copyfileobj(response, output)
                    if temporary.stat().st_size == 0:
                        raise ValueError(f"Downloaded empty GO input from {url}")
                    temporary.replace(destination)
                finally:
                    temporary.unlink(missing_ok=True)
                print(f"Downloaded GO input: {destination}")
            digest = hashlib.sha256()
            with destination.open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
            old_record = previous_manifest.get(destination.name)
            if cached and old_record and old_record["sha256"] != digest.hexdigest():
                raise ValueError(f"Cached GO input differs from its recorded checksum: {destination}")
            manifest[destination.name] = {
                "url": old_record["url"] if cached and old_record else url,
                "sha256": digest.hexdigest(),
            }
        temporary_manifest = manifest_path.with_name(f"{manifest_path.name}.tmp")
        try:
            temporary_manifest.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            temporary_manifest.replace(manifest_path)
        finally:
            temporary_manifest.unlink(missing_ok=True)
        print(f"Recorded GO input sources and checksums: {manifest_path}")
        return files[0][1], files[1][1]


    def run_gene_set_analysis(
        magma_bin: Path, gene_results: Path, set_file: Path, output_prefix: Path
    ) -> Path:
        command = [
            str(magma_bin),
            "--gene-results",
            str(gene_results),
            "--set-annot",
            str(set_file),
            "--out",
            str(output_prefix),
        ]
        print("Running MAGMA gene-set analysis", flush=True)
        subprocess.run(command, check=True)
        output_path = Path(f"{output_prefix}.gsa.out")
        if not output_path.is_file() or output_path.stat().st_size == 0:
            raise RuntimeError(f"MAGMA did not produce nonempty gene-set results: {output_path}")
        print(f"Saved MAGMA gene-set results: {output_path}")
        return output_path


    def run_individual_gene_sets(
        magma_bin: Path, gene_results: Path, set_file: Path, output_dir: Path,
        obo_path: Path | None = None,
    ) -> dict:
        """Run competitive gene-set tests and record the exact set definitions used."""
        required = [magma_bin, gene_results, set_file]
        if obo_path is not None:
            required.append(obo_path)
        require_files(required)
        if not os.access(magma_bin, os.X_OK):
            raise PermissionError(f"MAGMA binary is not executable: {magma_bin}")
        if not gene_results.name.endswith(".genes.raw"):
            raise ValueError("Gene-set analysis requires MAGMA .genes.raw results from Section 4")
        gene_prefix = str(gene_results)[:-len(".genes.raw")]
        gene_log = Path(f"{gene_prefix}.log")
        require_files([gene_log, Path(f"{gene_prefix}.genes.out")])
        log_text = gene_log.read_text(encoding="utf-8")
        if "End time is" not in log_text or "using model: principal components regression" not in log_text:
            raise ValueError("Complete the individual-level regression in Section 4 first")
        output_dir.mkdir(parents=True, exist_ok=True)
        set_results = run_gene_set_analysis(
            magma_bin, gene_results, set_file,
            output_dir / "dgrp_lifespan_female_individual_geneset",
        )
        summary = summarize_magma_results(
            set_results, "VARIABLE", output_dir / "gene_set_results_bonferroni.tsv",
            go_names=load_go_names(obo_path) if obo_path is not None else None,
            significant_output_path=output_dir / "gene_set_results_significant.tsv",
        )
        counts = count_summary_results(summary)
        set_hash = hashlib.sha256(set_file.read_bytes()).hexdigest()
        manifest = output_dir / "individual_gene_sets_manifest.json"
        manifest.write_text(json.dumps({
            "gene_results": str(gene_results.resolve()),
            "gene_model": "linreg",
            "gene_sets": str(set_file.resolve()),
            "gene_sets_sha256": set_hash,
            "go_obo": str(obo_path.resolve()) if obo_path is not None else None,
            "multiple_testing": "Bonferroni", "alpha": 0.05,
            **counts,
        }, indent=2) + "\n", encoding="utf-8")
        return {"set_results": set_results, "summary": summary, "manifest": manifest, **counts}


    def compare_magma_tables(
        summary_table: Path, individual_table: Path, output_prefix: Path, top_n: int = 20,
    ) -> dict:
        """Describe agreement without treating correlated methods as independent tests."""
        from scipy.stats import pearsonr, spearmanr

        require_files([summary_table, individual_table])
        if summary_table.resolve() == individual_table.resolve():
            raise ValueError("Select results from two different analysis files")
        if top_n < 1:
            raise ValueError("Top-ranked overlap requires a positive top_n")

        def read_results(path: Path) -> dict:
            results = {}
            with path.open(encoding="utf-8", newline="") as source:
                reader = csv.DictReader(source, delimiter="\t")
                required = {"ID", "P", "P_BONFERRONI", "SIGNIFICANT"}
                if reader.fieldnames is None or not required.issubset(reader.fieldnames):
                    raise ValueError(f"Missing comparison columns in {path}")
                for row in reader:
                    identifier = row["ID"]
                    if not identifier or identifier in results:
                        raise ValueError(f"Missing or duplicate result ID in {path}: {identifier}")
                    p_value, adjusted = float(row["P"]), float(row["P_BONFERRONI"])
                    if any(not math.isfinite(p) or not 0 <= p <= 1 for p in (p_value, adjusted)):
                        raise ValueError(f"Invalid p-value for {identifier} in {path}")
                    if row["SIGNIFICANT"] not in {"true", "false"}:
                        raise ValueError(f"Invalid significance flag for {identifier} in {path}")
                    results[identifier] = {
                        "p": p_value, "adjusted": adjusted,
                        "significant": row["SIGNIFICANT"] == "true",
                        "label": row.get("GENE_NAME") or row.get("GO_TERM_NAME") or identifier,
                    }
            if not results:
                raise ValueError(f"No comparison results in {path}")
            return results

        summary_rows, individual_rows = read_results(summary_table), read_results(individual_table)
        shared = sorted(summary_rows.keys() & individual_rows.keys())
        summary_logp = [-math.log10(max(summary_rows[key]["p"], 1e-300)) for key in shared]
        individual_logp = [-math.log10(max(individual_rows[key]["p"], 1e-300)) for key in shared]
        variable_scores = len(shared) >= 2 and len(set(summary_logp)) > 1 and len(set(individual_logp)) > 1
        summary_sig = {key for key in shared if summary_rows[key]["significant"]}
        individual_sig = {key for key in shared if individual_rows[key]["significant"]}
        significant_union = summary_sig | individual_sig
        k = min(top_n, len(shared))
        summary_top = set(sorted(shared, key=lambda key: (summary_rows[key]["p"], key))[:k])
        individual_top = set(sorted(shared, key=lambda key: (individual_rows[key]["p"], key))[:k])
        metrics = {
            "summary_table": str(summary_table.resolve()),
            "individual_table": str(individual_table.resolve()),
            "summary_tested": len(summary_rows), "individual_tested": len(individual_rows),
            "shared": len(shared),
            "summary_only": len(summary_rows.keys() - individual_rows.keys()),
            "individual_only": len(individual_rows.keys() - summary_rows.keys()),
            "summary_significant_all": sum(row["significant"] for row in summary_rows.values()),
            "individual_significant_all": sum(row["significant"] for row in individual_rows.values()),
            "pearson_logp": float(pearsonr(summary_logp, individual_logp).statistic) if variable_scores else None,
            "spearman_logp": float(spearmanr(summary_logp, individual_logp).statistic) if variable_scores else None,
            "significant_both_shared": len(summary_sig & individual_sig),
            "significant_summary_only_shared": len(summary_sig - individual_sig),
            "significant_individual_only_shared": len(individual_sig - summary_sig),
            "significant_neither_shared": len(shared) - len(significant_union),
            "significant_jaccard_shared": len(summary_sig & individual_sig) / len(significant_union) if significant_union else None,
            "top_requested": top_n, "top_compared_shared": k,
            "top_overlap_shared": len(summary_top & individual_top),
            "notes": [
                "Descriptive agreement does not establish statistical equivalence or a significant difference between methods.",
                "Correlations and top-ranked overlap use only shared IDs; ties are ordered by ID.",
                "Bonferroni significance retains each original run's tested universe; it is not recomputed on shared IDs.",
                "Zero p-values are clipped to 1e-300 for correlations; undefined metrics are null.",
                "Historical identity of gene-set definitions and per-set gene membership is not verified by this table comparison.",
            ],
        }
        table_path = Path(f"{output_prefix}.tsv")
        metrics_path = Path(f"{output_prefix}.json")
        if any(path.resolve() in {summary_table.resolve(), individual_table.resolve()} for path in (table_path, metrics_path)):
            raise ValueError("Comparison outputs must not overwrite input results")
        output_prefix.parent.mkdir(parents=True, exist_ok=True)
        with table_path.open("w", encoding="utf-8", newline="") as destination:
            writer = csv.writer(destination, delimiter="\t", lineterminator="\n")
            writer.writerow([
                "ID", "LABEL", "STATUS", "P_SUMMARY", "P_INDIVIDUAL",
                "P_BONFERRONI_SUMMARY", "P_BONFERRONI_INDIVIDUAL",
                "SIGNIFICANT_SUMMARY", "SIGNIFICANT_INDIVIDUAL",
                "DELTA_LOGP_INDIVIDUAL_MINUS_SUMMARY",
            ])
            for key in sorted(summary_rows.keys() | individual_rows.keys()):
                left, right = summary_rows.get(key), individual_rows.get(key)
                status = "shared" if left and right else "summary_only" if left else "individual_only"
                values = [key, (left or right)["label"], status]
                for field in ("p", "adjusted", "significant"):
                    values.extend(
                        "" if row is None else str(row[field]).lower() if field == "significant" else format(row[field], ".17g")
                        for row in (left, right)
                    )
                delta = math.log10(max(left["p"], 1e-300)) - math.log10(max(right["p"], 1e-300)) if left and right else None
                writer.writerow([*values, "" if delta is None else format(delta, ".17g")])
        metrics_path.write_text(json.dumps(metrics, indent=2, allow_nan=False) + "\n", encoding="utf-8")
        return {"table": table_path, "metrics_file": metrics_path, "metrics": metrics}


    def summarize_magma_results(
        result_path: Path,
        identifier_column: str,
        output_path: Path,
        gene_metadata: dict[str, tuple[str, str]] | None = None,
        go_names: dict[str, str] | None = None,
        significant_output_path: Path | None = None,
    ) -> Path:
        records = []
        columns = None
        with result_path.open("r", encoding="utf-8") as source:
            for line_number, line in enumerate(source, start=1):
                if not line.strip() or line.startswith("#"):
                    continue
                fields = line.split()
                if columns is None:
                    columns = fields
                    if identifier_column not in columns or "P" not in columns:
                        raise ValueError(f"Unexpected MAGMA result header in {result_path}")
                    continue
                if len(fields) != len(columns):
                    raise ValueError(f"Invalid MAGMA result at {result_path}:{line_number}")
                row = dict(zip(columns, fields))
                if row.get("TYPE", "SET") != "SET":
                    continue
                p_value = float(row["P"])
                if not math.isfinite(p_value) or not 0 <= p_value <= 1:
                    raise ValueError(f"Invalid MAGMA p-value at {result_path}:{line_number}")
                identifier = row.get("FULL_NAME", row[identifier_column])
                records.append((identifier, p_value))
        if not records:
            raise ValueError(f"No tested MAGMA results found in {result_path}")

        test_count = len(records)
        threshold = 0.05 / test_count
        significant = sum(p_value <= threshold for _, p_value in records)
        for destination_path, significant_only in (
            (output_path, False),
            (significant_output_path, True),
        ):
            if destination_path is None:
                continue
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            temporary_path = destination_path.with_name(f"{destination_path.name}.tmp")
            try:
                with temporary_path.open("w", encoding="utf-8") as destination:
                    columns = ["ID"]
                    if gene_metadata is not None:
                        columns.extend(("GENE_NAME", "GENE_BIOTYPE"))
                    if go_names is not None:
                        columns.append("GO_TERM_NAME")
                    columns.extend(("P", "P_BONFERRONI", "SIGNIFICANT"))
                    destination.write("\t".join(columns) + "\n")
                    for identifier, p_value in sorted(records, key=lambda item: (item[1], item[0])):
                        if significant_only and p_value > threshold:
                            continue
                        adjusted = min(1.0, p_value * test_count)
                        values = [identifier]
                        if gene_metadata is not None:
                            values.extend(gene_metadata.get(identifier, (identifier, "")))
                        if go_names is not None:
                            values.append(go_names.get(identifier, ""))
                        values.extend(
                            (f"{p_value:.17g}", f"{adjusted:.17g}", str(p_value <= threshold).lower())
                        )
                        destination.write("\t".join(values) + "\n")
                temporary_path.replace(destination_path)
            finally:
                temporary_path.unlink(missing_ok=True)
        print(
            f"MAGMA {identifier_column.lower()} results: {test_count} tests, "
            f"Bonferroni threshold {threshold:.6g}, {significant} significant: {output_path}"
        )
        return output_path


    def count_summary_results(summary_path: Path) -> dict[str, int]:
        tested = 0
        significant = 0
        with summary_path.open("r", encoding="utf-8", newline="") as source:
            reader = csv.DictReader(source, delimiter="\t")
            if reader.fieldnames is None or "SIGNIFICANT" not in reader.fieldnames:
                raise ValueError(f"Invalid MAGMA summary header: {summary_path}")
            for row in reader:
                value = row["SIGNIFICANT"]
                if value not in {"true", "false"}:
                    raise ValueError(f"Invalid significance value in {summary_path}: {value}")
                tested += 1
                significant += value == "true"
        if tested == 0:
            raise ValueError(f"No MAGMA results found in {summary_path}")
        return {"tested": tested, "significant": significant}


    def write_run_manifest(args: argparse.Namespace, set_file: Path | None) -> Path:
        output_path = args.out_dir / "run_manifest.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        binary_hash = hashlib.sha256()
        with args.magma_bin.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                binary_hash.update(chunk)
        gene_summary = count_summary_results(args.out_dir / "gene_results_bonferroni.tsv")
        gene_set_summary = (
            count_summary_results(args.out_dir / "gene_set_results_bonferroni.tsv")
            if set_file is not None
            else None
        )
        manifest = {
            "stage": args.stage,
            "sumstats": str(args.sumstats.resolve()),
            "gtf": str(args.gtf.resolve()),
            "bfile": str(args.bfile.resolve()),
            "magma_bin": str(args.magma_bin.resolve()),
            "magma_sha256": binary_hash.hexdigest(),
            "gene_sets": str(set_file.resolve()) if set_file is not None else None,
            "sample_size_column": "N",
            "multiple_testing": "Bonferroni",
            "alpha": 0.05,
            "gene_results": str((args.out_dir / "gene_results_bonferroni.tsv").resolve()),
            "gene_summary": gene_summary,
            "gene_set_results": (
                str((args.out_dir / "gene_set_results_bonferroni.tsv").resolve())
                if set_file is not None
                else None
            ),
            "gene_set_summary": gene_set_summary,
        }
        temporary_path = output_path.with_name(f"{output_path.name}.tmp")
        try:
            temporary_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            temporary_path.replace(output_path)
        finally:
            temporary_path.unlink(missing_ok=True)
        print(f"Saved MAGMA run manifest: {output_path}")
        return output_path


    def main(argv: list[str] | None = None) -> None:
        args = parse_args(argv)
        if args.stage == "inputs":
            ensure_gwas_inputs(args)
            return
        if args.stage == "install":
            print(f"MAGMA executable: {resolve_magma_binary(args.magma_bin)}")
            return
        if args.stage == "validate":
            validate_inputs(args)
            return
        out_dir = args.out_dir
        gene_locations = out_dir / "gene_loc.txt"
        snp_locations = out_dir / "snp_loc.txt"
        pvalues = out_dir / "lifespan_female.magma.p.txt"
        annotation = out_dir / "dgrp_lifespan_female.genes.annot"
        bfile = out_dir / "merged_qc_magma"
        gene_results = out_dir / "dgrp_lifespan_female_gene.genes.raw"

        if args.stage == "report":
            gene_output = out_dir / "dgrp_lifespan_female_gene.genes.out"
            require_files([args.gtf, gene_output])
            summarize_magma_results(
                gene_output,
                "GENE",
                out_dir / "gene_results_bonferroni.tsv",
                gene_metadata=load_gene_metadata(args.gtf),
                significant_output_path=out_dir / "gene_results_significant.tsv",
            )
            set_output = out_dir / "dgrp_lifespan_female_geneset.gsa.out"
            if set_output.is_file() and set_output.stat().st_size > 0:
                obo_path = args.go_obo
                if obo_path is None:
                    cached_obo = out_dir / "go_inputs" / "go-basic.obo"
                    if cached_obo.is_file():
                        obo_path = cached_obo
                if obo_path is not None:
                    require_files([obo_path])
                summarize_magma_results(
                    set_output,
                    "VARIABLE",
                    out_dir / "gene_set_results_bonferroni.tsv",
                    go_names=load_go_names(obo_path) if obo_path is not None else None,
                    significant_output_path=out_dir / "gene_set_results_significant.tsv",
                )
            return

        if args.stage in {"annotate", "genes", "sets"}:
            args.magma_bin = resolve_magma_binary(args.magma_bin)

        if args.stage in {"all", "prepare"}:
            ensure_gwas_inputs(args)
            validate_inputs(args)
            gene_locations = write_gene_locations(args.gtf, gene_locations, force=args.force)
            snp_locations = write_snp_locations(args.bfile, snp_locations, force=args.force)
            pvalues = write_magma_pvalues(args.sumstats, pvalues, force=args.force)
            bfile = prepare_magma_bfile(args.bfile, bfile, force=args.force)

        if args.stage in {"all", "prepare", "sets"}:
            if args.stage == "sets":
                require_files([args.magma_bin, gene_locations, gene_results])
                if not os.access(args.magma_bin, os.X_OK):
                    raise PermissionError(f"MAGMA binary is not executable: {args.magma_bin}")
            set_file = args.gene_sets
            gaf_path, obo_path = args.go_gaf, args.go_obo
            if args.download_go:
                gaf_path, obo_path = download_go_inputs(out_dir / "go_inputs")
            if gaf_path is not None:
                require_files([gaf_path, obo_path])
                set_file = build_go_gene_sets(
                    gaf_path, obo_path, gene_locations, out_dir / "go_biological_process.sets.txt"
                )
            if args.stage == "sets" and set_file is None:
                set_file = out_dir / "go_biological_process.sets.txt"
                cached_obo = out_dir / "go_inputs" / "go-basic.obo"
                if cached_obo.is_file():
                    obo_path = cached_obo
            if set_file is not None:
                require_files([set_file])
                validate_gene_sets(set_file, gene_locations)
        else:
            set_file = None
            obo_path = None

        if args.stage == "prepare":
            return

        if args.stage in {"all", "annotate"}:
            if args.stage == "annotate":
                require_files([args.magma_bin, snp_locations, gene_locations])
                if not os.access(args.magma_bin, os.X_OK):
                    raise PermissionError(f"MAGMA binary is not executable: {args.magma_bin}")
            annotation = run_annotation(
                args.magma_bin, snp_locations, gene_locations, out_dir / "dgrp_lifespan_female"
            )
        if args.stage == "annotate":
            return

        if args.stage in {"all", "genes"}:
            if args.stage == "genes":
                require_files([
                    args.magma_bin,
                    args.gtf,
                    pvalues,
                    annotation,
                    *bfile_files(bfile),
                ])
                if not os.access(args.magma_bin, os.X_OK):
                    raise PermissionError(f"MAGMA binary is not executable: {args.magma_bin}")
            gene_results = run_gene_analysis(
                args.magma_bin,
                bfile,
                pvalues,
                annotation,
                out_dir / "dgrp_lifespan_female_gene",
            )
            summarize_magma_results(
                out_dir / "dgrp_lifespan_female_gene.genes.out",
                "GENE",
                out_dir / "gene_results_bonferroni.tsv",
                gene_metadata=load_gene_metadata(args.gtf),
                significant_output_path=out_dir / "gene_results_significant.tsv",
            )
        if args.stage == "genes":
            return

        if set_file is not None:
            set_results = run_gene_set_analysis(
                args.magma_bin,
                gene_results,
                set_file,
                out_dir / "dgrp_lifespan_female_geneset",
            )
            summarize_magma_results(
                set_results,
                "VARIABLE",
                out_dir / "gene_set_results_bonferroni.tsv",
                go_names=load_go_names(obo_path) if obo_path is not None else None,
                significant_output_path=out_dir / "gene_set_results_significant.tsv",
            )
        if args.stage == "all":
            write_run_manifest(args, set_file)

    # Keep CLI dispatch in setup so Marimo saves preserve it. The notebook runtime
    # executes setup with an active Marimo context; a plain Python invocation does not.
    if __name__ == "__main__" and not mo.running_in_notebook():
        main()
        raise SystemExit(0)



@app.cell
def __(mo):
    mo.md(r"""
    # Drosophila Female Lifespan MAGMA
    Configure inputs and run a selected stage of the MAGMA gene and gene-set analysis. This file contains both the interactive notebook and the command-line pipeline.
    """)
    return


@app.cell
def __():
    from pathlib import Path as _Path

    project_root = _Path(__file__).resolve().parents[1]
    pipeline_script = project_root / "notebooks" / "drosophila_female_lifespan_magma_pipeline.py"
    defaults = {
        "sumstats": project_root / "data/gwas/tmp/lifespan_female_gwas_sumstats.tsv",
        "gtf": project_root / "data/genes/Drosophila_melanogaster.BDGP6.54.62.chr.gtf.gz",
        "bfile": project_root / "data/gwas/tmp/merged_qc",
        "magma_bin": project_root / "tools/magma/magma",
        "out_dir": project_root / "results/magma_female",
        "go_gaf": project_root / "results/magma_female/go_inputs/DROME-mod.gaf.gz",
        "go_obo": project_root / "results/magma_female/go_inputs/go-basic.obo",
        "gene_sets": project_root / "results/magma_female/go_biological_process.sets.txt",
    }
    return defaults, pipeline_script, project_root


@app.cell
def __(mo):
    mo.md(r"""
    ## Inputs and Run Settings
    """)
    return


@app.cell
def __(defaults, mo):
    sumstats_input = mo.ui.text(value=str(defaults["sumstats"]), label="GWAS summary statistics")
    gtf_input = mo.ui.text(value=str(defaults["gtf"]), label="Drosophila GTF annotation")
    bfile_input = mo.ui.text(value=str(defaults["bfile"]), label="PLINK genotype prefix")
    magma_bin_input = mo.ui.text(value=str(defaults["magma_bin"]), label="MAGMA executable")
    out_dir_input = mo.ui.text(value=str(defaults["out_dir"]), label="Output directory")
    stage_input = mo.ui.dropdown(
        options=["all", "inputs", "install", "validate", "prepare", "annotate", "genes", "sets", "report"],
        value="all", label="Pipeline stage",
    )
    gene_source_input = mo.ui.dropdown(
        options=["No gene-set analysis", "Download Gene Ontology", "Use gene-set file", "Use GAF and OBO files"],
        value="Download Gene Ontology", label="Gene-set source",
    )
    gene_sets_input = mo.ui.text(value=str(defaults["gene_sets"]), label="Gene-set file")
    gaf_input = mo.ui.text(value=str(defaults["go_gaf"]), label="Gene Ontology GAF file")
    obo_input = mo.ui.text(value=str(defaults["go_obo"]), label="Gene Ontology OBO file")
    force_input = mo.ui.checkbox(value=False, label="Rebuild cached outputs")
    mo.vstack([
        sumstats_input, gtf_input, bfile_input, magma_bin_input, out_dir_input,
        stage_input, force_input, gene_source_input, gene_sets_input, gaf_input, obo_input,
    ])
    return (bfile_input, force_input, gaf_input, gene_sets_input, gene_source_input,
            magma_bin_input, obo_input, out_dir_input, stage_input, sumstats_input, gtf_input)


@app.cell
def __(mo):
    run_button = mo.ui.run_button(label="Run selected MAGMA stage")
    run_button
    return (run_button,)


@app.cell
def __(bfile_input, defaults, force_input, gaf_input, gene_sets_input, gene_source_input,
      magma_bin_input, mo, obo_input, out_dir_input, pipeline_script, run_button,
      stage_input, subprocess, sumstats_input, gtf_input, sys):
    mo.stop(not run_button.value, mo.md("Set the inputs above, choose a stage, then press the run button."))
    command = [
        sys.executable, str(pipeline_script),
        "--sumstats", sumstats_input.value,
        "--gtf", gtf_input.value,
        "--bfile", bfile_input.value,
        "--magma-bin", magma_bin_input.value,
        "--out-dir", out_dir_input.value,
        "--stage", stage_input.value,
    ]
    if force_input.value:
        command.append("--force")
    if gene_source_input.value == "Download Gene Ontology":
        command.append("--download-go")
    elif gene_source_input.value == "Use gene-set file":
        command.extend(["--gene-sets", gene_sets_input.value])
    elif gene_source_input.value == "Use GAF and OBO files":
        command.extend(["--go-gaf", gaf_input.value, "--go-obo", obo_input.value])
    print("Running:", " ".join(command), flush=True)
    try:
        subprocess.run(command, cwd=pipeline_script.parents[1], check=True)
    except subprocess.CalledProcessError as error:
        raise RuntimeError(f"MAGMA stage '{stage_input.value}' failed with exit code {error.returncode}.") from error
    print(f"MAGMA stage '{stage_input.value}' completed successfully.")
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## Individual-level MAGMA — Section 1: Inputs

    This analysis will use each DGRP line's genotypes and female mean lifespan
    directly. The summary-statistics analysis above uses SNP p-values and uses
    genotypes to estimate LD.

    Select the same QC genotypes, phenotype, and population-structure covariates
    used for the female lifespan GWAS. PC1–PC4 are adjustment covariates; they
    are separate from the within-gene genotype components used by MAGMA.
    Samples will be matched by FID and IID, not by row order.

    This section configures inputs only. Sample validation and the analysis
    will be added in subsequent sections. Use a separate output directory so
    the two approaches can be compared later.
    """)
    return


@app.cell
def __(defaults, mo, project_root):
    individual_bfile_input = mo.ui.text(
        value=str(defaults["bfile"]), label="Individual-level PLINK genotype prefix"
    )
    individual_pheno_input = mo.ui.text(
        value=str(project_root / "data/gwas/lifespan_female.pheno"),
        label="Female lifespan phenotype file (FID, IID, phenotype)",
    )
    individual_trait_input = mo.ui.text(
        value="S18_1537_F", label="Phenotype column"
    )
    individual_covar_input = mo.ui.text(
        value=str(project_root / "data/gwas/tmp/dgrp_pca.eigenvec"),
        label="Population-structure covariate file",
    )
    individual_covariates_input = mo.ui.text(
        value="PC1,PC2,PC3,PC4", label="Covariate columns (comma-separated)"
    )
    individual_out_dir_input = mo.ui.text(
        value=str(project_root / "results/magma_female_individual"),
        label="Individual-level output directory",
    )
    mo.vstack([
        individual_bfile_input, individual_pheno_input, individual_trait_input,
        individual_covar_input, individual_covariates_input, individual_out_dir_input,
    ])
    return (individual_bfile_input, individual_pheno_input, individual_trait_input,
            individual_covar_input, individual_covariates_input, individual_out_dir_input)


@app.cell
def __(mo):
    mo.md(r"""
    ## Individual-level MAGMA — Section 2: Validate and match samples

    Check that the PLINK files exist and that phenotype and covariate columns
    contain finite numeric values. FID and IID must be unique in each file.
    Every phenotype sample must have genotypes and covariates; missing matches
    stop validation instead of silently reducing the analysis cohort.

    Extra genotype or covariate samples are counted but are not included in
    the matched cohort. Matched values follow genotype sample order.
    This checks sample metadata, not genotype integrity or covariate collinearity.
    No analysis is run and no files are written.
    """)
    return


@app.cell
def __(mo):
    individual_validate_button = mo.ui.run_button(label="Validate individual-level inputs")
    individual_validate_button
    return (individual_validate_button,)


@app.cell
def __(individual_bfile_input, individual_covar_input, individual_covariates_input,
       individual_pheno_input, individual_trait_input, individual_validate_button,
       mo, validate_individual_inputs):
    from pathlib import Path as _Path

    mo.stop(not individual_validate_button.value, mo.md("Press the validation button to check the inputs."))
    individual_validation = validate_individual_inputs(
        _Path(individual_bfile_input.value).expanduser(),
        _Path(individual_pheno_input.value).expanduser(),
        individual_trait_input.value.strip(),
        _Path(individual_covar_input.value).expanduser(),
        [name.strip() for name in individual_covariates_input.value.split(",")],
    )
    mo.vstack([
        mo.md("**Sample validation passed.** Matched data are ready for the next section."),
        mo.ui.table([
            {"Count": name.replace("_", " ").capitalize(), "Samples": individual_validation[name]}
            for name in (
                "genotype_samples", "phenotype_samples", "covariate_samples",
                "matched_samples", "genotype_samples_without_phenotype",
            )
        ], selection=None),
    ])
    return (individual_validation,)


@app.cell
def __(mo):
    mo.md(r"""
    ## Individual-level MAGMA — Section 3: Prepare matched analysis files

    Write two tab-separated files in the individual-level output directory:

    - `individual.phenotype.tsv`: FID, IID, and the selected lifespan phenotype.
    - `individual.covariates.tsv`: FID, IID, and only the selected covariates.

    Both files contain the same matched samples in genotype order. MAGMA
    accepts FID and IID as the first two columns, followed by numeric variables
    with an optional header (see the [MAGMA documentation](https://cncr.nl/research/magma/)).

    Preparation rechecks the source files before exporting. Pressing the button
    regenerates these two files. The original inputs are preserved.
    Gene annotation and individual-level regression come in the next section.
    """)
    return


@app.cell
def __(mo):
    individual_prepare_button = mo.ui.run_button(label="Prepare matched MAGMA input files")
    individual_prepare_button
    return (individual_prepare_button,)


@app.cell
def __(individual_bfile_input, individual_covar_input, individual_covariates_input,
       individual_out_dir_input, individual_pheno_input, prepare_individual_files,
       individual_prepare_button, individual_trait_input, individual_validation,
       mo, out_dir_input):
    from pathlib import Path as _Path

    mo.stop(not individual_prepare_button.value, mo.md("Validate Section 2, then press the preparation button."))
    _output_dir = _Path(individual_out_dir_input.value).expanduser()
    if _output_dir.resolve() == _Path(out_dir_input.value).expanduser().resolve():
        raise ValueError("Choose a different output directory from the summary-statistics analysis")
    individual_prepared = prepare_individual_files(
        _Path(individual_bfile_input.value).expanduser(),
        _Path(individual_pheno_input.value).expanduser(),
        individual_trait_input.value.strip(),
        _Path(individual_covar_input.value).expanduser(),
        [name.strip() for name in individual_covariates_input.value.split(",")],
        _output_dir,
    )
    mo.vstack([
        mo.md(f"**Prepared files for {individual_prepared['matched_samples']} matched samples.**"),
        mo.ui.table([
            {"File": name.capitalize(), "Path": str(individual_prepared[name])}
            for name in ("phenotype", "covariates")
        ], selection=None),
    ])
    return (individual_prepared,)


@app.cell
def __(mo):
    mo.md(r"""
    ## Individual-level MAGMA — Section 4: Annotate genes and run regression

    Map SNPs to genes using the selected Drosophila GTF and the same gene-body
    boundaries and chromosome mapping as the summary-statistics pipeline.
    Run MAGMA's `--gene-model linreg`: regress female lifespan on the genotype
    components within each gene, adjusting for the covariates selected in Section 1.
    See the [MAGMA manual, gene analysis](https://ibg.colorado.edu/cdrom2021/Day10-posthuma/magma_session/manual_v1.09a.pdf).

    DGRP observations are female strain means and the genotype FAM sex is
    unknown. Disable MAGMA's automatic sex covariate on X (`chrX-use-sex=0`)
    so unknown sex codes do not exclude all X genes. Genotype sex codes and
    chromosome labels are preserved.

    The run revalidates and regenerates matched phenotype/covariate files to
    use the current inputs. It writes gene p-values, intermediate `.genes.raw`
    results for later gene-set analysis, and a Bonferroni-corrected gene table.
    Rerunning replaces results in the individual-level output directory.
    The matched sample count is the input cohort size; per-gene sample counts
    and any filtering are recorded by MAGMA in the gene table and log.

    Gene-set testing and comparison with the summary-statistics results will
    follow in subsequent sections.
    """)
    return


@app.cell
def __(mo):
    individual_run_button = mo.ui.run_button(label="Run individual-level MAGMA gene analysis")
    individual_run_button
    return (individual_run_button,)


@app.cell
def __(gtf_input, individual_bfile_input, individual_covar_input,
       individual_covariates_input, individual_out_dir_input, individual_pheno_input,
       run_individual_analysis, individual_prepared, individual_run_button,
       individual_trait_input, magma_bin_input, mo, out_dir_input):
    from pathlib import Path as _Path

    mo.stop(not individual_run_button.value, mo.md("Complete Sections 2–3, then press the analysis button."))
    _output_dir = _Path(individual_out_dir_input.value).expanduser()
    if _output_dir.resolve() == _Path(out_dir_input.value).expanduser().resolve():
        raise ValueError("Choose a different output directory from the summary-statistics analysis")
    individual_gene_analysis = run_individual_analysis(
        _Path(magma_bin_input.value).expanduser(),
        _Path(individual_bfile_input.value).expanduser(),
        _Path(gtf_input.value).expanduser(),
        _Path(individual_pheno_input.value).expanduser(),
        individual_trait_input.value.strip(),
        _Path(individual_covar_input.value).expanduser(),
        [name.strip() for name in individual_covariates_input.value.split(",")],
        _output_dir,
    )
    mo.vstack([
        mo.md(f"**Individual-level gene analysis completed** with {individual_gene_analysis['matched_samples']} input samples."),
        mo.ui.table([
            {"Output": name.replace("_", " ").capitalize(), "Path": str(individual_gene_analysis[name])}
            for name in ("annotation", "gene_results", "gene_table", "summary")
        ], selection=None),
    ])
    return (individual_gene_analysis,)


@app.cell
def __(mo):
    mo.md(r"""
    ## Individual-level MAGMA — Section 5: Gene-set analysis

    Use the completed `.genes.raw` results from Section 4 to test whether genes
    in each set show stronger association than other analysed genes. This is
    MAGMA's competitive gene-set analysis; it identifies associated sets rather
    than establishing causality.

    Select the exact same gene-set file used for the summary-statistics run.
    For downloaded Gene Ontology inputs, this is normally the generated
    `go_biological_process.sets.txt` file. Reuse that file rather than downloading
    new definitions for this run. The optional OBO file supplies GO term names;
    leave it blank for custom sets without GO names.

    This section writes raw gene-set results, a Bonferroni-corrected table,
    significant sets, and a manifest with a checksum of the set definitions.
    The correction includes all sets tested in this run. Differences in which
    genes are testable can affect the set sizes and will need to be considered
    when comparing the two approaches in the next section.
    """)
    return


@app.cell
def __(gene_sets_input, mo, obo_input):
    individual_sets_input = mo.ui.text(
        value=gene_sets_input.value, label="Gene-set file used for the summary-statistics run"
    )
    individual_obo_input = mo.ui.text(
        value=obo_input.value, label="GO OBO file for term names (optional)"
    )
    individual_sets_button = mo.ui.run_button(label="Run individual-level gene-set analysis")
    mo.vstack([individual_sets_input, individual_obo_input, individual_sets_button])
    return individual_obo_input, individual_sets_button, individual_sets_input


@app.cell
def __(individual_gene_analysis, individual_obo_input, run_individual_gene_sets,
       individual_sets_button, individual_sets_input, magma_bin_input, mo):
    from pathlib import Path as _Path

    mo.stop(not individual_sets_button.value, mo.md("Complete Section 4, select the shared gene-set file, then press run."))
    _obo_value = individual_obo_input.value.strip()
    individual_set_analysis = run_individual_gene_sets(
        _Path(magma_bin_input.value).expanduser(),
        individual_gene_analysis["gene_results"],
        _Path(individual_sets_input.value).expanduser(),
        individual_gene_analysis["gene_results"].parent,
        _Path(_obo_value).expanduser() if _obo_value else None,
    )
    mo.vstack([
        mo.md(
            f"**Gene-set analysis completed:** {individual_set_analysis['tested']} sets tested; "
            f"{individual_set_analysis['significant']} significant after Bonferroni correction."
        ),
        mo.ui.table([
            {"Output": name.replace("_", " ").capitalize(), "Path": str(individual_set_analysis[name])}
            for name in ("set_results", "summary", "manifest")
        ], selection=None),
    ])
    return (individual_set_analysis,)


@app.cell
def __(mo):
    mo.md(r"""
    ## Individual-level MAGMA — Section 6: Compare the two approaches

    Match gene and gene-set results by ID. Export tables containing all IDs,
    both p-values, adjusted p-values, significance flags, and the difference
    in −log10(p). A positive difference means stronger individual-level evidence.
    IDs tested by only one approach remain visible as unmatched results.

    Correlations and top-ranked overlap use shared IDs only. Top-ranked lists
    break p-value ties by ID. Bonferroni flags retain each run's original number
    of tests. Missing correlations or an empty significant-set union are
    reported as undefined rather than as evidence of agreement.

    These are descriptive comparisons. High correlation does not establish
    equivalence, and different significant lists do not establish a statistically
    significant difference between methods. Gene-set definitions should be
    identical, but per-set gene membership can still differ because of gene
    filtering. This section does not verify historical gene-set identity.

    Complete both full analyses before interpreting these results. The small
    integration tests used during development are not full biological results.
    """)
    return


@app.cell
def __(mo):
    comparison_top_input = mo.ui.number(start=1, stop=1000, value=20, label="Top-ranked shared IDs to compare")
    comparison_button = mo.ui.run_button(label="Compare summary-statistics and individual-level results")
    mo.vstack([comparison_top_input, comparison_button])
    return comparison_button, comparison_top_input


@app.cell
def __(comparison_button, comparison_top_input, individual_gene_analysis,
       compare_magma_tables, individual_set_analysis, mo, out_dir_input):
    from pathlib import Path as _Path

    mo.stop(not comparison_button.value, mo.md("Complete both approaches, then press the comparison button."))
    _summary_dir = _Path(out_dir_input.value).expanduser()
    _comparison_dir = individual_gene_analysis["summary"].parent / "comparison"
    magma_comparisons = {}
    for _label, _filename, _individual in (
        ("Genes", "gene_results_bonferroni.tsv", individual_gene_analysis["summary"]),
        ("Gene sets", "gene_set_results_bonferroni.tsv", individual_set_analysis["summary"]),
    ):
        magma_comparisons[_label] = compare_magma_tables(
            _summary_dir / _filename, _individual,
            _comparison_dir / _label.lower().replace(" ", "_"),
            int(comparison_top_input.value),
        )
    mo.vstack([
        mo.ui.table([
            {"Results": _label, **{
                _key: _result["metrics"][_key]
                for _key in (
                    "shared", "summary_only", "individual_only", "spearman_logp",
                    "pearson_logp", "significant_both_shared", "top_compared_shared", "top_overlap_shared",
                )
            }}
            for _label, _result in magma_comparisons.items()
        ], selection=None),
        mo.ui.table([
            {"Results": _label, "Comparison table": str(_result["table"]),
             "Metrics and interpretation notes": str(_result["metrics_file"])}
            for _label, _result in magma_comparisons.items()
        ], selection=None),
    ])
    return (magma_comparisons,)


if __name__ == "__main__":
    app.run()
