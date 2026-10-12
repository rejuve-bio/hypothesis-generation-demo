#!/usr/bin/env python3

import argparse
from collections import defaultdict
import errno
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import urllib.request


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SUMSTATS = BASE_DIR / "data" / "gwas" / "tmp" / "lifespan_female_gwas_sumstats.tsv"
DEFAULT_GTF = BASE_DIR / "data" / "genes" / "Drosophila_melanogaster.BDGP6.54.62.chr.gtf.gz"
DEFAULT_BFILE = BASE_DIR / "data" / "gwas" / "tmp" / "merged_qc"
DEFAULT_MAGMA_BIN = BASE_DIR / "tools" / "magma" / "magma"
DEFAULT_OUT_DIR = BASE_DIR / "results" / "magma_female"
GO_GAF_URL = "https://current.geneontology.org/annotations/gaf/DROME-mod.gaf.gz"
GO_OBO_URL = "https://current.geneontology.org/ontology/go-basic.obo"
CHROM_MAP = {"2L": "1", "2R": "2", "3L": "3", "3R": "4", "4": "5", "X": "X", "23": "X"}
GENE_ID_PATTERN = re.compile(r'gene_id "([^"]+)"')
GENE_NAME_PATTERN = re.compile(r'gene_name "([^"]+)"')
GENE_BIOTYPE_PATTERN = re.compile(r'gene_biotype "([^"]+)"')


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run MAGMA gene and gene-set analysis for DGRP female lifespan."
    )
    parser.add_argument("--sumstats", type=Path, default=DEFAULT_SUMSTATS)
    parser.add_argument("--gtf", type=Path, default=DEFAULT_GTF)
    parser.add_argument("--bfile", type=Path, default=DEFAULT_BFILE)
    parser.add_argument("--magma-bin", type=Path, default=DEFAULT_MAGMA_BIN)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument(
        "--stage", choices=("all", "prepare", "annotate", "genes", "sets", "report"), default="all"
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
        *(Path(f"{args.bfile}{extension}") for extension in (".bed", ".bim", ".fam")),
    ]
    if args.stage != "prepare":
        required_files.append(args.magma_bin)
    if args.gene_sets is not None:
        required_files.append(args.gene_sets)
    if args.go_gaf is not None:
        required_files.extend((args.go_gaf, args.go_obo))
    require_files(required_files)
    if args.stage != "prepare" and not os.access(args.magma_bin, os.X_OK):
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


def require_files(paths: list[Path]) -> None:
    missing = [path for path in paths if not path.is_file() or path.stat().st_size == 0]
    if missing:
        raise FileNotFoundError(
            "Missing or empty MAGMA inputs:\n" + "\n".join(str(path) for path in missing)
        )


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
                with urllib.request.urlopen(url, timeout=120) as response, temporary.open(
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


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
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

    if args.stage in {"all", "prepare"}:
        validate_inputs(args)
        gene_locations = write_gene_locations(args.gtf, gene_locations)
        snp_locations = write_snp_locations(args.bfile, snp_locations)
        pvalues = write_magma_pvalues(args.sumstats, pvalues)
        bfile = prepare_magma_bfile(args.bfile, bfile)

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
                *(Path(f"{bfile}{extension}") for extension in (".bed", ".bim", ".fam")),
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


if __name__ == "__main__":
    main()
