import marimo

__generated_with = "0.17.8"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    return (mo,)


@app.cell
def __(mo):
    mo.md(r"""
    # deltaSVM Pipeline
    Human autosomal SNP allelic effects on transcription-factor binding, using the
    94 high-confidence human TF models from [ren-lab/deltaSVM](https://github.com/ren-lab/deltaSVM).
    The models were trained on [Yin et al., 2017](https://pubmed.ncbi.nlm.nih.gov/28473536/)
    and evaluated with [Yan et al., 2021](https://doi.org/10.1038/s41586-021-03211-0).
    This notebook does not supply fly TF models.

    Use Linux or WSL with Python 3.10 or later, Git, Make, GCC and G++.
    Install the notebook runner with `python -m pip install marimo`, then open it with
    `python -m marimo edit notebooks/deltaSVM_pipeline.py`.
    Run setup and input preparation first. Sections 4 and 5 are alternative scoring
    paths; sections 6–8 resume prepared runs. Opening this notebook starts no downloads or jobs.
    """)
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## Configuration and Sources
    The default workspace is `~/deltaSVM-work`. Set `DELTASVM_WORK_DIR` before opening
    the notebook to choose another location. All generated data stay in that workspace.

    | Input | Public source | Local override |
    |---|---|---|
    | GRCh38 reference | [NCBI GRCh38 no-alt analysis set](https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/001/405/GCA_000001405.15_GRCh38/seqs_for_alignment_pipelines.ucsc_ids/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.gz) | `DELTASVM_REFERENCE` (uncompressed FASTA) |
    | Human dbSNP variants | [NCBI dbSNP build 156, GRCh38 VCF](https://ftp.ncbi.nih.gov/snp/archive/b156/VCF/GCF_000001405.40.gz) | `DELTASVM_VCF` (gzip-compressed VCF) |
    | Models and code | [Ren lab repository](https://github.com/ren-lab/deltaSVM) | `DELTASVM_MODELS_DIR` |
    | Weights and thresholds | [Ren lab resource links](https://github.com/ren-lab/deltaSVM#models) | `DELTASVM_WEIGHTS`, `DELTASVM_OBS_THRESHOLDS`, `DELTASVM_PBS_THRESHOLDS` |
    | Human TF identifiers | [HGNC complete set](https://hgnc.genenames.org/download/) | `DELTASVM_HGNC_FILE` (TSV) |
    | Optional variant intersection | [Open Targets 25.03 GRCh38 variants](https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/25.03/output/variant/) | `DELTASVM_OT_DIR` |
    | gkmpredict | [Dongwon-Lee/lsgkm](https://github.com/Dongwon-Lee/lsgkm) | built in the workspace |

    Each download URL can be overridden with the corresponding `DELTASVM_*_URL`
    variable shown in the configuration below. The historical Ren lab resource server
    may be unavailable: supply existing weights and threshold files or working mirror
    URLs. Download errors stop the step; they never silently produce placeholder models.

    The default NCBI input is the fixed dbSNP build 156 GRCh38 VCF. The default
    filter retains variants marked `COMMON=1`, matching the common-variant intent of
    the original `00-common_all.vcf.gz` input. Set `DELTASVM_COMMON_ONLY=0` to include
    all biallelic SNVs. Set `DELTASVM_MAX_SNPS=1000` for a small first run; `0` processes the
    entire VCF. The full VCF is about 28 GB compressed and the reference is
    about 3 GB uncompressed; Open Targets adds several GB. The notebook records source
    URLs, software revisions, checksums and run settings in `provenance.json`.

    Set `DELTASVM_USE_OT=1` to restrict dbSNP variants to those represented in the
    Open Targets variant dataset. This is a selective phenotype-associated subset,
    not a comprehensive variant catalog, and its download is optional.
    `DELTASVM_CHROMOSOMES=1,2,3` selects chromosomes; the default is all 22 autosomes.
    `DELTASVM_CHROMOSOME=1` chooses the single-chromosome sections.
    `DELTASVM_TF_JOBS`, `DELTASVM_THREADS` and `DELTASVM_CHROM_JOBS` default to 1, 2 and 1.
    """)
    return


@app.cell
def __():
    import os
    import sys
    from pathlib import Path

    def configured_path(name, default):
        return Path(os.environ.get(name, str(default))).expanduser().resolve()

    workspace = configured_path("DELTASVM_WORK_DIR", Path.home() / "deltaSVM-work")
    chromosomes = tuple("chr" + c.removeprefix("chr") for c in os.environ.get(
        "DELTASVM_CHROMOSOMES", ",".join(map(str, range(1, 23)))
    ).split(","))
    if not chromosomes or any(c not in {f"chr{i}" for i in range(1, 23)} for c in chromosomes):
        raise ValueError("Select human autosomes 1 through 22.")
    if len(set(chromosomes)) != len(chromosomes):
        raise ValueError("Chromosomes must not be repeated.")
    single_chromosome = "chr" + os.environ.get("DELTASVM_CHROMOSOME", "1").removeprefix("chr")
    if single_chromosome not in chromosomes:
        raise ValueError("DELTASVM_CHROMOSOME must be in DELTASVM_CHROMOSOMES.")
    settings = {
        "workspace": workspace,
        "reference": configured_path("DELTASVM_REFERENCE", workspace / "resources/hs38/hs38.fa"),
        "vcf": configured_path("DELTASVM_VCF", workspace / "downloads/dbsnp-grch38.vcf.gz"),
        "models": configured_path("DELTASVM_MODELS_DIR", workspace / "vendor/deltaSVM/gkmsvm_models"),
        "weights": configured_path("DELTASVM_WEIGHTS", workspace / "resources/models.weights.txt"),
        "obs": configured_path("DELTASVM_OBS_THRESHOLDS", workspace / "resources/thresholds.obs.tsv"),
        "pbs": configured_path("DELTASVM_PBS_THRESHOLDS", workspace / "resources/thresholds.pbs.tsv"),
        "hgnc": configured_path("DELTASVM_HGNC_FILE", workspace / "resources/hgnc_complete_set.tsv"),
        "ot_dir": configured_path("DELTASVM_OT_DIR", workspace / "downloads/opentargets-25.03/variant"),
        "reference_url": os.environ.get("DELTASVM_REFERENCE_URL", "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/001/405/GCA_000001405.15_GRCh38/seqs_for_alignment_pipelines.ucsc_ids/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.gz"),
        "vcf_url": os.environ.get("DELTASVM_VCF_URL", "https://ftp.ncbi.nih.gov/snp/archive/b156/VCF/GCF_000001405.40.gz"),
        "weights_url": os.environ.get("DELTASVM_WEIGHTS_URL", "https://renlab.sdsc.edu/yunjiang/deltaSVM/resources/models.weights.txt"),
        "obs_url": os.environ.get("DELTASVM_OBS_THRESHOLDS_URL", "https://renlab.sdsc.edu/yunjiang/deltaSVM/resources/threhsolds.obs.tsv"),
        "pbs_url": os.environ.get("DELTASVM_PBS_THRESHOLDS_URL", "https://renlab.sdsc.edu/yunjiang/deltaSVM/resources/threhsolds.pbs.tsv"),
        "hgnc_url": os.environ.get("DELTASVM_HGNC_URL", "https://storage.googleapis.com/public-download-files/hgnc/tsv/tsv/hgnc_complete_set.txt"),
        "ot_url": os.environ.get("DELTASVM_OT_URL", "https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/25.03/output/variant/"),
        "ren_revision": os.environ.get("DELTASVM_REN_REVISION", "566b29557fa7fc37d24a94057a1bcdf08cdae000"),
        "lsgkm_revision": os.environ.get("DELTASVM_LSGKM_REVISION", "d7ef90f7cb326b190619c51a13843da48c99e239"),
        "ot_local": "DELTASVM_OT_DIR" in os.environ,
        "chromosomes": chromosomes,
        "single": single_chromosome,
        "use_ot": os.environ.get("DELTASVM_USE_OT", "0") == "1",
        "common_only": os.environ.get("DELTASVM_COMMON_ONLY", "1") == "1",
        "max_snps": int(os.environ.get("DELTASVM_MAX_SNPS", "0")),
        "tf_jobs": int(os.environ.get("DELTASVM_TF_JOBS", "1")),
        "threads": int(os.environ.get("DELTASVM_THREADS", "2")),
        "chrom_jobs": int(os.environ.get("DELTASVM_CHROM_JOBS", "1")),
    }
    if settings["max_snps"] < 0 or any(settings[k] < 1 for k in ("tf_jobs", "threads", "chrom_jobs")):
        raise ValueError("Worker counts must be positive and MAX_SNPS must be nonnegative.")
    return Path, settings, sys


@app.cell
def __(Path, settings):
    import hashlib
    import json
    import shutil
    import subprocess
    import urllib.request
    import urllib.parse
    import gzip
    import tempfile
    from concurrent.futures import ThreadPoolExecutor
    from functools import lru_cache

    def file_digest(path, algorithm="sha256"):
        path = Path(path)
        stat = path.stat()
        return digest_cached(str(path), stat.st_size, stat.st_mtime_ns, algorithm)

    @lru_cache(maxsize=512)
    def digest_cached(path, size, modified, algorithm):
        digest = hashlib.new(algorithm)
        with Path(path).open("rb") as stream:
            for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @lru_cache(maxsize=2)
    def supported_kmers(path, size, modified):
        supported = set()
        complement = str.maketrans("ACGT", "TGCA")
        with Path(path).open() as stream:
            for line in stream:
                row = line.split()
                if len(row) != 95 or len(row[0]) != 10 or set(row[0]) - set("ACGT"):
                    raise ValueError("Invalid or incomplete deltaSVM weight row; expected a 10-mer and 94 scores.")
                for value in row[1:]:
                    float(value)
                supported.add(row[0])
                supported.add(row[0].translate(complement)[::-1])
        if not supported:
            raise ValueError("The deltaSVM weight table is empty.")
        return supported

    def record_source(name, path, **details):
        destination = settings["workspace"] / "provenance.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        manifest = json.loads(destination.read_text()) if destination.exists() else {}
        manifest["settings"] = {k: str(v) if isinstance(v, Path) else v for k, v in settings.items()}
        manifest.setdefault("sources", {})[name] = {"path": str(path), **details}
        temporary = destination.with_suffix(".json.part")
        temporary.write_text(json.dumps(manifest, indent=2) + "\n")
        temporary.replace(destination)

    def download_file(url, destination, expected_md5=None):
        import re
        import time

        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.is_file() or destination.stat().st_size == 0:
            temporary = destination.with_name(destination.name + ".part")
            try:
                offset = temporary.stat().st_size if temporary.is_file() else 0
                headers = {"User-Agent": "deltaSVM-notebook"}
                if offset:
                    headers["Range"] = f"bytes={offset}-"
                request = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(request, timeout=120) as response:
                    status = getattr(response, "status", response.getcode())
                    content_range = response.headers.get("Content-Range", "")
                    match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+|\*)", content_range)
                    if status == 206:
                        if not match or int(match.group(1)) != offset:
                            raise ValueError(f"Unexpected partial response for {url}: {content_range}")
                        total = int(match.group(3)) if match.group(3) != "*" else None
                        mode = "ab" if offset else "wb"
                    elif status == 200:
                        offset = 0
                        total = int(response.headers["Content-Length"]) if response.headers.get("Content-Length") else None
                        mode = "wb"
                    else:
                        raise ValueError(f"Unexpected HTTP status {status} for {url}")
                    received = offset
                    last_report = time.monotonic()
                    with temporary.open(mode) as output:
                        while True:
                            chunk = response.read(8 * 1024 * 1024)
                            if not chunk:
                                break
                            output.write(chunk)
                            received += len(chunk)
                            now = time.monotonic()
                            if now - last_report >= 5:
                                if total:
                                    print(f"{destination.name}: {received:,}/{total:,} bytes ({100 * received / total:.1f}%)", flush=True)
                                else:
                                    print(f"{destination.name}: {received:,} bytes", flush=True)
                                last_report = now
                if not temporary.is_file() or not temporary.stat().st_size or (total and temporary.stat().st_size != total):
                    raise ValueError(f"Incomplete download: {url}")
                if expected_md5 and file_digest(temporary, "md5") != expected_md5:
                    temporary.unlink(missing_ok=True)
                    raise ValueError(f"Checksum mismatch: {url}")
                temporary.replace(destination)
            except Exception as error:
                raise RuntimeError(f"Cannot download {url}. Supply a local file or a working URL override.") from error
        if expected_md5 and file_digest(destination, "md5") != expected_md5:
            raise ValueError(f"Checksum mismatch for cached file: {destination}")
        return destination

    def fetch_input(name, path, url):
        existed = path.is_file() and path.stat().st_size > 0
        download_file(url, path)
        record_source(name, path, status="existing local file" if existed else "downloaded",
                      configured_url=url, sha256=file_digest(path))
        return path

    def execute(command, cwd=None, log=None):
        print(" ".join(map(str, command)), flush=True)
        if log is None:
            subprocess.run(list(map(str, command)), cwd=cwd, check=True)
        else:
            Path(log).parent.mkdir(parents=True, exist_ok=True)
            with Path(log).open("w") as stream:
                subprocess.run(list(map(str, command)), cwd=cwd, stdout=stream, stderr=subprocess.STDOUT, check=True)

    def clone_source(name, url, revision):
        directory = settings["workspace"] / "vendor" / name
        directory.parent.mkdir(parents=True, exist_ok=True)
        if not directory.exists():
            execute(["git", "clone", url, directory])
            execute(["git", "checkout", revision], cwd=directory)
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=directory, text=True).strip()
        record_source(name, directory, url=url, requested_revision=revision, commit=actual)
        return directory

    return ThreadPoolExecutor, clone_source, download_file, execute, fetch_input, file_digest, gzip, json, record_source, shutil, supported_kmers, tempfile, urllib


@app.cell
def __(mo):
    mo.md(r"""
    ## 0a. Download and Build Software
    This step installs missing Python libraries in the notebook's interpreter, clones
    the two upstream repositories at pinned commits and compiles Linux binaries.
    Software revision overrides are supported; the resolved commits are recorded.
    It leaves existing source checkouts in place.
    """)
    return


@app.cell
def __(mo):
    software_button = mo.ui.run_button(label='Install and build software')
    software_button
    return (software_button,)


@app.cell
def __(mo, software_button, clone_source, execute, settings, shutil, sys):
    mo.stop(not software_button.value, mo.md("Press the button above to run this step."))
    import importlib.util
    import platform

    if platform.system() != "Linux":
        raise RuntimeError("The supplied gkm-SVM tools require Linux; use Linux or WSL.")
    _missing_tools = [name for name in ("git", "make", "gcc", "g++") if not shutil.which(name)]
    if _missing_tools:
        raise RuntimeError("Install these system tools first: " + ", ".join(_missing_tools))
    _packages = {"pysam": "pysam>=0.23,<0.24", "pandas": "pandas>=2.2,<3", "pyarrow": "pyarrow>=17,<22"}
    _missing_packages = [package for module, package in _packages.items() if importlib.util.find_spec(module) is None]
    if _missing_packages:
        execute([sys.executable, "-m", "pip", "install", *_missing_packages])
    _ren = clone_source("deltaSVM", "https://github.com/ren-lab/deltaSVM.git", settings["ren_revision"])
    _lsgkm = clone_source("lsgkm", "https://github.com/Dongwon-Lee/lsgkm.git", settings["lsgkm_revision"])
    _scripts = settings["workspace"] / "scripts"
    _scripts.mkdir(parents=True, exist_ok=True)
    execute(["make"], cwd=_lsgkm / "src")
    shutil.copy2(_lsgkm / "bin/gkmpredict", _scripts / "gkmpredict")
    execute(["g++", "-std=c++11", "-O3", "-o", _scripts / "deltasvm_subset_multi", _ren / "scripts/deltasvm_subset_multi.cpp"])
    print("Software ready.")
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 0b. Download Reference, Weights and Thresholds
    The reference is GRCh38 with UCSC chromosome names. Downloaded gzip data are
    expanded once and indexed with pysam. Existing files supplied in Configuration
    are reused. Interrupted downloads resume when the server supports byte ranges;
    otherwise the downloader restarts the file. Download progress and integrity checks
    are reported. The model and threshold checks require 94 matching human TFs.
    The order of the PBS thresholds defines the columns of the deltaSVM weight table.
    """)
    return


@app.cell
def __(settings):
    def load_thresholds():
        obs = [line.split() for line in settings["obs"].read_text().splitlines() if line.strip()]
        pbs = [line.split() for line in settings["pbs"].read_text().splitlines() if line.strip()]
        if len(obs) != 94 or len(pbs) != 94 or any(len(row) != 3 for row in obs) or any(len(row) != 2 for row in pbs):
            raise ValueError("Expected 94 OBS rows (TF, threshold, model) and 94 PBS rows (TF, threshold).")
        if len({r[0] for r in obs}) != 94 or {r[0] for r in obs} != {r[0] for r in pbs}:
            raise ValueError("OBS and PBS files must contain the same 94 unique TFs.")
        for row in obs:
            float(row[1])
            if not (settings["models"] / (row[2] + ".model.txt")).is_file():
                raise FileNotFoundError(f"Missing gkm-SVM model: {row[2]}")
        for row in pbs:
            float(row[1])
        return obs, pbs

    return (load_thresholds,)


@app.cell
def __(mo):
    resources_button = mo.ui.run_button(label='Download and validate reference and models')
    resources_button
    return (resources_button,)


@app.cell
def __(mo, resources_button, download_file, fetch_input, file_digest, gzip, load_thresholds, record_source, settings, shutil):
    mo.stop(not resources_button.value, mo.md("Press the button above to run this step."))
    import pysam

    _reference = settings["reference"]
    if not _reference.is_file() or _reference.stat().st_size == 0:
        _compressed = download_file(settings["reference_url"], settings["workspace"] / "downloads/grch38.fa.gz")
        _reference.parent.mkdir(parents=True, exist_ok=True)
        _temporary = _reference.with_name(_reference.name + ".part")
        with gzip.open(_compressed, "rb") as _source, _temporary.open("wb") as _destination:
            shutil.copyfileobj(_source, _destination, length=8 * 1024 * 1024)
        _temporary.replace(_reference)
        record_source("reference", _reference, url=settings["reference_url"], sha256=file_digest(_reference))
    else:
        record_source("reference", _reference, status="existing local file", sha256=file_digest(_reference))
    pysam.faidx(str(_reference))
    with pysam.FastaFile(str(_reference)) as _genome:
        if _genome.get_reference_length("chr1") != 248956422:
            raise ValueError("The supplied reference is not the expected human GRCh38 reference.")
    for _name in ("weights", "obs", "pbs"):
        fetch_input(_name, settings[_name], settings[_name + "_url"])
    _obs, _pbs = load_thresholds()
    with settings["weights"].open() as _weights:
        _first = _weights.readline().split()
    if len(_first) != 95 or len(_first[0]) != 10 or set(_first[0]) - set("ACGT"):
        raise ValueError("Expected a 10-mer and 94 scores in the weight table.")
    for _score in _first[1:]:
        float(_score)
    print("Reference, 94 models, thresholds and weights ready.")
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 1. Download dbSNP and Prepare Chromosome Batches
    Downloads the GRCh38 dbSNP VCF when no local VCF is supplied. For the default NCBI
    source, its published MD5 is verified. RefSeq accession names are converted to
    `chr1`–`chr22`. Only biallelic A/C/G/T SNVs on selected autosomes are kept.
    The VCF header must identify GRCh38. SNP batches and rsID mappings are generated
    together; outputs are published only after successful extraction.
    """)
    return


@app.cell
def __(Path, download_file, file_digest, gzip, json, record_source, settings, tempfile, urllib):
    def prepare_snps():
        source = settings["vcf"]
        existed = source.is_file() and source.stat().st_size > 0
        expected_md5 = None
        if not existed and settings["vcf_url"].startswith("https://ftp.ncbi.nih.gov/snp/"):
            with urllib.request.urlopen(settings["vcf_url"] + ".md5", timeout=60) as response:
                expected_md5 = response.read().decode().split()[0]
        download_file(settings["vcf_url"], source, expected_md5)
        record_source("dbsnp", source, status="existing local file" if existed else "downloaded",
                      configured_url=settings["vcf_url"], sha256=file_digest(source), md5=expected_md5)
        root = settings["workspace"]
        batches = root / "snp_batches"
        batches.mkdir(parents=True, exist_ok=True)
        selected = set(settings["chromosomes"])
        counts = {chrom: 0 for chrom in selected}
        written = 0
        build_ok = False
        with tempfile.TemporaryDirectory(dir=root, prefix="prepare-") as temporary:
            temporary = Path(temporary)
            outputs = {chrom: (temporary / f"{chrom}.tsv").open("w") for chrom in selected}
            try:
                with gzip.open(source, "rt") as vcf, (temporary / "rsid_map.tsv").open("w") as mapping:
                    mapping.write("variant\trsId\n")
                    for line in vcf:
                        if line.startswith("##"):
                            if "GRCh38" in line or "GCF_000001405.40" in line:
                                build_ok = True
                            continue
                        if line.startswith("#"):
                            continue
                        if not build_ok:
                            raise ValueError("VCF header does not identify GRCh38; do not mix genome builds.")
                        fields = line.rstrip().split("\t")
                        contig, pos, rsid, ref, alt = fields[:5]
                        if contig.startswith("NC_"):
                            number = contig.split(".")[0].removeprefix("NC_")
                            chrom = f"chr{int(number)}" if number.isdigit() else contig
                        else:
                            chrom = "chr" + contig.removeprefix("chr")
                        if chrom not in selected or ref not in "ACGT" or alt not in "ACGT" or len(ref) != 1 or len(alt) != 1 or ref == alt:
                            continue
                        if settings["common_only"] and "COMMON=1" not in fields[7].split(";"):
                            continue
                        variant = f"{chrom}_{pos}_{ref}_{alt}"
                        outputs[chrom].write(variant + "\n")
                        mapping.write(f"{variant}\t{rsid}\n")
                        counts[chrom] += 1
                        written += 1
                        if settings["max_snps"] and written >= settings["max_snps"]:
                            break
            finally:
                for output in outputs.values():
                    output.close()
            if not build_ok or not written:
                raise ValueError("No GRCh38 SNVs matched the selected chromosomes and filters.")
            for chrom in selected:
                (temporary / f"{chrom}.tsv").replace(batches / f"{chrom}.tsv")
            (temporary / "rsid_map.tsv").replace(root / "rsid_map.tsv")
        record_source("snp_batches", batches, counts=counts, common_only=settings["common_only"], max_snps=settings["max_snps"])
        print(f"Prepared {written:,} SNPs: {counts}")

    return (prepare_snps,)


@app.cell
def __(mo):
    snps_button = mo.ui.run_button(label='Download and prepare SNPs')
    snps_button
    return (snps_button,)


@app.cell
def __(mo, snps_button, prepare_snps):
    mo.stop(not snps_button.value, mo.md("Press the button above to run this step."))
    prepare_snps()
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 2. Download and Filter Against Open Targets
    Downloads all Parquet partitions from the pinned 25.03 variant directory, or
    uses `DELTASVM_OT_DIR`. Only the `variantId` column is read. A disk-backed SQLite
    index avoids loading the whole dataset into memory. Variant IDs are converted
    from `1_position_ref_alt` to `chr1_position_ref_alt` before intersection.
    With `DELTASVM_USE_OT=0`, this step is skipped and raw dbSNP batches are used.
    """)
    return


@app.cell
def __(Path, download_file, file_digest, json, record_source, settings, urllib):
    import sqlite3
    from html.parser import HTMLParser

    class ParquetLinks(HTMLParser):
        def __init__(self):
            super().__init__()
            self.links = []

        def handle_starttag(self, tag, attrs):
            href = dict(attrs).get("href", "")
            if tag == "a" and href.endswith(".parquet"):
                self.links.append(href)

    def filter_opentargets():
        import pyarrow.parquet as pq

        if not settings["use_ot"]:
            print("Open Targets filtering disabled; scoring raw dbSNP batches.")
            return
        directory = settings["ot_dir"]
        directory.mkdir(parents=True, exist_ok=True)
        files = sorted(directory.glob("*.parquet"))
        if not settings["ot_local"] or not files:
            parser = ParquetLinks()
            with urllib.request.urlopen(settings["ot_url"], timeout=120) as response:
                parser.feed(response.read().decode())
            urls = sorted({urllib.parse.urljoin(settings["ot_url"], link) for link in parser.links})
            if not urls:
                raise ValueError("No Parquet partitions found at the configured Open Targets URL.")
            for url in urls:
                download_file(url, directory / Path(urllib.parse.urlparse(url).path).name)
            files = sorted(directory.glob("*.parquet"))
        record_source("opentargets", directory, configured_url=settings["ot_url"],
                      files={f.name: file_digest(f) for f in files}, assembly="GRCh38")
        index = settings["workspace"] / "opentargets.sqlite"
        temporary = index.with_suffix(".sqlite.part")
        if temporary.exists():
            temporary.unlink()
        with sqlite3.connect(temporary) as connection:
            connection.execute("CREATE TABLE variants (id TEXT PRIMARY KEY) WITHOUT ROWID")
            for source in files:
                for batch in pq.ParquetFile(source).iter_batches(columns=["variantId"], batch_size=65536):
                    values = [("chr" + str(v).removeprefix("chr"),) for v in batch.column(0).to_pylist() if v is not None]
                    connection.executemany("INSERT OR IGNORE INTO variants VALUES (?)", values)
                connection.commit()
                print(f"Indexed {source.name}", flush=True)
            for chrom in settings["chromosomes"]:
                source = settings["workspace"] / "snp_batches" / f"{chrom}.tsv"
                destination = source.with_name(f"{chrom}_ot.tsv")
                temporary_output = destination.with_suffix(".tsv.part")
                kept = 0
                with source.open() as input_stream, temporary_output.open("w") as output:
                    for line in input_stream:
                        if connection.execute("SELECT 1 FROM variants WHERE id=?", (line.strip(),)).fetchone():
                            output.write(line)
                            kept += 1
                temporary_output.replace(destination)
                destination.with_suffix(".inputs.json").write_text(json.dumps({"source_sha256": file_digest(source)}) + "\n")
                print(f"{chrom}: {kept:,} SNPs retained")
        temporary.replace(index)

    return filter_opentargets, sqlite3


@app.cell
def __(mo):
    ot_button = mo.ui.run_button(label='Download and filter Open Targets variants')
    ot_button
    return (ot_button,)


@app.cell
def __(mo, ot_button, filter_opentargets):
    mo.stop(not ot_button.value, mo.md("Press the button above to run this step."))
    filter_opentargets()
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 3. Prepare a Chromosome and Compute deltaSVM Scores
    Select the chromosome with `DELTASVM_CHROMOSOME`. Resources are read from the
    shared workspace, avoiding a full genome copy per run. REF alleles must match
    GRCh38. Boundary variants and sequences containing ambiguous bases are reported
    in `data/skipped.tsv` and excluded before scoring. Inputs and settings are hashed
    so changing them cannot silently reuse an old run.
    """)
    return


@app.cell
def __(Path, execute, file_digest, json, load_thresholds, settings, shutil, supported_kmers):
    def batch_path(chrom):
        if chrom not in settings["chromosomes"]:
            raise ValueError(f"Chromosome not selected: {chrom}")
        suffix = "_ot" if settings["use_ot"] else ""
        source = settings["workspace"] / "snp_batches" / f"{chrom}{suffix}.tsv"
        if settings["use_ot"]:
            metadata = source.with_suffix(".inputs.json")
            raw = source.with_name(f"{chrom}.tsv")
            if not metadata.is_file() or json.loads(metadata.read_text())["source_sha256"] != file_digest(raw):
                raise RuntimeError(f"{chrom}: Open Targets intersection is missing or stale; run section 2 again.")
        return source

    def run_signature(chrom):
        paths = [batch_path(chrom), settings["reference"], settings["weights"], settings["obs"], settings["pbs"]]
        obs, _ = load_thresholds()
        paths += [settings["models"] / (row[2] + ".model.txt") for row in obs]
        return {str(path): file_digest(path) for path in paths}

    def validate_run(chrom):
        directory = settings["workspace"] / "runs" / chrom
        metadata = directory / "inputs.json"
        if not metadata.is_file() or json.loads(metadata.read_text()) != run_signature(chrom):
            raise RuntimeError(f"{chrom}: run inputs changed or were not prepared; run section 3 again.")
        if not (directory / "out/pbs.pred.tsv").is_file() or not (directory / "out/pbs.pred.tsv").stat().st_size:
            raise RuntimeError(f"{chrom}: missing deltaSVM scores; run section 3 again.")
        return directory

    def setup_chromosome(chrom):
        import pysam

        batch = batch_path(chrom)
        if not batch.is_file() or not batch.stat().st_size:
            raise ValueError(f"Missing or empty SNP batch: {batch}")
        directory = settings["workspace"] / "runs" / chrom
        signature = run_signature(chrom)
        metadata = directory / "inputs.json"
        if metadata.exists() and json.loads(metadata.read_text()) != signature:
            raise RuntimeError(f"{chrom}: existing run uses different inputs. Move that run directory aside or use a new workspace.")
        for name in ("data", "tmp", "out", "log"):
            (directory / name).mkdir(parents=True, exist_ok=True)
        shutil.copy2(batch, directory / "input_snp.tsv")
        if not metadata.exists():
            weight_stat = settings["weights"].stat()
            supported = supported_kmers(str(settings["weights"]), weight_stat.st_size, weight_stat.st_mtime_ns)
            with pysam.FastaFile(str(settings["reference"])) as genome, batch.open() as variants, \
                 (directory / "data/selex_allelic_oligos.ref.fa").open("w") as reference, \
                 (directory / "data/selex_allelic_oligos.alt.fa").open("w") as alternate, \
                 (directory / "data/skipped.tsv").open("w") as skipped:
                skipped.write("variant\treason\n")
                accepted = 0
                for line in variants:
                    variant = line.strip()
                    contig, position, ref, alt = variant.split("_")
                    position = int(position)
                    if contig != chrom or len(ref) != 1 or len(alt) != 1 or ref not in "ACGT" or alt not in "ACGT":
                        raise ValueError(f"Invalid SNP in {chrom} batch: {variant}")
                    if position < 20 or position + 20 > genome.get_reference_length(contig):
                        skipped.write(f"{variant}\tchromosome boundary\n")
                        continue
                    sequence = genome.fetch(contig, position - 20, position + 20).upper()
                    if sequence[19] != ref:
                        skipped.write(f"{variant}\tREF does not match GRCh38\n")
                        continue
                    if set(sequence) - set("ACGT"):
                        skipped.write(f"{variant}\tambiguous reference bases\n")
                        continue
                    alt_sequence = sequence[:19] + alt + sequence[20:]
                    if any(seq[i:i + 10] not in supported for seq in (sequence, alt_sequence) for i in range(31)):
                        raise ValueError(f"{variant}: weights missing for a required 10-mer; supply the complete weight table.")
                    reference.write(f">{variant}\n{sequence}\n")
                    alternate.write(f">{variant}\n{alt_sequence}\n")
                    accepted += 1
            if not accepted:
                raise ValueError(f"{chrom}: no scoreable SNP sequences; see data/skipped.tsv")
            metadata.write_text(json.dumps(signature, indent=2) + "\n")
        output = directory / "out/pbs.pred.tsv"
        if not output.exists() or not output.stat().st_size:
            temporary = output.with_suffix(".tsv.part")
            execute([settings["workspace"] / "scripts/deltasvm_subset_multi",
                     directory / "data/selex_allelic_oligos.ref.fa", directory / "data/selex_allelic_oligos.alt.fa",
                     settings["weights"], temporary, settings["pbs"]], log=directory / "log/deltasvm.log")
            if not temporary.stat().st_size:
                raise ValueError(f"{chrom}: scoring produced no output.")
            execute(["sort", "-k1,1", "-k3,3", "-o", temporary, temporary])
            temporary.replace(output)
        print(f"{chrom}: sequences and deltaSVM scores ready.")
        return directory

    return batch_path, run_signature, setup_chromosome, validate_run


@app.cell
def __(mo):
    setup_button = mo.ui.run_button(label='Prepare selected chromosome')
    setup_button
    return (setup_button,)


@app.cell
def __(mo, setup_button, settings, setup_chromosome):
    mo.stop(not setup_button.value, mo.md("Press the button above to run this step."))
    setup_chromosome(settings["single"])
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 4. Run Selected Chromosomes in Parallel
    Prepares, scores and exports each nonempty selected SNP batch. Chromosome jobs
    and TF jobs are bounded by Configuration. Every worker failure is propagated.
    Completed runs are reused only when their input fingerprints match.
    """)
    return


@app.cell
def __(ThreadPoolExecutor, execute, load_thresholds, settings, validate_run):
    def predict_chromosome(chrom):
        directory = validate_run(chrom)
        obs, _ = load_thresholds()

        def predict_tf(row):
            tf, threshold, model = row
            merged = directory / "tmp" / f"{tf}.merge.gkm.tsv"
            if merged.is_file() and merged.stat().st_size:
                return
            outputs = []
            for allele in ("ref", "alt"):
                output = directory / "tmp" / f"{tf}.{allele}.gkm.tsv"
                temporary = output.with_suffix(".tsv.part")
                execute([settings["workspace"] / "scripts/gkmpredict", "-T", settings["threads"],
                         directory / "data" / f"selex_allelic_oligos.{allele}.fa",
                         settings["models"] / f"{model}.model.txt", temporary],
                        log=directory / "log" / f"{model}.{allele}.log")
                execute(["sort", "-k1,1", "-o", temporary, temporary])
                temporary.replace(output)
                outputs.append(output)
            temporary = merged.with_suffix(".tsv.part")
            with outputs[0].open() as reference, outputs[1].open() as alternate, temporary.open("w") as output:
                from itertools import zip_longest
                for ref_line, alt_line in zip_longest(reference, alternate):
                    if ref_line is None or alt_line is None:
                        raise ValueError(f"{tf}: allele prediction lengths differ.")
                    ref_id, ref_score = ref_line.rstrip().split("\t")
                    alt_id, alt_score = alt_line.rstrip().split("\t")
                    if ref_id != alt_id:
                        raise ValueError(f"{tf}: allele SNP identifiers differ.")
                    output.write(f"{ref_id}\t{ref_score}\t{alt_score}\n")
            if not temporary.stat().st_size:
                raise ValueError(f"{tf}: empty gkm-SVM predictions.")
            temporary.replace(merged)
        with ThreadPoolExecutor(max_workers=settings["tf_jobs"]) as pool:
            list(pool.map(predict_tf, obs))
        print(f"{chrom}: all TF predictions complete.")

    return (predict_chromosome,)


@app.cell
def __(Path, execute, load_thresholds, settings, validate_run):
    def summarize_chromosome(chrom):
        from itertools import zip_longest

        directory = validate_run(chrom)
        obs, _ = load_thresholds()
        observed = directory / "out/obs.pred.tsv"
        temporary_obs = observed.with_suffix(".tsv.part")
        with temporary_obs.open("w") as output:
            for tf, threshold, model in obs:
                with (directory / "tmp" / f"{tf}.merge.gkm.tsv").open() as predictions:
                    for line in predictions:
                        snp, ref, alt = line.rstrip().split("\t")
                        bound = "Y" if max(float(ref), float(alt)) >= float(threshold) else "N"
                        output.write(f"{snp}\t{tf}\t{ref}\t{alt}\t{bound}\n")
        execute(["sort", "-k1,1", "-k2,2", "-o", temporary_obs, temporary_obs])
        temporary_obs.replace(observed)
        summary = directory / "out/summary.pred.tsv"
        temporary = summary.with_suffix(".tsv.part")
        with observed.open() as obs_stream, (directory / "out/pbs.pred.tsv").open() as pbs_stream, temporary.open("w") as output:
            output.write("snp\ttf\tallele1_bind\tallele2_bind\tseq_binding\tdeltaSVM_score\tpreferred_allele\n")
            for obs_line, pbs_line in zip_longest(obs_stream, pbs_stream):
                if obs_line is None or pbs_line is None:
                    raise ValueError("OBS and PBS prediction counts differ.")
                snp, tf, ref, alt, bound = obs_line.rstrip().split("\t")
                pbs_snp, score, pbs_tf, effect = pbs_line.rstrip().split("\t")
                if (snp, tf) != (pbs_snp, pbs_tf):
                    raise ValueError("OBS and PBS SNP/TF identifiers differ.")
                output.write(f"{snp}\t{tf}\t{ref}\t{alt}\t{bound}\t{score}\t{effect}\n")
        temporary.replace(summary)
        return summary

    def export_chromosome(chrom):
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        directory = validate_run(chrom)
        summary = directory / "out/summary.pred.tsv"
        destination = directory / "out" / f"{chrom}_deltasvm.parquet"
        temporary = destination.with_suffix(".parquet.part")
        schema = pa.schema([(name, pa.float64() if name == "Score" else pa.string()) for name in
                            ("variant", "rsId", "TfId", "TfName", "Effect", "Score")])
        rows = 0
        with pq.ParquetWriter(temporary, schema) as writer:
            for chunk in pd.read_csv(summary, sep="\t", chunksize=100000):
                chunk = chunk.rename(columns={"snp": "variant", "tf": "TfName", "preferred_allele": "Effect", "deltaSVM_score": "Score"})
                chunk = chunk.loc[chunk["Effect"].isin(["Gain", "Loss"])].copy()
                chunk["rsId"] = "."
                chunk["TfId"] = "."
                chunk = chunk[["variant", "rsId", "TfId", "TfName", "Effect", "Score"]]
                writer.write_table(pa.Table.from_pandas(chunk, schema=schema, preserve_index=False))
                rows += len(chunk)
        temporary.replace(destination)
        print(f"{chrom}: wrote {rows:,} Gain/Loss rows to {destination}")
        return destination

    return export_chromosome, summarize_chromosome


@app.cell
def __(ThreadPoolExecutor, batch_path, export_chromosome, predict_chromosome, settings, setup_chromosome, summarize_chromosome):
    def complete_chromosome(chrom):
        setup_chromosome(chrom)
        predict_chromosome(chrom)
        summarize_chromosome(chrom)
        return export_chromosome(chrom)

    def selected_nonempty():
        selected = []
        for chrom in settings["chromosomes"]:
            source = batch_path(chrom)
            if not source.is_file():
                raise FileNotFoundError(f"Run SNP preparation/filtering first: {source}")
            if source.stat().st_size:
                selected.append(chrom)
        if not selected:
            raise ValueError("No nonempty SNP batches to score.")
        return selected

    def run_parallel():
        with ThreadPoolExecutor(max_workers=settings["chrom_jobs"]) as pool:
            return list(pool.map(complete_chromosome, selected_nonempty()))

    return complete_chromosome, run_parallel, selected_nonempty


@app.cell
def __(mo):
    parallel_button = mo.ui.run_button(label='Run selected chromosomes')
    parallel_button
    return (parallel_button,)


@app.cell
def __(mo, parallel_button, run_parallel):
    mo.stop(not parallel_button.value, mo.md("Press the button above to run this step."))
    run_parallel()
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 5. Single Chromosome Full Run
    Runs preparation, deltaSVM, all 94 gkm-SVM models, summary and Parquet export
    for the configured chromosome. No existing run directories are deleted.
    """)
    return


@app.cell
def __(mo):
    single_button = mo.ui.run_button(label='Run one chromosome')
    single_button
    return (single_button,)


@app.cell
def __(mo, single_button, complete_chromosome, settings):
    mo.stop(not single_button.value, mo.md("Press the button above to run this step."))
    complete_chromosome(settings["single"])
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 6. Resume gkmpredict Across Selected Chromosomes
    Requires completed deltaSVM scoring from section 3. Missing or failed score files
    produce an error immediately. Completed nonempty TF prediction files are reused.
    """)
    return


@app.cell
def __(mo):
    gkm_button = mo.ui.run_button(label='Resume gkm-SVM predictions')
    gkm_button
    return (gkm_button,)


@app.cell
def __(mo, gkm_button, ThreadPoolExecutor, predict_chromosome, selected_nonempty, settings):
    mo.stop(not gkm_button.value, mo.md("Press the button above to run this step."))
    with ThreadPoolExecutor(max_workers=settings["chrom_jobs"]) as _pool:
        list(_pool.map(predict_chromosome, selected_nonempty()))
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 7. Restart and Complete a Chromosome Run
    Validates the prepared run, fills missing TF predictions, then rebuilds its
    summary and Parquet output. Source inputs must still match the prepared run.
    """)
    return


@app.cell
def __(mo):
    restart_button = mo.ui.run_button(label='Complete selected prepared run')
    restart_button
    return (restart_button,)


@app.cell
def __(mo, restart_button, export_chromosome, predict_chromosome, settings, summarize_chromosome):
    mo.stop(not restart_button.value, mo.md("Press the button above to run this step."))
    predict_chromosome(settings["single"])
    summarize_chromosome(settings["single"])
    export_chromosome(settings["single"])
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 8. Orchestrate Full Pipeline Sequentially
    Completes every selected nonempty chromosome batch in order. Each chromosome
    is prepared if necessary; failures stop the pipeline before starting the next one.
    """)
    return


@app.cell
def __(mo):
    pipeline_button = mo.ui.run_button(label='Run selected chromosomes sequentially')
    pipeline_button
    return (pipeline_button,)


@app.cell
def __(mo, pipeline_button, complete_chromosome, selected_nonempty):
    mo.stop(not pipeline_button.value, mo.md("Press the button above to run this step."))
    for _chrom in selected_nonempty():
        complete_chromosome(_chrom)
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 9. Check Pipeline Progress
    Shows prepared score files, TF prediction counts, exported results and whether
    the run's input fingerprint still matches the selected source data.
    """)
    return


@app.cell
def __(mo):
    progress_button = mo.ui.run_button(label='Check progress')
    progress_button
    return (progress_button,)


@app.cell
def __(mo, progress_button, json, run_signature, settings):
    mo.stop(not progress_button.value, mo.md("Press the button above to run this step."))
    for _chrom in settings["chromosomes"]:
        _directory = settings["workspace"] / "runs" / _chrom
        _count = sum(p.stat().st_size > 0 for p in (_directory / "tmp").glob("*.merge.gkm.tsv"))
        _pbs = _directory / "out/pbs.pred.tsv"
        _parquet = _directory / "out" / f"{_chrom}_deltasvm.parquet"
        _metadata = _directory / "inputs.json"
        try:
            _matches = _metadata.is_file() and json.loads(_metadata.read_text()) == run_signature(_chrom)
        except (FileNotFoundError, ValueError):
            _matches = False
        print(f"{_chrom}: {_count}/94 TFs | PBS={_pbs.is_file() and _pbs.stat().st_size > 0} | Parquet={_parquet.is_file()} | inputs match={_matches}")
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 10. Convert the Selected Chromosome Results to Parquet
    Exports Gain/Loss predictions from `runs/chrN/out/summary.pred.tsv` in bounded
    chunks. The next section can attach dbSNP rsIDs from `rsid_map.tsv`. `TfId` remains
    `.` because the Ren Lab models do not supply stable TF identifiers. Predictions describe the supplied human
    TF models and do not provide fly-specific biological validation.
    """)
    return


@app.cell
def __(mo):
    export_button = mo.ui.run_button(label='Export selected chromosome')
    export_button
    return (export_button,)


@app.cell
def __(mo, export_button, export_chromosome, settings):
    mo.stop(not export_button.value, mo.md("Press the button above to run this step."))
    export_chromosome(settings["single"])
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 11. Attach dbSNP rsIDs to the Selected Chromosome Results
    Uses the rsID map produced during variant preparation to replace placeholder rsIDs
    in the selected chromosome's Parquet file. The mapping is indexed in SQLite on
    disk, and Parquet row groups are processed in bounded batches.
    """)
    return


@app.cell
def __(mo):
    rsid_button = mo.ui.run_button(label='Attach dbSNP rsIDs')
    rsid_button
    return (rsid_button,)


@app.cell
def __(Path, file_digest, json, mo, rsid_button, settings):
    def attach_rsids(chrom):
        import sqlite3
        import pyarrow as pa
        import pyarrow.parquet as pq

        mapping = settings["workspace"] / "rsid_map.tsv"
        if not mapping.is_file() or not mapping.stat().st_size:
            raise FileNotFoundError("Run the dbSNP preparation section before attaching rsIDs.")
        parquet = settings["workspace"] / "runs" / chrom / "out" / f"{chrom}_deltasvm.parquet"
        if not parquet.is_file() or not parquet.stat().st_size:
            raise FileNotFoundError(f"Export the {chrom} predictions to Parquet before attaching rsIDs.")
        database = settings["workspace"] / "rsid_map.sqlite"
        metadata = database.with_suffix(".json")
        source_digest = file_digest(mapping)
        recorded_digest = json.loads(metadata.read_text()).get("sha256") if metadata.is_file() else None
        if not database.is_file() or recorded_digest != source_digest:
            temporary_database = database.with_suffix(".sqlite.part")
            temporary_database.unlink(missing_ok=True)
            with sqlite3.connect(temporary_database) as connection:
                connection.execute("CREATE TABLE rsids (variant TEXT PRIMARY KEY, rsid TEXT) WITHOUT ROWID")
                batch = []
                with mapping.open() as stream:
                    next(stream, None)
                    for line in stream:
                        fields = line.rstrip("\n").split("\t", 1)
                        if len(fields) == 2:
                            batch.append((fields[0], fields[1]))
                        if len(batch) == 100000:
                            connection.executemany("INSERT OR REPLACE INTO rsids VALUES (?, ?)", batch)
                            connection.commit()
                            batch.clear()
                if batch:
                    connection.executemany("INSERT OR REPLACE INTO rsids VALUES (?, ?)", batch)
                    connection.commit()
            temporary_database.replace(database)
            temporary_metadata = metadata.with_suffix(".json.part")
            temporary_metadata.write_text(json.dumps({"sha256": source_digest}) + "\n")
            temporary_metadata.replace(metadata)
        temporary_parquet = parquet.with_suffix(".parquet.part")
        source = pq.ParquetFile(parquet)
        rows = 0
        annotated = 0
        with sqlite3.connect(database) as connection, pq.ParquetWriter(temporary_parquet, source.schema_arrow) as writer:
            connection.execute("CREATE TEMP TABLE selected (variant TEXT PRIMARY KEY) WITHOUT ROWID")
            for record_batch in source.iter_batches(batch_size=50000):
                frame = pa.Table.from_batches([record_batch]).to_pandas()
                variants = frame["variant"].astype(str).tolist()
                connection.executemany("INSERT OR IGNORE INTO selected VALUES (?)", ((variant,) for variant in variants))
                lookup = dict(connection.execute("SELECT rsids.variant, rsids.rsid FROM rsids JOIN selected USING (variant)").fetchall())
                frame["rsId"] = frame["variant"].map(lookup).fillna(".")
                annotated += sum(value != "." for value in frame["rsId"])
                rows += len(frame)
                writer.write_table(pa.Table.from_pandas(frame, schema=source.schema_arrow, preserve_index=False))
                connection.execute("DELETE FROM selected")
        temporary_parquet.replace(parquet)
        print(f"{chrom}: attached rsIDs to {annotated:,} of {rows:,} result rows.")

    mo.stop(not rsid_button.value, mo.md("Press the button above to run this step."))
    attach_rsids(settings["single"])
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 12. Add Ensembl Gene IDs for Human TFs
    Downloads the HGNC complete set and maps approved TF symbols to Ensembl gene IDs.
    The selected chromosome's Parquet file is updated in bounded batches; symbols
    without an HGNC Ensembl ID remain `.` and are listed in `tf_id_unmapped.tsv`.
    """)
    return


@app.cell
def __(mo):
    tf_id_button = mo.ui.run_button(label='Add Ensembl IDs to TF results')
    tf_id_button
    return (tf_id_button,)


@app.cell
def __(Path, download_file, file_digest, mo, record_source, settings, tf_id_button):
    def annotate_tf_ids(chrom):
        import csv
        import pyarrow as pa
        import pyarrow.parquet as pq

        hgnc_path = settings["hgnc"]
        existed = hgnc_path.is_file() and hgnc_path.stat().st_size > 0
        download_file(settings["hgnc_url"], hgnc_path)
        record_source("hgnc", hgnc_path,
                      status="existing local file" if existed else "downloaded",
                      configured_url=settings["hgnc_url"], sha256=file_digest(hgnc_path))
        parquet = settings["workspace"] / "runs" / chrom / "out" / f"{chrom}_deltasvm.parquet"
        if not parquet.is_file() or not parquet.stat().st_size:
            raise FileNotFoundError(f"Export the {chrom} predictions to Parquet before adding TF identifiers.")
        symbol_to_ensembl = {}
        with hgnc_path.open(encoding="utf-8-sig", newline="") as stream:
            for row in csv.DictReader(stream, delimiter="\t"):
                symbol = row.get("symbol", "").strip()
                ensembl = row.get("ensembl_gene_id", "").strip().split("|")[0]
                if symbol and ensembl:
                    symbol_to_ensembl[symbol] = ensembl
        temporary = parquet.with_suffix(".parquet.part")
        source = pq.ParquetFile(parquet)
        unmapped = set()
        rows = 0
        with pq.ParquetWriter(temporary, source.schema_arrow) as writer:
            for record_batch in source.iter_batches(batch_size=50000):
                frame = pa.Table.from_batches([record_batch]).to_pandas()
                frame["TfId"] = frame["TfName"].map(symbol_to_ensembl).fillna(".")
                unmapped.update(frame.loc[frame["TfId"].eq("."), "TfName"].astype(str).unique())
                rows += len(frame)
                writer.write_table(pa.Table.from_pandas(frame, schema=source.schema_arrow, preserve_index=False))
        temporary.replace(parquet)
        unmapped_path = settings["workspace"] / "runs" / chrom / "out" / "tf_id_unmapped.tsv"
        unmapped_path.write_text("TfName\n" + "".join(f"{name}\n" for name in sorted(unmapped)))
        print(f"{chrom}: mapped TF Ensembl IDs for {rows:,} result rows; {len(unmapped):,} symbols remain unmapped.")

    mo.stop(not tf_id_button.value, mo.md("Press the button above to run this step."))
    annotate_tf_ids(settings["single"])
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 13. Combine Selected Chromosome Results
    Combines the per-chromosome Parquet exports into
    `results/deltasvm_selected_chromosomes.parquet` using bounded batches. Every
    selected chromosome must have an export, and all files must have the same columns
    and types. Run annotation consistently across chromosomes before combining if you
    want rsID and Ensembl ID columns in the combined file.
    """)
    return


@app.cell
def __(mo):
    combine_button = mo.ui.run_button(label='Combine selected chromosome results')
    combine_button
    return (combine_button,)


@app.cell
def __(file_digest, mo, record_source, settings):
    def combine_chromosome_results():
        import pyarrow.parquet as pq

        chromosomes = settings["chromosomes"]
        sources = [settings["workspace"] / "runs" / chrom / "out" / f"{chrom}_deltasvm.parquet"
                   for chrom in chromosomes]
        missing = [str(path) for path in sources if not path.is_file() or not path.stat().st_size]
        if missing:
            raise FileNotFoundError("Export results for every selected chromosome first: " + ", ".join(missing))
        parquet_files = [pq.ParquetFile(path) for path in sources]
        schema = parquet_files[0].schema_arrow
        for chrom, parquet in zip(chromosomes[1:], parquet_files[1:]):
            if not schema.equals(parquet.schema_arrow, check_metadata=False):
                raise ValueError(f"{chrom} has a different Parquet schema. Apply the same annotations to every selected chromosome before combining.")
        output_directory = settings["workspace"] / "results"
        output_directory.mkdir(parents=True, exist_ok=True)
        output = output_directory / "deltasvm_selected_chromosomes.parquet"
        temporary = output.with_suffix(".parquet.part")
        rows = 0
        try:
            with pq.ParquetWriter(temporary, schema) as writer:
                for parquet in parquet_files:
                    for batch in parquet.iter_batches(batch_size=50000):
                        writer.write_batch(batch)
                        rows += batch.num_rows
            temporary.replace(output)
        finally:
            temporary.unlink(missing_ok=True)
        record_source("combined_deltasvm_results", output,
                      chromosomes=list(chromosomes),
                      source_sha256={str(path): file_digest(path) for path in sources},
                      sha256=file_digest(output), rows=rows)
        print(f"Combined {rows:,} rows across {len(chromosomes)} chromosomes: {output}")

    return (combine_chromosome_results,)


@app.cell
def __(combine_button, combine_chromosome_results, mo):
    mo.stop(not combine_button.value, mo.md("Press the button above to run this step."))
    combine_chromosome_results()
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 14. Validate and Summarize the Combined Results
    Streams the combined Parquet file and writes chromosome-level quality metrics to
    `results/deltasvm_qc_report.tsv`. The report includes row and effect counts,
    distinct TF symbols, invalid scores, and missing rsID or Ensembl ID annotations.
    """)
    return


@app.cell
def __(mo):
    qc_button = mo.ui.run_button(label='Validate combined results')
    qc_button
    return (qc_button,)


@app.cell
def __(file_digest, mo, record_source, settings):
    def validate_combined_results():
        import math
        import pyarrow as pa
        import pyarrow.parquet as pq
        import pandas as pd

        source = settings["workspace"] / "results" / "deltasvm_selected_chromosomes.parquet"
        if not source.is_file() or not source.stat().st_size:
            raise FileNotFoundError("Combine the selected chromosome results before running quality checks.")
        parquet = pq.ParquetFile(source)
        required = {"variant", "rsId", "TfId", "TfName", "Effect", "Score"}
        missing_columns = sorted(required - set(parquet.schema_arrow.names))
        if missing_columns:
            raise ValueError("Combined results are missing required columns: " + ", ".join(missing_columns))
        summaries = {}
        for batch in parquet.iter_batches(batch_size=50000, columns=sorted(required)):
            frame = pa.Table.from_batches([batch]).to_pandas()
            chromosomes = frame["variant"].astype(str).str.split("_", n=1).str[0]
            scores = pd.to_numeric(frame["Score"], errors="coerce").tolist()
            for chrom, effect, score, rsid, tfid, tf_name in zip(
                chromosomes.tolist(), frame["Effect"].tolist(), scores, frame["rsId"].tolist(),
                frame["TfId"].tolist(), frame["TfName"].tolist()
            ):
                row = summaries.setdefault(chrom, {
                    "rows": 0, "gain": 0, "loss": 0, "invalid_effect": 0,
                    "invalid_score": 0, "missing_rsid": 0, "missing_tfid": 0,
                    "tf_names": set()
                })
                row["rows"] += 1
                row["gain"] += effect == "Gain"
                row["loss"] += effect == "Loss"
                row["invalid_effect"] += effect not in ("Gain", "Loss")
                row["invalid_score"] += score is None or not math.isfinite(score)
                row["missing_rsid"] += rsid is None or pd.isna(rsid) or rsid in (".", "")
                row["missing_tfid"] += tfid is None or pd.isna(tfid) or tfid in (".", "")
                row["tf_names"].add(str(tf_name))
        report = settings["workspace"] / "results" / "deltasvm_qc_report.tsv"
        fields = ("chromosome", "rows", "gain", "loss", "invalid_effect", "invalid_score",
                  "missing_rsid", "missing_tfid", "distinct_tf_names")
        with report.open("w") as stream:
            stream.write("\t".join(fields) + "\n")
            for chrom in sorted(summaries):
                row = summaries[chrom]
                values = (chrom, row["rows"], row["gain"], row["loss"], row["invalid_effect"],
                          row["invalid_score"], row["missing_rsid"], row["missing_tfid"], len(row["tf_names"]))
                stream.write("\t".join(map(str, values)) + "\n")
        record_source("combined_deltasvm_qc_report", report,
                      combined_results=str(source), combined_sha256=file_digest(source),
                      report_sha256=file_digest(report))
        print(f"Quality report saved to {report}")
        print(f"Rows: {sum(row['rows'] for row in summaries.values()):,}; chromosomes: {len(summaries)}")

    return (validate_combined_results,)


@app.cell
def __(mo, qc_button, validate_combined_results):
    mo.stop(not qc_button.value, mo.md("Press the button above to run this step."))
    validate_combined_results()
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 15. Summarize Scores by Transcription Factor
    Streams the combined results and writes per-TF row counts, Gain/Loss counts, mean
    score, and maximum absolute score to `results/deltasvm_tf_summary.tsv`.
    """)
    return


@app.cell
def __(mo):
    tf_summary_button = mo.ui.run_button(label='Create TF score summary')
    tf_summary_button
    return (tf_summary_button,)


@app.cell
def __(file_digest, mo, record_source, settings):
    def summarize_tf_scores():
        import math
        import pyarrow as pa
        import pyarrow.parquet as pq
        import pandas as pd

        source = settings["workspace"] / "results" / "deltasvm_selected_chromosomes.parquet"
        if not source.is_file() or not source.stat().st_size:
            raise FileNotFoundError("Combine the selected chromosome results before summarizing TF scores.")
        parquet = pq.ParquetFile(source)
        required = {"TfName", "Effect", "Score"}
        missing = sorted(required - set(parquet.schema_arrow.names))
        if missing:
            raise ValueError("Combined results are missing required columns: " + ", ".join(missing))
        summary = {}
        for batch in parquet.iter_batches(batch_size=50000, columns=sorted(required)):
            frame = pa.Table.from_batches([batch]).to_pandas()
            scores = pd.to_numeric(frame["Score"], errors="coerce").tolist()
            for name, effect, score in zip(frame["TfName"].tolist(), frame["Effect"].tolist(), scores):
                key = str(name)
                row = summary.setdefault(key, {"rows": 0, "gain": 0, "loss": 0,
                                               "score_sum": 0.0, "valid_scores": 0,
                                               "max_abs_score": 0.0})
                row["rows"] += 1
                row["gain"] += effect == "Gain"
                row["loss"] += effect == "Loss"
                if score is not None and math.isfinite(score):
                    row["score_sum"] += score
                    row["valid_scores"] += 1
                    row["max_abs_score"] = max(row["max_abs_score"], abs(score))
        report = settings["workspace"] / "results" / "deltasvm_tf_summary.tsv"
        fields = ("TfName", "rows", "gain", "loss", "mean_score", "max_abs_score")
        with report.open("w") as stream:
            stream.write("\t".join(fields) + "\n")
            for name, row in sorted(summary.items()):
                mean_score = row["score_sum"] / row["valid_scores"] if row["valid_scores"] else "."
                values = (name, row["rows"], row["gain"], row["loss"], mean_score, row["max_abs_score"])
                stream.write("\t".join(map(str, values)) + "\n")
        record_source("deltaSVM_tf_summary", report,
                      combined_results=str(source), combined_sha256=file_digest(source),
                      report_sha256=file_digest(report))
        print(f"TF score summary saved for {len(summary)} TFs: {report}")

    return (summarize_tf_scores,)


@app.cell
def __(mo, summarize_tf_scores, tf_summary_button):
    mo.stop(not tf_summary_button.value, mo.md("Press the button above to run this step."))
    summarize_tf_scores()
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 16. Export Results Using Effect and Score Filters
    Creates `results/deltasvm_filtered.parquet` from the combined results. Choose Gain,
    Loss, or both effects and set the minimum absolute deltaSVM score.
    """)
    return


@app.cell
def __(mo):
    minimum_score_input = mo.ui.text(value="0", label="Minimum absolute deltaSVM score")
    effect_input = mo.ui.dropdown(options=["Both", "Gain", "Loss"], value="Both", label="Effect")
    filtered_export_button = mo.ui.run_button(label='Create filtered Parquet export')
    mo.vstack([minimum_score_input, effect_input, filtered_export_button])
    return effect_input, filtered_export_button, minimum_score_input


@app.cell
def __(file_digest, record_source, settings):
    def export_filtered_results(minimum_score, effect_choice):
        import math
        import pyarrow as pa
        import pyarrow.parquet as pq
        import pandas as pd

        try:
            minimum_score = float(minimum_score)
        except (TypeError, ValueError):
            raise ValueError("Enter a numeric minimum absolute score.")
        if not math.isfinite(minimum_score) or minimum_score < 0:
            raise ValueError("The minimum absolute score must be a finite number at least zero.")
        if effect_choice not in ("Both", "Gain", "Loss"):
            raise ValueError("Select Both, Gain, or Loss for the effect filter.")
        source = settings["workspace"] / "results" / "deltasvm_selected_chromosomes.parquet"
        if not source.is_file() or not source.stat().st_size:
            raise FileNotFoundError("Combine the selected chromosome results before creating a filtered export.")
        parquet = pq.ParquetFile(source)
        required = {"Effect", "Score"}
        missing = sorted(required - set(parquet.schema_arrow.names))
        if missing:
            raise ValueError("Combined results are missing required columns: " + ", ".join(missing))
        destination = settings["workspace"] / "results" / "deltasvm_filtered.parquet"
        temporary = destination.with_suffix(".parquet.part")
        rows = 0
        try:
            with pq.ParquetWriter(temporary, parquet.schema_arrow) as writer:
                for batch in parquet.iter_batches(batch_size=50000):
                    frame = pa.Table.from_batches([batch]).to_pandas()
                    scores = pd.to_numeric(frame["Score"], errors="coerce")
                    selected = scores.map(lambda score: math.isfinite(score) if not pd.isna(score) else False)
                    selected &= scores.abs() >= minimum_score
                    if effect_choice != "Both":
                        selected &= frame["Effect"].eq(effect_choice)
                    filtered = frame.loc[selected]
                    if not filtered.empty:
                        writer.write_table(pa.Table.from_pandas(filtered, schema=parquet.schema_arrow,
                                                               preserve_index=False))
                        rows += len(filtered)
            if not rows:
                raise ValueError("No rows matched the selected effect and minimum score.")
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)
        record_source("filtered_deltasvm_results", destination,
                      source=str(source), source_sha256=file_digest(source),
                      minimum_absolute_score=minimum_score, effect=effect_choice,
                      rows=rows, sha256=file_digest(destination))
        print(f"Saved {rows:,} filtered rows to {destination}")

    return (export_filtered_results,)


@app.cell
def __(effect_input, export_filtered_results, filtered_export_button, minimum_score_input, mo):
    mo.stop(not filtered_export_button.value, mo.md("Choose filters above, then press the export button."))
    export_filtered_results(minimum_score_input.value, effect_input.value)
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 17. Export Top Variants for Each TF
    Selects the variants with the largest absolute deltaSVM scores for each
    transcription factor and writes them to `results/deltasvm_top_variants_by_tf.tsv`.
    The ranking keeps only the requested number of rows per TF in memory.
    """)
    return


@app.cell
def __(mo):
    top_variants_input = mo.ui.text(value="25", label="Maximum variants per TF")
    top_variants_button = mo.ui.run_button(label='Export top variants by TF')
    mo.vstack([top_variants_input, top_variants_button])
    return top_variants_button, top_variants_input


@app.cell
def __(file_digest, record_source, settings):
    def export_top_variants_per_tf(limit_value):
        import csv
        import heapq
        import math
        import pyarrow as pa
        import pyarrow.parquet as pq
        import pandas as pd

        try:
            limit = int(limit_value)
        except (TypeError, ValueError):
            raise ValueError("Enter a whole number of variants per TF.")
        if limit < 1 or limit > 1000:
            raise ValueError("The number of variants per TF must be between 1 and 1000.")
        source = settings["workspace"] / "results" / "deltasvm_selected_chromosomes.parquet"
        if not source.is_file() or not source.stat().st_size:
            raise FileNotFoundError("Combine the selected chromosome results before ranking variants.")
        parquet = pq.ParquetFile(source)
        fields = ("variant", "rsId", "TfId", "TfName", "Effect", "Score")
        missing = sorted(set(fields) - set(parquet.schema_arrow.names))
        if missing:
            raise ValueError("Combined results are missing required columns: " + ", ".join(missing))
        rankings = {}
        for batch in parquet.iter_batches(batch_size=50000, columns=list(fields)):
            frame = pa.Table.from_batches([batch]).to_pandas()
            scores = pd.to_numeric(frame["Score"], errors="coerce").tolist()
            for variant, rsid, tfid, name, effect, score in zip(
                frame["variant"].tolist(), frame["rsId"].tolist(), frame["TfId"].tolist(),
                frame["TfName"].tolist(), frame["Effect"].tolist(), scores
            ):
                if score is None or not math.isfinite(score):
                    continue
                key = (abs(score), score, str(variant), str(rsid), str(tfid), str(effect))
                heap = rankings.setdefault(str(name), [])
                if len(heap) < limit:
                    heapq.heappush(heap, key)
                elif key > heap[0]:
                    heapq.heapreplace(heap, key)
        if not rankings:
            raise ValueError("No finite deltaSVM scores were available for ranking.")
        destination = settings["workspace"] / "results" / "deltasvm_top_variants_by_tf.tsv"
        temporary = destination.with_suffix(".tsv.part")
        rows = 0
        try:
            with temporary.open("w", newline="") as stream:
                writer = csv.writer(stream, delimiter="\t")
                writer.writerow(fields)
                for name in sorted(rankings):
                    for absolute_score, score, variant, rsid, tfid, effect in sorted(rankings[name], reverse=True):
                        writer.writerow((variant, rsid, tfid, name, effect, score))
                        rows += 1
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)
        record_source("top_deltasvm_variants_by_tf", destination,
                      source=str(source), source_sha256=file_digest(source),
                      maximum_per_tf=limit, rows=rows, sha256=file_digest(destination))
        print(f"Saved {rows:,} ranked variant rows across {len(rankings)} TFs to {destination}")

    return (export_top_variants_per_tf,)


@app.cell
def __(export_top_variants_per_tf, mo, top_variants_button, top_variants_input):
    mo.stop(not top_variants_button.value, mo.md("Choose a per-TF row limit, then press the export button."))
    export_top_variants_per_tf(top_variants_input.value)
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 18. Check Combined Results Are Current
    Checks the combined Parquet checksum, selected chromosome list, and source file
    checksums against the provenance manifest. Results are written to
    `results/deltasvm_combined_validation.tsv`.
    """)
    return


@app.cell
def __(mo):
    combined_validation_button = mo.ui.run_button(label='Check combined result provenance')
    combined_validation_button
    return (combined_validation_button,)


@app.cell
def __(file_digest, json, record_source, settings):
    def validate_combined_provenance():
        from pathlib import Path

        workspace = settings["workspace"]
        manifest_path = workspace / "provenance.json"
        combined_path = workspace / "results" / "deltasvm_selected_chromosomes.parquet"
        if not manifest_path.is_file():
            raise FileNotFoundError("The provenance manifest is missing. Run the combine section first.")
        manifest = json.loads(manifest_path.read_text())
        record = manifest.get("sources", {}).get("combined_deltasvm_results")
        findings = []
        if not isinstance(record, dict):
            findings.append("combined result provenance record is missing")
        else:
            if record.get("chromosomes") != list(settings["chromosomes"]):
                findings.append("selected chromosome list differs from the combined result")
            expected_output_digest = record.get("sha256")
            if not combined_path.is_file() or not combined_path.stat().st_size:
                findings.append("combined Parquet file is missing or empty")
            elif not expected_output_digest or file_digest(combined_path) != expected_output_digest:
                findings.append("combined Parquet checksum differs from provenance")
            for source_path, expected_digest in record.get("source_sha256", {}).items():
                source = Path(source_path)
                if not source.is_file() or not source.stat().st_size:
                    findings.append(f"source export is missing: {source_path}")
                elif file_digest(source) != expected_digest:
                    findings.append(f"source export changed: {source_path}")
        status = "current" if not findings else "stale"
        report = workspace / "results" / "deltasvm_combined_validation.tsv"
        report.parent.mkdir(parents=True, exist_ok=True)
        temporary = report.with_suffix(".tsv.part")
        with temporary.open("w") as stream:
            stream.write("status\tdetail\n")
            if findings:
                for finding in findings:
                    stream.write(f"{status}\t{finding}\n")
            else:
                stream.write("current\tCombined results match the selected chromosomes and recorded source checksums.\n")
        temporary.replace(report)
        record_source("combined_results_validation", report, status=status, findings=findings,
                      combined_results=str(combined_path), report_sha256=file_digest(report))
        if findings:
            print("Combined results are stale: " + "; ".join(findings))
        else:
            print(f"Combined results are current: {combined_path}")
        print(f"Validation report saved to {report}")

    return (validate_combined_provenance,)


@app.cell
def __(combined_validation_button, mo, validate_combined_provenance):
    mo.stop(not combined_validation_button.value, mo.md("Press the button above to check the combined output."))
    validate_combined_provenance()
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 19. Package Results for Sharing
    Bundles the combined Parquet file, available summary reports, and the current
    provenance manifest into `results/deltasvm_results_bundle.zip`. The archive
    includes a manifest with file sizes and SHA-256 checksums.
    """)
    return


@app.cell
def __(mo):
    bundle_button = mo.ui.run_button(label='Create results bundle')
    bundle_button
    return (bundle_button,)


@app.cell
def __(file_digest, json, record_source, settings):
    def create_results_bundle():
        import zipfile
        from datetime import datetime, timezone

        workspace = settings["workspace"]
        results = workspace / "results"
        provenance = workspace / "provenance.json"
        required = results / "deltasvm_selected_chromosomes.parquet"
        if not required.is_file() or not required.stat().st_size:
            raise FileNotFoundError("Combine chromosome results before packaging the result bundle.")
        if not provenance.is_file() or not provenance.stat().st_size:
            raise FileNotFoundError("The provenance manifest is missing. Run the combine section first.")
        filenames = (
            "deltasvm_selected_chromosomes.parquet",
            "deltasvm_qc_report.tsv",
            "deltasvm_tf_summary.tsv",
            "deltasvm_filtered.parquet",
            "deltasvm_top_variants_by_tf.tsv",
            "deltasvm_combined_validation.tsv",
            "deltasvm_manifest_validation.tsv",
            "deltasvm_run_manifest.json",
            f"{settings['single']}_run_record.json",
            "deltasvm_skipped_variants.tsv",
            "deltasvm_skipped_variant_summary.tsv",
            "deltasvm_bundle_validation.tsv",
            "deltasvm_selected_chromosomes.tsv.gz",
            "deltasvm_filtered.tsv.gz",
            "README.txt",
        )
        files = [(results / name, f"results/{name}") for name in filenames if (results / name).is_file()]
        files.append((provenance, "provenance.json"))
        inventory = [{"path": arcname, "size_bytes": path.stat().st_size,
                      "sha256": file_digest(path)} for path, arcname in files]
        bundle_manifest = {
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "chromosomes": list(settings["chromosomes"]),
            "files": inventory,
        }
        destination = results / "deltasvm_results_bundle.zip"
        temporary = destination.with_suffix(".zip.part")
        try:
            with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as archive:
                for path, arcname in files:
                    archive.write(path, arcname=arcname)
                archive.writestr("bundle_manifest.json", json.dumps(bundle_manifest, indent=2) + "\n")
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)
        record_source("deltaSVM_results_bundle", destination,
                      included_files=[item["path"] for item in inventory],
                      sha256=file_digest(destination), size_bytes=destination.stat().st_size)
        print(f"Created results bundle with {len(files)} files: {destination}")

    return (create_results_bundle,)


@app.cell
def __(bundle_button, create_results_bundle, mo):
    mo.stop(not bundle_button.value, mo.md("Press the button above to package available results."))
    create_results_bundle()
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 20. Download the Results Bundle
    After creating the bundle in section 19, use the download button to save the ZIP
    archive to your computer. The file is streamed from disk when the download starts.
    """)
    return


@app.cell
def __(mo, settings):
    bundle_path = settings["workspace"] / "results" / "deltasvm_results_bundle.zip"

    def open_results_bundle():
        if not bundle_path.is_file() or not bundle_path.stat().st_size:
            raise FileNotFoundError("Create the results bundle in section 19 before downloading it.")
        return bundle_path.open("rb")

    mo.download(data=open_results_bundle, filename="deltasvm_results_bundle.zip",
                mimetype="application/zip", label="Download deltaSVM results bundle")
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 21. Write a Reproducibility Manifest
    Records the selected configuration, software versions, source provenance, and
    checksums for available result files in `results/deltasvm_run_manifest.json`.
    """)
    return


@app.cell
def __(mo):
    run_manifest_button = mo.ui.run_button(label='Write reproducibility manifest')
    run_manifest_button
    return (run_manifest_button,)


@app.cell
def __(file_digest, json, record_source, settings):
    def write_run_manifest():
        import platform
        import sys
        from datetime import datetime, timezone
        from importlib.metadata import PackageNotFoundError, version

        workspace = settings["workspace"]
        results = workspace / "results"
        results.mkdir(parents=True, exist_ok=True)
        provenance_path = workspace / "provenance.json"
        provenance = json.loads(provenance_path.read_text()) if provenance_path.is_file() else {}
        packages = {}
        for package in ("marimo", "pandas", "pyarrow", "pysam"):
            try:
                packages[package] = version(package)
            except PackageNotFoundError:
                packages[package] = None
        outputs = {}
        if results.is_dir():
            for path in sorted(results.iterdir()):
                if path.is_file() and path.name != "deltasvm_run_manifest.json":
                    outputs[path.name] = {"size_bytes": path.stat().st_size,
                                          "sha256": file_digest(path)}
        normalized_settings = {
            key: [str(item) for item in value] if isinstance(value, tuple)
            else str(value) if hasattr(value, "as_posix") else value
            for key, value in settings.items()
        }
        manifest = {
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "python": sys.version,
            "platform": platform.platform(),
            "packages": packages,
            "settings": normalized_settings,
            "provenance_sources": provenance.get("sources", {}),
            "outputs": outputs,
        }
        destination = results / "deltasvm_run_manifest.json"
        temporary = destination.with_suffix(".json.part")
        temporary.write_text(json.dumps(manifest, indent=2) + "\n")
        temporary.replace(destination)
        record_source("deltaSVM_run_manifest", destination,
                      sha256=file_digest(destination), output_count=len(outputs))
        print(f"Reproducibility manifest saved to {destination}")

    return (write_run_manifest,)


@app.cell
def __(mo, run_manifest_button, write_run_manifest):
    mo.stop(not run_manifest_button.value, mo.md("Press the button above to write the manifest."))
    write_run_manifest()
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 22. Validate the Reproducibility Manifest
    Checks that the selected chromosome list and recorded result checksums still match
    the current workspace. The check is saved to `results/deltasvm_manifest_validation.tsv`.
    """)
    return


@app.cell
def __(mo):
    manifest_validation_button = mo.ui.run_button(label='Validate reproducibility manifest')
    manifest_validation_button
    return (manifest_validation_button,)


@app.cell
def __(file_digest, json, record_source, settings):
    def validate_run_manifest():
        results = settings["workspace"] / "results"
        manifest_path = results / "deltasvm_run_manifest.json"
        if not manifest_path.is_file() or not manifest_path.stat().st_size:
            raise FileNotFoundError("Write the reproducibility manifest in section 21 first.")
        manifest = json.loads(manifest_path.read_text())
        findings = []
        saved_chromosomes = manifest.get("settings", {}).get("chromosomes")
        if saved_chromosomes != list(settings["chromosomes"]):
            findings.append("selected chromosome list differs from the manifest")
        expected_outputs = manifest.get("outputs", {})
        for name, details in sorted(expected_outputs.items()):
            path = results / name
            if not path.is_file() or not path.stat().st_size:
                findings.append(f"output is missing or empty: {name}")
            elif file_digest(path) != details.get("sha256"):
                findings.append(f"output checksum differs from the manifest: {name}")
        status = "current" if not findings else "stale"
        report = results / "deltasvm_manifest_validation.tsv"
        temporary = report.with_suffix(".tsv.part")
        with temporary.open("w") as stream:
            stream.write("status\tdetail\n")
            if findings:
                for finding in findings:
                    stream.write(f"{status}\t{finding}\n")
            else:
                stream.write("current\tSelected chromosomes and recorded output checksums match the manifest.\n")
        temporary.replace(report)
        record_source("deltaSVM_manifest_validation", report, status=status,
                      findings=findings, report_sha256=file_digest(report))
        if findings:
            print("Reproducibility manifest is stale: " + "; ".join(findings))
        else:
            print(f"Reproducibility manifest is current for {len(expected_outputs)} outputs.")
        print(f"Validation report saved to {report}")

    return (validate_run_manifest,)


@app.cell
def __(manifest_validation_button, mo, validate_run_manifest):
    mo.stop(not manifest_validation_button.value, mo.md("Press the button above to check the manifest."))
    validate_run_manifest()
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 23. Export a Selected Chromosome Run Record
    Saves the current input fingerprints, run settings, completed TF count, and
    checksums for available chromosome outputs to `results/chrN_run_record.json`.
    The record is created only when the run inputs still match their saved fingerprints.
    """)
    return


@app.cell
def __(mo):
    run_record_button = mo.ui.run_button(label='Export selected chromosome run record')
    run_record_button
    return (run_record_button,)


@app.cell
def __(file_digest, json, record_source, run_signature, settings):
    def export_chromosome_run_record():
        from datetime import datetime, timezone

        chrom = settings["single"]
        directory = settings["workspace"] / "runs" / chrom
        input_record = directory / "inputs.json"
        if not input_record.is_file():
            raise FileNotFoundError(f"Prepare and run {chrom} before exporting its run record.")
        input_fingerprints = json.loads(input_record.read_text())
        current_fingerprints = run_signature(chrom)
        if input_fingerprints != current_fingerprints:
            raise RuntimeError(f"{chrom} inputs changed after scoring. Rerun the chromosome before exporting its record.")
        output_paths = (
            directory / "input_snp.tsv",
            directory / "data" / "skipped.tsv",
            directory / "out" / "pbs.pred.tsv",
            directory / "out" / "obs.pred.tsv",
            directory / "out" / "summary.pred.tsv",
            directory / "out" / f"{chrom}_deltasvm.parquet",
            directory / "out" / "tf_id_unmapped.tsv",
        )
        outputs = {
            str(path.relative_to(directory)): {"size_bytes": path.stat().st_size,
                                              "sha256": file_digest(path)}
            for path in output_paths if path.is_file()
        }
        completed_tfs = sum(path.is_file() and path.stat().st_size > 0
                            for path in (directory / "tmp").glob("*.merge.gkm.tsv"))
        record = {
            "created_utc": datetime.now(timezone.utc).isoformat(),
            "chromosome": chrom,
            "settings": {"threads": settings["threads"], "tf_jobs": settings["tf_jobs"],
                         "use_opentargets": settings["use_ot"],
                         "common_variants_only": settings["common_only"],
                         "maximum_variants": settings["max_snps"]},
            "input_fingerprints": input_fingerprints,
            "completed_tf_predictions": completed_tfs,
            "output_files": outputs,
        }
        results = settings["workspace"] / "results"
        results.mkdir(parents=True, exist_ok=True)
        destination = results / f"{chrom}_run_record.json"
        temporary = destination.with_suffix(".json.part")
        temporary.write_text(json.dumps(record, indent=2) + "\n")
        temporary.replace(destination)
        record_source(f"{chrom}_run_record", destination,
                      chromosome=chrom, sha256=file_digest(destination),
                      output_count=len(outputs))
        print(f"Run record saved for {chrom}: {destination}")

    return (export_chromosome_run_record,)


@app.cell
def __(export_chromosome_run_record, mo, run_record_button):
    mo.stop(not run_record_button.value, mo.md("Press the button above to export the chromosome run record."))
    export_chromosome_run_record()
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 24. Summarize Skipped Variants
    Combines `data/skipped.tsv` from each selected chromosome into
    `results/deltasvm_skipped_variants.tsv` and writes exclusion counts by reason to
    `results/deltasvm_skipped_variant_summary.tsv`.
    """)
    return


@app.cell
def __(mo):
    skipped_variants_button = mo.ui.run_button(label='Summarize skipped variants')
    skipped_variants_button
    return (skipped_variants_button,)


@app.cell
def __(file_digest, record_source, settings):
    def summarize_skipped_variants():
        import csv
        from collections import Counter

        sources = [(chrom, settings["workspace"] / "runs" / chrom / "data" / "skipped.tsv")
                   for chrom in settings["chromosomes"]]
        missing = [str(path) for _, path in sources if not path.is_file() or not path.stat().st_size]
        if missing:
            raise FileNotFoundError("Prepare every selected chromosome before summarizing skipped variants: " + ", ".join(missing))
        results = settings["workspace"] / "results"
        results.mkdir(parents=True, exist_ok=True)
        detail_path = results / "deltasvm_skipped_variants.tsv"
        summary_path = results / "deltasvm_skipped_variant_summary.tsv"
        detail_temporary = detail_path.with_suffix(".tsv.part")
        summary_temporary = summary_path.with_suffix(".tsv.part")
        counts = Counter()
        rows = 0
        try:
            with detail_temporary.open("w", newline="") as detail_stream:
                writer = csv.writer(detail_stream, delimiter="\t")
                writer.writerow(("chromosome", "variant", "reason"))
                for chrom, source in sources:
                    with source.open(encoding="utf-8", newline="") as source_stream:
                        reader = csv.DictReader(source_stream, delimiter="\t")
                        if not reader.fieldnames or not {"variant", "reason"}.issubset(reader.fieldnames):
                            raise ValueError(f"Unexpected skipped-variant columns in {source}")
                        for row in reader:
                            reason = row.get("reason", "")
                            writer.writerow((chrom, row.get("variant", ""), reason))
                            counts[reason] += 1
                            rows += 1
            with summary_temporary.open("w", newline="") as summary_stream:
                writer = csv.writer(summary_stream, delimiter="\t")
                writer.writerow(("reason", "variants"))
                for reason, count in sorted(counts.items()):
                    writer.writerow((reason, count))
            detail_temporary.replace(detail_path)
            summary_temporary.replace(summary_path)
        finally:
            detail_temporary.unlink(missing_ok=True)
            summary_temporary.unlink(missing_ok=True)
        source_digests = {str(path): file_digest(path) for _, path in sources}
        record_source("deltasvm_skipped_variants", detail_path,
                      chromosomes=list(settings["chromosomes"]), source_sha256=source_digests,
                      rows=rows, sha256=file_digest(detail_path))
        record_source("deltasvm_skipped_variant_summary", summary_path,
                      detail_file=str(detail_path), sha256=file_digest(summary_path))
        print(f"Combined {rows:,} skipped variants across {len(sources)} chromosomes.")
        print(f"Reports saved to {detail_path} and {summary_path}")

    return (summarize_skipped_variants,)


@app.cell
def __(mo, skipped_variants_button, summarize_skipped_variants):
    mo.stop(not skipped_variants_button.value, mo.md("Press the button above to summarize exclusions."))
    summarize_skipped_variants()
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 25. Write a Data Dictionary for the Results
    Creates `results/README.txt` with the result column definitions, identifier
    conventions, effect labels, and a guide to the generated reports.
    """)
    return


@app.cell
def __(mo):
    data_dictionary_button = mo.ui.run_button(label='Write results data dictionary')
    data_dictionary_button
    return (data_dictionary_button,)


@app.cell
def __(file_digest, record_source, settings):
    def write_results_data_dictionary():
        results = settings["workspace"] / "results"
        results.mkdir(parents=True, exist_ok=True)
        destination = results / "README.txt"
        content = f"""deltaSVM result files

Selected chromosomes: {', '.join(settings['chromosomes'])}
Genome build: GRCh38

Combined prediction table
File: deltasvm_selected_chromosomes.parquet
Columns:
  variant: contig_position_reference_allele_alternate_allele identifier.
  rsId: dbSNP identifier when mapped; a period means no mapping was available.
  TfId: HGNC Ensembl gene identifier for the TF symbol when available; a period means no mapping was available.
  TfName: transcription factor symbol used by the supplied human TF models.
  Effect: Gain or Loss label from the pipeline's preferred allele prediction.
  Score: numeric deltaSVM score from the supplied model resources.

Compressed text export
deltasvm_selected_chromosomes.tsv.gz: tab-separated export of the combined prediction table, compressed with gzip.
deltasvm_filtered.tsv.gz: optional effect and absolute-score filtered TSV export, compressed with gzip.

Reports and derived files
deltasvm_qc_report.tsv: per-chromosome result counts, effect counts, invalid scores, and missing identifier counts.
deltasvm_tf_summary.tsv: per-TF row counts, effect counts, mean score, and maximum absolute score.
deltasvm_filtered.parquet: optional rows filtered by effect and minimum absolute score; settings are in provenance.json.
deltasvm_top_variants_by_tf.tsv: optional highest absolute-score variants retained per TF.
deltasvm_skipped_variants.tsv: skipped variants with chromosome and exclusion reason.
deltasvm_skipped_variant_summary.tsv: skipped variant counts grouped by exclusion reason.
deltasvm_combined_validation.tsv: check of combined output checksums and selected chromosomes.
deltasvm_manifest_validation.tsv: check of manifest output checksums and selected chromosomes.
deltasvm_bundle_validation.tsv: check that archived files match their recorded sizes and checksums.
deltasvm_run_manifest.json: software versions, settings, source provenance, and result checksums.
{settings['single']}_run_record.json: selected chromosome input fingerprints, run settings, TF completion count, and output checksums.
deltasvm_results_bundle.zip: archive of available result files and provenance.

Periods in rsId and TfId fields indicate unavailable annotations. Annotation coverage is reported in deltasvm_qc_report.tsv.
The predictions use the supplied human TF models and do not provide fly-specific biological validation.
"""
        temporary = destination.with_suffix(".txt.part")
        temporary.write_text(content)
        temporary.replace(destination)
        record_source("deltaSVM_results_data_dictionary", destination,
                      sha256=file_digest(destination), chromosomes=list(settings["chromosomes"]))
        print(f"Results data dictionary saved to {destination}")

    return (write_results_data_dictionary,)


@app.cell
def __(data_dictionary_button, mo, write_results_data_dictionary):
    mo.stop(not data_dictionary_button.value, mo.md("Press the button above to document the results."))
    write_results_data_dictionary()
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 26. Verify the Results Bundle
    Checks each packaged file against the SHA-256 checksum and size recorded in the
    bundle manifest, then writes `results/deltasvm_bundle_validation.tsv`.
    """)
    return


@app.cell
def __(mo):
    bundle_validation_button = mo.ui.run_button(label='Verify results bundle')
    bundle_validation_button
    return (bundle_validation_button,)


@app.cell
def __(file_digest, json, record_source, settings):
    def validate_results_bundle():
        import hashlib
        import zipfile

        results = settings["workspace"] / "results"
        bundle = results / "deltasvm_results_bundle.zip"
        if not bundle.is_file() or not bundle.stat().st_size:
            raise FileNotFoundError("Create the results bundle in section 19 before verifying it.")
        findings = []
        with zipfile.ZipFile(bundle) as archive:
            try:
                embedded = json.loads(archive.read("bundle_manifest.json"))
            except KeyError:
                raise ValueError("The ZIP archive does not contain bundle_manifest.json.")
            for item in embedded.get("files", []):
                name = item.get("path")
                try:
                    info = archive.getinfo(name)
                except KeyError:
                    findings.append(f"file is missing from archive: {name}")
                    continue
                digest = hashlib.sha256()
                size = 0
                with archive.open(info) as stream:
                    for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                        digest.update(chunk)
                        size += len(chunk)
                if size != item.get("size_bytes"):
                    findings.append(f"file size differs from bundle manifest: {name}")
                if digest.hexdigest() != item.get("sha256"):
                    findings.append(f"file checksum differs from bundle manifest: {name}")
        status = "valid" if not findings else "invalid"
        report = results / "deltasvm_bundle_validation.tsv"
        temporary = report.with_suffix(".tsv.part")
        with temporary.open("w") as stream:
            stream.write("status\tdetail\n")
            if findings:
                for finding in findings:
                    stream.write(f"{status}\t{finding}\n")
            else:
                stream.write("valid\tEvery packaged file matches its size and SHA-256 checksum.\n")
        temporary.replace(report)
        record_source("deltaSVM_bundle_validation", report, status=status,
                      bundle=str(bundle), bundle_sha256=file_digest(bundle),
                      findings=findings, report_sha256=file_digest(report))
        if findings:
            print("Results bundle validation failed: " + "; ".join(findings))
        else:
            print("Every file in the results bundle passed checksum validation.")
        print(f"Validation report saved to {report}")

    return (validate_results_bundle,)


@app.cell
def __(bundle_validation_button, mo, validate_results_bundle):
    mo.stop(not bundle_validation_button.value, mo.md("Press the button above to verify the archive."))
    validate_results_bundle()
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 27. Export the Combined Results as Compressed TSV
    Writes `results/deltasvm_selected_chromosomes.tsv.gz` in bounded batches for
    analysis tools that do not read Parquet files.
    """)
    return


@app.cell
def __(mo):
    tsv_export_button = mo.ui.run_button(label='Export combined results as compressed TSV')
    tsv_export_button
    return (tsv_export_button,)


@app.cell
def __(file_digest, record_source, settings):
    def export_combined_tsv():
        import gzip
        import pyarrow.parquet as pq

        source = settings["workspace"] / "results" / "deltasvm_selected_chromosomes.parquet"
        if not source.is_file() or not source.stat().st_size:
            raise FileNotFoundError("Combine the selected chromosome results before creating a TSV export.")
        parquet = pq.ParquetFile(source)
        if not parquet.metadata.num_rows:
            raise ValueError("The combined Parquet file contains no rows to export.")
        destination = settings["workspace"] / "results" / "deltasvm_selected_chromosomes.tsv.gz"
        temporary = destination.with_suffix(".gz.part")
        rows = 0
        first_batch = True
        try:
            with gzip.open(temporary, "wt", encoding="utf-8", newline="") as stream:
                for batch in parquet.iter_batches(batch_size=50000):
                    frame = batch.to_pandas()
                    frame.to_csv(stream, sep="\t", index=False, header=first_batch,
                                 na_rep=".", lineterminator="\n")
                    first_batch = False
                    rows += len(frame)
            if not rows:
                raise ValueError("The combined Parquet file contains no rows to export.")
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)
        record_source("combined_deltasvm_tsv_gzip", destination,
                      source=str(source), source_sha256=file_digest(source),
                      chromosomes=list(settings["chromosomes"]), rows=rows,
                      sha256=file_digest(destination))
        print(f"Exported {rows:,} rows to {destination}")

    return (export_combined_tsv,)


@app.cell
def __(export_combined_tsv, mo, tsv_export_button):
    mo.stop(not tsv_export_button.value, mo.md("Press the button above to create the compressed TSV."))
    export_combined_tsv()
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 28. Download the Compressed TSV
    After creating the compressed TSV in section 27, use the download button to save
    it directly without downloading the full results bundle.
    """)
    return


@app.cell
def __(mo, settings):
    tsv_path = settings["workspace"] / "results" / "deltasvm_selected_chromosomes.tsv.gz"

    def open_combined_tsv():
        if not tsv_path.is_file() or not tsv_path.stat().st_size:
            raise FileNotFoundError("Create the compressed TSV in section 27 before downloading it.")
        return tsv_path.open("rb")

    mo.download(data=open_combined_tsv,
                filename="deltasvm_selected_chromosomes.tsv.gz",
                mimetype="application/gzip", label="Download compressed deltaSVM TSV")
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 29. Export Filtered Results as Compressed TSV
    Applies the effect and minimum absolute score selected in section 16 and writes
    matching rows to `results/deltasvm_filtered.tsv.gz`.
    """)
    return


@app.cell
def __(mo):
    filtered_tsv_button = mo.ui.run_button(label='Export filtered results as compressed TSV')
    filtered_tsv_button
    return (filtered_tsv_button,)


@app.cell
def __(file_digest, record_source, settings):
    def export_filtered_tsv(minimum_score_value, effect_choice):
        import gzip
        import math
        import pyarrow as pa
        import pyarrow.parquet as pq
        import pandas as pd

        try:
            minimum_score = float(minimum_score_value)
        except (TypeError, ValueError):
            raise ValueError("Enter a numeric minimum absolute score in section 16.")
        if not math.isfinite(minimum_score) or minimum_score < 0:
            raise ValueError("The minimum absolute score must be a finite number at least zero.")
        if effect_choice not in ("Both", "Gain", "Loss"):
            raise ValueError("Select Both, Gain, or Loss in section 16.")
        source = settings["workspace"] / "results" / "deltasvm_selected_chromosomes.parquet"
        if not source.is_file() or not source.stat().st_size:
            raise FileNotFoundError("Combine the selected chromosome results before creating a filtered TSV.")
        parquet = pq.ParquetFile(source)
        destination = settings["workspace"] / "results" / "deltasvm_filtered.tsv.gz"
        temporary = destination.with_suffix(".gz.part")
        rows = 0
        first_batch = True
        try:
            with gzip.open(temporary, "wt", encoding="utf-8", newline="") as stream:
                for batch in parquet.iter_batches(batch_size=50000):
                    frame = pa.Table.from_batches([batch]).to_pandas()
                    scores = pd.to_numeric(frame["Score"], errors="coerce")
                    selected = scores.map(lambda score: math.isfinite(score) if not pd.isna(score) else False)
                    selected &= scores.abs() >= minimum_score
                    if effect_choice != "Both":
                        selected &= frame["Effect"].eq(effect_choice)
                    filtered = frame.loc[selected]
                    if not filtered.empty:
                        filtered.to_csv(stream, sep="\t", index=False, header=first_batch,
                                        na_rep=".", lineterminator="\n")
                        first_batch = False
                        rows += len(filtered)
            if not rows:
                raise ValueError("No rows matched the selected effect and minimum score.")
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)
        record_source("filtered_deltasvm_tsv_gzip", destination,
                      source=str(source), source_sha256=file_digest(source),
                      minimum_absolute_score=minimum_score, effect=effect_choice,
                      rows=rows, sha256=file_digest(destination))
        print(f"Exported {rows:,} filtered rows to {destination}")

    return (export_filtered_tsv,)


@app.cell
def __(effect_input, export_filtered_tsv, filtered_tsv_button, minimum_score_input, mo):
    mo.stop(not filtered_tsv_button.value, mo.md("Choose filters in section 16, then press the export button."))
    export_filtered_tsv(minimum_score_input.value, effect_input.value)
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 30. Download the Filtered TSV
    After exporting the filtered TSV in section 29, use the download button to save
    the compressed file directly.
    """)
    return


@app.cell
def __(mo, settings):
    filtered_tsv_path = settings["workspace"] / "results" / "deltasvm_filtered.tsv.gz"

    def open_filtered_tsv():
        if not filtered_tsv_path.is_file() or not filtered_tsv_path.stat().st_size:
            raise FileNotFoundError("Create the filtered TSV in section 29 before downloading it.")
        return filtered_tsv_path.open("rb")

    mo.download(data=open_filtered_tsv, filename="deltasvm_filtered.tsv.gz",
                mimetype="application/gzip", label="Download filtered deltaSVM TSV")
    return


@app.cell
def __(mo):
    mo.md(r"""
    ## 31. Download Individual Reports and Tables
    Use the buttons below to download available result files individually. Generate
    an output in its section before downloading it.
    """)
    return


@app.cell
def __(mo, settings):
    results_directory = settings["workspace"] / "results"
    download_files = (
        ("deltasvm_qc_report.tsv", "text/tab-separated-values"),
        ("deltasvm_tf_summary.tsv", "text/tab-separated-values"),
        ("deltasvm_top_variants_by_tf.tsv", "text/tab-separated-values"),
        ("deltasvm_skipped_variants.tsv", "text/tab-separated-values"),
        ("deltasvm_skipped_variant_summary.tsv", "text/tab-separated-values"),
        ("deltasvm_filtered.parquet", "application/octet-stream"),
        ("deltasvm_run_manifest.json", "application/json"),
        (f"{settings['single']}_run_record.json", "application/json"),
        ("README.txt", "text/plain"),
    )
    download_buttons = []
    for filename, mimetype in download_files:
        path = results_directory / filename

        def open_result(path=path):
            if not path.is_file() or not path.stat().st_size:
                raise FileNotFoundError(f"Generate {path.name} in its pipeline section before downloading it.")
            return path.open("rb")

        download_buttons.append(mo.download(data=open_result, filename=filename,
                                            mimetype=mimetype, label=f"Download {filename}"))
    mo.vstack(download_buttons)
    return


if __name__ == "__main__":
    app.run()
