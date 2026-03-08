import marimo

__generated_with = "0.9.14"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    import urllib.request
    import os
    import re
    import subprocess
    import pandas as pd
    import numpy as np
    import tarfile
    from pathlib import Path
    import gzip
    import hashlib
    from datetime import datetime
    return mo, urllib, os, re, subprocess, pd, np, tarfile, Path, gzip, hashlib, datetime


@app.cell
def __(mo):
    mo.md("""
This notebook reproduces the LDSC cell-type–specific heritability analysis  
from *Epigenomic dissection of Alzheimer's disease pinpoints causal variants and reveals epigenome erosion*.

All analyses are performed using **GRCh38 / hg38** coordinates.
""")
    return


@app.cell
def __(mo):
    GWAS_INPUT_FILE = "data/gwas/atrial_fibrillation.h.tsv.gz"

    gwas_stem = mo.state(GWAS_INPUT_FILE)
    return (GWAS_INPUT_FILE, gwas_stem)


@app.cell
def __(GWAS_INPUT_FILE, os):
    import re as _re

    _basename = os.path.basename(GWAS_INPUT_FILE)
    _no_ext = _basename
    for _ext in [".tsv.gz", ".txt.gz", ".gz", ".tsv", ".txt", ".csv"]:
        if _no_ext.endswith(_ext):
            _no_ext = _no_ext[: -len(_ext)]
            break

    GWAS_STEM = _re.sub(r"[^A-Za-z0-9_\-]", "_", _no_ext)

    SSF_FILE        = f"data/ssf/{GWAS_STEM}.h.tsv.gz"
    SSF_YAML        = f"{SSF_FILE}-meta.yaml"
    SUMSTATS_FILE   = f"data/ldsc_input/{GWAS_STEM}.sumstats.gz"
    RESULTS_PREFIX  = f"results/{GWAS_STEM}_CellTypeSpecific"

    print(f"GWAS stem      : {GWAS_STEM}")
    print(f"SSF file       : {SSF_FILE}")
    print(f"Sumstats file  : {SUMSTATS_FILE}")
    print(f"Results prefix : {RESULTS_PREFIX}")

    return (GWAS_STEM, SSF_FILE, SSF_YAML, SUMSTATS_FILE, RESULTS_PREFIX)


@app.cell
def __(mo):
    mo.md("""
This cell will:
1. Create a Python 2.7 conda environment for LDSC
2. Install LDSC and its dependencies
3. Install BEDTools (required for annotation generation)
""")
    return


@app.cell
def __(Path, subprocess, os):
    TOOLS_DIR = Path("tools")
    LDSC_DIR = TOOLS_DIR / "ldsc"
    TOOLS_DIR.mkdir(exist_ok=True)

    env_check = subprocess.run(
        ["conda", "env", "list"],
        capture_output=True,
        text=True
    )
    ldsc_env_exists = "ldsc27" in env_check.stdout

    if not ldsc_env_exists:
        print("Creating Python 2.7 conda environment for LDSC...")
        subprocess.run(["conda", "create", "-n", "ldsc27", "python=2.7", "-y"], check=True)

    conda_prefix = subprocess.run(
        ["conda", "env", "list", "--json"],
        capture_output=True, text=True, check=True
    )
    import json
    envs = json.loads(conda_prefix.stdout)["envs"]
    ldsc27_path = [e for e in envs if "ldsc27" in e][0]

    bedtools_path = os.path.join(ldsc27_path, "bin", "bedtools")
    if os.path.exists(bedtools_path):
        bedtools_check = subprocess.run([bedtools_path, "--version"], capture_output=True, text=True)
        print(f"BEDTools already installed: {bedtools_check.stdout.strip()}")
    else:
        print("Installing BEDTools in ldsc27 environment...")
        subprocess.run(
            ["conda", "install", "-n", "ldsc27", "-c", "bioconda", "bedtools", "-y"],
            check=True
        )

    if not LDSC_DIR.exists():
        print("Cloning LDSC repository...")
        subprocess.run(
            ["git", "clone", "https://github.com/bulik/ldsc.git", str(LDSC_DIR)],
            check=True
        )

    check_numpy = subprocess.run(
        [os.path.join(ldsc27_path, "bin", "python"), "-c", "import numpy"],
        capture_output=True
    )

    if check_numpy.returncode != 0:
        print("Installing LDSC dependencies in ldsc27 environment...")
        subprocess.run(
            ["conda", "install", "-n", "ldsc27", "-y", "openssl=1.0.2", "-c", "conda-forge"],
            check=True
        )
        subprocess.run(
            ["conda", "install", "-n", "ldsc27", "-y", "numpy", "scipy", "pandas", "bitarray", "-c", "conda-forge"],
            check=True
        )
        subprocess.run(
            ["conda", "install", "-n", "ldsc27", "-y", "pybedtools", "pysam=0.15.3", "-c", "bioconda", "-c", "conda-forge"],
            check=True
        )
        print("Dependencies installed!")

    print("Verifying BEDTools is accessible to pybedtools...")
    pybedtools_check = subprocess.run(
        [os.path.join(ldsc27_path, "bin", "python"), "-c",
         "from pybedtools import BedTool; import pybedtools.helpers as helpers; print('BEDTools path:', helpers.get_bedtools_path())"],
        capture_output=True, text=True
    )

    if pybedtools_check.returncode != 0:
        print("WARNING: pybedtools can't find BEDTools!")
        print("Error:", pybedtools_check.stderr)
    else:
        print("pybedtools can access BEDTools")
        print(pybedtools_check.stdout)

    ldsc_script = LDSC_DIR / "ldsc.py"
    subprocess.run(["chmod", "+x", str(ldsc_script)], check=True)

    python27_path = os.path.join(ldsc27_path, "bin", "python")

    print(f"LDSC environment ready!")
    print(f"Python 2.7 path: {python27_path}")
    print(f"LDSC path: {ldsc_script}")

    return (ldsc_script, python27_path, ldsc27_path)


@app.cell
def __(mo):
    mo.md("## 1. Discover cell types and resolve BED sources")
    return


@app.cell
def __(os, urllib, pd, subprocess, re):
    os.makedirs("data/peaks", exist_ok=True)
    os.makedirs("data/reference", exist_ok=True)
    os.makedirs("data/gwas", exist_ok=True)
    os.makedirs("data/beds", exist_ok=True)

    BROAD_CELL_TYPES = ["Ast", "Ex", "In", "Microglia", "OPC", "Oligo", "PerEndo"]
    BROAD_BASE_URL = "https://personal.broadinstitute.org/bjames/AD_snATAC/major_celltype_matrices/"

    CATLAS_DIR = "humanenhancer_atac_data"
    CATLAS_URL = "http://catlas.org/humanenhancer/data/cCREs/"

    BED_SEARCH_DIRS = [CATLAS_DIR, "data/beds"]

    def _sanitize_name(raw_name):
        return re.sub(r'[^A-Za-z0-9_\-]', '_', raw_name)

    def _ensure_catlas_downloaded():
        bed_files = [
            f for f in os.listdir(CATLAS_DIR)
            if f.endswith(".bed") and not f.startswith(".")
        ] if os.path.exists(CATLAS_DIR) else []
        if bed_files:
            print(f"{CATLAS_DIR} already has {len(bed_files)} BED files, skipping download")
            return
        print(f"Downloading all cell type BEDs from {CATLAS_URL}")
        os.makedirs(CATLAS_DIR, exist_ok=True)
        subprocess.run(
            ["wget", "-r", "-np", "-nH", "--cut-dirs=3", "-R", "index.html*", "-P", CATLAS_DIR, CATLAS_URL],
            check=True
        )
        downloaded = [f for f in os.listdir(CATLAS_DIR) if f.endswith(".bed")]
        print(f"Downloaded {len(downloaded)} BED files to {CATLAS_DIR}/")

    def _find_local_bed(raw_name):
        for d in BED_SEARCH_DIRS:
            candidate = os.path.join(d, f"{raw_name}.bed")
            if os.path.exists(candidate):
                return candidate
        return None

    def _peak_annotation_to_bed(ct):
        peak_txt = f"data/peaks/{ct}.peak.annotation.txt"
        bed_out = f"data/beds/{ct}.bed"
        if os.path.exists(bed_out):
            return bed_out
        if not os.path.exists(peak_txt):
            url = f"{BROAD_BASE_URL}{ct}.peak.annotation.txt"
            print(f"  Downloading peak annotation for {ct}...")
            urllib.request.urlretrieve(url, peak_txt)
        peaks = pd.read_csv(peak_txt, sep="\t")
        _bed = peaks[['seqnames', 'start', 'end']].copy()
        _bed.columns = ['chr', 'start', 'end']
        _bed.to_csv(bed_out, sep="\t", index=False, header=False)
        return bed_out

    _ensure_catlas_downloaded()

    raw_names = set()
    for _d in BED_SEARCH_DIRS:
        if os.path.exists(_d):
            for _f in os.listdir(_d):
                if _f.endswith(".bed") and not _f.startswith("."):
                    raw_names.add(os.path.splitext(_f)[0])
    for _ct in BROAD_CELL_TYPES:
        raw_names.add(_ct)

    print(f"Resolving BED files for {len(raw_names)} cell types")

    cell_type_beds = {}
    name_conflicts = {}

    for _raw in sorted(raw_names):
        _safe = _sanitize_name(_raw)
        if _safe in name_conflicts:
            print(f"  Name collision after sanitization: '{_raw}' -> '{_safe}' (already used by '{name_conflicts[_safe]}')")
            _safe = _safe + "_2"
        name_conflicts[_safe] = _raw

        _local = _find_local_bed(_raw)
        if _local:
            cell_type_beds[_safe] = _local
            marker = "(local BED)" if _raw == _safe else f"(local BED, '{_raw}' -> '{_safe}')"
            print(f"  {_safe}  ->  {_local}  {marker}")
        else:
            _bed = _peak_annotation_to_bed(_raw)
            cell_type_beds[_safe] = _bed
            print(f"  {_safe}  ->  {_bed}  (converted from peak annotation)")

    all_cell_types = sorted(cell_type_beds.keys())
    print(f"\n{len(all_cell_types)} cell types ready")

    if not os.path.exists("data/reference/GRCh38.tgz"):
        print("\nDownloading GRCh38 reference with baseline LD scores...")
        urllib.request.urlretrieve(
            "https://zenodo.org/records/10515792/files/GRCh38.tgz?download=1",
            "data/reference/GRCh38.tgz"
        )
        print("GRCh38 reference downloaded")
    else:
        print("\nGRCh38 reference already downloaded")

    if not os.path.exists(GWAS_INPUT_FILE):
        print(f"Downloading GWAS summary statistics to {GWAS_INPUT_FILE}...")
        urllib.request.urlretrieve(
            "http://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/GCST90027001-GCST90028000/GCST90027158/GCST90027158_buildGRCh38.tsv.gz",
            GWAS_INPUT_FILE
        )
        print("GWAS data downloaded")
    else:
        print(f"GWAS data already present: {GWAS_INPUT_FILE}")

    print("\nAll sources resolved!")
    return (all_cell_types, cell_type_beds)


@app.cell
def __(mo):
    mo.md("## 2. Extract reference LD panels and baseline LD scores")
    return


@app.cell
def __(tarfile, os):
    print("Checking GRCh38.tgz contents...")

    if os.path.exists("data/reference/GRCh38.tgz"):
        with tarfile.open("data/reference/GRCh38.tgz", "r:gz") as _tar:
            members = _tar.getmembers()
            print(f"Archive contains {len(members)} items")
            print("\nTop-level structure:")
            seen = set()
            for m in members[:50]:
                parts = m.name.split('/')
                if len(parts) > 1:
                    top = parts[0] + "/" + parts[1]
                    if top not in seen:
                        print(f"  {top}")
                        seen.add(top)
    return


@app.cell
def __(tarfile, os):
    print("Extracting GRCh38 reference files...")

    if not os.path.exists("data/reference/GRCh38"):
        try:
            print("  Verifying GRCh38.tgz integrity...")
            with tarfile.open("data/reference/GRCh38.tgz", "r:gz") as _tar:
                _tar.getmembers()
            print("  File integrity verified")
        except (EOFError, tarfile.ReadError) as e:
            print(f"\n  Error: GRCh38.tgz is corrupted!")
            print(f"  Please delete it and re-run: rm data/reference/GRCh38.tgz")
            raise

        print("  Extracting...")
        with tarfile.open("data/reference/GRCh38.tgz", "r:gz") as _tar:
            _tar.extractall("data/reference")
        print("GRCh38 reference extracted successfully")
    else:
        print("GRCh38 directory exists")

    nested_files = [
        ("data/reference/GRCh38/baselineLD_v2.2.tgz", "data/reference", "baselineLD_v2.2"),
        ("data/reference/GRCh38/plink_files.tgz", "data/reference/GRCh38", "plink_files/1000G.EUR.hg38.1.bim"),
        ("data/reference/GRCh38/weights.tgz", "data/reference/GRCh38", "weights")
    ]

    for tar_file, extract_to, check_file in nested_files:
        check_path = os.path.join(extract_to, check_file)
        if os.path.exists(check_path):
            print(f"  {os.path.basename(tar_file)} already extracted")
            continue

        if os.path.exists(tar_file):
            print(f"  Extracting {os.path.basename(tar_file)}...")
            with tarfile.open(tar_file, "r:gz") as _tar:
                _tar.extractall(extract_to)
            print(f"  {os.path.basename(tar_file)} extracted")

            if not os.path.exists(check_path):
                print(f"  Warning: Expected file {check_path} not found after extraction")
        else:
            print(f"  Warning: {tar_file} not found")

    critical_file = "data/reference/GRCh38/plink_files/1000G.EUR.hg38.1.bim"
    if os.path.exists(critical_file):
        print(f"\nAll reference files ready! Verified: {critical_file}")
    else:
        print(f"\nERROR: Critical file missing: {critical_file}")
        print("Checking what files exist in data/reference/GRCh38/:")
        if os.path.exists("data/reference/GRCh38"):
            files = os.listdir("data/reference/GRCh38")
            print(f"  Found {len(files)} files/directories")
            for _f in sorted(files)[:10]:
                print(f"    - {_f}")
        else:
            print("  Directory doesn't exist!")

    return


@app.cell
def __(mo):
    mo.md("## 3. Convert GWAS to GWAS-SSF format")
    return


@app.cell
def __(GWAS_INPUT_FILE, SSF_FILE, SSF_YAML, pd, os, gzip, subprocess, hashlib, datetime):
    os.makedirs("data/ssf", exist_ok=True)

    if os.path.exists(SSF_FILE):
        print(f"SSF file already exists, skipping conversion: {SSF_FILE}")
    else:
        print(f"Converting {GWAS_INPUT_FILE} -> {SSF_FILE}")

        _df = pd.read_csv(GWAS_INPUT_FILE, sep="\t", compression="gzip")
        _cols_lower = {col.lower(): col for col in _df.columns}
        _col_map = {}

        if "variant" in _cols_lower:
            print("Detected Neale format")
            _df[['chromosome', 'base_pair_location', 'other_allele', 'effect_allele']] = \
                _df[_cols_lower['variant']].str.split(':', expand=True)
        else:
            print("Detected PLINK/standard format")
            _col_map['chromosome'] = _cols_lower.get('chr') or _cols_lower.get('chrom') or _cols_lower.get('chromosome')
            _col_map['base_pair_location'] = _cols_lower.get('bp') or _cols_lower.get('pos') or _cols_lower.get('base_pair_location')
            _col_map['effect_allele'] = _cols_lower.get('a1') or _cols_lower.get('effect_allele')
            _col_map['other_allele'] = _cols_lower.get('a2') or _cols_lower.get('other_allele')

        _col_map['beta'] = _cols_lower.get('beta')
        _col_map['standard_error'] = (_cols_lower.get('se') or _cols_lower.get('stderr') or _cols_lower.get('standard_error'))
        _col_map['p_value'] = _cols_lower.get('p') or _cols_lower.get('pval') or _cols_lower.get('p_value')
        _col_map['effect_allele_frequency'] = (
            _cols_lower.get('a1_freq') or _cols_lower.get('frq') or
            _cols_lower.get('af') or _cols_lower.get('effect_allele_frequency')
        )
        _col_map['rsid'] = (
            _cols_lower.get('id') or _cols_lower.get('snp') or
            _cols_lower.get('rsid') or _cols_lower.get('variant_id')
        )

        if "variant" not in _cols_lower:
            _rename_map = {v: k for k, v in _col_map.items() if v is not None}
            _df = _df.rename(columns=_rename_map)

        _df['chromosome'] = _df['chromosome'].astype(str).str.replace('chr', '', regex=False)
        _df['chromosome'] = _df['chromosome'].replace({'23': 'X', '24': 'Y', '26': 'MT'})

        print("Filtering chromosomes (removing X, Y, MT)...")
        _before = len(_df)
        _df = _df[~_df['chromosome'].isin(['X', 'Y', 'MT'])]
        print(f"  Removed {_before - len(_df)} variants on sex/MT chromosomes")

        _df['base_pair_location'] = pd.to_numeric(_df['base_pair_location'], errors='coerce')
        _df['effect_allele'] = _df['effect_allele'].str.upper()
        _df['other_allele'] = _df['other_allele'].str.upper()

        if 'effect_allele_frequency' in _df.columns:
            _df['effect_allele_frequency'] = _df['effect_allele_frequency'].fillna('NA')
        else:
            _df['effect_allele_frequency'] = 'NA'

        if 'rsid' in _df.columns:
            _df['rsid'] = _df['rsid'].fillna('NA')
        else:
            _df['rsid'] = 'NA'

        ssf_columns = ['chromosome', 'base_pair_location', 'effect_allele', 'other_allele',
                       'beta', 'standard_error', 'p_value', 'effect_allele_frequency', 'rsid']
        _df_ssf = _df[ssf_columns]

        print(f"Writing SSF file: {SSF_FILE}")
        _df_ssf.to_csv(SSF_FILE, sep="\t", index=False, compression="gzip")

        print("Creating tabix index...")
        subprocess.run(
            ["tabix", "-c", "N", "-S", "1", "-s", "1", "-b", "2", "-e", "2", SSF_FILE],
            check=False
        )

        with gzip.open(SSF_FILE, 'rb') as f:
            _md5 = hashlib.md5(f.read()).hexdigest()

        with open(SSF_YAML, 'w') as _f:
            _f.write(f"""# Study meta-data
date_metadata_last_modified: {datetime.now().strftime('%Y-%m-%d')}

genome_assembly: GRCh38
coordinate_system: 1-based

data_file_name: {os.path.basename(SSF_FILE)}
file_type: GWAS-SSF v0.1
data_file_md5sum: {_md5}

is_harmonised: false
is_sorted: false
""")

        print("SSF conversion complete")
        chromosomes_present = sorted(_df_ssf['chromosome'].unique(), key=lambda x: int(x))
        print(f"Chromosomes in data: {', '.join(chromosomes_present)}")

    return


@app.cell
def __(mo):
    mo.md("## 4. Convert SSF to LDSC format")
    return


@app.cell
def __(SSF_FILE, SUMSTATS_FILE, pd, np, os):
    os.makedirs("data/ldsc_input", exist_ok=True)

    if os.path.exists(SUMSTATS_FILE):
        print(f"LDSC sumstats file already exists: {SUMSTATS_FILE}")
    else:
        print(f"Converting {SSF_FILE} -> {SUMSTATS_FILE}")

        _df = pd.read_csv(SSF_FILE, sep='\t', compression='gzip')
        print(f"Input: {len(_df)} variants")

        _ldsc = pd.DataFrame()

        if 'rsid' in _df.columns and (_df['rsid'] != 'NA').any():
            _ldsc['SNP'] = _df['rsid']
            _missing = (_ldsc['SNP'].isna()) | (_ldsc['SNP'] == 'NA')
            _ldsc.loc[_missing, 'SNP'] = (
                _df.loc[_missing, 'chromosome'].astype(str) + ':' +
                _df.loc[_missing, 'base_pair_location'].astype(str)
            )
        else:
            _ldsc['SNP'] = (
                _df['chromosome'].astype(str) + ':' +
                _df['base_pair_location'].astype(str)
            )

        _ldsc['A1'] = _df['effect_allele'].str.upper()
        _ldsc['A2'] = _df['other_allele'].str.upper()
        _ldsc['Z'] = _df['beta'] / _df['standard_error']

        _n_cases = 111326
        _n_controls = 677663
        _effective_n = 4 / (1/_n_cases + 1/_n_controls)
        _ldsc['N'] = int(_effective_n)
        print(f"Using effective sample size: {int(_effective_n):,}")

        if 'p_value' in _df.columns:
            _ldsc['P'] = _df['p_value']

        _before = len(_ldsc)
        _ldsc = _ldsc[np.isfinite(_ldsc['Z'])]
        if _before > len(_ldsc):
            print(f"Removed {_before - len(_ldsc)} variants with invalid Z-scores")

        _before = len(_ldsc)
        _ldsc = _ldsc.drop_duplicates(subset=['SNP'])
        if _before > len(_ldsc):
            print(f"Removed {_before - len(_ldsc)} duplicate variants")

        print(f"Output: {len(_ldsc)} variants")

        _ldsc.to_csv(SUMSTATS_FILE, sep='\t', index=False, compression='gzip')

        print(f"LDSC format conversion complete")
        print(f"  Mean |Z|: {_ldsc['Z'].abs().mean():.3f}")
        print(f"  Median |Z|: {_ldsc['Z'].abs().median():.3f}")
        if 'P' in _ldsc.columns:
            _sig = (_ldsc['P'] < 5e-8).sum()
            print(f"  Genome-wide significant (P < 5e-8): {_sig:,}")

    return


@app.cell
def __(mo):
    mo.md("## 5. Generate cell-type–specific binary annotations (BED → .annot.gz)")
    return


@app.cell
def __(subprocess, os, all_cell_types, cell_type_beds, python27_path, ldsc27_path):
    os.makedirs("data/annotations", exist_ok=True)

    env = os.environ.copy()
    env["PATH"] = f"{ldsc27_path}/bin:" + env.get("PATH", "")

    for _ct in all_cell_types:
        print(f"\nProcessing {_ct}...")
        _all_exist = all(
            os.path.exists(f"data/annotations/{_ct}.{_chrom}.annot.gz")
            for _chrom in range(1, 23)
        )

        if _all_exist:
            print(f"  All {_ct} annotations already exist, skipping")
            continue

        _bed_file = cell_type_beds[_ct]

        for _chrom in range(1, 23):
            annot_file = f"data/annotations/{_ct}.{_chrom}.annot.gz"
            if os.path.exists(annot_file):
                print(f"  Chromosome {_chrom} (exists)", end=" ", flush=True)
                continue

            print(f"  Chromosome {_chrom}...", end=" ", flush=True)
            _result = subprocess.run([
                python27_path, "tools/ldsc/make_annot.py",
                "--bed-file", _bed_file,
                "--bimfile", f"data/reference/GRCh38/plink_files/1000G.EUR.hg38.{_chrom}.bim",
                "--annot-file", annot_file
            ], capture_output=True, text=True, env=env)

            if _result.returncode != 0:
                print(f"\n\nERROR on chromosome {_chrom}:")
                print("STDERR:", _result.stderr)
                print("STDOUT:", _result.stdout)
                raise subprocess.CalledProcessError(_result.returncode, _result.args)
            print("done")
        print(f"  {_ct} complete")

    print("\nAll annotations generated")
    return


@app.cell
def __(mo):
    mo.md("## 6. Calculate LD scores for each cell type and chromosome")
    return


@app.cell
def __(subprocess, os, all_cell_types, python27_path):
    os.makedirs("data/ldscores", exist_ok=True)

    for _ct in all_cell_types:
        print(f"\nProcessing {_ct}...")
        os.makedirs(f"data/ldscores/{_ct}", exist_ok=True)

        _all_exist = all(
            os.path.exists(f"data/ldscores/{_ct}/{_ct}.{_chrom}.l2.ldscore.gz")
            for _chrom in range(1, 23)
        )

        if _all_exist:
            print(f"  All {_ct} LD scores already exist, skipping")
            continue

        for _chrom in range(1, 23):
            ldscore_file = f"data/ldscores/{_ct}/{_ct}.{_chrom}.l2.ldscore.gz"
            if os.path.exists(ldscore_file):
                print(f"  Chromosome {_chrom} (exists)", end=" ", flush=True)
                continue

            print(f"  Chromosome {_chrom}...", end=" ", flush=True)
            subprocess.run([
                python27_path, "tools/ldsc/ldsc.py",
                "--l2",
                "--bfile", f"data/reference/GRCh38/plink_files/1000G.EUR.hg38.{_chrom}",
                "--ld-wind-cm", "1.0",
                "--annot", f"data/annotations/{_ct}.{_chrom}.annot.gz",
                "--thin-annot",
                "--out", f"data/ldscores/{_ct}/{_ct}.{_chrom}"
            ], check=True, capture_output=True)
            print("done")
        print(f"  {_ct} complete")

    print("\nAll LD scores calculated")
    return


@app.cell
def __(mo):
    mo.md("## 7. Create CTS (cell-type–specific) reference file")
    return


@app.cell
def __(os, all_cell_types, GWAS_STEM):
    os.makedirs("results", exist_ok=True)
    cts_path = f"data/{GWAS_STEM}_cell_types.cts"
    with open(cts_path, "w") as _f:
        for _ct in all_cell_types:
            _f.write(f"{_ct}\tdata/ldscores/{_ct}/{_ct}.\n")
    print(f"CTS reference file created: {cts_path} ({len(all_cell_types)} cell types)")
    return (cts_path,)


@app.cell
def __(mo):
    mo.md("## 8. Run LDSC cell-type–specific heritability analysis (CTS)")
    return


@app.cell
def __(all_cell_types, python27_path, SUMSTATS_FILE, GWAS_STEM, RESULTS_PREFIX, os, pd, datetime):
    from concurrent.futures import ProcessPoolExecutor, as_completed

    BATCH_SIZE  = 25
    MAX_WORKERS = 4

    def run_cts_batch(args):
        batch_idx, cell_type_subset, py_path, sumstats, gwas_stem = args
        import os, subprocess
        cts_path = f"data/{gwas_stem}_cell_types_batch_{batch_idx}.cts"
        with open(cts_path, "w") as f:
            for ct in cell_type_subset:
                f.write(f"{ct}\tdata/ldscores/{ct}/{ct}.\n")

        out_prefix  = f"results/{gwas_stem}_CTS_batch_{batch_idx}"
        result_path = f"{out_prefix}.cell_type_results.txt"
        if os.path.exists(result_path):
            return batch_idx, True, "already exists"

        r = subprocess.run([
            py_path, "tools/ldsc/ldsc.py",
            "--h2-cts",         sumstats,
            "--ref-ld-chr",     "data/reference/baselineLD_v2.2/baselineLD.",
            "--ref-ld-chr-cts", cts_path,
            "--w-ld-chr",       "data/reference/GRCh38/weights/weights.hm3_noMHC.",
            "--out",            out_prefix,
        ], capture_output=True, text=True)

        if r.returncode != 0:
            return batch_idx, False, r.stderr[:300]
        return batch_idx, True, "ok"

    if SUMSTATS_FILE is None or not os.path.exists(SUMSTATS_FILE):
        print(f"Skipping LDSC analysis - sumstats file not found: {SUMSTATS_FILE}")
    else:
        os.makedirs("results", exist_ok=True)
        batches = [all_cell_types[i:i+BATCH_SIZE] for i in range(0, len(all_cell_types), BATCH_SIZE)]
        print(f"Running {len(batches)} batches of <=={BATCH_SIZE} cell types, {MAX_WORKERS} workers")
        print(f"Started: {datetime.now().strftime('%H:%M:%S')}")

        tasks  = [(i, b, python27_path, SUMSTATS_FILE, GWAS_STEM) for i, b in enumerate(batches)]
        failed = []
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(run_cts_batch, t): t[0] for t in tasks}
            for fut in as_completed(futures):
                idx, ok, msg = fut.result()
                print(f"  {'done' if ok else 'FAILED'} Batch {idx:03d} -> {msg}")
                if not ok:
                    failed.append(idx)

        print(f"Finished: {datetime.now().strftime('%H:%M:%S')}")
        if failed:
            print(f"Failed batches: {failed}")

        dfs = []
        for i in range(len(batches)):
            p = f"results/{GWAS_STEM}_CTS_batch_{i}.cell_type_results.txt"
            if os.path.exists(p):
                dfs.append(pd.read_csv(p, sep="\t"))

        if dfs:
            _ranked = pd.concat(dfs, ignore_index=True).sort_values("Coefficient_P_value")
            _ranked_csv = f"{RESULTS_PREFIX}_ranked.csv"
            _ranked_tsv = f"{RESULTS_PREFIX}.cell_type_results.txt"
            _ranked.to_csv(_ranked_csv, index=False)
            _ranked.to_csv(_ranked_tsv, sep="\t", index=False)
            print(f"\nMerged {len(_ranked)} cell types -> {_ranked_csv}")
            print(_ranked[["Name","Coefficient","Coefficient_std_error","Coefficient_P_value"]].head(10).to_string(index=False))

    return


@app.cell
def __(mo):
    mo.md("## 9. Rank cell types by heritability enrichment significance")
    return


@app.cell
def __(RESULTS_PREFIX, pd, os):
    results_file = f"{RESULTS_PREFIX}.cell_type_results.txt"

    if not os.path.exists(results_file):
        print(f"Results file not found: {results_file}")
        print("Please run Step 8 first to generate the cell-type-specific analysis results.")
        ranked = None
    else:
        results = pd.read_csv(results_file, sep="\t")
        ranked = results.sort_values("Coefficient_P_value")
        _ranked_csv = f"{RESULTS_PREFIX}_ranked.csv"
        ranked.to_csv(_ranked_csv, index=False)

        print("Cell types ranked by heritability enrichment p-value:\n")
        print(ranked[["Name", "Coefficient", "Coefficient_std_error", "Coefficient_P_value"]].to_string(index=False))
        print(f"\nResults saved to {_ranked_csv}")

    return (ranked,)


if __name__ == "__main__":
    app.run()