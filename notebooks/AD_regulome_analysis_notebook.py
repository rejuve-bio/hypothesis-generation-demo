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
    from pathlib import Path
    import gzip
    import hashlib
    from datetime import datetime
    return mo, urllib, os, re, subprocess, pd, np, Path, gzip, hashlib, datetime


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
    GWAS_INPUT_FILE = "data/gwas/30061737-GCST006414-EFO_0000275.h.tsv.gz"
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

    SSF_FILE       = f"data/ssf/{GWAS_STEM}.h.tsv.gz"
    SSF_YAML       = f"{SSF_FILE}-meta.yaml"
    SUMSTATS_FILE  = f"data/ldsc_input/{GWAS_STEM}.sumstats.gz"
    RESULTS_PREFIX = f"results/{GWAS_STEM}_CellTypeSpecific"

    print(f"GWAS stem      : {GWAS_STEM}")
    print(f"SSF file       : {SSF_FILE}")
    print(f"Sumstats file  : {SUMSTATS_FILE}")
    print(f"Results prefix : {RESULTS_PREFIX}")

    return (GWAS_STEM, SSF_FILE, SSF_YAML, SUMSTATS_FILE, RESULTS_PREFIX)


@app.cell
def __(mo):
    mo.md("## 0. Setup: Download and configure LDSC")
    return


@app.cell
def __(Path, subprocess, os):
    TOOLS_DIR = Path("tools")
    LDSC_DIR = TOOLS_DIR / "ldsc"
    TOOLS_DIR.mkdir(exist_ok=True)

    env_check = subprocess.run(["conda", "env", "list"], capture_output=True, text=True)
    ldsc_env_exists = "ldsc27" in env_check.stdout

    if not ldsc_env_exists:
        subprocess.run(["conda", "create", "-n", "ldsc27", "python=2.7", "-y"], check=True)

    conda_prefix = subprocess.run(
        ["conda", "env", "list", "--json"],
        capture_output=True, text=True, check=True
    )
    import json
    envs = json.loads(conda_prefix.stdout)["envs"]
    ldsc27_path = [e for e in envs if "ldsc27" in e][0]

    bedtools_path = os.path.join(ldsc27_path, "bin", "bedtools")
    if not os.path.exists(bedtools_path):
        subprocess.run(
            ["conda", "install", "-n", "ldsc27", "-c", "bioconda", "bedtools", "-y"],
            check=True
        )

    if not LDSC_DIR.exists():
        subprocess.run(
            ["git", "clone", "https://github.com/bulik/ldsc.git", str(LDSC_DIR)],
            check=True
        )

    check_numpy = subprocess.run(
        [os.path.join(ldsc27_path, "bin", "python"), "-c", "import numpy"],
        capture_output=True
    )

    if check_numpy.returncode != 0:
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
def __(os, urllib, pd, subprocess, re, GWAS_INPUT_FILE):
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
            _safe = _safe + "_2"
        name_conflicts[_safe] = _raw

        _local = _find_local_bed(_raw)
        if _local:
            cell_type_beds[_safe] = _local
            print(f"  {_safe}  ->  {_local}")
        else:
            _bed = _peak_annotation_to_bed(_raw)
            cell_type_beds[_safe] = _bed
            print(f"  {_safe}  ->  {_bed}  (converted from peak annotation)")

    all_cell_types = sorted(cell_type_beds.keys())
    print(f"\n{len(all_cell_types)} cell types ready")

    if not os.path.exists("data/reference/GRCh38.tgz"):
        print("Downloading GRCh38 reference...")
        urllib.request.urlretrieve(
            "https://zenodo.org/records/10515792/files/GRCh38.tgz?download=1",
            "data/reference/GRCh38.tgz"
        )

    if not os.path.exists("data/reference/hm3_no_MHC.list.txt"):
        print("Downloading HapMap3 SNP list...")
        urllib.request.urlretrieve(
            "https://zenodo.org/records/10515792/files/hm3_no_MHC.list.txt?download=1",
            "data/reference/hm3_no_MHC.list.txt"
        )

    if not os.path.exists(GWAS_INPUT_FILE):
        print(f"Downloading GWAS summary statistics to {GWAS_INPUT_FILE}...")
        urllib.request.urlretrieve(
            "http://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/GCST90027001-GCST90028000/GCST90027158/GCST90027158_buildGRCh38.tsv.gz",
            GWAS_INPUT_FILE
        )

    print("\nAll sources resolved!")
    return (all_cell_types, cell_type_beds)


@app.cell
def __(mo):
    mo.md("## 2. Extract reference LD panels and baseline LD scores")
    return


@app.cell
def __(subprocess, os):
    if not os.path.exists("data/reference/GRCh38"):
        subprocess.run(
            ["tar", "-xzf", "data/reference/GRCh38.tgz", "-C", "data/reference"],
            check=True
        )

    nested_files = [
        ("data/reference/GRCh38/baselineLD_v2.2.tgz", "data/reference", "baselineLD_v2.2"),
        ("data/reference/GRCh38/plink_files.tgz", "data/reference/GRCh38", "plink_files/1000G.EUR.hg38.1.bim"),
        ("data/reference/GRCh38/weights.tgz", "data/reference/GRCh38", "weights")
    ]

    for tar_file, extract_to, check_file in nested_files:
        _check_path = os.path.join(extract_to, check_file)
        if os.path.exists(_check_path):
            continue
        if os.path.exists(tar_file):
            subprocess.run(["tar", "-xzf", tar_file, "-C", extract_to], check=True)

    _critical = "data/reference/GRCh38/plink_files/1000G.EUR.hg38.1.bim"
    if os.path.exists(_critical):
        print("All reference files ready!")
    else:
        print(f"ERROR: Critical file missing: {_critical}")

    return


@app.cell
def __(mo):
    mo.md("## 3. Convert GWAS to GWAS-SSF format")
    return


@app.cell
def __(GWAS_INPUT_FILE, SSF_FILE, SSF_YAML, pd, os, gzip, subprocess, hashlib, datetime):
    os.makedirs("data/ssf", exist_ok=True)

    if os.path.exists(SSF_FILE):
        print(f"SSF file already exists, skipping: {SSF_FILE}")
    else:
        print(f"Converting {GWAS_INPUT_FILE} -> {SSF_FILE}")

        _df = pd.read_csv(GWAS_INPUT_FILE, sep="\t", compression="gzip")

        _has_hm = any(c.startswith("hm_") for c in _df.columns)

        if _has_hm:
            print("Detected GWAS Catalog harmonised format (hm_ columns)")
            _df2 = pd.DataFrame()
            _df2['chromosome']            = _df['hm_chrom'].astype(str)
            _df2['base_pair_location']    = pd.to_numeric(_df['hm_pos'], errors='coerce')
            _df2['effect_allele']         = _df['hm_effect_allele'].str.upper()
            _df2['other_allele']          = _df['hm_other_allele'].str.upper()
            _df2['beta']                  = pd.to_numeric(_df['hm_beta'], errors='coerce')
            _df2['standard_error']        = pd.to_numeric(_df['standard_error'], errors='coerce')
            _df2['p_value']               = pd.to_numeric(_df['p_value'], errors='coerce')
            _df2['effect_allele_frequency'] = pd.to_numeric(_df['hm_effect_allele_frequency'], errors='coerce').fillna('NA')
            _df2['rsid']                  = _df['hm_rsid'].fillna('NA')
            _df = _df2
        else:
            _cols_lower = {col.lower(): col for col in _df.columns}
            if "variant" in _cols_lower:
                print("Detected Neale format")
                _df[['chromosome', 'base_pair_location', 'other_allele', 'effect_allele']] = \
                    _df[_cols_lower['variant']].str.split(':', expand=True)
            else:
                print("Detected PLINK/standard format")
                _col_map = {}
                _col_map['chromosome']          = _cols_lower.get('chr') or _cols_lower.get('chrom') or _cols_lower.get('chromosome')
                _col_map['base_pair_location']  = _cols_lower.get('bp') or _cols_lower.get('pos') or _cols_lower.get('base_pair_location')
                _col_map['effect_allele']       = _cols_lower.get('a1') or _cols_lower.get('effect_allele')
                _col_map['other_allele']        = _cols_lower.get('a2') or _cols_lower.get('other_allele')
                _col_map['beta']                = _cols_lower.get('beta')
                _col_map['standard_error']      = _cols_lower.get('se') or _cols_lower.get('stderr') or _cols_lower.get('standard_error')
                _col_map['p_value']             = _cols_lower.get('p') or _cols_lower.get('pval') or _cols_lower.get('p_value')
                _col_map['effect_allele_frequency'] = _cols_lower.get('a1_freq') or _cols_lower.get('frq') or _cols_lower.get('af') or _cols_lower.get('effect_allele_frequency')
                _col_map['rsid']                = _cols_lower.get('id') or _cols_lower.get('snp') or _cols_lower.get('rsid') or _cols_lower.get('variant_id')
                _col_map['n_total']             = _cols_lower.get('n') or _cols_lower.get('n_total') or _cols_lower.get('sample_size')
                _rename_map = {v: k for k, v in _col_map.items() if v is not None}
                _df = _df.rename(columns=_rename_map)

        _df['chromosome'] = _df['chromosome'].astype(str).str.replace('chr', '', regex=False)
        _df['chromosome'] = _df['chromosome'].replace({'23': 'X', '24': 'Y', '26': 'MT'})

        _before = len(_df)
        _df = _df[~_df['chromosome'].isin(['X', 'Y', 'MT'])]
        print(f"  Removed {_before - len(_df)} variants on sex/MT chromosomes")

        _ssf_cols = ['chromosome', 'base_pair_location', 'effect_allele', 'other_allele',
                     'beta', 'standard_error', 'p_value', 'effect_allele_frequency', 'rsid']
        if 'n_total' in _df.columns:
            _ssf_cols.append('n_total')

        _df_ssf = _df[[c for c in _ssf_cols if c in _df.columns]]
        _df_ssf.to_csv(SSF_FILE, sep="\t", index=False, compression="gzip")

        subprocess.run(
            ["tabix", "-c", "N", "-S", "1", "-s", "1", "-b", "2", "-e", "2", SSF_FILE],
            check=False
        )

        with gzip.open(SSF_FILE, 'rb') as _f:
            _md5 = hashlib.md5(_f.read()).hexdigest()

        with open(SSF_YAML, 'w') as _f:
            _f.write(f"""# Study meta-data
date_metadata_last_modified: {datetime.now().strftime('%Y-%m-%d')} 
genome_assembly: GRCh38
coordinate_system: 1-based
data_file_name: {os.path.basename(SSF_FILE)}
file_type: GWAS-SSF v0.1
data_file_md5sum: {_md5}
is_harmonised: true
is_sorted: false
""")
        print("SSF conversion complete")

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
            _ldsc['SNP'] = _df['chromosome'].astype(str) + ':' + _df['base_pair_location'].astype(str)

        _ldsc['A1'] = _df['effect_allele'].str.upper()
        _ldsc['A2'] = _df['other_allele'].str.upper()
        _ldsc['Z']  = pd.to_numeric(_df['beta'], errors='coerce') / pd.to_numeric(_df['standard_error'], errors='coerce')
        _ldsc['N']  = 1030836
        print("Using hardcoded N = 1,030,836 (Nielsen et al. 2018 AF GWAS)")

        if 'p_value' in _df.columns:
            _ldsc['P'] = pd.to_numeric(_df['p_value'], errors='coerce')

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
            print(f"  Genome-wide significant (P < 5e-8): {(_ldsc['P'] < 5e-8).sum():,}")

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
            _annot_file = f"data/annotations/{_ct}.{_chrom}.annot.gz"
            if os.path.exists(_annot_file):
                print(f"  Chromosome {_chrom} (exists)", end=" ", flush=True)
                continue

            print(f"  Chromosome {_chrom}...", end=" ", flush=True)
            _result = subprocess.run([
                python27_path, "tools/ldsc/make_annot.py",
                "--bed-file", _bed_file,
                "--bimfile", f"data/reference/GRCh38/plink_files/1000G.EUR.hg38.{_chrom}.bim",
                "--annot-file", _annot_file
            ], capture_output=True, text=True, env=env)

            if _result.returncode != 0:
                print(f"\nERROR on chromosome {_chrom}:")
                print("STDERR:", _result.stderr)
                raise subprocess.CalledProcessError(_result.returncode, _result.args)
            print("done")
        print(f"  {_ct} complete")

    print("\nAll annotations generated")
    return


@app.cell
def __(mo):
    mo.md("## 6. Calculate LD scores for each cell type and chromosome (HapMap3 SNPs only)")
    return


@app.cell
def __(subprocess, os, all_cell_types, python27_path):
    import concurrent.futures
    import multiprocessing

    os.makedirs("data/ldscores", exist_ok=True)
    HM3_SNP_LIST = "data/reference/hm3_no_MHC.list.txt"

    def calculate_single_chrom(args):
        ct, chrom = args
        out_dir = f"data/ldscores/{ct}"
        os.makedirs(out_dir, exist_ok=True)

        ldscore_file = f"{out_dir}/{ct}.{chrom}.l2.ldscore.gz"
        if os.path.exists(ldscore_file):
            return f"[{ct}] Chr {chrom} already exists. Skipping."

        try:
            subprocess.run([
                python27_path, "tools/ldsc/ldsc.py",
                "--l2",
                "--bfile",      f"data/reference/GRCh38/plink_files/1000G.EUR.hg38.{chrom}",
                "--ld-wind-cm", "1.0",
                "--annot",      f"data/annotations/{ct}.{chrom}.annot.gz",
                "--thin-annot",
                "--print-snps", HM3_SNP_LIST,
                "--out",        f"{out_dir}/{ct}.{chrom}"
            ], check=True, capture_output=True)
            return f"[{ct}] Chr {chrom} calculation complete."
        except subprocess.CalledProcessError as e:
            return f"ERROR on [{ct}] Chr {chrom}: {e.stderr}"

    tasks = []
    for ct in all_cell_types:
        for chrom in range(1, 23):
            tasks.append((ct, chrom))
    max_workers = min(multiprocessing.cpu_count() - 1, 6)

    print(f"Starting parallel LDSC calculation with {max_workers} workers...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(calculate_single_chrom, tasks):
            print(result)

    print("\nAll LD scores calculated using HapMap3 SNPs")
    return


@app.cell
def __(mo):
    mo.md("## 7. Create CTS (cell-type–specific) reference file")
    return


@app.cell
def __(os, all_cell_types, GWAS_STEM):
    os.makedirs("results", exist_ok=True)
    cts_path = f"data/{GWAS_STEM}_cell_types.cts"

    COMPLETED_CELL_TYPES = []
    for _ct in all_cell_types:
        _all_exist = all(
            os.path.exists(f"data/ldscores/{_ct}/{_ct}.{_chrom}.l2.ldscore.gz")
            for _chrom in range(1, 23)
        )
        if _all_exist:
            COMPLETED_CELL_TYPES.append(_ct)
            print(f"  {_ct}: complete")
        else:
            _missing = [c for c in range(1, 23) if not os.path.exists(f"data/ldscores/{_ct}/{_ct}.{c}.l2.ldscore.gz")]
            print(f"  {_ct}: INCOMPLETE (missing chromosomes: {_missing})")

    with open(cts_path, "w") as _f:
        for _ct in COMPLETED_CELL_TYPES:
            _f.write(f"{_ct}\tdata/ldscores/{_ct}/{_ct}.\n")

    print(f"\nCTS file written with {len(COMPLETED_CELL_TYPES)} complete cell types: {cts_path}")
    return (cts_path, COMPLETED_CELL_TYPES)


@app.cell
def __(mo):
    mo.md("## 8. Run LDSC cell-type–specific heritability analysis (CTS)")
    return


@app.cell
def __(cts_path, python27_path, SUMSTATS_FILE, RESULTS_PREFIX, os, subprocess):

    _final_output = f"{RESULTS_PREFIX}.cell_type_results"

    if not os.path.exists(SUMSTATS_FILE):
        print(f"Skipping - sumstats file not found: {SUMSTATS_FILE}")

    elif not os.path.exists(cts_path):
        print(f"Skipping - CTS file not found: {cts_path}")

    elif os.path.exists(_final_output):
        print(f"Results already exist, skipping: {_final_output}")

    else:
        os.makedirs("results", exist_ok=True)
        print("Running LDSC cell-type–specific heritability analysis...")

        subprocess.run([
            python27_path, "tools/ldsc/ldsc.py",
            "--h2-cts",         SUMSTATS_FILE,
            "--ref-ld-chr",     "data/reference/baselineLD_v2.2/baselineLD.",
            "--ref-ld-chr-cts", cts_path,
            "--w-ld-chr",       "data/reference/GRCh38/weights/weights.hm3_noMHC.",
            "--out",            RESULTS_PREFIX,
        ], check=True)

        print("LDSC CTS analysis completed")

    return


@app.cell
def __(RESULTS_PREFIX, pd, os):
    results_file = f"{RESULTS_PREFIX}.cell_type_results.txt"

    if not os.path.exists(results_file):
        print(f"Results file not found: {results_file}")
        ranked = None
    else:
        results = pd.read_csv(results_file, sep="\t")

        ranked = results.sort_values("Coefficient_P_value")

        ranked_csv = f"{RESULTS_PREFIX}_ranked_by_pvalue.csv"
        ranked_txt = f"{RESULTS_PREFIX}_ranked_by_pvalue.txt"

        ranked.to_csv(ranked_csv, index=False)
        ranked.to_csv(ranked_txt, sep="\t", index=False)

        print("\nTop enriched cell types:\n")
        print(
            ranked[
                ["Name", "Coefficient", "Coefficient_std_error", "Coefficient_P_value"]
            ].head(10).to_string(index=False)
        )

        print(f"\nRanked CSV saved to: {ranked_csv}")
        print(f"Ranked TXT saved to: {ranked_txt}")
    return (ranked,)


if __name__ == "__main__":
    app.run()