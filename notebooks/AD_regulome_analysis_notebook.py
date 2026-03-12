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
    GWAS_INPUT_FILE = "data/gwas/AD_bellenguez_2022_hg38.tsv.gz"
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
## 0. Setup: Download and configure LDSC
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
    mo.md("## 1. Download cell-type peak annotations and reference files")
    return


@app.cell
def __(os, urllib, pd):
    os.makedirs("data/peaks", exist_ok=True)
    os.makedirs("data/reference", exist_ok=True)
    os.makedirs("data/gwas", exist_ok=True)
    os.makedirs("data/beds", exist_ok=True)

    BROAD_CELL_TYPES = ["Ast", "Ex", "In", "Microglia", "OPC", "Oligo", "PerEndo"]
    BROAD_BASE_URL = "https://personal.broadinstitute.org/bjames/AD_snATAC/major_celltype_matrices/"

    for _ct in BROAD_CELL_TYPES:
        peak_txt = f"data/peaks/{_ct}.peak.annotation.txt"
        bed_out  = f"data/beds/{_ct}.bed"

        if not os.path.exists(peak_txt):
            print(f"  Downloading peak annotation for {_ct}...")
            urllib.request.urlretrieve(f"{BROAD_BASE_URL}{_ct}.peak.annotation.txt", peak_txt)

        if not os.path.exists(bed_out):
            peaks = pd.read_csv(peak_txt, sep="\t")
            _bed = peaks[['seqnames', 'start', 'end']].copy()
            _bed.columns = ['chr', 'start', 'end']
            _bed.to_csv(bed_out, sep="\t", index=False, header=False)
            print(f"  Created BED for {_ct}")

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

    cell_type_beds = {ct: f"data/beds/{ct}.bed" for ct in BROAD_CELL_TYPES}
    all_cell_types = BROAD_CELL_TYPES

    print(f"\n{len(all_cell_types)} cell types ready: {', '.join(all_cell_types)}")
    print("HapMap3 SNP list ready: data/reference/hm3_no_MHC.list.txt")

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
        check_path = os.path.join(extract_to, check_file)
        if os.path.exists(check_path):
            continue
        if os.path.exists(tar_file):
            subprocess.run(
                ["tar", "-xzf", tar_file, "-C", extract_to],
                check=True
            )

    critical_file = "data/reference/GRCh38/plink_files/1000G.EUR.hg38.1.bim"
    if os.path.exists(critical_file):
        print(f"All reference files ready!")
    else:
        print(f"ERROR: Critical file missing: {critical_file}")

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
    os.makedirs("data/ldscores", exist_ok=True)

    HM3_SNP_LIST = "data/reference/hm3_no_MHC.list.txt"

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
                "--bfile",       f"data/reference/GRCh38/plink_files/1000G.EUR.hg38.{_chrom}",
                "--ld-wind-cm",  "1.0",
                "--annot",       f"data/annotations/{_ct}.{_chrom}.annot.gz",
                "--thin-annot",
                "--print-snps",  HM3_SNP_LIST,
                "--out",         f"data/ldscores/{_ct}/{_ct}.{_chrom}"
            ], check=True, capture_output=True)
            print("done")
        print(f"  {_ct} complete")

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
def __(python27_path, SUMSTATS_FILE, GWAS_STEM, RESULTS_PREFIX, cts_path, os, pd, datetime):
    if SUMSTATS_FILE is None or not os.path.exists(SUMSTATS_FILE):
        print(f"Skipping LDSC analysis - sumstats file not found: {SUMSTATS_FILE}")
    else:
        os.makedirs("results", exist_ok=True)
        print(f"Started: {datetime.now().strftime('%H:%M:%S')}")

        import subprocess as _sp
        _r = _sp.run([
            python27_path, "tools/ldsc/ldsc.py",
            "--h2-cts",         SUMSTATS_FILE,
            "--ref-ld-chr",     "data/reference/baselineLD_v2.2/baselineLD.",
            "--ref-ld-chr-cts", cts_path,
            "--w-ld-chr",       "data/reference/GRCh38/weights/weights.hm3_noMHC.",
            "--out",            RESULTS_PREFIX,
        ], capture_output=True, text=True)

        print(f"Finished: {datetime.now().strftime('%H:%M:%S')}")

        if _r.returncode != 0:
            print("LDSC stderr:", _r.stderr[-1000:])
            raise RuntimeError("LDSC CTS analysis failed")

        _results_file = f"{RESULTS_PREFIX}.cell_type_results.txt"
        _ranked = pd.read_csv(results_file, sep="\t").sort_values("Coefficient_P_value")
        _ranked.to_csv(f"{RESULTS_PREFIX}_ranked.csv", index=False)
        print(_ranked[["Name", "Coefficient", "Coefficient_std_error", "Coefficient_P_value"]].to_string(index=False))

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