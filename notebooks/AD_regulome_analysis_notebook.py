# -*- coding: utf-8 -*-
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
    import json
    import glob
    import concurrent.futures
    import multiprocessing
    return mo, urllib, os, re, subprocess, pd, np, Path, json, glob, concurrent, multiprocessing


@app.cell
def __(mo):
    mo.md("""
    This notebook reproduces the LDSC cell-type-specific heritability analysis
    from *A single-cell atlas of chromatin accessibility in the human genome* (Zhang et al. 2021).

    All analyses are performed using **GRCh38 / hg38** coordinates.

    Set `GWAS_INPUT_FILE` below to point to your GWAS summary statistics file.
    The pipeline will auto-detect column names and derive a stem from the filename.
    """)
    return


@app.cell
def __(mo, os):
    GWAS_INPUT_FILE = mo.ui.text(
        value="data/gwas/mdd_symptoms_2023-Clin-MDD1_depressed.txt.gz",
        label="GWAS input file path",
        full_width=True,
    )
    W_HM3_SNPLIST   = "/mnt/hdd_1/rediet/hypothesis-generation-demo/ldsc/data/w_hm3.snplist"
    HM3_NO_MHC_LIST = "data/reference/hm3_no_MHC.list.txt"
    CATLAS_DIR       = "humanenhancer_atac_data"
    CATLAS_URL       = "http://catlas.org/humanenhancer/data/cCREs/"
    BROAD_CELL_TYPES = ["Ast", "Ex", "In", "Microglia", "OPC", "Oligo", "PerEndo"]
    BROAD_BASE_URL   = "https://personal.broadinstitute.org/bjames/AD_snATAC/major_celltype_matrices/"

    mo.vstack([
        mo.md("### Configuration"),
        GWAS_INPUT_FILE,
        mo.md(f"w_hm3.snplist: `{W_HM3_SNPLIST}`"),
    ])
    return (
        GWAS_INPUT_FILE,
        W_HM3_SNPLIST,
        HM3_NO_MHC_LIST,
        CATLAS_DIR,
        CATLAS_URL,
        BROAD_CELL_TYPES,
        BROAD_BASE_URL,
    )


@app.cell
def __(GWAS_INPUT_FILE, os, re):
    _path = GWAS_INPUT_FILE.value
    _basename = os.path.basename(_path)
    _no_ext = _basename
    for _ext in [".tsv.gz", ".txt.gz", ".gz", ".tsv", ".txt", ".csv", ".bgz"]:
        if _no_ext.endswith(_ext):
            _no_ext = _no_ext[: -len(_ext)]
            break
    GWAS_STEM      = re.sub(r"[^A-Za-z0-9_\-]", "_", _no_ext)
    GWAS_FILE      = _path
    PREMUNGE_FILE  = f"data/munging_input/{GWAS_STEM}_premunge.tsv.gz"
    SUMSTATS_FILE  = f"data/ldsc_input/{GWAS_STEM}.sumstats.gz"
    MUNGE_PREFIX   = f"data/ldsc_input/{GWAS_STEM}"
    CTS_FILE       = f"data/{GWAS_STEM}_cell_types.cts"
    RESULTS_PREFIX = f"results/{GWAS_STEM}_CellTypeSpecific"

    print(f"GWAS stem      : {GWAS_STEM}")
    print(f"GWAS file      : {GWAS_FILE}")
    print(f"Premunge file  : {PREMUNGE_FILE}")
    print(f"Sumstats file  : {SUMSTATS_FILE}")
    print(f"Results prefix : {RESULTS_PREFIX}")
    return (
        GWAS_STEM,
        GWAS_FILE,
        PREMUNGE_FILE,
        SUMSTATS_FILE,
        MUNGE_PREFIX,
        CTS_FILE,
        RESULTS_PREFIX,
    )


@app.cell
def __(mo):
    mo.md("## 0. Setup: download and configure LDSC")
    return


@app.cell
def __(Path, subprocess, os, json):
    TOOLS_DIR = Path("tools")
    LDSC_DIR  = TOOLS_DIR / "ldsc"
    TOOLS_DIR.mkdir(exist_ok=True)

    _env_check = subprocess.run(["conda", "env", "list"], capture_output=True, text=True)
    if "ldsc27" not in _env_check.stdout:
        subprocess.run(["conda", "create", "-n", "ldsc27", "python=2.7", "-y"], check=True)

    _conda_json = subprocess.run(
        ["conda", "env", "list", "--json"], capture_output=True, text=True, check=True
    )
    _envs = json.loads(_conda_json.stdout)["envs"]
    ldsc27_path = [e for e in _envs if "ldsc27" in e][0]

    if not os.path.exists(os.path.join(ldsc27_path, "bin", "bedtools")):
        subprocess.run(
            ["conda", "install", "-n", "ldsc27", "-c", "bioconda", "bedtools", "-y"],
            check=True,
        )

    if not LDSC_DIR.exists():
        subprocess.run(
            ["git", "clone", "https://github.com/bulik/ldsc.git", str(LDSC_DIR)],
            check=True,
        )

    _check_np = subprocess.run(
        [os.path.join(ldsc27_path, "bin", "python"), "-c", "import numpy"],
        capture_output=True,
    )
    if _check_np.returncode != 0:
        subprocess.run(
            ["conda", "install", "-n", "ldsc27", "-y", "openssl=1.0.2", "-c", "conda-forge"],
            check=True,
        )
        subprocess.run(
            ["conda", "install", "-n", "ldsc27", "-y",
             "numpy", "scipy", "pandas", "bitarray", "-c", "conda-forge"],
            check=True,
        )
        subprocess.run(
            ["conda", "install", "-n", "ldsc27", "-y",
             "pybedtools", "pysam=0.15.3", "-c", "bioconda", "-c", "conda-forge"],
            check=True,
        )

    subprocess.run(["chmod", "+x", str(LDSC_DIR / "ldsc.py")], check=True)
    python27_path = os.path.join(ldsc27_path, "bin", "python")

    print(f"LDSC environment ready")
    print(f"Python 2.7 : {python27_path}")
    print(f"LDSC dir   : {LDSC_DIR}")
    return (TOOLS_DIR, LDSC_DIR, ldsc27_path, python27_path)


@app.cell
def __(mo):
    mo.md("## 1. Discover cell types and resolve BED sources")
    return


@app.cell
def __(
    os, urllib, pd, subprocess, re,
    CATLAS_DIR, CATLAS_URL, BROAD_CELL_TYPES, BROAD_BASE_URL,
    GWAS_FILE,
):
    for _d in ["data/peaks", "data/reference", "data/gwas", "data/beds"]:
        os.makedirs(_d, exist_ok=True)

    BED_SEARCH_DIRS = [CATLAS_DIR, "data/beds"]

    def _sanitize(name):
        return re.sub(r"[^A-Za-z0-9_\-]", "_", name)

    def _ensure_catlas():
        if os.path.exists(CATLAS_DIR):
            _beds = [f for f in os.listdir(CATLAS_DIR) if f.endswith(".bed")]
            if _beds:
                print(f"{CATLAS_DIR} has {len(_beds)} BED files, skipping download")
                return
        os.makedirs(CATLAS_DIR, exist_ok=True)
        subprocess.run(
            ["wget", "-r", "-np", "-nH", "--cut-dirs=3",
             "-R", "index.html*", "-P", CATLAS_DIR, CATLAS_URL],
            check=True,
        )
        print(f"Downloaded {len([f for f in os.listdir(CATLAS_DIR) if f.endswith('.bed')])} BED files")

    def _find_local_bed(name):
        for d in BED_SEARCH_DIRS:
            p = os.path.join(d, f"{name}.bed")
            if os.path.exists(p):
                return p
        return None

    def _peak_to_bed(ct):
        peak_txt = f"data/peaks/{ct}.peak.annotation.txt"
        bed_out  = f"data/beds/{ct}.bed"
        if os.path.exists(bed_out):
            return bed_out
        if not os.path.exists(peak_txt):
            urllib.request.urlretrieve(f"{BROAD_BASE_URL}{ct}.peak.annotation.txt", peak_txt)
        _peaks = pd.read_csv(peak_txt, sep="\t")
        _peaks[["seqnames", "start", "end"]].rename(
            columns={"seqnames": "chr"}
        ).to_csv(bed_out, sep="\t", index=False, header=False)
        return bed_out

    _ensure_catlas()

    raw_names = set()
    for _sd in BED_SEARCH_DIRS:
        if os.path.exists(_sd):
            for _f in os.listdir(_sd):
                if _f.endswith(".bed") and not _f.startswith("."):
                    raw_names.add(os.path.splitext(_f)[0])
    raw_names.update(BROAD_CELL_TYPES)

    cell_type_beds = {}
    _seen = {}
    for _raw in sorted(raw_names):
        _safe = _sanitize(_raw)
        if _safe in _seen:
            _safe += "_2"
        _seen[_safe] = _raw
        _local = _find_local_bed(_raw)
        cell_type_beds[_safe] = _local if _local else _peak_to_bed(_raw)

    all_cell_types = sorted(cell_type_beds.keys())
    print(f"{len(all_cell_types)} cell types ready")

    for _url, _dst in [
        (
            "https://zenodo.org/records/10515792/files/GRCh38.tgz?download=1",
            "data/reference/GRCh38.tgz",
        ),
        (
            "https://zenodo.org/records/10515792/files/hm3_no_MHC.list.txt?download=1",
            "data/reference/hm3_no_MHC.list.txt",
        ),
    ]:
        if not os.path.exists(_dst):
            print(f"Downloading {_dst}...")
            urllib.request.urlretrieve(_url, _dst)

    print("All sources resolved")
    return (all_cell_types, cell_type_beds, BED_SEARCH_DIRS)


@app.cell
def __(mo):
    mo.md("## 2. Extract reference LD panels")
    return


@app.cell
def __(subprocess, os):
    if not os.path.exists("data/reference/GRCh38"):
        subprocess.run(
            ["tar", "-xzf", "data/reference/GRCh38.tgz", "-C", "data/reference"],
            check=True,
        )

    for _tgz, _base, _check in [
        (
            "data/reference/GRCh38/plink_files.tgz",
            "data/reference/GRCh38",
            "plink_files/1000G.EUR.hg38.1.bim",
        ),
        (
            "data/reference/GRCh38/weights.tgz",
            "data/reference/GRCh38",
            "weights",
        ),
    ]:
        if not os.path.exists(os.path.join(_base, _check)) and os.path.exists(_tgz):
            subprocess.run(["tar", "-xzf", _tgz, "-C", _base], check=True)

    _critical = "data/reference/GRCh38/plink_files/1000G.EUR.hg38.1.bim"
    print("Reference files ready" if os.path.exists(_critical) else f"ERROR: missing {_critical}")
    return


@app.cell
def __(mo):
    mo.md("## 3. Munge GWAS summary statistics")
    return


@app.cell
def __(
    GWAS_FILE, PREMUNGE_FILE, SUMSTATS_FILE, MUNGE_PREFIX,
    W_HM3_SNPLIST, python27_path, pd, os, subprocess, glob,
):
    import gzip as _gzip

    os.makedirs("data/ldsc_input", exist_ok=True)
    os.makedirs("data/munging_input", exist_ok=True)

    if os.path.exists(SUMSTATS_FILE):
        print(f"Munged sumstats already exists, skipping: {SUMSTATS_FILE}")
    else:
        if not os.path.exists(PREMUNGE_FILE):
            _skip = 0
            with _gzip.open(GWAS_FILE, "rt") as _fh:
                for _line in _fh:
                    if _line.startswith("##"):
                        _skip += 1
                    else:
                        break

            _df = pd.read_csv(GWAS_FILE, sep="\t", compression="gzip", skiprows=_skip)
            _df.columns = [c.lstrip("#").strip() for c in _df.columns]
            print(f"  Columns: {list(_df.columns)}")

            _cl = {c.lower(): c for c in _df.columns}

            _rsid_col  = _cl.get("id") or _cl.get("snp") or _cl.get("rsid") or _cl.get("variant_id")
            _a1_col    = _cl.get("a1") or _cl.get("alt") or _cl.get("effect_allele")
            _a2_col    = _cl.get("a2") or _cl.get("ref") or _cl.get("reference") or _cl.get("other_allele")
            _beta_col  = _cl.get("beta") or _cl.get("b") or _cl.get("logor")
            _se_col    = _cl.get("se") or _cl.get("stderr") or _cl.get("standard_error")
            _p_col     = _cl.get("pval") or _cl.get("pvalue") or _cl.get("p_value") or _cl.get("p")
            _n_col     = _cl.get("neff") or _cl.get("n") or _cl.get("n_total") or _cl.get("sample_size")

            _df = _df.rename(columns={
                _a1_col: "A1", _a2_col: "A2", _beta_col: "BETA",
                _se_col: "SE", _p_col: "P", _n_col: "N",
            })

            if _rsid_col and _df[_rsid_col].astype(str).str.match(r"^rs\d+$", na=False).sum() > 0:
                _df["SNP"] = _df[_rsid_col]
            else:
                _bim_map = pd.concat([
                    pd.read_csv(f, sep="\t", header=None, names=["CHR","SNP","CM","BP","A1b","A2b"])
                    .assign(key=lambda x: x["CHR"].astype(str) + ":" + x["BP"].astype(str))[["key","SNP"]]
                    for f in sorted(glob.glob("data/reference/GRCh38/plink_files/1000G.EUR.hg38.*.bim"))
                ]).drop_duplicates("key").set_index("key")["SNP"]
                _varid = _cl.get("varid") or _cl.get("variant")
                _df["key"] = _df[_varid].str.split(":").str[:2].str.join(":") if _varid else (
                    _df[_cl.get("chrom") or _cl.get("chr")].astype(str).str.replace("chr","",regex=False)
                    + ":" + _df[_cl.get("pos") or _cl.get("position")].astype(str)
                )
                _df["SNP"] = _df["key"].map(_bim_map)
                _df = _df.drop(columns=["key"])

            _keep = [c for c in ["SNP","A1","A2","BETA","SE","P","N"] if c in _df.columns]
            _df[_keep].dropna(subset=["SNP","A1","A2","BETA","P"]).to_csv(
                PREMUNGE_FILE, sep="\t", index=False, compression="gzip"
            )
            print(f"  Written {len(_df):,} variants to {PREMUNGE_FILE}")

        _peek = pd.read_csv(PREMUNGE_FILE, sep="\t", compression="gzip", nrows=2)
        subprocess.run([
            python27_path, "tools/ldsc/munge_sumstats.py",
            "--sumstats",        PREMUNGE_FILE,
            "--out",             MUNGE_PREFIX,
            "--snp",             "SNP",
            "--a1",              "A1",
            "--a2",              "A2",
            "--signed-sumstats", "BETA,0",
            "--p",               "P",
            "--merge-alleles",   W_HM3_SNPLIST,
        ] + (["--N-col", "N"] if "N" in _peek.columns else []), check=True)
        print(f"Munging complete: {SUMSTATS_FILE}")

    return


@app.cell
def __(mo):
    mo.md("## 4. Generate cell-type-specific binary annotations (BED -> .annot.gz)")
    return


@app.cell
def __(subprocess, os, all_cell_types, cell_type_beds, python27_path, ldsc27_path):
    os.makedirs("data/annotations", exist_ok=True)
    _env = os.environ.copy()
    _env["PATH"] = f"{ldsc27_path}/bin:" + _env.get("PATH", "")

    for _ct in all_cell_types:
        _all_exist = all(
            os.path.exists(f"data/annotations/{_ct}.{_ch}.annot.gz")
            for _ch in range(1, 23)
        )
        if _all_exist:
            print(f"  {_ct}: all annotations exist, skipping")
            continue

        print(f"\nProcessing {_ct}...")
        _bed = cell_type_beds[_ct]
        for _ch in range(1, 23):
            _out = f"data/annotations/{_ct}.{_ch}.annot.gz"
            if os.path.exists(_out):
                continue
            _r = subprocess.run(
                [
                    python27_path, "tools/ldsc/make_annot.py",
                    "--bed-file",   _bed,
                    "--bimfile",    f"data/reference/GRCh38/plink_files/1000G.EUR.hg38.{_ch}.bim",
                    "--annot-file", _out,
                ],
                capture_output=True, text=True, env=_env,
            )
            if _r.returncode != 0:
                print(f"  ERROR chr {_ch}: {_r.stderr}")
                raise subprocess.CalledProcessError(_r.returncode, _r.args)
            print(f"  chr{_ch}", end=" ", flush=True)
        print(f"\n  {_ct} done")

    print("\nAll annotations generated")
    return


@app.cell
def __(mo):
    mo.md("## 5. Calculate LD scores (HapMap3 SNPs only)")
    return


@app.cell
def __(subprocess, os, all_cell_types, python27_path, concurrent, multiprocessing, HM3_NO_MHC_LIST):
    os.makedirs("data/ldscores", exist_ok=True)

    def _calc_ld(args):
        ct, ch = args
        _dir  = f"data/ldscores/{ct}"
        os.makedirs(_dir, exist_ok=True)
        _out  = f"{_dir}/{ct}.{ch}.l2.ldscore.gz"
        if os.path.exists(_out):
            return f"[{ct}] chr{ch} exists"
        try:
            subprocess.run(
                [
                    python27_path, "tools/ldsc/ldsc.py",
                    "--l2",
                    "--bfile",      f"data/reference/GRCh38/plink_files/1000G.EUR.hg38.{ch}",
                    "--ld-wind-cm", "1.0",
                    "--annot",      f"data/annotations/{ct}.{ch}.annot.gz",
                    "--thin-annot",
                    "--print-snps", HM3_NO_MHC_LIST,
                    "--out",        f"{_dir}/{ct}.{ch}",
                ],
                check=True, capture_output=True,
            )
            return f"[{ct}] chr{ch} done"
        except subprocess.CalledProcessError as e:
            return f"ERROR [{ct}] chr{ch}: {e.stderr}"

    _tasks      = [(ct, ch) for ct in all_cell_types for ch in range(1, 23)]
    _max_workers = min(multiprocessing.cpu_count() - 1, 6)
    print(f"Running LD score calculation ({_max_workers} workers, {len(_tasks)} tasks)...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=_max_workers) as _ex:
        for _res in _ex.map(_calc_ld, _tasks):
            print(_res)

    print("\nAll LD scores calculated")
    return


@app.cell
def __(mo):
    mo.md("## 6. Create CTS reference file")
    return


@app.cell
def __(os, all_cell_types, GWAS_STEM, CTS_FILE):
    os.makedirs("results", exist_ok=True)

    COMPLETED_CELL_TYPES = []
    for _ct in all_cell_types:
        _complete = all(
            os.path.exists(f"data/ldscores/{_ct}/{_ct}.{_ch}.l2.ldscore.gz")
            for _ch in range(1, 23)
        )
        if _complete:
            COMPLETED_CELL_TYPES.append(_ct)
        else:
            _missing = [c for c in range(1, 23)
                        if not os.path.exists(f"data/ldscores/{_ct}/{_ct}.{c}.l2.ldscore.gz")]
            print(f"  {_ct}: INCOMPLETE — missing chr {_missing}")

    with open(CTS_FILE, "w") as _f:
        for _ct in COMPLETED_CELL_TYPES:
            _f.write(f"{_ct}\tdata/ldscores/{_ct}/{_ct}.\n")

    print(f"\nCTS file written: {CTS_FILE}  ({len(COMPLETED_CELL_TYPES)} cell types)")
    return (COMPLETED_CELL_TYPES,)


@app.cell
def __(mo):
    mo.md("## 7. Run LDSC cell-type-specific heritability analysis")
    return


@app.cell
def __(CTS_FILE, python27_path, SUMSTATS_FILE, RESULTS_PREFIX, os, subprocess):
    if not os.path.exists(SUMSTATS_FILE):
        print(f"Skipping — sumstats not found: {SUMSTATS_FILE}")
    elif not os.path.exists(CTS_FILE):
        print(f"Skipping — CTS file not found: {CTS_FILE}")
    else:
        os.makedirs("results", exist_ok=True)
        print("Running LDSC CTS analysis...")
        subprocess.run(
            [
                python27_path, "tools/ldsc/ldsc.py",
                "--h2-cts",         SUMSTATS_FILE,
                "--ref-ld-chr",     (
                    "data/reference/GRCh38/baselineLD_v2.2/baselineLD.,"
                    "data/ldscores/all_merged_cCREs/all_merged_cCREs."
                ),
                "--ref-ld-chr-cts", CTS_FILE,
                "--w-ld-chr",       "data/reference/GRCh38/weights/weights.hm3_noMHC.",
                "--out",            RESULTS_PREFIX,
            ],
            check=True,
        )
        print("LDSC CTS analysis complete")
    return


@app.cell
def __(mo):
    mo.md("## 8. Results")
    return


@app.cell
def __(RESULTS_PREFIX, pd, os):
    from statsmodels.stats.multitest import fdrcorrection as _fdr

    _results_file = f"{RESULTS_PREFIX}.cell_type_results.txt"

    if not os.path.exists(_results_file):
        print(f"Results file not found: {_results_file}")
        ranked = None
    else:
        _results = pd.read_csv(_results_file, sep="\t")
        _, _results["FDR"] = _fdr(_results["Coefficient_P_value"].fillna(1))
        ranked = _results.sort_values("Coefficient_P_value")

        ranked.to_csv(f"{RESULTS_PREFIX}_ranked.csv", index=False)
        ranked.to_csv(f"{RESULTS_PREFIX}_ranked.txt", sep="\t", index=False)

        print("\nTop enriched cell types:\n")
        print(
            ranked[["Name", "Coefficient", "Coefficient_std_error", "Coefficient_P_value", "FDR"]]
            .head(15)
            .to_string(index=False)
        )
        print(f"\nFDR < 0.05: {(ranked['FDR'] < 0.05).sum()} cell types")
        print(f"Saved to  : {RESULTS_PREFIX}_ranked.csv")

    return (ranked,)


if __name__ == "__main__":
    app.run()