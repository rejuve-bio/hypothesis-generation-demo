import marimo

__generated_with = "0.9.14"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    import urllib.request
    import os
    import subprocess
    import pandas as pd
    import tarfile
    from pathlib import Path
    return mo, urllib, os, subprocess, pd, tarfile, Path


@app.cell
def __(mo):
    mo.md("""
# LDSC Cell-Type Analysis – Alzheimer's Disease

This notebook reproduces the LDSC cell-type–specific heritability analysis  
from *Epigenomic dissection of Alzheimer's disease pinpoints causal variants and reveals epigenome erosion*.

All analyses are performed using **GRCh38 / hg38** coordinates.
""")
    return


@app.cell
def __(mo):
    mo.md("""
## 0. Setup: Download and configure LDSC

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
        subprocess.run([
            "conda", "create", "-n", "ldsc27", 
            "python=2.7", "-y"
        ], check=True)
    

    conda_prefix = subprocess.run(
        ["conda", "env", "list", "--json"],
        capture_output=True,
        text=True,
        check=True
    )
    import json
    envs = json.loads(conda_prefix.stdout)["envs"]
    ldsc27_path = [e for e in envs if "ldsc27" in e][0]
    
    bedtools_check = subprocess.run(
        [os.path.join(ldsc27_path, "bin", "bedtools"), "--version"],
        capture_output=True,
        text=True
    )
    
    if bedtools_check.returncode != 0:
        print("Installing BEDTools in ldsc27 environment...")
        subprocess.run([
            "conda", "install", "-n", "ldsc27",
            "-c", "bioconda", "bedtools", "-y"
        ], check=True)
    else:
        print(f"✓ BEDTools already installed: {bedtools_check.stdout.strip()}")
    
  
    if not LDSC_DIR.exists():
        print("Cloning LDSC repository...")
        subprocess.run([
            "git", "clone",
            "https://github.com/bulik/ldsc.git",
            str(LDSC_DIR)
        ], check=True)
    
    check_numpy = subprocess.run(
        [os.path.join(ldsc27_path, "bin", "python"), "-c", "import numpy"],
        capture_output=True
    )
    
    if check_numpy.returncode != 0:
        print("Installing LDSC dependencies in ldsc27 environment...")
        
        # Install OpenSSL 1.0 FIRST
        print("Installing OpenSSL 1.0...")
        subprocess.run([
            "conda", "install", "-n", "ldsc27", "-y",
            "openssl=1.0.2", "-c", "conda-forge"
        ], check=True)
        
        # Install core dependencies
        subprocess.run([
            "conda", "install", "-n", "ldsc27", "-y",
            "numpy", "scipy", "pandas", "bitarray", "-c", "conda-forge"
        ], check=True)
        
        # Install pybedtools with pysam
        print("Installing pybedtools and pysam...")
        subprocess.run([
            "conda", "install", "-n", "ldsc27", "-y",
            "pybedtools", "pysam=0.15.3", "-c", "bioconda", "-c", "conda-forge"
        ], check=True)
        
        print("✓ Dependencies installed!")
    
    # Verify pybedtools can find bedtools
    print("\nVerifying BEDTools is accessible to pybedtools...")
    pybedtools_check = subprocess.run(
        [os.path.join(ldsc27_path, "bin", "python"), "-c", 
         "from pybedtools import BedTool; import pybedtools.helpers as helpers; print('BEDTools path:', helpers.get_bedtools_path())"],
        capture_output=True,
        text=True
    )
    
    if pybedtools_check.returncode != 0:
        print("WARNING: pybedtools can't find BEDTools!")
        print("Error:", pybedtools_check.stderr)
    else:
        print("✓ pybedtools can access BEDTools")
        print(pybedtools_check.stdout)
    
    ldsc_script = LDSC_DIR / "ldsc.py"
    subprocess.run(["chmod", "+x", str(ldsc_script)], check=True)
    
    python27_path = os.path.join(ldsc27_path, "bin", "python")
    
    print(f"\n✓ LDSC environment ready!")
    print(f"Python 2.7 path: {python27_path}")
    print(f"LDSC path: {ldsc_script}")
    
    return (ldsc_script, python27_path, ldsc27_path)


@app.cell
def __(mo):
    mo.md("## 1. Download cell-type peak annotations, reference panels, and GWAS summary statistics")
    return


@app.cell
def __(os, urllib):
    os.makedirs("data/peaks", exist_ok=True)
    os.makedirs("data/reference", exist_ok=True)
    os.makedirs("data/gwas", exist_ok=True)

    cell_types = ["Ast", "Ex", "In", "Microglia", "OPC", "Oligo", "PerEndo"]
    base_url = "https://personal.broadinstitute.org/bjames/AD_snATAC/major_celltype_matrices/"

    print("Downloading cell type peak annotations...")
    for _ct in cell_types:
        url = f"{base_url}{_ct}.peak.annotation.txt"
        out = f"data/peaks/{_ct}.peak.annotation.txt"
        if not os.path.exists(out):
            print(f"  Downloading {_ct}...")
            urllib.request.urlretrieve(url, out)
        else:
            print(f"  ✓ {_ct} already downloaded")

    if not os.path.exists("data/reference/GRCh38.tgz"):
        print("\nDownloading GRCh38 reference with baseline LD scores (this may take several minutes)...")
        urllib.request.urlretrieve(
            "https://zenodo.org/records/10515792/files/GRCh38.tgz?download=1",
            "data/reference/GRCh38.tgz"
        )
        print("✓ GRCh38 reference downloaded")
    else:
        print("\n✓ GRCh38 reference already downloaded")

    if not os.path.exists("data/gwas/AD_bellenguez_2022_hg38.tsv.gz"):
        print("Downloading Alzheimer's GWAS summary statistics (this may take several minutes)...")
        urllib.request.urlretrieve(
            "http://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/GCST90027001-GCST90028000/GCST90027158/GCST90027158_buildGRCh38.tsv.gz",
            "data/gwas/AD_bellenguez_2022_hg38.tsv.gz"
        )
        print("✓ GWAS data downloaded")
    else:
        print("✓ GWAS data already downloaded")

    print("\n✓ All downloads complete!")
    return (cell_types,)


@app.cell
def __(mo):
    mo.md("## 2. Extract reference LD panels and baseline LD scores")
    return


@app.cell  
def __(tarfile, os):
    """First check what's actually in the GRCh38.tgz archive"""
    print("Checking GRCh38.tgz contents...")
    
    if os.path.exists("data/reference/GRCh38.tgz"):
        with tarfile.open("data/reference/GRCh38.tgz", "r:gz") as tar:
            members = tar.getmembers()
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
            with tarfile.open("data/reference/GRCh38.tgz", "r:gz") as tar:
                tar.getmembers()
            print("  ✓ File integrity verified")
        except (EOFError, tarfile.ReadError) as e:
            print(f"\n  ✗ Error: GRCh38.tgz is corrupted!")
            print(f"  Please delete it and re-run: rm data/reference/GRCh38.tgz")
            raise
        
        print("  Extracting (this may take a few minutes)...")
        with tarfile.open("data/reference/GRCh38.tgz", "r:gz") as tar:
            tar.extractall("data/reference")
        print("✓ GRCh38 reference extracted successfully")
    else:
        print("✓ GRCh38 directory exists")
    

    nested_files = [
        ("data/reference/GRCh38/baselineLD_v2.2.tgz", "data/reference", "baselineLD_v2.2"),
        ("data/reference/GRCh38/plink_files.tgz", "data/reference/GRCh38", "1000G.EUR.hg38.1.bed"),
        ("data/reference/GRCh38/weights.tgz", "data/reference/GRCh38", "weights")
    ]
    
    for tar_file, extract_to, check_file in nested_files:
        check_path = os.path.join(extract_to, check_file)
        if os.path.exists(check_path):
            print(f"  ✓ {os.path.basename(tar_file)} already extracted")
            continue
            
        if os.path.exists(tar_file):
            print(f"  Extracting {os.path.basename(tar_file)}...")
            with tarfile.open(tar_file, "r:gz") as tar:
                tar.extractall(extract_to)
            print(f"  ✓ {os.path.basename(tar_file)} extracted")
            
          
            if not os.path.exists(check_path):
                print(f" Warning: Expected file {check_path} not found after extraction")
        else:
            print(f"  Warning: {tar_file} not found")
    
    
    critical_file = "data/reference/GRCh38/1000G.EUR.hg38.1.bim"
    if os.path.exists(critical_file):
        print(f"\n✓ All reference files ready! Verified: {critical_file}")
    else:
        print(f"\n✗ ERROR: Critical file missing: {critical_file}")
        print("Checking what files exist in data/reference/GRCh38/:")
        if os.path.exists("data/reference/GRCh38"):
            files = os.listdir("data/reference/GRCh38")
            print(f"  Found {len(files)} files/directories")
            for f in sorted(files)[:10]:  
                print(f"    - {f}")
        else:
            print("  Directory doesn't exist!")
    
    return


@app.cell
def __(mo):
    mo.md("## 3. Munge Alzheimer's disease GWAS summary statistics")
    return


@app.cell
def __(subprocess, os, python27_path):
    os.makedirs("data/munged", exist_ok=True)

    munged_file = "data/munged/AD_bellenguez_2022_hg38_munged.sumstats.gz"
    
    if os.path.exists(munged_file):
        print("✓ Munged GWAS file already exists, skipping munging step")
    else:
        print("\n" + "="*60)
        print("STEP 3: Munging GWAS summary statistics")
        print("="*60)
        subprocess.run([
            python27_path, "tools/ldsc/munge_sumstats.py",
            "--sumstats", "data/gwas/AD_bellenguez_2022_hg38.tsv.gz",
            "--out", "data/munged/AD_bellenguez_2022_hg38_munged",
            "--a1", "effect_allele",
            "--a2", "other_allele",
            "--p", "p_value",
            "--snp", "variant_id",
            "--N-col", "n_total"
        ], check=True)
        
        print("\n✓ GWAS munging complete")
    return


@app.cell
def __(mo):
    mo.md("## 4. Generate cell-type–specific binary annotations (BED → .annot.gz)")
    return


@app.cell
def __(pd, subprocess, os, cell_types, python27_path, ldsc27_path):
    os.makedirs("data/annotations", exist_ok=True)

    print("\n" + "="*60)
    print("STEP 4: Generating cell-type annotations")
    print("="*60)
    

    env = os.environ.copy()
    env["PATH"] = f"{ldsc27_path}/bin:" + env.get("PATH", "")
    
    for _ct in cell_types:
        print(f"\nProcessing {_ct}...")
        _all_exist = all(
            os.path.exists(f"data/annotations/{_ct}.{_chrom}.annot.gz") 
            for _chrom in range(1, 23)
        )
        
        if _all_exist:
            print(f"  ✓ All {_ct} annotations already exist, skipping")
            continue
        
        # Create BED file
        peaks = pd.read_csv(f"data/peaks/{_ct}.peak.annotation.txt", sep="\t")
        _bed = peaks[['seqnames', 'start', 'end']].copy()
        _bed.columns = ['chr', 'start', 'end']
        _bed_file = f"data/annotations/{_ct}.bed"
        _bed.to_csv(_bed_file, sep="\t", index=False, header=False)
        
        # Generate annotations for each chromosome
        for _chrom in range(1, 23):
            annot_file = f"data/annotations/{_ct}.{_chrom}.annot.gz"
            if os.path.exists(annot_file):
                print(f"  Chromosome {_chrom} ✓ (exists)", end=" ", flush=True)
                continue
                
            print(f"  Chromosome {_chrom}...", end=" ", flush=True)
            result = subprocess.run([
                python27_path, "tools/ldsc/make_annot.py",
                "--bed-file", _bed_file,
                "--bimfile", f"data/reference/GRCh38/plink_files/1000G.EUR.hg38.{_chrom}.bim",
                "--annot-file", annot_file
            ], capture_output=True, text=True, env=env)
            
            if result.returncode != 0:
                print(f"\n\nERROR on chromosome {_chrom}:")
                print("STDERR:", result.stderr)
                print("STDOUT:", result.stdout)
                raise subprocess.CalledProcessError(result.returncode, result.args)
            print("✓")
        print(f"  ✓ {_ct} complete")
    
    print("\n✓ All annotations generated")
    return


@app.cell
def __(mo):
    mo.md("## 5. Calculate LD scores for each cell type and chromosome")
    return


@app.cell
def __(subprocess, os, cell_types, python27_path):
    os.makedirs("data/ldscores", exist_ok=True)

    print("\n" + "="*60)
    print("STEP 5: Calculating LD scores (this may take 10-30 minutes)")
    print("="*60)
    
    for _ct in cell_types:
        print(f"\nProcessing {_ct}...")
        os.makedirs(f"data/ldscores/{_ct}", exist_ok=True)
    
        _all_exist = all(
            os.path.exists(f"data/ldscores/{_ct}/{_ct}.{_chrom}.l2.ldscore.gz")
            for _chrom in range(1, 23)
        )
        
        if _all_exist:
            print(f"  ✓ All {_ct} LD scores already exist, skipping")
            continue
        
        for _chrom in range(1, 23):
            ldscore_file = f"data/ldscores/{_ct}/{_ct}.{_chrom}.l2.ldscore.gz"
            if os.path.exists(ldscore_file):
                print(f"  Chromosome {_chrom} ✓ (exists)", end=" ", flush=True)
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
            print("✓")
        print(f"  ✓ {_ct} complete")
    
    print("\n✓ All LD scores calculated")
    return


@app.cell
def __(mo):
    mo.md("## 6. Create CTS (cell-type–specific) reference file")
    return


@app.cell
def __(os, cell_types):
    os.makedirs("results", exist_ok=True)
    with open("data/cell_types.cts", "w") as f:
        for _ct in cell_types:
            f.write(f"{_ct}    data/ldscores/{_ct}/{_ct}.\n")
    print("✓ CTS reference file created")
    return


@app.cell
def __(mo):
    mo.md("## 7. Run LDSC cell-type–specific heritability analysis (CTS)")
    return


@app.cell
def __(subprocess, python27_path):
    print("\n" + "="*60)
    print("STEP 7: Running cell-type-specific heritability analysis")
    print("="*60 + "\n")
    
    subprocess.run([
        python27_path, "tools/ldsc/ldsc.py",
        "--h2-cts", "data/munged/AD_bellenguez_2022_hg38_munged.sumstats.gz",
        "--ref-ld-chr", "data/reference/baselineLD_v2.2/baselineLD.",
        "--ref-ld-chr-cts", "data/cell_types.cts",
        "--w-ld-chr", "data/reference/GRCh38/weights/weights.hm3_noMHC.",
        "--out", "results/AD_CellTypeSpecific"
    ], check=True)
    
    print("\n✓ Cell-type-specific analysis complete")
    return


@app.cell
def __(mo):
    mo.md("## 8. Rank cell types by heritability enrichment significance")
    return


@app.cell
def __(pd):
    print("\n" + "="*60)
    print("STEP 8: Ranking cell types by significance")
    print("="*60 + "\n")
    
    results = pd.read_csv(
        "results/AD_CellTypeSpecific.cell_type_results.txt", sep="\t"
    )
    ranked = results.sort_values("Coefficient_P_value")
    ranked.to_csv("results/AD_CellTypeSpecific_ranked.csv", index=False)
    
    print("Cell types ranked by heritability enrichment p-value:\n")
    print(ranked[["Name", "Coefficient", "Coefficient_std_error", "Coefficient_P_value"]].to_string(index=False))
    
    print("\n✓ Results saved to results/AD_CellTypeSpecific_ranked.csv")
    return (ranked,)


if __name__ == "__main__":
    app.run()