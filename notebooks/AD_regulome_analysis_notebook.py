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
    import gzip
    import hashlib
    from datetime import datetime
    return mo, urllib, os, subprocess, pd, tarfile, Path, gzip, hashlib, datetime


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
    

    bedtools_path = os.path.join(ldsc27_path, "bin", "bedtools")
    if os.path.exists(bedtools_path):
        bedtools_check = subprocess.run(
            [bedtools_path, "--version"],
            capture_output=True,
            text=True
        )
        print(f"✓ BEDTools already installed: {bedtools_check.stdout.strip()}")
    else:
        print("Installing BEDTools in ldsc27 environment...")
        subprocess.run([
            "conda", "install", "-n", "ldsc27",
            "-c", "bioconda", "bedtools", "-y"
        ], check=True)
    
  
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
        
     
        print("Installing OpenSSL 1.0...")
        subprocess.run([
            "conda", "install", "-n", "ldsc27", "-y",
            "openssl=1.0.2", "-c", "conda-forge"
        ], check=True)
        
      
        subprocess.run([
            "conda", "install", "-n", "ldsc27", "-y",
            "numpy", "scipy", "pandas", "bitarray", "-c", "conda-forge"
        ], check=True)
        
       
        print("Installing pybedtools and pysam...")
        subprocess.run([
            "conda", "install", "-n", "ldsc27", "-y",
            "pybedtools", "pysam=0.15.3", "-c", "bioconda", "-c", "conda-forge"
        ], check=True)
        
        print("✓ Dependencies installed!")
    

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

    cell_types = ["Ast"]
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
            print("  ✓ File integrity verified")
        except (EOFError, tarfile.ReadError) as e:
            print(f"\n  ✗ Error: GRCh38.tgz is corrupted!")
            print(f"  Please delete it and re-run: rm data/reference/GRCh38.tgz")
            raise
        
        print("  Extracting (this may take a few minutes)...")
        with tarfile.open("data/reference/GRCh38.tgz", "r:gz") as _tar:
            _tar.extractall("data/reference")
        print("✓ GRCh38 reference extracted successfully")
    else:
        print("✓ GRCh38 directory exists")
    

    nested_files = [
        ("data/reference/GRCh38/baselineLD_v2.2.tgz", "data/reference", "baselineLD_v2.2"),
        ("data/reference/GRCh38/plink_files.tgz", "data/reference/GRCh38", "plink_files/1000G.EUR.hg38.1.bim"),
        ("data/reference/GRCh38/weights.tgz", "data/reference/GRCh38", "weights")
    ]
    
    for tar_file, extract_to, check_file in nested_files:
        check_path = os.path.join(extract_to, check_file)
        if os.path.exists(check_path):
            print(f"  ✓ {os.path.basename(tar_file)} already extracted")
            continue
            
        if os.path.exists(tar_file):
            print(f"  Extracting {os.path.basename(tar_file)}...")
            with tarfile.open(tar_file, "r:gz") as _tar:
                _tar.extractall(extract_to)
            print(f"  ✓ {os.path.basename(tar_file)} extracted")
            
            if not os.path.exists(check_path):
                print(f"  Warning: Expected file {check_path} not found after extraction")
        else:
            print(f"  Warning: {tar_file} not found")
    
    
    critical_file = "data/reference/GRCh38/plink_files/1000G.EUR.hg38.1.bim"
    if os.path.exists(critical_file):
        print(f"\n✓ All reference files ready! Verified: {critical_file}")
    else:
        print(f"\n✗ ERROR: Critical file missing: {critical_file}")
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
    mo.md("""
## 3. Convert GWAS to SSF format (embedded harmonizer logic)

This cell converts the input GWAS file to GWAS-SSF format, filtering out X/Y/MT chromosomes.
This replicates the to_gwas_ssf() function from harmonizer.sh.
""")
    return


@app.cell
def __(pd, os, gzip, subprocess, hashlib, datetime):
    """Convert GWAS to SSF format"""
    
    os.makedirs("data/ssf", exist_ok=True)
    
    input_gwas = "data/gwas/AD_bellenguez_2022_hg38.tsv.gz"
    output_ssf = "data/ssf/AD_bellenguez_2022_hg38.tsv.gz"
    output_yaml = f"{output_ssf}-meta.yaml"
    
    if os.path.exists(output_ssf):
        print("✓ SSF file already exists, skipping conversion")
    else:
        print("\n" + "="*60)
        print("STEP 3: Converting GWAS to SSF format")
        print("="*60)
        
      
        print(f"Reading input file: {input_gwas}")
        _df = pd.read_csv(input_gwas, sep="\t", compression="gzip")
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
        _col_map['standard_error'] = (_cols_lower.get('se') or _cols_lower.get('stderr') or 
                                      _cols_lower.get('standard_error'))
        _col_map['p_value'] = _cols_lower.get('p') or _cols_lower.get('pval') or _cols_lower.get('p_value')
        _col_map['effect_allele_frequency'] = (_cols_lower.get('a1_freq') or _cols_lower.get('frq') or 
                                                _cols_lower.get('af') or _cols_lower.get('effect_allele_frequency'))
        _col_map['rsid'] = (_cols_lower.get('id') or _cols_lower.get('snp') or 
                           _cols_lower.get('rsid') or _cols_lower.get('variant_id'))
        
      
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
        
     
        print(f"Writing SSF file: {output_ssf}")
        _df_ssf.to_csv(output_ssf, sep="\t", index=False, compression="gzip")
        
    
        print("Creating tabix index...")
        subprocess.run([
            "tabix", "-c", "N", "-S", "1", "-s", "1", "-b", "2", "-e", "2", output_ssf
        ], check=False) 
        

        with gzip.open(output_ssf, 'rb') as f:
            _md5 = hashlib.md5(f.read()).hexdigest()
        
        
        with open(output_yaml, 'w') as f:
            f.write(f"""# Study meta-data
date_metadata_last_modified: {datetime.now().strftime('%Y-%m-%d')}

# Genotyping Information
genome_assembly: GRCh38
coordinate_system: 1-based

# Summary Statistic information
data_file_name: {os.path.basename(output_ssf)}
file_type: GWAS-SSF v0.1
data_file_md5sum: {_md5}

# Harmonization status
is_harmonised: false
is_sorted: false
""")
        
        print("✓ SSF conversion complete")
        
       
        chromosomes_present = sorted(_df_ssf['chromosome'].unique(), key=lambda x: int(x))
        print(f"Chromosomes in data: {', '.join(chromosomes_present)}")
    
    return (output_ssf, output_yaml)


@app.cell
def __(mo):
    mo.md("""
## 4. Setup harmonization environment (Nextflow-based)

**Note:** The full harmonization step requires:
- Nextflow installed
- Harmonizer workflow repository
- Reference data

For this analysis, we can skip the Nextflow harmonization and directly convert 
the SSF file to LDSC format since the data is already in GRCh38.
""")
    return


@app.cell
def __(mo):
    mo.md("## 5. Convert SSF to LDSC format")
    return


@app.cell
def __(os, subprocess, python27_path, output_ssf):
    os.makedirs("data/ldsc_input", exist_ok=True)
    
    ldsc_sumstats_file = "data/ldsc_input/AD_harmonized_munged.sumstats.gz"
    
    if os.path.exists(ldsc_sumstats_file):
        print("✓ LDSC-formatted file already exists")
    else:
        print("\n" + "="*60)
        print("STEP 5: Converting SSF data to LDSC format")
        print("="*60)

        subprocess.run([
            python27_path, "tools/ldsc/munge_sumstats.py",
            "--sumstats", output_ssf,
            "--out", "data/ldsc_input/AD_harmonized_munged",
            "--a1", "effect_allele",
            "--a2", "other_allele",
            "--p", "p_value",
            "--snp", "rsid",
            "--signed-sumstats", "beta,0",
            "--N", "487511"  
        ], check=True)
        
        print("\n✓ LDSC format conversion complete")
    
    return (ldsc_sumstats_file,)


@app.cell
def __(mo):
    mo.md("## 6. Generate cell-type–specific binary annotations (BED → .annot.gz)")
    return


@app.cell
def __(pd, subprocess, os, cell_types, python27_path, ldsc27_path):
    os.makedirs("data/annotations", exist_ok=True)

    print("\n" + "="*60)
    print("STEP 6: Generating cell-type annotations")
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
        
 
        peaks = pd.read_csv(f"data/peaks/{_ct}.peak.annotation.txt", sep="\t")
        _bed = peaks[['seqnames', 'start', 'end']].copy()
        _bed.columns = ['chr', 'start', 'end']
        _bed_file = f"data/annotations/{_ct}.bed"
        _bed.to_csv(_bed_file, sep="\t", index=False, header=False)
        
       
        for _chrom in range(1, 23):
            annot_file = f"data/annotations/{_ct}.{_chrom}.annot.gz"
            if os.path.exists(annot_file):
                print(f"  Chromosome {_chrom} ✓ (exists)", end=" ", flush=True)
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
            print("✓")
        print(f"  ✓ {_ct} complete")
    
    print("\n✓ All annotations generated")
    return


@app.cell
def __(mo):
    mo.md("## 7. Calculate LD scores for each cell type and chromosome")
    return


@app.cell
def __(subprocess, os, cell_types, python27_path):
    os.makedirs("data/ldscores", exist_ok=True)

    print("\n" + "="*60)
    print("STEP 7: Calculating LD scores (this may take 10-30 minutes)")
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
    mo.md("## 8. Create CTS (cell-type–specific) reference file")
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
    mo.md("## 9. Run LDSC cell-type–specific heritability analysis (CTS)")
    return


@app.cell
def __(subprocess, python27_path, ldsc_sumstats_file, os):
    if ldsc_sumstats_file is None or not os.path.exists(ldsc_sumstats_file):
        print("⚠ Skipping LDSC analysis - no munged sumstats file available")
        print("  Please complete harmonization and munging steps first")
    else:
        print("\n" + "="*60)
        print("STEP 9: Running cell-type-specific heritability analysis")
        print("="*60 + "\n")
        
        subprocess.run([
            python27_path, "tools/ldsc/ldsc.py",
            "--h2-cts", ldsc_sumstats_file,
            "--ref-ld-chr", "data/reference/baselineLD_v2.2/baselineLD.",
            "--ref-ld-chr-cts", "data/cell_types.cts",
            "--w-ld-chr", "data/reference/GRCh38/weights/weights.hm3_noMHC.",
            "--out", "results/AD_CellTypeSpecific"
        ], check=True)
        
        print("\n✓ Cell-type-specific analysis complete")
    return


@app.cell
def __(mo):
    mo.md("## 10. Rank cell types by heritability enrichment significance")
    return


@app.cell
def __(pd, os):
    print("\n" + "="*60)
    print("STEP 10: Ranking cell types by significance")
    print("="*60 + "\n")
    
    results_file = "results/AD_CellTypeSpecific.cell_type_results.txt"
    
    if not os.path.exists(results_file):
        print(f"⚠ Results file not found: {results_file}")
        print("Please run Step 9 first to generate the cell-type-specific analysis results.")
        ranked = None
    else:
        results = pd.read_csv(results_file, sep="\t")
        ranked = results.sort_values("Coefficient_P_value")
        ranked.to_csv("results/AD_CellTypeSpecific_ranked.csv", index=False)
        
        print("Cell types ranked by heritability enrichment p-value:\n")
        print(ranked[["Name", "Coefficient", "Coefficient_std_error", "Coefficient_P_value"]].to_string(index=False))
        
        print("\n✓ Results saved to results/AD_CellTypeSpecific_ranked.csv")
    
    return (ranked,)


if __name__ == "__main__":
    app.run()