import marimo

__generated_with = "0.1.0"
app = marimo.App()

@app.cell
def __():
    import marimo as mo
    import subprocess
    import os
    import pandas as pd
    import numpy as np
    from pathlib import Path
    import sys
    
    mo.md("""
    # LDSC Cell-Type-Specific Heritability Analysis

    LDSC analysis for BMI using GTEx multi-tissue gene expression data.

    ## Prerequisites
    - LDSC software installed
    - Reference data downloaded (1000G Phase 3, baseline LD scores)
    - GTEx multi-tissue gene expression LD scores
    - GWAS summary statistics (munged format)
    """)
    return mo, np, os, pd, subprocess, sys, Path

@app.cell
def cell_setup_dirs(Path, subprocess):
    BASE_DIR = Path("/mnt/hdd_1/rediet/hypothesis-generation-demo/ldsc_ph")
    LDSC_DIR = BASE_DIR / "ldsc"
    LDSC_REPO_DIR = LDSC_DIR / "ldsc_repo"

    ldsc_script = LDSC_REPO_DIR / "ldsc.py"
    if not ldsc_script.exists():
        print("Cloning LDSC repository...")
        LDSC_REPO_DIR.mkdir(parents=True, exist_ok=True)
        subprocess.run([
            "git", "clone", "https://github.com/bulik/ldsc.git", str(LDSC_REPO_DIR)
        ], check=True)
        print(f"LDSC repository cloned successfully to {LDSC_REPO_DIR}")
        
        req_file = LDSC_REPO_DIR / "requirements.txt"
        if req_file.exists():
            print("Installing Python dependencies from requirements.txt...")
            subprocess.run([
                "pip", "install", "-r", str(req_file)
            ], check=True)
        else:
            print("No requirements.txt found in the cloned repo.")
    else:
        print("LDSC repository already exists")
    if ldsc_script.exists():
        print(f"Found ldsc.py at: {ldsc_script}")
    else:
        print("ERROR: ldsc.py still not found after cloning!")

    RESULTS_DIR = BASE_DIR / "results"
    RESULTS_DIR.mkdir(exist_ok=True)

    return (
        BASE_DIR,
        LDSC_DIR,
        LDSC_REPO_DIR,
        RESULTS_DIR,
        ldsc_script,
    )

@app.cell
def cell_copy_ldsc_script(LDSC_DIR, LDSC_REPO_DIR, subprocess):
    source_dir = LDSC_DIR / "source"
    source_dir.mkdir(exist_ok=True)
    source_script = source_dir / "ldsc.py"
    cloned_script = LDSC_REPO_DIR / "ldsc.py"
    
    if not source_script.exists() and cloned_script.exists():
        subprocess.run([
            "cp", str(cloned_script), str(source_script)
        ], check=True)
        print(f"Copied ldsc.py to {source_script}")
    
    source_ldscore = source_dir / "ldscore"
    cloned_ldscore = LDSC_REPO_DIR / "ldscore"
    
    if not source_ldscore.exists() and cloned_ldscore.exists():
        subprocess.run([
            "cp", "-r", str(cloned_ldscore), str(source_ldscore)
        ], check=True)
        print(f"Copied ldscore module to {source_ldscore}")
    if source_script.exists():
        subprocess.run(["chmod", "+x", str(source_script)], check=True)
        print("Made ldsc.py executable")

    return source_script,

@app.cell
def __(mo, os):
    config = {
        'ldsc_dir': '/mnt/hdd_1/rediet/hypothesis-generation-demo/ldsc_ph/ldsc',
        'results_dir': '/mnt/hdd_1/rediet/hypothesis-generation-demo/ldsc_ph/results',
        'gwas_file': '21001_munged.gwas.imputed_v3.both_sexes.tsv',
        'baseline_ld': '1000G_Phase3_baselineLD_ldscores/baselineLD.',
        'weights_ld': '1000G_Phase3_weights_hm3_no_MHC/weights.hm3_noMHC.',
        'cts_file': 'Multi_tissue_gene_expr_gtex.ldcts',
        'output_prefix': 'current_Mock_BMI_Multi_tissue_gtex',
        'ldsc_script': 'source/ldsc.py'
    }

    mo.md(f"""
    ## Current Configuration:
    - **LDSC Directory**: `{config['ldsc_dir']}`
    - **Results Directory**: `{config['results_dir']}`
    - **GWAS File**: `{config['gwas_file']}`
    - **Output Prefix**: `{config['output_prefix']}`
    """)
    return config,

@app.cell
def __(config, os, subprocess):
    def build_ldsc_command(cfg):
        cmd = [
            'python2', 
            os.path.join(cfg['ldsc_dir'], cfg['ldsc_script']),
            '--h2-cts', cfg['gwas_file'],
            '--ref-ld-chr', cfg['baseline_ld'],
            '--out', os.path.join(cfg['results_dir'], cfg['output_prefix']),
            '--ref-ld-chr-cts', cfg['cts_file'],
            '--w-ld-chr', cfg['weights_ld']
        ]
        return cmd
    
    def run_ldsc_analysis(cfg):
        original_dir = os.getcwd()
        os.chdir(cfg['ldsc_dir'])
        
        try:
            cmd = build_ldsc_command(cfg)
            print("Running LDSC command:")
            print(" ".join(cmd))
            print("Analysis in progress...")

            import time
            start_time = time.time()
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=cfg['ldsc_dir'],
                bufsize=1,
                universal_newlines=True
            )
            
            output_lines = []
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    print(output.strip())
                    output_lines.append(output.strip())

            rc = process.poll()
            elapsed_time = time.time() - start_time
            print(f"\nAnalysis completed in {elapsed_time:.2f} seconds")

            if rc != 0:
                print(f"Command failed with return code: {rc}")
            else:
                print("Analysis completed successfully!")
                
            class MockResult:
                def __init__(self, returncode, stdout):
                    self.returncode = returncode
                    self.stdout = "\n".join(stdout)
                    self.stderr = ""

            return MockResult(rc, output_lines)
        finally:
            os.chdir(original_dir)

    cmd = build_ldsc_command(config)
    print("LDSC Command to be executed:")
    print(" ".join(cmd))
    return build_ldsc_command, cmd, run_ldsc_analysis
