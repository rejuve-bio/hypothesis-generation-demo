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



if __name__ == "__main__":
    app.run()