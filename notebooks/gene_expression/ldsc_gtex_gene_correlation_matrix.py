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

if __name__ == "__main__":
    app.run()