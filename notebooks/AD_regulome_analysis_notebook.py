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
    return mo, urllib, os, subprocess, pd, tarfile


@app.cell
def __(mo):
    mo.md("""
# LDSC Cell-Type Analysis – Alzheimer’s Disease

This notebook reproduces the LDSC cell-type–specific heritability analysis  
from *Epigenomic dissection of Alzheimer’s disease pinpoints causal variants and reveals epigenome erosion*.
""")
    return


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

    for ct in cell_types:
        url = f"{base_url}{ct}.peak.annotation.txt"
        output = f"data/peaks/{ct}.peak.annotation.txt"
        if not os.path.exists(output):
            urllib.request.urlretrieve(url, output)

    if not os.path.exists("data/reference/GRCh38.tgz"):
        urllib.request.urlretrieve(
            "https://zenodo.org/records/10515792/files/GRCh38.tgz?download=1",
            "data/reference/GRCh38.tgz"
        )

    if not os.path.exists("data/reference/baseline_ldscores.tgz"):
        urllib.request.urlretrieve(
            "https://zenodo.org/records/10515792/files/1000G_Phase3_baselineLD_v2.2_ldscores.tgz?download=1",
            "data/reference/baseline_ldscores.tgz"
        )

    if not os.path.exists("data/gwas/AD_bellenguez_2022_hg38.tsv.gz"):
        urllib.request.urlretrieve(
            "http://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/GCST90027001-GCST90028000/GCST90027158/GCST90027158_buildGRCh38.tsv.gz",
            "data/gwas/AD_bellenguez_2022_hg38.tsv.gz"
        )
    return base_url, cell_types


@app.cell
def __(mo):
    mo.md("## 2. Extract reference LD panels and baseline LD scores")
    return


@app.cell
def __(tarfile, os):
    if not os.path.exists("data/reference/GRCh38"):
        with tarfile.open("data/reference/GRCh38.tgz", "r:gz") as tar:
            tar.extractall("data/reference/")

    if not os.path.exists("data/reference/1000G_Phase3_baselineLD_v2.2_ldscores"):
        with tarfile.open("data/reference/baseline_ldscores.tgz", "r:gz") as tar:
            tar.extractall("data/reference/")
    return


@app.cell
def __(mo):
    mo.md("## 3. Munge Alzheimer’s disease GWAS summary statistics")
    return


@app.cell
def __(subprocess, os):
    os.makedirs("data/munged", exist_ok=True)

    subprocess.run([
        "python", "ldsc/ldsc.py",
        "--sumstats", "data/gwas/AD_bellenguez_2022_hg38.tsv.gz",
        "--out", "data/munged/AD_bellenguez_2022_hg38_munged",
        "--a1", "effect_allele",
        "--a2", "other_allele",
        "--p", "p_value",
        "--snp", "variant_id",
        "--N-col", "n_total"
    ], check=True)
    return


@app.cell
def __(mo):
    mo.md("## 4. Generate cell-type–specific binary annotations (BED → .annot.gz)")
    return


@app.cell
def __(pd, subprocess, os, cell_types):
    os.makedirs("data/annotations", exist_ok=True)

    for ct in cell_types:
        peaks = pd.read_csv(f"data/peaks/{ct}.peak.annotation.txt", sep="\t")
        bed = peaks[['seqnames', 'start', 'end']].copy()
        bed.columns = ['chr', 'start', 'end']
        bed_file = f"data/annotations/{ct}.bed"
        bed.to_csv(bed_file, sep="\t", index=False, header=False)
        
        for chrom in range(1, 23):
            subprocess.run([
                "python", "ldsc/make_annot.py",
                "--bed-file", bed_file,
                "--bimfile", f"data/reference/GRCh38/plink_files/1000G.EUR.hg38.{chrom}.bim",
                "--annot-file", f"data/annotations/{ct}.{chrom}.annot.gz"
            ], check=True)
    return bed, bed_file, chrom, ct, peaks


@app.cell
def __(mo):
    mo.md("## 5. Calculate LD scores for each cell type and chromosome")
    return


@app.cell
def __(subprocess, os, cell_types):
    os.makedirs("data/ldscores", exist_ok=True)

    for ct in cell_types:
        os.makedirs(f"data/ldscores/{ct}", exist_ok=True)
        
        for chrom in range(1, 23):
            subprocess.run([
                "python", "ldsc/ldsc.py",
                "--l2",
                "--bfile", f"data/reference/GRCh38/plink_files/1000G.EUR.hg38.{chrom}",
                "--ld-wind-cm", "1.0",
                "--annot", f"data/annotations/{ct}.{chrom}.annot.gz",
                "--thin-annot",
                "--out", f"data/ldscores/{ct}/{ct}.{chrom}"
            ], check=True)
    return


@app.cell
def __(mo):
    mo.md("## 6. Create CTS (cell-type–specific) reference file")
    return


@app.cell
def __(os, cell_types):
    os.makedirs("results", exist_ok=True)

    with open("data/cell_types.cts", "w") as f:
        for ct in cell_types:
            f.write(f"{ct}    data/ldscores/{ct}/{ct}.\n")
    return f,


@app.cell
def __(mo):
    mo.md("## 7. Run LDSC cell-type–specific heritability analysis (CTS)")
    return


@app.cell
def __(subprocess):
    subprocess.run([
        "python", "ldsc/ldsc.py",
        "--h2-cts", "data/munged/AD_bellenguez_2022_hg38_munged.sumstats.gz",
        "--ref-ld-chr", "data/reference/GRCh38/baselineLD_v2.2/baselineLD.",
        "--ref-ld-chr-cts", "data/cell_types.cts",
        "--w-ld-chr", "data/reference/GRCh38/weights/weights.hm3_noMHC.",
        "--out", "results/AD_CellTypeSpecific"
    ], check=True)
    return


@app.cell
def __(mo):
    mo.md("## 8. Rank cell types by heritability enrichment significance")
    return


@app.cell
def __(pd):
    results = pd.read_csv("results/AD_CellTypeSpecific.cell_type_results.txt", sep="\t")
    results_sorted = results.sort_values("Coefficient_P_value")
    results_sorted.to_csv("results/AD_CellTypeSpecific_ranked.csv", index=False)
    results_sorted
    return results, results_sorted


if __name__ == "__main__":
    app.run()
