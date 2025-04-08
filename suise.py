import gwaslab as gl
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import gzip
from rpy2.robjects.packages import importr
import rpy2.robjects as ro
import rpy2.robjects.numpy2ri as numpy2ri
import rpy2.robjects.pandas2ri as pandas2ri

# Activate R-Python converters
numpy2ri.activate()
pandas2ri.activate()

def load_gwas_data(file_path):
    """Load GWAS data from a compressed TSV file."""
    with gzip.open(file_path, 'rt') as f:
        gwas_data_df = pd.read_csv(f, sep='\t')
    return gwas_data_df

def preprocess_gwas_data(gwas_data_df):
    """Preprocess GWAS data by splitting variant info and renaming columns."""
    # Split variant information
    gwas_data_df['CHR'] = gwas_data_df['variant'].str.split(':').str[0]
    gwas_data_df['POS'] = gwas_data_df['variant'].str.split(':').str[1]
    gwas_data_df['A2'] = gwas_data_df['variant'].str.split(':').str[2]
    gwas_data_df['A1'] = gwas_data_df['variant'].str.split(':').str[3]
    
    # Rename columns
    gwas_data_df = gwas_data_df.rename(columns={'variant': 'SNPID', 'pval': 'P'})
    
    return gwas_data_df

def filter_significant_snps(gwas_data_df, maf_threshold=0.05, p_threshold=5e-8):
    """Filter significant SNPs based on MAF and p-value thresholds."""
    # Apply filters
    minor_af_filtered_df = gwas_data_df[gwas_data_df['minor_AF'] > maf_threshold]
    significant_snp_df = minor_af_filtered_df[minor_af_filtered_df['P'] <= p_threshold]
    significant_snp_df = significant_snp_df[~significant_snp_df['SNPID'].str.startswith('X:')]
    
    return significant_snp_df

def prepare_cojo_file(significant_snp_df, output_path):
    """Prepare data for COJO analysis and save to file."""
    formatted_cojo_df = significant_snp_df.rename(columns={
        'SNPID': 'SNP',
        'A1': 'A1',
        'A2': 'A2',
        'minor_AF': 'freq',
        'beta': 'b',
        'se': 'se',
        'P': 'p',
        'n_complete_samples': 'N'
    })
    
    cojo_ready_df = formatted_cojo_df[['SNP', 'A1', 'A2', 'freq', 'b', 'se', 'p', 'N']]
    cojo_ready_df.to_csv(output_path, sep=" ", index=False)
    return cojo_ready_df

def extract_region_snps(significant_snp_df, variant_position, window_size=500000):
    """Extract SNPs within a window around a variant position."""
    start_pos = variant_position - window_size
    end_pos = variant_position + window_size
    region_snp_df = significant_snp_df[
        (significant_snp_df['POS'] >= start_pos) & 
        (significant_snp_df['POS'] <= end_pos)
    ]
    region_snp_df["log_pvalue"] = -np.log10(region_snp_df["P"])
    return region_snp_df

def run_susie_analysis(snp_df, ld_matrix, n=503, L=10):
    """Run SuSiE analysis on SNP data with LD matrix."""
    susieR = importr('susieR')
    ro.r('set.seed(123)')
    
    fit = susieR.susie_rss(
        bhat=snp_df["beta"].values.reshape(len(snp_df), 1),
        shat=snp_df["se"].values.reshape(len(snp_df), 1),
        R=ld_matrix,
        L=L,
        n=n
    )
    
    return fit

def get_credible_sets(fit, ld_matrix, coverage=0.95, min_abs_corr=0.5):
    """Get credible sets from SuSiE fit."""
    susieR = importr('susieR')
    credible_sets = susieR.susie_get_cs(
        fit, 
        coverage=coverage, 
        min_abs_corr=min_abs_corr, 
        Xcorr=ld_matrix
    )
    return credible_sets

def plot_susie_results(snp_df, fit, ld_matrix, col_to_plot="MLOG10P", window_size=500000):
    """Plot SuSiE results with credible sets."""
    # Prepare data
    if col_to_plot == "MLOG10P":
        snp_df[col_to_plot] = -np.log10(snp_df["P"])
    
    lead_pos = snp_df["P"].idxmin()
    lead_x = snp_df.loc[lead_pos, "POS"]
    lead_y = snp_df.loc[lead_pos, col_to_plot]
    
    # Get credible sets
    credible_sets = get_credible_sets(fit, ld_matrix)[0]
    n_cs = len(credible_sets)
    
    # Create figure
    plt.style.use("ggplot")
    fig, ax = plt.subplots(figsize=(15, 7))
    
    # Plot all SNPs
    p = ax.scatter(
        snp_df["POS"], 
        snp_df[col_to_plot], 
        c=ld_matrix[lead_pos]
    )
    
    # Annotate lead variant
    ax.annotate(
        f"Lead Variant: {snp_df.loc[lead_pos, 'SNPID']}", 
        (lead_x, lead_y), 
        textcoords="offset points", 
        xytext=(0, lead_y + (0.02 if col_to_plot == "pip" else 2)), 
        ha='center', 
        fontsize=12
    )
    
    # Highlight credible sets
    for i in range(n_cs):
        cs_index = credible_sets[i]
        pos = snp_df.loc[np.array(cs_index) - 1, "POS"]
        y = snp_df.loc[np.array(cs_index) - 1, col_to_plot]
        ax.scatter(
            pos, y, 
            marker='o', s=40, 
            label=f"CS{i+1}", 
            edgecolors="green", 
            facecolors="none"
        )
    
    # Set plot attributes
    plt.colorbar(p, label="LD to lead variant")
    ax.set_xlabel("Position")
    ax.set_ylabel(col_to_plot)
    ax.set_xlim((lead_x - window_size, lead_x + window_size))
    plt.legend()
    plt.show()

def plot_ld_matrices(ld_r, ld_r2, snp_names=None):
    """Plot LD matrices (r and r2)."""
    plt.figure(figsize=(10, 10), dpi=200)
    fig, ax = plt.subplots(ncols=2, figsize=(20, 10))
    
    if snp_names is not None:
        ld_r.columns = snp_names
        ld_r.index = snp_names
        ld_r2.columns = snp_names
        ld_r2.index = snp_names
    
    sns.heatmap(data=ld_r, cmap="Spectral", ax=ax[0])
    sns.heatmap(data=ld_r2, cmap="Spectral", ax=ax[1])
    
    ax[0].set_title("LD r matrix")
    ax[1].set_title("LD r2 matrix")
    plt.show()

def main():
    # Load and preprocess GWAS data
    file_path = "../data/susie/gwas/21001_raw.gwas.imputed_v3.both_sexes.tsv.bgz"
    gwas_data_df = load_gwas_data(file_path)
    gwas_data_df = preprocess_gwas_data(gwas_data_df)
    
    # Filter significant SNPs
    significant_snp_df = filter_significant_snps(gwas_data_df)
    significant_snp_df.to_csv("../data/susie/processed_raw_data/significant_snps.csv", index=False)
    
    # Prepare COJO file
    cojo_ready_df = prepare_cojo_file(
        significant_snp_df, 
        "../data/susie/reformated_data_for_cojo/cojo_extracted_file.csv"
    )
    
    # After running COJO analysis (external command)
    cojo_results_df = pd.read_csv("../data/susie/cojo/all_chr/all_chr_cojo.jma.cojo", delim_whitespace=True)
    
    # Example analysis for a specific variant
    variant_position = 53828066
    region_snp_df = extract_region_snps(significant_snp_df, variant_position)
    region_snp_df.to_csv("chr16_all_region_snps.csv")
    
    # Load LD matrices (after running PLINK commands)
    ld_r = pd.read_csv("../data/susie/ALL_chr/ld/test_sig_locus_mt.ld", sep="\t", header=None)
    ld_r2 = pd.read_csv("../data/susie/ALL_chr/ld/test_sig_locus_mt_r2.ld", sep="\t", header=None)
    
    # Plot LD matrices
    plot_ld_matrices(ld_r, ld_r2)
    
    # Run SuSiE analysis
    fit = run_susie_analysis(
        region_snp_df, 
        ld_r.values,
        n=503,
        L=10
    )
    
    # Plot results
    plot_susie_results(region_snp_df, fit, ld_r.values, col_to_plot="MLOG10P")
    plot_susie_results(region_snp_df, fit, ld_r.values, col_to_plot="pip")

if __name__ == "__main__":
    main()