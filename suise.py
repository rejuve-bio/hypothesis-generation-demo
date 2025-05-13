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

@flow(log_prints=True)
def finemapping_flow(current_user_id):
    """Flow to handle fine mapping"""

    POPULATION = "EUR"
    SAMPLE_PANEL_URL = "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/integrated_call_samples_v3.20130502.ALL.panel"
    OUTPUT_DIR = "./data/susie"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Step 0: Load and preprocess GWAS data
    file_path = "./data/susie/gwas/21001_raw.gwas.imputed_v3.both_sexes.tsv.bgz"
    print("loading gwas data")
    gwas_data_df = load_gwas_data(file_path)
    print("preprocessing gwas data")
    gwas_data_df = preprocess_gwas_data(gwas_data_df)

    
    # Filter significant SNPs
    print("Filtering significant SNPs")
    significant_snp_df = filter_significant_snps(gwas_data_df, output_dir=OUTPUT_DIR)


    # Step 1: Download and prepare VCF files
    print("Downloading and preparing VCF files")
    vcf_files = download_and_prepare_vcfs(
        output_dir=OUTPUT_DIR,
        population=POPULATION,
        sample_panel_url=SAMPLE_PANEL_URL,
    )

    # Step 2: Generate snplist file
    print("Generating snplist file")
    gwas_snplist_file = generate_snplist_file(
        gwas_snps=significant_snp_df,
        output_dir=OUTPUT_DIR
    )
    print("GWAS snplist file: ", gwas_snplist_file)
    
    # Step 3: Generate binary files from VCFs
    print("Generating binary files from VCFs")
    binary_files = generate_binary_from_vcf(
        vcf_files=vcf_files,
        gwas_snplist_file=gwas_snplist_file,
        output_dir=OUTPUT_DIR,
        population=POPULATION
    )
    print("Binary files: ", binary_files)
    
    # Step 4: Merge binary files
    print("Merging binary files")
    merged_binary = merge_plink_binaries(
        binary_files=binary_files,
        output_dir=OUTPUT_DIR,
        population=POPULATION
    )
    print("Merged binary file: ", merged_binary)


    # Step 5: Prepare COJO file
    print("Preparing COJO file")
    cojo_file_path = prepare_cojo_file(
        significant_snp_df=significant_snp_df, 
        output_dir=OUTPUT_DIR
    )
    
    print("merged binary file: ", merged_binary)
    print("cojo file path: ", cojo_file_path)

    # Step 6: Run COJO analysis
    print("Running COJO analysis")
    # cojo_results_path = run_cojo_analysis(
    #     merged_binary_path=merged_binary,
    #     cojo_file_path=cojo_file_path,
    #     output_dir=OUTPUT_DIR,
    #     maf_threshold=0.05
    # )
    # print("COJO results path: ", cojo_results_path)
    cojo_results_path = "./data/susie/cojo/all_chr/all_chr_cojo.jma.cojo"

    # most_significant_snp = cojo_results_df.sort_values(by='p').head(1)
    # print("Most significant SNP: ", most_significant_snp)
    # 16  16:53802494:C:T  53802494  ...  0.012651  2.627990e-205  0.123927

    # Step 7: Expanding region of each independet snp identified by cojo
    print("Expanding SNP regions")
    expanded_regions = expand_snp_regions(
        cojo_results_path=cojo_results_path,
        significant_snp_df=significant_snp_df,
        output_dir=OUTPUT_DIR,
        window_size=500000
    )


    # Step 7.1: Map COJO results with gene type
    print("Mapping COJO results with gene type")
    mapped_cojo_results = mapping_cojo(cojo_results_path, output_dir=OUTPUT_DIR)
    print("Mapped COJO results: ", mapped_cojo_results)

    # # Step 7.2: Group mapped COJO results by gene type
    print("Grouping mapped COJO results by gene type")
    grouped_cojo_results = grouping_cojo(
        mapped_cojo_snps=mapped_cojo_results, 
        expanded_region_files=expanded_regions, 
        output_dir=OUTPUT_DIR
        )
    print("Grouped COJO results: ", grouped_cojo_results)

     # Extract and return the gene types for user selection
    gene_types = extract_gene_types(grouped_cojo_results)
    print("Gene types: ", gene_types)

    selected_gene = "GP2"

    # Get the specific region files for the selected gene
    gene_region_files = get_gene_region_files(grouped_cojo_results, selected_gene)
    print("Gene region files: ", gene_region_files)



    # Step 8: Generate LD matrices for each expanded region
    print("Generating LD matrices")
    ld_dir = calculate_ld_for_regions(
        # region_files=expanded_regions,
        region_files=gene_region_files, # instead of the whole expanded region it will be the chosen gene grouped expendad regions
        plink_bfile=merged_binary,
        output_dir=OUTPUT_DIR
    )
    print("LD matrices directory: ", ld_dir)

    # For demonstration, process one region
    region_file = gene_region_files[0]
    print("Region file: ", region_file)
    region_name = os.path.basename(region_file).split('_snps')[0]
    print("Region name: ", region_name)
    
    ld_file = f"{ld_dir}/{region_name}_snps_ld.ld"
    ld_r = pd.read_csv(ld_file, sep="\t", header=None)
    R_df = ld_r.values
    
    # Load the expanded region file
    expanded_region_snps = pd.read_csv(region_file, sep="\t")
    
    bim_file_path = f"{OUTPUT_DIR}/plink_binary/merged_{POPULATION.lower()}.bim"
    
    # Step 9: Check dimensionality of LD matrices
    print("Checking LD dimensions")
    filtered_snp = check_ld_dimensions(R_df, expanded_region_snps, bim_file_path)
    
    # Step 10: Check if LD matrix is positive semi-definite
    print("Checking if LD matrix is positive semi-definite") 
    R_df = check_ld_semidefiniteness(R_df)
    
    # Step 11: Run SuSiE analysis
    print("Running SuSiE analysis")
    fit = run_susie_analysis(
        filtered_snp, 
        R_df,
        n=503,  # Sample size
        L=10    # Number of causal variants
    )
    
    # Step 12: Format credible sets
    print("Formatting credible sets")
    credible_sets = formattating_credible_sets(filtered_snp, fit, R_df)

    return credible_sets.to_dict(orient="records")

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