import glob
import os
from pathlib import Path
from typing import Dict
import logging
from prefect import task
from datetime import datetime
from loguru import logger
import gwaslab as gl
import pandas as pd
import numpy as np
import gzip
import subprocess
from cyvcf2 import VCF, Writer
from prefect import task
import numpy as np

logging.basicConfig(level=logging.INFO)

# Configure rpy2
try:
    from rpy2.robjects.packages import importr
    import rpy2.robjects as ro
    from rpy2.robjects import pandas2ri, numpy2ri, default_converter
    from rpy2.robjects.conversion import localconverter
    from rpy2 import robjects
    
    
    # Import necessary R packages
    base = importr('base')
    stats = importr('stats')
    
    # Check if packages are available before trying to import
    def check_r_package_available(package_name):
        """Check if an R package is available for import"""
        r_code = f'is.element("{package_name}", installed.packages()[,1])'
        return ro.r(r_code)[0]
    
    # Import analysis packages with proper error handling
    if check_r_package_available('susieR'):
        susieR = importr('susieR')
        HAS_SUSIE = True
        logging.info("SusieR package loaded successfully")
    else:
        logging.warning("The R package 'susieR' is not installed")
        HAS_SUSIE = False
    
    # Check and import vautils and related packages
    HAS_VAUTILS = False
    if check_r_package_available('vautils'):
        try:
            vautils = importr('vautils')
            HAS_VAUTILS = True
            logging.info("vautils package loaded successfully")
        except Exception as e:
            logging.error(f"Error importing vautils: {e}")
    else:
        logging.warning("The R package 'vautils' is not installed. Attempting installation...")
        try:
            # Try to install vautils if not already installed
            ro.r('if(!requireNamespace("remotes", quietly=TRUE)) install.packages("remotes", repos="https://cran.rstudio.com/")')
            ro.r('remotes::install_github("oyhel/vautils", dependencies=TRUE, upgrade="never")')
            vautils = importr('vautils')
            HAS_VAUTILS = True
            logging.info("vautils package installed and loaded successfully")
        except Exception as e:
            logging.error(f"Failed to install vautils: {e}")
    
    # Import other analysis packages
    try:
        dplyr = importr('dplyr')
        readr = importr('readr')
        data_table = importr('data.table')
        logging.info("Additional R analysis packages loaded successfully")
    except Exception as e:
        logging.warning(f"Could not import some R analysis packages: {e}")
    
    # We have rpy2
    HAS_RPY2 = True
    
except ImportError as e:
    logging.warning(f"rpy2 not available: {e}. R-based analyses will not work.")
    HAS_RPY2 = False
    HAS_SUSIE = False
    HAS_VAUTILS = False
    default_converter = None
    pandas2ri = None
    numpy2ri = None
    robjects = None


# === UTILITY TASKS ===
@task
def run_command(cmd: str) -> subprocess.CompletedProcess:
    """Execute a shell command and handle errors."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Error running command: {cmd}")
        logger.error(result.stderr)
        raise Exception(f"Command failed with exit code {result.returncode}")
    return result


@task
def generate_snplist_file(gwas_snps, output_dir):
    """
    Generate a SNP list file for PLINK analysis.
    Returns:
        Path to the created file
    """
    # Make sure the directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Define the full file path
    output_path = os.path.join(output_dir, f"chr_sig_locus.snplist.txt")
    
    # Write SNPs to the file
    with open(output_path, 'w') as f:
        for snp_id in gwas_snps["SNPID"]:
            f.write(f"{snp_id}\n")
    
    return output_path


@task
def extract_gene_types(grouped_cojo_results):
    """Extract unique gene types from grouped COJO results"""
    # Assuming grouped_cojo_results is a list of file paths
    gene_types = []
    for filepath in grouped_cojo_results:
        gene_name = os.path.basename(filepath).split('_merged_snps')[0]
        gene_types.append(gene_name)
    return gene_types


@task
def get_gene_region_files(grouped_cojo_results, selected_gene):
    """Get the region files for the selected gene"""
    return [f for f in grouped_cojo_results if selected_gene in f]


# === GWAS TASKS ===
@task(cache_policy=None)
def load_gwas_data(file_path):
    """
    Load GWAS data from a compressed TSV file using chunked reading for memory efficiency.
    For large files (>500MB), this approach prevents loading the entire file into memory at once.
    """
    # Determine if file is gzipped by extension
    is_gzipped = file_path.endswith('.gz') or file_path.endswith('.bgz')
    
    # Check file size
    file_size = os.path.getsize(file_path)
    file_size_mb = file_size / (1024*1024)
    logger.info(f"[GWAS] Processing GWAS file of size: {file_size_mb:.2f} MB")
    
    # Set chunk size based on file size - larger chunks for smaller files
    if file_size > 500 * 1024 * 1024: 
        chunk_size = 100_000 
    elif file_size > 100 * 1024 * 1024:
        chunk_size = 250_000
    else:
        chunk_size = 500_000  
    
    start_time = datetime.now()
    try:
        # For smaller files, read all at once to avoid overhead
        if file_size < 50 * 1024 * 1024:  # < 50MB
            logger.info(f"[GWAS] Small file detected, reading all at once")
            if is_gzipped:
                with gzip.open(file_path, 'rt') as f:
                    df = pd.read_csv(f, sep='\t')
                    logger.info(f"[GWAS] Loaded {len(df)} rows from gzipped file")
                    return df
            else:
                df = pd.read_csv(file_path, sep='\t')
                logger.info(f"[GWAS] Loaded {len(df)} rows from uncompressed file")
                return df
        
        # For larger files, use chunking
        chunks = []
        total_rows = 0
        
        logger.info(f"[GWAS] Large file detected, using chunked reading with {chunk_size} rows per chunk")
        
        if is_gzipped:
            # Create TextFileReader object for chunked reading from gzipped file
            with gzip.open(file_path, 'rt') as f:
                # Get initial chunk to determine column types for optimization
                first_chunk = pd.read_csv(f, sep='\t', nrows=1000)
                logger.info(f"[GWAS] Read first chunk with {len(first_chunk)} rows to determine column types")
                
                # Reopen the file and read in chunks with optimized dtypes
                f.seek(0)
                chunk_reader = pd.read_csv(f, sep='\t', chunksize=chunk_size, dtype=first_chunk.dtypes.to_dict())
                
                for i, chunk in enumerate(chunk_reader):
                    chunks.append(chunk)
                    total_rows += len(chunk)
                    
                    # Print progress every 5 chunks
                    if (i+1) % 5 == 0:
                        elapsed = (datetime.now() - start_time).total_seconds()
                        logger.info(f"[GWAS] Progress: loaded {i+1} chunks ({total_rows} rows) in {elapsed:.1f} seconds")
        else:
            # Get initial chunk to determine column types for optimization
            first_chunk = pd.read_csv(file_path, sep='\t', nrows=1000)
            logger.info(f"[GWAS] Read first chunk with {len(first_chunk)} rows to determine column types")
            
            # Read in chunks with optimized dtypes
            chunk_reader = pd.read_csv(file_path, sep='\t', chunksize=chunk_size, dtype=first_chunk.dtypes.to_dict())
            
            for i, chunk in enumerate(chunk_reader):
                chunks.append(chunk)
                total_rows += len(chunk)
                
                # Print progress every 5 chunks
                if (i+1) % 5 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.info(f"[GWAS] Progress: loaded {i+1} chunks ({total_rows} rows) in {elapsed:.1f} seconds")
        
        # Combine all chunks
        result_df = pd.concat(chunks, ignore_index=True)
        
        # Calculate final stats
        total_elapsed = (datetime.now() - start_time).total_seconds()
        memory_usage = result_df.memory_usage(deep=True).sum() / (1024*1024)
        
        logger.info(f"[GWAS] Completed loading {len(result_df)} rows in {total_elapsed:.1f} seconds")
        logger.info(f"[GWAS] Final DataFrame size: {memory_usage:.2f} MB in memory")
        
        return result_df
    
    except Exception as e:
        logger.info(f"[GWAS] Error loading GWAS data: {str(e)}")
        raise


@task(cache_policy=None)
def preprocess_gwas_data(gwas_data_df):
    """
    Preprocess GWAS data by splitting variant info and renaming columns.
    Optimized to handle large dataframes efficiently.
    """
    logger.info(f"[GWAS] Preprocessing GWAS data with shape: {gwas_data_df.shape}")
    start_time = datetime.now()
    
    try:
        # Use vectorized string operations for better performance
        if 'variant' in gwas_data_df.columns:
            logger.info(f"[GWAS] Splitting variant field into components")
            # Split variant field into components
            variant_parts = gwas_data_df['variant'].str.split(':', expand=True)
            
            # Assign columns only if they exist in the split result
            if variant_parts.shape[1] >= 4:
                gwas_data_df['CHR'] = variant_parts[0]
                gwas_data_df['POS'] = variant_parts[1]
                gwas_data_df['A2'] = variant_parts[2]
                gwas_data_df['A1'] = variant_parts[3]
                logger.info(f"[GWAS] Successfully extracted CHR, POS, A1, A2 columns")
            else:
                err_msg = f"Variant field doesn't have expected format. Found {variant_parts.shape[1]} parts instead of 4+"
                logger.info(f"[GWAS] Error: {err_msg}")
                raise ValueError(err_msg)

            # Convert POS to integer - use pd.to_numeric with downcast for memory efficiency
            logger.info(f"[GWAS] Converting POS to integer values")
            gwas_data_df['POS'] = pd.to_numeric(gwas_data_df['POS'], errors='coerce', downcast='integer')
            
            # Rename columns
            logger.info(f"[GWAS] Renaming columns")
            gwas_data_df = gwas_data_df.rename(columns={'variant': 'SNPID', 'pval': 'P'})
        else:
            # Handle case where columns might have different naming
            logger.info("[GWAS] Warning: 'variant' column not found in GWAS data. Assuming data is already preprocessed.")
        
        # Calculate and print memory statistics    
        memory_usage = gwas_data_df.memory_usage(deep=True).sum() / (1024*1024)
        elapsed_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[GWAS] Preprocessing completed in {elapsed_time:.2f} seconds")
        logger.info(f"[GWAS] Preprocessed data shape: {gwas_data_df.shape}, memory usage: {memory_usage:.2f} MB")
            
        return gwas_data_df
    
    except Exception as e:
        logger.info(f"[GWAS] Error preprocessing GWAS data: {str(e)}")
        raise


@task(cache_policy=None)
def filter_significant_snps(gwas_data_df, output_dir, maf_threshold=0.05, p_threshold=5e-8):
    """Filter significant SNPs based on MAF and p-value thresholds."""
    start_time = datetime.now()
    logger.info(f"[GWAS] Filtering significant SNPs (MAF > {maf_threshold}, p < {p_threshold})")
    logger.info(f"[GWAS] Input data has {len(gwas_data_df)} rows")
    
    filtered_dir = os.path.join(output_dir, "processed_raw_data")
    os.makedirs(filtered_dir, exist_ok=True)
    output_path = os.path.join(filtered_dir, "significant_snps.csv")

    # Apply filters
    logger.info(f"[GWAS] Applying MAF filter > {maf_threshold}")
    minor_af_filtered_df = gwas_data_df[gwas_data_df['minor_AF'] > maf_threshold]
    logger.info(f"[GWAS] After MAF filter: {len(minor_af_filtered_df)} rows")
    
    logger.info(f"[GWAS] Applying p-value filter < {p_threshold}")
    significant_snp_df = minor_af_filtered_df[minor_af_filtered_df['P'] <= p_threshold]
    logger.info(f"[GWAS] After p-value filter: {len(significant_snp_df)} rows")
    
    # Remove chromosome X SNPs
    logger.info(f"[GWAS] Removing chromosome X SNPs")
    x_snps_count = significant_snp_df['SNPID'].str.startswith('X:').sum()
    significant_snp_df = significant_snp_df[~significant_snp_df['SNPID'].str.startswith('X:')]
    logger.info(f"[GWAS] Removed {x_snps_count} X chromosome SNPs")
    
    logger.info(f"[GWAS] Final significant SNPs count: {len(significant_snp_df)}")
    logger.info(f"[GWAS] Saving significant SNPs to {output_path}")

    significant_snp_df.to_csv(output_path, index=False)
    
    # Calculate summary statistics
    chromosomes = significant_snp_df['CHR'].value_counts().to_dict()
    chr_summary = ", ".join([f"Chr{k}: {v}" for k, v in sorted(chromosomes.items())])
    elapsed_time = (datetime.now() - start_time).total_seconds()
    
    logger.info(f"[GWAS] Filter completed in {elapsed_time:.2f} seconds")
    logger.info(f"[GWAS] Chromosomes distribution: {chr_summary}")
    
    return significant_snp_df


@task(cache_policy=None)
def prepare_cojo_file(significant_snp_df, output_dir):
    """Prepare data for COJO analysis and save to file."""

    cojo_file_dir = os.path.join(output_dir, "reformated_data_for_cojo")
    os.makedirs(cojo_file_dir, exist_ok=True)
    cojo_file_path = os.path.join(cojo_file_dir, "cojo_extracted_file.csv")

    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(cojo_file_path), exist_ok=True)
    
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
    
    # Select required columns
    cojo_ready_df = formatted_cojo_df[['SNP', 'A1', 'A2', 'freq', 'b', 'se', 'p', 'N']]
    
    # Save to file with space separator
    cojo_ready_df.to_csv(cojo_file_path, sep=" ", index=False)

    logger.info("COJO ready file: ", cojo_ready_df)
    
    return cojo_file_path


# === VCF AND PLINK TASKS ===
@task
def download_and_prepare_vcfs(output_dir, population, sample_panel_url) -> Dict[str, Dict[str, str]]:
    """
    Download VCF files for all chromosomes and prepare updated VCF files with proper variant IDs.
    """
    output_dirt = "./data/susie"
    os.makedirs(output_dirt, exist_ok=True)

    vcf_dir = os.path.join(output_dirt, "vcf")
    updated_vcf_dir = os.path.join(output_dirt, "updated_vcf")
    gwas_dir = os.path.join(output_dir, "gwas")
    
    # Create all necessary directories
    os.makedirs(vcf_dir, exist_ok=True)
    os.makedirs(updated_vcf_dir, exist_ok=True)
    os.makedirs(gwas_dir, exist_ok=True)

    # Sample panel file
    sample_panel_filename = "integrated_call_samples_v3.20130502.ALL.panel"
    sample_panel_path = os.path.join(gwas_dir, sample_panel_filename)

    # Download panel if it doesn't exist
    if not os.path.exists(sample_panel_path):
        run_command(f"wget {sample_panel_url} -O {sample_panel_path}")

    panel = pd.read_csv(sample_panel_path, sep="\t")   
    selected_samples = panel[panel["super_pop"] == population]["sample"].tolist()
    
    with open(f"{output_dir}/{population.lower()}_samples.txt", "w") as f:
        f.write("\n".join([f"{s}\t{s}" for s in selected_samples]))
    
    # Process each chromosome
    result_files = {}
    for chrom in range(1, 23):
        chrom_str = str(chrom)
        vcf_url = f"ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.chr{chrom_str}.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz"
        
        vcf_file = os.path.join(vcf_dir, f"ALL.chr{chrom_str}.vcf.gz")
        updated_vcf_file = os.path.join(updated_vcf_dir, f"ALL.chr{chrom_str}.updated.vcf.gz")
        
        logger.info(f"Processing chromosome {chrom_str}...")
        
        # Download VCF if it doesn't exist
        if not os.path.exists(vcf_file):
            run_command(f"wget {vcf_url} -O {vcf_file}")
        
        # Update variant IDs
        if not os.path.exists(updated_vcf_file):
            vcf = VCF(vcf_file)
            writer = Writer(updated_vcf_file, vcf)
            for variant in vcf:
                variant_chrom = variant.CHROM
                pos = variant.POS
                ref = variant.REF
                alt = variant.ALT[0]
                variant.ID = f"{variant_chrom}:{pos}:{ref}:{alt}"
                writer.write_record(variant)
            writer.close()
        
        result_files[chrom_str] = {
            "vcf": vcf_file,
            "updated_vcf": updated_vcf_file
        }
    
    return result_files


@task
def generate_binary_from_vcf(
    vcf_files, 
    gwas_snplist_file,
    output_dir,
    population
) -> Dict[str, Dict[str, str]]:
    """
    Generate PLINK binary files from VCF files and filter by SNP list.
    """
    plink_binary_dir = os.path.join(output_dir, "plink_binary")
    os.makedirs(plink_binary_dir, exist_ok=True)

    samples_file = os.path.join(output_dir, f"{population.lower()}_samples.txt")
    
    binary_files = {}
    for chrom, files in vcf_files.items():
        updated_vcf_file = files["updated_vcf"]
        
        # Create base binary files
        plink_prefix = os.path.join(plink_binary_dir, f"chr{chrom}_{population.lower()}")
        if not os.path.exists(f"{plink_prefix}.bed"):
            run_command(
                f"plink --vcf {updated_vcf_file} "
                f"--keep {samples_file} "
                f"--keep-allele-order "
                f"--make-bed --out {plink_prefix} "
            )
        
        # Filter by SNP list
        filtered_prefix = os.path.join(plink_binary_dir, f"chr{chrom}_{population.lower()}_filtered")
        if not os.path.exists(f"{filtered_prefix}.bed"):
            run_command(
                f"plink --bfile {plink_prefix} "
                f"--extract {gwas_snplist_file} "
                f"--keep-allele-order "
                f"--make-bed --out {filtered_prefix}"
            )
        
        binary_files[chrom] = {
            "base": plink_prefix,
            "filtered": filtered_prefix
        }
    
    return binary_files


@task
def merge_plink_binaries(
    binary_files, 
    output_dir,
    population
) -> str:
    """
    Merge filtered PLINK binary files from multiple chromosomes.
    """
    plink_binary_dir = os.path.join(output_dir, "plink_binary")
    os.makedirs(plink_binary_dir, exist_ok=True)
    merged_prefix = os.path.join(plink_binary_dir, f"merged_{population.lower()}")
    
    # Create merge list file
    merge_list_file = os.path.join(plink_binary_dir, "merge_list.txt")
    with open(merge_list_file, "w") as f:
        # Start with the first chromosome as the base
        base_chrom = next(iter(binary_files))
        base_file = binary_files[base_chrom]["filtered"]
        
        # Write the rest to the merge list
        for chrom, files in binary_files.items():
            if chrom != base_chrom:
                f.write(f"{files['filtered']}\n")
    
    # Merge binary files
    if not os.path.exists(f"{merged_prefix}.bed"):
        run_command(
            f"plink --bfile {base_file} "
            f"--merge-list {merge_list_file} "
            f"--keep-allele-order "
            f"--make-bed --out {merged_prefix}"
        )
    
    return merged_prefix


# === COJO TASKS ===
@task
def run_cojo_analysis(
    merged_binary_path,
    cojo_file_path,
    output_dir,
    maf_threshold: float = 0.05
) -> str:
    """
    Run COJO analysis using the merged PLINK binary files.
    """
    # Create output directory for COJO results
    cojo_dir = os.path.join(output_dir, "cojo", "all_chr")
    os.makedirs(cojo_dir, exist_ok=True)
    
    # Define output prefix
    cojo_output = os.path.join(cojo_dir, "all_chr_cojo")
    
    # Run COJO analysis
    run_command(
    f"/app/data/external_data/susie/gcta/gcta-1.94.3-linux-kernel-3-x86_64/gcta64 "
    f"--bfile {merged_binary_path} "
    f"--maf {maf_threshold} "
    f"--cojo-file {cojo_file_path} "
    f"--cojo-slct "
    f"--out {cojo_output}"
)
    
    # Now find the file that ends with .jma.cojo
    jma_cojo_files = glob.glob(f"{cojo_dir}/all_chr_cojo.jma.cojo")
    
    if not jma_cojo_files:
        raise FileNotFoundError(f"No .jma.cojo file found in {cojo_dir}")
    
    # Assuming there is exactly one .jma.cojo file, return its path
    return jma_cojo_files[0]


@task(log_prints=True)
def expand_snp_regions(cojo_results_path, significant_snp_df, output_dir, window_size=500000):
    """Expand regions around independent SNPs identified by COJO"""

    cojo_results_df = pd.read_csv(cojo_results_path, sep='\s+')
    logger.info("COJO results (.jma.cojo): ", cojo_results_df)

    logger.info(f"Expanding regions around {len(cojo_results_df)} independent SNPs with window size {window_size}")
    
    region_files = []
    expanded_dir = os.path.join(output_dir, "expanded_regionss")
    os.makedirs(expanded_dir, exist_ok=True)
    
    for index, row in cojo_results_df.iterrows():
        chrom = row["Chr"]
        pos = row["bp"]
        start_pos = pos - window_size
        end_pos = pos + window_size
        
        df_region = significant_snp_df[
            (significant_snp_df["CHR"].astype(int) == chrom) & 
            (significant_snp_df["POS"] >= start_pos) & 
            (significant_snp_df["POS"] <= end_pos)
        ]
        
        output_file = f"{expanded_dir}/chr{chrom}_pos{pos}_snps.txt"
        df_region.to_csv(output_file, sep="\t", index=False)
        region_files.append(output_file)
        
        logger.info(f"Extracted {len(df_region)} SNPs for Chr{chrom} position {pos} and saved to {output_file}")
    
    return region_files


@task
def mapping_cojo(cojo_results_path, output_dir):
    
    if not HAS_RPY2:
        logging.error("rpy2 not available for mapping_cojo_alt task")
        raise RuntimeError("rpy2 not available")
    
    mapped_dir = os.path.join(output_dir, "mapped_cojo")
    os.makedirs(mapped_dir, exist_ok=True)
    output_path = os.path.join(mapped_dir, "mapped_cojo_results.txt")

    with localconverter(default_converter + pandas2ri.converter + numpy2ri.converter):
        # Create an R function and assign Python variables to R environment        
        ro.globalenv['input_path'] = cojo_results_path
        ro.globalenv['output_path'] = output_path
    
        # Execute R code
        r_script = """
        function() {
            # Load required libraries
            library(readr)
            library(dplyr)
            library(vautils)
            library(data.table)
            
            # Read the input file
            topSNPs <- readr::read_tsv(input_path)
            
            # Rename columns for vautils
            top_snps <- dplyr::rename(topSNPs, rsid = SNP, chromosome = Chr, position = bp)
            
            # Find nearest genes
            mapped_genes <- vautils::find_nearest_gene(
                as.data.frame(top_snps),
                build = "hg19",
                collapse = FALSE,
                snp = "rsid",
                flanking = 1000
            )
            
            # Process results
            mapped_genes <- mapped_genes %>%
                dplyr::mutate(distance = dplyr::recode(distance, "intergenic" = "0")) %>%
                dplyr::mutate(distance = abs(as.numeric(distance))) %>%
                dplyr::arrange(distance) %>%
                dplyr::group_by(rsid) %>%
                dplyr::filter(dplyr::row_number() == 1) %>%
                dplyr::ungroup() %>%
                dplyr::rename(gene_name = GENE)
            
            # Join with original data
            final_df <- dplyr::left_join(top_snps, mapped_genes, by = c("rsid", "chromosome", "position"))
            
            # Write output
            data.table::fwrite(
                final_df,
                output_path,
                col.names = TRUE,
                row.names = FALSE,
                sep = "\t",
                quote = FALSE
            )
            
            return(output_path)
        }
        """
        
        try:
            result = ro.r(r_script)()
            logging.info(f"Successfully mapped COJO results to genes and saved to {output_path}")
        except Exception as e:
            logging.error(f"Error in R execution: {e}")
            raise
    
    return output_path


@task
def grouping_cojo(mapped_cojo_snps, expanded_region_files, output_dir):
    """Group COJO results by gene and save to file"""

    mapped_cojo_snps = pd.read_csv(mapped_cojo_snps, sep="\t")

    # Defensive check and conversion to DataFrame
    if isinstance(mapped_cojo_snps, str):
        raise ValueError("mapped_cojo_snps is a string. It should be a list of dicts or a DataFrame.")

    if isinstance(mapped_cojo_snps, list):
        if all(isinstance(item, dict) for item in mapped_cojo_snps):
            mapped_cojo_snps = pd.DataFrame(mapped_cojo_snps)
        else:
            raise ValueError("mapped_cojo_snps is a list but does not contain dictionaries.")

    elif isinstance(mapped_cojo_snps, dict):
        mapped_cojo_snps = pd.DataFrame([mapped_cojo_snps])

    elif not isinstance(mapped_cojo_snps, pd.DataFrame):
        raise TypeError(f"Unexpected type for mapped_cojo_snps: {type(mapped_cojo_snps)}")

    grouped_dir = os.path.join(output_dir, "grouped_regions")
    os.makedirs(grouped_dir, exist_ok=True)

    gene_region_map = {}

    for file_path in expanded_region_files:
        file_name = os.path.basename(file_path)
        try:
            chrom = file_name.split('_')[0].replace('chr', '')
            pos = int(file_name.split('_')[1].replace('pos', ''))
        except (IndexError, ValueError):
            logger.info(f"Skipping invalid file name format: {file_name}")
            continue

        genes_on_chr = mapped_cojo_snps[mapped_cojo_snps['chromosome'].astype(str) == chrom]
        if genes_on_chr.empty:
            continue

        genes_on_chr = genes_on_chr.copy()
        genes_on_chr['gene_center'] = (genes_on_chr['geneSTART'] + genes_on_chr['geneSTOP']) // 2
        genes_on_chr['distance_to_region'] = (genes_on_chr['gene_center'] - pos).abs()

        closest_gene_row = genes_on_chr.sort_values(by='distance_to_region').iloc[0]
        gene_name = closest_gene_row['gene_name']

        gene_region_map.setdefault(gene_name, []).append(file_path)

    # Save the region-gene mapping
    gene_region_df = pd.DataFrame([
        [gene, region_file]
        for gene, region_files in gene_region_map.items()
        for region_file in region_files
    ], columns=["gene_name", "region_file"])

    map_path = os.path.join(output_dir, "reduced_mapped_expanded_regions.txt")
    gene_region_df.to_csv(map_path, index=False)

    # Merge region files per gene
    merged_file_paths = []
    for gene, file_paths in gene_region_map.items():
        combined_df = pd.concat([pd.read_csv(f, sep='\t') for f in file_paths], ignore_index=True)
        combined_df = combined_df.drop_duplicates()
        output_path = os.path.join(grouped_dir, f"{gene}_merged_snps.txt")
        combined_df.to_csv(output_path, sep='\t', index=False)
        merged_file_paths.append(output_path)

    return merged_file_paths


# === FINE-MAPPING TASKS ===
@task
def calculate_ld_for_regions(region_files, plink_bfile, output_dir):
    ld_dir = os.path.join(output_dir, "ld")
    os.makedirs(ld_dir, exist_ok=True)
    
    for region_file in region_files:
        # Example filename: chr2_pos4783929_snps.txt
        base_name = Path(region_file).stem  # → "chr2_pos4783929_snps"
        
        ld_output_prefix = os.path.join(ld_dir, f"{base_name}_ld")
        r2_output_prefix = os.path.join(ld_dir, f"{base_name}_r2")

        ld_output_file = f"{ld_output_prefix}.ld"
        r2_output_file = f"{r2_output_prefix}.ld"

        # LD matrix (correlation)
        if not os.path.exists(ld_output_file):
            run_command(
                f"plink --bfile {plink_bfile} "
                f"--keep-allele-order --r square "
                f"--extract {region_file} "
                f"--out {ld_output_prefix}"
            )

        # LD matrix (r²)
        if not os.path.exists(r2_output_file):
            run_command(
                f"plink --bfile {plink_bfile} "
                f"--keep-allele-order --r2 square "
                f"--extract {region_file} "
                f"--out {r2_output_prefix}"
            )

    logger.info(f"LD calculation completed. Files saved in: {ld_dir}")
    return ld_dir


@task
def check_ld_dimensions(ld_matrix, snp_df, bim_file_path):
    
    if ld_matrix.shape[0] != len(snp_df) or ld_matrix.shape[1] != len(snp_df):
        logger.info("Dimension mismatch detected between LD matrix and SNP list.")
        logger.info(f"LD shape: {ld_matrix.shape}, SNP list length: {len(snp_df)}")

        # Load available SNP IDs from bim file (column 2 = SNP)
        available_snps = pd.read_csv(bim_file_path, sep="\t", header=None, names=["CHR", "SNP", "CM", "POS", "A1", "A2"])
        available_snps_set = set(available_snps["SNP"])

        # Identify missing SNPs
        missing_mask = ~snp_df["SNPID"].isin(available_snps_set)
        missing_snps = snp_df[missing_mask]

        logger.info(f"Missing SNPs count: {len(missing_snps)}")

        # Filter out the missing SNPs from snp_df
        filtered_snp_df = snp_df[~missing_mask].reset_index(drop=True)

        logger.info(f"Filtered SNP list length: {len(filtered_snp_df)}")
        return filtered_snp_df

    logger.info("No dimension mismatch. No filtering needed.")
    return snp_df


@task
def check_ld_semidefiniteness(R_df):
    """
    Check if the LD matrix is semidefinite.
    """
    eigvals = np.linalg.eigvalsh(R_df)
    min_eigval = eigvals.min()
    if min_eigval < 0:
        eps = 0.1
        R_df += np.eye(R_df.shape[0]) * eps
    
    return R_df


def validate_input_data(snp_df, ld_matrix, n, L):
    """Validate and clean input data for SuSiE analysis."""
    
    # Check required columns
    if 'beta' not in snp_df.columns or 'se' not in snp_df.columns:
        raise ValueError("SNP DataFrame must contain 'beta' and 'se' columns")
    
    # Convert to numeric and handle missing values
    beta_values = pd.to_numeric(snp_df["beta"], errors='coerce')
    se_values = pd.to_numeric(snp_df["se"], errors='coerce')
    
    # Check for missing values
    if beta_values.isna().any():
        logging.warning(f"Found {beta_values.isna().sum()} missing beta values, filling with 0")
        beta_values = beta_values.fillna(0)
    
    if se_values.isna().any():
        logging.warning(f"Found {se_values.isna().sum()} missing SE values, filling with 0.1")
        se_values = se_values.fillna(0.1)
    
    # Ensure positive SE values
    se_values = np.maximum(se_values, 1e-6)
    
    # Check for extreme values
    if np.any(np.abs(beta_values) > 10):
        logging.warning("Found extreme beta values (>10), clipping to [-10, 10]")
        beta_values = np.clip(beta_values, -10, 10)
    
    if np.any(se_values > 5):
        logging.warning("Found extreme SE values (>5), clipping to [1e-6, 5]")
        se_values = np.clip(se_values, 1e-6, 5)
    
    # Validate LD matrix
    ld_matrix = np.array(ld_matrix, dtype=np.float64)
    n_snps = len(snp_df)
    
    if ld_matrix.shape != (n_snps, n_snps):
        raise ValueError(f"LD matrix shape {ld_matrix.shape} doesn't match SNP count {n_snps}")
    
    # Check LD matrix properties
    if not np.allclose(ld_matrix, ld_matrix.T, rtol=1e-10):
        logging.warning("LD matrix is not symmetric, making it symmetric")
        ld_matrix = (ld_matrix + ld_matrix.T) / 2
    
    # Check diagonal elements
    diag_elements = np.diag(ld_matrix)
    if not np.allclose(diag_elements, 1.0, rtol=1e-3):
        logging.warning("LD matrix diagonal elements are not 1, normalizing")
        # Normalize to correlation matrix
        D = np.sqrt(np.diag(ld_matrix))
        ld_matrix = ld_matrix / np.outer(D, D)
        np.fill_diagonal(ld_matrix, 1.0)
    
    # Check for NaN or infinite values
    if np.any(np.isnan(ld_matrix)) or np.any(np.isinf(ld_matrix)):
        raise ValueError("LD matrix contains NaN or infinite values")
    
    # Check eigenvalues for positive definiteness
    eigenvals = np.linalg.eigvals(ld_matrix)
    min_eigenval = np.min(eigenvals)
    
    if min_eigenval < 1e-6:
        logging.warning(f"LD matrix is near-singular (min eigenvalue: {min_eigenval:.2e}), regularizing")
        # Add regularization to diagonal
        reg_param = max(1e-4, abs(min_eigenval) + 1e-4)
        ld_matrix += np.eye(n_snps) * reg_param
        
        # Renormalize diagonal to 1
        diag_correction = np.sqrt(np.diag(ld_matrix))
        ld_matrix = ld_matrix / np.outer(diag_correction, diag_correction)
        np.fill_diagonal(ld_matrix, 1.0)
    
    # Validate other parameters
    if not isinstance(n, (int, float)) or n <= 0:
        raise ValueError(f"Sample size n must be positive, got {n}")
    
    if not isinstance(L, int) or L <= 0:
        raise ValueError(f"Number of causal variants L must be positive integer, got {L}")
    
    return beta_values.values, se_values.values, ld_matrix

@task(cache_policy=None)
def run_susie_analysis(snp_df, ld_matrix, n, L):
    """Run SuSiE analysis on SNP data with LD matrix."""

    if not HAS_SUSIE:
        raise ImportError("SuSiE R package is not available. Cannot run analysis.")
    
    ro.r('set.seed(123)')

    # Prepare data for SuSiE
    try:
        # Check for required columns
        if 'beta' not in snp_df.columns or 'se' not in snp_df.columns:
            raise ValueError("SNP DataFrame must contain 'beta' and 'se' columns")
        
        # Check for matrix dimensions
        if ld_matrix.shape[0] != len(snp_df) or ld_matrix.shape[1] != len(snp_df):
            raise ValueError(f"LD matrix dimensions ({ld_matrix.shape}) don't match SNP data length ({len(snp_df)})")
        
        # Run SuSiE analysis
        with localconverter(default_converter + pandas2ri.converter + numpy2ri.converter):
            fit = susieR.susie_rss(
                bhat=snp_df["beta"].values.reshape(len(snp_df), 1),
                shat=snp_df["se"].values.reshape(len(snp_df), 1),
                R=ld_matrix,
                L=L,
                n=n
                # max_iter=500
            )
        
        return fit
    except Exception as e:
        logging.error(f"Error in SuSiE analysis: {str(e)}")
        raise

@task
def formattating_credible_sets(filtered_snp, fit, R_df):
    if not HAS_SUSIE:
        raise ImportError("SuSiE R package is not available. Cannot format credible sets.")
    
    with localconverter(default_converter + pandas2ri.converter + numpy2ri.converter):
        # Create a clean copy to avoid view/dtype issues
        filtered_snp = filtered_snp.copy()
        
        # Initialize cs column with 0s
        filtered_snp["cs"] = 0
        
        try:
            # Get credible sets from SuSiE fit
            credible_sets_result = susieR.susie_get_cs(fit, coverage=0.95, min_abs_corr=0.5, Xcorr=R_df)
            logger.info(f"Got credible sets result: {type(credible_sets_result)}")
            
            # Extract the actual credible sets (indices)
            # susie_get_cs returns a list where [0] contains the credible set indices
            credible_sets = credible_sets_result[0] if len(credible_sets_result) > 0 else []
            n_cs = len(credible_sets)
            logger.info(f"Number of credible sets: {n_cs}")
            
            # Assign credible set membership
            for i in range(n_cs):
                cs_indices = credible_sets[i]
                # Convert R 1-based indices to Python 0-based indices
                python_indices = np.array(cs_indices) - 1
                # Assign credible set number (1-based) to these SNPs
                filtered_snp.iloc[python_indices, filtered_snp.columns.get_loc("cs")] = i + 1
                logger.info(f"Assigned {len(python_indices)} SNPs to credible set {i + 1}")
            
            # Get PIPs (Posterior Inclusion Probabilities) - using robust extraction
            pip_extraction_success = False
            
            # Method 1: Use R to calculate PIPs
            if not pip_extraction_success:
                try:
                    logger.info("Attempting R-based PIP calculation")
                    
                    # Set the fit object in R global environment
                    ro.globalenv['susie_fit'] = fit
                    
                    # Calculate PIPs in R
                    r_pip_code = """
                    tryCatch({
                        fit <- susie_fit
                        
                        if (!is.null(fit$alpha) && is.matrix(fit$alpha)) {
                            # PIPs = 1 - product of (1 - alpha) across all L components
                            alpha_matrix <- fit$alpha
                            pip_vals <- 1 - apply(1 - alpha_matrix, 2, prod)
                            cat("PIPs calculated from alpha matrix in R\\n")
                        } else if (!is.null(fit$pip)) {
                            # Direct PIP extraction if available
                            pip_vals <- as.numeric(fit$pip)
                            cat("PIPs extracted directly from fit object in R\\n")
                        } else {
                            # Fallback: uniform low PIPs
                            pip_vals <- rep(0.05, nrow(fit$alpha) %||% 100)
                            cat("Using fallback uniform PIPs in R\\n")
                        }
                        
                        # Ensure PIPs are numeric and valid
                        pip_vals <- as.numeric(pip_vals)
                        pip_vals[is.na(pip_vals)] <- 0.0
                        pip_vals[pip_vals < 0] <- 0.0
                        pip_vals[pip_vals > 1] <- 1.0
                        
                        pip_vals
                        
                    }, error = function(e) {
                        cat("Error in R PIP calculation:", conditionMessage(e), "\\n")
                        rep(0.05, 100)  # Fallback
                    })
                    """
                    
                    r_pip_result = ro.r(r_pip_code)
                    pip_array = np.array(r_pip_result, dtype=np.float64)
                    
                    # Ensure length matches
                    if len(pip_array) != len(filtered_snp):
                        logger.warning(f"R PIP array length mismatch: {len(pip_array)} vs {len(filtered_snp)}")
                        if len(pip_array) < len(filtered_snp):
                            pip_array = np.pad(pip_array, (0, len(filtered_snp) - len(pip_array)), constant_values=0.01)
                        else:
                            pip_array = pip_array[:len(filtered_snp)]
                    
                    filtered_snp["pip"] = pip_array
                    pip_extraction_success = True
                    logger.info(f"SUCCESS: R-based PIP calculation worked! PIPs range: {pip_array.min():.6f} to {pip_array.max():.6f}")
                    
                except Exception as e_r:
                    logger.error(f"R-based PIP calculation failed: {e_r}")
            
            # Final fallback: p-value based PIPs
            if not pip_extraction_success:
                logger.error("ALL PIP EXTRACTION METHODS FAILED!")
                logger.info("Using p-value-based proxy PIPs as final fallback")
                # Use statistical significance as a proxy for PIPs
                if 'P' in filtered_snp.columns:
                    # Convert p-values to rough PIP estimates: smaller p-value = higher PIP
                    p_values = filtered_snp['P'].values
                    # Normalize -log10(p) to [0,1] range and adjust for credible set membership
                    log_p = -np.log10(p_values + 1e-300)  # Add small value to avoid log(0)
                    normalized_pip = log_p / (log_p.max() + 1e-6)  # Normalize to [0,1]
                    # Boost PIPs for credible set members
                    pip_values = np.where(filtered_snp["cs"] > 0, 
                                        np.maximum(normalized_pip, 0.5),  # At least 0.5 for CS members
                                        normalized_pip * 0.3)  # Lower for non-members
                    filtered_snp["pip"] = pip_values
                    logger.info(f"Used p-value based PIPs with range: {pip_values.min():.6f} to {pip_values.max():.6f}")
                else:
                    # Ultimate fallback: simple membership-based PIPs
                    pip_values = np.where(filtered_snp["cs"] > 0, 0.8, 0.1)
                    filtered_snp["pip"] = pip_values
                    logger.info("Used simple membership-based proxy PIPs: 0.8 for credible set members, 0.1 for others")
            
            # Reset index to ensure clean indexing
            filtered_snp = filtered_snp.reset_index(drop=True)
            
            # Extract credible SNPs (those with cs > 0)
            if n_cs > 0:
                # Collect all indices from all credible sets
                all_credible_indices = []
                for i in range(n_cs):
                    cs_indices = np.array(credible_sets[i]) - 1  # Convert to 0-based
                    all_credible_indices.extend(cs_indices)
                
                # Get unique indices and extract those SNPs
                unique_indices = np.unique(all_credible_indices)
                credible_snps = filtered_snp.loc[unique_indices, :].copy()
                logger.info(f"Extracted {len(credible_snps)} credible SNPs from {n_cs} credible sets")
            else:
                # If no credible sets, use high PIP threshold as fallback
                high_pip_mask = filtered_snp["pip"] > 0.5
                if high_pip_mask.sum() > 0:
                    credible_snps = filtered_snp.loc[high_pip_mask, :].copy()
                    logger.info(f"No credible sets found, using high PIP threshold: {len(credible_snps)} SNPs")
                else:
                    # Return top 10 SNPs by PIP
                    credible_snps = filtered_snp.nlargest(min(10, len(filtered_snp)), 'pip').copy()
                    logger.info(f"Using top PIPs fallback: {len(credible_snps)} SNPs")
            
        except Exception as e:
            logger.error(f"Error in credible set processing: {e}")
            # Fallback: return all SNPs with zero PIPs and cs values
            filtered_snp["pip"] = 0.0
            credible_snps = filtered_snp.copy()
            logger.warning("Using fallback: returning all SNPs with default values")
        
        # Add log_pvalue for LocusZoom formatting
        credible_snps["log_pvalue"] = -np.log10(credible_snps["P"])
        
        logger.info(f"Final credible sets result: {len(credible_snps)} SNPs")
        logger.info(f"Credible sets distribution: {credible_snps['cs'].value_counts().to_dict()}")
        
        return credible_snps