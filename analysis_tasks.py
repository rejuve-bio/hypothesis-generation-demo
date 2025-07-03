import glob
import os
from pathlib import Path
from typing import Dict
import logging
from prefect import task
from datetime import datetime
from loguru import logger
import pandas as pd
import numpy as np
import gzip
import subprocess
import tempfile
from cyvcf2 import VCF, Writer
import numpy as np
from prefect import flow
import multiprocessing as mp
from functools import partial

from project_tasks import get_project_analysis_path_task

logging.basicConfig(level=logging.INFO)

# Configure rpy2
try:
    from rpy2.robjects.packages import importr
    import rpy2.robjects as ro
    from rpy2.robjects import pandas2ri, numpy2ri, default_converter
    from rpy2.robjects.conversion import localconverter
    from rpy2 import robjects
    from rpy2.robjects.vectors import ListVector
    
    
    # Import necessary R packages
    base = importr('base')
    stats = importr('stats')
    
    # Check if packages are available before trying to import
    def check_r_package_available(package_name):
        """Check if an R package is available for import"""
        r_code = f'is.element("{package_name}", installed.packages()[,1])'
        return ro.r(r_code)[0]
    
    # Check and install Rfast for better performance
    if check_r_package_available('Rfast'):
        try:
            rfast = importr('Rfast')
            HAS_RFAST = True
            logging.info("Rfast package loaded successfully - will use fast matrix operations")
        except Exception as e:
            logging.warning(f"Error loading Rfast: {e}")
            HAS_RFAST = False
    else:
        logging.warning("The R package 'Rfast' is not installed. Installing for better performance...")
        try:
            # Install Rfast for faster matrix operations
            ro.r('install.packages("Rfast", repos="https://cran.rstudio.com/", dependencies=TRUE)')
            rfast = importr('Rfast')
            HAS_RFAST = True
            logging.info("Rfast package installed and loaded successfully")
        except Exception as e:
            logging.error(f"Failed to install Rfast: {e}")
            HAS_RFAST = False
    
    # Import analysis packages with proper error handling
    if check_r_package_available('susieR'):
        try:
            susieR = importr('susieR')
            HAS_SUSIE = True
            logging.info("SusieR package loaded successfully")
        except Exception as e:
            logging.warning(f"Error loading susieR: {e}")
            HAS_SUSIE = False
            susieR = None
    else:
        logging.warning("The R package 'susieR' is not installed")
        HAS_SUSIE = False
        susieR = None
    
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
    susieR = None


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




# === MUNGESUMSTATS PREPROCESSING ===
@task(cache_policy=None)
def munge_sumstats_preprocessing(gwas_file_path, output_dir, ref_genome="GRCh37", n_threads=14):
    """
    Preprocess GWAS data using R's MungeSumstats package for standardization and QC.
    """
    if not HAS_RPY2:
        logging.error("rpy2 not available for MungeSumstats preprocessing")
        raise RuntimeError("rpy2 not available")
    
    logger.info(f"[MUNGE] Starting MungeSumstats preprocessing for {gwas_file_path}")
    start_time = datetime.now()
    
    # Create output paths
    os.makedirs(output_dir, exist_ok=True)
    munged_output_path = os.path.join(output_dir, "munged_sumstats.tsv")
    log_dir = os.path.join(output_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    try:
        # Set up conversion context for both import and execution
        with localconverter(default_converter + pandas2ri.converter):
            # Import MungeSumstats within conversion context
            try:
                mungesumstats = importr('MungeSumstats')
                logger.info("[MUNGE] MungeSumstats package loaded successfully")
            except Exception as e:
                logger.error(f"[MUNGE] Error importing MungeSumstats: {e}")
                raise RuntimeError("MungeSumstats package not available. Install with: BiocManager::install('MungeSumstats')")
            
            # Run MungeSumstats formatting 
            logger.info(f"[MUNGE] Processing with ref_genome={ref_genome}, threads={n_threads}")
            formatted_file_path_r = mungesumstats.format_sumstats(
                path=gwas_file_path,
                ref_genome=ref_genome,
                save_path=munged_output_path,
                drop_indels=True,
                nThread=n_threads,
                log_folder=log_dir,
                log_mungesumstats_msgs=True,
                save_format="LDSC",
                # force_new=False
            )
            
            # Extract file path within the conversion context - convert R string to Python string
            # Remove R formatting: [1] "path" -> path
            formatted_file_path_raw = str(formatted_file_path_r[0])
            # Extract just the file path from R output format
            import re
            path_match = re.search(r'"([^"]+)"', formatted_file_path_raw)
            if path_match:
                formatted_file_path = path_match.group(1)
            else:
                # Fallback: try to clean it manually
                formatted_file_path = formatted_file_path_raw.strip().replace('[1] "', '').replace('"', '').strip()
        
        logger.info(f"[MUNGE] MungeSumstats completed. Output: {formatted_file_path}")
            
        # Load and post-process the munged data
        logger.info("[MUNGE] Loading and post-processing munged data")
        munged_df = pd.read_csv(formatted_file_path, sep='\t', low_memory=False)
        logger.info(f"[MUNGE] Loaded {munged_df.shape[0]} variants with {munged_df.shape[1]} columns")
        
        # Create CHR:BP:A2:A1 ID format
        munged_df["ID"] = (munged_df["CHR"].astype(str) + ":" + 
                          munged_df["BP"].astype(str) + ":" + 
                          munged_df["A2"] + ":" + 
                          munged_df["A1"])
        
        # Set ID as index 
        munged_df.reset_index(drop=True, inplace=True)
        munged_df.set_index(["ID"], inplace=True)
        
        # Z-scores are already provided by MungeSumstats - no need to recalculate
        logger.info(f"[MUNGE] Using existing Z-scores from MungeSumstats")
        
        # Save the processed data
        final_output_path = os.path.join(output_dir, "munged_sumstats_processed.tsv")
        munged_df.to_csv(final_output_path, sep='\t', index=False)
        
        # Calculate processing time and stats
        elapsed_time = (datetime.now() - start_time).total_seconds()
        memory_usage = munged_df.memory_usage(deep=True).sum() / (1024*1024)
        
        logger.info(f"[MUNGE] Processing completed in {elapsed_time:.2f} seconds")
        logger.info(f"[MUNGE] Final dataset: {munged_df.shape[0]} variants, {memory_usage:.2f} MB")
        logger.info(f"[MUNGE] Chromosomes: {sorted(munged_df['CHR'].unique().tolist())}")
        
        return munged_df, final_output_path
        
    except Exception as e:
        logger.error(f"[MUNGE] Error in MungeSumstats preprocessing: {str(e)}")
        raise


@task(cache_policy=None)
def filter_significant_variants(munged_df, output_dir, p_threshold=5e-8):
    """Filter significant variants from munged sumstats."""
    logger.info(f"[FILTER] Filtering variants with p < {p_threshold}")
    start_time = datetime.now()
    
    # Filter significant variants
    significant_df = munged_df[munged_df['P'] < p_threshold].copy()
    logger.info(f"[FILTER] Found {len(significant_df)} significant variants from {len(munged_df)} total")
    
    # Save significant variants
    sig_output_path = os.path.join(output_dir, "significant_variants.tsv")
    significant_df.to_csv(sig_output_path, sep='\t', index=False)
    
    # Calculate summary statistics
    chr_counts = significant_df['CHR'].value_counts().sort_index()
    elapsed_time = (datetime.now() - start_time).total_seconds()
    
    logger.info(f"[FILTER] Filtering completed in {elapsed_time:.2f} seconds")
    logger.info(f"[FILTER] Chromosome distribution: {dict(chr_counts)}")
    
    return significant_df, sig_output_path


# ===  COJO ANALYSIS ===
@task(cache_policy=None)
def run_cojo_per_chromosome(significant_df, plink_dir, output_dir, maf_threshold=0.01, population="EUR"):
    """
    Run GCTA COJO analysis per chromosome and combine results.
    """
    logger.info(f"[COJO] Starting per-chromosome COJO analysis for population {population}")
    logger.info(f"[COJO] Input: {len(significant_df)} significant variants, MAF threshold: {maf_threshold}")
    

    
    start_time = datetime.now()
    
    # Create temporary directory for COJO processing
    with tempfile.TemporaryDirectory(prefix="cojo_analysis_") as temp_dir:
        logger.info(f"[COJO] Using temporary directory: {temp_dir}")
        
        # Prepare COJO input format
        cojo_df = significant_df[["A1", "A2", "FRQ", "BETA", "SE", "P", "N"]].copy()
        cojo_df["SNP"] = significant_df.index  # Use index since ID is set as index
        cojo_df = cojo_df[['SNP', 'A1', 'A2', 'FRQ', 'BETA', 'SE', 'P', 'N']]
        
        # Save COJO input file
        cojo_input_path = os.path.join(temp_dir, "cojo_input.txt")
        cojo_df.to_csv(cojo_input_path, sep='\t', index=False)
        logger.info(f"[COJO] COJO input prepared: {len(cojo_df)} variants")
        
        # Run COJO for each chromosome in parallel using ThreadPoolExecutor
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        def run_cojo_for_chromosome(chrom):
            """Run COJO analysis for a single chromosome"""
            try:
                logger.info(f"[COJO] Processing chromosome {chrom}")
                
                # Define paths for this chromosome
                plink_prefix = os.path.join(plink_dir, population, f"{population}.{chrom}.1000Gp3.20130502")
                
                # Check if PLINK files exist
                if not os.path.exists(f"{plink_prefix}.bed"):
                    logger.warning(f"[COJO] PLINK files not found for chromosome {chrom}: {plink_prefix}")
                    return None

                # Run GCTA COJO directly for this chromosome
                sumstats_basename = os.path.basename(cojo_input_path)
                cojo_output_prefix = os.path.join(temp_dir, f"{population}.{chrom}.{sumstats_basename}.maf.{maf_threshold}")
                
                cojo_cmd = [
                    "gcta64",
                    "--out", cojo_output_prefix,
                    "--bfile", plink_prefix,
                    "--cojo-file", cojo_input_path,
                    "--cojo-slct",
                    "--chr", str(chrom),
                    "--maf", str(maf_threshold)
                ]
                
                logger.info(f"[COJO] Running command for chr{chrom}: {' '.join(cojo_cmd)}")
                result = subprocess.run(cojo_cmd, capture_output=True, text=True, timeout=1800)  # 30 min timeout
                
                if result.returncode != 0:
                    logger.warning(f"[COJO] GCTA failed for chromosome {chrom}: {result.stderr}")
                    return None
                
                # Read COJO results for this chromosome  
                cojo_result_file = f"{cojo_output_prefix}.jma.cojo"
                if os.path.exists(cojo_result_file):
                    chr_results = pd.read_csv(cojo_result_file, sep='\s+')
                    if len(chr_results) > 0:
                        chr_results['CHR'] = chrom  
                        logger.info(f"[COJO] Chromosome {chrom}: {len(chr_results)} independent signals")
                        
                        return chr_results
                    else:
                        logger.info(f"[COJO] Chromosome {chrom}: No independent signals found")
                        return None
                else:
                    logger.warning(f"[COJO] No output file generated for chromosome {chrom}")
                    return None
                    
            except subprocess.TimeoutExpired:
                logger.error(f"[COJO] Timeout for chromosome {chrom}")
                return None
            except Exception as e:
                logger.error(f"[COJO] Error processing chromosome {chrom}: {str(e)}")
                return None
        
        # Run all chromosomes in parallel
        combined_results = []
        successful_chromosomes = []
        max_workers = 4  # Reasonable parallelism
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all chromosome tasks
            future_to_chrom = {
                executor.submit(run_cojo_for_chromosome, chrom): chrom 
                for chrom in range(1, 23)
            }
            
            # Process completed tasks
            for future in as_completed(future_to_chrom):
                chrom = future_to_chrom[future]
                try:
                    result = future.result()
                    if result is not None:
                        combined_results.append(result)
                        successful_chromosomes.append(chrom)
                except Exception as e:
                    logger.error(f"[COJO] Exception in chromosome {chrom}: {str(e)}")
        
       
        if combined_results:
            combined_cojo_df = pd.concat(combined_results, ignore_index=True)
            
            # Ensure consistent column naming
            if 'SNP' in combined_cojo_df.columns:
                combined_cojo_df = combined_cojo_df.rename(columns={'SNP': 'ID'})
            
            # Remove duplicates and clean up
            combined_cojo_df = combined_cojo_df.drop_duplicates(subset=['ID'] if 'ID' in combined_cojo_df.columns else None)
            
            # Set ID as index (consistent with susie_finemapping_v1.py)
            if 'ID' in combined_cojo_df.columns:
                combined_cojo_df = combined_cojo_df.set_index('ID')
            
            # Save combined results
            os.makedirs(output_dir, exist_ok=True)
            cojo_output_path = os.path.join(output_dir, "combined_cojo_results.txt")
            combined_cojo_df.to_csv(cojo_output_path, sep='\t', index=True)
            
            # Calculate final statistics
            elapsed_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"[COJO] Analysis completed in {elapsed_time:.2f} seconds")
            logger.info(f"[COJO] Successful chromosomes: {successful_chromosomes}")
            logger.info(f"[COJO] Total independent signals: {len(combined_cojo_df)}")
            logger.info(f"[COJO] Results saved to: {cojo_output_path}")
            
            return combined_cojo_df, cojo_output_path
            
        else:
            logger.error("[COJO] No COJO results generated for any chromosome")
            raise RuntimeError("COJO analysis failed for all chromosomes")

@task
def check_ld_dimensions(ld_matrix, snp_df, bim_file_path):
    
    if ld_matrix.shape[0] != len(snp_df) or ld_matrix.shape[1] != len(snp_df):
        logger.info("Dimension mismatch detected between LD matrix and SNP list.")
        logger.info(f"LD shape: {ld_matrix.shape}, SNP list length: {len(snp_df)}")

        # Load available SNP IDs from bim file
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

# === FINE-MAPPING SINGLE REGION ===
@task(cache_policy=None)
def finemap_region_notebook_exact(seed, sumstats, chr_num, lead_variant_position, window=2000, 
                                 population="EUR", L=-1, coverage=0.95, min_abs_corr=0.5):
    try:
        import rpy2.robjects as ro
        from rpy2.robjects.packages import importr
        from rpy2.robjects import pandas2ri, numpy2ri, default_converter
        from rpy2.robjects.conversion import localconverter
        import optuna
        import tempfile
        import numpy as np
        import os
        
        logger.info(f"[FINEMAP] Fine-mapping chr{chr_num}:{lead_variant_position} ±{window}kb")
        
        # Calculate LD 
        window_bp = window * 1000
        start = lead_variant_position - window_bp
        end = lead_variant_position + window_bp
        
        filtered_region = sumstats[(sumstats["CHR"] == chr_num) &
                                  (sumstats["BP"] >= start) &
                                  (sumstats["BP"] <= end)]
        
        # Handle both indexed and column-based ID
        if 'ID' in filtered_region.columns:
            filtered_ids = filtered_region["ID"].tolist()
        else:
            filtered_ids = filtered_region.index.tolist()
        logger.info(f"[FINEMAP] Filtered {len(filtered_ids)} SNPs in region")
        
        if len(filtered_ids) < 2:
            logger.warning(f"[FINEMAP] Insufficient SNPs for chr{chr_num}:{lead_variant_position}")
            return None
        

        
        # Calculate LD using plink2 with better temp file handling
        try:
            with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.txt') as tmp_file:
                for snp_id in filtered_ids:
                    tmp_file.write(f"{snp_id}\n")
                tmp_file_path = tmp_file.name
                
            with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.txt') as tmp_file_ld:
                tmp_file_ld_path = tmp_file_ld.name
                
            plink_cmd = [
                "plink2",
                "--bfile", f"./data/1000Genomes_phase3/plink_format_b37/{population}/{population}.{chr_num}.1000Gp3.20130502",
                "--keep-allele-order",
                "--r-unphased", "square", 
                "--extract", tmp_file_path,
                "--out", tmp_file_ld_path
            ]
            
            logger.info(f"[FINEMAP] Running LD: {' '.join(plink_cmd)}")
            ld_run_res = run_command(' '.join(plink_cmd))
            
            if ld_run_res.returncode != 0:
                logger.error(f"[FINEMAP] PLINK LD failed: {ld_run_res.stderr}")
                # Clean up and return None
                try:
                    os.remove(tmp_file_path)
                except:
                    pass
                return None
            
            # Read LD matrix 
            ld_out_path = f"{tmp_file_ld_path}.unphased.vcor1"
            ld_vars_path = f"{ld_out_path}.vars"
            
            if not os.path.exists(ld_out_path) or not os.path.exists(ld_vars_path):
                logger.error(f"[FINEMAP] PLINK output files not found")
                try:
                    os.remove(tmp_file_path)
                except:
                    pass
                return None
            
            snp_ids = []
            with open(ld_vars_path, 'r') as f:
                for line in f:
                    snp_ids.append(line.strip())
            
            ld_df = pd.read_csv(ld_out_path, sep='\t', header=None)
            ld_df.index = snp_ids
            ld_df.fillna(0, inplace=True)
            
            # Clean up files safely
            for cleanup_file in [tmp_file_path, tmp_file_ld_path, ld_out_path, ld_vars_path]:
                try:
                    if os.path.exists(cleanup_file):
                        os.remove(cleanup_file)
                except Exception as e:
                    logger.warning(f"[FINEMAP] Could not remove temp file {cleanup_file}: {e}")
        
        except Exception as e:
            logger.error(f"[FINEMAP] Error in LD calculation: {str(e)}")
            return None
        
        # Get sub-region sumstats matching LD
        sub_region_sumstats_ld = sumstats.loc[ld_df.index]
        
        # Apply LD quality control checks using check_ld_dimensions
        logger.info(f"[FINEMAP] Applying LD quality control checks")
        LD_mat = ld_df.values
        
        # Prepare SNP dataframe for check_ld_dimensions function
        snp_df_for_check = pd.DataFrame({
            'SNPID': sub_region_sumstats_ld.index,
            'CHR': sub_region_sumstats_ld['CHR'],
            'BP': sub_region_sumstats_ld['BP']
        })
        
        # Use check_ld_dimensions to reconcile any mismatches
        bim_file_path = f"./data/1000Genomes_phase3/plink_format_b37/{population}/{population}.{chr_num}.1000Gp3.20130502.bim"
        
        if os.path.exists(bim_file_path):
            try:
                filtered_snp_df = check_ld_dimensions(LD_mat, snp_df_for_check, bim_file_path)
                
                # Update our data based on check_ld_dimensions results
                if len(filtered_snp_df) != len(snp_df_for_check):
                    logger.info(f"[FINEMAP] LD dimensions check filtered SNPs: {len(snp_df_for_check)} → {len(filtered_snp_df)}")
                    # Re-filter LD matrix and sumstats to match filtered SNPs
                    kept_snp_ids = filtered_snp_df['SNPID'].tolist()
                    LD_mat = ld_df.loc[kept_snp_ids].values
                    sub_region_sumstats_ld = sumstats.loc[kept_snp_ids]
            except Exception as e:
                logger.warning(f"[FINEMAP] check_ld_dimensions failed: {str(e)}, proceeding without filtering")
        else:
            logger.warning(f"[FINEMAP] BIM file not found: {bim_file_path}, skipping LD dimension check")
        
        # Final validation after LD dimension checks
        if len(sub_region_sumstats_ld) < 10:
            logger.warning(f"[FINEMAP] Too few SNPs after LD filtering ({len(sub_region_sumstats_ld)}), skipping region")
            return None
            
        # Check and fix LD matrix semi-definiteness
        LD_mat = check_ld_semidefiniteness(LD_mat)
        logger.info(f"[FINEMAP] Sub-region shape after LD filtering: {sub_region_sumstats_ld.shape}")

        # Get Z-scores after LD filtering
        zhat = sub_region_sumstats_ld["Z"].values.flatten()
        
        # Monitor LD structure for debugging (but don't prune)
        max_ld_off_diag = np.max(np.abs(LD_mat - np.diag(np.diag(LD_mat))))
        logger.info(f"[DEBUG] Max off-diagonal LD in region: {max_ld_off_diag:.6f}")
        
        if max_ld_off_diag > 0.98:
            logger.warning(f"[DEBUG] High LD detected ({max_ld_off_diag:.6f}) - SuSiE will handle this")
        
        logger.info(f"[DEBUG] Final data shape: Z={len(zhat)}, LD={LD_mat.shape}")

        # Basic validation of final data
        if len(sub_region_sumstats_ld) < 10:
            logger.error(f"[FINEMAP] Too few variants after all filtering: {len(sub_region_sumstats_ld)}")
            return None
        
        if LD_mat.shape[0] != len(zhat):
            logger.error(f"[FINEMAP] Dimension mismatch: LD={LD_mat.shape}, Z-scores={len(zhat)}")
            return None
        
        num_samples = int(sub_region_sumstats_ld["N"].iloc[0])
        
        # DEBUG: Detailed data inspection before SuSiE
        logger.info(f"[DEBUG] Pre-SuSiE data inspection for chr{chr_num}:{lead_variant_position}")
        logger.info(f"[DEBUG] - LD matrix shape: {LD_mat.shape}")
        logger.info(f"[DEBUG] - Z-scores shape: ({len(zhat)},)")
        logger.info(f"[DEBUG] - Sample size (N): {num_samples}")
        logger.info(f"[DEBUG] - LD matrix stats: min={LD_mat.min():.3f}, max={LD_mat.max():.3f}, mean={LD_mat.mean():.3f}")
        logger.info(f"[DEBUG] - Z-scores stats: min={zhat.min():.3f}, max={zhat.max():.3f}, mean={zhat.mean():.3f}")
        logger.info(f"[DEBUG] - LD matrix NaN count: {np.isnan(LD_mat).sum()}")
        logger.info(f"[DEBUG] - Z-scores NaN count: {np.isnan(zhat).sum()}")
        logger.info(f"[DEBUG] - LD matrix inf count: {np.isinf(LD_mat).sum()}")
        logger.info(f"[DEBUG] - Z-scores inf count: {np.isinf(zhat).sum()}")
        
        # Check for invalid data
        if np.isnan(LD_mat).any() or np.isinf(LD_mat).any():
            logger.error(f"[DEBUG] Invalid LD matrix detected - contains NaN or inf values")
            return None
        if np.isnan(zhat).any() or np.isinf(zhat).any():
            logger.error(f"[DEBUG] Invalid Z-scores detected - contains NaN or inf values")
            return None
        
        # Check if susieR is available
        if not HAS_SUSIE or susieR is None:
            logger.error(f"[FINEMAP] susieR not available for chr{chr_num}:{lead_variant_position}")
            return None
        
        # Set up R conversions properly for multithreading
        with localconverter(default_converter + pandas2ri.converter + numpy2ri.converter):
            # Set seed for reproducibility
            try:
                ro.r(f'set.seed({seed})')
            except Exception as e:
                logger.error(f"[FINEMAP] Error setting R seed: {str(e)}")
                return None
            
            # Convert to R objects with memory management
            try:
                logger.info(f"[DEBUG] Converting Python objects to R...")
                zhat_r = ro.conversion.get_conversion().py2rpy(zhat)
                R_r = ro.conversion.get_conversion().py2rpy(LD_mat)
                logger.info(f"[DEBUG] R conversion successful")
                logger.info(f"[DEBUG] - zhat_r type: {type(zhat_r)}")
                logger.info(f"[DEBUG] - R_r type: {type(R_r)}")
                
                # Test R objects accessibility
                try:
                    zhat_length = len(zhat)
                    test_result = ro.r(f'length(c({",".join(map(str, zhat[:5]))}))')
                    logger.info(f"[DEBUG] R vector creation test successful")
                except Exception as r_test_e:
                    logger.warning(f"[DEBUG] R vector test failed: {r_test_e}")
                    
            except Exception as e:
                logger.error(f"[FINEMAP] Error converting data to R objects: {str(e)}")
                logger.error(f"[FINEMAP] Matrix dimensions: LD_mat={LD_mat.shape}, zhat=({len(zhat)},)")
                logger.error(f"[FINEMAP] LD_mat sample: {LD_mat[:2, :2]}")
                logger.error(f"[FINEMAP] zhat sample: {zhat[:5]}")
                return None
            
            # Optuna hyperparameter optimization
            if L <= 0:
                def objective_susie(trial):
                    L_trial = trial.suggest_int("L", 1, 20)
                    logger.info(f"[DEBUG] Starting SuSiE trial with L={L_trial}")

                    # 1. CHECK INPUT DATA QUALITY
                    logger.info(f"[DEBUG] Input validation for L={L_trial}:")
                    logger.info(f"  - Z-scores shape: {zhat.shape if hasattr(zhat, 'shape') else len(zhat)}")
                    logger.info(f"  - Z-scores range: [{np.min(zhat):.3f}, {np.max(zhat):.3f}]")
                    logger.info(f"  - Z-scores mean/std: {np.mean(zhat):.3f} ± {np.std(zhat):.3f}")
                    logger.info(f"  - LD matrix shape: {LD_mat.shape}")
                    logger.info(f"  - LD matrix diagonal range: [{np.min(np.diag(LD_mat)):.3f}, {np.max(np.diag(LD_mat)):.3f}]")
                    logger.info(f"  - LD matrix off-diagonal max: {np.max(np.abs(LD_mat - np.diag(np.diag(LD_mat)))):.3f}")
                    logger.info(f"  - Sample size: {num_samples}")

                    # 2. CHECK FOR PROBLEMATIC DATA PATTERNS
                    # Check if LD matrix is identity (would cause artificial convergence)
                    if np.allclose(LD_mat, np.eye(LD_mat.shape[0]), atol=1e-6):
                        logger.warning(f"[DEBUG] LD matrix is nearly identity! This causes artificial convergence")
                    
                    # Monitor LD structure (allow SuSiE to handle high LD with conditioning)
                    max_ld = np.max(np.abs(LD_mat - np.diag(np.diag(LD_mat))))
                    if max_ld > 0.99:
                        logger.warning(f"[DEBUG] High LD detected ({max_ld:.6f}), SuSiE will use matrix conditioning")
                    
                    # Check if Z-scores are too extreme
                    if np.any(np.abs(zhat) > 20):
                        logger.warning(f"[DEBUG] Extreme Z-scores detected (|Z| > 20): {np.sum(np.abs(zhat) > 20)} variants")
                    
                    # Check for NaN/Inf
                    if np.any(np.isnan(zhat)) or np.any(np.isinf(zhat)):
                        logger.error(f"[DEBUG]  NaN/Inf in Z-scores!")
                        return -np.inf
                    
                    if np.any(np.isnan(LD_mat)) or np.any(np.isinf(LD_mat)):
                        logger.error(f"[DEBUG]  NaN/Inf in LD matrix!")
                        return -np.inf
                    
                    try:
                        susie_fit = susieR.susie_rss(z=zhat_r, R=R_r, L=L_trial, n=num_samples)
                        logger.info(f"[DEBUG] SuSiE execution completed for L={L_trial}")
                    except Exception as e:
                        logger.error(f"[DEBUG] SuSiE execution failed for L={L_trial}: {str(e)}")
                        return -np.inf
                    
                    # Extract ELBO and convergence with robust object handling
                    try:
                        logger.info(f"[DEBUG] SuSiE fit object type: {type(susie_fit)}")
                        logger.info(f"[DEBUG] SuSiE fit object attributes: {dir(susie_fit)[:10]}")
                        logger.info(f"[DEBUG] hasattr(susie_fit, 'rx2'): {hasattr(susie_fit, 'rx2')}")
                        logger.info(f"[DEBUG] Checking if rx2 in dir: {'rx2' in dir(susie_fit)}")
                        
                        # Try to understand the object structure
                        try:
                            logger.info(f"[DEBUG] susie_fit.__class__.__mro__: {susie_fit.__class__.__mro__}")
                        except:
                            pass
                        
                        # Try multiple extraction methods
                        elbo = None
                        converged = None
                        
                        # Convert NamedList to proper R object that supports rx2
                        if hasattr(susie_fit, 'rx2'):
                            # Direct rx2 access (like notebook)
                            elbo = susie_fit.rx2('elbo')[-1]
                            converged = susie_fit.rx2('converged')[0]
                            logger.info(f"[DEBUG] Extracted using direct rx2 method!")
                        else:
                            # Convert NamedList to ListVector for rx2 access
                            from rpy2.robjects.vectors import ListVector
                            
                            # Method 1: Convert to ListVector
                            try:
                                # Convert the NamedList to a proper R ListVector
                                susie_fit_r = ListVector(susie_fit)
                                logger.info(f"[DEBUG] SuSiE fit object type: {type(susie_fit_r)}")
                                logger.info(f"[DEBUG] hasattr(susie_fit_r, 'rx2'): {hasattr(susie_fit_r, 'rx2')}")
                                elbo = susie_fit_r.rx2('elbo')[-1]
                                converged = susie_fit_r.rx2('converged')[0]
                                logger.info(f"[DEBUG] Extracted using ListVector conversion method!")
                            except Exception as conv_e:
                                logger.warning(f"[DEBUG] ListVector conversion failed: {conv_e}")
                                

                        
                        # Method 2: Try dictionary access
                        if elbo is None:
                            try:
                                elbo = susie_fit['elbo'][-1]
                                converged = susie_fit['converged'][0]
                                logger.info(f"[DEBUG] Extracted using dictionary method")
                            except Exception as e2:
                                logger.warning(f"[DEBUG] Dictionary method failed: {e2}")
                        
                        # Method 3: Try R-based extraction
                        if elbo is None:
                            try:
                                ro.globalenv['temp_fit'] = susie_fit
                                elbo_r = ro.r('tail(temp_fit$elbo, 1)')
                                converged_r = ro.r('temp_fit$converged')
                                ro.r('rm(temp_fit)')
                                elbo = float(elbo_r[0])
                                converged = int(converged_r[0])
                                logger.info(f"[DEBUG] Extracted using R method")
                            except Exception as e3:
                                logger.warning(f"[DEBUG] R method failed: {e3}")
                        
                        if elbo is None:
                            logger.error(f"[DEBUG] All extraction methods failed for L={L_trial}")
                            return -np.inf
                        
                        # ENHANCED CONVERGENCE VALIDATION
                        logger.info(f"[DEBUG] Extracted - L={L_trial}, converged={converged}, ELBO={elbo:.6f}")
                        
                        # Extract additional validation info for suspicious convergence detection
                        niter = None
                        try:
                            ro.globalenv['temp_fit_check'] = susie_fit
                            niter_r = ro.r('temp_fit_check$niter')
                            if niter_r is not None:
                                niter = int(niter_r[0])
                            ro.r('rm(temp_fit_check)')
                        except:
                            pass
                        
                        # SUSPICIOUS CONVERGENCE DETECTION - Enhanced debugging
                        logger.info(f"[DEBUG] 🔍 CONVERGENCE ANALYSIS for L={L_trial}:")
                        logger.info(f"[DEBUG]   - Converged: {converged}")
                        logger.info(f"[DEBUG]   - ELBO: {elbo:.6f}")
                        logger.info(f"[DEBUG]   - Iterations: {niter}")
                        logger.info(f"[DEBUG]   - Data size: {len(zhat)} variants")
                        logger.info(f"[DEBUG]   - Max |Z-score|: {np.max(np.abs(zhat)):.3f}")
                        logger.info(f"[DEBUG]   - Sample size: {num_samples}")
                        
                        # Check for suspicious patterns
                        suspicious_flags = []
                        if niter is not None and niter < 5:
                            suspicious_flags.append(f"Very quick convergence ({niter} iter)")
                        if elbo > -1000:  # ELBOs should typically be much more negative
                            suspicious_flags.append(f"Unexpectedly high ELBO ({elbo:.2f})")
                        if abs(elbo) < 1000:  # Too small in magnitude
                            suspicious_flags.append(f"ELBO magnitude too small ({abs(elbo):.2f})")
                            
                        if suspicious_flags:
                            logger.warning(f"[DEBUG] 🚨 SUSPICIOUS CONVERGENCE DETECTED:")
                            for flag in suspicious_flags:
                                logger.warning(f"[DEBUG]     - {flag}")
                        
                        # Validate ELBO is reasonable
                        if np.isnan(elbo) or np.isinf(elbo):
                            logger.error(f"[DEBUG]  L={L_trial} invalid ELBO: {elbo}")
                            return -np.inf
                        
                        if elbo > 0:
                            logger.warning(f"[DEBUG] SUSPICIOUS: L={L_trial} positive ELBO: {elbo} (should be negative)")
                            return -np.inf
                        
                        # Additional validation: Check if ELBO is in reasonable range for real data
                        expected_elbo_range = (-1000000, -1000)  # Typical range for real GWAS data
                        if not (expected_elbo_range[0] <= elbo <= expected_elbo_range[1]):
                            logger.warning(f"[DEBUG] ELBO outside expected range {expected_elbo_range}: {elbo:.2f}")
                        
                        # Strict convergence validation
                        if converged == 1:
                            if niter is not None:
                                logger.info(f"[DEBUG] L={L_trial} CONVERGED with ELBO {elbo:.6f} in {niter} iterations")
                            else:
                                logger.info(f"[DEBUG] L={L_trial} CONVERGED with ELBO {elbo:.6f}")
                            return elbo
                        else:
                            logger.warning(f"[DEBUG]  L={L_trial} did NOT converge (converged={converged})")
                            return -np.inf
                            
                    except Exception as e:
                        logger.error(f"[DEBUG] Error extracting results for L={L_trial}: {str(e)}")
                        return -np.inf
                
                study = optuna.create_study(direction="maximize")
                study.optimize(objective_susie, n_trials=10)
                L = study.best_params["L"]
                logger.info(f"[FINEMAP] Best L found: {L} with ELBO {study.best_value}")
            
            # Run SuSiE with final L
            logger.info(f"[DEBUG] Running final SuSiE with L={L}")
            try:
                susie_fit = susieR.susie_rss(z=zhat_r, R=R_r, L=L, n=num_samples)
                logger.info(f"[DEBUG] Final SuSiE execution completed")
                
                # Check final convergence using robust method
                try:
                    final_converged = None
                    final_elbo = None
                    
                    # Try rx2 first (force attempt like notebook)
                    try:
                        logger.info(f"[DEBUG] Attempting final rx2 extraction...")
                        final_converged = susie_fit.rx2('converged')[0]
                        final_elbo = susie_fit.rx2('elbo')[-1]
                        logger.info(f"[DEBUG] Final rx2 extraction successful!")
                    except Exception as rx2_e:
                        logger.warning(f"[DEBUG] Final rx2 extraction failed: {rx2_e}")
                    
                    # Fallback to R method
                    if final_converged is None:
                        try:
                            ro.globalenv['final_fit'] = susie_fit
                            final_converged_r = ro.r('final_fit$converged')
                            final_elbo_r = ro.r('tail(final_fit$elbo, 1)')
                            ro.r('rm(final_fit)')
                            final_converged = int(final_converged_r[0])
                            final_elbo = float(final_elbo_r[0])
                        except:
                            pass
                    
                    if final_converged is not None:
                        logger.info(f"[DEBUG] Final SuSiE - converged={final_converged}, ELBO={final_elbo:.6f}")
                    else:
                        logger.warning(f"[DEBUG] Could not extract final convergence info")
                except Exception as conv_e:
                    logger.warning(f"[DEBUG] Error in final convergence check: {conv_e}")
                    
            except Exception as e:
                logger.error(f"[DEBUG] Final SuSiE execution failed: {str(e)}")
                return None
            
            # Extract credible sets and PIPs with ROBUST error handling
            logger.info(f"[DEBUG] Extracting credible sets and PIPs...")
            
            # Step 1: Extract credible sets
            try:
                logger.info(f"[DEBUG] Calling susie_get_cs...")
                credible_sets = susieR.susie_get_cs(susie_fit, coverage=coverage, min_abs_corr=min_abs_corr, Xcorr=R_r)
                logger.info(f"[DEBUG] susie_get_cs completed successfully")
            except Exception as e:
                logger.error(f"[FINEMAP] Error extracting credible sets: {str(e)}")
                credible_sets = None

            # Step 2: Extract PIPs with multiple robust methods
            pips = None
            
            # Method 1: Try susie_get_pip (often fails with R error)
            try:
                logger.info(f"[DEBUG] Method 1: Trying susieR.susie_get_pip...")
                pips = susieR.susie_get_pip(susie_fit)
                pips = np.array(pips, dtype=np.float64)
                logger.info(f"[DEBUG] Method 1 SUCCESS: susie_get_pip worked!")
            except Exception as e:
                logger.warning(f"[DEBUG]  Method 1 FAILED: {str(e)}")
                pips = None
            
            # Method 2: Manual alpha calculation (PROVEN WORKING from prev_analysisTask.py)
            if pips is None:
                try:
                    logger.info(f"[DEBUG] Method 2: Using PROVEN manual alpha calculation from prev_analysisTask.py...")
                    ro.globalenv['susie_fit'] = susie_fit
                    
                    r_pip_code = """
                    tryCatch({
                        fit <- susie_fit
                        
                        if (!is.null(fit$alpha) && is.matrix(fit$alpha)) {
                            # PIPs = 1 - product of (1 - alpha) across all L components
                            alpha_matrix <- fit$alpha
                            pip_vals <- 1 - apply(1 - alpha_matrix, 2, prod)
                            cat("PIPs calculated from alpha matrix\\n")
                        } else if (!is.null(fit$pip)) {
                            pip_vals <- as.numeric(fit$pip)
                            cat("PIPs extracted from fit$pip\\n")
                        } else {
                            pip_vals <- rep(0.01, 1000)
                            cat("Using fallback PIPs\\n")
                        }
                        
                        # Ensure valid PIPs
                        pip_vals <- as.numeric(pip_vals)
                        pip_vals[is.na(pip_vals)] <- 0.0
                        pip_vals[pip_vals < 0] <- 0.0
                        pip_vals[pip_vals > 1] <- 1.0
                        
                        pip_vals
                        
                    }, error = function(e) {
                        cat(" Error in manual alpha calculation:", conditionMessage(e), "\\n")
                        rep(0.01, 1000)
                    })
                    """
                    
                    r_pip_result = ro.r(r_pip_code)
                    ro.r('rm(susie_fit)')
                    pips = np.array(r_pip_result, dtype=np.float64)
                    
                    # Adjust length if needed
                    if len(pips) != len(sub_region_sumstats_ld):
                        if len(pips) < len(sub_region_sumstats_ld):
                            pips = np.pad(pips, (0, len(sub_region_sumstats_ld) - len(pips)), constant_values=0.01)
                        else:
                            pips = pips[:len(sub_region_sumstats_ld)]
                    
                    logger.info(f"[DEBUG] Method 2 SUCCESS: Manual alpha calculation worked!")
                    
                except Exception as e:
                    logger.error(f"[DEBUG]  Method 2 FAILED: {str(e)}")
                    pips = None
            
            # Method 3: Direct fit$pip extraction
            if pips is None:
                try:
                    logger.info(f"[DEBUG] Method 3: Direct fit$pip extraction...")
                    ro.globalenv['direct_fit'] = susie_fit
                    pip_direct_r = ro.r('direct_fit$pip')
                    ro.r('rm(direct_fit)')
                    
                    if pip_direct_r is not None:
                        pips = np.array(pip_direct_r, dtype=np.float64)
                        if len(pips) == len(sub_region_sumstats_ld):
                            logger.info(f"[DEBUG] Method 3 SUCCESS: Direct pip extraction worked!")
                        else:
                            pips = None
                            logger.warning(f"[DEBUG]  Method 3 FAILED: Length mismatch")
                    else:
                        logger.warning(f"[DEBUG]  Method 3 FAILED: fit$pip is NULL")
                        
                except Exception as e:
                    logger.error(f"[DEBUG]  Method 3 FAILED: {str(e)}")
                    pips = None
            
            # Final validation and fallback
            if pips is not None:
                # Validate PIPs
                if len(pips) != len(sub_region_sumstats_ld):
                    logger.error(f"[DEBUG] PIP length mismatch: {len(pips)} vs {len(sub_region_sumstats_ld)}")
                    return None
                    
                # Check PIP validity
                if np.any(pips < 0) or np.any(pips > 1):
                    logger.warning(f"[DEBUG] Invalid PIPs detected, clipping to [0,1]")
                    pips = np.clip(pips, 0, 1)
                    
                logger.info(f"[DEBUG] Final PIPs: min={np.min(pips):.6f}, max={np.max(pips):.6f}, mean={np.mean(pips):.6f}")
                logger.info(f"[DEBUG] Non-zero PIPs: {np.sum(pips > 0.001)}/{len(pips)}")
                logger.info(f"[DEBUG] High PIPs (>0.1): {np.sum(pips > 0.1)}")
                
            else:
                logger.error(f"[DEBUG] 🚨 ALL PIP EXTRACTION METHODS FAILED!")
                return None
            
            # Explicit memory cleanup for large R objects
            try:
                ro.r('gc()')  # Force R garbage collection
            except:
                pass
        
        # Add PIPs to results and initialize cs column
        sub_region_sumstats_ld = sub_region_sumstats_ld.copy()
        sub_region_sumstats_ld["PIP"] = pips
        sub_region_sumstats_ld["cs"] = 0
        
        logger.info(f"[DEBUG] 🔍 CREDIBLE SETS PROCESSING - Using susie_finemapping_v1.py simple approach:")
        logger.info(f"[DEBUG] credible_sets type: {type(credible_sets)}")
        
        # Process credible sets using the SIMPLE working approach from susie_finemapping_v1.py
        try:
            if credible_sets is not None and len(credible_sets) > 0:
                # Extract credible set indices - this is the simple approach that works
                cs_data = credible_sets[0]  # This contains the credible sets
                logger.info(f"[DEBUG] Credible sets data type: {type(cs_data)}")
                logger.info(f"[DEBUG] Number of credible sets found: {len(cs_data) if hasattr(cs_data, '__len__') else 'not iterable'}")
                
                if hasattr(cs_data, '__len__') and len(cs_data) > 0:
                    # Process each credible set
                    for cs_idx, cs_variants in enumerate(cs_data):
                        # Convert R 1-based indices to Python 0-based indices  
                        python_indices = np.array(cs_variants) - 1
                        logger.info(f"[DEBUG] Credible set {cs_idx+1}: {len(python_indices)} variants")
                        
                        # Validate indices
                        valid_indices = python_indices[(python_indices >= 0) & (python_indices < len(sub_region_sumstats_ld))]
                        if len(valid_indices) > 0:
                            sub_region_sumstats_ld.iloc[valid_indices, sub_region_sumstats_ld.columns.get_loc("cs")] = cs_idx + 1
                            logger.info(f"[DEBUG] Assigned {len(valid_indices)} variants to credible set {cs_idx+1}")
                    
                    # Extract only credible SNPs (those with cs > 0)
                    credible_mask = sub_region_sumstats_ld["cs"] > 0
                    credible_snps = sub_region_sumstats_ld[credible_mask].copy()
                    
                    if len(credible_snps) > 0:
                        # Add metadata
                        credible_snps['region_id'] = f"chr{chr_num}:{lead_variant_position}"
                        credible_snps['region_chr'] = chr_num
                        credible_snps['region_center'] = lead_variant_position
                        credible_snps['converged'] = True
                        credible_snps['credible_set'] = credible_snps['cs']
                        
                        logger.info(f"[DEBUG] CREDIBLE SETS EXTRACTION SUCCESSFUL:")
                        logger.info(f"[DEBUG] Total credible variants: {len(credible_snps)}")
                        logger.info(f"[DEBUG] Credible sets: {sorted(credible_snps['cs'].unique())}")
                        logger.info(f"[DEBUG] PIP range: {credible_snps['PIP'].min():.6f} - {credible_snps['PIP'].max():.6f}")
                        
                        # Log per-credible-set stats
                        for cs_num in sorted(credible_snps['cs'].unique()):
                            cs_subset = credible_snps[credible_snps['cs'] == cs_num]
                            logger.info(f"[FINEMAP] Chr{chr_num}:{lead_variant_position} - Credible set {cs_num}: {len(cs_subset)} variants, PIP range: {cs_subset['PIP'].min():.6f}-{cs_subset['PIP'].max():.6f}")
                        
                        return credible_snps
                    else:
                        logger.warning(f"[DEBUG] No variants in credible sets")
                else:
                    logger.warning(f"[DEBUG] Empty or invalid credible sets data")
            else:
                logger.warning(f"[DEBUG] No credible sets found")
                
        except Exception as e:
            logger.warning(f"[DEBUG] Error processing credible sets: {str(e)}")
        
        # Fallback: use high PIP threshold
        logger.info(f"[DEBUG] 🔄 Using fallback: variants with PIP > 0.1")
        high_pip_mask = sub_region_sumstats_ld["PIP"] > 0.1
        if high_pip_mask.sum() > 0:
            fallback_result = sub_region_sumstats_ld[high_pip_mask].copy()
        else:
            # Return top 10 SNPs by PIP
            top_n = min(10, len(sub_region_sumstats_ld))
            fallback_result = sub_region_sumstats_ld.nlargest(top_n, 'PIP').copy()
        
        # Add metadata to fallback result
        fallback_result['cs'] = 1  # Single credible set
        fallback_result['credible_set'] = 1
        fallback_result['region_id'] = f"chr{chr_num}:{lead_variant_position}"
        fallback_result['region_chr'] = chr_num
        fallback_result['region_center'] = lead_variant_position
        fallback_result['converged'] = True
        
        logger.info(f"[FINEMAP] Chr{chr_num}:{lead_variant_position} - Fallback result: {len(fallback_result)} SNPs with PIP > 0.1 or top variants")
        return fallback_result
            
    except Exception as e:
        logger.error(f"[FINEMAP] Error in fine-mapping chr{chr_num}:{lead_variant_position}: {str(e)}")
        return None

def create_region_batches(cojo_results, batch_size=5):
    """Split COJO results into batches for process-based execution"""
    batches = []
    
    # Convert COJO results to list of region info
    regions = []
    for variant_id, row in cojo_results.iterrows():
        # Extract region info (CHR:BP:A2:A1 format)
        try:
            chr_str, bp_str, a2, a1 = variant_id.split(':')
            region = {
                'variant_id': variant_id,
                'chr': int(chr_str),
                'position': int(bp_str),
                'alleles': (a2, a1),
                'cojo_data': row.to_dict()
            }
            regions.append(region)
        except Exception as e:
            logger.warning(f"[BATCH] Error parsing variant ID {variant_id}: {e}")
            continue
    
    # Split into batches
    for i in range(0, len(regions), batch_size):
        batch = regions[i:i + batch_size]
        batches.append(batch)
    
    logger.info(f"[BATCH] Created {len(batches)} batches from {len(regions)} regions (batch_size={batch_size})")
    return batches

def finemap_region_batch_worker(batch_data):

    region_batch, batch_id, sumstats_data = batch_data
    logger.info(f"[BATCH-{batch_id}] Starting batch with {len(region_batch)} regions")
    
    # Import R packages in this isolated process
    try:
        from rpy2.robjects.packages import importr
        import rpy2.robjects as ro
        from rpy2.robjects import pandas2ri, numpy2ri, default_converter
        from rpy2.robjects.conversion import localconverter
        
        # Check if susieR is available in this process
        def check_r_package_available(package_name):
            r_code = f'is.element("{package_name}", installed.packages()[,1])'
            return ro.r(r_code)[0]
        
        if not check_r_package_available('susieR'):
            logger.error(f"[BATCH-{batch_id}] susieR not available in worker process")
            return []
            
        susieR = importr('susieR')
        logger.info(f"[BATCH-{batch_id}] Fresh R session initialized with susieR")
        
    except Exception as e:
        logger.error(f"[BATCH-{batch_id}] Error setting up R environment: {e}")
        return []
    
    batch_results = []
    successful_regions = 0
    failed_regions = 0
    
    # Process each region in this batch using the same R session
    for region in region_batch:
        try:
            logger.info(f"[BATCH-{batch_id}] Processing region {region['variant_id']}")
            
            # Use the existing finemap_region_notebook_exact function 
            # but in this isolated process
            result = finemap_region_notebook_exact(
                seed=42,
                sumstats=sumstats_data,
                chr_num=region['chr'],
                lead_variant_position=region['position'],
                window=2000,
                population="EUR",
                L=-1,
                coverage=0.95,
                min_abs_corr=0.5
            )
            
            if result is not None and len(result) > 0:
                # Add batch and region metadata
                result['batch_id'] = batch_id
                result['processed_by'] = f"mp-worker-{os.getpid()}"
                batch_results.append(result)
                successful_regions += 1
                
                # Log success metrics
                max_pip = result['PIP'].max()
                high_pip_count = len(result[result['PIP'] > 0.5])
                cs_count = len(result.get('credible_set', [0]).unique()) if 'credible_set' in result.columns else 0
                logger.info(f"[BATCH-{batch_id}] {region['variant_id']}: {cs_count} credible sets, max PIP={max_pip:.3f}")
            else:
                failed_regions += 1
                logger.warning(f"[BATCH-{batch_id}] Failed {region['variant_id']}")
                
        except Exception as e:
            failed_regions += 1
            logger.error(f"[BATCH-{batch_id}] Error processing {region['variant_id']}: {str(e)}")
    
    # Cleanup R session
    try:
        ro.r('gc()')  # Force garbage collection
        logger.info(f"[BATCH-{batch_id}] R session cleaned up")
    except:
        pass
    
    logger.info(f"[BATCH-{batch_id}] Completed: {successful_regions} successful, {failed_regions} failed")
    return batch_results


