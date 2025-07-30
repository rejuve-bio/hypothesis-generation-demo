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
from prefect import flow
import multiprocessing as mp
from functools import partial
import psutil
import gc
import optuna
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import Config
import re
logging.basicConfig(level=logging.INFO)

try:
    import rpy2.robjects as ro
    from rpy2.robjects.packages import importr
    from rpy2.robjects import pandas2ri, numpy2ri, default_converter
    from rpy2.robjects.conversion import localconverter
    from rpy2 import robjects
    from rpy2.robjects.vectors import ListVector
    from rpy2.robjects.conversion import Converter
    
    # Import necessary R packages
    base = importr('base')
    stats = importr('stats')
    
    # Check if packages are available before trying to import
    def check_r_package_available(package_name):
        r_code = f'is.element("{package_name}", installed.packages()[,1])'
        return ro.r(r_code)[0]
    
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

    HAS_RPY2 = True
    
except ImportError as e:
    logging.warning(f"rpy2 not available: {e}. R-based analyses will not work.")
    HAS_RPY2 = False
    HAS_SUSIE = False
    # Set all rpy2 related variables to None
    ro = None
    importr = None
    pandas2ri = None
    numpy2ri = None
    default_converter = None
    localconverter = None
    robjects = None
    susieR = None
    ListVector = None
    Converter = None

# === RPY2 MULTIPROCESSING HELPERS ===
def initialize_rpy2_for_worker():
    try:
        # Force activation of converters in worker process
        pandas2ri.activate()
        numpy2ri.activate()
        
        # Create combined converter
        global_converter = default_converter + pandas2ri.converter + numpy2ri.converter        
        
        return global_converter, True, None
        
    except Exception as e:
        # Alternative initialization if standard method fails
        try:
            
            # Manual converter setup
            cv = Converter('worker_converter')
            cv += pandas2ri.converter
            cv += numpy2ri.converter
            
            return cv, True, None
            
        except Exception as alt_e:
            return None, False, str(alt_e)

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
            
            formatted_file_path_raw = str(formatted_file_path_r[0])
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
        logger.info(f"[MUNGE] Available columns: {list(munged_df.columns)}")
        
        # Preserve rs_id information if available
        if 'SNP' in munged_df.columns:
            logger.info(f"[MUNGE] Found SNP column with rs_ids, preserving as RS_ID")
            munged_df['RS_ID'] = munged_df['SNP']
        elif 'RSID' in munged_df.columns:
            logger.info(f"[MUNGE] Found RSID column, preserving as RS_ID")
            munged_df['RS_ID'] = munged_df['RSID']
        else:
            logger.info(f"[MUNGE] No rs_id column found, will use positional ID only")
            munged_df['RS_ID'] = None
        
        # Create CHR:BP:A2:A1 ID format
        munged_df["ID"] = (munged_df["CHR"].astype(str) + ":" + 
                          munged_df["BP"].astype(str) + ":" + 
                          munged_df["A2"] + ":" + 
                          munged_df["A1"])
        
        # Set ID as index but keep RS_ID as a column
        munged_df.reset_index(drop=True, inplace=True)
        munged_df.set_index(["ID"], inplace=True)
        
        # Save the processed data
        final_output_path = os.path.join(output_dir, "munged_sumstats_processed.tsv")
        munged_df.to_csv(final_output_path, sep='\t', index=False)
        
        # Calculate processing time and stats
        elapsed_time = (datetime.now() - start_time).total_seconds()

        logger.info(f"[MUNGE] Processing completed in {elapsed_time:.2f} seconds")
        return munged_df, final_output_path
        
    except Exception as e:
        logger.error(f"[MUNGE] Error in MungeSumstats preprocessing: {str(e)}")
        raise


@task(cache_policy=None)
def filter_significant_variants(munged_df, output_dir, p_threshold=5e-8):
    """Filter significant variants from munged sumstats."""
    logger.info(f"[FILTER] Filtering variants with p < {p_threshold}")

    significant_df = munged_df[munged_df['P'] < p_threshold].copy()
    sig_output_path = os.path.join(output_dir, "significant_variants.tsv")
    significant_df.to_csv(sig_output_path, sep='\t', index=False)
    
    return significant_df, sig_output_path


# ===  COJO ANALYSIS ===
@task(cache_policy=None)
def run_cojo_per_chromosome(significant_df, plink_dir, output_dir, maf_threshold=0.01, population="EUR"):
    """
    Run GCTA COJO analysis per chromosome and combine results.
    """
    logger.info(f"[COJO] Starting per-chromosome COJO analysis for population {population}")
    
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
                result = subprocess.run(cojo_cmd, capture_output=True, text=True, timeout=1800)
                
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
        max_workers = 4 
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
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
            
            if 'SNP' in combined_cojo_df.columns:
                combined_cojo_df = combined_cojo_df.rename(columns={'SNP': 'ID'})
            
            # Remove duplicates and clean up
            combined_cojo_df = combined_cojo_df.drop_duplicates(subset=['ID'] if 'ID' in combined_cojo_df.columns else None)
            
            # Set ID as index
            if 'ID' in combined_cojo_df.columns:
                combined_cojo_df = combined_cojo_df.set_index('ID')
            
            # Save combined results
            os.makedirs(output_dir, exist_ok=True)
            cojo_output_path = os.path.join(output_dir, "combined_cojo_results.txt")
            combined_cojo_df.to_csv(cojo_output_path, sep='\t', index=True)
            
            logger.info(f"[COJO] Results saved to: {cojo_output_path}")           
            return combined_cojo_df, cojo_output_path
            
        else:
            logger.error("[COJO] No COJO results generated for any chromosome")
            raise RuntimeError("COJO analysis failed for all chromosomes")

@task
def check_ld_dimensions(ld_matrix, snp_df, bim_file_path):
    
    if ld_matrix.shape[0] != len(snp_df) or ld_matrix.shape[1] != len(snp_df):
        logger.info("Dimension mismatch detected between LD matrix and SNP list.")
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
        eps = 1e-6 
        R_df += np.eye(R_df.shape[0]) * eps
    
    return R_df

# === FINE-MAPPING SINGLE REGION ===
@task(cache_policy=None)
def finemap_region(seed, sumstats, chr_num, lead_variant_position, window=2000, 
                                 population="EUR", L=-1, coverage=0.95, min_abs_corr=0.5):
    try:     
        logger.info(f"[FINEMAP] Fine-mapping chr{chr_num}:{lead_variant_position} ±{window}kb")
        

        config = Config.from_env()
        plink_dir = config.plink_dir
        
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
         
        # Calculate LD
        try:
            with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.txt') as tmp_file:
                for snp_id in filtered_ids:
                    tmp_file.write(f"{snp_id}\n")
                tmp_file_path = tmp_file.name
                
            with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.txt') as tmp_file_ld:
                tmp_file_ld_path = tmp_file_ld.name
                
            plink_cmd = [
                "plink2",
                "--bfile", f"{plink_dir}/{population}/{population}.{chr_num}.1000Gp3.20130502",
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
        LD_mat = ld_df.values
        
        # Prepare SNP dataframe for check_ld_dimensions function
        snp_df_for_check = pd.DataFrame({
            'SNPID': sub_region_sumstats_ld.index,
            'CHR': sub_region_sumstats_ld['CHR'],
            'BP': sub_region_sumstats_ld['BP']
        })
        
        # Use check_ld_dimensions to reconcile any mismatches
        bim_file_path = f"{plink_dir}/{population}/{population}.{chr_num}.1000Gp3.20130502.bim"
        
        if os.path.exists(bim_file_path):
            try:
                filtered_snp_df = check_ld_dimensions(LD_mat, snp_df_for_check, bim_file_path)
                
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

        # Get Z-scores after LD filtering - calculate if not available
        if "Z" in sub_region_sumstats_ld.columns:
            zhat = sub_region_sumstats_ld["Z"].values.reshape(len(sub_region_sumstats_ld), 1)
        elif "BETA" in sub_region_sumstats_ld.columns and "SE" in sub_region_sumstats_ld.columns:
            logger.info(f"[FINEMAP] Z-scores not available, calculating from BETA/SE")
            zhat = (sub_region_sumstats_ld["BETA"] / sub_region_sumstats_ld["SE"]).values.reshape(len(sub_region_sumstats_ld), 1)
        else:
            logger.error(f"[FINEMAP] Neither Z-scores nor BETA/SE available for Z-score calculation")
            return None

        # Basic validation of final data
        if len(sub_region_sumstats_ld) < 10:
            logger.error(f"[FINEMAP] Too few variants after all filtering: {len(sub_region_sumstats_ld)}")
            return None
        
        if LD_mat.shape[0] != len(zhat):
            logger.error(f"[FINEMAP] Dimension mismatch: LD={LD_mat.shape}, Z-scores={len(zhat)}")
            return None
        
        num_samples = int(sub_region_sumstats_ld["N"].iloc[0])
        
        # Check for invalid data
        if np.isnan(LD_mat).any() or np.isinf(LD_mat).any():
            logger.error(f"[DEBUG] Invalid LD matrix detected - contains NaN or inf values")
            return None
        if np.isnan(zhat).any() or np.isinf(zhat).any():
            logger.error(f"[DEBUG] Invalid Z-scores detected - contains NaN or inf values")
            return None
        
        # Simplified susieR availability check
        try:
            # Quick check if susieR is available
            if not check_r_package_available('susieR'):
                logger.error(f"[FINEMAP] susieR package not available for chr{chr_num}:{lead_variant_position}")
                return None
            logger.info(f"[FINEMAP] susieR package available for chr{chr_num}:{lead_variant_position}")
        except Exception as e:
            logger.error(f"[FINEMAP] Error checking susieR availability: {e}")
            return None
        
        # Set up R conversions properly for multithreading
        try:
            ro.r(f'set.seed({seed})')
        except Exception as e:
            logger.error(f"[FINEMAP] Error setting R seed: {str(e)}")
            return None
        
        # Convert data within context, then exit context
        with (ro.default_converter + numpy2ri.converter).context():
            try:
                zhat_r = ro.conversion.get_conversion().py2rpy(zhat)
                R_r = ro.conversion.get_conversion().py2rpy(LD_mat)
                    
            except Exception as e:
                logger.error(f"[FINEMAP] Error converting data to R objects: {str(e)}")
                return None
        
        # All SuSiE operations happen outside conversion context (like simplified_finemapping.py)
        
        # Import susieR once for all operations
        try:
            local_susieR = importr('susieR')
            logger.info(f"[FINEMAP] susieR imported for chr{chr_num}:{lead_variant_position}")
        except Exception as e:
            logger.error(f"[FINEMAP] Error importing susieR: {e}")
            return None
        
        # Optuna hyperparameter optimization
        if L <= 0:
            def objective_susie(trial):
                L_trial = trial.suggest_int("L", 1, 20)
                logger.info(f"[DEBUG] Starting SuSiE trial with L={L_trial}")                
                
                try:
                    # Use the local susieR instance
                    susie_fit = local_susieR.susie_rss(z=zhat_r, R=R_r, L=L_trial, n=num_samples)
                    logger.info(f"[DEBUG] SuSiE execution completed for L={L_trial}")
                except Exception as e:
                    logger.error(f"[DEBUG] SuSiE execution failed for L={L_trial}: {str(e)}")
                    return -np.inf
                
                # Extract ELBO and convergence
                try:
                    # Primary method: ListVector conversion (known to work reliably)
                    try:
                        susie_fit_r = ListVector(susie_fit)
                        elbo = susie_fit_r.rx2('elbo')[-1]
                        converged = susie_fit_r.rx2('converged')[0]
                    except Exception:
                        # Fallback: R-based extraction
                        ro.globalenv['temp_fit'] = susie_fit
                        elbo_r = ro.r('tail(temp_fit$elbo, 1)')
                        converged_r = ro.r('temp_fit$converged')
                        ro.r('rm(temp_fit)')
                        elbo = float(elbo_r[0])
                        converged = int(converged_r[0])
                    
                    # Validate ELBO is reasonable
                    if np.isnan(elbo) or np.isinf(elbo) or elbo > 0:
                        logger.warning(f"[DEBUG] L={L_trial} invalid ELBO: {elbo}")
                        return -np.inf
                    
                    # Check convergence
                    if converged == 1:
                        logger.info(f"[DEBUG] L={L_trial} converged with ELBO {elbo:.6f}")
                        return elbo
                    else:
                        logger.warning(f"[DEBUG] L={L_trial} did not converge")
                        return -np.inf
                        
                except Exception as e:
                    logger.error(f"[DEBUG] Error extracting results for L={L_trial}: {str(e)}")
                    return -np.inf
            
            study = optuna.create_study(direction="maximize")
            study.optimize(objective_susie, n_trials=10)
            L = study.best_params["L"]
            logger.info(f"[FINEMAP] Best L found: {L} with ELBO {study.best_value}")
        
        # Run SuSiE with final L
        logger.info(f"[FINEMAP] Running SuSiE with L={L}")
        try:
            # Use the local susieR instance
            susie_fit = local_susieR.susie_rss(z=zhat_r, R=R_r, L=L, n=num_samples)
        except Exception as e:
            logger.error(f"[FINEMAP] SuSiE execution failed: {str(e)}")
            return None
        
        # Extract credible sets and PIPs
        logger.info(f"[FINEMAP] Extracting credible sets and PIPs...")
        
        # Step 1: Extract credible sets
        try:
            # Use the local susieR instance
            credible_sets = local_susieR.susie_get_cs(susie_fit, coverage=coverage, min_abs_corr=min_abs_corr, Xcorr=R_r)
        except Exception as e:
            logger.error(f"[FINEMAP] Error extracting credible sets: {str(e)}")
            credible_sets = None

        # Step 2: Extract PIPs - Using the local susieR instance
        logger.info(f"[FINEMAP] Extracting PIPs using local susieR instance...")
        try:
            # Use the local susieR instance
            pips = np.array(local_susieR.susie_get_pip(susie_fit), dtype=np.float64)
            logger.info(f"[DEBUG] susie_get_pip SUCCESS: {len(pips)} PIPs")
            
        except Exception as e:
            logger.warning(f"[DEBUG] susie_get_pip failed: {type(e).__name__}: {e}")
            logger.info(f"[DEBUG] Falling back to manual alpha extraction...")
            
            # Fallback: manual alpha calculation
            try:
                ro.globalenv['susie_fit'] = susie_fit
                r_pip_code = """
                tryCatch({
                    fit <- susie_fit
                    if (!is.null(fit$alpha) && is.matrix(fit$alpha)) {
                        alpha_matrix <- fit$alpha
                        pip_vals <- 1 - apply(1 - alpha_matrix, 2, prod)
                    } else if (!is.null(fit$pip)) {
                        pip_vals <- as.numeric(fit$pip)
                    } else {
                        stop("No alpha matrix or pip found")
                    }
                    
                    # Ensure valid PIPs
                    pip_vals <- as.numeric(pip_vals)
                    pip_vals[is.na(pip_vals)] <- 0.0
                    pip_vals[pip_vals < 0] <- 0.0
                    pip_vals[pip_vals > 1] <- 1.0
                    pip_vals
                }, error = function(e) {
                    rep(0.01, %d)
                })
                """ % len(sub_region_sumstats_ld)
                
                r_pip_result = ro.r(r_pip_code)
                ro.r('rm(susie_fit)')
                pips = np.array(r_pip_result, dtype=np.float64)
                logger.info(f"[DEBUG] Manual extraction SUCCESS: {len(pips)} PIPs")
                
            except Exception as fallback_e:
                logger.error(f"[DEBUG] Manual extraction FAILED: {fallback_e}")
                # Last resort: uniform small PIPs
                pips = np.full(len(sub_region_sumstats_ld), 0.01, dtype=np.float64)
                logger.warning(f"[DEBUG] Using uniform PIPs as last resort: {len(pips)} PIPs")
        
        # Ensure PIP array has correct length
        if len(pips) != len(sub_region_sumstats_ld):
            logger.warning(f"[DEBUG] Adjusting PIP length: {len(pips)} -> {len(sub_region_sumstats_ld)}")
            if len(pips) < len(sub_region_sumstats_ld):
                pips = np.pad(pips, (0, len(sub_region_sumstats_ld) - len(pips)), constant_values=0.01)
            else:
                pips = pips[:len(sub_region_sumstats_ld)]
        
        # Validate PIPs
        if len(pips) != len(sub_region_sumstats_ld):
            logger.error(f"[DEBUG] PIP length mismatch: {len(pips)} vs {len(sub_region_sumstats_ld)}")
            return None
            
        # Check PIP validity
        if np.any(pips < 0) or np.any(pips > 1):
            logger.warning(f"[FINEMAP] Invalid PIPs detected, clipping to [0,1]")
            pips = np.clip(pips, 0, 1)
        
        # Explicit memory cleanup for large R objects
        try:
            ro.r('gc()')  # Force R garbage collection
        except:
            pass
        
        # Add PIPs to results and initialize cs column
        sub_region_sumstats_ld = sub_region_sumstats_ld.copy()
        sub_region_sumstats_ld["PIP"] = pips
        sub_region_sumstats_ld["cs"] = 0
        
        # Process credible sets
        try:
            if credible_sets is not None and len(credible_sets) > 0:
                cs_data = credible_sets[0]                
                if hasattr(cs_data, '__len__') and len(cs_data) > 0:
                    # Process each credible set
                    for cs_idx, cs_variants in enumerate(cs_data):
                        # Convert R 1-based indices to Python 0-based indices  
                        python_indices = np.array(cs_variants) - 1
                        # Validate indices
                        valid_indices = python_indices[(python_indices >= 0) & (python_indices < len(sub_region_sumstats_ld))]
                        if len(valid_indices) > 0:
                            sub_region_sumstats_ld.iloc[valid_indices, sub_region_sumstats_ld.columns.get_loc("cs")] = cs_idx + 1
                    
                    credible_mask = sub_region_sumstats_ld["cs"] > 0
                    credible_snps = sub_region_sumstats_ld[credible_mask].copy()
                    
                    if len(credible_snps) > 0:
                        # Add metadata
                        credible_snps['region_id'] = f"chr{chr_num}:{lead_variant_position}"
                        credible_snps['region_chr'] = chr_num
                        credible_snps['region_center'] = lead_variant_position
                        credible_snps['converged'] = True
                        credible_snps['credible_set'] = credible_snps['cs']
                        
                        # Log per-credible-set stats
                        for cs_num in sorted(credible_snps['cs'].unique()):
                            cs_subset = credible_snps[credible_snps['cs'] == cs_num]
                            logger.info(f"[FINEMAP] Chr{chr_num}:{lead_variant_position} - Credible set {cs_num}: {len(cs_subset)} variants, PIP range: {cs_subset['PIP'].min():.6f}-{cs_subset['PIP'].max():.6f}")
                        
                        # Format for LocusZoom and return
                        return credible_snps 
                    else:
                        logger.warning(f"[FINEMAP] No variants in credible sets")
                else:
                    logger.warning(f"[FINEMAP] Empty or invalid credible sets data")
            else:
                logger.warning(f"[FINEMAP] No credible sets found")
                
        except Exception as e:
            logger.warning(f"[FINEMAP] Error processing credible sets: {str(e)}")
        
        # Fallback: use high PIP threshold
        logger.info(f"[FINEMAP] Using fallback: variants with PIP > 0.1")
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
        
        # Return DataFrame directly for multiprocessing
        return fallback_result
            
    except Exception as e:
        logger.error(f"[FINEMAP] Error in fine-mapping chr{chr_num}:{lead_variant_position}: {str(e)}")
        return None

def create_region_batches(cojo_results, batch_size=3):
    """Split COJO results into smaller batches for memory-efficient processing"""
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
    
    # Split into smaller batches (reduced from 5 to 3 for memory efficiency)
    for i in range(0, len(regions), batch_size):
        batch = regions[i:i + batch_size]
        batches.append(batch)
    
    logger.info(f"[BATCH] Created {len(batches)} batches from {len(regions)} regions")
    return batches

def finemap_region_batch_worker(batch_data):

    # Unpack the batch_data tuple
    if len(batch_data) == 3:
        region_batch, batch_id, sumstats_file_path = batch_data
        additional_params = None
    elif len(batch_data) == 4:
        region_batch, batch_id, sumstats_file_path, additional_params = batch_data
    else:
        raise ValueError(f"Expected 3 or 4 elements in batch_data, got {len(batch_data)}")
    
    sumstats_data = load_sumstats_from_file(sumstats_file_path)
    
    if sumstats_data is None:
        logger.error(f"[BATCH-{batch_id}] Failed to load sumstats from file")
        return []

    db = None
    user_id = None
    project_id = None
    
    if additional_params:
        db_params = additional_params.get('db_params', None)
        user_id = additional_params.get('user_id', None)
        project_id = additional_params.get('project_id', None)
        finemap_params = additional_params.get('finemap_params', {})
        
        # Extract parameters
        seed = finemap_params['seed']
        window = finemap_params['window']
        L = finemap_params['L']
        coverage = finemap_params['coverage']
        min_abs_corr = finemap_params['min_abs_corr']
        population = finemap_params['population']
        
        # Recreate database connection in worker process
        if db_params:
            try:
                # Import Database locally to avoid circular imports in multiprocessing
                from db import Database
                db = Database(db_params['uri'], db_params['db_name'])
                logger.info(f"[BATCH-{batch_id}] Database connection recreated in worker process")
            except Exception as db_e:
                logger.error(f"[BATCH-{batch_id}] Error recreating database connection: {db_e}")
                db = None
    else:
        raise ValueError("No additional_params provided to worker - this should not happen")
    
    batch_results = []
    successful_regions = 0
    failed_regions = 0
    
    logger.info(f"[BATCH-{batch_id}] Initializing single R session for entire batch")
    
    # Single R session initialization 
    global_converter, r_session_initialized, init_error = initialize_rpy2_for_worker()
    
    if not r_session_initialized:
        logger.error(f"[BATCH-{batch_id}] rpy2 initialization failed: {init_error}")
        return []
    
    logger.info(f"[BATCH-{batch_id}] rpy2 conversion context initialized successfully")
    
    # Now initialize susieR with clean R environment
    susieR = None
    try:
        # Clean R environment at start of worker to prevent variable pollution
        try:
            ro.r('rm(list=ls(all.names=TRUE))')  # Clear any existing objects
            ro.r('gc(verbose=FALSE, full=TRUE)')  # Full garbage collection
            # Clear problematic environment variables
            ro.r('Sys.unsetenv(c("LD_LIBRARY_PATH", "R_SESSION_TMPDIR"))')
        except Exception as clean_e:
            logger.warning(f"[BATCH-{batch_id}] Initial R cleanup warning: {clean_e}")
        
        # Check susieR availability and import (no conversion context needed for imports)
        if not check_r_package_available('susieR'):
            logger.error(f"[BATCH-{batch_id}] susieR not available in this worker")
            return []
            
        susieR = importr('susieR')
        logger.info(f"[BATCH-{batch_id}] susieR imported successfully with clean environment")
        
        # Set global R options for memory management
        ro.r('options(warn=-1)')
        ro.r('options(scipen=999)') 
        
    except Exception as susie_e:
        logger.error(f"[BATCH-{batch_id}] susieR import failed: {susie_e}")
        return []
    
    # Process regions with shared R session
    for region_idx, region in enumerate(region_batch):
        region_id = region['variant_id']
        
        logger.info(f"[BATCH-{batch_id}] Processing region {region_idx+1}/{len(region_batch)}: {region_id}")
        
        try:
            
            # Use the shared R session with proper context management
            logger.info(f"[BATCH-{batch_id}] Running fine-mapping for {region_id} with shared R session")
            
            # Call finemap_region directly without additional conversion context
            # The function handles its own conversion context internally
            try:
                result = finemap_region(
                    seed=seed,
                    sumstats=sumstats_data,
                    chr_num=region['chr'],
                    lead_variant_position=region['position'],
                    window=window,
                    population=population,
                    L=L,
                    coverage=coverage,
                    min_abs_corr=min_abs_corr
                )
                
                if result is not None and len(result) > 0:
                    # Add batch and region metadata
                    result['batch_id'] = batch_id
                    result['region_idx'] = region_idx
                    result['processed_by'] = f"mp-worker-{os.getpid()}"
                    batch_results.append(result)
                    successful_regions += 1
                    
                    # Save to database if available
                    if db and user_id and project_id:
                        try:
                            from utils import transform_credible_sets_to_locuszoom
                            
                            lead_variant_id = region['variant_id']
                            credible_sets_data = []
                            
                            # Transform results
                            if 'cs' in result.columns:
                                for cs_id in sorted(result['cs'].unique()):
                                    if cs_id > 0:
                                        cs_variants = result[result['cs'] == cs_id]
                                        locuszoom_data = transform_credible_sets_to_locuszoom(cs_variants)
                                        
                                        credible_set = {
                                            "set_id": int(cs_id),
                                            "coverage": coverage,
                                            "variants": locuszoom_data
                                        }
                                        credible_sets_data.append(credible_set)
                            
                            # Save to database
                            metadata = {
                                "chr": region['chr'],
                                "position": region['position'],
                                "finemap_window_kb": window,
                                "population": population,
                                "seed": seed,
                                "L": L,
                                "coverage": coverage,
                                "min_abs_corr": min_abs_corr,
                                "total_variants_analyzed": len(result),
                                "credible_sets_count": len(credible_sets_data),
                                "completed_at": datetime.now().isoformat()
                            }
                            
                            db.save_lead_variant_credible_sets(
                                user_id, project_id, lead_variant_id, 
                                {
                                    "credible_sets": credible_sets_data,
                                    "metadata": metadata
                                }
                            )
                            
                            logger.info(f"[BATCH-{batch_id}] Saved {len(credible_sets_data)} credible sets for {lead_variant_id}")
                            
                        except Exception as save_e:
                            logger.error(f"[BATCH-{batch_id}] Error saving credible sets for {region_id}: {save_e}")
                else:
                    failed_regions += 1
                    logger.warning(f"[BATCH-{batch_id}] No results for {region_id}")
                    
            except Exception as finemap_e:
                failed_regions += 1
                logger.error(f"[BATCH-{batch_id}] Fine-mapping failed for {region_id}: {str(finemap_e)}")
        
        except Exception as processing_e:
            failed_regions += 1
            logger.error(f"[BATCH-{batch_id}] Error processing {region_id}: {str(processing_e)}")
        
        # cleanup after each region to prevent accumulation
        try:
            # Force Python garbage collection
            collected = gc.collect()
            
            # Aggressive R cleanup (no conversion context to avoid pollution)
            try:
                # Remove all user objects except base packages
                ro.r('rm(list=ls()[!(ls() %in% c("base", "stats", "utils", "methods", "grDevices", "graphics"))])')
                
                # Clear temporary variables that might be lingering
                ro.r('rm(list=ls(pattern="^temp_", envir=.GlobalEnv))')
                ro.r('rm(list=ls(pattern="^susie_", envir=.GlobalEnv))')
                
                # Force R garbage collection
                ro.r('gc(verbose=FALSE, full=TRUE)')
                
                # Clear R's internal caches
                ro.r('if(exists(".Last.value")) rm(.Last.value)')
                
            except Exception as r_cleanup_e:
                logger.warning(f"[BATCH-{batch_id}] R cleanup error after {region_id}: {r_cleanup_e}")
                
        except Exception as cleanup_e:
            logger.warning(f"[BATCH-{batch_id}] Cleanup error after {region_id}: {cleanup_e}")
    
    # Final cleanup of R session
    if r_session_initialized:
        try:
            # Comprehensive final R session cleanup (no conversion context)
            ro.r('rm(list=ls(all.names=TRUE))')  # Clear everything including hidden objects
            ro.r('gc(verbose=FALSE, full=TRUE)')  # Full garbage collection
            
            # Clear R environment variables that might cause pollution
            ro.r('Sys.unsetenv(c("LD_LIBRARY_PATH", "R_SESSION_TMPDIR"))')
            
            # Reset R options to defaults
            ro.r('options(warn=0)')  # Reset warning level
            
            logger.info(f"[BATCH-{batch_id}] Final R session cleanup completed")
        except Exception as final_cleanup_e:
            logger.warning(f"[BATCH-{batch_id}] Final cleanup error: {final_cleanup_e}") 
    
    return batch_results

# === MEMORY-EFFICIENT DATA SHARING ===
def save_sumstats_for_workers(sumstats_df, temp_dir=None):
    """Save sumstats to a temporary file for memory-efficient worker access"""
    if temp_dir is None:
        temp_dir = tempfile.gettempdir()
    
    # Create a unique temporary file
    temp_file = tempfile.NamedTemporaryFile(
        mode='w', suffix='.tsv', dir=temp_dir, delete=False
    )
    temp_path = temp_file.name
    temp_file.close()
    
    # Save the DataFrame
    sumstats_df.to_csv(temp_path, sep='\t', index=True)
    logger.info(f"[MEMORY] Saved sumstats to temporary file: {temp_path}")
    logger.info(f"[MEMORY] File size: {os.path.getsize(temp_path) / (1024**2):.1f} MB")
    
    return temp_path

def load_sumstats_from_file(temp_path):
    """Load sumstats from temporary file in worker process"""
    try:
        # Load with proper index handling
        sumstats_df = pd.read_csv(temp_path, sep='\t', index_col=0, low_memory=False)
        logger.info(f"[WORKER] Loaded sumstats: {sumstats_df.shape[0]} variants, {sumstats_df.shape[1]} columns")
        return sumstats_df
    except Exception as e:
        logger.error(f"[WORKER] Error loading sumstats from {temp_path}: {e}")
        return None

def cleanup_sumstats_file(temp_path):
    """Clean up temporary sumstats file after all workers are done"""
    try:
        if os.path.exists(temp_path):
            os.remove(temp_path)
            logger.info(f"[MEMORY] Cleaned up temporary sumstats file: {temp_path}")
        else:
            logger.warning(f"[MEMORY] Temporary file not found for cleanup: {temp_path}")
    except Exception as e:
        logger.error(f"[MEMORY] Error cleaning up temporary file {temp_path}: {e}")


