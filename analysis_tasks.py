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
import shutil
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
from utils import transform_credible_sets_to_locuszoom

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

# === NEXTFLOW HARMONIZATION ===
@task(cache_policy=None)
def harmonize_sumstats_with_nextflow(gwas_file_path, output_dir, ref_genome="GRCh38", 
                                     ref_dir=None, code_repo=None, script_dir=None,
                                     threshold=0.99, sample_size=None, timeout_seconds=14400, 
                                     cleanup_upload=True):
    """
    Harmonize GWAS summary statistics using Nextflow-based harmonization pipeline.
    """
    logger.info(f"[HARMONIZE] Starting Nextflow harmonization for {gwas_file_path}")
    start_time = datetime.now()
    
    # Convert gwas_file_path to absolute path
    gwas_file_path = os.path.abspath(gwas_file_path)
    logger.info(f"[HARMONIZE] Resolved absolute path: {gwas_file_path}")
    
    # Get configuration
    config = Config.from_env()
    if ref_dir is None:
        ref_dir = config.get_harmonizer_ref_dir(ref_genome)
    if code_repo is None:
        code_repo = getattr(config, 'harmonizer_code_repo', '/app/gwas-sumstats-harmoniser')
    if script_dir is None:
        script_dir = getattr(config, 'harmonizer_script_dir', '/app/scripts/1000Genomes_phase3')
    
    # Create output paths
    os.makedirs(output_dir, exist_ok=True)
    log_dir = os.path.join(output_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    try:
        # Path to harmonizer script
        harmonizer_script = os.path.join(script_dir, "6_harmoniser.sh")
        
        if not os.path.exists(harmonizer_script):
            raise FileNotFoundError(f"Harmonizer script not found: {harmonizer_script}")
        
        harmonizer_cmd = [
            "bash", harmonizer_script,
            "--input", gwas_file_path,
            "--build", ref_genome,
            "--threshold", str(threshold),
            "--ref", ref_dir,
            "--code-repo", code_repo
        ]
        
        logger.info(f"[HARMONIZE] Running command: {' '.join(harmonizer_cmd)}")
        
        # Run harmonizer
        result = subprocess.run(
            harmonizer_cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds
        )
        
        if result.returncode != 0:
            logger.error(f"[HARMONIZE] Harmonizer failed with exit code {result.returncode}")
            logger.error(f"[HARMONIZE] STDOUT: {result.stdout}")
            logger.error(f"[HARMONIZE] STDERR: {result.stderr}")
            raise RuntimeError(f"Harmonization failed with exit code {result.returncode}")
        
        logger.info(f"[HARMONIZE] Harmonizer completed successfully")
        logger.info(f"[HARMONIZE] Last 20 lines of output:")
        for line in result.stdout.strip().split('\n')[-20:]:
            logger.info(f"  {line}")
        
        harmonized_file_path = None
        for line in result.stdout.strip().split('\n'):
            if line.startswith('HARMONIZED_OUTPUT_PATH='):
                harmonized_file_path = line.split('=', 1)[1]
                logger.info(f"[HARMONIZE] Found harmonized file from script output: {harmonized_file_path}")
                break
        
        if not harmonized_file_path:
            logger.error(f"[HARMONIZE] Bash script did not output HARMONIZED_OUTPUT_PATH")
            # Log the path to nextflow log for inspection
            input_dir = os.path.dirname(gwas_file_path)
            nextflow_log = os.path.join(input_dir, ".nextflow.log")
            if os.path.exists(nextflow_log):
                logger.error(f"[HARMONIZE] Nextflow log found at: {nextflow_log}")
            else:
                logger.error(f"[HARMONIZE] Nextflow log not found at: {nextflow_log}")
            raise FileNotFoundError(
                "Harmonizer script did not output harmonized file path. "
            )
        
        if not os.path.exists(harmonized_file_path):
            raise FileNotFoundError(f"Harmonized file not found at path: {harmonized_file_path}")
        
        harmonized_output_path = os.path.join(output_dir, os.path.basename(harmonized_file_path))
        upload_dir = os.path.dirname(harmonized_file_path)
        parent_upload_dir = os.path.dirname(gwas_file_path)
        
        if os.path.abspath(harmonized_file_path) != os.path.abspath(harmonized_output_path):
            shutil.move(harmonized_file_path, harmonized_output_path)
            logger.info(f"[HARMONIZE] Moved harmonized file to: {harmonized_output_path}")
        
        try:
            gwas_basename = os.path.splitext(os.path.basename(gwas_file_path))[0]
            
            # 1. Remove SSF intermediate files (including .tbi tabix index files)
            for pattern in [f"{gwas_basename}.tsv*", f"{gwas_basename}*-meta.yaml", f"{gwas_basename}*.tbi"]:
                for f in glob.glob(os.path.join(upload_dir, pattern)):
                    if os.path.exists(f) and f != gwas_file_path:
                        os.remove(f)
                        logger.info(f"[HARMONIZE] Cleaned up: {f}")
                # Also check parent directory
                for f in glob.glob(os.path.join(parent_upload_dir, pattern)):
                    if os.path.exists(f) and f != gwas_file_path:
                        os.remove(f)
                        logger.info(f"[HARMONIZE] Cleaned up: {f}")
            
            # 2. Move logs to output_dir instead of deleting
            upload_log_dir = os.path.join(upload_dir, "logs")
            if os.path.exists(upload_log_dir):
                output_log_dir = os.path.join(output_dir, "logs")
                if not os.path.exists(output_log_dir):
                    shutil.move(upload_log_dir, output_log_dir)
                    logger.info(f"[HARMONIZE] Moved logs to: {output_log_dir}")
            
            # 3. Remove Nextflow work directories 
            nextflow_work_dir = os.path.join(upload_dir, "work")
            if os.path.exists(nextflow_work_dir):
                shutil.rmtree(nextflow_work_dir)
                logger.info(f"[HARMONIZE] Removed Nextflow work directory: {nextflow_work_dir}")
            # Also check parent directory for work dir
            parent_work_dir = os.path.join(parent_upload_dir, "work")
            if os.path.exists(parent_work_dir):
                shutil.rmtree(parent_work_dir)
                logger.info(f"[HARMONIZE] Removed Nextflow work directory: {parent_work_dir}")
            
            # 4. Remove Nextflow timestamp directories 
            for item in os.listdir(upload_dir):
                item_path = os.path.join(upload_dir, item)
                if os.path.isdir(item_path) and (
                    len(item.split('_')) >= 2 or  
                    item.startswith('2') or  
                    item == '.nextflow' or  
                    item.startswith('results') 
                ):
                    try:
                        shutil.rmtree(item_path)
                        logger.info(f"[HARMONIZE] Removed directory: {item_path}")
                    except Exception as e:
                        logger.warning(f"[HARMONIZE] Could not remove directory {item_path}: {e}")
            
            # 5. Remove .nextflow.log files from both upload_dir and parent_upload_dir
            for log_dir in [upload_dir, parent_upload_dir]:
                for f in glob.glob(os.path.join(log_dir, ".nextflow.log*")):
                    try:
                        os.remove(f)
                        logger.info(f"[HARMONIZE] Removed: {f}")
                    except Exception as e:
                        logger.warning(f"[HARMONIZE] Could not remove {f}: {e}")
            
            # 6.  remove original uploaded file
            if cleanup_upload and os.path.exists(gwas_file_path):
                os.remove(gwas_file_path)
                logger.info(f"[HARMONIZE] Removed original uploaded file: {gwas_file_path}")
            elif not cleanup_upload:
                logger.info(f"[HARMONIZE] Keeping original uploaded file: {gwas_file_path}")
            
            logger.info(f"[HARMONIZE] Cleanup complete for upload directory: {upload_dir}")
            
        except Exception as cleanup_error:
            logger.warning(f"[HARMONIZE] Error during cleanup: {cleanup_error}")
        
        harmonized_file_path = harmonized_output_path
            
        # Load harmonized data
        logger.info("[HARMONIZE] Loading harmonized SSF data")
        
        # Read gzipped TSV with harmonized columns
        harmonized_df = pd.read_csv(harmonized_file_path, sep='\t', compression='gzip', low_memory=False)
        logger.info(f"[HARMONIZE] Loaded {harmonized_df.shape[0]} variants with {harmonized_df.shape[1]} columns")
        logger.info(f"[HARMONIZE] Available columns: {list(harmonized_df.columns)}")
        
        # Validate required columns
        required_cols = ['chromosome', 'base_pair_location', 'effect_allele', 'other_allele', 
                        'beta', 'standard_error', 'p_value']
        missing_cols = [col for col in required_cols if col not in harmonized_df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns in harmonized output: {missing_cols}")
        
        # Add N column if not present (required for downstream analysis)
        n_was_added = False
        if 'N' not in harmonized_df.columns:
            if sample_size is None:
                raise ValueError(
                    "[HARMONIZE] Sample size (N) not found in input file and not provided as parameter. "
                    "Please provide sample_size parameter or ensure input file has N column."
                )
            logger.info(f"[HARMONIZE] Adding sample size N={sample_size} from parameter")
            harmonized_df['N'] = sample_size
            n_was_added = True
        else:
            logger.info(f"[HARMONIZE] Using sample size from input file: N={harmonized_df['N'].iloc[0]}")
        
        # Set variant_id as index
        needs_resave = False
        if 'variant_id' in harmonized_df.columns:
            harmonized_df.set_index('variant_id', inplace=True)
            if '_' in harmonized_df.index[0]:
                harmonized_df.index = harmonized_df.index.str.replace('_', ':', regex=False)
                logger.info(f"[HARMONIZE] Converted variant_id format from underscores to colons (e.g., {harmonized_df.index[0]})")
                needs_resave = True
            else:
                logger.info(f"[HARMONIZE] variant_id already in colon format (e.g., {harmonized_df.index[0]})")
        else:
            # Create variant_id if not present (already in colon format)
            harmonized_df['variant_id'] = (
                harmonized_df['chromosome'].astype(str) + ':' +
                harmonized_df['base_pair_location'].astype(str) + ':' +
                harmonized_df['other_allele'] + ':' +
                harmonized_df['effect_allele']
            )
            harmonized_df.set_index('variant_id', inplace=True)
            logger.info(f"[HARMONIZE] Created variant_id in colon format (e.g., {harmonized_df.index[0]})")
            needs_resave = True
        
        # Save the updated DataFrame back to file if N was added or format was converted
        if n_was_added or needs_resave:
            reasons = []
            if n_was_added:
                reasons.append("N column added")
            if needs_resave:
                reasons.append("variant_id format converted to colons")
            logger.info(f"[HARMONIZE] Saving updated harmonized file ({', '.join(reasons)}) to: {harmonized_file_path}")
            harmonized_df.to_csv(harmonized_file_path, sep='\t', compression='gzip', index=True)
            logger.info(f"[HARMONIZE] Updated file saved successfully")
        
        # Calculate processing time
        elapsed_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[HARMONIZE] Processing completed in {elapsed_time:.2f} seconds")

        return harmonized_df, harmonized_file_path
        
    except subprocess.TimeoutExpired:
        logger.error(f"[HARMONIZE] Harmonization timeout after {timeout_seconds} seconds")
        raise RuntimeError(f"Harmonization timeout after {timeout_seconds} seconds")
    except Exception as e:
        logger.error(f"[HARMONIZE] Error in harmonization: {str(e)}")
        raise


@task(cache_policy=None)
def filter_significant_variants(harmonized_df, output_dir, p_threshold=5e-8):
    """Filter significant variants from harmonized sumstats."""
    logger.info(f"[FILTER] Filtering variants with p_value < {p_threshold}")

    # Support both old (P) and new (p_value) column names
    p_col = 'p_value' if 'p_value' in harmonized_df.columns else 'P'
    
    significant_df = harmonized_df[harmonized_df[p_col] < p_threshold].copy()
    sig_output_path = os.path.join(output_dir, "significant_variants.tsv")
    significant_df.to_csv(sig_output_path, sep='\t', index=True)
    
    return significant_df, sig_output_path


# ===  COJO ANALYSIS ===
@task(cache_policy=None)
def run_cojo_per_chromosome(significant_df, plink_dir, output_dir, maf_threshold=0.01, population="EUR", ref_genome="GRCh38"):
    """
    Run GCTA COJO analysis per chromosome and combine results.
    """
    logger.info(f"[COJO] Starting per-chromosome COJO analysis for population {population}")

    config = Config.from_env()
    
    # Create temporary directory for COJO processing
    with tempfile.TemporaryDirectory(prefix="cojo_analysis_") as temp_dir:
        logger.info(f"[COJO] Using temporary directory: {temp_dir}")
        
        # Support both old and new column names
        a1_col = 'effect_allele' if 'effect_allele' in significant_df.columns else 'A1'
        a2_col = 'other_allele' if 'other_allele' in significant_df.columns else 'A2'
        frq_col = 'effect_allele_frequency' if 'effect_allele_frequency' in significant_df.columns else 'FRQ'
        beta_col = 'beta' if 'beta' in significant_df.columns else 'BETA'
        se_col = 'standard_error' if 'standard_error' in significant_df.columns else 'SE'
        p_col = 'p_value' if 'p_value' in significant_df.columns else 'P'
        
        # Prepare COJO input format
        cojo_df = significant_df[[a1_col, a2_col, frq_col, beta_col, se_col, p_col, "N"]].copy()
        
        # Use variant_id as SNP (already in colon format: chr:pos:ref:alt after harmonization)
        cojo_df["SNP"] = significant_df.index
        logger.info(f"[COJO] Using variant_id as SNP identifier (sample: {cojo_df['SNP'].iloc[0]})")
        
        # Rename to GCTA expected column names
        cojo_df = cojo_df.rename(columns={
            a1_col: 'A1',
            a2_col: 'A2',
            frq_col: 'FRQ',
            beta_col: 'BETA',
            se_col: 'SE',
            p_col: 'P'
        })
        cojo_df = cojo_df[['SNP', 'A1', 'A2', 'FRQ', 'BETA', 'SE', 'P', 'N']]
        
        # Save COJO input file
        cojo_input_path = os.path.join(temp_dir, "cojo_input.txt")
        cojo_df.to_csv(cojo_input_path, sep=' ', index=False)
        logger.info(f"[COJO] COJO input prepared: {len(cojo_df)} variants")
        
        # Debug: Show first few rows
        logger.info(f"[COJO] Sample input (first 3 rows):")
        for i, row in cojo_df.head(3).iterrows():
            logger.info(f"  {row['SNP']} | A1={row['A1']} A2={row['A2']} | N={row['N']}")
        
        def run_cojo_for_chromosome(chrom):
            """Run COJO analysis for a single chromosome"""
            try:
                logger.info(f"[COJO] Processing chromosome {chrom}")
                
                # Get file pattern for this build
                file_pattern = config.get_plink_file_pattern(ref_genome, population, chrom)
                
                # Define paths for this chromosome
                plink_prefix = os.path.join(plink_dir, population, file_pattern)
                
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
                    logger.warning(f"[COJO] GCTA failed for chromosome {chrom}")
                    logger.warning(f"[COJO] STDOUT (full): {result.stdout}")  # Full output
                    logger.warning(f"[COJO] STDERR (full): {result.stderr}")  # Full output
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

# === FINE-MAPPING HELPER FUNCTIONS ===

def calculate_ld_matrix(chr_num, filtered_ids, sumstats, plink_dir, population, ref_genome, maf, config):
    try:
        with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.txt') as tmp_file:
            for snp_id in filtered_ids:
                tmp_file.write(f"{snp_id}\n")
            tmp_file_path = tmp_file.name
            
        with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.txt') as tmp_file_ld:
            tmp_file_ld_path = tmp_file_ld.name
            
        # Get file pattern for this build
        file_pattern = config.get_plink_file_pattern(ref_genome, population, chr_num)
        
        plink_cmd = [
            "plink",
            "--bfile", f"{plink_dir}/{population}/{file_pattern}",
            "--keep-allele-order",
            "--r", "square",
            "--extract", tmp_file_path,
            "--maf", str(maf),  
            "--write-snplist",
            "--out", tmp_file_ld_path
        ]
        
        logger.info(f"[LD] Running PLINK: {' '.join(plink_cmd)}")
        ld_run_res = run_command(' '.join(plink_cmd))
        
        if ld_run_res.returncode != 0:
            logger.error(f"[LD] PLINK failed: {ld_run_res.stderr}")
            try:
                os.remove(tmp_file_path)
            except:
                pass
            return None, None
        
        ld_out_path = f"{tmp_file_ld_path}.ld"
        ld_snplist_path = f"{tmp_file_ld_path}.snplist"
        if not os.path.exists(ld_out_path):
            logger.error(f"[LD] PLINK output not found: {ld_out_path}")
            try:
                os.remove(tmp_file_path)
            except:
                pass
            return None, None
            
        # Read SNP list
        snp_ids = []
        if os.path.exists(ld_snplist_path):
            with open(ld_snplist_path, 'r') as f:
                for line in f:
                    snp_ids.append(line.strip())
        else:
            logger.warning(f"[LD] SNP list not found: {ld_snplist_path}")
            snp_ids = filtered_ids

        # Read LD matrix
        ld_df = pd.read_csv(ld_out_path, sep=r'\s+', header=None)

        # Ensure dimensions match
        if len(snp_ids) != ld_df.shape[0]:
            logger.warning(f"[LD] SNP count mismatch: {len(snp_ids)} IDs vs {ld_df.shape[0]} rows")
            snp_ids = snp_ids[:ld_df.shape[0]]

        ld_df.index = snp_ids
        ld_df.columns = snp_ids
        ld_df.fillna(0, inplace=True)

        # Clean up files
        cleanup_files = [tmp_file_path, f"{tmp_file_ld_path}.log", 
                        f"{tmp_file_ld_path}.nosex", ld_out_path, ld_snplist_path]
        for cleanup_file in cleanup_files:
            try:
                if os.path.exists(cleanup_file):
                    os.remove(cleanup_file)
            except Exception as e:
                logger.warning(f"[LD] Could not remove temp file {cleanup_file}: {e}")
        
        # Get sub-region sumstats matching LD
        sub_region_sumstats_ld = sumstats.loc[ld_df.index]
        
        # Perform LD dimension check
        chr_col = 'chromosome' if 'chromosome' in sub_region_sumstats_ld.columns else 'CHR'
        bp_col = 'base_pair_location' if 'base_pair_location' in sub_region_sumstats_ld.columns else 'BP'
        
        snp_df_for_check = pd.DataFrame({
            'SNPID': sub_region_sumstats_ld.index,
            'CHR': sub_region_sumstats_ld[chr_col],
            'BP': sub_region_sumstats_ld[bp_col]
        })
        
        file_pattern = config.get_plink_file_pattern(ref_genome, population, chr_num)
        bim_file_path = f"{plink_dir}/{population}/{file_pattern}.bim"
        
        if os.path.exists(bim_file_path):
            try:
                filtered_snp_df = check_ld_dimensions(ld_df.values, snp_df_for_check, bim_file_path)
                
                if len(filtered_snp_df) != len(snp_df_for_check):
                    logger.info(f"[LD] Dimension check filtered SNPs: {len(snp_df_for_check)} → {len(filtered_snp_df)}")
                    # Re-filter LD matrix and sumstats to match filtered SNPs
                    kept_snp_ids = filtered_snp_df['SNPID'].tolist()
                    ld_df = ld_df.loc[kept_snp_ids, kept_snp_ids]
                    sub_region_sumstats_ld = sumstats.loc[kept_snp_ids]
            except Exception as e:
                logger.warning(f"[LD] check_ld_dimensions failed: {str(e)}, proceeding without filtering")
        else:
            logger.warning(f"[LD] BIM file not found: {bim_file_path}, skipping dimension check")
        
        # Check and fix LD matrix semi-definiteness
        LD_mat = check_ld_semidefiniteness(ld_df.values)
        ld_df = pd.DataFrame(LD_mat, index=ld_df.index, columns=ld_df.columns)
        
        logger.info(f"[LD] Final LD matrix shape: {ld_df.shape}")
        return ld_df, sub_region_sumstats_ld
        
    except Exception as e:
        logger.error(f"[LD] Error in LD calculation: {str(e)}")
        return None, None


def run_susie_finemapping(seed, ld_df, sub_region_sumstats_ld, chr_num, lead_variant_position, 
                          L=-1, coverage=0.95, min_abs_corr=0.5):
    try:
        # Validate inputs
        LD_mat = ld_df.values
        
        # Compute Z-scores
        beta_col = "BETA" if "BETA" in sub_region_sumstats_ld.columns else "beta"
        se_col = "SE" if "SE" in sub_region_sumstats_ld.columns else "standard_error"

        if beta_col in sub_region_sumstats_ld.columns and se_col in sub_region_sumstats_ld.columns:
            logger.info(f"[SUSIE] Calculating Z-scores from {beta_col}/{se_col}")
            beta = sub_region_sumstats_ld[beta_col].values
            se = sub_region_sumstats_ld[se_col].values
            zhat = (beta / se).reshape(len(sub_region_sumstats_ld), 1)
        else:
            logger.error(f"[SUSIE] Missing BETA or SE columns for Z-score calculation")
            return None
        
        if LD_mat.shape[0] != len(zhat):
            logger.error(f"[SUSIE] Dimension mismatch: LD={LD_mat.shape}, Z-scores={len(zhat)}")
            return None
        
        num_samples = int(sub_region_sumstats_ld["N"].iloc[0])
        
        # Check for invalid data
        if np.isnan(LD_mat).any() or np.isinf(LD_mat).any():
            logger.error(f"[SUSIE] Invalid LD matrix - contains NaN or inf values")
            return None
        if np.isnan(zhat).any() or np.isinf(zhat).any():
            logger.error(f"[SUSIE] Invalid Z-scores - contains NaN or inf values")
            return None
        
        # Check susieR availability
        try:
            if not check_r_package_available('susieR'):
                logger.error(f"[SUSIE] susieR package not available")
                return None
            logger.info(f"[SUSIE] susieR package available")
        except Exception as e:
            logger.error(f"[SUSIE] Error checking susieR availability: {e}")
            return None
        
        # Set R seed
        try:
            ro.r(f'set.seed({seed})')
        except Exception as e:
            logger.error(f"[SUSIE] Error setting R seed: {str(e)}")
            return None
        
        # Convert data to R objects
        with (ro.default_converter + numpy2ri.converter).context():
            try:
                zhat_r = ro.conversion.get_conversion().py2rpy(zhat)
                R_r = ro.conversion.get_conversion().py2rpy(LD_mat)
            except Exception as e:
                logger.error(f"[SUSIE] Error converting data to R objects: {str(e)}")
                return None
        
        # Import susieR
        try:
            local_susieR = importr('susieR')
            logger.info(f"[SUSIE] susieR imported successfully")
        except Exception as e:
            logger.error(f"[SUSIE] Error importing susieR: {e}")
            return None
        
        # Hyperparameter optimization if L not specified
        if L <= 0:
            def objective_susie(trial):
                L_trial = trial.suggest_int("L", 1, 20)
                logger.info(f"[SUSIE] Starting trial with L={L_trial}")                
                
                try:
                    susie_fit = local_susieR.susie_rss(z=zhat_r, R=R_r, L=L_trial, n=num_samples)
                    logger.info(f"[SUSIE] Execution completed for L={L_trial}")
                except Exception as e:
                    logger.error(f"[SUSIE] Execution failed for L={L_trial}: {str(e)}")
                    return -np.inf
                
                # Extract ELBO and convergence
                try:
                    try:
                        susie_fit_r = ListVector(susie_fit)
                        elbo = susie_fit_r.rx2('elbo')[-1]
                        converged = susie_fit_r.rx2('converged')[0]
                    except Exception:
                        ro.globalenv['temp_fit'] = susie_fit
                        elbo_r = ro.r('tail(temp_fit$elbo, 1)')
                        converged_r = ro.r('temp_fit$converged')
                        ro.r('rm(temp_fit)')
                        elbo = float(elbo_r[0])
                        converged = int(converged_r[0])
                    
                    if np.isnan(elbo) or np.isinf(elbo) or elbo > 0:
                        logger.warning(f"[SUSIE] L={L_trial} invalid ELBO: {elbo}")
                        return -np.inf
                    
                    if converged == 1:
                        logger.info(f"[SUSIE] L={L_trial} converged with ELBO {elbo:.6f}")
                        return elbo
                    else:
                        logger.warning(f"[SUSIE] L={L_trial} did not converge")
                        return -np.inf
                        
                except Exception as e:
                    logger.error(f"[SUSIE] Error extracting results for L={L_trial}: {str(e)}")
                    return -np.inf
            
            study = optuna.create_study(direction="maximize")
            study.optimize(objective_susie, n_trials=20)
            L = study.best_params["L"]
            logger.info(f"[SUSIE] Best L found: {L} with ELBO {study.best_value}")
        
        # Run SuSiE with final L
        logger.info(f"[SUSIE] Running SuSiE with L={L}")
        try:
            susie_fit = local_susieR.susie_rss(z=zhat_r, R=R_r, L=L, n=num_samples)
        except Exception as e:
            logger.error(f"[SUSIE] SuSiE execution failed: {str(e)}")
            return None
        
        # Extract credible sets and PIPs
        logger.info(f"[SUSIE] Extracting credible sets and PIPs...")
        
        # Extract credible sets
        try:
            credible_sets = local_susieR.susie_get_cs(susie_fit, coverage=coverage, min_abs_corr=min_abs_corr, Xcorr=R_r)
        except Exception as e:
            logger.error(f"[SUSIE] Error extracting credible sets: {str(e)}")
            credible_sets = None

        # Extract PIPs
        logger.info(f"[SUSIE] Extracting PIPs...")
        try:
            pips = np.array(local_susieR.susie_get_pip(susie_fit), dtype=np.float64)
            logger.info(f"[SUSIE] Extracted {len(pips)} PIPs")
        except Exception as e:
            logger.error(f"[SUSIE] susie_get_pip failed: {type(e).__name__}: {e}")
            return None
        
        # Ensure PIP array has correct length
        if len(pips) != len(sub_region_sumstats_ld):
            logger.warning(f"[SUSIE] Adjusting PIP length: {len(pips)} -> {len(sub_region_sumstats_ld)}")
            if len(pips) < len(sub_region_sumstats_ld):
                pips = np.pad(pips, (0, len(sub_region_sumstats_ld) - len(pips)), constant_values=0.01)
            else:
                pips = pips[:len(sub_region_sumstats_ld)]
        
        # Validate PIPs
        if len(pips) != len(sub_region_sumstats_ld):
            logger.error(f"[SUSIE] PIP length mismatch: {len(pips)} vs {len(sub_region_sumstats_ld)}")
            return None
            
        if np.any(pips < 0) or np.any(pips > 1):
            logger.warning(f"[SUSIE] Invalid PIPs detected, clipping to [0,1]")
            pips = np.clip(pips, 0, 1)
        
        # Force R garbage collection
        try:
            ro.r('gc()')
        except:
            pass
        
        # Add PIPs to results and initialize cs column
        result_df = sub_region_sumstats_ld.copy()
        result_df["PIP"] = pips
        result_df["cs"] = 0
        
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
                        valid_indices = python_indices[(python_indices >= 0) & (python_indices < len(result_df))]
                        if len(valid_indices) > 0:
                            result_df.iloc[valid_indices, result_df.columns.get_loc("cs")] = cs_idx + 1
                    
                    credible_mask = result_df["cs"] > 0
                    credible_snps = result_df[credible_mask].copy()
                    
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
                            pip_min = cs_subset['PIP'].min()
                            pip_max = cs_subset['PIP'].max()
                            logger.info(f"[SUSIE] Credible set {cs_num}: {len(cs_subset)} variants, PIP range: {pip_min:.6f}-{pip_max:.6f}")
                        
                        # Format for LocusZoom and return
                        return credible_snps 
                    else:
                        logger.warning(f"[SUSIE] No variants in credible sets")
                else:
                    logger.warning(f"[SUSIE] Empty or invalid credible sets data")
            else:
                logger.warning(f"[SUSIE] No credible sets found")
                
        except Exception as e:
            logger.warning(f"[SUSIE] Error processing credible sets: {str(e)}")
        
        logger.warning(f"[SUSIE] No credible sets identified")
        return None
        
    except Exception as e:
        logger.error(f"[SUSIE] Error in fine-mapping: {str(e)}")
        return None


# === FINE-MAPPING ORCHESTRATION ===
@task(cache_policy=None)
def finemap_region(seed, sumstats, chr_num, lead_variant_position, window=2000, 
                                 population="EUR", L=-1, coverage=0.95, min_abs_corr=0.5, maf=0.01, 
                                 ref_genome="GRCh38", plink_dir=None):
    try:     
        logger.info(f"[FINEMAP] Fine-mapping chr{chr_num}:{lead_variant_position} ±{window}kb")

        # Get config
        config = Config.from_env()
        
        # Step 1: Filter variants in the region
        window_bp = window * 1000
        start = lead_variant_position - window_bp
        end = lead_variant_position + window_bp
        
        chr_col = 'chromosome' if 'chromosome' in sumstats.columns else 'CHR'
        bp_col = 'base_pair_location' if 'base_pair_location' in sumstats.columns else 'BP'
        
        filtered_region = sumstats[(sumstats[chr_col] == chr_num) &
                                  (sumstats[bp_col] >= start) &
                                  (sumstats[bp_col] <= end)]
        
        filtered_ids = filtered_region.index.tolist()
        logger.info(f"[FINEMAP] Filtered {len(filtered_ids)} SNPs in region")
        
        if len(filtered_ids) < 2:
            logger.warning(f"[FINEMAP] Insufficient SNPs for chr{chr_num}:{lead_variant_position}")
            return None
        
        # Step 2: Calculate LD matrix
        ld_df, sub_region_sumstats_ld = calculate_ld_matrix(
            chr_num=chr_num,
            filtered_ids=filtered_ids,
            sumstats=sumstats,
            plink_dir=plink_dir,
            population=population,
            ref_genome=ref_genome,
            maf=maf,
            config=config
        )
        
        if ld_df is None or sub_region_sumstats_ld is None:
            logger.error(f"[FINEMAP] LD calculation failed for chr{chr_num}:{lead_variant_position}")
            return None
        
        logger.info(f"[FINEMAP] LD matrix calculated: {ld_df.shape}")
        
        # Step 3: Run SuSiE fine-mapping
        result = run_susie_finemapping(
            seed=seed,
            ld_df=ld_df,
            sub_region_sumstats_ld=sub_region_sumstats_ld,
            chr_num=chr_num,
            lead_variant_position=lead_variant_position,
            L=L,
            coverage=coverage,
            min_abs_corr=min_abs_corr
        )
        
        if result is not None:
            logger.info(f"[FINEMAP] Successfully fine-mapped chr{chr_num}:{lead_variant_position}")
        else:
            logger.warning(f"[FINEMAP] No credible sets identified for chr{chr_num}:{lead_variant_position}")
        
        return result
            
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
        ref_genome = finemap_params.get('ref_genome', 'GRCh38')
        maf_threshold = finemap_params.get('maf_threshold', 0.01)
        plink_dir = finemap_params.get('plink_dir', None)
        
        # Recreate database connection in worker process
        if db_params:
            try:
                # Import AnalysisHandler locally to avoid circular imports in multiprocessing
                from db import AnalysisHandler
                analysis_handler = AnalysisHandler(db_params['uri'], db_params['db_name'])
                logger.info(f"[BATCH-{batch_id}] Analysis handler connection recreated in worker process")
            except Exception as db_e:
                logger.error(f"[BATCH-{batch_id}] Error recreating analysis handler connection: {db_e}")
                analysis_handler = None
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
                    min_abs_corr=min_abs_corr,
                    maf=maf_threshold,
                    ref_genome=ref_genome,
                    plink_dir=plink_dir
                )
                
                if result is not None and len(result) > 0:
                    # Add batch and region metadata
                    result['batch_id'] = batch_id
                    result['region_idx'] = region_idx
                    result['processed_by'] = f"mp-worker-{os.getpid()}"
                    batch_results.append(result)
                    successful_regions += 1
                    
                    # Save to database if available
                    if analysis_handler and user_id and project_id:
                        try:
                            # Determine lead variant
                            try:
                                if 'cs' in result.columns and (result['cs'] > 0).any():
                                    candidate_df = result[result['cs'] > 0]
                                else:
                                    candidate_df = result

                                lead_variant_id = None

                                if 'PIP' in candidate_df.columns:
                                    try:
                                        lead_variant_id = candidate_df['PIP'].astype(float).idxmax()
                                    except Exception:
                                        lead_variant_id = None

                                if lead_variant_id is None:
                                    lead_variant_id = str(candidate_df.index[0])

                                logger.info(f"[BATCH-{batch_id}] Lead variant selected from results: {lead_variant_id}")
                            except Exception as lead_e:
                                logger.warning(f"[BATCH-{batch_id}] Lead selection from results failed ({lead_e}); using region seed {region['variant_id']}")
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
                                "ref_genome": ref_genome,
                                "maf_threshold": maf_threshold,
                                "seed": seed,
                                "L": L,
                                "coverage": coverage,
                                "min_abs_corr": min_abs_corr,
                                "total_variants_analyzed": len(result),
                                "credible_sets_count": len(credible_sets_data),
                                "completed_at": datetime.now().isoformat()
                            }
                            
                            # Save each credible set separately
                            for credible_set in credible_sets_data:
                                # Add metadata and completion time to each credible set
                                credible_set["metadata"] = metadata
                                credible_set["completed_at"] = metadata["completed_at"]
                                
                                # Save individual credible set
                                analysis_handler.save_credible_set(user_id, project_id, credible_set)
                            
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



