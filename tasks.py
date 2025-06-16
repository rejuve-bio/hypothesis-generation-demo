import glob
import os
from pathlib import Path
from typing import Dict, Optional
import logging
from flask import json
from prefect import task
from datetime import datetime, timezone
from uuid import uuid4
from socketio_instance import socketio
from status_tracker import status_tracker, TaskState
from utils import emit_task_update
from loguru import logger
import gwaslab as gl
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import gzip
import subprocess
from cyvcf2 import VCF, Writer
from prefect import task, flow
from typing import List, Dict, Optional
import numpy as np
import contextlib


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

### Enrich Tasks
@task(retries=2, cache_policy=None)
def check_enrich(db, current_user_id, credible_set_id, variant, phenotype, hypothesis_id):
    """Check if enrichment exists for credible set, variant, and phenotype"""
    try: 
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existence of enrichment data",
            state=TaskState.STARTED,
            progress=0  
        )
        
        if db.check_enrich(current_user_id, phenotype, variant):
            enrich = db.get_enrich_by_phenotype_and_variant(phenotype, variant, current_user_id)
            
            emit_task_update(
                hypothesis_id=hypothesis_id,
                task_name="Verifying existence of enrichment data",
                state=TaskState.COMPLETED,
                progress=80,
                details={"found": True, "enrich": enrich}
            )
            return enrich
            
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existence of enrichment data",
            state=TaskState.COMPLETED,
            details={"found": False},
            next_task="Getting candidate genes"
        )
        return None
        
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existence of enrichment data",
            state=TaskState.FAILED,
            error=str(e)
        )
        raise

@task(retries=2)
def get_candidate_genes(prolog_query, variant, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting candidate genes",
            state=TaskState.STARTED,
            next_task="Predicting causal gene",
        )

        result = prolog_query.get_candidate_genes(variant)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting candidate genes",
            state=TaskState.COMPLETED,
            details={"genes_count": len(result)}
        )
        return result
    
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting candidate genes",
            state=TaskState.FAILED,
            error=str(e)
        )
        raise

@task(retries=2)
def predict_causal_gene(llm, phenotype, candidate_genes, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Predicting causal gene",
            state=TaskState.STARTED,
            next_task="Getting relevant gene proof"
        )

        logger.info("Executing: predict causal gene")
        result = llm.predict_casual_gene(phenotype, candidate_genes)["causal_gene"]

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Predicting causal gene",
            state=TaskState.COMPLETED,
            details={"predicted_gene": result}
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Predicting causal gene",
            state=TaskState.FAILED,
            error=str(e)
        )
        raise

@task(retries=2)
def get_relevant_gene_proof(prolog_query, variant, causal_gene, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting relevant gene proof",
            state=TaskState.STARTED,
            next_task="Creating enrich data"
        )

        logger.info("Executing: get relevant gene proof")
        result = prolog_query.get_relevant_gene_proof(variant, causal_gene)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting relevant gene proof",
            state=TaskState.COMPLETED,
            details={"relevant_gene_proof": result}
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting relevant gene proof",
            state=TaskState.FAILED,
            next_task="Retrying to predict causal gene",
            error=str(e)          
        )
        raise

@task(retries=2)
def retry_predict_causal_gene(llm, phenotype, candidate_genes, proof, causal_gene, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to predict causal gene",
            state=TaskState.RETRYING,
            next_task="Retrying to get relevant gene proof"
        )

        logger.info(f"Retrying predict causal gene with proof: {proof}")
        result = llm.predict_casual_gene(phenotype, candidate_genes, rule=proof, prev_gene=causal_gene)["causal_gene"]

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to predict causal gene",
            state=TaskState.COMPLETED,
            details={"retry_predict_causal_gene": result}
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to predict causal gene",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(retries=2)
def retry_get_relevant_gene_proof(prolog_query, variant, causal_gene, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to get relevant gene proof",
            state=TaskState.RETRYING,
            next_task="Creating enrich data"
        )

        logger.info("Retrying get relevant gene proof")
        result = prolog_query.get_relevant_gene_proof(variant, causal_gene)
       
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to get relevant gene proof",
            state=TaskState.COMPLETED,
            details={"retry_relevant_gene_proof": result}
        ) 
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to get relevant gene proof",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise
        
@task(cache_policy=None)
def create_enrich_data(db, user_id, project_id, credible_set_id, variant, phenotype, causal_gene, relevant_gos, causal_graph, hypothesis_id):
    """Create enrichment data with project and credible set references"""
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Creating enrich data",
            state=TaskState.STARTED
        )

        logger.info("Creating enrich data in the database with project context")
        enrich_id = db.create_enrich(
            user_id, project_id, credible_set_id, variant, 
            phenotype, causal_gene, relevant_gos, causal_graph
        )

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Creating enrich data",
            state=TaskState.COMPLETED,
            details={"enrichment_id": enrich_id}
        )
        
        hypothesis_history = status_tracker.get_history(hypothesis_id)
        logger.info("Updating hypothesis in the database...")
        hypothesis_data = {
                "task_history": hypothesis_history,
            }
        db.update_hypothesis(hypothesis_id, hypothesis_data)

        return enrich_id
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Creating enrich data",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

### Hypothesis Tasks
@task(cache_policy=None, retries=2)
def check_hypothesis(db, current_user_id, enrich_id, go_id, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existence of hypothesis data",
            state=TaskState.STARTED,
            next_task="Getting enrichement data"
        )

        logger.info("Checking hypothesis data")
        if db.check_hypothesis(current_user_id, enrich_id, go_id):
            hypothesis = db.get_hypothesis_by_enrich_and_go(enrich_id, go_id, current_user_id)
            emit_task_update(
                hypothesis_id=hypothesis_id,
                task_name="Verifying existence of hypothesis data",
                state=TaskState.COMPLETED,
                progress=100,
                details={"found": True, "hypothesis": hypothesis}
            )
            return hypothesis
        
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existence of hypothesis data",
            state=TaskState.COMPLETED,
            details={"found": False}
        )
        return None
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existance of hypothesis data",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(cache_policy=None, retries=2)
def get_enrich(db, current_user_id, enrich_id, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting enrichement data",
            state=TaskState.STARTED,
            next_task="Getting gene data"
        )

        logger.info("Fetching enrich data...")
        result = db.get_enrich(current_user_id, enrich_id)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting enrichement data",
            state=TaskState.COMPLETED,
            details={"get_enrich": result}
        )
        return result

    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting enrichement data",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(retries=2)
def get_gene_ids(prolog_query, gene_names, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting gene data",
            state=TaskState.STARTED,
            next_task="Querying gene data"
        )

        logger.info("Fetching gene IDs...")
        result = prolog_query.get_gene_ids(gene_names)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting gene data",
            state=TaskState.COMPLETED,
            details={"get_gene_ids": result}
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting gene data",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(retries=2)
def execute_gene_query(prolog_query, query, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying gene data",
            state=TaskState.STARTED,
            next_task="Querying variant data"
        )

        logger.info("Executing Prolog query to retrieve gene names...")
        result = prolog_query.execute_query(query)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying gene data",
            state=TaskState.COMPLETED,
            details={"execute_gene_query": result}
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying gene data",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(retries=2)
def execute_variant_query(prolog_query, query, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying variant data",
            state=TaskState.STARTED,
            next_task="Querying phenotype data"
        )
        logger.info("Executing Prolog query to retrieve variant ids...")
        result = prolog_query.execute_query(query)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying variant data",
            state=TaskState.COMPLETED,
            details={"execute_variant_query": result}
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying variant data",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(retries=2)
def execute_phenotype_query(prolog_query, phenotype, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying phenotype data",
            state=TaskState.STARTED,
            next_task="Generating graph summary"
        )
        logger.info("Executing Prolog query to retrieve phenotype id...")
        result = prolog_query.execute_query(f"term_name(efo(X), {phenotype})")

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying phenotype data",
            state=TaskState.COMPLETED,
            details={"execute_phenotype_query": result}
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying phenotype data",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(retries=2)
def summarize_graph(llm, causal_graph, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating graph summary",
            state=TaskState.STARTED,
            next_task="Generating hypothesis"
        )

        logger.info("Summarizing causal graph...")
        result = llm.summarize_graph(causal_graph)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating graph summary",
            state=TaskState.COMPLETED,
            details={"summarize_graph": result}
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating graph summary",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(cache_policy=None, retries=2)
def create_hypothesis(db, enrich_id, go_id, variant_id, phenotype, causal_gene, causal_graph, summary, current_user_id, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating hypothesis",
            state=TaskState.STARTED,
            details={"go_id": go_id}
        )
        hypothesis_history = status_tracker.get_history(hypothesis_id)
        logger.info("Creating hypothesis in the database...")
        hypothesis_data = {
                "enrich_id": enrich_id,
                "go_id": go_id,
                "variant": variant_id,
                "phenotype": phenotype,
                "causal_gene": causal_gene,
                "graph": causal_graph,
                "summary": summary,
                "biological_context": "",
                "status": "completed",
                "task_history": hypothesis_history,
            }
        db.update_hypothesis(hypothesis_id, hypothesis_data)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating hypothesis",
            state=TaskState.COMPLETED,
            details={
                "status": "completed",
                "result": hypothesis_data
            }
        )
        hypothesis_history = status_tracker.get_history(hypothesis_id)
        logger.info("Updating hypothesis in the database...")
        hypothesis_data = {
                "task_history": hypothesis_history,
            }
        db.update_hypothesis(hypothesis_id, hypothesis_data)
        
        return hypothesis_id
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating hypothesis",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

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
def run_command(cmd: str) -> subprocess.CompletedProcess:
    """Execute a shell command and handle errors."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Error running command: {cmd}")
        logger.error(result.stderr)
        raise Exception(f"Command failed with exit code {result.returncode}")
    return result

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
    

@task(cache_policy=None)
def run_susie_analysis(snp_df, ld_matrix, n=503, L=10):
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
            # TODO: EXTRACT missing snps
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
        filtered_snp["cs"] = 0
        
        # Check if fit is valid
        logger.info(f"fit object type: {type(fit)}")
        
        # Debug the fit object
        try:
            if hasattr(fit, 'names'):
                logger.info(f"fit names: {list(fit.names)}")
            if hasattr(fit, 'rx2'):
                # Check key components of SuSiE results
                try:
                    alpha = fit.rx2('alpha')
                    logger.info(f"fit alpha shape: {alpha.shape if hasattr(alpha, 'shape') else 'no shape'}")
                except:
                    logger.info("No alpha in fit")
                
                try:
                    sets = fit.rx2('sets')
                    logger.info(f"fit sets: {sets}")
                except:
                    logger.info("No sets in fit")
        except Exception as e:
            logger.info(f"Error debugging fit object: {e}")
        
        # Try to get the credible sets
        try:
            cs_result = susieR.susie_get_cs(fit, coverage=0.95, min_abs_corr=0.5, Xcorr=R_df)
            logger.info(f"cs_result type: {type(cs_result)}")
            logger.info(f"cs_result keys: {list(cs_result.keys()) if hasattr(cs_result, 'keys') else 'No keys'}")
            
            # Debug: Check R object names/components
            if hasattr(cs_result, 'names'):
                logger.info(f"cs_result names: {list(cs_result.names)}")
            if hasattr(cs_result, 'rx2'):
                logger.info("cs_result has rx2 method")
            
            # Try to inspect the R object structure
            try:
                logger.info(f"cs_result str representation: {str(cs_result)[:500]}")
            except:
                logger.info("Could not get string representation of cs_result")
                
        except Exception as e:
            logger.info(f"Error getting credible sets: {e}")
            cs_result = None
        
        # Try to get the PIPs with proper error handling
        pip_extraction_success = False
        try:
            # Try multiple methods to get PIPs
            pips = None
            
            # Method 1: Direct function call
            try:
                pips = susieR.susie_get_pip(fit)
                logger.info("Successfully got PIPs using susie_get_pip")
            except Exception as e1:
                logger.info(f"susie_get_pip failed: {e1}")
                
                # Method 2: Try to extract PIPs from fit object directly
                try:
                    if hasattr(fit, 'rx2'):
                        pip_matrix = fit.rx2('pip')
                        if pip_matrix is not None:
                            pips = pip_matrix
                            logger.info("Successfully extracted PIPs from fit object")
                except Exception as e2:
                    logger.info(f"Direct PIP extraction failed: {e2}")
            
            # Process PIPs if we got them
            if pips is not None:
                # Convert R object to numpy array and handle data types
                try:
                    pip_array = np.array(pips, dtype=np.float64)
                    logger.info(f"PIP array shape: {pip_array.shape}")
                    logger.info(f"PIP array type: {pip_array.dtype}")
                    logger.info(f"PIP array range: {pip_array.min():.6f} to {pip_array.max():.6f}")
                    
                    # Ensure the array is 1D and matches the DataFrame length
                    if pip_array.ndim > 1:
                        pip_array = pip_array.flatten()
                    
                    # Handle length mismatch
                    if len(pip_array) != len(filtered_snp):
                        logger.warning(f"PIP array length ({len(pip_array)}) doesn't match SNP data length ({len(filtered_snp)})")
                        # Pad with zeros or truncate as needed
                        if len(pip_array) < len(filtered_snp):
                            pip_array = np.pad(pip_array, (0, len(filtered_snp) - len(pip_array)), constant_values=0.0)
                        else:
                            pip_array = pip_array[:len(filtered_snp)]
                    
                    # Create a clean copy of the DataFrame to avoid view issues
                    filtered_snp = filtered_snp.copy()
                    filtered_snp["pip"] = pip_array.astype(float)
                    pip_extraction_success = True
                    
                    # Log top PIPs for debugging
                    top_pips = filtered_snp.nlargest(5, 'pip')[['pip']]
                    logger.info(f"Top 5 PIPs:\n{top_pips}")
                    
                except Exception as e3:
                    logger.info(f"Error processing PIP array: {e3}")
            
        except Exception as e:
            logger.info(f"Error in PIP extraction: {e}")
        
        # Fallback if PIP extraction failed
        if not pip_extraction_success:
            logger.info("Warning: Could not extract PIPs, setting all to 0.0")
            filtered_snp = filtered_snp.copy()
            filtered_snp["pip"] = 0.0
        
        # Initialize as empty in case we don't have valid credible sets
        credible_snp_indices = []
        
        # Only process if we have valid credible sets
        if cs_result is not None:
            # Safely access the 'cs' key from R object
            try:
                # Try to get the 'cs' component from R object
                credible_sets = cs_result.rx2('cs') if hasattr(cs_result, 'rx2') else None
                if credible_sets is None:
                    # Try alternative access methods
                    try:
                        credible_sets = cs_result['cs']
                    except:
                        logger.info("No 'cs' component found in cs_result")
                        credible_sets = None
                
                if credible_sets is not None:
                    # Convert to a list if it's not already
                    if isinstance(credible_sets, dict):
                        # If 'cs' is itself a dictionary, extract its values
                        cs_values = list(credible_sets.values())
                        n_cs = len(cs_values)
                    else:
                        # Try to get the length of the credible sets
                        n_cs = len(credible_sets) if hasattr(credible_sets, '__len__') else 0
                        cs_values = credible_sets
                        
                    logger.info(f"Number of credible sets: {n_cs}")
                    
                    # Only proceed if we have credible sets and cs_index exists
                    cs_indices = None
                    try:
                        cs_indices = cs_result.rx2('cs_index') if hasattr(cs_result, 'rx2') else None
                        if cs_indices is None:
                            cs_indices = cs_result['cs_index']
                    except:
                        logger.info("No 'cs_index' component found in cs_result")
                        cs_indices = None
                    
                    if n_cs > 0 and cs_indices is not None:
                        # Process each credible set - fix for handling scalar values
                        for i, indices in enumerate(cs_indices):
                            # Handle different types of indices - could be scalar, list, array
                            if isinstance(indices, (np.int32, np.int64, int)):
                                # It's a single index - convert to Python 0-indexed
                                idx = int(indices) - 1
                                filtered_snp.loc[idx, "cs"] = i + 1
                                credible_snp_indices.append(idx)
                            elif hasattr(indices, '__len__') and len(indices) > 0:
                                # It's a sequence - convert all indices
                                indices_array = np.array(indices) - 1
                                # Mark these SNPs with their credible set number
                                filtered_snp.loc[indices_array, "cs"] = i + 1
                                credible_snp_indices.extend(indices_array)
                                
            except Exception as e:
                logger.info(f"Error processing credible sets from R object: {e}")
                # Continue with empty credible_snp_indices
        
        # If we found any credible SNPs, return them
        if credible_snp_indices:
            credible_snps = filtered_snp.loc[credible_snp_indices, :].copy()
            logger.info(f"Found {len(credible_snps)} SNPs in formal credible sets")
        else:
            # Otherwise, use PIP threshold as fallback
            try:
                # Ensure PIP column is numeric before comparison
                pip_values = pd.to_numeric(filtered_snp["pip"], errors='coerce').fillna(0.0)
                
                # Try different thresholds
                high_pip_mask = pip_values > 0.5
                medium_pip_mask = pip_values > 0.1
                low_pip_mask = pip_values > 0.01
                
                if high_pip_mask.sum() > 0:
                    credible_snps = filtered_snp.loc[high_pip_mask, :].copy()
                    logger.info(f"Using high PIP threshold (>0.5): {len(credible_snps)} SNPs")
                elif medium_pip_mask.sum() > 0:
                    credible_snps = filtered_snp.loc[medium_pip_mask, :].copy()
                    logger.info(f"Using medium PIP threshold (>0.1): {len(credible_snps)} SNPs")
                elif low_pip_mask.sum() > 0:
                    credible_snps = filtered_snp.loc[low_pip_mask, :].copy()
                    logger.info(f"Using low PIP threshold (>0.01): {len(credible_snps)} SNPs")
                else:
                    # Return top 10 SNPs by PIP if no SNPs meet any threshold
                    credible_snps = filtered_snp.nlargest(min(10, len(filtered_snp)), 'pip').copy()
                    logger.info(f"No SNPs meet PIP thresholds, returning top {len(credible_snps)} SNPs by PIP")
                    
            except Exception as e:
                logger.warning(f"Error filtering by PIP threshold: {e}")
                # Return top 5 SNPs as absolute fallback
                try:
                    credible_snps = filtered_snp.head(5).copy()
                    credible_snps["pip"] = 0.0  # Set default PIP
                    logger.info(f"Fallback: returning first {len(credible_snps)} SNPs")
                except:
                    # Return empty DataFrame with same structure if all else fails
                    credible_snps = filtered_snp.iloc[0:0].copy()
                    logger.warning("All fallbacks failed, returning empty DataFrame")
            
        logger.info(f"Final result: {len(credible_snps)} credible SNPs")
        return credible_snps

### Project-based Analysis Task

@task(cache_policy=None)
def save_analysis_state_task(db, user_id, project_id, state_data):
    """Save analysis state to file system"""
    try:
        db.save_analysis_state(user_id, project_id, state_data)
        logger.info(f"Saved analysis state for project {project_id}")
        return True
    except Exception as e:
        logger.info(f"Error saving analysis state: {str(e)}")
        raise

@task(cache_policy=None)
def load_analysis_state_task(db, user_id, project_id):
    """Load analysis state from file system"""
    try:
        state = db.load_analysis_state(user_id, project_id)
        if state:
            logger.info(f"Loaded analysis state for project {project_id}")
        else:
            logger.info(f"No analysis state found for project {project_id}")
        return state
    except Exception as e:
        logger.info(f"Error loading analysis state: {str(e)}")
        raise

@task(cache_policy=None)
def create_analysis_result_task(db, project_id, population, gene_types_identified):
    """Create analysis result entry in database"""
    try:
        analysis_id = db.create_analysis_result(project_id, population, gene_types_identified)
        logger.info(f"Created analysis result {analysis_id} for project {project_id}")
        return analysis_id
    except Exception as e:
        logger.info(f"Error creating analysis result: {str(e)}")
        raise

@task(cache_policy=None)
def create_credible_sets_task(db, analysis_id, credible_sets_data):
    """Create credible sets entries in database"""
    try:
        credible_set_ids = {}
        for gene_type, data in credible_sets_data.items():
            credible_set_id = db.create_credible_set(analysis_id, gene_type, data)
            credible_set_ids[gene_type] = credible_set_id
            logger.info(f"Created credible set {credible_set_id} for gene type {gene_type}")
        
        return credible_set_ids
    except Exception as e:
        logger.info(f"Error creating credible sets: {str(e)}")
        raise

@task(cache_policy=None)
def check_existing_credible_sets(db, analysis_id, requested_gene_types):
    """Check which credible sets already exist and which need to be computed"""
    try:
        existing_sets = {}
        missing_gene_types = []
        
        for gene_type in requested_gene_types:
            if db.check_credible_set_exists(analysis_id, gene_type):
                credible_set = db.get_credible_sets(analysis_id, gene_type=gene_type)
                if credible_set:
                    existing_sets[gene_type] = credible_set[0]  # Get first result
                    logger.info(f"Found existing credible set for {gene_type}")
            else:
                missing_gene_types.append(gene_type)
                logger.info(f"Need to compute credible set for {gene_type}")
        
        return existing_sets, missing_gene_types
    except Exception as e:
        logger.info(f"Error checking existing credible sets: {str(e)}")
        raise

@task(cache_policy=None)
def get_project_analysis_path_task(db, user_id, project_id):
    """Get the analysis path for a project"""
    try:
        analysis_path = db.get_project_analysis_path(user_id, project_id)
        
        # Create directory structure if it doesn't exist
        os.makedirs(analysis_path, exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "preprocessed_data"), exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "plink_binary"), exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "cojo"), exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "expanded_regions"), exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "ld"), exist_ok=True)
        
        logger.info(f"Project analysis path: {analysis_path}")
        return analysis_path
    except Exception as e:
        logger.info(f"Error getting project analysis path: {str(e)}")
        raise
