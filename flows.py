import asyncio
import os
import time
from flask import json
from loguru import logger
from prefect import flow
from status_tracker import TaskState
import multiprocessing as mp

from tasks import (
    check_enrich, create_enrich_data, get_candidate_genes, predict_causal_gene, 
    get_relevant_gene_proof, retry_predict_causal_gene, retry_get_relevant_gene_proof,
    check_hypothesis, get_enrich, get_gene_ids, execute_gene_query, execute_variant_query,
    summarize_graph, create_hypothesis, execute_phenotype_query
)

from analysis_tasks import (
    munge_sumstats_preprocessing, filter_significant_variants, run_cojo_per_chromosome, create_region_batches, finemap_region_batch_worker,
    save_sumstats_for_workers, cleanup_sumstats_file
)

from project_tasks import (
    save_analysis_state_task, create_analysis_result_task, 
    get_project_analysis_path_task
)

import pandas as pd
from datetime import datetime, timezone
from prefect.task_runners import ThreadPoolTaskRunner

from utils import emit_task_update
from config import Config, create_dependencies

### Enrichment Flow
@flow(log_prints=True, persist_result=False, task_runner=ThreadPoolTaskRunner(max_workers=4))
def enrichment_flow(current_user_id, phenotype, variant, hypothesis_id, project_id):
    """
    Fully project-based enrichment flow that initializes dependencies from centralized config
    """
    # Initialize dependencies from environment variables
    config = Config.from_env()
    deps = create_dependencies(config)
    
    enrichr = deps['enrichr']
    llm = deps['llm']
    prolog_query = deps['prolog_query']
    hypotheses = deps['hypotheses']
    
    try:
        logger.info(f"Running project-based enrichment for project {project_id}, variant {variant}")
        
        # Check for existing enrichment data
        enrich = check_enrich.submit(deps['enrichment'], current_user_id, variant, phenotype, hypothesis_id).result()
        
        if enrich:
            logger.info("Retrieved enrich data from saved db")
            return {"id": enrich['id']}, 200

        # Run enrichment analysis pipeline
        candidate_genes = get_candidate_genes.submit(prolog_query, variant, hypothesis_id).result()
        causal_gene = predict_causal_gene.submit(llm, phenotype, candidate_genes, hypothesis_id).result()
        causal_graph, proof = get_relevant_gene_proof.submit(prolog_query, variant, causal_gene, hypothesis_id).result()

        if causal_graph is None:
            causal_gene = retry_predict_causal_gene.submit(llm, phenotype, candidate_genes, proof, causal_gene, hypothesis_id).result()
            causal_graph, proof = retry_get_relevant_gene_proof.submit(prolog_query, variant, causal_gene, hypothesis_id).result()
            logger.info(f"Retried causal gene: {causal_gene}")
            logger.info(f"Retried causal graph: {causal_graph}")

        enrich_tbl = enrichr.run(causal_gene)
        relevant_gos = llm.get_relevant_go(phenotype, enrich_tbl)

        # Create enrichment data with project context
        enrich_id = create_enrich_data.submit(
            deps['enrichment'], current_user_id, project_id, variant, 
            phenotype, causal_gene, relevant_gos, causal_graph, hypothesis_id
        ).result()

        # Update hypothesis with enrichment ID
        hypotheses.update_hypothesis(hypothesis_id, {
            "enrich_id": enrich_id,
        })

        logger.info(f"Enrichment flow completed: {enrich_id}")
        
        return {"id": enrich_id}, 201
    except Exception as e:
        logger.error(f"Enrichment flow failed: {str(e)}")
        
        # Update hypothesis with error state
        hypotheses.update_hypothesis(hypothesis_id, {
            "status": "failed",
            "error": str(e),
            "updated_at": datetime.now(timezone.utc).isoformat(timespec='milliseconds') + "Z",
        })

        # Emit failure update
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Enrichment",
            state=TaskState.FAILED,
            error=str(e),
            progress=0
        )
        raise

### Hypothesis Flow
@flow(log_prints=True)
def hypothesis_flow(current_user_id, hypothesis_id, enrich_id, go_id, db, prolog_query, llm):
    hypothesis = check_hypothesis(db, current_user_id, enrich_id, go_id, hypothesis_id)
    if hypothesis:
        logger.info("Retrieved hypothesis data from saved db")
        return {"summary": hypothesis.get('summary'), "graph": hypothesis.get('graph')}, 200

    enrich_data = get_enrich(db, current_user_id, enrich_id, hypothesis_id)
    if not enrich_data:
        return {"message": "Invalid enrich_id or access denied."}, 404

    go_term = [go for go in enrich_data["GO_terms"] if go["id"] == go_id]
    go_name = go_term[0]["name"]
    causal_gene = enrich_data['causal_gene']
    variant_id = enrich_data['variant']
    phenotype = enrich_data['phenotype']
    coexpressed_gene_names = go_term[0]["genes"]
    causal_graph = enrich_data['causal_graph']

    logger.info(f"Enrich data: {enrich_data}")

    causal_gene_id = get_gene_ids(prolog_query, [causal_gene.lower()], hypothesis_id)[0]
    coexpressed_gene_ids = get_gene_ids(prolog_query, [g.lower() for g in coexpressed_gene_names], hypothesis_id)

    nodes, edges = causal_graph["nodes"], causal_graph["edges"]

    gene_nodes = [n for n in nodes if n["type"] == "gene"]
    gene_ids = [n['id'] for n in gene_nodes]
    gene_entities = [f"gene({id})" for id in gene_ids]
    query = f"maplist(gene_name, {gene_entities}, X)".replace("'", "")

    gene_names = execute_gene_query(prolog_query, query, hypothesis_id)
    for id, name, node in zip(gene_ids, gene_names, gene_nodes):
        node["id"] = id
        node["name"] = name.upper()
    
    variant_nodes = [n for n in nodes if n["type"] == "snp"]
    variant_rsids = [n['id'] for n in variant_nodes]
    variant_entities = [f"snp({id})" for id in variant_rsids]
    query = f"maplist(variant_id, {variant_entities}, X)".replace("'", "")

    variant_ids = execute_variant_query(prolog_query, query, hypothesis_id)
    for variant_id, rsid, node in zip(variant_ids, variant_rsids, variant_nodes):
        variant_id = variant_id.replace("'", "")
        node["id"] = variant_id
        node["name"] = rsid
        source_edges = [e for e in edges if e["source"] == rsid]
        target_edges = [e for e in edges if e["target"] == rsid]
        for edge in source_edges:
            edge["source"] = variant_id
        for edge in target_edges:
            edge["target"] = variant_id
            
    nodes.append({"id": go_id, "type": "go", "name": go_name})
    phenotype_id = execute_phenotype_query(prolog_query, phenotype, hypothesis_id)

    nodes.append({"id": phenotype_id, "type": "phenotype", "name": phenotype})
    edges.append({"source": go_id, "target": phenotype_id, "label": "involved_in"})
    for gene_id, gene_name in zip(coexpressed_gene_ids, coexpressed_gene_names):
        nodes.append({"id": gene_id, "type": "gene", "name": gene_name})
        edges.append({"source": gene_id, "target": go_id, "label": "enriched_in"})
        edges.append({"source": causal_gene_id, "target": gene_id, "label": "coexpressed_with"})


    causal_graph = {"nodes": nodes, "edges": edges}

    summary = summarize_graph(llm, causal_graph, hypothesis_id)

    
    hypothesis_id = create_hypothesis(db, enrich_id, go_id, variant_id, phenotype, causal_gene, causal_graph, summary, current_user_id, hypothesis_id)

    
    return {"summary": summary, "graph": causal_graph}, 201



@flow(log_prints=True)
def analysis_pipeline_flow(projects_handler, analysis_handler, mongodb_uri, db_name, user_id, project_id, gwas_file_path, ref_genome="GRCh37", 
                           population="EUR", batch_size=5, max_workers=3,
                           maf_threshold=0.01, seed=42, window=2000, L=-1, 
                           coverage=0.95, min_abs_corr=0.5):
    """
    Complete analysis pipeline flow using Prefect for orchestration
    but multiprocessing for fine-mapping batches (R safety)
    """
    
    logger.info(f"[PIPELINE] Starting Prefect analysis pipeline with multiprocessing fine-mapping")
    logger.info(f"[PIPELINE] Project: {project_id}, User: {user_id}")
    logger.info(f"[PIPELINE] File: {gwas_file_path}")
    logger.info(f"[PIPELINE] Batch size: {batch_size} regions per worker process")
    logger.info(f"[PIPELINE] Max workers: {max_workers}")
    logger.info(f"[PIPELINE] Parameters: maf={maf_threshold}, seed={seed}, window={window}kb, L={L}, coverage={coverage}, min_abs_corr={min_abs_corr}")
    
    try:
        # Get project-specific output directory (using Prefect task)
        output_dir = get_project_analysis_path_task.submit(projects_handler, user_id, project_id).result()
        logger.info(f"[PIPELINE] Using output directory: {output_dir}")
        
        # Save initial analysis state
        initial_state = {
            "status": "Running",
            "stage": "Preprocessing",
            "progress": 10,
            "message": "Starting MungeSumstats preprocessing",
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        save_analysis_state_task.submit(projects_handler, user_id, project_id, initial_state).result()
        
        logger.info(f"[PIPELINE] Stage 1: MungeSumstats preprocessing")
        munged_file_result = munge_sumstats_preprocessing.submit(gwas_file_path, output_dir, ref_genome=ref_genome, n_threads=14).result()
        
        # Extract the actual file path from the result
        if isinstance(munged_file_result, tuple):
            munged_df, munged_file = munged_file_result
        else:
            munged_file = munged_file_result
            munged_df = pd.read_csv(munged_file, sep='\t')
        
        # Update analysis state after preprocessing
        preprocessing_state = {
            "status": "Running",
            "stage": "Filtering",
            "progress": 30,
            "message": "Preprocessing completed, filtering significant variants",
            "started_at": initial_state["started_at"]
        }
        save_analysis_state_task.submit(projects_handler, user_id, project_id, preprocessing_state).result()
        
        logger.info(f"[PIPELINE] Stage 2: Loading and filtering variants")
        significant_df_result = filter_significant_variants.submit(munged_df, output_dir).result()
        
        # Extract the actual DataFrame
        if isinstance(significant_df_result, tuple):
            significant_df, _ = significant_df_result
        else:
            significant_df = significant_df_result
        
        # Update analysis state after filtering
        filtering_state = {
            "status": "Running",
            "stage": "Cojo",
            "progress": 50,
            "message": "Filtering completed, running COJO analysis"
        }
        save_analysis_state_task.submit(projects_handler, user_id, project_id, filtering_state).result()
        
        logger.info(f"[PIPELINE] Stage 3: COJO analysis")
       
        config = Config.from_env()
        plink_dir = config.plink_dir
        cojo_result = run_cojo_per_chromosome.submit(significant_df, plink_dir, output_dir, maf_threshold=maf_threshold, population=population).result()
        
        # Extract the actual DataFrame
        if isinstance(cojo_result, tuple):
            cojo_results, _ = cojo_result
        else:
            cojo_results = cojo_result
        
        if cojo_results is None or len(cojo_results) == 0:
            logger.error("[PIPELINE] No COJO results to process")
            # Save failed state
            failed_state = {
                "status": "Failed",
                "stage": "Cojo",
                "progress": 50,
                "message": "COJO analysis failed - no independent signals found",
            }
            save_analysis_state_task.submit(projects_handler, user_id, project_id, failed_state).result()
            return None
        
        # Update analysis state after COJO
        cojo_state = {
            "status": "Running",
            "stage": "Fine_mapping",
            "progress": 70,
            "message": "COJO analysis completed, starting fine-mapping"
        }
        save_analysis_state_task.submit(projects_handler, user_id, project_id, cojo_state).result()
        
        logger.info(f"[PIPELINE] Stage 4: Multiprocessing fine-mapping)")
        logger.info(f"[PIPELINE] Processing {len(cojo_results)} regions with {batch_size} regions per batch")
        
        region_batches = create_region_batches(cojo_results, batch_size=batch_size)
        logger.info(f"[PIPELINE] Created {len(region_batches)} batches for {max_workers} worker processes")
        
        sumstats_temp_file = save_sumstats_for_workers(significant_df, output_dir)
        
        # Prepare batch data for multiprocessing
        batch_data_list = []
        for i, batch in enumerate(region_batches):
            db_params = {
                'uri': mongodb_uri,
                'db_name': db_name
            }
            batch_data = (batch, f"batch_{i}", sumstats_temp_file, {
                'db_params': db_params,
                'user_id': user_id,
                'project_id': project_id,
                'finemap_params': {
                    'seed': seed,
                    'window': window,
                    'L': L,
                    'coverage': coverage,
                    'min_abs_corr': min_abs_corr,
                    'population': population
                }
            })
            batch_data_list.append(batch_data)
        
        original_method = mp.get_start_method()
        if original_method != 'spawn':
            logger.info(f"[PIPELINE] Switching multiprocessing method from '{original_method}' to 'spawn' to reduce memory usage")
            mp.set_start_method('spawn', force=True)
        
        all_results = []
        successful_batches = 0
        failed_batches = 0
        
        try:
            with mp.Pool(max_workers) as pool:
                try:
                    # Process all batches in parallel
                    batch_results_list = pool.map(finemap_region_batch_worker, batch_data_list)
                    
                    # Collect results
                    for i, batch_results in enumerate(batch_results_list):
                        if batch_results and len(batch_results) > 0:
                            all_results.extend(batch_results)
                            successful_batches += 1
                            logger.info(f"[PIPELINE] Batch {i} completed with {len(batch_results)} regions")
                        else:
                            failed_batches += 1
                            logger.warning(f"[PIPELINE] Batch {i} failed or returned no results")
                            
                except Exception as e:
                    logger.error(f"[PIPELINE] Error in multiprocessing: {str(e)}")
                    raise
                finally:
                    # Clean up temporary sumstats file after all workers are done
                    cleanup_sumstats_file(sumstats_temp_file)
        finally:
            # Restore original multiprocessing method
            if original_method != 'spawn':
                try:
                    mp.set_start_method(original_method, force=True)
                    logger.info(f"[PIPELINE] Restored multiprocessing method to '{original_method}'")
                except:
                    logger.warning(f"[PIPELINE] Could not restore multiprocessing method to '{original_method}'")
        
        # Combine and save results
        if all_results:
            logger.info(f"[PIPELINE] Combining results from {successful_batches} successful batches")
            combined_results = pd.concat(all_results, ignore_index=True)
            
            # Save results using Prefect tasks
            results_file = create_analysis_result_task.submit(analysis_handler, user_id, project_id, combined_results, output_dir).result()
            
            # Summary statistics
            total_variants = len(combined_results)
            high_pip_variants = len(combined_results[combined_results['PIP'] > 0.5])
            total_credible_sets = combined_results.get('credible_set', pd.Series([0])).max()
            
            # Save completed analysis state
            completed_state = {
                "status": "Completed",
                "progress": 100,
                "message": "Analysis completed successfully",
            }
            save_analysis_state_task.submit(projects_handler, user_id, project_id, completed_state).result()
            
            logger.info(f"[PIPELINE] Analysis completed successfully!")
            logger.info(f"[PIPELINE] - Total variants: {total_variants}")
            logger.info(f"[PIPELINE] - High-confidence variants (PIP > 0.5): {high_pip_variants}")
            logger.info(f"[PIPELINE] - Total credible sets: {total_credible_sets}")
            logger.info(f"[PIPELINE] - Successful batches: {successful_batches}/{len(region_batches)}")
            logger.info(f"[PIPELINE] - Results saved: {results_file}")
            
            return {
                "results_file": results_file,
                "total_variants": total_variants,
                "high_pip_variants": high_pip_variants,
                "total_credible_sets": total_credible_sets
            }
        else:
            logger.error("[PIPELINE]  No fine-mapping results generated")
            # Save failed state for fine-mapping
            failed_finemap_state = {
                "status": "Failed",
                "stage": "Fine_mapping",
                "progress": 70,
                "message": "Fine-mapping failed - no results generated",
            }
            save_analysis_state_task.submit(projects_handler, user_id, project_id, failed_finemap_state).result()
            raise RuntimeError("All fine-mapping batches failed")
            
    except Exception as e:
        logger.error(f"[PIPELINE]  Analysis pipeline failed: {str(e)}")
        # Save failed analysis state
        try:
            failed_state = {
                "status": "Failed",
                "stage": "Unknown",
                "progress": 0,
                "message": f"Analysis pipeline failed: {str(e)}",
            }
            save_analysis_state_task.submit(projects_handler, user_id, project_id, failed_state).result()
        except Exception as state_e:
            logger.error(f"[PIPELINE] Failed to save error state: {str(state_e)}")
        raise
