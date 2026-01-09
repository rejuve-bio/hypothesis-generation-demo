from loguru import logger
from prefect import flow
from status_tracker import TaskState
import multiprocessing as mp

from tasks import (
    check_enrich, create_enrich_data, get_candidate_genes,
    get_relevant_gene_proof, retry_get_relevant_gene_proof,
    check_hypothesis, get_enrich, get_gene_ids, execute_gene_query, execute_variant_query,
    summarize_graph, create_hypothesis, execute_phenotype_query,
    extract_causal_gene_from_graph, 
    process_child_enrichments_simple
)

from analysis_tasks import (
    harmonize_sumstats_with_nextflow, filter_significant_variants, run_cojo_per_chromosome, create_region_batches, finemap_region_batch_worker,
    save_sumstats_for_workers, cleanup_sumstats_file
)

from project_tasks import (
    save_analysis_state_task, create_analysis_result_task, 
    get_project_analysis_path_task
)

from gene_expression_tasks import (
run_combined_ldsc_tissue_analysis
)

import pandas as pd
from datetime import datetime, timezone
from prefect.task_runners import ThreadPoolTaskRunner

from utils import emit_task_update
from config import Config, create_dependencies
from threading import Thread
import traceback


### Enrichment Flow
@flow(log_prints=True, persist_result=False, task_runner=ThreadPoolTaskRunner(max_workers=4))
def enrichment_flow(current_user_id, phenotype, variant, hypothesis_id, project_id, seed):
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
    gene_expression = deps['gene_expression']
    projects = deps['projects']
    enrichment = deps['enrichment']
    
    try:
        logger.info(f"Running project-based enrichment for project {project_id}, variant {variant}")
        
        # Check for existing enrichment data
        enrich = check_enrich.submit(enrichment, current_user_id, variant, phenotype, hypothesis_id).result()
        
        if enrich:
            logger.info("Retrieved enrich data from saved db")
            return {"id": enrich['id']}, 200

        candidate_genes = get_candidate_genes.submit(prolog_query, variant, hypothesis_id).result()
        graphs_list = get_relevant_gene_proof.submit(prolog_query, variant, hypothesis_id, seed).result()

        if not graphs_list or len(graphs_list) == 0:
            graphs_list = retry_get_relevant_gene_proof.submit(prolog_query, variant, hypothesis_id, seed).result()
            logger.info(f"Retried graphs: {len(graphs_list) if graphs_list else 0} graphs received")
        
        # If still no graphs after retry, fail the enrichment
        if not graphs_list or len(graphs_list) == 0:
            error_msg = f"No causal graphs found for variant {variant}. Prolog server returned 0 graphs."
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.info(f"Creating enrichments for {len(graphs_list)} graphs from Prolog server")

        # Sort graphs by probability (highest first)
        graphs_with_prob = []
        for i, graph in enumerate(graphs_list):
            prob = graph.get('prob', {}).get('value', 0.0)
            graphs_with_prob.append((i, graph, prob))
        
        graphs_with_prob.sort(key=lambda x: x[2], reverse=True)
        logger.info(f"Graph probabilities: {[(i, prob) for i, _, prob in graphs_with_prob]}")

        # Extract causal genes from ALL graphs
        graph_genes = []
        for idx, (original_i, graph, prob) in enumerate(graphs_with_prob):
            # Extract variant nodes from the graph
            variant_nodes = [n for n in graph.get("nodes", []) if n.get("type") == "snp"]
            
            gene_id, gene_name = extract_causal_gene_from_graph(graph, variant_nodes)
            
            # Use extracted gene or fail 
            extracted_gene = gene_name or gene_id
            if not extracted_gene:
                error_msg = f"No causal gene found in graph {idx+1}/{len(graphs_with_prob)} (prob={prob:.3f}). Graph may contain no genes or no direct SNP-gene connections."
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            this_causal_gene = extracted_gene
            logger.info(f"Graph {idx+1}: Extracted causal gene '{this_causal_gene}' (prob={prob:.3f})")
            graph_genes.append((idx, this_causal_gene))
        
        # Check if all graphs have the same causal gene
        unique_genes = set(gene for _, gene in graph_genes)
        use_shared_enrichment = len(unique_genes) == 1
        
        # Get tissue selection for enrichment
        tissue_selection = gene_expression.get_tissue_selection(current_user_id, project_id, variant)
        selected_tissue = None
        
        if tissue_selection:
            selected_tissue = tissue_selection['tissue_name']
            logger.info(f"Found user tissue selection: {selected_tissue} for variant {variant}")
        else:
            # Get top tissue from LDSC analysis (from MongoDB)
            ldsc_results = gene_expression.get_ldsc_results_for_project(current_user_id, project_id, limit=1, format='selection')
            if ldsc_results and len(ldsc_results) > 0:
                selected_tissue = ldsc_results[0].get('tissue_name')
                logger.info(f"Using top tissue from LDSC (MongoDB): {selected_tissue}")

        # Create enrichments for all graphs
        enrichment_data = []
        main_enrichment_id = None
        shared_enrichment_cache = {}  # Cache for shared enrichments
        
        for idx, (original_i, graph, prob) in enumerate(graphs_with_prob):
            this_causal_gene = graph_genes[idx][1]
            
            if use_shared_enrichment and this_causal_gene in shared_enrichment_cache:
                logger.info(f"Reusing shared enrichment for gene {this_causal_gene}")
                relevant_gos = shared_enrichment_cache[this_causal_gene]
            else:
                # Run enrichment for this specific gene
                logger.info(f"Running enrichment for gene {this_causal_gene} (graph {idx+1}/{len(graphs_with_prob)})")
                
                emit_task_update(
                    hypothesis_id=hypothesis_id,
                    task_name=f"Enrichment Analysis ({idx+1}/{len(graphs_with_prob)})",
                    state=TaskState.STARTED,
                    details={"causal_gene": this_causal_gene, "tissue": selected_tissue or "standard"},
                    progress=45 + (idx * 15 // len(graphs_with_prob))
                )
                
                if selected_tissue:
                    enrich_tbl = enrichr.run(this_causal_gene, tissue_name=selected_tissue, 
                                            user_id=current_user_id, project_id=project_id)
                else:
                    enrich_tbl = enrichr.run(this_causal_gene)
                
                relevant_gos = llm.get_relevant_go(phenotype, enrich_tbl)
                
                # Cache if shared
                if use_shared_enrichment:
                    shared_enrichment_cache[this_causal_gene] = relevant_gos
            
            # Store metadata for the highest probability graph in the main hypothesis
            if idx == 0:
                best_causal_gene = this_causal_gene
                
                # Update hypothesis with best graph metadata
                hypotheses.update_hypothesis(hypothesis_id, {
                    "causal_gene": best_causal_gene,
                    "enrichment_stage": "enrichment_running"
                })
            
            # Create enrichment for this graph
            enrich_id = create_enrich_data.submit(
                enrichment, hypotheses, current_user_id, project_id, variant, 
                phenotype, this_causal_gene, relevant_gos, {
                    "graph": graph,
                    "graph_index": original_i,
                    "total_graphs": len(graphs_list),
                }, hypothesis_id
            ).result()            
            enrichment_data.append({
                "enrich_id": enrich_id,
                "graph_index": original_i,
                "graph_probability": prob,
                "causal_gene": this_causal_gene
            })
            
            if idx == 0:
                main_enrichment_id = enrich_id

        all_enrich_ids = [e['enrich_id'] for e in enrichment_data]
        
        # Update original hypothesis with main enrichment and children info
        hypotheses.update_hypothesis(hypothesis_id, {
            "enrich_id": main_enrichment_id,
            "child_enrich_ids": all_enrich_ids[1:],
            "status": "pending"
        })

        logger.info(f"Created {len(enrichment_data)} enrichments, main: {main_enrichment_id}")
        logger.info(f"Child enrichments (will be processed on-demand): {all_enrich_ids[1:]}")

        
        # Return main enrichment
        return {"id": main_enrichment_id}, 200
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
def hypothesis_flow(current_user_id, hypothesis_id, enrich_id, go_id, hypotheses, prolog_query, llm, enrichment):
    
    hypothesis = check_hypothesis(hypotheses, current_user_id, enrich_id, go_id, hypothesis_id)
    if hypothesis:
        logger.info("Retrieved hypothesis data from saved db")
        return {"summary": hypothesis.get('summary'), "graph": hypothesis.get('graph')}, 200
    # Check if this hypothesis has child enrichments
    parent_hypothesis = hypotheses.get_hypotheses(current_user_id, hypothesis_id)
    if parent_hypothesis and 'child_enrich_ids' in parent_hypothesis:
        child_enrich_ids = parent_hypothesis.get('child_enrich_ids', [])
        if child_enrich_ids and len(child_enrich_ids) > 0:
            logger.info(f"Triggering background processing for {len(child_enrich_ids)} child enrichments")
                        
            # Create deps dict from current context
            deps_for_bg = {
                'hypotheses': hypotheses,
                'enrichment': enrichment,
                'prolog_query': prolog_query,
                'llm': llm
            }
            
            def run_background_hypotheses():
                try:
                    process_child_enrichments_simple(
                        current_user_id, child_enrich_ids, hypothesis_id, deps_for_bg
                    )
                except Exception as bg_e:
                    logger.error(f"Background child hypothesis generation failed: {str(bg_e)}")
                    logger.error(traceback.format_exc())
            
            bg_thread = Thread(target=run_background_hypotheses)
            bg_thread.start()
            logger.info(f"Background thread started for child enrichments (processing in parallel)")

    enrich_data = get_enrich(enrichment, current_user_id, enrich_id, hypothesis_id)
    if not enrich_data:
        return {"message": "Invalid enrich_id or access denied."}, 404

    go_term = [go for go in enrich_data["GO_terms"] if go["id"] == go_id]
    go_name = go_term[0]["name"]
    causal_gene = enrich_data['causal_gene']
    variant_id = enrich_data['variant']
    phenotype = enrich_data['phenotype']
    coexpressed_gene_names = go_term[0]["genes"]
    causal_graph_data = enrich_data['causal_graph']
    
    graph = causal_graph_data["graph"]
    graph_index = causal_graph_data.get("graph_index", 0)
    total_graphs = causal_graph_data.get("total_graphs", 1)
        
    graph_prob = graph.get('prob', {}).get('value', 1.0)
    logger.info(f"Processing graph {graph_index + 1}/{total_graphs} with probability {graph_prob}")
    
    causal_graph = graph      
    
    coexpressed_gene_ids = get_gene_ids(prolog_query, [g.lower() for g in coexpressed_gene_names], hypothesis_id)

    nodes, edges = causal_graph["nodes"], causal_graph["edges"]
    
    causal_gene_id = causal_gene.lower()
    causal_gene_name = causal_gene.upper()
    logger.info(f"Using causal gene from enrichment: {causal_gene_name} (ID: {causal_gene_id})")
    
    # Standardize variant IDs
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

    gene_nodes = [n for n in nodes if n["type"] == "gene"]
    gene_ids = [n['id'] for n in gene_nodes]
    gene_entities = [f"gene({id})" for id in gene_ids]
    query = f"maplist(gene_name, {gene_entities}, X)".replace("'", "")

    gene_names = execute_gene_query(prolog_query, query, hypothesis_id)
    for id, name, node in zip(gene_ids, gene_names, gene_nodes):
        node["id"] = id
        node["name"] = name.upper()
    
    nodes.append({"id": go_id, "type": "go", "name": go_name})
    phenotype_result = execute_phenotype_query(prolog_query, phenotype, hypothesis_id)
    
    phenotype_id = phenotype_result[0] if isinstance(phenotype_result, list) and phenotype_result else phenotype_result

    nodes.append({"id": phenotype_id, "type": "phenotype", "name": phenotype})
    edges.append({"source": go_id, "target": phenotype_id, "label": "involved_in"})
    for gene_id, gene_name in zip(coexpressed_gene_ids, coexpressed_gene_names):
        nodes.append({"id": gene_id, "type": "gene", "name": gene_name})
        edges.append({"source": gene_id, "target": go_id, "label": "enriched_in"})
        edges.append({"source": causal_gene_id, "target": gene_id, "label": "coexpressed_with"})

    final_causal_graph = {"nodes": nodes, "edges": edges, "probability": graph_prob}

    summary = summarize_graph(llm, {"nodes": nodes, "edges": edges}, hypothesis_id)

    create_hypothesis(hypotheses, enrich_id, go_id, variant_id, phenotype, causal_gene_name, final_causal_graph, 
                     summary, current_user_id, hypothesis_id)
    
    return {"summary": summary, "graph": final_causal_graph}, 201


@flow(log_prints=True)
def analysis_pipeline_flow(projects_handler, analysis_handler,gene_expression, mongodb_uri, db_name, user_id, project_id, gwas_file_path, ref_genome="GRCh38", 
                           population="EUR", batch_size=5, max_workers=3,
                           maf_threshold=0.01, seed=42, window=2000, L=-1, 
                           coverage=0.95, min_abs_corr=0.5, sample_size=None):
    """
    Complete analysis pipeline flow using Prefect for orchestration
    but multiprocessing for fine-mapping batches (R safety)
    """
    
    logger.info(f"[PIPELINE] Starting Prefect analysis pipeline with multiprocessing fine-mapping")
    logger.info(f"[PIPELINE] Project: {project_id}, User: {user_id}")
    logger.info(f"[PIPELINE] File: {gwas_file_path}")
    logger.info(f"[PIPELINE] Batch size: {batch_size} regions per worker process")
    logger.info(f"[PIPELINE] Max workers: {max_workers}")
    logger.info(f"[PIPELINE] Parameters: maf={maf_threshold}, seed={seed}, window={window}kb, L={L}, coverage={coverage}, min_abs_corr={min_abs_corr}, N={sample_size}")
    
    try:
        # Get project-specific output directory (using Prefect task)
        output_dir = get_project_analysis_path_task.submit(projects_handler, user_id, project_id).result()
        logger.info(f"[PIPELINE] Using output directory: {output_dir}")
        
        # Save initial analysis state
        initial_state = {
            "status": "Running",
            "stage": "Harmonization",
            "progress": 10,
            "message": "Starting Nextflow harmonization",
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        save_analysis_state_task.submit(projects_handler, user_id, project_id, initial_state).result()
        
        logger.info(f"[PIPELINE] Stage 1: Nextflow harmonization")
        harmonized_file_result = harmonize_sumstats_with_nextflow.submit(
            gwas_file_path, output_dir, ref_genome=ref_genome, sample_size=sample_size
        ).result()
        
        # Extract the actual file path from the result
        if isinstance(harmonized_file_result, tuple):
            harmonized_df, harmonized_file = harmonized_file_result
        else:
            harmonized_file = harmonized_file_result
            harmonized_df = pd.read_csv(harmonized_file, sep='\t', index_col=0)

        # Start LDSC + tissue analysis immediately after harmonization (runs in parallel)
        logger.info(f"[PIPELINE] Starting LDSC + tissue analysis in parallel after harmonization")
        ldsc_tissue_future = run_combined_ldsc_tissue_analysis.submit(gene_expression, projects_handler,
            harmonized_file, output_dir, project_id, user_id
        )
        logger.info(f"[PIPELINE] LDSC + tissue analysis started in background")
        
        # Update analysis state after harmonization
        harmonization_state = {
            "status": "Running",
            "stage": "Filtering",
            "progress": 30,
            "message": "Harmonization completed, filtering significant variants",
            "started_at": initial_state["started_at"]
        }
        save_analysis_state_task.submit(projects_handler, user_id, project_id, harmonization_state).result()
        
        logger.info(f"[PIPELINE] Stage 2: Loading and filtering variants")
        significant_df_result = filter_significant_variants.submit(harmonized_df, output_dir).result()
        
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
                    'population': population,
                    'ref_genome': ref_genome,
                    'maf_threshold': maf_threshold
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
            
            # Wait for parallel LDSC + tissue analysis to complete
            logger.info(f"[PIPELINE] Stage 5: Waiting for LDSC + Tissue Analysis to complete")
            
            # Update analysis state for waiting on LDSC + tissue analysis
            ldsc_tissue_state = {
                "status": "Running",
                "stage": "LDSC_Tissue_Analysis",
                "progress": 85,
                "message": "Fine-mapping completed, waiting for LDSC and tissue analysis"
            }
            save_analysis_state_task.submit(projects_handler, user_id, project_id, ldsc_tissue_state).result()
            
            try:
                # Wait for the parallel LDSC + tissue analysis task to complete
                ldsc_tissue_result = ldsc_tissue_future.result()
                
                logger.info(f"[PIPELINE] LDSC + tissue analysis completed successfully!")
                logger.info(f"[PIPELINE] - Analysis run ID: {ldsc_tissue_result['analysis_run_id']}")
                ldsc_status = "completed"
                
            except Exception as ldsc_e:
                logger.error(f"[PIPELINE] LDSC + tissue analysis failed: {str(ldsc_e)}")
                # LDSC results are required for tissue selection and enrichment - fail the pipeline
                failed_ldsc_state = {
                    "status": "Failed",
                    "stage": "LDSC_Analysis",
                    "progress": 90,
                    "message": f"LDSC tissue analysis failed: {str(ldsc_e)}. Tissue-specific enrichment will not be available.",
                }
                save_analysis_state_task.submit(projects_handler, user_id, project_id, failed_ldsc_state).result()
                raise RuntimeError(f"LDSC tissue analysis failed - required for enrichment: {str(ldsc_e)}")
            
            # Save completed analysis state
            completed_state = {
                "status": "Completed",
                "progress": 100,
                "message": "Analysis completed successfully",
                "ldsc_status": ldsc_status
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
