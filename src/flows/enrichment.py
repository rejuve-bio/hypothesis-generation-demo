import os
from datetime import datetime, timezone
from uuid import uuid4

from loguru import logger
from prefect import flow
from prefect_dask import DaskTaskRunner
from services.status_tracker import TaskState

from src.config import Config, create_dependencies
from tasks import (
    check_enrich,
    get_candidate_genes,
    get_relevant_gene_proof,
    retry_get_relevant_gene_proof,
    create_enrich_data,
    extract_causal_gene_from_graph,
    get_coexpression_matrix_for_tissue,
)
from src.utils import emit_task_update


### Enrichment Flow
@flow(
    log_prints=True,
    persist_result=False,
    task_runner=DaskTaskRunner(address=os.getenv("DASK_ADDRESS"))
)
def enrichment_flow(current_user_id, phenotype, variant, hypothesis_id, project_id, seed):
    """
    Fully project-based enrichment flow that initializes dependencies from centralized config
    """
    # Initialize dependencies from environment variables
    config = Config.from_env()
    deps = create_dependencies(config)

    # Initialize StatusTracker for Prefect context
    from services.status_tracker import StatusTracker
    status_tracker = StatusTracker()
    status_tracker.initialize(deps['tasks'])

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
        enrich = check_enrich.submit(current_user_id, variant, phenotype, hypothesis_id).result()

        if enrich:
            logger.info("Retrieved enrich data from saved db")
            return {"id": enrich['id']}, 200

        candidate_genes = get_candidate_genes.submit(variant, hypothesis_id).result()
        graphs_list = get_relevant_gene_proof.submit(variant, hypothesis_id, seed).result()

        if not graphs_list or len(graphs_list) == 0:
            graphs_list = retry_get_relevant_gene_proof.submit(variant, hypothesis_id, seed).result()
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
                    # Offload coexpression to Dask (avoids OOM in prefect-deployment)
                    tissue_uberon_id = enrichr.get_tissue_uberon_id(current_user_id, project_id, selected_tissue)
                    coexpression_data = None
                    if tissue_uberon_id:
                        coexpression_data = get_coexpression_matrix_for_tissue.submit(
                            this_causal_gene, tissue_uberon_id, cell_type=None, k=500
                        ).result()
                    enrich_tbl = enrichr.run(
                        this_causal_gene, tissue_name=selected_tissue,
                        user_id=current_user_id, project_id=project_id,
                        coexpression_data=coexpression_data
                    )
                else:
                    enrich_tbl = enrichr.run(this_causal_gene)

                # Check if enrichment table is empty
                if enrich_tbl is None or len(enrich_tbl) == 0:
                    logger.warning(f"No enrichment results found for gene {this_causal_gene}. Skipping LLM relevance scoring.")
                    relevant_gos = []
                else:
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
                current_user_id, project_id, variant,
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


### Child Enrichment Batch Flow
@flow(
    log_prints=True,
    task_runner=DaskTaskRunner(address=os.getenv("DASK_ADDRESS"))
)
def child_enrichment_batch_flow(current_user_id, child_enrich_ids, parent_hypothesis_id):
    """
    Process child enrichments in background, each using its FIRST GO term.
    """
    from flows.hypothesis import hypothesis_flow

    logger.info(f"[CHILD_BATCH] Starting batch processing for {len(child_enrich_ids)} child enrichments")

    config = Config.from_env()
    deps = create_dependencies(config)
    hypotheses = deps['hypotheses']
    enrichment = deps['enrichment']

    # Get parent hypothesis to extract project_id
    parent_hypothesis = hypotheses.get_hypotheses(current_user_id, parent_hypothesis_id)
    parent_project_id = parent_hypothesis.get('project_id') if parent_hypothesis else None

    for enrich_id in child_enrich_ids:
        logger.info(f"[CHILD_BATCH] Processing child enrichment {enrich_id}")

        try:
            # Get enrichment data to find first GO term
            enrich_data = enrichment.get_enrich(current_user_id, enrich_id)
            if not enrich_data:
                logger.warning(f"[CHILD_BATCH] Could not get enrichment data for {enrich_id}")
                continue

            # Get GO terms from enrichment
            go_terms = enrich_data.get("GO_terms", [])
            if not go_terms or len(go_terms) == 0:
                logger.warning(f"[CHILD_BATCH] No GO terms found for enrichment {enrich_id}")
                continue

            # Get FIRST GO term only
            go_term = go_terms[0]
            go_id = go_term.get("id")
            if not go_id:
                logger.warning(f"[CHILD_BATCH] No GO ID found in first GO term for enrichment {enrich_id}")
                continue

            # Get phenotype and variant from enrichment data
            phenotype = enrich_data.get('phenotype')
            variant = enrich_data.get('variant')

            # Check if hypothesis already exists for this enrichment + GO combination
            all_hypotheses = hypotheses.get_hypotheses(current_user_id)
            existing_hyp = None
            if isinstance(all_hypotheses, list):
                for h in all_hypotheses:
                    if h.get('enrich_id') == enrich_id and h.get('go_id') == go_id:
                        existing_hyp = h
                        break

            if existing_hyp:
                logger.info(f"[CHILD_BATCH] Hypothesis already exists for enrichment {enrich_id} + GO {go_id}")
                continue

            # Create unique hypothesis ID
            new_hypothesis_id = str(uuid4())

            # Create the hypothesis record
            hypothesis_data = {
                "id": new_hypothesis_id,
                "enrich_id": enrich_id,
                "go_id": go_id,
                "phenotype": phenotype,
                "variant": variant,
                "status": "pending",
                "created_at": datetime.now(timezone.utc).isoformat(timespec='milliseconds') + "Z",
                "task_history": [],
            }

            if parent_project_id:
                hypothesis_data["project_id"] = parent_project_id

            hypotheses.create_hypothesis(current_user_id, hypothesis_data)
            logger.info(f"[CHILD_BATCH] Created hypothesis record {new_hypothesis_id} for child enrichment {enrich_id}")

            logger.info(f"[CHILD_BATCH] Generating hypothesis for child enrichment {enrich_id}, GO: {go_id}")
            hypothesis_flow(current_user_id, new_hypothesis_id, enrich_id, go_id)
            logger.info(f"[CHILD_BATCH] Successfully generated hypothesis {new_hypothesis_id}")

        except Exception as hyp_e:
            logger.error(f"[CHILD_BATCH] Failed to generate hypothesis for enrichment {enrich_id}: {str(hyp_e)}")
            continue

    logger.info(f"[CHILD_BATCH] Batch processing completed")
    return {"processed": len(child_enrich_ids), "status": "completed"}
