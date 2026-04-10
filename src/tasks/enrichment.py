import json
import logging
from datetime import datetime, timezone

from loguru import logger
from prefect import task
from services.status_tracker import TaskState, status_tracker
from src.utils import emit_task_update, get_deps


logging.basicConfig(level=logging.INFO)


def parse_prolog_graphs(raw_response):
    """Parse graphs from Prolog response, skipping any that fail to parse."""
    graphs_raw = raw_response.get('response', [])
    graphs_list = []

    for i, graph_str in enumerate(graphs_raw):
        try:
            if isinstance(graph_str, str):
                graph = json.loads(graph_str)
            else:
                graph = graph_str
            graphs_list.append(graph)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse graph {i}: {e}")
            logger.error(f"Raw graph data: {graph_str}")

    logger.info(f"Parsed {len(graphs_list)} graphs from Prolog response")
    return graphs_list


def extract_causal_gene_from_graph(graph, variant_nodes):
    """
    Extract the most likely causal gene from the Prolog graph structure using topology.
    """
    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])

    # Get all gene nodes
    gene_nodes = [n for n in nodes if n.get("type") == "gene"]
    if not gene_nodes:
        return None, None

    # Find genes directly connected to SNPs
    snp_ids = [n.get("id", n.get("name", "")) for n in variant_nodes]
    directly_connected_genes = []

    for edge in edges:
        source = edge.get("source", "")
        target = edge.get("target", "")

        # Check if edge connects SNP to gene
        for snp_id in snp_ids:
            if source == snp_id and any(target == g.get("id", "") for g in gene_nodes):
                gene_node = next((g for g in gene_nodes if g.get("id", "") == target), None)
                if gene_node:
                    directly_connected_genes.append(gene_node)
            elif target == snp_id and any(source == g.get("id", "") for g in gene_nodes):
                gene_node = next((g for g in gene_nodes if g.get("id", "") == source), None)
                if gene_node:
                    directly_connected_genes.append(gene_node)

    # use the first one
    if directly_connected_genes:
        causal_gene_node = directly_connected_genes[0]
        return causal_gene_node.get("id", ""), causal_gene_node.get("name", "")

    return None, None


### Enrich Tasks
@task(retries=2)
def check_enrich(current_user_id, variant, phenotype, hypothesis_id):
    """Check if enrichment exists for variant and phenotype"""
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existence of enrichment data",
            state=TaskState.STARTED,
            progress=0
        )
        deps = get_deps()
        enrichment = deps["enrichment"]

        if enrichment.check_enrich(current_user_id, phenotype, variant):
            enrich = enrichment.get_enrich_by_phenotype_and_variant(phenotype, variant, current_user_id)

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
            details={"found": False}
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
def get_candidate_genes(variant, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting candidate genes",
            state=TaskState.STARTED,
            next_task="Predicting causal gene",
        )
        deps = get_deps()
        prolog_query = deps["prolog_query"]
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
def predict_causal_gene(phenotype, candidate_genes, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Predicting causal gene",
            state=TaskState.STARTED,
            next_task="Getting relevant gene proof"
        )

        logger.info("Executing: predict causal gene")
        deps = get_deps()
        llm = deps["llm"]
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
def get_relevant_gene_proof(variant, hypothesis_id, seed):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting relevant gene proof",
            state=TaskState.STARTED,
            next_task="Creating enrich data"
        )

        logger.info("Executing: get relevant gene proof")
        deps = get_deps()
        prolog_query = deps["prolog_query"]
        raw_response = prolog_query.get_relevant_gene_proof(variant, seed, samples=10)
        graphs_list = parse_prolog_graphs(raw_response)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting relevant gene proof",
            state=TaskState.COMPLETED,
            details={"relevant_gene_proof": graphs_list, "num_graphs": len(graphs_list)}
        )
        return graphs_list
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
def retry_predict_causal_gene(phenotype, candidate_genes, proof, causal_gene, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to predict causal gene",
            state=TaskState.RETRYING,
            next_task="Retrying to get relevant gene proof"
        )

        logger.info(f"Retrying predict causal gene with proof: {proof}")
        deps = get_deps()
        llm = deps["llm"]
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
def retry_get_relevant_gene_proof(variant, hypothesis_id, seed):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to get relevant gene proof",
            state=TaskState.RETRYING,
            next_task="Creating enrich data"
        )

        logger.info("Retrying get relevant gene proof")
        deps = get_deps()
        prolog_query = deps["prolog_query"]
        raw_response = prolog_query.get_relevant_gene_proof(variant, seed, samples=10)
        graphs_list = parse_prolog_graphs(raw_response)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to get relevant gene proof",
            state=TaskState.COMPLETED,
            details={"retry_relevant_gene_proof": graphs_list, "num_graphs": len(graphs_list)}
        )
        return graphs_list
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to get relevant gene proof",
            state=TaskState.FAILED,
            error=str(e)
        )
        raise


@task()
def create_enrich_data(user_id, project_id, variant, phenotype, causal_gene, relevant_gos, causal_graph, hypothesis_id):
    """Create enrichment data with project references"""
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Creating enrich data",
            state=TaskState.STARTED
        )

        logger.info("Creating enrich data in the database with project context")
        deps = get_deps()
        enrichment = deps["enrichment"]
        enrich_id = enrichment.create_enrich(
            user_id, project_id, variant,
            phenotype, causal_gene, relevant_gos, causal_graph
        )

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Creating enrich data",
            state=TaskState.COMPLETED,
            details={"enrichment_id": enrich_id}
        )

        hypothesis_history = status_tracker.get_history(hypothesis_id)
        clean_history = []
        for task in hypothesis_history:
            task_copy = task.copy()
            task_copy.pop('details', None)  # Remove details to reduce size
            clean_history.append(task_copy)

        limited_history = clean_history[-50:] if len(clean_history) > 50 else clean_history

        logger.info("Updating hypothesis in the database...")
        hypothesis_data = {
                "task_history": limited_history,
            }
        hypotheses = deps["hypotheses"]
        hypotheses.update_hypothesis(hypothesis_id, hypothesis_data)

        return enrich_id
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Creating enrich data",
            state=TaskState.FAILED,
            error=str(e)
        )
        raise
