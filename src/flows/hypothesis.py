import os

from loguru import logger
from prefect import flow
from prefect_dask import DaskTaskRunner

from src.config import Config, create_dependencies
from src.tasks import (
    check_hypothesis,
    get_enrich,
    get_gene_ids,
    execute_gene_query,
    execute_variant_query,
    execute_phenotype_query,
    summarize_graph,
    create_hypothesis,
)


### Hypothesis Flow
@flow(
    log_prints=True,
    task_runner=DaskTaskRunner(address=os.getenv("DASK_ADDRESS"))
)
def hypothesis_flow(current_user_id, hypothesis_id, enrich_id, go_id):
    config = Config.from_env()
    deps = create_dependencies(config)
    hypotheses = deps['hypotheses']
    enrichment = deps['enrichment']

    hypothesis = check_hypothesis.submit(current_user_id, enrich_id, go_id, hypothesis_id).result()
    if hypothesis:
        logger.info("Retrieved hypothesis data from saved db")
        return {"summary": hypothesis.get('summary'), "graph": hypothesis.get('graph')}, 200

    # Check if this hypothesis has child enrichments and trigger background processing
    parent_hypothesis = hypotheses.get_hypotheses(current_user_id, hypothesis_id)
    if parent_hypothesis and 'child_enrich_ids' in parent_hypothesis:
        child_enrich_ids = parent_hypothesis.get('child_enrich_ids', [])
        if child_enrich_ids and len(child_enrich_ids) > 0:
            logger.info(f"Triggering background processing for {len(child_enrich_ids)} child enrichments")

            # Import here to avoid circular dependency
            from src.run_deployment import invoke_child_batch_deployment

            # Trigger the child batch deployment (fire-and-forget)
            invoke_child_batch_deployment(current_user_id, child_enrich_ids, hypothesis_id)
            logger.info(f"Child batch deployment triggered for {len(child_enrich_ids)} enrichments")

    enrich_data = get_enrich.submit(current_user_id, enrich_id, hypothesis_id).result()
    if not enrich_data:
        return {"message": "Invalid enrich_id or access denied."}, 404

    go_term = [go for go in enrich_data["GO_terms"] if go["id"] == go_id]
    if not go_term:
        logger.error(f"GO term {go_id} not found in enrichment {enrich_id}")
        return {"message": f"GO term {go_id} not found in this enrichment."}, 404

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

    coexpressed_gene_ids = get_gene_ids.submit([g.lower() for g in coexpressed_gene_names], hypothesis_id).result()

    nodes, edges = causal_graph["nodes"], causal_graph["edges"]

    causal_gene_id = causal_gene.lower()
    causal_gene_name = causal_gene.upper()
    logger.info(f"Using causal gene from enrichment: {causal_gene_name} (ID: {causal_gene_id})")

    # Standardize variant IDs
    variant_nodes = [n for n in nodes if n["type"] == "snp"]
    variant_rsids = [n['id'] for n in variant_nodes]
    variant_entities = [f"snp({id})" for id in variant_rsids]
    query = f"maplist(variant_id, {variant_entities}, X)".replace("'", "")

    variant_ids = execute_variant_query.submit(query, hypothesis_id).result()
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

    gene_names = execute_gene_query.submit(query, hypothesis_id).result()
    for gene_id_node, gene_name, node in zip(gene_ids, gene_names, gene_nodes):
        node["name"] = gene_name

    phenotype_result = execute_phenotype_query.submit(phenotype, hypothesis_id).result()
    phenotype_id = phenotype_result[0] if isinstance(phenotype_result, list) and phenotype_result else phenotype_result

    nodes.append({"id": phenotype_id, "type": "phenotype", "name": phenotype})
    edges.append({"source": go_id, "target": phenotype_id, "label": "involved_in"})
    for gene_id, gene_name in zip(coexpressed_gene_ids, coexpressed_gene_names):
        nodes.append({"id": gene_id, "type": "gene", "name": gene_name})
        edges.append({"source": gene_id, "target": go_id, "label": "enriched_in"})
        edges.append({"source": causal_gene_id, "target": gene_id, "label": "coexpressed_with"})

    final_causal_graph = {"nodes": nodes, "edges": edges, "probability": graph_prob}

    summary = summarize_graph.submit({"nodes": nodes, "edges": edges}, hypothesis_id).result()

    create_hypothesis.submit(enrich_id, go_id, variant_id, phenotype, causal_gene_name, final_causal_graph,
                     summary, current_user_id, hypothesis_id).result()

    return {"summary": summary, "graph": final_causal_graph}, 201
