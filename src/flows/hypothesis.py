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


def _normalize_hypothesis_graph(graph: dict, enrichr) -> dict:
    """Uppercase Ensembl node/edge IDs so they match Prolog (ENSG…).

    enrichr.to_ensembl_id() returns lowercase ids; Prolog graph nodes use ENSG….
    React Flow requires edge source/target to match a node id exactly.
    """
    if not graph:
        return graph

    nodes = list(graph.get("nodes") or [])
    edges = list(graph.get("edges") or [])
    id_map = {}

    for node in nodes:
        nid = str(node.get("id", ""))
        if enrichr.is_ensembl_id(nid):
            new_id = nid.upper()
            if nid != new_id:
                id_map[nid] = new_id
            node["id"] = new_id

    for edge in edges:
        src, tgt = edge.get("source"), edge.get("target")
        if src in id_map:
            edge["source"] = id_map[src]
        elif enrichr.is_ensembl_id(str(src or "")):
            edge["source"] = str(src).upper()
        if tgt in id_map:
            edge["target"] = id_map[tgt]
        elif enrichr.is_ensembl_id(str(tgt or "")):
            edge["target"] = str(tgt).upper()

    out = dict(graph)
    out["nodes"] = nodes
    out["edges"] = edges
    return out


### Hypothesis Flow
@flow(
    log_prints=True,
    task_runner=DaskTaskRunner(address=os.getenv("DASK_ADDRESS"))
)
def hypothesis_flow(current_user_id, hypothesis_id, enrich_id, go_id):
    config = Config.from_env()
    deps = create_dependencies(config)
    hypotheses = deps['hypotheses']
    enrichr = deps['enrichr']

    hypothesis = check_hypothesis.submit(current_user_id, enrich_id, go_id, hypothesis_id).result()
    saved_graph = hypothesis.get("graph") if hypothesis else None
    if saved_graph and saved_graph.get("nodes"):
        logger.info("Retrieved hypothesis data from saved db")
        graph = _normalize_hypothesis_graph(saved_graph, enrichr)
        return {"summary": hypothesis.get("summary"), "graph": graph}, 200
    if hypothesis:
        logger.info(
            "Hypothesis record exists but has no graph yet; generating "
            f"(status={hypothesis.get('status')})"
        )

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

    coexpressed_gene_ids = get_gene_ids.submit(
        [g.lower() for g in coexpressed_gene_names], hypothesis_id
    ).result()

    nodes, edges = causal_graph["nodes"], causal_graph["edges"]
    for node in nodes:
        nid = str(node.get("id", ""))
        if enrichr.is_ensembl_id(nid):
            node["id"] = nid.upper()
    for edge in edges:
        if enrichr.is_ensembl_id(str(edge.get("source") or "")):
            edge["source"] = str(edge["source"]).upper()
        if enrichr.is_ensembl_id(str(edge.get("target") or "")):
            edge["target"] = str(edge["target"]).upper()

    causal_gene_symbol = enrichr.to_symbol(causal_gene)
    causal_gene_ensembl = enrichr.to_ensembl_id(causal_gene_symbol) or causal_gene
    if enrichr.is_ensembl_id(str(causal_gene_ensembl)):
        causal_gene_ensembl = str(causal_gene_ensembl).upper()
    logger.info(
        f"Using causal gene from enrichment: {causal_gene_symbol} "
        f"(Ensembl: {causal_gene_ensembl})"
    )

    # Standardize variant IDs (variant_id/2 may be unavailable or return no results)
    variant_nodes = [n for n in nodes if n["type"] == "snp"]
    variant_rsids = [n['id'] for n in variant_nodes]
    variant_entities = [f"snp({id})" for id in variant_rsids]
    query = f"maplist(variant_id, {variant_entities}, X)".replace("'", "")

    try:
        variant_ids = execute_variant_query.submit(query, hypothesis_id).result()
        if not variant_ids or len(variant_ids) != len(variant_rsids):
            raise ValueError("variant_id query returned no or incomplete results")
    except Exception as e:
        logger.warning(
            f"variant_id/2 query failed ({e}); using rsIDs directly as variant IDs"
        )
        variant_ids = variant_rsids

    for resolved_id, rsid, node in zip(variant_ids, variant_rsids, variant_nodes):
        resolved_id = str(resolved_id).replace("'", "")
        node["id"] = resolved_id
        node["name"] = rsid
        source_edges = [e for e in edges if e["source"] == rsid]
        target_edges = [e for e in edges if e["target"] == rsid]
        for edge in source_edges:
            edge["source"] = resolved_id
        for edge in target_edges:
            edge["target"] = resolved_id

    gene_nodes = [n for n in nodes if n["type"] == "gene"]
    prolog_gene_nodes = []
    prolog_gene_ids = []

    for node in gene_nodes:
        node_id = node.get("id", "")
        if enrichr.is_ensembl_id(node_id):
            prolog_gene_nodes.append(node)
            prolog_gene_ids.append(node_id)
        else:
            node["name"] = enrichr.to_symbol(node.get("name") or node_id)

    if prolog_gene_ids:
        gene_entities = [f"gene({gene_id})" for gene_id in prolog_gene_ids]
        query = f"maplist(gene_name, {gene_entities}, X)".replace("'", "")
        prolog_gene_names = execute_gene_query.submit(query, hypothesis_id).result()
        for gene_id_node, prolog_name, node in zip(
            prolog_gene_ids, prolog_gene_names, prolog_gene_nodes
        ):
            node["name"] = (
                enrichr.to_symbol(prolog_name)
                if prolog_name
                else enrichr.to_symbol(node.get("name") or gene_id_node)
            )

    phenotype_result = execute_phenotype_query.submit(phenotype, hypothesis_id).result()
    phenotype_id = phenotype_result[0] if isinstance(phenotype_result, list) and phenotype_result else phenotype_result

    existing_ids = {n.get("id") for n in nodes}

    if causal_gene_ensembl not in existing_ids:
        nodes.append({
            "id": causal_gene_ensembl,
            "type": "gene",
            "name": causal_gene_symbol,
        })
        existing_ids.add(causal_gene_ensembl)

    if go_id not in existing_ids:
        nodes.append({"id": go_id, "type": "go", "name": go_name})
        existing_ids.add(go_id)

    nodes.append({"id": phenotype_id, "type": "phenotype", "name": phenotype})
    existing_ids.add(phenotype_id)
    edges.append({"source": go_id, "target": phenotype_id, "label": "involved_in"})
    for gene_id, gene_name in zip(coexpressed_gene_ids, coexpressed_gene_names):
        if enrichr.is_ensembl_id(str(gene_id)):
            gene_id = str(gene_id).upper()
        symbol = enrichr.to_symbol(gene_name)
        if gene_id not in existing_ids:
            nodes.append({"id": gene_id, "type": "gene", "name": symbol})
            existing_ids.add(gene_id)
        edges.append({"source": gene_id, "target": go_id, "label": "enriched_in"})
        edges.append({
            "source": causal_gene_ensembl,
            "target": gene_id,
            "label": "coexpressed_with",
        })

    final_causal_graph = _normalize_hypothesis_graph(
        {"nodes": nodes, "edges": edges, "probability": graph_prob},
        enrichr,
    )

    summary = summarize_graph.submit({"nodes": nodes, "edges": edges}, hypothesis_id).result()

    create_hypothesis.submit(
        enrich_id, go_id, variant_id, phenotype, causal_gene_symbol, final_causal_graph,
        summary, current_user_id, hypothesis_id
    ).result()

    return {"summary": summary, "graph": final_causal_graph}, 201
