import os
import traceback
from prefect import task
from datetime import datetime, timedelta
from uuid import uuid4
from socketio_instance import socketio
from typing import Dict, List
import requests
import logging

### Enrich Tasks
@task(retries=2, cache_policy=None)
def check_enrich(db, current_user_id, phenotype, variant):
    socketio.emit('task_update', {'message': {'Executing': 'check enrich', 'Next_task': 'get candidate genes'}})
    socketio.sleep(0)
    print("Checking enrich data")
    if db.check_enrich(current_user_id, phenotype, variant):
        enrich = db.get_enrich_by_phenotype_and_variant(phenotype, variant, current_user_id)
        socketio.emit('task_update', {'message': 'Finished: check enrich'})
        socketio.sleep(0)
        return enrich
    socketio.emit('task_update', {'message': 'Not found: check enrich'})
    socketio.sleep(0)
    return None

@task(retries=2)
def get_candidate_genes(prolog_query, variant):
    try:
        socketio.emit('task_update', {'message': {'Executing': 'get candidate genes', 'Next_task': 'predict causal gene'}})
        socketio.sleep(0)
        print("Executing: get candidate genes")
        result = prolog_query.get_candidate_genes(variant)
        socketio.emit('task_update', {'message': 'Finished: get candidate genes'})
        socketio.sleep(0)
        return result
    except Exception as e:
        socketio.emit('task_update', {'message': 'Failed: get candidate genes', 'error': str(e)})
        socketio.sleep(0)
        raise

@task(retries=2)
def predict_causal_gene(llm, phenotype, candidate_genes):
    try:
        socketio.emit('task_update', {'message': {'Executing': 'predict causal gene', 'Next_task': 'get relevant gene proof'}})
        socketio.sleep(0)
        print("Executing: predict causal gene")
        result = llm.predict_casual_gene(phenotype, candidate_genes)["causal_gene"]
        socketio.emit('task_update', {'message': 'Finished: predict causal gene'})
        socketio.sleep(0)
        return result
    except Exception as e:
        socketio.emit('task_update', {'message': 'Failed: predict causal gene', 'error': str(e)})
        socketio.sleep(0)
        raise

@task(retries=2)
def get_relevant_gene_proof(prolog_query, variant, causal_gene):
    try:
        socketio.emit('task_update', {'message': {'Executing': 'get relevant gene proof', 'Next_task': 'create enrich data'}})
        socketio.sleep(0)
        print("Executing: get relevant gene proof")
        result = prolog_query.get_relevant_gene_proof(variant, causal_gene)
        socketio.emit('task_update', {'message': {'Finished': 'get relevant gene proof'}})
        socketio.sleep(0)
        return result
    except Exception as e:
        socketio.emit('task_update', {'message': {'Failed': 'get relevant gene proof', 'Next_task': 'retry predict causal gene', 'error': str(e)}})
        socketio.sleep(0)
        raise

@task(retries=2)
def retry_predict_causal_gene(llm, phenotype, candidate_genes, proof, causal_gene):
    try:
        socketio.emit('task_update', {'message': {'Executing': 'retry predict causal gene', 'Next_task': 'retry get relevant gene proof'}})
        socketio.sleep(0)
        print(f"Retrying predict causal gene with proof: {proof}")
        result = llm.predict_casual_gene(phenotype, candidate_genes, rule=proof, prev_gene=causal_gene)["causal_gene"]
        socketio.emit('task_update', {'message': {'Finished': 'retry predict causal gene'}})
        socketio.sleep(0)
        return result
    except Exception as e:
        socketio.emit('task_update', {'message': {'Failed': 'retry predict causal gene', 'Next_task': None, 'error': str(e)}})
        socketio.sleep(0)
        raise

@task(retries=2)
def retry_get_relevant_gene_proof(prolog_query, variant, causal_gene):
    try:
        socketio.emit('task_update', {'message': {'Executing': 'retry get relevant gene proof', 'Next_task': 'create enrich data'}})
        socketio.sleep(0)
        print("Retrying get relevant gene proof")
        result = prolog_query.get_relevant_gene_proof(variant, causal_gene)
        socketio.emit('task_update', {'message': {'Finished': 'retry get relevant gene proof'}})
        socketio.sleep(0)
        return result
    except Exception as e:
        socketio.emit('task_update', {'message': {'Failed': 'retry get relevant gene proof', 'Next_task': None, 'error': str(e)}})
        socketio.sleep(0)
        raise

@task(cache_policy=None)
def create_enrich_data(db, variant, phenotype, causal_gene, relevant_gos, causal_graph, current_user_id):
    socketio.emit('task_update', {'message': {'Executing': 'create enrich data', 'Next task': None}})
    socketio.sleep(0)
    print("Creating enrich data in the database")
    enrich_data = {
        "id": str(uuid4()),
        "created_on": datetime.utcnow().isoformat(timespec='milliseconds') + "Z",
        "variant": variant,
        "phenotype": phenotype,
        "causal_gene": causal_gene,
        "GO_terms": relevant_gos,
        "causal_graph": causal_graph
    }
    db.create_enrich(current_user_id, enrich_data)
    socketio.emit('task_update', {'message': {'Finished': 'create enrich data'}})
    socketio.sleep(0)
    return enrich_data["id"]

### Hypothesis Tasks
@task(cache_policy=None, retries=2)
def check_hypothesis(db, current_user_id, enrich_id, go_id):
    socketio.emit('task_update', {'message': 'Executing: check hypothesis', 'next_task': 'get enrich'})
    print("Checking hypothesis data")
    if db.check_hypothesis(current_user_id, enrich_id, go_id):
        hypothesis = db.get_hypothesis_by_enrich_and_go(enrich_id, go_id, current_user_id)
        socketio.emit('task_update', {'message': 'Finished: check hypothesis'})
        return hypothesis
    socketio.emit('task_update', {'message': 'Not found: check hypothesis'})
    return None

@task(cache_policy=None, retries=2)
def get_enrich(db, current_user_id, enrich_id):
    socketio.emit('task_update', {'message': 'Executing: get enrich', 'next_task': 'get gene ids'})
    print("Fetching enrich data...")
    result = db.get_enrich(current_user_id, enrich_id)
    socketio.emit('task_update', {'message': 'Finished: get enrich'})
    return result

@task(retries=2)
def get_gene_ids(prolog_query, gene_names):
    socketio.emit('task_update', {'message': 'Executing: get gene ids', 'next_task': 'execute gene query'})
    print("Fetching gene IDs...")
    result = prolog_query.get_gene_ids(gene_names)
    socketio.emit('task_update', {'message': 'Finished: get gene ids'})
    return result

@task(retries=2)
def execute_gene_query(prolog_query, query):
    socketio.emit('task_update', {'message': 'Executing: execute gene query', 'next_task': 'execute variant query'})
    print("Executing Prolog query to retrieve gene names...")
    result = prolog_query.execute_query(query)
    socketio.emit('task_update', {'message': 'Finished: execute gene query'})
    return result

@task(retries=2)
def execute_variant_query(prolog_query, query):
    socketio.emit('task_update', {'message': 'Executing: execute variant query', 'next_task': 'execute phenotype query'})
    print("Executing Prolog query to retrieve variant ids...")
    result = prolog_query.execute_query(query)
    socketio.emit('task_update', {'message': 'Finished: execute variant query'})
    return result

@task(retries=2)
def execute_phenotype_query(prolog_query, phenotype):
    socketio.emit('task_update', {'message': 'Executing: execute phenotype query', 'next_task': 'summarize graph'})
    print("Executing Prolog query to retrieve phenotype id...")
    result = prolog_query.execute_query(f"term_name(efo(X), {phenotype})")
    socketio.emit('task_update', {'message': 'Finished: execute phenotype query'})
    return result

@task(retries=2)
def summarize_graph(llm, causal_graph):
    socketio.emit('task_update', {'message': 'Executing: summarize graph', 'next_task': 'create hypothesis'})
    print("Summarizing causal graph...")
    result = llm.summarize_graph(causal_graph)
    socketio.emit('task_update', {'message': 'Finished: summarize graph'})
    return result

@task(cache_policy=None, retries=2)
def create_hypothesis(db, enrich_id, go_id, variant_id, phenotype, causal_gene, causal_graph, summary, current_user_id):
    socketio.emit('task_update', {'message': 'Executing: create hypothesis', 'next_task': 'None'})
    print("Creating hypothesis in the database...")
    hypothesis_data = {
            "id": str(uuid4()),
            "enrich_id": enrich_id,
            "go_id": go_id,
            "variant_id": variant_id,
            "phenotype": phenotype,
            "causal_gene": causal_gene,
            "graph": causal_graph,
            "summary": summary,
            "biological_context": ""
        }
    db.create_hypothesis(current_user_id, hypothesis_data)
    socketio.emit('task_update', {'message': 'Finished: create hypothesis'})
    return hypothesis_data["id"]

@task(retries=2)
def get_node_annotations(nodes: List[Dict], token: str):
    """Query the annotation service to get additional properties for nodes"""

    # Configuration for annotation service URL and headers
    annotation_url = os.getenv("ANNOTATION_URL")
    params = {"source": "hypothesis"}
    headers = {"Authorization": f"Bearer {token}"}
    
    # Process each node individually
    node_properties = {}
    
    # Prepare batch request for all nodes
    request_body = {
        "requests": {
            "nodes": [
                {
                    "node_id": f"n{i}",
                    "id": node["id"],
                    "type": node["type"],
                    "properties": {}
                }
                for i, node in enumerate(nodes)
            ],
            "predicates": []
        }
    }

    try:
        
        response = requests.post(annotation_url, json=request_body, params=params, headers=headers)
        response.raise_for_status()
        annotations = response.json()

        # Process annotations
        node_properties = {}
        missed_nodes = []
        
        # First pass - process successful annotations
        found_node_ids = set(node["data"]["id"].split()[-1] for node in annotations.get("nodes", []))
        
        # Process successful annotations
        for node in annotations.get("nodes", []):
            node_id = node["data"]["id"].split()[-1]
            properties = {
                k: v for k, v in node["data"].items() 
                if k not in ["id", "type"]
            }
            if properties:  # If we got properties
                node_properties[node_id] = properties
        
        # Add nodes that weren't returned in the response to missed_nodes
        missed_nodes = [node for node in nodes if node["id"] not in found_node_ids]
                

        # Retry with name as ID for missed nodes if they have a name
        if missed_nodes:
            retry_body = {
                "requests": {
                    "nodes": [
                        {
                            "node_id": f"retry_{i}",
                            "id": node.get("name", ""),  # Use name as ID
                            "type": node["type"],
                            "properties": {}
                        }
                        for i, node in enumerate(missed_nodes)
                        if node.get("name")  # Only retry nodes with names
                    ],
                    "predicates": []
                }
            }

            if retry_body["requests"]["nodes"]:  # Only send request if there are nodes to retry
                retry_response = requests.post(annotation_url, json=retry_body, params=params, headers=headers)
                retry_response.raise_for_status()
                retry_annotations = retry_response.json()

                # Process retry results
                for node in retry_annotations.get("nodes", []):
                    # Extract the ID portion after the type
                    node_id = node["data"]["id"].split()[-1]
                    # Map back to original node ID using name
                    original_node = next(
                        (n for n in missed_nodes if n.get("name") == node_id), 
                        None
                    )
                    if original_node:
                        properties = {
                            k: v for k, v in node["data"].items() 
                            if k not in ["id", "type"]
                        }
                        if properties:
                            node_properties[original_node["id"]] = properties

        # Fill in empty properties for any remaining nodes
        for node in nodes:
            if node["id"] not in node_properties:
                node_properties[node["id"]] = {}

        return node_properties

    except Exception as e:
        logging.error(f"Error querying annotation service: {str(e)}")
        logging.error(traceback.format_exc())
        # Return empty properties for all nodes on failure
        return {node["id"]: {} for node in nodes}
