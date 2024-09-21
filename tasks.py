from prefect import task
from flask_socketio import SocketIO
from datetime import datetime, timedelta
from uuid import uuid4

socketio = SocketIO()

### Enrich Tasks
@task(retries=2)
def get_candidate_genes(prolog_query, variant):
    try:
        socketio.emit('task_update', {'message': 'Executing: get candidate genes', 'next task': 'predict causal gene'})
        print("Executing: get candidate genes")
        result = prolog_query.get_candidate_genes(variant)
        socketio.emit('task_update', {'message': 'Finished: get candidate genes'})
        return result
    except Exception as e:
        socketio.emit('task_update', {'message': 'Failed: get candidate genes', 'error': str(e)})
        raise

@task(retries=2)
def predict_causal_gene(llm, phenotype, candidate_genes):
    try:
        socketio.emit('task_update', {'message': 'Executing: predict causal gene', 'next task': 'get relevant gene proof'})
        print("Executing: predict causal gene")
        result = llm.predict_casual_gene(phenotype, candidate_genes)["causal_gene"]
        socketio.emit('task_update', {'message': 'Finished: predict causal gene'})
        return result
    except Exception as e:
        socketio.emit('task_update', {'message': 'Failed: predict causal gene', 'error': str(e)})
        raise

@task(retries=2)
def get_relevant_gene_proof(prolog_query, variant, causal_gene):
    try:
        socketio.emit('task_update', {'message': 'Executing: get relevant gene proof', 'next task': 'retry predict causal gene'})
        print("Executing: get relevant gene proof")
        result = prolog_query.get_relevant_gene_proof(variant, causal_gene)
        socketio.emit('task_update', {'message': 'Finished: get relevant gene proof'})
        return result
    except Exception as e:
        socketio.emit('task_update', {'message': 'Failed: get relevant gene proof', 'error': str(e)})
        raise

@task(retries=2)
def retry_predict_causal_gene(llm, phenotype, candidate_genes, proof, causal_gene):
    try:
        socketio.emit('task_update', {'message': 'Executing: retry predict causal gene', 'next task': 'retry get relevant gene proof'})
        print(f"Retrying predict causal gene with proof: {proof}")
        result = llm.predict_casual_gene(phenotype, candidate_genes, rule=proof, prev_gene=causal_gene)["causal_gene"]
        socketio.emit('task_update', {'message': 'Finished: retry predict causal gene'})
        return result
    except Exception as e:
        socketio.emit('task_update', {'message': 'Failed: retry predict causal gene', 'error': str(e)})
        raise

@task(retries=2)
def retry_get_relevant_gene_proof(prolog_query, variant, causal_gene):
    try:
        socketio.emit('task_update', {'message': 'Executing: retry get relevant gene proof', 'next task': 'create enrich data'})
        print("Retrying get relevant gene proof")
        result = prolog_query.get_relevant_gene_proof(variant, causal_gene)
        socketio.emit('task_update', {'message': 'Finished: retry get relevant gene proof'})
        return result
    except Exception as e:
        socketio.emit('task_update', {'message': 'Failed: retry get relevant gene proof', 'error': str(e)})
        raise

@task
def create_enrich_data(db, variant, phenotype, causal_gene, relevant_gos, causal_graph, current_user_id):
    socketio.emit('task_update', {'message': 'Executing: create enrich data', 'next task': 'check enrich'})
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
    socketio.emit('task_update', {'message': 'Finished: create enrich data'})
    return enrich_data["id"]

@task
def check_enrich(db, current_user_id, phenotype, variant):
    socketio.emit('task_update', {'message': 'Executing: check enrich', 'next task': 'check hypothesis'})
    print("Checking enrich data")
    if db.check_enrich(current_user_id, phenotype, variant):
        enrich = db.get_enrich_by_phenotype_and_variant(phenotype, variant, current_user_id)
        socketio.emit('task_update', {'message': 'Finished: check enrich'})
        return enrich
    socketio.emit('task_update', {'message': 'Not found: check enrich'})
    return None

### Hypothesis Tasks
@task
def check_hypothesis(db, current_user_id, enrich_id, go_id):
    socketio.emit('task_update', {'message': 'Executing: check hypothesis', 'next task': 'get enrich'})
    print("Checking hypothesis data")
    if db.check_hypothesis(current_user_id, enrich_id, go_id):
        hypothesis = db.get_hypothesis_by_enrich_and_go(enrich_id, go_id, current_user_id)
        socketio.emit('task_update', {'message': 'Finished: check hypothesis'})
        return hypothesis
    socketio.emit('task_update', {'message': 'Not found: check hypothesis'})
    return None

@task(retries=2)
def get_enrich(db, current_user_id, enrich_id):
    socketio.emit('task_update', {'message': 'Executing: get enrich', 'next task': 'get gene ids'})
    print("Fetching enrich data...")
    result = db.get_enrich(current_user_id, enrich_id)
    socketio.emit('task_update', {'message': 'Finished: get enrich'})
    return result

@task(retries=2)
def get_gene_ids(prolog_query, gene_names):
    socketio.emit('task_update', {'message': 'Executing: get gene ids', 'next task': 'execute gene query'})
    print("Fetching gene IDs...")
    result = prolog_query.get_gene_ids(gene_names)
    socketio.emit('task_update', {'message': 'Finished: get gene ids'})
    return result

@task(retries=2)
def execute_gene_query(prolog_query, query):
    socketio.emit('task_update', {'message': 'Executing: execute gene query', 'next task': 'execute variant query'})
    print("Executing Prolog query to retrieve gene names...")
    result = prolog_query.execute_query(query)
    socketio.emit('task_update', {'message': 'Finished: execute gene query'})
    return result

@task(retries=2)
def execute_variant_query(prolog_query, query):
    socketio.emit('task_update', {'message': 'Executing: execute variant query', 'next task': 'execute phenotype query'})
    print("Executing Prolog query to retrieve variant ids...")
    result = prolog_query.execute_query(query)
    socketio.emit('task_update', {'message': 'Finished: execute variant query'})
    return result

@task(retries=2)
def execute_phenotype_query(prolog_query, phenotype):
    socketio.emit('task_update', {'message': 'Executing: execute phenotype query', 'next task': 'summarize graph'})
    print("Executing Prolog query to retrieve phenotype id...")
    result = prolog_query.execute_query(f"term_name(efo(X), {phenotype})")
    socketio.emit('task_update', {'message': 'Finished: execute phenotype query'})
    return result

@task(retries=2)
def summarize_graph(llm, causal_graph):
    socketio.emit('task_update', {'message': 'Executing: summarize graph', 'next task': 'create hypothesis'})
    print("Summarizing causal graph...")
    result = llm.summarize_graph(causal_graph)
    socketio.emit('task_update', {'message': 'Finished: summarize graph'})
    return result

@task(retries=2)
def create_hypothesis(db, enrich_id, go_id, variant_id, phenotype, causal_gene, causal_graph, summary, current_user_id):
    socketio.emit('task_update', {'message': 'Executing: create hypothesis', 'next task': 'None'})
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