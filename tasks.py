from prefect import task
from datetime import datetime, timedelta
from uuid import uuid4

### Enrich Tasks
@task(retries=2)
def get_candidate_genes(prolog_query, variant):
    try:
        print("Executing: get_candidate_genes")
        return prolog_query.get_candidate_genes(variant)
    except Exception as e:
        print(f"Error in get_candidate_genes: {e}")
        raise

@task(retries=2)
def predict_causal_gene(llm, phenotype, candidate_genes):
    try:
        print("Executing: predict_causal_gene")
        return llm.predict_casual_gene(phenotype, candidate_genes)["causal_gene"]
    except Exception as e:
        print(f"Error in predict_causal_gene: {e}")
        raise

@task(retries=2)
def get_relevant_gene_proof(prolog_query, variant, causal_gene):
    try:
        print("Executing: get_relevant_gene_proof")
        return prolog_query.get_relevant_gene_proof(variant, causal_gene)
    except Exception as e:
        print(f"Error in get_relevant_gene_proof: {e}")
        raise

@task(retries=2)
def retry_predict_causal_gene(llm, phenotype, candidate_genes, proof, causal_gene):
    try:
        print(f"Retrying predict_causal_gene with proof: {proof}")
        return llm.predict_casual_gene(phenotype, candidate_genes, rule=proof, prev_gene=causal_gene)["causal_gene"]
    except Exception as e:
        print(f"Error in retry_predict_causal_gene: {e}")
        raise

@task(retries=2)
def retry_get_relevant_gene_proof(prolog_query, variant, causal_gene):
    try:
        print("Retrying get_relevant_gene_proof")
        return prolog_query.get_relevant_gene_proof(variant, causal_gene)
    except Exception as e:
        print(f"Error in retry_get_relevant_gene_proof: {e}")
        raise

@task
def create_enrich_data(db, variant, phenotype, causal_gene, relevant_gos, causal_graph, current_user_id):
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
    return enrich_data["id"]

@task
def check_enrich(db, current_user_id, phenotype, variant):
    print("Checking enrich data")
    if db.check_enrich(current_user_id, phenotype, variant):
        enrich = db.get_enrich_by_phenotype_and_variant(phenotype, variant, current_user_id)
        return enrich
    return None


### Hypothesis Tasks
@task
def check_hypothesis(db, current_user_id, enrich_id, go_id):
    print("Checking hypothesis data")
    if db.check_hypothesis(current_user_id, enrich_id, go_id):
        hypothesis = db.get_hypothesis_by_enrich_and_go(enrich_id, go_id, current_user_id)
        return hypothesis
    return None

@task(retries=2)
def get_enrich(db, current_user_id, enrich_id):
    print("Fetching enrich data...")
    return db.get_enrich(current_user_id, enrich_id)

@task(retries=2)
def get_gene_ids(prolog_query, gene_names):
    print("Fetching gene IDs...")
    return prolog_query.get_gene_ids(gene_names)


@task(retries=2)
def execute_gene_query(prolog_query, query):
    print("Executing Prolog query to retrieve gene names...")
    return prolog_query.execute_query(query)

@task(retries=2)
def execute_variant_query(prolog_query, query):
    print("Executing Prolog query to retrieve variant ids...")
    return prolog_query.execute_query(query)

@task(retries=2)
def execute_phenotype_query(prolog_query, phenotype):
    print("Executing Prolog query to retrieve phenotype id...")
    return prolog_query.execute_query(f"term_name(efo(X), {phenotype})")

@task(retries=2)
def summarize_graph(llm, causal_graph):
    print("Summarizing causal graph...")
    return llm.summarize_graph(causal_graph)

@task(retries=2)
def create_hypothesis(db, enrich_id, go_id, variant_id, phenotype, causal_gene, causal_graph, summary, current_user_id):
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
    return hypothesis_data["id"]