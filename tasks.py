from prefect import task
from datetime import datetime, timezone
from uuid import uuid4
from socketio_instance import socketio
from enum import Enum
from status_tracker import status_tracker, TaskState
from utils import emit_task_update

### Enrich Tasks
@task(retries=2, cache_policy=None)
def check_enrich(db, current_user_id, phenotype, variant, hypothesis_id):
    try:

        
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verify existence of enrichment data",
            state=TaskState.STARTED,
            progress=0  
        )
        
        if db.check_enrich(current_user_id, phenotype, variant):
            enrich = db.get_enrich_by_phenotype_and_variant(phenotype, variant, current_user_id)
            
            emit_task_update(
                hypothesis_id=hypothesis_id,
                task_name="Verify existence of enrichment data",
                state=TaskState.COMPLETED,
                progress=50,
                details={"found": True, "enrich": enrich}
            )
            return enrich
            
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verify existence of enrichment data",
            state=TaskState.COMPLETED,
            details={"found": False},
            next_task="Getting candidate genes"
        )
        return None
        
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verify existence of enrichment data",
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

        print("Executing: get candidate genes")
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

        print("Executing: predict causal gene")
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

        print("Executing: get relevant gene proof")
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

        print(f"Retrying predict causal gene with proof: {proof}")
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

        print("Retrying get relevant gene proof")
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
def create_enrich_data(db, variant, phenotype, causal_gene, relevant_gos, causal_graph, current_user_id, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Creating enrich data",
            state=TaskState.STARTED
        )

        print("Creating enrich data in the database")
        enrich_data = {
            "id": str(uuid4()),
            "created_at": datetime.now(timezone.utc).isoformat(timespec='milliseconds') + "Z",
            "variant": variant,
            "phenotype": phenotype,
            "causal_gene": causal_gene,
            "GO_terms": relevant_gos,
            "causal_graph": causal_graph
        }
        db.create_enrich(current_user_id, enrich_data)

        hypothesis_history = status_tracker.get_history(hypothesis_id)
        print("Updating hypothesis in the database...")
        hypothesis_data = {
                "task_history": hypothesis_history,
            }
        db.update_hypothesis(hypothesis_id, hypothesis_data)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Creating enrich data",
            state=TaskState.COMPLETED,
            details={"enrichment_id": enrich_data["id"]}
        )
        return enrich_data["id"]
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
            task_name="Veryfing existence of hypothesis data",
            state=TaskState.STARTED,
            next_task="Getting enrichement data"
        )

        print("Checking hypothesis data")
        if db.check_hypothesis(current_user_id, enrich_id, go_id):
            hypothesis = db.get_hypothesis_by_enrich_and_go(enrich_id, go_id, current_user_id)
            emit_task_update(
                hypothesis_id=hypothesis_id,
                task_name="Veryfing existence of hypothesis data",
                state=TaskState.COMPLETED,
                details={"found": True, "hypothesis": hypothesis}
            )
            return hypothesis
        
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Veryfing existance of hypothesis data",
            state=TaskState.COMPLETED,
            details={"found": False}
        )
        return None
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Veryfing existance of hypothesis data",
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

        print("Fetching enrich data...")
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

        print("Fetching gene IDs...")
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

        print("Executing Prolog query to retrieve gene names...")
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
        print("Executing Prolog query to retrieve variant ids...")
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
        print("Executing Prolog query to retrieve phenotype id...")
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

        print("Summarizing causal graph...")
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
        print("Creating hypothesis in the database...")
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
                "result": hypothesis_data  # Include the complete result
            }
        )
        return hypothesis_id
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating hypothesis",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise
