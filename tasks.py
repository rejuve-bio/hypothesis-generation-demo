import logging
from prefect import task
from uuid import uuid4
from status_tracker import status_tracker, TaskState
from utils import emit_task_update
from loguru import logger
from cyvcf2 import VCF, Writer
from prefect import task, flow


logging.basicConfig(level=logging.INFO)

### Enrich Tasks
@task(retries=2, cache_policy=None)
def check_enrich(enrichment, current_user_id, variant, phenotype, hypothesis_id):
    """Check if enrichment exists for variant and phenotype"""
    try: 
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existence of enrichment data",
            state=TaskState.STARTED,
            progress=0  
        )
        
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
            details={"found": False},
            next_task="Getting candidate genes"
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
def get_candidate_genes(prolog_query, variant, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting candidate genes",
            state=TaskState.STARTED,
            next_task="Predicting causal gene",
        )

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

        logger.info("Executing: predict causal gene")
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

        logger.info("Executing: get relevant gene proof")
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

        logger.info(f"Retrying predict causal gene with proof: {proof}")
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

        logger.info("Retrying get relevant gene proof")
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
def create_enrich_data(enrichment, hypotheses, user_id, project_id, variant, phenotype, causal_gene, relevant_gos, causal_graph, hypothesis_id):
    """Create enrichment data with project references"""
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Creating enrich data",
            state=TaskState.STARTED
        )

        logger.info("Creating enrich data in the database with project context")
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
        logger.info("Updating hypothesis in the database...")
        hypothesis_data = {
                "task_history": hypothesis_history,
            }
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

### Hypothesis Tasks
@task(cache_policy=None, retries=2)
def check_hypothesis(hypotheses, current_user_id, enrich_id, go_id, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existence of hypothesis data",
            state=TaskState.STARTED,
            next_task="Getting enrichement data"
        )

        logger.info("Checking hypothesis data")
        if hypotheses.check_hypothesis(current_user_id, enrich_id, go_id):
            hypothesis = hypotheses.get_hypothesis_by_enrich_and_go(enrich_id, go_id, current_user_id)
            emit_task_update(
                hypothesis_id=hypothesis_id,
                task_name="Verifying existence of hypothesis data",
                state=TaskState.COMPLETED,
                progress=100,
                details={"found": True, "hypothesis": hypothesis}
            )
            return hypothesis
        
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existence of hypothesis data",
            state=TaskState.COMPLETED,
            details={"found": False}
        )
        return None
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existance of hypothesis data",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(cache_policy=None, retries=2)
def get_enrich(enrichment, current_user_id, enrich_id, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting enrichement data",
            state=TaskState.STARTED,
            next_task="Getting gene data"
        )

        logger.info("Fetching enrich data...")
        result = enrichment.get_enrich(current_user_id, enrich_id)

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

        logger.info("Fetching gene IDs...")
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

        logger.info("Executing Prolog query to retrieve gene names...")
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
        logger.info("Executing Prolog query to retrieve variant ids...")
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
        logger.info("Executing Prolog query to retrieve phenotype id...")
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

        logger.info("Summarizing causal graph...")
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
def create_hypothesis(hypotheses, enrich_id, go_id, variant_id, phenotype, causal_gene, causal_graph, summary, current_user_id, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating hypothesis",
            state=TaskState.STARTED,
            details={"go_id": go_id}
        )
        hypothesis_history = status_tracker.get_history(hypothesis_id)
        logger.info("Creating hypothesis in the database...")
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
        hypotheses.update_hypothesis(hypothesis_id, hypothesis_data)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating hypothesis",
            state=TaskState.COMPLETED,
            details={
                "status": "completed",
                "result": hypothesis_data
            }
        )
        hypothesis_history = status_tracker.get_history(hypothesis_id)
        logger.info("Updating hypothesis in the database...")
        hypothesis_data = {
                "task_history": hypothesis_history,
            }
        hypotheses.update_hypothesis(hypothesis_id, hypothesis_data)
        
        return hypothesis_id
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating hypothesis",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

