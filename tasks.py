import logging
from prefect import task
from uuid import uuid4
from status_tracker import status_tracker, TaskState
from utils import emit_task_update
from loguru import logger
from cyvcf2 import VCF, Writer
from prefect import task, flow
import json
from datetime import datetime, timezone
from threading import Thread
import traceback



logging.basicConfig(level=logging.INFO)

### Helper Functions

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


def extract_probability(hypothesis, enrichment, user_id):
    """Extract probability from hypothesis graph or enrichment data."""
    probability = None
    
    # First try to get from hypothesis graph
    if hypothesis.get("graph") and isinstance(hypothesis["graph"], dict):
        probability = hypothesis["graph"].get("probability")
    
    # If no probability in hypothesis, try to get from enrichment data
    if probability is None and hypothesis.get("enrich_id"):
        try:
            enrich_data = enrichment.get_enrich(user_id, hypothesis["enrich_id"])
            if enrich_data and enrich_data.get("causal_graph"):
                causal_graph = enrich_data["causal_graph"]
                if isinstance(causal_graph, dict) and causal_graph.get("graph"):
                    graph = causal_graph["graph"]
                    if isinstance(graph, dict):
                        probability = graph.get('prob', {}).get('value') if isinstance(graph.get('prob'), dict) else None
        except Exception as e:
            logger.warning(f"Could not get enrichment data for hypothesis {hypothesis['id']}: {e}")
    
    return probability


def get_related_hypotheses(current_hypothesis, hypotheses, enrichment, user_id):
    """Get all hypotheses in the same project with the same variant."""
    all_variant_hypotheses = []
    
    project_id = current_hypothesis.get('project_id')
    variant = current_hypothesis.get('variant') or current_hypothesis.get('variant_id')
    
    if not project_id or not variant:
        # If no project/variant info, return just the current hypothesis
        current_probability = extract_probability(current_hypothesis, enrichment, user_id)
        return [{
            'id': current_hypothesis['id'],
            'causal_gene': current_hypothesis.get('causal_gene'),
            'probability': current_probability,
            'status': current_hypothesis.get('status', 'pending'),
            'go_id': current_hypothesis.get('go_id')
        }]
    
    try:
        # Get all hypotheses for the user
        all_hypotheses = hypotheses.get_hypotheses(user_id)
        if isinstance(all_hypotheses, list):
            for h in all_hypotheses:
                # Include ALL hypotheses with same project and variant (including current)
                if (h.get('project_id') == project_id and 
                    (h.get('variant') == variant or h.get('variant_id') == variant)):
                    
                    probability = extract_probability(h, enrichment, user_id)
                    all_variant_hypotheses.append({
                        'id': h['id'],
                        'causal_gene': h.get('causal_gene'),
                        'probability': probability,
                        'status': h.get('status', 'pending'),
                        'go_id': h.get('go_id')
                    })
            
            # Sort by confidence
            all_variant_hypotheses.sort(key=lambda x: x.get('probability') or 0, reverse=True)
            
    except Exception as e:
        logger.warning(f"Could not get variant hypotheses: {e}")
        current_probability = extract_probability(current_hypothesis, enrichment, user_id)
        all_variant_hypotheses = [{
            'id': current_hypothesis['id'],
            'causal_gene': current_hypothesis.get('causal_gene'),
            'probability': current_probability,
            'status': current_hypothesis.get('status', 'pending'),
            'go_id': current_hypothesis.get('go_id')
        }]
    
    return all_variant_hypotheses


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


def process_child_enrichments_simple(current_user_id, child_enrich_ids, parent_hypothesis_id, deps):
    """
    Process child enrichments in background each using its FIRST GO term
    """
    # Import here to avoid circular imports
    from flows import hypothesis_flow
    
    hypotheses = deps['hypotheses']
    enrichment = deps['enrichment']
    prolog_query = deps['prolog_query']
    llm = deps['llm']
    
    logger.info(f"Background processing started for {len(child_enrich_ids)} child enrichments")
    
    # Get parent hypothesis to extract project_id
    parent_hypothesis = hypotheses.get_hypotheses(current_user_id, parent_hypothesis_id)
    parent_project_id = parent_hypothesis.get('project_id') if parent_hypothesis else None
    
    try:
        for enrich_id in child_enrich_ids:
            logger.info(f"Processing child enrichment {enrich_id}")
            
            try:
                # Get enrichment data to find first GO term
                enrich_data = get_enrich.fn(enrichment, current_user_id, enrich_id, parent_hypothesis_id)
                if not enrich_data:
                    logger.warning(f"Could not get enrichment data for {enrich_id}")
                    continue
                
                # Get GO terms from enrichment
                go_terms = enrich_data.get("GO_terms", [])
                if not go_terms or len(go_terms) == 0:
                    logger.warning(f"No GO terms found for enrichment {enrich_id}")
                    continue
                
                # Get FIRST GO term only
                go_term = go_terms[0]
                go_id = go_term.get("id")
                if not go_id:
                    logger.warning(f"No GO ID found in first GO term for enrichment {enrich_id}")
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
                    logger.info(f"Hypothesis already exists for enrichment {enrich_id} + GO {go_id}")
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
                logger.info(f"Created hypothesis record {new_hypothesis_id} for child enrichment {enrich_id}")
                
                logger.info(f"Generating hypothesis for child enrichment {enrich_id}, GO: {go_id}")
                hypothesis_flow.fn(current_user_id, new_hypothesis_id, enrich_id, go_id, hypotheses, prolog_query, llm, enrichment)
                logger.info(f"Successfully generated background hypothesis {new_hypothesis_id}")
                
            except Exception as hyp_e:
                logger.error(f"Failed to generate hypothesis for enrichment {enrich_id}: {str(hyp_e)}")
                logger.exception(hyp_e)
                continue
        
        logger.info(f"Background hypothesis generation completed")
        
    except Exception as e:
        logger.error(f"Background hypothesis generation flow failed: {str(e)}")
        raise

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
def get_relevant_gene_proof(prolog_query, variant, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting relevant gene proof",
            state=TaskState.STARTED,
            next_task="Creating enrich data"
        )

        logger.info("Executing: get relevant gene proof")
        raw_response = prolog_query.get_relevant_gene_proof(variant, samples=10)
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
def retry_get_relevant_gene_proof(prolog_query, variant, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to get relevant gene proof",
            state=TaskState.RETRYING,
            next_task="Creating enrich data"
        )

        logger.info("Retrying get relevant gene proof")
        raw_response = prolog_query.get_relevant_gene_proof(variant, samples=10)
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
        
        # Remove 'details' field from task history to reduce size
        clean_history = []
        for task in hypothesis_history:
            task_copy = task.copy()
            task_copy.pop('details', None)  # Remove details field
            clean_history.append(task_copy)
        
        # Limit task_history to last 50 entries to avoid MongoDB document size limit (16MB)
        limited_history = clean_history[-50:] if len(clean_history) > 50 else clean_history
        
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
                "task_history": limited_history,
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

