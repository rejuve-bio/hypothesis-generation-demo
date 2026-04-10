from loguru import logger
from prefect import task
from services.status_tracker import TaskState, status_tracker
from src.utils import emit_task_update, get_deps


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


### Hypothesis Tasks
@task(retries=2)
def check_hypothesis(current_user_id, enrich_id, go_id, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existence of hypothesis data",
            state=TaskState.STARTED,
            next_task="Getting enrichement data"
        )

        logger.info("Checking hypothesis data")
        deps = get_deps()
        hypotheses = deps["hypotheses"]

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


@task(retries=2)
def get_enrich(current_user_id, enrich_id, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting enrichement data",
            state=TaskState.STARTED,
            next_task="Getting gene data"
        )

        logger.info("Fetching enrich data...")
        deps = get_deps()
        enrichment = deps["enrichment"]
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
def get_gene_ids(gene_names, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting gene data",
            state=TaskState.STARTED,
            next_task="Querying gene data"
        )

        logger.info("Fetching gene IDs...")
        deps = get_deps()
        prolog_query = deps["prolog_query"]
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
def execute_gene_query(query, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying gene data",
            state=TaskState.STARTED,
            next_task="Querying variant data"
        )

        logger.info("Executing Prolog query to retrieve gene names...")
        deps = get_deps()
        prolog_query = deps["prolog_query"]
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
def execute_variant_query(query, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying variant data",
            state=TaskState.STARTED,
            next_task="Querying phenotype data"
        )
        logger.info("Executing Prolog query to retrieve variant ids...")
        deps = get_deps()
        prolog_query = deps["prolog_query"]
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
def execute_phenotype_query(phenotype, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying phenotype data",
            state=TaskState.STARTED,
            next_task="Generating graph summary"
        )
        logger.info("Executing Prolog query to retrieve phenotype id...")
        deps = get_deps()
        prolog_query = deps["prolog_query"]
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
def summarize_graph(causal_graph, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating graph summary",
            state=TaskState.STARTED,
            next_task="Generating hypothesis"
        )

        logger.info("Summarizing causal graph...")
        deps = get_deps()
        llm = deps["llm"]
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


@task(retries=2)
def create_hypothesis(enrich_id, go_id, variant_id, phenotype, causal_gene, causal_graph, summary, current_user_id, hypothesis_id):
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
            task_copy.pop('details', None)
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
        deps = get_deps()
        hypotheses = deps["hypotheses"]
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
