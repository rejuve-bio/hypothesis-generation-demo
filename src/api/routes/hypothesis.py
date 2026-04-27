from __future__ import annotations

import asyncio
import os

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from loguru import logger

from src.api.dependencies import _deps
from src.api.auth import get_current_user_id
from src.run_deployment import invoke_hypothesis_deployment
from src.services.status_tracker import TaskState, status_tracker
from src.tasks import extract_probability, get_related_hypotheses
from src.utils import (
    normalize_status_responses,
    public_task_history_entries,
    serialize_datetime_fields,
)

router = APIRouter()

_HYPOTHESIS_FLOW_WAIT_TIMEOUT = float(os.getenv("HYPOTHESIS_FLOW_WAIT_TIMEOUT", "120"))


def _response_from_hypothesis_document(hypothesis_id: str, doc: dict | None) -> dict:
    return {
        "id": hypothesis_id,
        "summary": doc.get("summary") if doc else None,
        "graph": doc.get("graph") if doc else None,
    }


@router.get("/hypothesis")
async def get_hypothesis(
    id: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
):
    hypotheses = _deps["hypotheses"]
    enrichment = _deps["enrichment"]
    gene_expression = _deps.get("gene_expression")

    if id:
        hypothesis = hypotheses.get_hypotheses(current_user_id, id)
        if not hypothesis:
            raise HTTPException(
                status_code=404, detail="Hypothesis not found or access denied."
            )

        required_fields = ["enrich_id", "go_id", "summary", "graph"]
        is_complete = all(field in hypothesis for field in required_fields)

        task_history = status_tracker.get_history(id)
        for task in task_history:
            task.pop("details", None)

        pending_tasks = [t for t in task_history if t.get("state") == TaskState.STARTED.value]
        last_pending_task = [pending_tasks[-1]] if pending_tasks else []

        confidence = extract_probability(hypothesis, enrichment, current_user_id)
        related_hypotheses = get_related_hypotheses(
            hypothesis, hypotheses, enrichment, current_user_id
        )

        if is_complete:
            enrich_id = hypothesis.get("enrich_id")
            enrich_data = enrichment.get_enrich(current_user_id, enrich_id)
            if isinstance(enrich_data, dict):
                enrich_data.pop("causal_graph", None)

            response_data: dict = {
                "id": id,
                "variant": hypothesis.get("variant") or hypothesis.get("variant_id"),
                "enrich_id": enrich_id,
                "phenotype": hypothesis["phenotype"],
                "status": "Completed",
                "created_at": hypothesis.get("created_at"),
                "probability": confidence,
                "hypotheses": related_hypotheses,
                "result": enrich_data,
                "summary": hypothesis.get("summary"),
                "graph": hypothesis.get("graph"),
            }

            if "tissue_rankings" in hypothesis:
                response_data["tissue_rankings"] = hypothesis["tissue_rankings"]
                response_data["enrichment_type"] = hypothesis.get(
                    "enrichment_type", "tissue_enhanced"
                )
            else:
                response_data["enrichment_type"] = "standard"

            selected_tissue = None
            if gene_expression:
                try:
                    variant_id = (
                        hypothesis.get("variant_rsid")
                        or hypothesis.get("variant")
                        or hypothesis.get("variant_id")
                    )
                    project_id = hypothesis.get("project_id")
                    if variant_id and project_id:
                        tissue_selection = gene_expression.get_tissue_selection(
                            current_user_id, project_id, variant_id
                        )
                        if tissue_selection:
                            selected_tissue = tissue_selection.get("tissue_name")
                except Exception as ts_e:
                    logger.warning(f"Could not get tissue selection: {ts_e}")

            response_data["tissue_selected"] = selected_tissue
            return serialize_datetime_fields(response_data)

        latest_state = status_tracker.get_latest_state(id)

        status_data: dict = {
            "id": id,
            "variant": hypothesis.get("variant") or hypothesis.get("variant_id"),
            "phenotype": hypothesis["phenotype"],
            "status": "Running",
            "created_at": hypothesis.get("created_at"),
            "task_history": public_task_history_entries(last_pending_task),
            "probability": confidence,
            "hypotheses": related_hypotheses,
        }

        if "tissue_rankings" in hypothesis:
            status_data["tissue_rankings"] = hypothesis["tissue_rankings"]
            status_data["causal_gene"] = hypothesis.get("causal_gene")
            status_data["enrichment_stage"] = hypothesis.get("enrichment_stage")
            if hypothesis.get("enrichment_stage") == "tissue_analysis_complete":
                status_data["tissue_results_ready"] = True

        if "enrich_id" in hypothesis and hypothesis.get("enrich_id") is not None:
            enrich_id = hypothesis.get("enrich_id")
            status_data["enrich_id"] = enrich_id
            enrich_data = enrichment.get_enrich(current_user_id, enrich_id)
            if isinstance(enrich_data, dict):
                enrich_data.pop("causal_graph", None)
            status_data["result"] = enrich_data

        persisted = normalize_status_responses(hypothesis.get("status"))
        if persisted == "Failed":
            status_data["status"] = "Failed"
            if hypothesis.get("error") is not None:
                status_data["error"] = hypothesis.get("error")
        elif latest_state and latest_state.get("state") == "failed":
            status_data["status"] = "Failed"
            status_data["error"] = latest_state.get("error")

        selected_tissue = None
        if gene_expression:
            try:
                variant_id = (
                    hypothesis.get("variant_rsid")
                    or hypothesis.get("variant")
                    or hypothesis.get("variant_id")
                )
                project_id = hypothesis.get("project_id")
                if variant_id and project_id:
                    tissue_selection = gene_expression.get_tissue_selection(
                        current_user_id, project_id, variant_id
                    )
                    if tissue_selection:
                        selected_tissue = tissue_selection.get("tissue_name")
            except Exception as ts_e:
                logger.warning(f"Could not get tissue selection: {ts_e}")

        status_data["tissue_selected"] = selected_tissue
        return serialize_datetime_fields(status_data)

    # List all hypotheses for the user
    all_hypotheses = hypotheses.get_hypotheses(user_id=current_user_id)
    formatted: list[dict] = []
    for hypothesis in all_hypotheses:
        pending_tasks = [
            t
            for t in status_tracker.get_history(hypothesis["id"])
            if t.get("state") == TaskState.STARTED.value
        ]
        last_pending_task = [pending_tasks[-1]] if pending_tasks else []

        entry: dict = {
            "id": hypothesis["id"],
            "phenotype": hypothesis.get("phenotype"),
            "variant": hypothesis.get("variant") or hypothesis.get("variant_id"),
            "created_at": hypothesis.get("created_at"),
            "status": normalize_status_responses(hypothesis.get("status")),
            "task_history": public_task_history_entries(last_pending_task),
        }
        for field in ("enrich_id", "biological_context", "causal_gene"):
            if field in hypothesis and hypothesis.get(field) is not None:
                entry[field] = hypothesis[field]

        formatted.append(entry)

    return serialize_datetime_fields(formatted)


@router.post("/hypothesis", status_code=200)
async def post_hypothesis(
    id: str | None = Query(None, alias="id"),
    go: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
):
    """Generate hypothesis synchronously and return graph + summary immediately."""
    hypotheses = _deps["hypotheses"]
    enrich_id = id
    go_id = go

    if not go_id:
        raise HTTPException(status_code=400, detail="go (GO term ID) is required")

    hypothesis = hypotheses.get_hypothesis_by_enrich(current_user_id, enrich_id)
    if not hypothesis:
        raise HTTPException(
            status_code=404, detail="No hypothesis found for this enrichment"
        )

    hypothesis_id = hypothesis["id"]

    def run_hypothesis_deployment_blocking():
        return invoke_hypothesis_deployment(
            current_user_id,
            hypothesis_id,
            enrich_id,
            go_id,
            wait_timeout=_HYPOTHESIS_FLOW_WAIT_TIMEOUT,
        )

    loop = asyncio.get_running_loop()
    try:
        flow_run = await loop.run_in_executor(None, run_hypothesis_deployment_blocking)
    except Exception as e:
        logger.exception("Hypothesis Prefect run_deployment failed")
        raise HTTPException(
            status_code=503,
            detail=(
                "Could not run the hypothesis flow on Prefect. If the API logs show "
                "404 for deployments/name/hypothesis-flow/hypothesis-generation-deployment, "
                "the deployment is not registered—restart prefect-deployment so "
                "src/deployments.py runs. "
                f"Details: {e!s}"
            ),
        ) from e

    state = flow_run.state
    if state is None:
        raise HTTPException(
            status_code=502, detail="Hypothesis flow run has no state; check Prefect."
        )

    if not state.is_final():
        raise HTTPException(
            status_code=504,
            detail=(
                "Hypothesis generation did not finish in time; the run may still be "
                "active in Prefect. Try again shortly."
            ),
        )

    if state.is_failed() or state.is_crashed() or state.is_cancelled():
        raise HTTPException(
            status_code=500,
            detail=state.message or "Hypothesis flow failed or was cancelled.",
        )

    if not state.is_completed():
        raise HTTPException(
            status_code=500,
            detail=state.message or "Hypothesis flow did not complete successfully.",
        )

    try:
        flow_return = state.result(raise_on_failure=True)
    except Exception as e:
        logger.exception("Could not load hypothesis flow result from Prefect state")
        raise HTTPException(
            status_code=500,
            detail=f"Hypothesis flow finished but result could not be read: {e}",
        ) from e

    if (
        isinstance(flow_return, tuple)
        and len(flow_return) == 2
        and isinstance(flow_return[1], int)
    ):
        body, status_code = flow_return[0], flow_return[1]
        if status_code == 404:
            raise HTTPException(
                status_code=404, detail=body.get("message", "Not found")
            )
        if status_code in (200, 201):
            refreshed = hypotheses.get_hypotheses(current_user_id, hypothesis_id)
            return _response_from_hypothesis_document(hypothesis_id, refreshed)
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected hypothesis flow status code: {status_code}",
        )

    refreshed = hypotheses.get_hypotheses(current_user_id, hypothesis_id)
    return _response_from_hypothesis_document(hypothesis_id, refreshed)


@router.delete("/hypothesis")
async def delete_hypothesis(
    hypothesis_id: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
):
    hypotheses = _deps["hypotheses"]
    if hypothesis_id:
        return hypotheses.delete_hypothesis(current_user_id, hypothesis_id)
    raise HTTPException(status_code=400, detail="Hypothesis ID is required")


@router.post("/hypothesis/delete")
async def bulk_delete_hypotheses(
    data: dict = Body(...),
    current_user_id: str = Depends(get_current_user_id),
):
    hypotheses = _deps["hypotheses"]
    hypothesis_ids = data.get("hypothesis_ids")

    if not hypothesis_ids:
        raise HTTPException(
            status_code=400, detail="hypothesis_ids is required in request body"
        )
    if not isinstance(hypothesis_ids, list):
        raise HTTPException(status_code=400, detail="hypothesis_ids must be a list")
    if not hypothesis_ids:
        raise HTTPException(
            status_code=400, detail="hypothesis_ids list cannot be empty"
        )

    result, status_code = hypotheses.bulk_delete_hypotheses(current_user_id, hypothesis_ids)
    return JSONResponse(content=result, status_code=status_code)


@router.post("/chat")
async def chat(
    request: Request,
    current_user_id: str = Depends(get_current_user_id),
):
    form = await request.form()
    query = form.get("query")
    hypothesis_id = form.get("hypothesis_id")

    hypotheses = _deps["hypotheses"]
    llm = _deps["llm"]

    hypothesis = hypotheses.get_hypotheses(current_user_id, hypothesis_id)
    if not hypothesis:
        raise HTTPException(
            status_code=404, detail="Hypothesis not found or access denied"
        )

    graph = hypothesis.get("graph")
    response = llm.chat(query, graph)
    return {"response": response}
