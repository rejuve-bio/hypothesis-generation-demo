from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import Union

from fastapi import APIRouter, Depends, Form, HTTPException, Query
from fastapi.responses import JSONResponse
from loguru import logger

from src.api.dependencies import (
    get_demo_template_handler,
    get_enrichment_handler,
    get_gene_expression_handler,
    get_hypothesis_handler,
    get_llm,
    get_project_handler,
)
from src.api.auth import get_current_user_id
from src.api.schemas import (
    BulkDeleteHypothesesRequest,
    BulkDeleteHypothesesResponse,
    ErrorResponse,
    FlexibleDict,
    FlexibleList,
    HypothesisChatForm,
    HypothesisChatResponse,
    HypothesisGraphResponse,
    MessageResponse,
)
from src.db import (
    DemoTemplateHandler,
    EnrichmentHandler,
    GeneExpressionHandler,
    HypothesisHandler,
    ProjectHandler,
)
from src.services.demo import (
    resolve_enrich_and_hypothesis_for_write,
    resolve_hypothesis_data_user_id,
)
from src.services.llm import LLM
from src.run_deployment import invoke_hypothesis_deployment
from src.services.status_tracker import TaskState, status_tracker
from src.tasks import extract_probability, get_related_hypotheses
from src.utils import (
    normalize_status_responses,
    public_task_history_entries,
    serialize_datetime_fields,
)

router = APIRouter(tags=["hypothesis"])

_HYPOTHESIS_FLOW_WAIT_TIMEOUT = float(os.getenv("HYPOTHESIS_FLOW_WAIT_TIMEOUT", "120"))


def _response_from_hypothesis_document(
    hypothesis_id: str,
    doc: dict | None,
    *,
    enrich_id: str | None = None,
    project_id: str | None = None,
    forked: bool = False,
) -> dict:
    response = {
        "id": hypothesis_id,
        "hypothesis_id": hypothesis_id,
        "summary": doc.get("summary") if doc else None,
        "graph": doc.get("graph") if doc else None,
        "warnings": doc.get("warnings", []) if doc else [],
    }
    if enrich_id is not None:
        response["enrich_id"] = enrich_id
    if project_id is not None:
        response["project_id"] = project_id
    response["forked"] = forked
    return response


def _mark_hypothesis_failed(
    hypotheses: HypothesisHandler, hypothesis_id: str, error: str
) -> None:
    """Persist a failed status so the hypothesis never sits ambiguously
    incomplete forever (e.g. a forked copy with no summary/graph yet)."""
    try:
        hypotheses.update_hypothesis(
            hypothesis_id,
            {
                "status": "failed",
                "error": error,
                "updated_at": datetime.now(timezone.utc).isoformat(
                    timespec="milliseconds"
                )
                + "Z",
            },
        )
    except Exception:
        logger.exception(f"Could not mark hypothesis {hypothesis_id} as failed")


@router.get(
    "/hypothesis",
    response_model=Union[FlexibleDict, FlexibleList],
    summary="Get hypothesis by id or list all",
)
async def get_hypothesis(
    id: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
    hypotheses: HypothesisHandler = Depends(get_hypothesis_handler),
    enrichment: EnrichmentHandler = Depends(get_enrichment_handler),
    gene_expression: GeneExpressionHandler = Depends(get_gene_expression_handler),
    demo_templates: DemoTemplateHandler = Depends(get_demo_template_handler),
):

    if id:
        data_user_id = resolve_hypothesis_data_user_id(
            demo_templates, hypotheses, current_user_id, id
        )
        if not data_user_id:
            raise HTTPException(
                status_code=404, detail="Hypothesis not found or access denied."
            )

        hypothesis = hypotheses.get_hypotheses(data_user_id, id)
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

        confidence = extract_probability(hypothesis, enrichment, data_user_id)
        related_hypotheses = get_related_hypotheses(
            hypothesis, hypotheses, enrichment, data_user_id
        )

        if is_complete:
            enrich_id = hypothesis.get("enrich_id")
            enrich_data = enrichment.get_enrich(data_user_id, enrich_id)
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
                "warnings": hypothesis.get("warnings", []),
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
                            data_user_id, project_id, variant_id
                        )
                        if tissue_selection:
                            selected_tissue = tissue_selection.get("tissue_name")
                except Exception as ts_e:
                    logger.warning(f"Could not get tissue selection: {ts_e}")

            response_data["tissue_selected"] = selected_tissue
            return FlexibleDict.model_validate(
                serialize_datetime_fields(response_data)
            )

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
            enrich_data = enrichment.get_enrich(data_user_id, enrich_id)
            if isinstance(enrich_data, dict):
                enrich_data.pop("causal_graph", None)
            status_data["result"] = enrich_data

        persisted = normalize_status_responses(hypothesis.get("status"))
        if persisted == "Failed":
            status_data["status"] = "Failed"
            if hypothesis.get("error") is not None:
                status_data["error"] = hypothesis.get("error")
            if hypothesis.get("error_detail") is not None:
                status_data["error_detail"] = hypothesis.get("error_detail")
        elif latest_state and latest_state.get("state") == "failed":
            status_data["status"] = "Failed"
            status_data["error"] = latest_state.get("error")
            task_details = latest_state.get("details")
            if isinstance(task_details, dict) and task_details.get("error_type"):
                status_data["error_detail"] = task_details

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
                        data_user_id, project_id, variant_id
                    )
                    if tissue_selection:
                        selected_tissue = tissue_selection.get("tissue_name")
            except Exception as ts_e:
                logger.warning(f"Could not get tissue selection: {ts_e}")

        status_data["tissue_selected"] = selected_tissue
        return FlexibleDict.model_validate(serialize_datetime_fields(status_data))

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

    return FlexibleList.model_validate(serialize_datetime_fields(formatted))


@router.post(
    "/hypothesis",
    status_code=200,
    response_model=HypothesisGraphResponse,
    summary="Generate hypothesis from enrichment",
)
async def post_hypothesis(
    id: str | None = Query(None, alias="id"),
    go: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
    hypotheses: HypothesisHandler = Depends(get_hypothesis_handler),
    enrichment: EnrichmentHandler = Depends(get_enrichment_handler),
    projects: ProjectHandler = Depends(get_project_handler),
    demo_templates: DemoTemplateHandler = Depends(get_demo_template_handler),
    gene_expression: GeneExpressionHandler = Depends(get_gene_expression_handler),
):
    """Generate hypothesis synchronously and return graph + summary immediately."""
    enrich_id = id
    go_id = go

    if not go_id:
        raise HTTPException(status_code=400, detail="go (GO term ID) is required")

    resolved = resolve_enrich_and_hypothesis_for_write(
        demo_templates=demo_templates,
        projects=projects,
        enrichment=enrichment,
        hypotheses=hypotheses,
        current_user_id=current_user_id,
        enrich_id=enrich_id,
        gene_expression=gene_expression,
    )
    if not resolved:
        raise HTTPException(
            status_code=404, detail="No hypothesis found for this enrichment"
        )

    write_ctx = resolved

    def run_hypothesis_deployment_blocking():
        return invoke_hypothesis_deployment(
            write_ctx.data_user_id,
            write_ctx.hypothesis_id,
            write_ctx.enrich_id,
            go_id,
            wait_timeout=_HYPOTHESIS_FLOW_WAIT_TIMEOUT,
        )

    loop = asyncio.get_running_loop()
    try:
        flow_run = await loop.run_in_executor(None, run_hypothesis_deployment_blocking)
    except Exception as e:
        logger.exception("Hypothesis Prefect run_deployment failed")
        _mark_hypothesis_failed(hypotheses, write_ctx.hypothesis_id, str(e))
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
        _mark_hypothesis_failed(
            hypotheses, write_ctx.hypothesis_id, "Hypothesis flow run has no state."
        )
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
        error_detail = state.message or "Hypothesis flow failed or was cancelled."
        _mark_hypothesis_failed(hypotheses, write_ctx.hypothesis_id, error_detail)
        raise HTTPException(status_code=500, detail=error_detail)

    if not state.is_completed():
        error_detail = state.message or "Hypothesis flow did not complete successfully."
        _mark_hypothesis_failed(hypotheses, write_ctx.hypothesis_id, error_detail)
        raise HTTPException(status_code=500, detail=error_detail)

    try:
        flow_return = state.result(raise_on_failure=True)
    except Exception as e:
        logger.exception("Could not load hypothesis flow result from Prefect state")
        _mark_hypothesis_failed(hypotheses, write_ctx.hypothesis_id, str(e))
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
            error_detail = body.get("message", "Not found")
            _mark_hypothesis_failed(hypotheses, write_ctx.hypothesis_id, error_detail)
            raise HTTPException(status_code=404, detail=error_detail)
        if status_code in (200, 201):
            refreshed = hypotheses.get_hypotheses(
                write_ctx.data_user_id, write_ctx.hypothesis_id
            )
            return HypothesisGraphResponse.model_validate(
                _response_from_hypothesis_document(
                    write_ctx.hypothesis_id,
                    refreshed,
                    enrich_id=write_ctx.enrich_id,
                    project_id=write_ctx.project_id,
                    forked=write_ctx.forked,
                )
            )
        error_detail = f"Unexpected hypothesis flow status code: {status_code}"
        _mark_hypothesis_failed(hypotheses, write_ctx.hypothesis_id, error_detail)
        raise HTTPException(status_code=500, detail=error_detail)

    refreshed = hypotheses.get_hypotheses(write_ctx.data_user_id, write_ctx.hypothesis_id)
    return HypothesisGraphResponse.model_validate(
        _response_from_hypothesis_document(
            write_ctx.hypothesis_id,
            refreshed,
            enrich_id=write_ctx.enrich_id,
            project_id=write_ctx.project_id,
            forked=write_ctx.forked,
        )
    )


@router.delete(
    "/hypothesis",
    response_model=MessageResponse,
    responses={404: {"model": MessageResponse}},
)
async def delete_hypothesis(
    hypothesis_id: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
    hypotheses: HypothesisHandler = Depends(get_hypothesis_handler),
):
    if hypothesis_id:
        result, status_code = hypotheses.delete_hypothesis(
            current_user_id, hypothesis_id
        )
        return JSONResponse(content=result, status_code=status_code)
    raise HTTPException(status_code=400, detail="Hypothesis ID is required")


@router.post(
    "/hypothesis/delete",
    response_model=BulkDeleteHypothesesResponse,
    responses={
        207: {"model": BulkDeleteHypothesesResponse},
        400: {"model": ErrorResponse},
        404: {"model": MessageResponse},
    },
    summary="Bulk delete hypotheses",
)
async def bulk_delete_hypotheses(
    data: BulkDeleteHypothesesRequest,
    current_user_id: str = Depends(get_current_user_id),
    hypotheses: HypothesisHandler = Depends(get_hypothesis_handler),
):
    hypothesis_ids = data.hypothesis_ids
    if not hypothesis_ids:
        raise HTTPException(
            status_code=400, detail="hypothesis_ids is required in request body"
        )
    if not isinstance(hypothesis_ids, list):
        raise HTTPException(status_code=400, detail="hypothesis_ids must be a list")

    result, status_code = hypotheses.bulk_delete_hypotheses(
        current_user_id, hypothesis_ids
    )
    return JSONResponse(content=result, status_code=status_code)


@router.post(
    "/chat",
    response_model=HypothesisChatResponse,
    summary="Chat over a hypothesis graph",
)
async def chat(
    query: str | None = Form(None),
    hypothesis_id: str | None = Form(None),
    current_user_id: str = Depends(get_current_user_id),
    hypotheses: HypothesisHandler = Depends(get_hypothesis_handler),
    llm: LLM = Depends(get_llm),
):
    form = {"query": query, "hypothesis_id": hypothesis_id}
    try:
        chat_form = HypothesisChatForm.from_form(form)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    hypothesis = hypotheses.get_hypotheses(current_user_id, chat_form.hypothesis_id)
    if not hypothesis:
        raise HTTPException(
            status_code=404, detail="Hypothesis not found or access denied"
        )

    graph = hypothesis.get("graph")
    response = llm.chat(chat_form.query, graph)
    return HypothesisChatResponse(response=response)
