from __future__ import annotations

import asyncio
import json
import os
import re
import tempfile
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import jwt
import requests as _http
from fastapi import (
    APIRouter,
    Body,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    UploadFile,
)
from fastapi.responses import FileResponse, JSONResponse
from loguru import logger
from werkzeug.utils import secure_filename

from auth import get_current_user_id, verify_service_token
from project_tasks import count_gwas_records, get_project_with_full_data
from run_deployment import (
    invoke_analysis_pipeline_deployment,
    invoke_enrichment_deployment,
)
from flows import hypothesis_flow
from socketio_instance import sio
from status_tracker import TaskState, status_tracker
from tasks import extract_probability, get_related_hypotheses
from utils import (
    allowed_file,
    compute_file_md5,
    convert_variants_to_object_array,
    get_shared_temp_dir,
    serialize_datetime_fields,
)

router = APIRouter()


def _download_to_path_sync(url: str, path: str) -> int:
    with _http.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return os.path.getsize(path)


_deps: dict[str, Any] = {}

_JWT_SECRET = os.getenv("JWT_SECRET")


def init_deps(deps: dict[str, Any]) -> None:
    """Called once at application startup to register shared dependencies."""
    global _deps
    _deps.update(deps)


def _extract_token_from_environ(environ: dict) -> str | None:
    """Pull a Bearer token from Socket.IO connect environ (ASGI scope extras)."""
    auth_header = environ.get("HTTP_AUTHORIZATION", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:]

    query_string = environ.get("QUERY_STRING", "")
    for param in query_string.split("&"):
        if param.startswith("token="):
            return param[6:]

    return None

@sio.on("connect")
async def handle_connect(sid: str, environ: dict, auth: dict | None = None) -> bool:
    """Authenticate every connecting client; reject if token is missing/invalid."""
    token: str | None = None

    if auth and isinstance(auth, dict):
        token = auth.get("token")

    if not token:
        token = _extract_token_from_environ(environ)

    if not token:
        logger.warning(f"[SIO] Rejected connection {sid}: no token")
        return False

    try:
        data = jwt.decode(token, _JWT_SECRET, algorithms=["HS256"])

        if data.get("service") == "prefect":
            await sio.save_session(sid, {"user_id": None, "is_service": True})
            logger.info(f"[SIO] Service (Prefect) connection accepted: {sid}")
        else:
            user_id = str(data["user_id"])
            await sio.save_session(sid, {"user_id": user_id, "is_service": False})
            logger.info(f"[SIO] User {user_id} connected: {sid}")

        return True

    except Exception as exc:
        logger.warning(f"[SIO] Rejected connection {sid}: {exc}")
        return False


@sio.on("disconnect")
async def handle_disconnect(sid: str) -> None:
    logger.info(f"[SIO] Client disconnected: {sid}")


@sio.on("subscribe_hypothesis")
async def handle_subscribe(sid: str, data: dict | str) -> dict:
    """Join a hypothesis room and immediately push the current state."""
    try:
        session = await sio.get_session(sid)
        user_id: str | None = session.get("user_id")

        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                return {"error": "Invalid JSON format"}

        if not isinstance(data, dict) or "hypothesis_id" not in data:
            return {"error": 'Expected format: {"hypothesis_id": "value"}'}

        hypothesis_id: str | None = data.get("hypothesis_id")
        if not hypothesis_id:
            return {"error": "hypothesis_id is required"}

        hypotheses_handler = _deps.get("hypotheses")
        hypothesis = (
            hypotheses_handler.get_hypotheses(user_id, hypothesis_id)
            if user_id
            else hypotheses_handler.get_hypothesis_by_id(hypothesis_id)
        )
        if not hypothesis:
            raise ValueError("Hypothesis not found or access denied")

        room = f"hypothesis_{hypothesis_id}"
        await sio.enter_room(sid, room)
        logger.info(f"[SIO] {sid} joined room '{room}'")

        task_history = status_tracker.get_history(hypothesis_id)
        required_fields = ["enrich_id", "go_id", "summary", "graph"]
        is_complete = all(field in hypothesis for field in required_fields)

        response_data: dict = {
            "hypothesis_id": hypothesis_id,
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds") + "Z",
            "task_history": task_history,
        }

        if is_complete:
            response_data.update(
                {"status": "completed", "result": hypothesis, "progress": 100}
            )
        else:
            latest_state = status_tracker.get_latest_state(hypothesis_id)
            progress = status_tracker.calculate_progress(task_history)
            response_data.update({"status": "pending", "progress": progress})

            if "tissue_rankings" in hypothesis:
                response_data["tissue_rankings"] = hypothesis["tissue_rankings"]
                response_data["tissue_results_ready"] = True
                response_data["causal_gene"] = hypothesis.get("causal_gene")
                response_data["enrichment_stage"] = hypothesis.get("enrichment_stage")

            if latest_state and latest_state.get("state") == "failed":
                response_data["status"] = "failed"
                response_data["error"] = latest_state.get("error")

        response_data = serialize_datetime_fields(response_data)

        # Emit current state back to just this client (same event the old code used)
        await sio.emit("task_update", response_data, to=sid)
        return {"status": "subscribed", "room": room}

    except Exception as exc:
        logger.error(f"[SIO] Error in subscribe_hypothesis: {exc}")
        return {"error": str(exc)}


@sio.on("subscribe_analysis")
async def handle_subscribe_analysis(sid: str, data: dict | str) -> dict:
    """Join an analysis pipeline room and push the current saved analysis state."""
    try:
        session = await sio.get_session(sid)
        user_id: str | None = session.get("user_id")
        if not user_id:
            return {"error": "Authentication required"}

        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                return {"error": "Invalid JSON format"}

        if not isinstance(data, dict) or "project_id" not in data:
            return {"error": 'Expected format: {"project_id": "value"}'}

        project_id: str | None = data.get("project_id")
        if not project_id:
            return {"error": "project_id is required"}

        projects = _deps["projects"]
        project = projects.get_projects(user_id, project_id)
        if not project:
            raise ValueError("Project not found or access denied")

        room = f"analysis_{project_id}"
        await sio.enter_room(sid, room)
        logger.info(f"[SIO] {sid} joined room '{room}'")

        analysis_state = projects.load_analysis_state(user_id, project_id) or {}
        response_data = {
            "project_id": project_id,
            "user_id": user_id,
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds") + "Z",
            **analysis_state,
        }
        response_data = serialize_datetime_fields(response_data)
        await sio.emit("analysis_update", response_data, to=sid)
        return {"status": "subscribed", "room": room}

    except Exception as exc:
        logger.error(f"[SIO] Error in subscribe_analysis: {exc}")
        return {"error": str(exc)}


@sio.on("task_update")
async def handle_task_update(sid: str, data: dict) -> None:
    try:
        session = await sio.get_session(sid)
        if not session.get("is_service"):
            logger.warning(f"[SIO] Non-service client {sid} tried to emit task_update – ignored")
            return

        target_room = data.pop("target_room", None)
        if not target_room:
            logger.error("[SIO] task_update missing target_room")
            return

        await sio.emit("task_update", data, room=target_room)
        logger.info(f"[SIO] Relayed task_update to room '{target_room}'")

    except Exception as exc:
        logger.error(f"[SIO] Error in handle_task_update: {exc}")


@router.post("/internal/task-update", status_code=200)
async def internal_task_update(
    payload: dict = Body(...),
    _: None = Depends(verify_service_token),
) -> dict:
    """Receive a Prefect task-update POST and broadcast to Socket.IO room."""
    target_room = payload.pop("target_room", None)
    if not target_room:
        raise HTTPException(status_code=400, detail="target_room is required")

    event_name = payload.pop("event", "task_update")
    await sio.emit(event_name, payload, room=target_room)
    logger.info(f"[HTTP bridge] Broadcast {event_name} to room '{target_room}'")
    return {"status": "broadcasted", "room": target_room, "event": event_name}


# ──────────────────────────────────────────────────────────────────────────────
# /enrich
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/enrich")
async def get_enrich(
    id: str | None = Query(None),
    project_id: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
):
    enrichment = _deps["enrichment"]

    if id:
        enrich = enrichment.get_enrich(current_user_id, id)
        if not enrich:
            raise HTTPException(status_code=404, detail="Enrich not found or access denied.")
        return serialize_datetime_fields(enrich)

    if project_id:
        enrichments = enrichment.get_enrich(user_id=current_user_id)
        if isinstance(enrichments, list):
            project_enrichments = [
                e for e in enrichments if e.get("project_id") == project_id
            ]
            return {"enrichments": serialize_datetime_fields(project_enrichments)}
        else:
            if enrichments and enrichments.get("project_id") == project_id:
                return {"enrichments": [serialize_datetime_fields(enrichments)]}
            return {"enrichments": []}

    enrich = enrichment.get_enrich(user_id=current_user_id)
    return serialize_datetime_fields(enrich)


@router.post("/enrich", status_code=202)
async def post_enrich(
    request: Request,
    current_user_id: str = Depends(get_current_user_id),
):
    body: dict = {}
    try:
        body = await request.json()
    except Exception:
        pass

    variant = request.query_params.get("variant") or body.get("variant")
    project_id = request.query_params.get("project_id") or body.get("project_id")
    seed = int(body.get("seed", 42))

    if not project_id:
        raise HTTPException(status_code=400, detail="project_id is required")
    if not variant:
        raise HTTPException(status_code=400, detail="variant is required")

    projects = _deps["projects"]
    enrichment = _deps["enrichment"]
    hypotheses = _deps["hypotheses"]
    gene_expression = _deps.get("gene_expression")

    project = projects.get_projects(current_user_id, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found or access denied")

    phenotype = project["phenotype"]

    tissue_name = request.query_params.get("tissue_name") or body.get("tissue_name")
    if not tissue_name:
        raise HTTPException(status_code=400, detail="tissue_name is required")

    try:
        available_tissues = gene_expression.get_ldsc_results_for_project(
            current_user_id, project_id, limit=20, format="selection"
        )
        tissue_names = [t.get("tissue_name") for t in (available_tissues or [])]
        if tissue_name not in tissue_names:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid tissue selection. Available tissues: {tissue_names}",
            )
        gene_expression.save_tissue_selection(
            current_user_id, project_id, variant, tissue_name
        )
        logger.info(f"Saved tissue selection in /enrich: {tissue_name} for variant {variant}")
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(f"Failed to save/validate tissue selection: {exc}")

    existing_hypothesis = hypotheses.get_hypothesis_by_phenotype_and_variant_in_project(
        current_user_id, project_id, phenotype, variant
    )

    if existing_hypothesis:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: invoke_enrichment_deployment(
                current_user_id=current_user_id,
                phenotype=phenotype,
                variant=variant,
                hypothesis_id=existing_hypothesis["id"],
                project_id=project_id,
                seed=seed,
            ),
        )
        return {"hypothesis_id": existing_hypothesis["id"], "project_id": project_id}

    hypothesis_id = str(uuid4())
    hypothesis_data = {
        "id": hypothesis_id,
        "project_id": project_id,
        "phenotype": phenotype,
        "variant": variant,
        "variant_rsid": variant,
        "status": "pending",
        "created_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds") + "Z",
        "task_history": [],
    }

    hypotheses.create_hypothesis(current_user_id, hypothesis_data)

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        lambda: invoke_enrichment_deployment(
            current_user_id=current_user_id,
            phenotype=phenotype,
            variant=variant,
            hypothesis_id=hypothesis_id,
            project_id=project_id,
            seed=seed,
        ),
    )
    return {"hypothesis_id": hypothesis_id, "project_id": project_id}


@router.delete("/enrich")
async def delete_enrich(
    id: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
):
    enrichment = _deps["enrichment"]
    if id:
        result = enrichment.delete_enrich(current_user_id, id)
        return result
    raise HTTPException(status_code=400, detail="enrich id is required!")


# ──────────────────────────────────────────────────────────────────────────────
# /hypothesis
# ──────────────────────────────────────────────────────────────────────────────

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
                "status": "completed",
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
            "status": "pending",
            "created_at": hypothesis.get("created_at"),
            "task_history": last_pending_task,
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

        if latest_state and latest_state.get("state") == "failed":
            status_data["status"] = "failed"
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
            "status": hypothesis.get("status"),
            "task_history": last_pending_task,
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

    def run_hypothesis_flow():
        result = hypothesis_flow(current_user_id, hypothesis_id, enrich_id, go_id)
        return result

    loop = asyncio.get_running_loop()
    result, status_code = await loop.run_in_executor(None, run_hypothesis_flow)

    if status_code == 404:
        raise HTTPException(status_code=404, detail=result.get("message", "Not found"))
    if status_code == 200:
        return {"id": hypothesis_id, "summary": result.get("summary"), "graph": result.get("graph")}
    return {"id": hypothesis_id, "summary": result.get("summary"), "graph": result.get("graph")}


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


# ──────────────────────────────────────────────────────────────────────────────
# /chat
# ──────────────────────────────────────────────────────────────────────────────

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


# ──────────────────────────────────────────────────────────────────────────────
# /projects
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/projects")
async def get_projects(
    id: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
):
    projects = _deps["projects"]
    analysis = _deps["analysis"]
    hypotheses = _deps["hypotheses"]
    enrichment = _deps["enrichment"]
    gene_expression = _deps.get("gene_expression")

    if id:
        response_data, status_code = get_project_with_full_data(
            projects,
            analysis,
            hypotheses,
            enrichment,
            current_user_id,
            id,
            gene_expression_handler=gene_expression,
        )
        if status_code == 200:
            response_data = serialize_datetime_fields(response_data)
        return JSONResponse(content=response_data, status_code=status_code)

    raw_projects = projects.get_projects(current_user_id)
    files = _deps["files"]
    enhanced_projects: list[dict] = []

    for project in raw_projects:
        enhanced: dict = {
            "id": project["id"],
            "name": project["name"],
            "phenotype": project.get("phenotype", ""),
            "created_at": project.get("created_at"),
        }

        file_metadata = files.get_file_metadata(current_user_id, project["gwas_file_id"])
        enhanced["gwas_file"] = file_metadata["download_url"]
        enhanced["gwas_records_count"] = file_metadata["record_count"]

        try:
            analysis_state = projects.load_analysis_state(current_user_id, project["id"])
            enhanced["status"] = (
                analysis_state.get("status", "Not_started")
                if analysis_state
                else "Not_started"
            )
        except Exception as state_e:
            logger.warning(f"Could not load analysis state for project {project['id']}: {state_e}")
            enhanced["status"] = "Completed"

        enhanced["population"] = project.get("population")
        enhanced["ref_genome"] = project.get("ref_genome")

        total_credible_sets = 0
        total_variants = 0
        try:
            credible_sets_raw = analysis.get_credible_sets_for_project(
                current_user_id, project["id"]
            )
            if credible_sets_raw and isinstance(credible_sets_raw, list):
                total_credible_sets = len(credible_sets_raw)
                total_variants = sum(
                    cs.get("variants_count", 0) for cs in credible_sets_raw
                )
        except Exception as cs_e:
            logger.warning(f"Could not load credible sets for {project['id']}: {cs_e}")

        enhanced["total_credible_sets_count"] = total_credible_sets
        enhanced["total_variants_count"] = total_variants

        hypothesis_count = 0
        try:
            all_hyp = hypotheses.get_hypotheses(current_user_id)
            if isinstance(all_hyp, list):
                hypothesis_count = sum(
                    1 for h in all_hyp if h.get("project_id") == project["id"]
                )
            elif all_hyp and all_hyp.get("project_id") == project["id"]:
                hypothesis_count = 1
        except Exception as hyp_e:
            logger.warning(f"Could not count hypotheses for {project['id']}: {hyp_e}")

        enhanced["hypothesis_count"] = hypothesis_count
        enhanced_projects.append(enhanced)

    return {"projects": serialize_datetime_fields(enhanced_projects)}


@router.delete("/projects")
async def delete_project(
    id: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
):
    if not id:
        raise HTTPException(status_code=400, detail="Project ID is required")
    projects = _deps["projects"]
    success = projects.delete_project(current_user_id, id)
    if success:
        return {"message": "Project deleted successfully"}
    raise HTTPException(status_code=404, detail="Project not found or access denied")


@router.post("/projects/delete")
async def bulk_delete_projects(
    data: dict = Body(...),
    current_user_id: str = Depends(get_current_user_id),
):
    projects = _deps["projects"]
    project_ids = data.get("project_ids")

    if not project_ids:
        raise HTTPException(
            status_code=400, detail="project_ids is required in request body"
        )
    if not isinstance(project_ids, list):
        raise HTTPException(status_code=400, detail="project_ids must be a list")
    if not project_ids:
        raise HTTPException(
            status_code=400, detail="project_ids list cannot be empty"
        )

    result = projects.bulk_delete_projects(current_user_id, project_ids)

    if result and isinstance(result, dict):
        if result["success"]:
            return {
                "message": f"Successfully deleted {result['deleted_count']} project(s)",
                "deleted_count": result["deleted_count"],
                "total_requested": result["total_requested"],
            }
        return JSONResponse(
            content={
                "message": (
                    f"Partially deleted {result['deleted_count']}/{result['total_requested']}"
                    " project(s)"
                ),
                "deleted_count": result["deleted_count"],
                "total_requested": result["total_requested"],
                "errors": result.get("errors"),
            },
            status_code=207,
        )
    raise HTTPException(status_code=500, detail="Failed to delete projects")


# ──────────────────────────────────────────────────────────────────────────────
# /analysis-pipeline
# ──────────────────────────────────────────────────────────────────────────────

@router.post("/analysis-pipeline", status_code=202)
async def post_analysis_pipeline(
    request: Request,
    current_user_id: str = Depends(get_current_user_id),
):
    try:
        form = await request.form()

        project_name: str | None = form.get("project_name")
        population: str = form.get("population", "EUR")
        max_workers: int = int(form.get("max_workers", 3))
        is_uploaded: bool = form.get("is_uploaded", "false").lower() == "true"

        gwas_file = form.get("gwas_file") if is_uploaded else None

        maf_threshold: float = float(form.get("maf_threshold", 0.01))
        seed: int = int(form.get("seed", 42))
        window: int = int(form.get("window", 2000))
        L: int = int(form.get("L", -1))
        coverage: float = float(form.get("coverage", 0.95))
        min_abs_corr: float = float(form.get("min_abs_corr", 0.5))
        batch_size: int = int(form.get("batch_size", 5))
        sample_size: int = int(form.get("sample_size", 10000))

        projects = _deps["projects"]
        files = _deps["files"]
        analysis = _deps["analysis"]
        gene_expression = _deps.get("gene_expression")
        config = _deps["config"]
        storage = _deps.get("storage")
        gwas_library = _deps.get("gwas_library")

        gwas_entry = None
        file_id_param: str | None = form.get("gwas_file") if not is_uploaded else None
        
        if not is_uploaded and file_id_param and gwas_library:
            gwas_entry = gwas_library.get_gwas_entry(file_id=file_id_param)

        phenotype: str | None = form.get("phenotype")
        if not phenotype and gwas_entry:
            phenotype = gwas_entry.get("description") or gwas_entry.get("phenotype_code")
            # Clean up leading '#' if it exists in the library description
            if isinstance(phenotype, str) and phenotype.startswith("#"):
                phenotype = phenotype.lstrip("#").strip()

        raw_ref_genome = form.get("ref_genome")
        ref_genome: str = raw_ref_genome or "GRCh37"
        
        if gwas_entry and (not raw_ref_genome or raw_ref_genome == "GRCh37"):
            inferred_build = gwas_entry.get("genome_build")
            if inferred_build:
                # Normalize common library build string formats
                if "38" in inferred_build:
                    ref_genome = "GRCh38"
                elif "37" in inferred_build or "19" in inferred_build:
                    ref_genome = "GRCh37"

        if not project_name:
            raise HTTPException(status_code=400, detail="project_name is required")
        if not phenotype:
            raise HTTPException(status_code=400, detail="phenotype is required (could not be inferred from library)")

        if is_uploaded and gwas_file and not allowed_file(gwas_file.filename):
            raise HTTPException(
                status_code=400,
                detail="Invalid file format. Supported: .tsv, .txt, .csv, .gz, .bgz",
            )
        if ref_genome not in ("GRCh37", "GRCh38"):
            raise HTTPException(
                status_code=400, detail="Reference genome must be GRCh37 or GRCh38"
            )
        if population not in ("EUR", "AFR", "AMR", "EAS", "SAS"):
            raise HTTPException(
                status_code=400,
                detail="Population must be one of: EUR, AFR, AMR, EAS, SAS",
            )
        if not (1 <= max_workers <= 16):
            raise HTTPException(
                status_code=400, detail="Max workers must be between 1-16"
            )
        if not (0.001 <= maf_threshold <= 0.5):
            raise HTTPException(
                status_code=400, detail="MAF threshold must be between 0.001-0.5"
            )
        if not (1 <= seed <= 999999):
            raise HTTPException(
                status_code=400, detail="Seed must be between 1-999999"
            )
        if window > 10000:
            raise HTTPException(
                status_code=400,
                detail="Fine-mapping window shouldn't be greater than 10000 kb",
            )
        if L != -1 and not (1 <= L <= 50):
            raise HTTPException(
                status_code=400, detail="L must be -1 (auto) or between 1-50"
            )
        if not (0.5 <= coverage <= 0.999):
            raise HTTPException(
                status_code=400, detail="Coverage must be between 0.5-0.999"
            )
        if not (0.5 <= min_abs_corr <= 1.0):
            raise HTTPException(
                status_code=400,
                detail="Min absolute correlation must be between 0.5-1.0",
            )
        if not (1 <= batch_size <= 20):
            raise HTTPException(
                status_code=400, detail="Batch size must be between 1-20"
            )

        start_time = datetime.now()
        file_metadata_id = None
        file_path: str | None = None
        filename: str | None = None
        file_size: int = 0
        gwas_records_count: int = 0
        original_filename: str | None = None
        file_id_new: str | None = None
        _file_needs_processing: bool = False
        _source_minio_path: str | None = None
        _source_download_url: str | None = None
        _minio_cache_key: str | None = None
        _gwas_library_id: str | None = None

        if not is_uploaded:
            file_id_param: str | None = form.get("gwas_file")
            if not file_id_param:
                raise HTTPException(
                    status_code=400,
                    detail="gwas_file parameter is required when is_uploaded=false",
                )

            logger.info(f"[API] Auto-detecting source for file ID: {file_id_param}")

            file_meta = None
            try:
                file_meta = files.get_file_metadata(current_user_id, file_id_param)
            except Exception as exc:
                logger.info(f"[API] Not a valid user file ID, checking library: {exc}")

            if file_meta:
                storage_key = file_meta.get("storage_key")
                filename = file_meta.get("filename")
                file_size = file_meta.get("file_size", 0)
                gwas_records_count = file_meta.get("record_count", 0)
                original_filename = filename

                if not storage_key:
                    raise HTTPException(
                        status_code=400,
                        detail="File does not have storage information.",
                    )
                if not storage:
                    raise HTTPException(
                        status_code=500, detail="Storage service not available"
                    )

                # Let Prefect download from MinIO — no blocking I/O in the API.
                file_path = ""
                file_metadata_id = file_id_param
                _file_needs_processing = True
                _source_minio_path = storage_key
            else:
                if not gwas_entry:
                    raise HTTPException(
                        status_code=404,
                        detail=f"File not found in user library or system library: {file_id_param}",
                    )

                filename = gwas_entry.get("filename", file_id_param)
                original_filename = filename
                minio_path_lib = f"gwas_cache/{filename}"
                file_size = gwas_entry.get("file_size", 0)

                if storage and gwas_entry.get("downloaded") and gwas_entry.get("minio_path"):
                    minio_path = gwas_entry["minio_path"]
                    loop = asyncio.get_running_loop()
                    file_exists = await loop.run_in_executor(None, storage.exists, minio_path)
                    if file_exists:
                        # Cached in MinIO — Prefect will download it.
                        file_path = ""
                        _file_needs_processing = True
                        _source_minio_path = minio_path
                    else:
                        gwas_library.update_gwas_entry(
                            file_id_param, {"downloaded": False, "minio_path": None}
                        )

                if not _source_minio_path:
                    # Check local raw directory (rare edge case, already on disk)
                    raw_data_path = os.path.join(config.data_dir, "raw")
                    for ext in (".tsv", ".tsv.gz", ".tsv.bgz", ".txt", ".txt.gz", ".csv", ".csv.gz"):
                        candidate = os.path.join(raw_data_path, f"{file_id_param}{ext}")
                        if os.path.exists(candidate):
                            file_path = candidate
                            filename = f"{file_id_param}{ext}"
                            file_size = os.path.getsize(file_path)
                            _file_needs_processing = True
                            break

                if not _source_minio_path and not file_path:
                    # Not cached anywhere — Prefect will download from external URL.
                    download_url = (
                        gwas_entry.get("aws_url")
                        or (
                            re.search(r"(https?://[^\s]+)", gwas_entry["wget_command"]).group(1)
                            if gwas_entry.get("wget_command")
                            else None
                        )
                        or gwas_entry.get("dropbox_url")
                    )
                    if not download_url:
                        raise HTTPException(
                            status_code=404,
                            detail=f"No download URL available for {file_id_param}",
                        )
                    file_path = ""
                    _file_needs_processing = True
                    _source_download_url = download_url
                    _minio_cache_key = minio_path_lib if storage else None
                    _gwas_library_id = file_id_param

                if not _source_minio_path and not _source_download_url and not file_path:
                    raise HTTPException(
                        status_code=404,
                        detail=f"GWAS file not found: {file_id_param}",
                    )

                gwas_records_count = 0  # filled in by prepare_gwas_file_task

        else:
            # Uploaded file
            if not gwas_file or gwas_file.filename == "":
                raise HTTPException(status_code=400, detail="No GWAS file uploaded")
            if not allowed_file(gwas_file.filename):
                raise HTTPException(
                    status_code=400,
                    detail="Invalid file format. Supported: .tsv, .txt, .csv, .gz, .bgz",
                )

            original_filename = gwas_file.filename
            filename = secure_filename(gwas_file.filename)
            file_id_new = str(uuid.uuid4())
            temp_dir = get_shared_temp_dir(user_id=current_user_id, prefix="upload")
            temp_file_path = os.path.join(temp_dir, filename)

            with open(temp_file_path, "wb") as fh:
                while chunk := await gwas_file.read(1024 * 1024):
                    fh.write(chunk)

            file_size = os.path.getsize(temp_file_path)
            md5_hash = compute_file_md5(temp_file_path)

            existing_file = None
            if md5_hash and storage:
                existing_file = files.find_file_by_md5(current_user_id, md5_hash)

            if existing_file:
                storage_key = existing_file.get("storage_key")
                if storage_key and storage.exists(storage_key):
                    storage.download_file(storage_key, temp_file_path)
                    file_path = temp_file_path
                    file_metadata_id = existing_file.get("_id")
                    gwas_records_count = existing_file.get(
                        "record_count", count_gwas_records(file_path)
                    )
                else:
                    raise HTTPException(
                        status_code=500,
                        detail="Failed to retrieve existing file from storage",
                    )
            else:
                # New file — hand off upload + record counting to the Prefect flow
                # so this HTTP response returns as soon as project_id is created.
                file_path = temp_file_path
                gwas_records_count = 0  # filled in by process_uploaded_file_task
                object_key = (
                    f"uploads/{current_user_id}/{file_id_new}/{filename}"
                    if storage else None
                )
                md5_hash_param: str | None = md5_hash if storage else None
                _file_needs_processing = True

        if not file_metadata_id:
            source = "gwas_library" if not is_uploaded else "user_upload"
            storage_key_arg = object_key if is_uploaded and storage else None
            md5_arg = md5_hash_param if is_uploaded else None

            file_metadata_id = files.create_file_metadata(
                user_id=current_user_id,
                filename=filename,
                original_filename=original_filename,
                file_path=file_path,
                file_type="gwas",
                file_size=file_size,
                record_count=gwas_records_count,
                download_url=f"/download/{str(uuid.uuid4())}",
                md5_hash=md5_arg,
                storage_key=storage_key_arg,
                source=source,
            )

        analysis_parameters = {
            "maf_threshold": maf_threshold,
            "seed": seed,
            "window": window,
            "L": L,
            "coverage": coverage,
            "min_abs_corr": min_abs_corr,
            "batch_size": batch_size,
            "max_workers": max_workers,
        }

        project_id = projects.create_project(
            user_id=current_user_id,
            name=project_name,
            gwas_file_id=file_metadata_id,
            phenotype=phenotype,
            population=population,
            ref_genome=ref_genome,
            analysis_parameters=analysis_parameters,
        )

        metadata_dir = os.path.join("data", "metadata", str(current_user_id))
        os.makedirs(metadata_dir, exist_ok=True)
        metadata = {
            "file_id": file_metadata_id,
            "user_id": current_user_id,
            "filename": filename,
            "original_filename": original_filename,
            "file_path": file_path,
            "file_type": "gwas",
            "upload_date": str(datetime.now()),
            "file_size": file_size,
            "project_id": project_id,
        }
        with open(os.path.join(metadata_dir, f"{file_metadata_id}.json"), "w") as fh:
            json.dump(metadata, fh)

        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[API] Project {project_id} ready in {total_time:.1f}s, firing Prefect")

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: invoke_analysis_pipeline_deployment(
                user_id=current_user_id,
                project_id=project_id,
                gwas_file_path=file_path,
                ref_genome=ref_genome,
                population=population,
                batch_size=batch_size,
                max_workers=max_workers,
                maf_threshold=maf_threshold,
                seed=seed,
                window=window,
                L=L,
                coverage=coverage,
                min_abs_corr=min_abs_corr,
                sample_size=sample_size,
                file_metadata_id=file_metadata_id if _file_needs_processing else None,
                file_needs_processing=_file_needs_processing,
                file_storage_key=object_key if _file_needs_processing else None,
                file_id_new=file_id_new if _file_needs_processing else None,
                file_source_minio_path=_source_minio_path,
                file_source_download_url=_source_download_url,
                file_minio_cache_key=_minio_cache_key,
                file_gwas_library_id=_gwas_library_id,
            ),
        )

        return {
            "status": "started",
            "project_id": project_id,
            "file_id": file_metadata_id,
            "message": "Analysis pipeline started successfully",
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[API] Error starting analysis pipeline: {exc}")
        raise HTTPException(
            status_code=500,
            detail=f"Error starting analysis pipeline: {exc}",
        )


# ──────────────────────────────────────────────────────────────────────────────
# /phenotypes
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/phenotypes")
async def get_phenotypes(
    id: str | None = Query(None),
    search: str | None = Query(None),
    limit: int | None = Query(None),
    skip: int = Query(0),
):
    phenotypes = _deps["phenotypes"]
    try:
        if id:
            phenotype = phenotypes.get_phenotypes(phenotype_id=id)
            if not phenotype:
                raise HTTPException(status_code=404, detail="Phenotype not found")
            return serialize_datetime_fields({"phenotype": phenotype})

        if limit is None:
            limit = 100

        all_phenotypes = phenotypes.get_phenotypes(
            limit=limit, skip=skip, search_term=search
        )
        total_count = phenotypes.count_phenotypes(search_term=search)

        response: dict = {
            "phenotypes": all_phenotypes,
            "total_count": total_count,
            "skip": skip,
            "limit": limit,
            "has_more": (skip + len(all_phenotypes)) < total_count,
            "next_skip": (
                skip + len(all_phenotypes)
                if (skip + len(all_phenotypes)) < total_count
                else None
            ),
        }
        if search:
            response["search_term"] = search

        return serialize_datetime_fields(response)

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error getting phenotypes: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to get phenotypes: {exc}"
        )


@router.post("/phenotypes", status_code=201)
async def post_phenotypes(data: list = Body(...)):
    phenotypes = _deps["phenotypes"]
    try:
        if not isinstance(data, list):
            raise HTTPException(
                status_code=400, detail="Expected JSON array of phenotypes"
            )

        phenotypes_data = []
        for item in data:
            if not isinstance(item, dict):
                continue
            phenotype = {"id": item.get("id", ""), "phenotype_name": item.get("name", "")}
            if phenotype["id"] and phenotype["phenotype_name"]:
                phenotypes_data.append(phenotype)
            else:
                logger.warning(f"Skipping invalid phenotype entry: {item}")

        if not phenotypes_data:
            raise HTTPException(
                status_code=400, detail="No valid phenotypes found in JSON data"
            )

        result = phenotypes.bulk_create_phenotypes(phenotypes_data)
        return {
            "message": "Phenotypes loaded successfully",
            "inserted_count": result["inserted_count"],
            "skipped_count": result["skipped_count"],
            "total_provided": len(phenotypes_data),
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error loading phenotypes: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to load phenotypes: {exc}"
        )


# ──────────────────────────────────────────────────────────────────────────────
# /credible-sets
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/credible-sets")
async def get_credible_sets(
    project_id: str | None = Query(None),
    credible_set_id: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
):
    analysis = _deps["analysis"]

    if not project_id:
        raise HTTPException(status_code=400, detail="project_id is required")
    if not credible_set_id:
        raise HTTPException(status_code=400, detail="Credible_set_id is required")

    try:
        credible_set = analysis.get_credible_set_by_id(
            current_user_id, project_id, credible_set_id
        )
        if not credible_set:
            raise HTTPException(
                status_code=404, detail="No credible set found with this ID"
            )

        variants_data = credible_set.get("variants_data", {})
        if not variants_data:
            raise HTTPException(
                status_code=404,
                detail="No variants data found for this credible set",
            )

        variants = variants_data.get("data", {})
        variants_array = convert_variants_to_object_array(variants)
        variants_array = serialize_datetime_fields(variants_array)
        return {"variants": variants_array}

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error fetching credible set: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch credible set: {exc}"
        )


# ──────────────────────────────────────────────────────────────────────────────
# /gwas-files
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/gwas-files")
async def get_gwas_files(
    search: str | None = Query(None),
    sex: str | None = Query(None),
    limit: int | None = Query(None),
    skip: int = Query(0),
):
    gwas_library = _deps.get("gwas_library")
    try:
        if limit is None:
            limit = 100

        entries = gwas_library.get_all_gwas_entries(
            search_term=search, sex_filter=sex, limit=limit, skip=skip
        )

        gwas_files: list[dict] = []
        for entry in entries:
            file_id = entry.get("file_id") or entry.get("filename")
            desc = entry.get("description") or ""
            if isinstance(desc, str) and desc.startswith("#"):
                desc = desc.lstrip("#").strip()
            gwas_file_entry: dict = {
                "id": file_id,
                "phenotype": desc,
                "phenotype_code": entry.get("phenotype_code"),
                "filename": entry.get("filename"),
                "sex": entry.get("sex"),
                "source": entry.get("source"),
                "downloaded": entry.get("downloaded", False),
                "download_count": entry.get("download_count", 0),
                "url": f"/gwas-files/download/{file_id}",
                "showcase_link": entry.get("showcase_link", ""),
                "sample_size": entry.get("sample_size"),
                "genome_build": entry.get("genome_build"),
            }
            if entry.get("file_size"):
                gwas_file_entry["file_size_mb"] = round(
                    entry["file_size"] / (1024 * 1024), 2
                )
            gwas_files.append(gwas_file_entry)

        total_count = gwas_library.get_entry_count(search_term=search, sex_filter=sex)
        return {
            "gwas_files": gwas_files,
            "total_files": total_count,
            "returned": len(gwas_files),
            "skip": skip,
            "limit": limit,
        }

    except Exception as exc:
        logger.error(f"Error fetching GWAS files: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch GWAS files: {exc}"
        )


@router.get("/gwas-files/download/{file_id}")
async def download_gwas_file(file_id: str):
    gwas_library = _deps.get("gwas_library")
    storage = _deps.get("storage")

    try:
        entry = gwas_library.get_gwas_entry(file_id=file_id)
        if not entry:
            raise HTTPException(
                status_code=404, detail="GWAS file not found in library"
            )

        filename = entry.get("filename", file_id)
        minio_path = f"gwas_cache/{filename}"

        if storage and entry.get("downloaded") and entry.get("minio_path"):
            cached_path = entry["minio_path"]
            if storage.exists(cached_path):
                gwas_library.increment_download_count(file_id)
                download_url = storage.generate_presigned_url(cached_path, expiration=3600)
                if download_url:
                    return {"download_url": download_url, "cached": True}

                with tempfile.NamedTemporaryFile(
                    delete=False, suffix=f"_{filename}"
                ) as tmp:
                    if storage.download_file(cached_path, tmp.name):
                        return FileResponse(
                            tmp.name,
                            media_type="text/tab-separated-values",
                            filename=filename,
                        )
            else:
                gwas_library.update_gwas_entry(
                    file_id, {"downloaded": False, "minio_path": None}
                )

        # Determine download URL
        download_url = None
        if entry.get("aws_url"):
            download_url = entry["aws_url"]
        elif entry.get("wget_command"):
            m = re.search(r"(https?://[^\s]+)", entry["wget_command"])
            if m:
                download_url = m.group(1)
        elif entry.get("dropbox_url"):
            download_url = entry["dropbox_url"]

        if not download_url:
            raise HTTPException(
                status_code=404, detail="No download URL available for this file"
            )

        with tempfile.NamedTemporaryFile(
            delete=False, suffix=f"_{filename}"
        ) as tmp:
            temp_path = tmp.name
            try:
                loop = asyncio.get_running_loop()
                file_size = await loop.run_in_executor(
                    None, _download_to_path_sync, download_url, temp_path
                )

                if storage:
                    if storage.upload_file(temp_path, minio_path):
                        gwas_library.mark_as_downloaded(file_id, minio_path, file_size)
                        gwas_library.increment_download_count(file_id)

                return FileResponse(
                    temp_path,
                    media_type="text/tab-separated-values",
                    filename=filename,
                )
            except Exception as exc:
                logger.error(f"[GWAS DOWNLOAD] Download failed: {exc}")
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                raise HTTPException(
                    status_code=500, detail=f"Failed to download file: {exc}"
                )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[GWAS DOWNLOAD] Error: {exc}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Download failed: {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# /user-files
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/user-files")
async def get_user_files(current_user_id: str = Depends(get_current_user_id)):
    files = _deps["files"]
    try:
        all_files = files.get_file_metadata(current_user_id)
        if not all_files:
            return {"files": [], "total_files": 0}

        if not isinstance(all_files, list):
            all_files = [all_files]

        user_files: list[dict] = []
        for file_meta in all_files:
            if file_meta.get("source") not in (None, "user_upload"):
                continue
            user_files.append(
                {
                    "id": file_meta.get("_id"),
                    "display_name": file_meta.get(
                        "original_filename", file_meta.get("filename")
                    ),
                    "filename": file_meta.get("filename"),
                    "file_size": file_meta.get("file_size", 0),
                    "file_size_mb": round(
                        file_meta.get("file_size", 0) / (1024 * 1024), 2
                    ),
                    "record_count": file_meta.get("record_count"),
                    "upload_date": file_meta.get("upload_date"),
                    "source": "user_upload",
                }
            )

        user_files.sort(key=lambda x: x.get("upload_date", ""), reverse=True)
        user_files = serialize_datetime_fields(user_files)
        return {"files": user_files, "total_files": len(user_files)}

    except Exception as exc:
        logger.error(f"Error fetching user files: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch user files: {exc}"
        )

@router.get("/health")
async def health_check():
    return {"status": "healthy"}
