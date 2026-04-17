from __future__ import annotations

import json
from datetime import datetime, timezone

import jwt
from loguru import logger

from src.api.dependencies import _JWT_SECRET, _deps
from src.services.status_tracker import status_tracker
from src.socketio_instance import sio
from src.utils import serialize_datetime_fields


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
