from datetime import datetime, timezone
import os
from loguru import logger
from src.services.status_tracker import status_tracker, TaskState
import requests as _requests
import pandas as pd
import numpy as np
import hashlib
import uuid
from dask.distributed import get_worker


def normalize_status_responses(raw: str | None) -> str:
    """Map stored status strings to Running, Failed, or Completed (API / sockets)."""
    if raw is None or not str(raw).strip():
        return "Running"
    key = str(raw).strip().lower().replace(" ", "_")
    if key in ("completed", "done"):
        return "Completed"
    if key in ("failed", "interrupted"):
        return "Failed"
    return "Running"


def analysis_state_for_public_api(state: dict | None) -> dict:
    if not state:
        return {"status": "Running", "message": "Waiting to start analysis."}

    out = {**state}
    raw = state.get("status")
    raw_key = str(raw or "").strip().lower().replace(" ", "_")
    out["status"] = normalize_status_responses(raw)

    msg = out.get("message")
    if msg is None or not str(msg).strip():
        if raw_key in ("uploading",):
            out["message"] = "Uploading or preparing GWAS file."
        elif raw_key in ("not_started", "notstarted"):
            out["message"] = "Waiting to start analysis."
        elif raw_key in ("completed", "done"):
            out["message"] = "Analysis completed successfully."
        elif raw_key in ("interrupted",):
            out["message"] = "Pipeline interrupted or cancelled."
        elif out["status"] == "Failed":
            out["message"] = "Analysis failed."
        else:
            out["message"] = "Waiting to start analysis."

    return out


def public_task_history_entries(entries: list | None) -> list:
    if not entries:
        return []
    out: list = []
    for e in entries:
        if not isinstance(e, dict):
            continue
        out.append({k: v for k, v in e.items() if k != "state"})
    return out


def emit_task_update(hypothesis_id, task_name, state, progress=0, details=None, next_task=None, error=None):
    """Emit a real-time progress update to WebSocket clients."""
    task_history = status_tracker.get_history(hypothesis_id)

    filtered_history = [entry for entry in task_history if entry["state"] == "completed"]
    latest_5_started_tasks = filtered_history[-5:]

    if progress == 0:
        progress = status_tracker.calculate_progress(task_history)

    status_tracker.add_update(hypothesis_id, progress, task_name, state, details, error)

    room = f"hypothesis_{hypothesis_id}"
    update = {
        "hypothesis_id": hypothesis_id,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec='milliseconds') + "Z",
        "task": task_name,
        "state": state.value,
        "progress": progress,
        "task_history": latest_5_started_tasks,
        "target_room": room,
    }

    if next_task:
        update["next_task"] = next_task
    if error:
        update["error"] = error
        update["status"] = "Failed"

    if state == TaskState.COMPLETED:
        if task_name == "Creating enrich data":
            update["status"] = "Completed"
            update["progress"] = 100
        elif task_name == "Verifying existence of enrichment data" and progress == 80:
            update["status"] = "Completed"
            update["progress"] = 100
        elif task_name == "Generating hypothesis" or (
            task_name == "Verifying existence of hypothesis data" and progress == 100
        ):
            update["status"] = "Completed"
            update["progress"] = 100
    elif state == TaskState.FAILED:
        update["status"] = "Failed"
        update["error"] = error

    if "status" not in update:
        if state == TaskState.FAILED:
            update["status"] = "Failed"
        elif state == TaskState.COMPLETED:
            update["status"] = "Running"
        else:
            update["status"] = "Running"

    public_update = {
        "hypothesis_id": hypothesis_id,
        "timestamp": update["timestamp"],
        "task": update["task"],
        "status": update["status"],
        "progress": update["progress"],
        "task_history": public_task_history_entries(update["task_history"]),
        "target_room": room,
    }
    if update.get("next_task"):
        public_update["next_task"] = update["next_task"]
    if update.get("error") is not None:
        public_update["error"] = update["error"]

    service_token = os.getenv("PREFECT_SERVICE_TOKEN")
    if not service_token:
        logger.error("PREFECT_SERVICE_TOKEN not set – task update will not be emitted.")
        return

    api_host = os.getenv("API_HOST") or os.getenv("FLASK_HOST", "localhost")
    api_port = os.getenv("API_PORT") or os.getenv("FLASK_PORT", "5000")
    url = f"http://{api_host}:{api_port}/internal/task-update"

    try:
        resp = _requests.post(
            url,
            json=public_update,
            headers={"Authorization": f"Bearer {service_token}"},
            timeout=5,
        )
        logger.info(f"Emitted task update [{resp.status_code}]: {task_name} – {state}")
    except Exception as exc:
        logger.error(f"Failed to POST task update to {url}: {exc}")
        logger.info(f"Status saved locally (no delivery): {task_name} – {state}")


def emit_analysis_update(user_id, project_id, state_data):
    """Push analysis pipeline progress to Socket.IO clients in room analysis_{project_id}."""
    service_token = os.getenv("PREFECT_SERVICE_TOKEN")
    if not service_token:
        logger.error("PREFECT_SERVICE_TOKEN not set – analysis update will not be emitted.")
        return

    api_host = os.getenv("API_HOST") or os.getenv("FLASK_HOST", "localhost")
    api_port = os.getenv("API_PORT") or os.getenv("FLASK_PORT", "5000")
    url = f"http://{api_host}:{api_port}/internal/task-update"

    room = f"analysis_{project_id}"
    public_state = analysis_state_for_public_api(state_data)
    payload = {
        "target_room": room,
        "event": "analysis_update",
        "project_id": project_id,
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds") + "Z",
        **public_state,
    }

    try:
        resp = _requests.post(
            url,
            json=payload,
            headers={"Authorization": f"Bearer {service_token}"},
            timeout=5,
        )
        logger.info(
            f"Emitted analysis update [{resp.status_code}]: project={project_id} "
            f"stage={state_data.get('stage')} status={state_data.get('status')}"
        )
    except Exception as exc:
        logger.error(f"Failed to POST analysis update to {url}: {exc}")


def save_analysis_state(user_id, state):
    """Save the analysis state for the second flow"""
    state_dir = os.path.join('data', 'states', user_id)
    os.makedirs(state_dir, exist_ok=True)
    
    with open(os.path.join(state_dir, 'analysis_state.json'), 'w') as f:
        json.dump(state, f, default=str)  # Use default=str to handle non-serializable objects

def allowed_file(filename):
    """Check if the file extension is allowed"""
    ALLOWED_EXTENSIONS = {'tsv', 'csv', 'txt', 'bgz', 'gz'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def compute_file_md5(file_path, chunk_size=8192):
    """
    Compute MD5 hash of a file
    """
    md5_hash = hashlib.md5()
    
    try:
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(chunk_size), b''):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()
    except Exception as e:
        logger.error(f"Error computing MD5 for {file_path}: {e}")
        return None

def get_shared_temp_dir(user_id=None, prefix=""):
    base = "/app/data/temp"
    
    if user_id:
        base = os.path.join(base, str(user_id))
    
    if prefix:
        dir_name = f"{prefix}_{uuid.uuid4().hex[:8]}"
    else:
        dir_name = uuid.uuid4().hex[:8]
    
    path = os.path.join(base, dir_name)
    os.makedirs(path, exist_ok=True)
    return path

def serialize_datetime_fields(data):
    """Convert datetime objects to ISO format strings for JSON serialization"""
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, dict):
                result[key] = serialize_datetime_fields(value)
            elif isinstance(value, list):
                result[key] = [serialize_datetime_fields(item) if isinstance(item, dict) else item for item in value]
            else:
                result[key] = value
        return result
    elif isinstance(data, list):
        return [serialize_datetime_fields(item) for item in data]
    else:
        return data

def transform_credible_sets_to_locuszoom(credible_sets_data):
    """Transform credible sets to LocusZoom format"""
    
    # Convert to DataFrame
    if isinstance(credible_sets_data, list):
        if len(credible_sets_data) > 0 and 'data' in credible_sets_data[0]:
            all_variants = []
            for cs_obj in credible_sets_data:
                all_variants.extend(cs_obj['data'])
            df = pd.DataFrame(all_variants)
        else:
            df = pd.DataFrame(credible_sets_data)
    else:
        df = credible_sets_data.copy() if hasattr(credible_sets_data, 'copy') else pd.DataFrame(credible_sets_data)
    
    if len(df) == 0:
        return {"data": {"beta": [], "chromosome": [], "log_pvalue": [], "position": [], 
                        "ref_allele": [], "ref_allele_freq": [], "variant": [], 
                        "posterior_prob": [], "is_member": [], "rs_id": []}, "lastPage": None}
    
    # Handle both uppercase (COJO format) and lowercase (harmonized format) column names
    beta_col = 'beta' if 'beta' in df.columns else 'BETA'
    chr_col = 'CHR' if 'CHR' in df.columns else 'chromosome'
    p_col = 'P' if 'P' in df.columns else 'p_value'
    bp_col = 'BP' if 'BP' in df.columns else 'base_pair_location'
    a1_col = 'A1' if 'A1' in df.columns else 'effect_allele'
    a2_col = 'A2' if 'A2' in df.columns else 'other_allele'
    frq_col = 'FRQ' if 'FRQ' in df.columns else 'effect_allele_frequency'
    rsid_col = 'RS_ID' if 'RS_ID' in df.columns else 'rsid'
    
    # Create LocusZoom format
    return {
        "data": {
            "beta": df[beta_col].astype(float).tolist(),
            "chromosome": df[chr_col].astype(int).tolist(), 
            "log_pvalue": (-np.log10(df[p_col].astype(float).clip(lower=1e-300))).tolist(),  # Clip to avoid log(0)
            "position": df[bp_col].astype(int).tolist(),
            "ref_allele": df[a2_col].astype(str).tolist(),
            "minor_allele": df[a1_col].astype(str).tolist(),
            "ref_allele_freq": df[frq_col].astype(float).tolist(),
            "variant": [f"{row[chr_col]}:{row[bp_col]}:{row[a2_col]}:{row[a1_col]}" for _, row in df.iterrows()],
            "posterior_prob": df['PIP'].astype(float).tolist(),
            "is_member": (df.get('cs', 0) != 0).tolist(),
            "rs_id": df[rsid_col].fillna('').astype(str).tolist() if rsid_col in df.columns else [''] * len(df)
        },
        "lastPage": None
    }


def convert_variants_to_object_array(variants_data):
    """
    Convert variants data from object-with-arrays format to array-of-objects format.
    """
    if not variants_data or not isinstance(variants_data, dict):
        return []
    
    # Get all field names
    field_names = list(variants_data.keys())
    if not field_names:
        return []
    
    # Get the length of arrays 
    first_field = field_names[0]
    if not isinstance(variants_data[first_field], list):
        return []
    
    array_length = len(variants_data[first_field])
    
    # Convert to array of objects
    result = []
    for i in range(array_length):
        variant_obj = {}
        for field_name in field_names:
            if isinstance(variants_data[field_name], list) and i < len(variants_data[field_name]):
                variant_obj[field_name] = variants_data[field_name][i]
            else:
                variant_obj[field_name] = None
        result.append(variant_obj)
    
    return result


def get_deps():    
    try:
        worker = get_worker()
    except ValueError as e:
        raise RuntimeError(f"Task not running on Dask worker: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to get Dask worker context: {e}")
    
    deps = getattr(worker, "deps", None)
    
    if not deps:
        err = getattr(worker, "deps_error", "unknown")
        raise RuntimeError(f"Worker dependencies not initialized: {err}")
    
    return deps