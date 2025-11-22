from datetime import datetime, timezone
import os
from flask import json
from loguru import logger
from socketio_instance import socketio
from status_tracker import status_tracker, TaskState
import socketio as sio
import os
import pandas as pd
import numpy as np
import threading
import time

# Global persistent client for Prefect connections
_prefect_client = None
_client_lock = threading.Lock()
_last_connection_time = 0
_connection_timeout = 300  # 5 minutes

def _get_or_create_prefect_client():
    """Get or create a persistent SocketIO client for Prefect updates"""
    global _prefect_client, _last_connection_time
    
    with _client_lock:
        current_time = time.time()
        
        # Check if we need to create a new client or reconnect
        if (_prefect_client is None or 
            not _prefect_client.connected or 
            (current_time - _last_connection_time) > _connection_timeout):
            
            # Clean up old client
            if _prefect_client:
                try:
                    _prefect_client.disconnect()
                except:
                    pass
            
            # Create new client
            flask_host = os.getenv('FLASK_HOST', 'flask-app')
            flask_port = os.getenv('FLASK_PORT', '5000')
            flask_url = f'http://{flask_host}:{flask_port}'
            
            service_token = os.getenv('PREFECT_SERVICE_TOKEN')
            if not service_token:
                logger.error("Warning: PREFECT_SERVICE_TOKEN not found. Task update will not be emitted.")
                return None
            
            try:
                _prefect_client = sio.SimpleClient()
                headers = {'Authorization': f'Bearer {service_token}'}
                _prefect_client.connect(
                    flask_url, 
                    headers=headers, 
                    transports=['websocket', 'polling'],
                    wait_timeout=10
                )
                _last_connection_time = current_time
                logger.info(f"Established persistent SocketIO connection to Flask server at {flask_url}")
                
            except Exception as e:
                logger.error(f"Failed to create persistent SocketIO connection: {e}")
                _prefect_client = None
                return None
        
        return _prefect_client

def emit_task_update(hypothesis_id, task_name, state, progress=0, details=None, next_task=None, error=None):
    """
    Emits real-time updates via WebSocket - only for progress tracking
    """
    task_history = status_tracker.get_history(hypothesis_id)

    # Filter to only include 'started' state entries and keep the latest 5
    filtered_history = [entry for entry in task_history if entry["state"] == "completed"]
    # latest_5_started_tasks = sorted(filtered_history, key=lambda x: x["timestamp"], reverse=True)[:5]
    latest_5_started_tasks = filtered_history[-5:]

    if progress == 0:
        progress = status_tracker.calculate_progress(task_history)

    # Update the status tracker first
    status_tracker.add_update(hypothesis_id, progress, task_name, state, details, error)

    room = f"hypothesis_{hypothesis_id}"
    update = {
        "hypothesis_id": hypothesis_id,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec='milliseconds') + "Z",
        "task": task_name,
        "state": state.value,
        "progress": progress,
        "task_history": latest_5_started_tasks
    }

    if next_task:
        update["next_task"] = next_task
    if error:
        update["error"] = error
        update["status"] = "failed"
    
    # Handle completion status
    if state == TaskState.COMPLETED:
        if task_name == "Creating enrich data" or (task_name =="Verifying existence of enrichment data" and progress == 80):
            update["status"] = "Enrichment_completed"
            update["progress"] = 80  # enrichment completion is 80%
        elif task_name == "Generating hypothesis" or (task_name == "Verifying existence of hypothesis data" and progress == 100):
            update["status"] = "Hypothesis_completed"
            update["progress"] = 100  # hypothesis completion is 100%
    elif state == TaskState.FAILED:
        update["status"] = "failed"
        update["error"] = error

    try:
        # Check if we're in Flask context (socketio server available)
        if socketio and hasattr(socketio, 'server') and socketio.server:
            socketio.emit('task_update', update, room=room)
            logger.info(f"Emitted task update to room {room}")
            socketio.sleep(0)
        else:
            # We're in Prefect context
            client = _get_or_create_prefect_client()
            if not client:
                logger.info(f"Status update saved locally (no connection): {update['task']} - {update['state']}")
                return
            
            try:
                # Include room information in the update data since client.emit() doesn't support room parameter
                update_with_room = {**update, 'target_room': room}
                client.emit('task_update', update_with_room)
                logger.info(f"Emitted task update via persistent connection: {update['task']} - {update['state']}")
                
            except Exception as client_e:
                logger.error(f"Failed to emit via persistent connection: {client_e}")
                # Reset client on error
                with _client_lock:
                    if _prefect_client:
                        try:
                            _prefect_client.disconnect()
                        except:
                            pass
                        _prefect_client = None
                # Fall back to just updating status without emission
                logger.info(f"Status update saved locally: {update['task']} - {update['state']}")
                
    except Exception as e:
        logger.error(f"Error emitting task update: {e}")



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

def get_user_file_path(files_handler, file_id, user_id):
    """Get file path from file ID using database metadata"""
    file_metadata = files_handler.get_file_metadata(user_id, file_id)
    if not file_metadata:
        raise FileNotFoundError(f"File with ID {file_id} not found for user {user_id}")
    
    return file_metadata['file_path']

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
    
    # Create LocusZoom format
    return {
        "data": {
            "beta": df['BETA'].astype(float).tolist(),
            "chromosome": df['CHR'].astype(int).tolist(), 
            "log_pvalue": (-np.log10(df['P'].astype(float).clip(lower=1e-300))).tolist(),  # Clip to avoid log(0)
            "position": df['BP'].astype(int).tolist(),
            "ref_allele": df['A2'].astype(str).tolist(),
            "minor_allele": df['A1'].astype(str).tolist(),
            "ref_allele_freq": df['FRQ'].astype(float).tolist(),
            "variant": [f"{row['CHR']}:{row['BP']}:{row['A2']}:{row['A1']}" for _, row in df.iterrows()],
            "posterior_prob": df['PIP'].astype(float).tolist(),
            "is_member": (df.get('cs', 0) != 0).tolist(),
            "rs_id": df['RS_ID'].fillna('').astype(str).tolist() if 'RS_ID' in df.columns else [''] * len(df)
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
