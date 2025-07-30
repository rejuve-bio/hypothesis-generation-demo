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
            # We're in Prefect context - use SocketIO client with service token
            flask_host = os.getenv('FLASK_HOST', 'flask-app')
            flask_port = os.getenv('FLASK_PORT', '5000')
            flask_url = f'http://{flask_host}:{flask_port}'
            
            # Get the system service token
            service_token = os.getenv('PREFECT_SERVICE_TOKEN')
            if not service_token:
                logger.error("Warning: PREFECT_SERVICE_TOKEN not found. Task update will not be emitted.")
                logger.info(f"Status update saved locally: {update['task']} - {update['state']}")
                return
            
            # Create a client connection to Flask app with service token
            client = sio.SimpleClient()
            try:
                # Connect with service token in headers
                headers = {'Authorization': f'Bearer {service_token}'}
                client.connect(
                    flask_url, 
                    headers=headers, 
                    transports=['websocket', 'polling'],
                    retry=True,  
                    retry_delay=3,  
                    retry_delay_max=5,  
                    retry_randomization=0.5,  
                    wait_timeout=10  
                )
                
                # Include room information in the update data since client.emit() doesn't support room parameter
                update_with_room = {**update, 'target_room': room}
                client.emit('task_update', update_with_room)
                logger.info(f"Emitted task update to Flask server at {flask_url}")
                client.disconnect()
            except Exception as client_e:
                logger.error(f"Failed to connect to Flask SocketIO server: {client_e}")
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

def get_user_file_path(db, file_id, user_id):
    """Get file path from file ID using database metadata"""
    file_metadata = db.get_file_metadata(user_id, file_id)
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
            "log_pvalue": (-np.log10(df['P'].astype(float))).tolist(),
            "position": df['BP'].astype(int).tolist(),
            "ref_allele": df['A2'].astype(str).tolist(),
            "alt_allele": df['A1'].astype(str).tolist(),
            "ref_allele_freq": df['FRQ'].astype(float).tolist(),
            "variant": [f"{row['CHR']}:{row['BP']}:{row['A2']}:{row['A1']}" for _, row in df.iterrows()],
            "posterior_prob": df['PIP'].astype(float).tolist() if 'PIP' in df.columns else [0.0] * len(df),
            "is_member": (df.get('cs', 0) != 0).tolist(),
            "rs_id": df['RS_ID'].fillna('').astype(str).tolist() if 'RS_ID' in df.columns else [''] * len(df)
        },
        "lastPage": None
    }
