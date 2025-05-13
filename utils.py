from datetime import datetime, timezone
import os

import eventlet
from flask import json
from socketio_instance import socketio
from status_tracker import status_tracker, TaskState
from prefect.context import get_run_context


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
        # Simple direct emit with namespace
        socketio.emit('task_update', update, room=room)
        print(f"Emitted task update to room {room}")
    except Exception as e:
        print(f"Error emitting task update: {e}")
        
    socketio.sleep(0)


def save_analysis_state(user_id, state):
    """Save the analysis state for the second flow"""
    state_dir = os.path.join('data', 'states', user_id)
    os.makedirs(state_dir, exist_ok=True)
    
    with open(os.path.join(state_dir, 'analysis_state.json'), 'w') as f:
        json.dump(state, f, default=str)  # Use default=str to handle non-serializable objects

def get_analysis_state(user_id):
    """Retrieve the analysis state for the second flow"""
    state_path = os.path.join('data', 'states', user_id, 'analysis_state.json')
    
    if not os.path.exists(state_path):
        raise FileNotFoundError(f"Analysis state not found for user {user_id}")
    
    with open(state_path, 'r') as f:
        return json.load(f)

def allowed_file(filename):
    """Check if the file extension is allowed"""
    ALLOWED_EXTENSIONS = {'tsv', 'csv', 'txt', 'bgz', 'gz'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_user_file_path(file_id, user_id):
    """Get the file path for the uploaded GWAS file without database"""
    # Check if metadata file exists
    metadata_path = os.path.join('data', 'metadata', user_id, f"{file_id}.json")
    
    if not os.path.exists(metadata_path):
        raise FileNotFoundError(f"File with ID {file_id} not found for user {user_id}")
    
    # Load metadata
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    
    # Verify the file exists on disk
    file_path = metadata['file_path']
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} does not exist on disk")
    
    return file_path