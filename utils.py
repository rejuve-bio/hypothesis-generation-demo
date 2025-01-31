from datetime import datetime, timezone

import eventlet
from socketio_instance import socketio
from status_tracker import status_tracker, TaskState
from prefect.context import get_run_context


def emit_task_update(hypothesis_id, task_name, state, progress=0, details=None, next_task=None, error=None):
    """
    Emits real-time updates via WebSocket - only for progress tracking
    """
    task_history = status_tracker.get_history(hypothesis_id)

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
        "task_history": task_history
    }

    # Add optional fields if they exist
    if details:
        update["details"] = details
    if next_task:
        update["next_task"] = next_task
    if error:
        update["error"] = error
        update["status"] = "failed"
    
    # Handle completion states
    if state == TaskState.COMPLETED:
        if task_name == "Creating enrich data" or task_name == "Enrichment process":
            update["status"] = "Enrichment_completed"
            update["progress"] = 50  # enrichment completion is 50%
        elif task_name == "Generating hypothesis":
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