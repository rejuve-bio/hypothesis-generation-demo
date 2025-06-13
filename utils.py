from datetime import datetime, timezone
from socketio_instance import socketio
from status_tracker import status_tracker, TaskState
import socketio as sio
import os


def emit_task_update(hypothesis_id, task_name, state, progress=0, details=None, next_task=None, error=None):
    """
    Emits real-time updates via WebSocket - only for progress tracking
    """
    task_history = status_tracker.get_history(hypothesis_id)

    # Filter to only include 'started' state entries and keep the latest 5
    filtered_history = [entry for entry in task_history if entry["state"] == "completed"]
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
    
    # Handle completion states
    if state == TaskState.COMPLETED:
        if task_name == "Creating enrich data": #or task_name == "Enrichment process":
            update["status"] = "Enrichment_completed"
            update["progress"] = 80  # enrichment completion is 50%
        elif task_name == "Generating hypothesis":
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
