import os
from threading import Thread, Timer
import uuid
from flask import json, request
from flask_restful import Resource
from flask_socketio import join_room, leave_room
from socketio_instance import socketio
from auth import socket_token_required, token_required
from datetime import datetime, timezone
from uuid import uuid4
from flows import hypothesis_flow, analysis_pipeline_flow
# finemapping_flow
from run_deployment import invoke_enrichment_deployment
from status_tracker import status_tracker, TaskState
from prefect import flow
from utils import allowed_file, transform_credible_sets_to_locuszoom
from loguru import logger
from werkzeug.utils import secure_filename
from utils import serialize_datetime_fields


class EnrichAPI(Resource):
    def __init__(self, enrichr, llm, prolog_query, enrichment, hypotheses, projects):
        self.enrichr = enrichr
        self.llm = llm
        self.prolog_query = prolog_query
        self.enrichment = enrichment
        self.hypotheses = hypotheses
        self.projects = projects

    @token_required
    def get(self, current_user_id):
        # Get the enrich_id from the query parameters
        enrich_id = request.args.get('id')
        project_id = request.args.get('project_id')
        
        if enrich_id:
                    # Fetch a specific enrich by enrich_id and user_id
            enrich = self.enrichment.get_enrich(current_user_id, enrich_id)
            if not enrich:
                return {"message": "Enrich not found or access denied."}, 404
            # Serialize datetime objects before returning
            enrich = serialize_datetime_fields(enrich)
            return enrich, 200
        
        if project_id:
            # Get all enrichments for a specific project
            enrichments = self.enrichment.get_enrich(user_id=current_user_id)
            if isinstance(enrichments, list):
                # Filter by project_id if it exists in the enrichment data
                project_enrichments = [e for e in enrichments if e.get('project_id') == project_id]
                project_enrichments = serialize_datetime_fields(project_enrichments)
                return {"enrichments": project_enrichments}, 200
            else:
                # Handle case where get_enrich returns a single item
                if enrichments and enrichments.get('project_id') == project_id:
                    enrichments = serialize_datetime_fields(enrichments)
                    return {"enrichments": [enrichments]}, 200
                return {"enrichments": []}, 200
          
        # Fetch all enrichments for the current user
        enrich = self.enrichment.get_enrich(user_id=current_user_id)
        # Serialize datetime objects before returning
        enrich = serialize_datetime_fields(enrich)
        return enrich, 200

    @token_required
    def post(self, current_user_id):
        args = request.args
        variant = args['variant']
        project_id = args.get('project_id')
        
        if not project_id:
            return {"error": "project_id is required"}, 400
        
        # Validate project exists and get phenotype from project
        project = self.projects.get_projects(current_user_id, project_id)
        if not project:
            return {"error": "Project not found or access denied"}, 404
        
        phenotype = project['phenotype']
        
        logger.info(f"Project-based enrichment request for project {project_id}")
        
        # Check for existing hypothesis in project context
        existing_hypothesis = self.hypotheses.get_hypothesis_by_phenotype_and_variant_in_project(
            current_user_id, project_id, phenotype, variant
        )
        
        if existing_hypothesis:
            
            logger.info(f"Re-running enrichment for existing hypothesis {existing_hypothesis['id']}")
            invoke_enrichment_deployment(
                current_user_id=current_user_id, 
                phenotype=phenotype, 
                variant=variant, 
                hypothesis_id=existing_hypothesis['id'],
                project_id=project_id
            )
            return {"hypothesis_id": existing_hypothesis['id'], "project_id": project_id}, 202
        
        # Generate hypothesis_id and create with project context (no credible_set_id in hypothesis)
        hypothesis_id = str(uuid4())
        hypothesis_data = {
            "id": hypothesis_id,
            "project_id": project_id,
            "phenotype": phenotype,
            "variant": variant,
            "status": "pending",
            "created_at": datetime.now(timezone.utc).isoformat(timespec='milliseconds') + "Z",
            "task_history": [],
        }
        
        self.hypotheses.create_hypothesis(current_user_id, hypothesis_data)
        
        invoke_enrichment_deployment(
            current_user_id=current_user_id, 
            phenotype=phenotype, 
            variant=variant, 
            hypothesis_id=hypothesis_id,
            project_id=project_id
        )
        
        return {"hypothesis_id": hypothesis_id}, 201
    
         
    @token_required
    def delete(self, current_user_id):
        enrich_id = request.args.get('id')
        if enrich_id:
            result = self.enrichment.delete_enrich(current_user_id, enrich_id)
            return result, 200
        return {"message": "enrich id is required!"}, 400


class HypothesisAPI(Resource):
    def __init__(self, enrichr, prolog_query, llm, hypotheses, enrichment):
        self.enrichr = enrichr
        self.prolog_query = prolog_query
        self.llm = llm
        self.hypotheses = hypotheses
        self.enrichment = enrichment

    @token_required
    def get(self, current_user_id):
        # Get the hypothesis_id from the query parameters
        hypothesis_id = request.args.get('id')

        if hypothesis_id:
            # Fetch a specific hypothesis by hypothesis_id and user_id
            hypothesis = self.hypotheses.get_hypotheses(current_user_id, hypothesis_id)
            if not hypothesis:
                return {"message": "Hypothesis not found or access denied."}, 404
            
            # Check if enrichment is complete
            required_fields = ['enrich_id', 'go_id', 'summary', 'graph']
            is_complete = all(field in hypothesis for field in required_fields)
            # Get task history
            task_history = status_tracker.get_history(hypothesis_id)
            for task in task_history:
                task.pop('details', None) 

            # Get only pending tasks from task history
            pending_tasks = [task for task in task_history if task.get('state') == TaskState.STARTED.value]
            last_pending_task = [pending_tasks[-1]] if pending_tasks else [] 
            logger.info(f"last_pending_task: {last_pending_task}")

            if is_complete:
                enrich_id = hypothesis.get('enrich_id')
                enrich_data = self.enrichment.get_enrich(current_user_id, enrich_id)
                # Remove 'causal_graph' field from enrich_data if it exists
                if isinstance(enrich_data, dict):
                    enrich_data.pop('causal_graph', None)
                response_data = {
                    'id': hypothesis_id,
                    'variant': hypothesis.get('variant') or hypothesis.get('variant_id'),
                    'enrich_id': enrich_id,
                    'phenotype': hypothesis['phenotype'],
                    "status": "completed",
                    "created_at": hypothesis.get('created_at'),
                    "result": enrich_data
                }
                # Serialize datetime objects before returning
                response_data = serialize_datetime_fields(response_data)
                return response_data, 200

            latest_state = status_tracker.get_latest_state(hypothesis_id)
            
            status_data = {
                'id': hypothesis_id,
                'variant': hypothesis.get('variant') or hypothesis.get('variant_id'),
                'phenotype': hypothesis['phenotype'],
                'status': 'pending',
                "created_at": hypothesis.get('created_at'),
                'task_history': last_pending_task,
            }
            if 'enrich_id' in hypothesis and hypothesis.get('enrich_id') is not None:
                enrich_id = hypothesis.get('enrich_id')
                status_data['enrich_id'] = enrich_id
                enrich_data = self.enrichment.get_enrich(current_user_id, enrich_id)
                if isinstance(enrich_data, dict):
                    enrich_data.pop('causal_graph', None)
                status_data['result'] = enrich_data
                

            # Check for failed state
            if latest_state and latest_state.get('state') == 'failed':
                status_data['status'] = 'failed'
                status_data['error'] = latest_state.get('error')

            # Serialize datetime objects before returning
            status_data = serialize_datetime_fields(status_data)
            return status_data, 200

        # Fetch all hypotheses for the current user
        hypotheses = self.hypotheses.get_hypotheses(user_id=current_user_id)
        
        # Filter and format the response for all hypotheses
        formatted_hypotheses = []
        for hypothesis in hypotheses:
            # Get only pending tasks from task history
            pending_tasks = [
                task for task in status_tracker.get_history(hypothesis['id']) 
                if task.get('state') == TaskState.STARTED.value
            ]
            last_pending_task = [pending_tasks[-1]] if pending_tasks else []
            
            formatted_hypothesis = {
                'id': hypothesis['id'],
                'phenotype': hypothesis['phenotype'],
                'variant': hypothesis.get('variant') or hypothesis.get('variant_id'),
                'created_at': hypothesis.get('created_at'),
                'status': hypothesis.get('status'),
                'task_history': last_pending_task
            }
            if 'enrich_id' in hypothesis and hypothesis.get('enrich_id') is not None:
                 formatted_hypothesis['enrich_id'] = hypothesis.get('enrich_id')
            if 'biological_context' in hypothesis and hypothesis.get('biological_context') is not None:
                formatted_hypothesis['biological_context'] = hypothesis.get('biological_context')
            if 'causal_gene' in hypothesis and hypothesis.get('causal_gene') is not None:
                formatted_hypothesis['causal_gene'] = hypothesis.get('causal_gene')
            formatted_hypotheses.append(formatted_hypothesis)
        
        # Serialize datetime objects before returning
        formatted_hypotheses = serialize_datetime_fields(formatted_hypotheses)
        return formatted_hypotheses, 200
        

    @token_required
    def post(self, current_user_id):
        enrich_id = request.args.get('id')
        go_id = request.args.get('go')

        # Get the hypothesis associated with this enrichment
        hypothesis = self.hypotheses.get_hypothesis_by_enrich(current_user_id, enrich_id)
        if not hypothesis:
            return {"message": "No hypothesis found for this enrichment"}, 404
        
        hypothesis_id = hypothesis['id']
        
        # Run the Prefect flow and return the result
        flow_result = hypothesis_flow(current_user_id, hypothesis_id, enrich_id, go_id, self.hypotheses, self.prolog_query, self.llm)

        return flow_result[0], flow_result[1]

    
    @token_required
    def delete(self, current_user_id):
        hypothesis_id = request.args.get('hypothesis_id')
        if hypothesis_id:
            return self.hypotheses.delete_hypothesis(current_user_id, hypothesis_id)
        return {"message": "Hypothesis ID is required"}, 400
        
    
class BulkHypothesisDeleteAPI(Resource):
    def __init__(self, hypotheses):
        self.hypotheses = hypotheses
        
    @token_required
    def post(self, current_user_id):
        data = request.get_json()
        
        if not data or 'hypothesis_ids' not in data:
            return {"message": "hypothesis_ids is required in request body"}, 400
            
        hypothesis_ids = data.get('hypothesis_ids')
        
        # Validate the list of IDs
        if not isinstance(hypothesis_ids, list):
            return {"message": "hypothesis_ids must be a list"}, 400
            
        if not hypothesis_ids:
            return {"message": "hypothesis_ids list cannot be empty"}, 400
            
        # Call the bulk delete method
        result, status_code = self.hypotheses.bulk_delete_hypotheses(current_user_id, hypothesis_ids)

        return result, status_code

class ChatAPI(Resource):
    def __init__(self, llm, hypotheses):
        self.llm = llm
        self.hypotheses = hypotheses

    @token_required
    def post(self, current_user_id):
        query = request.form.get('query')
        hypothesis_id = request.form.get('hypothesis_id')

        hypothesis = self.hypotheses.get_hypotheses(current_user_id, hypothesis_id)
        print(f"Hypothesis: {hypothesis}")
        
        if not hypothesis:
            return {"error": "Hypothesis not found or access denied"}, 404
        
        graph = hypothesis.get('graph')
        response = self.llm.chat(query, graph)
        response = {"response": response}
        return response

def init_socket_handlers(hypotheses_handler):
    logger.info("Initializing socket handlers...")
    
    # Store active timers for cleanup
    client_timers = {}
    
    @socketio.on('connect')
    def handle_connect(auth=None):

        try:
            logger.info("Client attempting to connect")
            client_id = request.sid

            logger.info(f"Client connected: {client_id}")
            
            # Set a reasonable timeout for all connections
            inactivity_timer = Timer(300, lambda: socketio.server.disconnect(client_id))
            inactivity_timer.start()
            
            # Store the timer for potential cleanup
            client_timers[client_id] = inactivity_timer
            
            return True
                
        except Exception as e:
            logger.error(f"Error in handle_connect: {str(e)}")
            return False

    @socketio.on('disconnect')
    def handle_disconnect():
        try:
            client_id = request.sid
            logger.info(f"Client disconnected: {client_id}")
            
            # Clean up any rooms the client was in
            try:
                # Get all rooms for this client
                rooms = socketio.server.rooms(client_id)
                if rooms:
                    logger.info(f"Cleaning up rooms for client {client_id}: {list(rooms)}")
                    # Leave all rooms
                    for room in rooms:
                        if room != client_id:  
                            leave_room(room)
                            logger.info(f"Client {client_id} left room: {room}")
            except Exception as room_e:
                logger.warning(f"Could not clean up rooms for {client_id}: {room_e}")
            
            # Cancel any pending timers for this client
            if client_id in client_timers:
                try:
                    client_timers[client_id].cancel()
                    del client_timers[client_id]
                    logger.info(f"Cancelled timeout timer for client: {client_id}")
                except Exception as timer_e:
                    logger.warning(f"Could not cancel timer for {client_id}: {timer_e}")
            
        except Exception as e:
            logger.error(f"Error in handle_disconnect: {str(e)}")

    @socketio.on('task_update')
    @socket_token_required  
    def handle_task_update(data, current_user_id=None):
        """
        Handle task updates from Prefect clients and broadcast to appropriate rooms
        """
        try:
            logger.info(f"Received task update from client: {data}")
            
            target_room = data.get('target_room')
            if not target_room:
                logger.error("No target_room specified in task update")
                return
            
            # Remove the target_room from data before broadcasting
            broadcast_data = {k: v for k, v in data.items() if k != 'target_room'}
            
            # Broadcast to the specific room
            socketio.emit('task_update', broadcast_data, room=target_room)
            logger.info(f"Broadcasted task update to room: {target_room}")
            
        except Exception as e:
            logger.error(f"Error handling task update: {str(e)}")

    @socketio.on('subscribe_hypothesis')
    @socket_token_required
    def handle_subscribe(data, current_user_id=None):
        try:
            logger.info(f"Received subscribe request: {data}")
            # Handle both string and dict input
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON string: {data}")
                    return {'error': 'Invalid JSON format'}, 400
            
            # Validate input
            if not isinstance(data, dict) or 'hypothesis_id' not in data:
                logger.error(f"Invalid data format: {data}")
                return {'error': 'Expected format: {"hypothesis_id": "value"}'}, 400
                
            hypothesis_id = data.get('hypothesis_id')
            if not hypothesis_id:
                logger.error("Missing hypothesis_id")
                return {'error': 'hypothesis_id is required'}, 400
            
            response_data = {
                'hypothesis_id': hypothesis_id,
                'timestamp': datetime.now(timezone.utc).isoformat(timespec='milliseconds') + "Z",
            }
            # Join a room specific to this hypothesis
            room = f"hypothesis_{hypothesis_id}"    
            join_room(room)
            logger.info(f"Joined room: {room}")

            # Verify room membership
            try:
                rooms = socketio.server.rooms(request.sid)
                logger.info(f"Current rooms for client {request.sid}: {rooms}")
            except Exception as room_e:
                logger.warning(f"Could not verify room membership: {room_e}")
            
           
            if current_user_id is None:
                hypothesis = db_instance.get_hypothesis_by_id(hypothesis_id)
            else:
                hypothesis = db_instance.get_hypotheses(current_user_id, hypothesis_id)
                
            if not hypothesis:
                logger.error(f"Hypothesis not found: {hypothesis_id}")
                raise ValueError("Hypothesis not found or access denied")

            # Get task history
            task_history = status_tracker.get_history(hypothesis_id)
            response_data['task_history'] = task_history
                
            # Check if hypothesis is complete
            required_fields = ['enrich_id', 'go_id', 'summary', 'graph']
            is_complete = all(field in hypothesis for field in required_fields)  
            
            if is_complete:
                response_data.update({
                    'status': 'completed',
                    'result': hypothesis,
                    'progress': 100
                })
            else:
                latest_state = status_tracker.get_latest_state(hypothesis_id)
                progress = status_tracker.calculate_progress(task_history)
                current_task = latest_state['task'] if latest_state else None
                error = latest_state.get('error') if latest_state and latest_state.get('state') == TaskState.FAILED else None
                
                response_data.update({
                    'status': 'pending',
                    'progress': progress
                })
            if current_task:
                response_data['current_task'] = current_task
            if error:
                response_data['error'] = error
            
            
            logger.info(f"Emitting task_update: {response_data}")
            socketio.emit('task_update', response_data, room=room)
            return {"status": "subscribed", "room": room}
            
        except Exception as e:
            logger.error(f"Error in handle_subscribe: {str(e)}")
            return {"error": str(e)}, 500

class ProjectsAPI(Resource):
    """
    API endpoint for managing projects
    """
    def __init__(self, projects, analysis, hypotheses):
        self.projects = projects
        self.analysis = analysis
        self.hypotheses = hypotheses
    
    @token_required
    def get(self, current_user_id):
        """Get all projects for a user or comprehensive data for a specific project"""
        project_id = request.args.get('id')
        
        if project_id:
            return self._get_project_with_full_data(current_user_id, project_id)
        
        projects = self.projects.get_projects(current_user_id)
        # Serialize datetime objects in all projects
        projects = serialize_datetime_fields(projects)
        return {"projects": projects}, 200
    
    def _get_project_with_full_data(self, current_user_id, project_id):
        """Get comprehensive project data including state, hypotheses, and credible sets"""
        try:
            # Get basic project info
            project = self.projects.get_projects(current_user_id, project_id)
            if not project:
                return {"error": "Project not found"}, 404
            
            # Get analysis state (may be None for new projects)
            analysis_state = self.projects.load_analysis_state(current_user_id, project_id)
            if not analysis_state:
                analysis_state = {"status": "not_started"}
            
            # Get credible sets (may be empty during processing)
            credible_sets_data = []
            try:
                credible_sets_raw = self.analysis.get_lead_variant_credible_sets(current_user_id, project_id)
                if credible_sets_raw:
                    if isinstance(credible_sets_raw, list):
                        credible_sets_data = [
                            {
                                "lead_variant_id": cs["lead_variant_id"],
                                "credible_sets": cs["data"].get("credible_sets", []),
                                "metadata": cs["data"].get("metadata", {})
                            }
                            for cs in credible_sets_raw
                        ]
                    else:
                        # Single result
                        credible_sets_data = [{
                            "lead_variant_id": credible_sets_raw["lead_variant_id"],
                            "credible_sets": credible_sets_raw["data"].get("credible_sets", []),
                            "metadata": credible_sets_raw["data"].get("metadata", {})
                        }]
            except Exception as cs_e:
                logger.warning(f"Could not load credible sets for project {project_id}: {cs_e}")
                credible_sets_data = []
            
            # Get hypotheses for this project (id + variant only)
            project_hypotheses = []
            try:
                all_hypotheses = self.hypotheses.get_hypotheses(current_user_id)
                if isinstance(all_hypotheses, list):
                    project_hypotheses = [
                        {
                            "id": h["id"], 
                            "variant": h.get("variant") or h.get("variant_id")
                        }
                        for h in all_hypotheses 
                        if h.get('project_id') == project_id
                    ]
            except Exception as hyp_e:
                logger.warning(f"Could not load hypotheses for project {project_id}: {hyp_e}")
                project_hypotheses = []
            
            # Build comprehensive response
            response = {
                "id": project["id"],
                "name": project["name"],
                "phenotype": project.get("phenotype", ""),
                "gwas_file_id": project["gwas_file_id"],
                "created_at": project.get("created_at"),
                "state": analysis_state,
                "hypotheses": project_hypotheses,
                "credible_sets": credible_sets_data
            }
            
            # Serialize datetime objects before returning
            response = serialize_datetime_fields(response)
            return response, 200
            
        except Exception as e:
            logger.error(f"Error getting comprehensive project data for {project_id}: {str(e)}")
            return {"error": f"Error retrieving project data: {str(e)}"}, 500
    
    @token_required
    def post(self, current_user_id):
        """Create a new project"""
        data = request.get_json()
        
        if not data or 'name' not in data or 'gwas_file_id' not in data or 'phenotype' not in data:
            return {"error": "Missing required fields: name, gwas_file_id, phenotype"}, 400
        
        try:
            project_id = self.projects.create_project(
                current_user_id, 
                data['name'], 
                data['gwas_file_id'],
                data['phenotype']
            )
            
            project = self.projects.get_projects(current_user_id, project_id)
            # Serialize datetime objects before returning
            project = serialize_datetime_fields(project)
            return {"project": project}, 201
        except Exception as e:
            return {"error": f"Failed to create project: {str(e)}"}, 500
    
    @token_required
    def delete(self, current_user_id):
        """Delete a project"""
        project_id = request.args.get('id')
        if not project_id:
            return {"error": "Project ID is required"}, 400
        
        success = self.projects.delete_project(current_user_id, project_id)
        if success:
            return {"message": "Project deleted successfully"}, 200
        return {"error": "Project not found or access denied"}, 404



class AnalysisPipelineAPI(Resource):
    def __init__(self, projects, files, analysis, config):
        self.projects = projects
        self.files = files
        self.analysis = analysis
        self.config = config

    @token_required
    def post(self, current_user_id):
        try:
            # Get form data and file
            project_name = request.form.get('project_name')
            phenotype = request.form.get('phenotype')
            ref_genome = request.form.get('ref_genome', 'GRCh37')
            population = request.form.get('population', 'EUR')
            max_workers = int(request.form.get('max_workers', 3))
            
            # Fine-mapping parameters with defaults
            maf_threshold = float(request.form.get('maf_threshold', 0.01))
            seed = int(request.form.get('seed', 42))
            window = int(request.form.get('window', 2000))
            L = int(request.form.get('L', -1))
            coverage = float(request.form.get('coverage', 0.95))
            min_abs_corr = float(request.form.get('min_abs_corr', 0.5))
            batch_size = int(request.form.get('batch_size', 5))
            
            # Validate required fields
            if not project_name:
                return {"error": "project_name is required"}, 400
            
            if not phenotype:
                return {"error": "phenotype is required"}, 400
            
            # Get uploaded file
            if 'gwas_file' not in request.files:
                return {"error": "No GWAS file uploaded"}, 400
            
            gwas_file = request.files['gwas_file']
            if gwas_file.filename == '':
                return {"error": "No file selected"}, 400
            
            logger.info(f"[API] Starting analysis pipeline")
            logger.info(f"[API] Project: {project_name}")
            logger.info(f"[API] File: {gwas_file.filename}")
            logger.info(f"[API] Reference: {ref_genome}, Population: {population}")
            logger.info(f"[API] Fine-mapping params: maf={maf_threshold}, seed={seed}, window={window}kb, L={L}, coverage={coverage}, min_abs_corr={min_abs_corr}")
            
            # Validate file
            if not allowed_file(gwas_file.filename):
                return {"error": "Invalid file format. Supported: .tsv, .txt, .csv, .gz, .bgz"}, 400
            
            # Validate parameters
            if ref_genome not in ["GRCh37", "GRCh38"]:
                return {"error": "Reference genome must be GRCh37 or GRCh38"}, 400
            
            if population not in ["EUR", "AFR", "AMR", "EAS", "SAS"]:
                return {"error": "Population must be one of: EUR, AFR, AMR, EAS, SAS"}, 400
            
            if max_workers < 1 or max_workers > 16:
                return {"error": "Max workers must be between 1-16"}, 400
            
            # Validate fine-mapping parameters
            if maf_threshold < 0.001 or maf_threshold > 0.5:
                return {"error": "MAF threshold must be between 0.001-0.5"}, 400
            
            if seed < 1 or seed > 999999:
                return {"error": "Seed must be between 1-999999"}, 400
            
            if window > 10000:
                return {"error": "Fine-mapping window shouldn't be greater than 10000 kb"}, 400
            
            if L != -1 and (L < 1 or L > 50):
                return {"error": "L must be -1 (auto) or between 1-50"}, 400
            
            if coverage < 0.5 or coverage > 0.999:
                return {"error": "Coverage must be between 0.5-0.999"}, 400
            
            if min_abs_corr < 0.5 or min_abs_corr > 1.0:
                return {"error": "Min absolute correlation must be between 0.5-1.0"}, 400
            
            if batch_size < 1 or batch_size > 20:
                return {"error": "Batch size must be between 1-20"}, 400
            
            # === FILE UPLOAD AND PROJECT CREATION  ===
            # Generate secure filename and file ID
            filename = secure_filename(gwas_file.filename)
            file_id = str(uuid.uuid4())
            
            # Create user upload directory
            user_upload_dir = os.path.join('data', 'uploads', current_user_id)
            os.makedirs(user_upload_dir, exist_ok=True)
            
            # File path for saving
            file_path = os.path.join(user_upload_dir, f"{file_id}_{filename}")
            
            logger.info(f"Starting upload for file {filename} (ID: {file_id})")
            start_time = datetime.now()
            
            # Save file
            gwas_file.save(file_path)
            file_size = os.path.getsize(file_path)
            
            # Create file metadata in database
            file_metadata_id = self.files.create_file_metadata(
                user_id=current_user_id,
                filename=filename,
                original_filename=gwas_file.filename,
                file_path=file_path,
                file_type='gwas',
                file_size=file_size
            )
            
            # Create project automatically
            project_id = self.projects.create_project(
                user_id=current_user_id,
                name=project_name,
                gwas_file_id=file_metadata_id,
                phenotype=phenotype
            )
            
            # Save metadata to file system
            metadata_dir = os.path.join('data', 'metadata', current_user_id)
            os.makedirs(metadata_dir, exist_ok=True)
            
            metadata = {
                'file_id': file_metadata_id,
                'user_id': current_user_id,
                'filename': filename,
                'original_filename': gwas_file.filename,
                'file_path': file_path,
                'file_type': 'gwas',
                'upload_date': str(datetime.now()),
                'file_size': file_size,
                'project_id': project_id
            }
            
            with open(os.path.join(metadata_dir, f"{file_metadata_id}.json"), 'w') as f:
                json.dump(metadata, f)
            
            total_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Completed: {filename} in {total_time:.1f} seconds")
            
            logger.info(f"[API] Created project {project_id} with file {file_metadata_id}")
            
            # Start pipeline in background thread
            def run_pipeline_background():
                try:
                    
                    
                    logger.info(f"[API] Running analysis pipeline for project {project_id}")
                    
                    # Run the analysis pipeline flow directly
                    credible_sets = analysis_pipeline_flow(
                        projects_handler=self.projects,
                        analysis_handler=self.analysis,
                        mongodb_uri=self.config.mongodb_uri,
                        db_name=self.config.db_name,
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
                        min_abs_corr=min_abs_corr
                    )
                    
                    logger.info(f"[API] Analysis pipeline for project {project_id} completed successfully")
                    if credible_sets and isinstance(credible_sets, dict):
                        logger.info(f"[API] Generated {credible_sets.get('total_variants', 0)} variants in {credible_sets.get('total_credible_sets', 0)} credible sets")
                    else:
                        logger.info(f"[API] Analysis completed but no credible sets generated")
                    
                except Exception as e:
                    logger.error(f"[API] Analysis pipeline for project {project_id} failed: {str(e)}")
            
            # Start background thread
            background_thread = Thread(target=run_pipeline_background)
            background_thread.start()
            
            logger.info(f"[API] Analysis pipeline started for project {project_id}")
            
            return {
                "status": "started",
                "project_id": project_id,
                "file_id": file_metadata_id,
                "message": "Analysis pipeline started successfully",
            }, 202
            
        except Exception as e:
            logger.error(f"[API] Error starting analysis pipeline: {str(e)}")
            return {"error": f"Error starting analysis pipeline: {str(e)}"}, 500




