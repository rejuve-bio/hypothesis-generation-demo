import os
from threading import Thread, Timer
import uuid
from flask import json, request, send_file
from flask_restful import Resource
from flask_socketio import join_room, leave_room
from socketio_instance import socketio
from auth import socket_token_required, token_required
from datetime import datetime, timezone
from uuid import uuid4
# finemapping_flow
from run_deployment import invoke_enrichment_deployment, invoke_analysis_pipeline_deployment, invoke_hypothesis_deployment
from status_tracker import status_tracker, TaskState
from prefect import flow
from utils import allowed_file, convert_variants_to_object_array, serialize_datetime_fields, compute_file_md5
from loguru import logger
from werkzeug.utils import secure_filename
from tasks import extract_probability, get_related_hypotheses
from project_tasks import count_gwas_records, get_project_with_full_data, extract_gwas_file_metadata
import glob


class EnrichAPI(Resource):
    def __init__(self, enrichr, llm, prolog_query, enrichment, hypotheses, projects, gene_expression=None):
        self.enrichr = enrichr
        self.llm = llm
        self.prolog_query = prolog_query
        self.enrichment = enrichment
        self.hypotheses = hypotheses
        self.projects = projects
        self.gene_expression = gene_expression

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
        json_data = request.get_json(silent=True) or {}
        
        variant =  request.args.get('variant') or json_data.get('variant')
        project_id = request.args.get('project_id') or json_data.get('project_id')
        seed = int(json_data.get('seed', 42))

        
        if not project_id:
            return {"error": "project_id is required"}, 400
        if not variant:
            return {"error": "variant is required"}, 400
        
        # Validate project exists and get phenotype from project
        project = self.projects.get_projects(current_user_id, project_id)
        if not project:
            return {"error": "Project not found or access denied"}, 404
        
        phenotype = project['phenotype']

        tissue_name = (request.args.get('tissue_name') or (json_data.get('tissue_name') if isinstance(json_data, dict) else None))
        if not tissue_name:
            return {"error": "tissue_name is required"}, 400

        # Validate tissue exists for the project and save selection 
        try:
            available_tissues = self.gene_expression.get_ldsc_results_for_project(current_user_id, project_id, limit=20, format='selection')
            tissue_names = [t.get('tissue_name') for t in (available_tissues or [])]
            if tissue_name not in tissue_names:
                return {"error": f"Invalid tissue selection. Available tissues: {tissue_names}"}, 400
            # Persist selection 
            self.gene_expression.save_tissue_selection(
                current_user_id, project_id, variant, tissue_name
            )
            logger.info(f"Saved tissue selection in /enrich: {tissue_name} for variant {variant} in project {project_id}")
        except Exception as e:
            logger.warning(f"Failed to save/validate tissue selection: {e}")
        
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
                project_id=project_id,
                seed=seed
            )
            return {"hypothesis_id": existing_hypothesis['id'], "project_id": project_id}, 202
        
        # Generate hypothesis_id and create with project context 
        hypothesis_id = str(uuid4())
        hypothesis_data = {
            "id": hypothesis_id,
            "project_id": project_id,
            "phenotype": phenotype,
            "variant": variant,
            "variant_rsid": variant,  
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
            project_id=project_id,
            seed=seed
        )
        
        return {"hypothesis_id": hypothesis_id, "project_id": project_id}, 202
    
         
    @token_required
    def delete(self, current_user_id):
        enrich_id = request.args.get('id')
        if enrich_id:
            result = self.enrichment.delete_enrich(current_user_id, enrich_id)
            return result, 200
        return {"message": "enrich id is required!"}, 400


class HypothesisAPI(Resource):
    def __init__(self, enrichr, prolog_query, llm, hypotheses, enrichment, gene_expression):
        self.enrichr = enrichr
        self.prolog_query = prolog_query
        self.llm = llm
        self.hypotheses = hypotheses
        self.enrichment = enrichment
        self.gene_expression = gene_expression

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

            # Extract confidence and related hypotheses once
            confidence = extract_probability(hypothesis, self.enrichment, current_user_id)
            related_hypotheses = get_related_hypotheses(hypothesis, self.hypotheses, self.enrichment, current_user_id)
            
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
                    "probability": confidence,
                    "hypotheses": related_hypotheses,
                    "result": enrich_data,
                    "summary": hypothesis.get('summary'),  
                    "graph": hypothesis.get('graph') 
                }

                if 'tissue_rankings' in hypothesis:
                    response_data['tissue_rankings'] = hypothesis['tissue_rankings']
                    response_data['enrichment_type'] = hypothesis.get('enrichment_type', 'tissue_enhanced')
                else:
                    response_data['enrichment_type'] = 'standard'

                # Get tissue selection from tissue_selections collection
                selected_tissue = None
                if self.gene_expression:
                    try:
                        variant_id = hypothesis.get('variant_rsid') or hypothesis.get('variant') or hypothesis.get('variant_id')
                        project_id = hypothesis.get('project_id')
                        if variant_id and project_id:
                            tissue_selection = self.gene_expression.get_tissue_selection(
                                current_user_id, project_id, variant_id
                            )
                            if tissue_selection:
                                selected_tissue = tissue_selection.get('tissue_name')
                                logger.info(f"Retrieved tissue selection for completed hypothesis {hypothesis_id} using variant_id={variant_id}: {selected_tissue}")
                            else:
                                logger.info(f"No tissue selection found for completed hypothesis {hypothesis_id}, variant_id={variant_id}")
                    except Exception as ts_e:
                        logger.warning(f"Could not get tissue selection for completed hypothesis {hypothesis_id}: {ts_e}")
                
                response_data['tissue_selected'] = selected_tissue

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
                "probability": confidence,
                "hypotheses": related_hypotheses,
            }

            if 'tissue_rankings' in hypothesis:
                status_data['tissue_rankings'] = hypothesis['tissue_rankings']
                status_data['causal_gene'] = hypothesis.get('causal_gene')
                status_data['enrichment_stage'] = hypothesis.get('enrichment_stage')
                
                # Check if tissue results are ready but enrichment is still ongoing
                if hypothesis.get('enrichment_stage') == 'tissue_analysis_complete':
                    status_data['tissue_results_ready'] = True
                    
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

            # Get tissue selection from tissue_selections collection
            selected_tissue = None
            if self.gene_expression:
                try:
                    # Use variant_rsid for lookup, fall back to variant if not available
                    variant_id = hypothesis.get('variant_rsid') or hypothesis.get('variant') or hypothesis.get('variant_id')
                    project_id = hypothesis.get('project_id')
                    if variant_id and project_id:
                        tissue_selection = self.gene_expression.get_tissue_selection(
                            current_user_id, project_id, variant_id
                        )
                        if tissue_selection:
                            selected_tissue = tissue_selection.get('tissue_name')
                            logger.info(f"Retrieved tissue selection for pending hypothesis {hypothesis_id} using variant_id={variant_id}: {selected_tissue}")
                        else:
                            logger.info(f"No tissue selection found for pending hypothesis {hypothesis_id}, variant_id={variant_id}")
                except Exception as ts_e:
                    logger.warning(f"Could not get tissue selection for pending hypothesis {hypothesis_id}: {ts_e}")
            
            status_data['tissue_selected'] = selected_tissue

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
                'phenotype': hypothesis.get('phenotype'),
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
        
        invoke_hypothesis_deployment(current_user_id, hypothesis_id, enrich_id, go_id)

        return {"message": "Hypothesis generation started", "id": hypothesis_id}, 202

    
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
            inactivity_timer = Timer(600, lambda: socketio.server.disconnect(client_id))  # 10 minutes
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
                hypothesis = hypotheses_handler.get_hypothesis_by_id(hypothesis_id)
            else:
                hypothesis = hypotheses_handler.get_hypotheses(current_user_id, hypothesis_id)
                
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

                if 'tissue_rankings' in hypothesis:
                    response_data['tissue_rankings'] = hypothesis['tissue_rankings']
                    response_data['tissue_results_ready'] = True
                    response_data['causal_gene'] = hypothesis.get('causal_gene')
                    response_data['enrichment_stage'] = hypothesis.get('enrichment_stage')

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
    def __init__(self, projects, files, analysis, hypotheses, enrichment, gene_expression):
        self.projects = projects
        self.files = files
        self.analysis = analysis
        self.hypotheses = hypotheses
        self.enrichment = enrichment
        self.gene_expression = gene_expression
    
    @token_required
    def get(self, current_user_id):
        """Get all projects for a user or comprehensive data for a specific project"""
        project_id = request.args.get('id')
        
        if project_id:
            response_data, status_code = get_project_with_full_data(
                self.projects, self.analysis, self.hypotheses, self.enrichment, 
                current_user_id, project_id, gene_expression_handler=self.gene_expression
            )
            if status_code == 200:
                response_data = serialize_datetime_fields(response_data)
            return response_data, status_code
        
        projects = self.projects.get_projects(current_user_id)
        enhanced_projects = []
        
        for project in projects:
            enhanced_project = {
                "id": project["id"],
                "name": project["name"],
                "phenotype": project.get("phenotype", ""),
                "created_at": project.get("created_at"),
            }
            
            # Add GWAS file information
            file_metadata = self.files.get_file_metadata(current_user_id, project["gwas_file_id"])
            enhanced_project["gwas_file"] = file_metadata["download_url"]
            enhanced_project["gwas_records_count"] = file_metadata["record_count"]
            
            # Add analysis status
            try:
                analysis_state = self.projects.load_analysis_state(current_user_id, project["id"])
                if analysis_state:
                    enhanced_project["status"] = analysis_state.get("status", "Not_started")
                else:
                    enhanced_project["status"] = "Not_started"  # Default for projects without analysis state
            except Exception as state_e:
                logger.warning(f"Could not load analysis state for project {project['id']}: {state_e}")
                enhanced_project["status"] = "Completed"
            
            # Get analysis parameters from project
            enhanced_project["population"] = project.get("population")
            enhanced_project["ref_genome"] = project.get("ref_genome")
            
            # Extract credible sets and variants counts
            total_credible_sets_count = 0
            total_variants_count = 0
            
            try:
                credible_sets_raw = self.analysis.get_credible_sets_for_project(current_user_id, project["id"])
                if credible_sets_raw:
                    if isinstance(credible_sets_raw, list) and credible_sets_raw:
                        # Calculate totals from credible sets
                        total_credible_sets_count = len(credible_sets_raw) if credible_sets_raw else 0
                        total_variants_count = sum(cs.get("variants_count", 0) for cs in credible_sets_raw) if credible_sets_raw else 0
    
            except Exception as cs_e:
                logger.warning(f"Could not load credible sets for project {project['id']}: {cs_e}")
            
            # Add counts to project
            enhanced_project["total_credible_sets_count"] = total_credible_sets_count
            enhanced_project["total_variants_count"] = total_variants_count
            
            # Count hypotheses for this project
            hypothesis_count = 0
            try:
                all_hypotheses = self.hypotheses.get_hypotheses(current_user_id)
                if isinstance(all_hypotheses, list):
                    hypothesis_count = len([h for h in all_hypotheses if h.get('project_id') == project["id"]])
                elif all_hypotheses and all_hypotheses.get('project_id') == project["id"]:
                    hypothesis_count = 1
            except Exception as hyp_e:
                logger.warning(f"Could not count hypotheses for project {project['id']}: {hyp_e}")
            
            enhanced_project["hypothesis_count"] = hypothesis_count
            
            enhanced_projects.append(enhanced_project)
        
        # Serialize datetime objects in all projects
        enhanced_projects = serialize_datetime_fields(enhanced_projects)
        return {"projects": enhanced_projects}, 200
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
class BulkProjectDeleteAPI(Resource):
    def __init__(self, projects):
        self.projects = projects
        
    @token_required
    def post(self, current_user_id):
        data = request.get_json()
        
        if not data or 'project_ids' not in data:
            return {"message": "project_ids is required in request body"}, 400
            
        project_ids = data.get('project_ids')
        
        # Validate the list of IDs
        if not isinstance(project_ids, list):
            return {"message": "project_ids must be a list"}, 400
            
        if not project_ids:
            return {"message": "project_ids list cannot be empty"}, 400
            
        # Call the bulk delete method
        result = self.projects.bulk_delete_projects(current_user_id, project_ids)
        
        if result and isinstance(result, dict):
            if result['success']:
                return {
                    "message": f"Successfully deleted {result['deleted_count']} project(s)",
                    "deleted_count": result['deleted_count'],
                    "total_requested": result['total_requested']
                }, 200
            else:
                return {
                    "message": f"Partially deleted {result['deleted_count']}/{result['total_requested']} project(s)",
                    "deleted_count": result['deleted_count'],
                    "total_requested": result['total_requested'],
                    "errors": result['errors']
                }, 207  # Multi-status
        else:
            return {"error": "Failed to delete projects"}, 500

class AnalysisPipelineAPI(Resource):
    def __init__(self, projects, files, analysis, gene_expression, config, storage, gwas_library):
        self.projects = projects
        self.files = files
        self.analysis = analysis
        self.gene_expression = gene_expression
        self.config = config
        self.storage = storage
        self.gwas_library = gwas_library

    @token_required
    def post(self, current_user_id):
        try:
            # Get form data and file
            project_name = request.form.get('project_name')
            phenotype = request.form.get('phenotype')
            ref_genome = request.form.get('ref_genome', 'GRCh38')
            population = request.form.get('population', 'EUR')
            max_workers = int(request.form.get('max_workers', 3))
            
            # Check the mode
            is_uploaded = request.form.get('is_uploaded', 'false').lower() == 'true'
            
            # Fine-mapping parameters with defaults
            maf_threshold = float(request.form.get('maf_threshold', 0.01))
            seed = int(request.form.get('seed', 42))
            window = int(request.form.get('window', 2000))
            L = int(request.form.get('L', -1))
            coverage = float(request.form.get('coverage', 0.95))
            min_abs_corr = float(request.form.get('min_abs_corr', 0.5))
            batch_size = int(request.form.get('batch_size', 5))
            sample_size = int(request.form.get('sample_size', 10000))
            # Validate required fields
            if not project_name:
                return {"error": "project_name is required"}, 400
            
            if not phenotype:
                return {"error": "phenotype is required"}, 400
            
            logger.info(f"[API] Starting analysis pipeline")
            if is_uploaded and not allowed_file(gwas_file.filename):
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
            
            # === FILE HANDLING AND PROJECT CREATION  ===
            start_time = datetime.now()
            
            if not is_uploaded:
                file_id_param = request.form.get('gwas_file')  
                
                if not file_id_param:
                    return {"error": "gwas_file parameter is required when is_uploaded=false"}, 400
                
                # Auto-detect file source: check user files first, then library
                logger.info(f"[API] Auto-detecting source for file ID: {file_id_param}")
                
                # Try user files first (catches ObjectId validation errors)
                file_metadata = None
                try:
                    file_metadata = self.files.get_file_metadata(current_user_id, file_id_param)
                except Exception as e:
                    logger.info(f"[API] Not a valid user file ID, checking library: {e}")
                
                if file_metadata:
                    # CASE 1: User's uploaded file
                    file_source = 'user'
                    logger.info(f"[API] File found in user library: {file_id_param}")
                    
                    storage_key = file_metadata.get('storage_key')
                    filename = file_metadata.get('filename')
                    file_size = file_metadata.get('file_size', 0)
                    gwas_records_count = file_metadata.get('record_count', 0)
                    
                    if not storage_key:
                        return {"error": "File does not have storage information. It may be an old entry that needs re-upload."}, 400
                    
                    if not self.storage:
                        return {"error": "Storage service not available"}, 500
                    
                    # Download from MinIO to shared temp location for processing
                    from utils import get_shared_temp_dir
                    temp_dir = get_shared_temp_dir(user_id=current_user_id, prefix="user_file")
                    temp_file_path = os.path.join(temp_dir, filename)
                    
                    if not self.storage.download_file(storage_key, temp_file_path):
                        return {"error": "Failed to retrieve file from storage"}, 500
                    
                    file_path = temp_file_path
                    gwas_file_path = file_path
                    file_metadata_id = file_id_param
                    
                    logger.info(f"[API] Reusing user file: {filename} from MinIO: {storage_key}")
                    
                else:
                    # Not in user files, check system library
                    gwas_entry = self.gwas_library.get_gwas_entry(file_id=file_id_param)
                    
                    if not gwas_entry:
                        return {"error": f"File not found in user library or system library: {file_id_param}"}, 404
                    
                    # CASE 2: Library file
                    file_source = 'library'
                    logger.info(f"[API] File found in system library: {file_id_param}")
                    
                    gwas_file_path = None
                    filename = gwas_entry.get('filename', file_id_param)
                    file_size = 0
                    
                    # Check if file is cached in MinIO
                    if self.storage and gwas_entry.get('downloaded') and gwas_entry.get('minio_path'):
                        minio_path = gwas_entry['minio_path']
                        
                        if self.storage.exists(minio_path):
                            # Download from MinIO to shared temp location for analysis
                            from utils import get_shared_temp_dir
                            temp_dir = get_shared_temp_dir(user_id=current_user_id, prefix="gwas_cache")
                            gwas_file_path = os.path.join(temp_dir, filename)
                            
                            logger.info(f"[API] Downloading from MinIO: {minio_path}")
                            if self.storage.download_file(minio_path, gwas_file_path):
                                file_size = os.path.getsize(gwas_file_path)
                                logger.info(f"[API] Downloaded from MinIO cache: {gwas_file_path}")
                            else:
                                gwas_file_path = None
                        else:
                            logger.warning(f"[API] MinIO cache missing: {minio_path}")
                    
                    # If not in MinIO, download from source
                    if not gwas_file_path:
                        logger.info(f"[API] File not in MinIO, downloading from source")
                        
                        # Get download URL
                        download_url = gwas_entry.get('aws_url') or gwas_entry.get('dropbox_url')
                        if not download_url and gwas_entry.get('wget_command'):
                            import re
                            url_match = re.search(r'(https?://[^\s]+)', gwas_entry['wget_command'])
                            if url_match:
                                download_url = url_match.group(1)
                        
                        if not download_url:
                            return {"error": f"No download URL available for {file_id_param}"}, 404
                        
                        # Download to shared temp location
                        from utils import get_shared_temp_dir
                        import requests
                        
                        temp_dir = get_shared_temp_dir(user_id=current_user_id, prefix="gwas_download")
                        gwas_file_path = os.path.join(temp_dir, filename)
                        
                        logger.info(f"[API] Downloading from {download_url}")
                        try:
                            response = requests.get(download_url, stream=True, timeout=600)
                            response.raise_for_status()
                            
                            with open(gwas_file_path, 'wb') as f:
                                for chunk in response.iter_content(chunk_size=8192):
                                    f.write(chunk)
                            
                            file_size = os.path.getsize(gwas_file_path)
                            logger.info(f"[API] Downloaded {file_size / (1024*1024):.2f} MB")
                            
                            # Upload to MinIO for future use
                            if self.storage:
                                minio_path = f"gwas_cache/{filename}"
                                if self.storage.upload_file(gwas_file_path, minio_path):
                                    self.gwas_library.mark_as_downloaded(file_id_param, minio_path, file_size)
                                    logger.info(f"[API] Uploaded to MinIO: s3://{minio_path}")
                            
                        except Exception as e:
                            logger.error(f"[API] Download failed: {e}")
                            return {"error": f"Failed to download file: {str(e)}"}, 500
                    
                    # Fallback to data/raw/ for legacy files
                    if not gwas_file_path:
                        logger.info(f"[API] File not in library, checking data/raw/")
                        raw_data_path = os.path.join(self.config.data_dir, 'raw')
                        
                        possible_extensions = ['.tsv', '.tsv.gz', '.tsv.bgz', '.txt', '.txt.gz', '.csv', '.csv.gz']
                        
                        for ext in possible_extensions:
                            candidate_path = os.path.join(raw_data_path, f"{file_id_param}{ext}")
                            if os.path.exists(candidate_path):
                                gwas_file_path = candidate_path
                                filename = f"{file_id_param}{ext}"
                                file_size = os.path.getsize(gwas_file_path)
                                logger.info(f"[API] Found in data/raw: {gwas_file_path}")
                                break
                    
                    if not gwas_file_path:
                        return {"error": f"GWAS file not found: {file_id_param}"}, 404
                    
                    file_path = gwas_file_path
                    gwas_records_count = count_gwas_records(file_path)
                    file_id = str(uuid.uuid4())
                    file_metadata_id = None 
                
            # CASE 3: New file upload with MD5 deduplication
            else:
                # Validate file upload
                if 'gwas_file' not in request.files:
                    return {"error": "No GWAS file uploaded"}, 400
                
                gwas_file = request.files['gwas_file']
                if gwas_file.filename == '':
                    return {"error": "No file selected"}, 400
                
                if not allowed_file(gwas_file.filename):
                    return {"error": "Invalid file format. Supported: .tsv, .txt, .csv, .gz, .bgz"}, 400
                
                # Generate secure filename and file ID
                filename = secure_filename(gwas_file.filename)
                file_id = str(uuid.uuid4())
                
                logger.info(f"[API] Starting upload for file {filename} (ID: {file_id})")
                
                # Create user-isolated temp directory in shared volume
                from utils import get_shared_temp_dir
                temp_dir = get_shared_temp_dir(user_id=current_user_id, prefix="upload")
                
                # Save to shared temp location
                temp_file_path = os.path.join(temp_dir, filename)
                gwas_file.save(temp_file_path)
                file_size = os.path.getsize(temp_file_path)
                
                # Compute MD5 hash for deduplication
                md5_hash = compute_file_md5(temp_file_path)
                logger.info(f"[API] Computed MD5: {md5_hash}")
                
                # Check if user already uploaded this file
                existing_file = None
                if md5_hash and self.storage:
                    existing_file = self.files.find_file_by_md5(current_user_id, md5_hash)
                
                if existing_file:
                    logger.info(f"[API] File already uploaded (MD5 match): {existing_file.get('_id')}")
                    logger.info(f"[API] Reusing existing MinIO object: {existing_file.get('storage_key')}")
                    
                    # Cleanup temp file - we'll reuse the existing one
                    try:
                        os.remove(temp_file_path)
                    except:
                        pass
                    
                    # Download existing file for processing
                    storage_key = existing_file.get('storage_key')
                    if storage_key and self.storage.exists(storage_key):
                        self.storage.download_file(storage_key, temp_file_path)
                        file_path = temp_file_path
                        gwas_file_path = file_path
                        file_metadata_id = existing_file.get('_id')
                        gwas_records_count = existing_file.get('record_count', count_gwas_records(file_path))
                    else:
                        return {"error": "Failed to retrieve existing file from storage"}, 500
                else:
                    # New file - upload to MinIO
                    gwas_records_count = count_gwas_records(temp_file_path)
                    
                    if self.storage:
                        # MinIO path: uploads/{user_id}/{file_id}/{filename}
                        object_key = f"uploads/{current_user_id}/{file_id}/{filename}"
                        
                        # Upload to MinIO for persistence
                        upload_success = self.storage.upload_file(temp_file_path, object_key)
                        
                        if not upload_success:
                            logger.error(f"[API] Failed to upload file to MinIO")
                            # Cleanup temp dir on failure
                            try:
                                import shutil
                                shutil.rmtree(temp_dir, ignore_errors=True)
                            except:
                                pass
                            return {"error": "File upload failed"}, 500
                        
                        # Use temp file for immediate processing
                        file_path = temp_file_path
                        gwas_file_path = file_path
                        
                        logger.info(f"[API] Uploaded to MinIO: s3://hypothesis/{object_key}")
                        logger.info(f"[API] Using temp file for processing: {file_path}")
                        
                        file_metadata_id = None  # Will create below
                    else:
                        # Fallback: Save to local disk
                        logger.warning("[API] MinIO not configured, falling back to local storage")
                        user_upload_dir = os.path.join('data', 'uploads', str(current_user_id))
                        os.makedirs(user_upload_dir, exist_ok=True)
                        file_path = os.path.join(user_upload_dir, f"{file_id}_{filename}")
                        gwas_file.save(file_path)
                        file_size = os.path.getsize(file_path)
                        gwas_file_path = file_path
                        gwas_records_count = count_gwas_records(file_path)
                        
                        file_metadata_id = None  # Will create below
            
            # Create file metadata in database (if not reusing existing)
            if not file_metadata_id:
                original_filename = gwas_file.filename if is_uploaded else filename
                
                # Determine source
                if not is_uploaded:
                    source = 'gwas_library'
                else:
                    source = 'user_upload'
                
                # Get storage_key and md5_hash if applicable
                storage_key = object_key if is_uploaded and self.storage else None
                md5_hash_param = md5_hash if is_uploaded and md5_hash else None
                
                file_metadata_id = self.files.create_file_metadata(
                    user_id=current_user_id,
                    filename=filename,
                    original_filename=original_filename,
                    file_path=file_path,
                    file_type='gwas',
                    file_size=file_size,
                    record_count=gwas_records_count,
                    download_url=f"/download/{str(uuid.uuid4())}",
                    md5_hash=md5_hash_param,
                    storage_key=storage_key,
                    source=source
                )
            
            # Prepare analysis parameters
            analysis_parameters = {
                'maf_threshold': maf_threshold,
                'seed': seed,
                'window': window,
                'L': L,
                'coverage': coverage,
                'min_abs_corr': min_abs_corr,
                'batch_size': batch_size,
                'max_workers': max_workers
            }
            
            # Create project with analysis parameters
            project_id = self.projects.create_project(
                user_id=current_user_id,
                name=project_name,
                gwas_file_id=file_metadata_id,
                phenotype=phenotype,
                population=population,
                ref_genome=ref_genome,
                analysis_parameters=analysis_parameters
            )
            
            # Save metadata to file system
            metadata_dir = os.path.join('data', 'metadata', str(current_user_id))
            os.makedirs(metadata_dir, exist_ok=True)
            
            metadata = {
                'file_id': file_metadata_id,
                'user_id': current_user_id,
                'filename': filename,
                'original_filename': original_filename,
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
            
            invoke_analysis_pipeline_deployment(
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
                sample_size=sample_size
            )
            
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
class PhenotypesAPI(Resource):
    """
    API endpoint for getting and loading phenotypes
    """
    def __init__(self, phenotypes):
        self.phenotypes = phenotypes

    def get(self):
        """Get phenotypes with pagination to prevent memory issues"""
        try:
            # Get query parameters
            phenotype_id = request.args.get('id')
            search_term = request.args.get('search')
            limit = request.args.get('limit', type=int)
            skip = request.args.get('skip', 0, type=int)
            
            if phenotype_id:
                # Get specific phenotype
                phenotype = self.phenotypes.get_phenotypes(phenotype_id=phenotype_id)
                if not phenotype:
                    return {"error": "Phenotype not found"}, 404
                return serialize_datetime_fields({"phenotype": phenotype}), 200
            
            # Set reasonable default limit to prevent memory issues
            if limit is None:
                limit = 100
                logger.info(f"No limit specified, using default limit of {limit} for memory protection")
            
            # Get phenotypes with pagination
            phenotypes = self.phenotypes.get_phenotypes(
                limit=limit, 
                skip=skip, 
                search_term=search_term
            )
            
            # Get total count for pagination
            total_count = self.phenotypes.count_phenotypes(search_term=search_term)
            
            response = {
                "phenotypes": phenotypes,
                "total_count": total_count,
                "skip": skip,
                "limit": limit,
                "has_more": (skip + len(phenotypes)) < total_count,
                "next_skip": skip + len(phenotypes) if (skip + len(phenotypes)) < total_count else None
            }
            
            if search_term:
                response["search_term"] = search_term
            
            return serialize_datetime_fields(response), 200
            
        except Exception as e:
            logger.error(f"Error getting phenotypes: {str(e)}")
            return {"error": f"Failed to get phenotypes: {str(e)}"}, 500

    def post(self):
        """
        Bulk load phenotypes from JSON data
        
        Expects JSON array with format:
        [
            {"name": "phenotype name", "id": "EFO_1234567"},
            ...
        ]
        """
        try:
            # Get JSON data from request
            data = request.get_json()
            
            if not data:
                return {"error": "No JSON data provided"}, 400
            
            if not isinstance(data, list):
                return {"error": "Expected JSON array of phenotypes"}, 400
            
            # Transform data to match database schema
            # Input format: {"name": "...", "id": "..."}
            # Database format: {"phenotype_name": "...", "id": "..."}
            phenotypes_data = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                
                # Map "name" to "phenotype_name" for database
                phenotype = {
                    "id": item.get("id", ""),
                    "phenotype_name": item.get("name", "")
                }
                
                # Validate that both fields exist
                if phenotype["id"] and phenotype["phenotype_name"]:
                    phenotypes_data.append(phenotype)
                else:
                    logger.warning(f"Skipping invalid phenotype entry: {item}")
            
            if not phenotypes_data:
                return {"error": "No valid phenotypes found in JSON data"}, 400
            
            # Bulk insert phenotypes
            logger.info(f"Loading {len(phenotypes_data)} phenotypes into database...")
            result = self.phenotypes.bulk_create_phenotypes(phenotypes_data)
            
            response = {
                "message": "Phenotypes loaded successfully",
                "inserted_count": result['inserted_count'],
                "skipped_count": result['skipped_count'],
                "total_provided": len(phenotypes_data)
            }
            
            logger.info(f"Phenotype load complete: {result['inserted_count']} inserted, {result['skipped_count']} skipped")
            
            return response, 201
            
        except Exception as e:
            logger.error(f"Error loading phenotypes: {str(e)}")
            return {"error": f"Failed to load phenotypes: {str(e)}"}, 500


class CredibleSetsAPI(Resource):
    """
    API endpoint for fetching credible sets
    """
    def __init__(self, analysis):
        self.analysis = analysis

    @token_required
    def get(self, current_user_id):
        """Get credible set details by credible set ID or lead variant ID"""
        project_id = request.args.get('project_id')
        credible_set_id = request.args.get('credible_set_id')
        
        if not project_id:
            return {"error": "project_id is required"}, 400
        
        if not credible_set_id:
            return {"error": "Credible_set_id is required"}, 400
        
        try:
            credible_set = self.analysis.get_credible_set_by_id(current_user_id, project_id, credible_set_id)
            if not credible_set:
                return {"message": "No credible set found with this ID"}, 404
            
            # Extract variants data 
            variants_data = credible_set.get("variants_data", {})
            if not variants_data:
                return {"message": "No variants data found for this credible set"}, 404
            
            variants = variants_data.get("data", {})
            
            # Convert from object-with-arrays format to array-of-objects format
            variants_array = convert_variants_to_object_array(variants)

            variants_array = serialize_datetime_fields(variants_array)
            return {
                "variants": variants_array
            }, 200

        except Exception as e:
            logger.error(f"Error fetching credible set: {str(e)}")
            return {"error": f"Failed to fetch credible set: {str(e)}"}, 500


class GWASFilesAPI(Resource):
    """
    API endpoint for serving GWAS files from the library collection
    """
    def __init__(self, config, gwas_library, storage):
        self.config = config
        self.gwas_library = gwas_library
        self.storage = storage

    def get(self):
        """Get list of available GWAS files from MongoDB"""
        try:
            # Get query parameters
            search_term = request.args.get('search')
            sex_filter = request.args.get('sex')
            limit = request.args.get('limit', type=int)
            skip = request.args.get('skip', 0, type=int)
            
            # Set default limit
            if limit is None:
                limit = 100
            
            # Get entries from collection
            entries = self.gwas_library.get_all_gwas_entries(
                search_term=search_term,
                sex_filter=sex_filter,
                limit=limit,
                skip=skip
            )
            
            # Transform entries to match API format
            gwas_files = []
            for entry in entries:
                file_id = entry.get('file_id') or entry.get('filename')
                gwas_file_entry = {
                    "id": file_id,  # Use filename as ID
                    "display_name": entry.get('display_name'),
                    "description": entry.get('description'),
                    "phenotype_code": entry.get('phenotype_code'),
                    "filename": entry.get('filename'),
                    "sex": entry.get('sex'),
                    "source": entry.get('source'),
                    "downloaded": entry.get('downloaded', False),  # Cached in MinIO
                    "download_count": entry.get('download_count', 0),
                    "url": f"/gwas-files/download/{file_id}",
                    "showcase_link": entry.get('showcase_link', ''),
                }
                
                # Add file size if available
                if entry.get('file_size'):
                    gwas_file_entry['file_size_mb'] = round(entry['file_size'] / (1024 * 1024), 2)
                
                gwas_files.append(gwas_file_entry)
            
            # Get total count for pagination
            total_count = self.gwas_library.get_entry_count(
                search_term=search_term,
                sex_filter=sex_filter
            )
            
            return {
                "gwas_files": gwas_files,
                "total_files": total_count,
                "returned": len(gwas_files),
                "skip": skip,
                "limit": limit
            }, 200
            
        except Exception as e:
            logger.error(f"Error fetching GWAS files from library: {str(e)}")
            return {"error": f"Failed to fetch GWAS files: {str(e)}"}, 500

class GWASFileDownloadAPI(Resource):
    """
    API endpoint for downloading GWAS files with MinIO caching
    """
    def __init__(self, config, gwas_library, storage):
        self.config = config
        self.gwas_library = gwas_library
        self.storage = storage

    def get(self, file_id):
        """Download a GWAS file by file_id (downloads on-demand to MinIO if needed)"""
        try:
            logger.info(f"[GWAS DOWNLOAD] Download request for file: {file_id}")
            
            # Get entry from library
            entry = self.gwas_library.get_gwas_entry(file_id=file_id)
            
            if not entry:
                logger.error(f"[GWAS DOWNLOAD] Entry not found for file: {file_id}")
                return {"error": "GWAS file not found in library"}, 404
            
            filename = entry.get('filename', file_id)
            minio_path = f"gwas_cache/{filename}"
            
            # Check if file is already cached in MinIO
            if self.storage and entry.get('downloaded') and entry.get('minio_path'):
                cached_path = entry['minio_path']
                
                # Verify file still exists in MinIO
                if self.storage.exists(cached_path):
                    logger.info(f"[GWAS DOWNLOAD] Serving from MinIO cache: {cached_path}")
                    
                    # Increment download count
                    self.gwas_library.increment_download_count(file_id)
                    
                    # Generate presigned URL for download
                    download_url = self.storage.generate_presigned_url(cached_path, expiration=3600)
                    
                    if download_url:
                        return {"download_url": download_url, "cached": True}, 200
                    else:
                        # Fallback: download to temp and serve
                        import tempfile
                        with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{filename}") as tmp:
                            if self.storage.download_file(cached_path, tmp.name):
                                return send_file(
                                    tmp.name,
                                    as_attachment=True,
                                    download_name=filename,
                                    mimetype='text/tab-separated-values'
                                )
                else:
                    logger.warning(f"[GWAS DOWNLOAD] Cached file missing from MinIO: {cached_path}")
                    # Mark as not downloaded and continue to download
                    self.gwas_library.update_gwas_entry(file_id, {
                        'downloaded': False,
                        'minio_path': None
                    })
            
            # File not cached - download on-demand
            logger.info(f"[GWAS DOWNLOAD] File not cached, downloading on-demand")
            
            # Determine download URL (prefer AWS, then wget, then Dropbox)
            download_url = None
            if entry.get('aws_url'):
                download_url = entry['aws_url']
                logger.info(f"Using AWS URL: {download_url}")
            elif entry.get('wget_command'):
                # Extract URL from wget command
                import re
                url_match = re.search(r'(https?://[^\s]+)', entry['wget_command'])
                if url_match:
                    download_url = url_match.group(1)
                    logger.info(f"Extracted URL from wget: {download_url}")
            elif entry.get('dropbox_url'):
                download_url = entry['dropbox_url']
                logger.info(f"Using Dropbox URL: {download_url}")
            
            if not download_url:
                logger.error(f"[GWAS DOWNLOAD] No download URL available for {file_id}")
                return {"error": "No download URL available for this file"}, 404
            
            # Download file to temp location
            import tempfile
            import requests
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{filename}") as tmp:
                temp_path = tmp.name
                
                logger.info(f"[GWAS DOWNLOAD] Downloading from {download_url}")
                try:
                    response = requests.get(download_url, stream=True, timeout=600)
                    response.raise_for_status()
                    
                    # Write to temp file
                    for chunk in response.iter_content(chunk_size=8192):
                        tmp.write(chunk)
                    
                    file_size = os.path.getsize(temp_path)
                    logger.info(f"[GWAS DOWNLOAD] Downloaded {file_size / (1024*1024):.2f} MB")
                    
                    # Upload to MinIO if available
                    if self.storage:
                        if self.storage.upload_file(temp_path, minio_path):
                            # Update database
                            self.gwas_library.mark_as_downloaded(file_id, minio_path, file_size)
                            self.gwas_library.increment_download_count(file_id)
                            logger.info(f"[GWAS DOWNLOAD] Uploaded to MinIO: s3://{minio_path}")
                        else:
                            logger.warning(f"[GWAS DOWNLOAD] Failed to upload to MinIO")
                    
                    # Serve the file
                    return send_file(
                        temp_path,
                        as_attachment=True,
                        download_name=filename,
                        mimetype='text/tab-separated-values'
                    )
                    
                except Exception as e:
                    logger.error(f"[GWAS DOWNLOAD] Download failed: {e}")
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                    return {"error": f"Failed to download file: {str(e)}"}, 500
            
        except Exception as e:
            logger.error(f"[GWAS DOWNLOAD] Error downloading file {file_id}: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"error": f"Download failed: {str(e)}"}, 500


class UserFilesAPI(Resource):
    """
    API endpoint for managing user-uploaded files
    """
    def __init__(self, files, storage):
        self.files = files
        self.storage = storage
    
    @token_required
    def get(self, current_user_id):
        """Get all user-uploaded GWAS files"""
        try:
            # Get all files for this user
            all_files = self.files.get_file_metadata(current_user_id)
            
            if not all_files:
                return {"files": []}, 200
            
            # Ensure it's a list
            if not isinstance(all_files, list):
                all_files = [all_files]
            
            # Filter to only user uploads and format for API
            user_files = []
            for file_meta in all_files:
                # Only include user uploads (exclude library files if marked)
                if file_meta.get('source') not in [None, 'user_upload']:
                    continue
                
                file_entry = {
                    "id": file_meta.get('_id'),
                    "display_name": file_meta.get('original_filename', file_meta.get('filename')),
                    "filename": file_meta.get('filename'),
                    "file_size": file_meta.get('file_size', 0),
                    "file_size_mb": round(file_meta.get('file_size', 0) / (1024 * 1024), 2),
                    "record_count": file_meta.get('record_count'),
                    "upload_date": file_meta.get('upload_date'),
                    "source": "user_upload"
                }
                
                user_files.append(file_entry)
            
            # Sort by upload date (most recent first)
            user_files = sorted(user_files, key=lambda x: x.get('upload_date', ''), reverse=True)
            
            # Serialize datetime fields
            user_files = serialize_datetime_fields(user_files)
            
            return {
                "files": user_files,
                "total_files": len(user_files)
            }, 200
            
        except Exception as e:
            logger.error(f"Error fetching user files: {str(e)}")
            return {"error": f"Failed to fetch user files: {str(e)}"}, 500

