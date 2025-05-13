import os
from threading import Thread, Timer
import uuid
from prefect.utilities.asyncutils import sync_compatible
from prefect.context import get_run_context
import asyncio
from flask import Flask, json, request, jsonify
from flask_jwt_extended import get_jwt_identity
from flask_restful import Resource, Api, reqparse
from flask_socketio import join_room
from socketio_instance import socketio
from auth import socket_token_required, token_required
from datetime import datetime, timezone
from uuid import uuid4
from flows import async_enrichment_process, enrichment_flow, finemapping_analysis_flow, hypothesis_flow,  preprocessing_flow
# finemapping_flow
from status_tracker import status_tracker, TaskState
from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner
from utils import allowed_file, emit_task_update, get_user_file_path
from loguru import logger
from summary import process_summary
from werkzeug.utils import secure_filename

class EnrichAPI(Resource):
    def __init__(self, enrichr, llm, prolog_query, db):
        self.enrichr = enrichr
        self.llm = llm
        self.prolog_query = prolog_query
        self.db = db

    @token_required
    def get(self, current_user_id):
        # Get the enrich_id from the query parameters
        enrich_id = request.args.get('id')
        if enrich_id:
            # Fetch a specific enrich by enrich_id and user_id
            enrich = self.db.get_enrich(current_user_id, enrich_id)
            if not enrich:
                return {"message": "Enrich not found or access denied."}, 404
            return enrich, 200
          
        # Fetch all hypotheses for the current user
        enrich = self.db.get_enrich(user_id=current_user_id)
        return enrich, 200

    @token_required
    def post(self, current_user_id):
        args = request.args
        phenotype, variant = args['phenotype'], args['variant']

        existing_hypothesis = self.db.get_hypothesis_by_phenotype_and_variant(current_user_id, phenotype, variant)

        # Define async flow function
        def run_async_flow():
            asyncio.run(async_enrichment_process(
                enrichr=self.enrichr, 
                llm=self.llm, 
                prolog_query=self.prolog_query, 
                db=self.db, 
                current_user_id=current_user_id, 
                phenotype=phenotype, 
                variant=variant, 
                hypothesis_id=existing_hypothesis['id'] if existing_hypothesis else hypothesis_id
            ))

        if existing_hypothesis:
            Thread(target=run_async_flow).start()
            return {"hypothesis_id": existing_hypothesis['id']}, 202
        
        # Generate hypothesis_id immediately
        hypothesis_id = str(uuid4())

        # Store initial hypothesis state
        hypothesis_data = {
            "id": hypothesis_id,
            "phenotype": phenotype,
            "variant": variant,
            "status": "pending",
            "created_at": datetime.now(timezone.utc).isoformat(timespec='milliseconds') + "Z",
            "task_history": [],
        }

        self.db.create_hypothesis(current_user_id, hypothesis_data)

        # Start the thread
        Thread(target=run_async_flow).start()

        
        return {"hypothesis_id": hypothesis_id}, 201
    
         
    @token_required
    def delete(self, current_user_id):
        enrich_id = request.args.get('id')
        if enrich_id:
            result = self.db.delete_enrich(current_user_id, enrich_id)
            return result, 200
        return {"message": "enrich id is required!"}, 400


class HypothesisAPI(Resource):
    def __init__(self, enrichr, prolog_query, llm, db):
        self.enrichr = enrichr
        self.prolog_query = prolog_query
        self.llm = llm
        self.db = db

    @token_required
    def get(self, current_user_id):
        # Get the hypothesis_id from the query parameters
        hypothesis_id = request.args.get('id')

        if hypothesis_id:
            # Fetch a specific hypothesis by hypothesis_id and user_id
            hypothesis = self.db.get_hypotheses(current_user_id, hypothesis_id)
            if not hypothesis:
                return {"message": "Hypothesis not found or access denied."}, 404
            
            # Check if enrichment is complete
            required_fields = ['enrich_id', 'go_id', 'summary', 'graph']
            is_complete = all(field in hypothesis for field in required_fields)
            # Get task history
            task_history = status_tracker.get_history(hypothesis_id)
            for task in task_history:
                task.pop('details', None)  # Remove 'details' if it exists

            # Get only pending tasks from task history
            pending_tasks = [task for task in task_history if task.get('state') == TaskState.STARTED.value]
            last_pending_task = [pending_tasks[-1]] if pending_tasks else [] 
            print(f"last_pending_task: {last_pending_task}")

            if is_complete:
                enrich_id = hypothesis.get('enrich_id')
                enrich_data = self.db.get_enrich(current_user_id, enrich_id)
                # Remove 'causal_graph' field from enrich_data if it exists
                if isinstance(enrich_data, dict):
                    enrich_data.pop('causal_graph', None)
                return {
                    'id': hypothesis_id,
                    'variant': hypothesis.get('variant') or hypothesis.get('variant_id'),
                    'enrich_id': enrich_id,
                    'phenotype': hypothesis['phenotype'],
                    "status": "completed",
                    "created_at": hypothesis.get('created_at'),
                    "result": enrich_data
                }, 200

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
                enrich_data = self.db.get_enrich(current_user_id, enrich_id)
                if isinstance(enrich_data, dict):
                    enrich_data.pop('causal_graph', None)
                status_data['result'] = enrich_data
                

            # Check for failed state
            if latest_state and latest_state.get('state') == 'failed':
                status_data['status'] = 'failed'
                status_data['error'] = latest_state.get('error')

            return status_data, 200

        # Fetch all hypotheses for the current user
        hypotheses = self.db.get_hypotheses(user_id=current_user_id)
        
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
            
        return formatted_hypotheses, 200
        # return hypotheses, 200

    @token_required
    def post(self, current_user_id):
        enrich_id = request.args.get('id')
        go_id = request.args.get('go')

        # Get the hypothesis associated with this enrichment
        hypothesis = self.db.get_hypothesis_by_enrich(current_user_id, enrich_id)
        if not hypothesis:
            return {"message": "No hypothesis found for this enrichment"}, 404
        
        hypothesis_id = hypothesis['id']
        
        # Run the Prefect flow and return the result
        flow_result = hypothesis_flow(current_user_id, hypothesis_id, enrich_id, go_id, self.db, self.prolog_query, self.llm)

        return flow_result[0], flow_result[1]

    
    @token_required
    def delete(self, current_user_id):
        hypothesis_id = request.args.get('hypothesis_id')
        if hypothesis_id:
            return self.db.delete_hypothesis(current_user_id, hypothesis_id)
        return {"message": "Hypothesis ID is required"}, 400
        
    
class BulkHypothesisDeleteAPI(Resource):
    def __init__(self, db):
        self.db = db
        
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
        result, status_code = self.db.bulk_delete_hypotheses(current_user_id, hypothesis_ids)

        return result, status_code

class ChatAPI(Resource):
    def __init__(self, llm, db):
        self.llm = llm
        self.db = db

    @token_required
    def post(self, current_user_id):
        query = request.form.get('query')
        hypothesis_id = request.form.get('hypothesis_id')

        hypothesis = self.db.get_hypotheses(current_user_id, hypothesis_id)
        print(f"Hypothesis: {hypothesis}")
        
        if not hypothesis:
            return {"error": "Hypothesis not found or access denied"}, 404
        
        graph = hypothesis.get('graph')
        response = self.llm.chat(query, graph)
        response = {"response": response}
        return response

# class AnalysisAPI(Resource):
#     def __init__(self, db):
#         self.db = db

#     @token_required
#     def post(self, current_user_id):
#         pass

#     @token_required
#     def get(self, current_user_id):

#         suisie_result = finemapping_flow(current_user_id)

#         return {
#             "suisie_result": suisie_result
#         }, 200
    
class AnalysisAPI(Resource):
    """
    API endpoint for GWAS preprocessing analysis.
    
    Endpoint:
        POST /analysis
            Uploads a GWAS file and runs the preprocessing workflow to identify gene types.
            Parameters:
                - population: The population to use for analysis (default: 'EUR')
                - gwas_file_id: ID of the uploaded GWAS file
            Returns:
                - gene_types: A list of gene types identified in the GWAS data
    """
    def __init__(self, db):
        self.db = db
    
    @token_required
    def post(self, current_user_id):
        # Parse input parameters
        data = request.get_json()
        population = data.get('population', 'EUR')  # Default to EUR if not provided
        gwas_file_id = data.get('gwas_file_id')
        
        if not gwas_file_id:
            return {"error": "Missing required parameter: gwas_file_id"}, 400
            
        try:
            # Get the file path from the file ID
            gwas_file_path = get_user_file_path(gwas_file_id, current_user_id)
            
            # Run the first part of the flow
            gene_types = preprocessing_flow(current_user_id, population, gwas_file_path)
            
            return {"gene_types": gene_types}, 200
        except FileNotFoundError as e:
            return {"error": str(e)}, 404
        except Exception as e:
            return {"error": f"Analysis failed: {str(e)}"}, 500

class FileUploadAPI(Resource):
    def __init__(self, db):
        self.db = db

    @token_required
    def post(self, current_user_id):
        """Handle GWAS file uploads without database"""
        if 'file' not in request.files:
            return {"error": "No file part"}, 400
            
        file = request.files['file']
        
        if file.filename == '':
            return {"error": "No selected file"}, 400
            
        if file and allowed_file(file.filename):
            # Generate a secure filename with UUID
            filename = secure_filename(file.filename)
            file_id = str(uuid.uuid4())
            
            # Create user upload directory
            user_upload_dir = os.path.join('data', 'uploads', current_user_id)
            os.makedirs(user_upload_dir, exist_ok=True)
            
            # Save the file
            file_path = os.path.join(user_upload_dir, f"{file_id}_{filename}")
            file.save(file_path)
            
            # Store file metadata in a JSON file
            metadata = {
                'file_id': file_id,
                'user_id': current_user_id,
                'filename': filename,
                'original_filename': file.filename,
                'file_path': file_path,
                'file_type': 'gwas',
                'upload_date': str(datetime.now()),
                'file_size': os.path.getsize(file_path)
            }
            
            # Save metadata to a JSON file
            metadata_dir = os.path.join('data', 'metadata', current_user_id)
            os.makedirs(metadata_dir, exist_ok=True)
            
            with open(os.path.join(metadata_dir, f"{file_id}.json"), 'w') as f:
                json.dump(metadata, f)
            
            return {
                'file_id': file_id,
                'filename': filename,
                'message': 'File successfully uploaded'
            }, 200
        
        return {"error": "File type not allowed"}, 400
    
def init_socket_handlers(db_instance):
    logger.info("Initializing socket handlers...")
    # @socketio.on('connect')
    # def handle_connect():
    #     logger.info("Client connected")
    #     print("Client connected")
    #     return True
    
    @socketio.on('connect')
    @socket_token_required
    def handle_connect(self, current_user_id):
        logger.info("somehting else")
        logger.info(f"Client connected: {current_user_id}")
        client_id = request.sid
        inactivity_timer = Timer(300, lambda: socketio.close_room(client_id))
        inactivity_timer.start()

        return True

    @socketio.on('disconnect')
    def handle_disconnect():
        logger.info("Client disconnected")

    @socketio.on('subscribe_hypothesis')
    @socket_token_required
    def handle_subscribe(data, current_user_id):
        try:
            logger.info(f"Received subscribe request: {data}")
            print(f"Received subscribe request: {data}")
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
            rooms = socketio.server.rooms(request.sid)
            logger.info(f"Current rooms for client {request.sid}: {rooms}")
            
            # Get hypothesis data
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

class AnalysisFinemappingAPI(Resource):
    """
    API endpoint for running fine-mapping analysis on selected gene types.
    
    Endpoint:
        POST /analysis/finemapping
            Runs the fine-mapping analysis on selected gene types.
            Expected JSON body:
                {
                    "gene_types": ["protein_coding", "lincRNA", ...]
                }
            Returns:
                - susie_result: A dictionary where keys are gene types and values are 
                  lists of credible sets for each gene type
    """
    def __init__(self, db):
        self.db = db
    
    @token_required
    def post(self, current_user_id):
        data = request.get_json()
        
        if not data or 'gene_types' not in data:
            return {"error": "Missing required parameter: gene_types in request body"}, 400
        
        selected_genes = data['gene_types']
        
        # Validate that gene_types is a list
        if not isinstance(selected_genes, list):
            return {"error": "gene_types must be an array/list of strings"}, 400
        
        if not selected_genes:
            return {"error": "gene_types list cannot be empty"}, 400
        
        try:
            # Run the second part of the flow with the selected gene(s)
            susie_result = finemapping_analysis_flow(current_user_id, selected_genes)
            
            return {"susie_result": susie_result}, 200
        except Exception as e:
            return {"error": f"Analysis failed: {str(e)}"}, 500
