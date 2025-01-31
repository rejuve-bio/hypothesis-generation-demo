from threading import Thread
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
from flows import async_enrichment_process, enrichment_flow, hypothesis_flow
from status_tracker import status_tracker, TaskState
from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner
from utils import emit_task_update
from loguru import logger

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
            "variant_id": variant,
            "status": "pending",
            "task_history": [],
        }

        self.db.create_hypothesis(current_user_id, hypothesis_data)

        # # Run in a separate thread
        # def run_async_flow():
        #     asyncio.run(async_enrichment_process(
        #         enrichr=self.enrichr, 
        #         llm=self.llm, 
        #         prolog_query=self.prolog_query, 
        #         db=self.db, 
        #         current_user_id=current_user_id, 
        #         phenotype=phenotype, 
        #         variant=variant, 
        #         hypothesis_id=hypothesis_id
        #     ))

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

            if is_complete:
                return {
                    'id': hypothesis_id,
                    'variant': hypothesis['variant_id'],
                    'phenotype': hypothesis['phenotype'],
                    "status": "completed",
                    "result": hypothesis,
                    "task_history": task_history
                }, 200

            latest_state = status_tracker.get_latest_state(hypothesis_id)

            status_data = {
                'id': hypothesis_id,
                'variant': hypothesis['variant_id'],
                'phenotype': hypothesis['phenotype'],
                'status': 'in_progress',
                'task_history': task_history,
            }

            # Check for failed state
            if latest_state and latest_state.get('state') == 'failed':
                status_data['status'] = 'failed'
                status_data['error'] = latest_state.get('error')

            return status_data, 200

        # Fetch all hypotheses for the current user
        hypotheses = self.db.get_hypotheses(user_id=current_user_id)
        return hypotheses, 200

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
        return {"message": "hypothesis id is required!"}
    
class HypothesisResultAPI(Resource):
    def __init__(self, db):
        self.db = db

    @token_required
    def get(self, hypothesis_id, current_user_id):

        if not hypothesis_id:
            return {"message": "Hypothesis ID is required"}, 400
        
        hypothesis = self.db.get_hypotheses(current_user_id, hypothesis_id)
        if not hypothesis:
            return {"message": "Hypothesis not found or access denied."}, 404

        # Check if enrichment is complete
        required_fields = ['enrich_id', 'go_id', 'summary', 'graph']
        is_complete = all(field in hypothesis for field in required_fields)
        # Get task history
        task_history = status_tracker.get_history(hypothesis_id)


        if is_complete:
            return {
                'id': hypothesis_id,
                'variant': hypothesis['variant_id'],
                'phenotype': hypothesis['phenotype'],
                "status": "completed",
                "result": hypothesis,
                "task_history": task_history
            }, 200

        latest_state = status_tracker.get_latest_state(hypothesis_id)

        status_data = {
            'id': hypothesis_id,
            'variant': hypothesis['variant_id'],
            'phenotype': hypothesis['phenotype'],
            'status': 'in_progress',
            'task_history': task_history,
        }

        # Check for failed state
        if latest_state and latest_state.get('state') == 'failed':
            status_data['status'] = 'failed'
            status_data['error'] = latest_state.get('error')

        return status_data, 200

class ChatAPI(Resource):
    def __init__(self, llm):
        self.llm = llm

    def post(self):
        query = request.form.get('query')
        graph = request.form.get('graph')
        response = self.llm.chat(query, graph)
        response = {"response": response}
        return response

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
                
                response_data.update({
                    'status': 'in_progress',
                    'progress': progress,
                    'current_task': latest_state['task'] if latest_state else None,
                    'error': latest_state.get('error') if latest_state and latest_state.get('state') == TaskState.FAILED else None
                })
            
            logger.info(f"Emitting task_update: {response_data}")
            socketio.emit('task_update', response_data, room=room)
            return {"status": "subscribed", "room": room}
            
        except Exception as e:
            logger.error(f"Error in handle_subscribe: {str(e)}")
            return {"error": str(e)}, 500


# def init_socket_handlers(db_instance):
#     @socketio.on('connect')
#     @socket_token_required
#     def handle_connect():
#         logger.info("Client connected")
#         print("Client connected")
#         return True

#     @socketio.on('disconnect')
#     @socket_token_required
#     def handle_disconnect():
#         logger.info("Client disconnected")

#     @socketio.on('subscribe_hypothesis')
#     @socket_token_required
#     def handle_subscribe(data, current_user_id):
#         try:
#             logger.info(f"Received subscribe request: {data}")
#             print(f"Received subscribe request: {data}")
#             # Handle both string and dict input
#             if isinstance(data, str):
#                 try:
#                     data = json.loads(data)
#                 except json.JSONDecodeError:
#                     logger.error(f"Invalid JSON string: {data}")
#                     return {'error': 'Invalid JSON format'}, 400
            
#             # Validate input
#             if not isinstance(data, dict) or 'hypothesis_id' not in data:
#                 logger.error(f"Invalid data format: {data}")
#                 return {'error': 'Expected format: {"hypothesis_id": "value"}'}, 400
                
#             hypothesis_id = data.get('hypothesis_id')
#             if not hypothesis_id:
#                 logger.error("Missing hypothesis_id")
#                 return {'error': 'hypothesis_id is required'}, 400
            
#             response_data = {
#                 'hypothesis_id': hypothesis_id,
#                 'timestamp': datetime.now(timezone.utc).isoformat(timespec='milliseconds') + "Z",
#             }
                
                
#             # Join a room specific to this hypothesis
#             room = f"hypothesis_{hypothesis_id}"    
#             join_room(room)
#             logger.info(f"Joined room: {room}")
            
#             # Get hypothesis data
#             hypothesis = db_instance.get_hypotheses(current_user_id, hypothesis_id)
#             if not hypothesis:
#                 logger.error(f"Hypothesis not found: {hypothesis_id}")
#                 raise ValueError("Hypothesis not found or access denied")

#             # Get task history
#             task_history = status_tracker.get_history(hypothesis_id)
#             response_data['task_history'] = task_history
                
#             # Check if hypothesis is complete
#             required_fields = ['enrich_id', 'go_id', 'summary', 'graph']
#             is_complete = all(field in hypothesis for field in required_fields)  
            
#             if is_complete:
#                 response_data.update({
#                     'status': 'completed',
#                     'result': hypothesis,
#                     'progress': 100
#                 })
#             else:
#                 latest_state = status_tracker.get_latest_state(hypothesis_id)
#                 progress = status_tracker.calculate_progress(task_history)
                
#                 response_data.update({
#                     'status': 'in_progress',
#                     'progress': progress,
#                     'current_task': latest_state['task'] if latest_state else None,
#                     'error': latest_state.get('error') if latest_state and latest_state.get('state') == TaskState.FAILED else None
#                 })
            
#             logger.info(f"Emitting task_update: {response_data}")
#             socketio.emit('task_update', response_data, room=room)
#             return {"status": "subscribed", "room": room}
            
#         except Exception as e:
#             logger.error(f"Error in handle_subscribe: {str(e)}")
#             return {"error": str(e)}, 500