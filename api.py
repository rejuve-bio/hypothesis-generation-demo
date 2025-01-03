from flask import Flask, request, jsonify
from flask_restful import Resource, Api, reqparse
from auth import token_required
from datetime import datetime, timedelta
from uuid import uuid4
from flows import enrichment_flow, hypothesis_flow

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
        
        # Run the Prefect flow and return the result
        flow_result = enrichment_flow(self.enrichr, self.llm, self.prolog_query, self.db, current_user_id, phenotype, variant)
        return flow_result[0], flow_result[1]

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
            return hypothesis, 200

        # Fetch all hypotheses for the current user
        hypotheses = self.db.get_hypotheses(user_id=current_user_id)
        return hypotheses, 200

    @token_required
    def post(self, current_user_id):
        enrich_id = request.args.get('id')
        go_id = request.args.get('go')
        
        # Run the Prefect flow and return the result
        flow_result = hypothesis_flow(current_user_id, enrich_id, go_id, self.db, self.prolog_query, self.llm)

        return flow_result[0], flow_result[1]

    
    @token_required
    def delete(self, current_user_id):
        hypothesis_id = request.args.get('hypothesis_id')
        if hypothesis_id:
            result = self.db.delete_hypothesis(current_user_id, hypothesis_id)
            return result, 200
        return {"message": "hypothesis id is required!"}, 400
    
class ChatAPI(Resource):
    def __init__(self, llm):
        self.llm = llm

    def post(self):
        query = request.form.get('query')
        graph = request.form.get('graph')
        response = self.llm.chat(query, graph)
        response = {"response": response}
        return response
