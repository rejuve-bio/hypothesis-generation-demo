from datetime import datetime, timezone
from pymongo import MongoClient
from werkzeug.security import generate_password_hash, check_password_hash
from bson.objectid import ObjectId

class Database:
    def __init__(self, uri, db_name):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.users_collection = self.db['users']
        self.hypothesis_collection = self.db['hypotheses']
        self.enrich_collection = self.db['enrich']
        self.task_updates_collection = self.db['task_updates'] 

    def create_user(self, email, password):
        if self.users_collection.find_one({'email': email}):
            return {'message': 'User already exists'}, 400
        
        hashed_password = generate_password_hash(password)
        self.users_collection.insert_one({'email': email, 'password': hashed_password})
        return {'message': 'User created successfully'}, 201

    def verify_user(self, email, password):
        user = self.users_collection.find_one({'email': email})
        if user and check_password_hash(user['password'], password):
            return {'message': 'Logged in successfully', 'user_id': str(user['_id'])}, 200
        return {'message': 'Invalid credentials'}, 401

    def create_hypothesis(self, user_id, data):
        data['user_id'] = user_id
        result = self.hypothesis_collection.insert_one(data)
        return {'message': 'Hypothesis created', 'id': str(result.inserted_id)}, 201
    
    def create_enrich(self, user_id, data):
        data['user_id'] = user_id
        result = self.enrich_collection.insert_one(data)
        return {'message': 'Enrichment created', 'id': str(result.inserted_id)}, 201

    def get_hypotheses(self, user_id=None, hypothesis_id=None):
        query = {}
        
        if user_id:
            query['user_id'] = user_id
        if hypothesis_id:
            query['id'] = hypothesis_id
            hypothesis = self.hypothesis_collection.find_one(query)
            if hypothesis:
                hypothesis["_id"] = str(hypothesis["_id"])
            else:
                print("No document found for the given hypothesis id.")
            return hypothesis

        hypotheses = list(self.hypothesis_collection.find(query))
        for hypothesis in hypotheses:
            hypothesis['_id'] = str(hypothesis['_id'])

        return hypotheses if hypotheses else []

    def check_hypothesis(self, user_id=None, enrich_id=None, go_id=None):
        query = {}
        
        if user_id:
            query['user_id'] = user_id
        if enrich_id:
            query['enrich_id'] = enrich_id
        if go_id:
            query['go_id'] = go_id
        
        hypothesis = self.hypothesis_collection.find_one(query)
        
        return hypothesis is not None
    
    def check_enrich(self, user_id=None, phenotype=None, variant_id=None):
        query = {}
        
        if user_id:
            query['user_id'] = user_id
        if phenotype:
            query['phenotype'] = phenotype
        if variant_id:
            query['variant'] = variant_id
        
        enrich = self.enrich_collection.find_one(query)
        
        return enrich is not None


    def get_hypothesis_by_enrich_and_go(self, enrich_id, go_id, user_id=None):
        query = {
            'enrich_id': enrich_id,
            'go_id': go_id,
            'user_id': user_id
        }
        hypothesis = self.hypothesis_collection.find_one(query)
        if hypothesis:
            hypothesis['_id'] = str(hypothesis['_id'])

        return hypothesis

    def get_enrich_by_phenotype_and_variant(self, phenotype, variant_id, user_id=None):
        query = {
            'phenotype': phenotype,
            'variant': variant_id,
            'user_id': user_id
        }
        
        enrich = self.enrich_collection.find_one(query)
        
        if enrich:
            enrich['_id'] = str(enrich['_id'])
        
        return enrich


    def get_enrich(self, user_id=None, enrich_id=None):
        query = {}
        
        if user_id:
            query['user_id'] = user_id
        if enrich_id:
            query['id'] = enrich_id
            enrich = self.enrich_collection.find_one(query)  
            if enrich:
                enrich['_id'] = str(enrich['_id'])
            else:
                print("No document found for the given enrich_id.")
            return enrich

        enriches = list(self.enrich_collection.find(query))
        for enrich in enriches:
            enrich['_id'] = str(enrich['_id'])

        return enriches if enriches else []

    def delete_hypothesis(self, user_id, hypothesis_id):
        result = self.hypothesis_collection.delete_one({'id': hypothesis_id, 'user_id': user_id})
        if result.deleted_count > 0:
            return {'message': 'Hypothesis deleted'}, 200
        return {'message': 'Hypothesis not found or not authorized'}, 404

    def bulk_delete_hypotheses(self, user_id, hypothesis_ids):
        """
        Delete multiple hypotheses by their IDs for a specific user.
        """
        if not hypothesis_ids or not isinstance(hypothesis_ids, list):
            return {'message': 'Invalid hypothesis_ids format. Expected a non-empty list.'}, 400

        results = {'successful': [], 'failed': []}

        # Bulk delete
        bulk_result = self.hypothesis_collection.delete_many({
            'id': {'$in': hypothesis_ids}, 
            'user_id': user_id
        })

        # Check if all were deleted
        if bulk_result.deleted_count == len(hypothesis_ids):
            return {
                'message': f'All {bulk_result.deleted_count} hypotheses deleted successfully',
                'deleted_count': bulk_result.deleted_count,
                'successful': hypothesis_ids,
                'failed': []
            }, 200

        # Identify which ones failed
        deleted_ids = set(hypothesis_ids[:bulk_result.deleted_count])  # Approximate success count
        failed_ids = list(set(hypothesis_ids) - deleted_ids)

        return {
            'message': f"{bulk_result.deleted_count} hypotheses deleted successfully, {len(failed_ids)} failed",
            'deleted_count': bulk_result.deleted_count,
            'successful': list(deleted_ids),
            'failed': [{'id': h_id, 'reason': 'Not found or not authorized'} for h_id in failed_ids]
        }, 207 if deleted_ids else 404  # Use 207 for partial success
    
    
    def delete_enrich(self, user_id, enrich_id):
        result = self.enrich_collection.delete_one({'id': enrich_id, 'user_id': user_id})
        if result.deleted_count > 0:
            return {'message': 'Enrich deleted'}, 200
        return {'message': 'Enrich not found or not authorized'}, 404
    

    def get_task_history(self, hypothesis_id):
        task_history = list(self.task_updates_collection.find({"hypothesis_id": hypothesis_id}))
        
        for update in task_history:
            update["_id"] = str(update["_id"])
        return task_history
    
    def get_latest_task_state(self, hypothesis_id):
        task_history = list(self.task_updates_collection.find({"hypothesis_id": hypothesis_id}).sort("timestamp", -1).limit(1))
        if task_history:
            return task_history[0]
        return None

    def update_hypothesis(self, hypothesis_id, data):
        # Remove _id if present in data to avoid modification errors
        if '_id' in data:
            del data['_id']
        
        result = self.hypothesis_collection.update_one(
            {'id': hypothesis_id},
            {'$set': data}
        )
        
        if result.matched_count > 0:
            return {'message': 'Hypothesis updated successfully'}, 200
        return {'message': 'Hypothesis not found'}, 404

    def save_task_history(self, hypothesis_id, task_history):
        """Save complete task history to DB"""
        # Delete existing history first
        self.task_updates_collection.delete_many({"hypothesis_id": hypothesis_id})
        
        # Insert new history as a batch
        if task_history:
            self.task_updates_collection.insert_many([
                {**update, "hypothesis_id": hypothesis_id}
                for update in task_history
            ])
    
    def get_hypothesis_by_phenotype_and_variant(self, user_id, phenotype, variant):
        return self.hypothesis_collection.find_one({
            'user_id': user_id,
            'phenotype': phenotype,
            'variant': variant
        })

    def get_hypothesis_by_enrich(self, user_id, enrich_id):
        return self.hypothesis_collection.find_one({
            'user_id': user_id,
            'enrich_id': enrich_id
        })