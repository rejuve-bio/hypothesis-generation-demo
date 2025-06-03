from datetime import datetime, timezone
from pymongo import MongoClient
from werkzeug.security import generate_password_hash, check_password_hash
from bson.objectid import ObjectId
import os
import json
from uuid import uuid4

class Database:
    def __init__(self, uri, db_name):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        
        # Existing collections
        self.users_collection = self.db['users']
        self.hypothesis_collection = self.db['hypotheses']
        self.enrich_collection = self.db['enrich']
        self.task_updates_collection = self.db['task_updates'] 
        self.summary_collection = self.db['summaries']
        self.processing_collection = self.db['processing_status']
        
        # New collections for project-based structure
        self.projects_collection = self.db['projects']
        self.file_metadata_collection = self.db['file_metadata']
        self.analysis_results_collection = self.db['analysis_results']
        self.credible_sets_collection = self.db['credible_sets']

    # ==================== USER METHODS ====================
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

    # ==================== PROJECT METHODS ====================
    def create_project(self, user_id, name, gwas_file_id):
        """Create a new project"""
        project_data = {
            'user_id': user_id,
            'name': name,
            'created_at': datetime.now(timezone.utc),
            'updated_at': datetime.now(timezone.utc),
            'status': 'active',
            'gwas_file_id': gwas_file_id
        }
        result = self.projects_collection.insert_one(project_data)
        return str(result.inserted_id)

    def get_projects(self, user_id, project_id=None):
        """Get projects for a user"""
        query = {'user_id': user_id}
        if project_id:
            query['_id'] = ObjectId(project_id)
            project = self.projects_collection.find_one(query)
            if project:
                project['_id'] = str(project['_id'])
            return project
        
        projects = list(self.projects_collection.find(query))
        for project in projects:
            project['_id'] = str(project['_id'])
        return projects

    def update_project(self, project_id, data):
        """Update project data"""
        data['updated_at'] = datetime.now(timezone.utc)
        result = self.projects_collection.update_one(
            {'_id': ObjectId(project_id)},
            {'$set': data}
        )
        return result.matched_count > 0

    def delete_project(self, user_id, project_id):
        """Delete a project"""
        result = self.projects_collection.delete_one({
            '_id': ObjectId(project_id),
            'user_id': user_id
        })
        return result.deleted_count > 0

    # ==================== FILE METADATA METHODS ====================
    def create_file_metadata(self, user_id, filename, original_filename, file_path, file_type, file_size, md5_hash=None):
        """Create file metadata entry"""
        file_data = {
            'user_id': user_id,
            'filename': filename,
            'original_filename': original_filename,
            'file_path': file_path,
            'file_type': file_type,
            'file_size': file_size,
            'upload_date': datetime.now(timezone.utc),
            'md5_hash': md5_hash
        }
        result = self.file_metadata_collection.insert_one(file_data)
        return str(result.inserted_id)

    def get_file_metadata(self, user_id, file_id=None):
        """Get file metadata"""
        query = {'user_id': user_id}
        if file_id:
            query['_id'] = ObjectId(file_id)
            file_meta = self.file_metadata_collection.find_one(query)
            if file_meta:
                file_meta['_id'] = str(file_meta['_id'])
            return file_meta
        
        files = list(self.file_metadata_collection.find(query))
        for file_meta in files:
            file_meta['_id'] = str(file_meta['_id'])
        return files

    def delete_file_metadata(self, user_id, file_id):
        """Delete file metadata"""
        result = self.file_metadata_collection.delete_one({
            '_id': ObjectId(file_id),
            'user_id': user_id
        })
        return result.deleted_count > 0

    # ==================== ANALYSIS RESULTS METHODS ====================
    def create_analysis_result(self, project_id, population, gene_types_identified, result_path=None):
        """Create analysis result entry"""
        analysis_data = {
            'project_id': project_id,
            'population': population,
            'analysis_date': datetime.now(timezone.utc),
            'status': 'completed',
            'gene_types_identified': gene_types_identified,
            'result_path': result_path
        }
        result = self.analysis_results_collection.insert_one(analysis_data)
        return str(result.inserted_id)

    def get_analysis_results(self, project_id, analysis_id=None):
        """Get analysis results for a project"""
        query = {'project_id': project_id}
        if analysis_id:
            query['_id'] = ObjectId(analysis_id)
            analysis = self.analysis_results_collection.find_one(query)
            if analysis:
                analysis['_id'] = str(analysis['_id'])
            return analysis
        
        results = list(self.analysis_results_collection.find(query))
        for analysis in results:
            analysis['_id'] = str(analysis['_id'])
        return results

    def update_analysis_result(self, analysis_id, data):
        """Update analysis result"""
        result = self.analysis_results_collection.update_one(
            {'_id': ObjectId(analysis_id)},
            {'$set': data}
        )
        return result.matched_count > 0

    # ==================== CREDIBLE SETS METHODS ====================
    def create_credible_set(self, analysis_id, gene_type, data):
        """Create credible set entry"""
        credible_set_data = {
            'analysis_id': analysis_id,
            'gene_type': gene_type,
            'data': data,
            'created_at': datetime.now(timezone.utc)
        }
        result = self.credible_sets_collection.insert_one(credible_set_data)
        return str(result.inserted_id)

    def get_credible_sets(self, analysis_id, gene_type=None, credible_set_id=None):
        """Get credible sets for an analysis"""
        if credible_set_id:
            credible_set = self.credible_sets_collection.find_one({'_id': ObjectId(credible_set_id)})
            if credible_set:
                credible_set['_id'] = str(credible_set['_id'])
            return credible_set
        
        query = {'analysis_id': analysis_id}
        if gene_type:
            query['gene_type'] = gene_type
        
        credible_sets = list(self.credible_sets_collection.find(query))
        for cs in credible_sets:
            cs['_id'] = str(cs['_id'])
        return credible_sets

    def check_credible_set_exists(self, analysis_id, gene_type):
        """Check if credible set exists for analysis and gene type"""
        return self.credible_sets_collection.find_one({
            'analysis_id': analysis_id,
            'gene_type': gene_type
        }) is not None

    def delete_credible_set(self, credible_set_id):
        """Delete credible set"""
        result = self.credible_sets_collection.delete_one({'_id': ObjectId(credible_set_id)})
        return result.deleted_count > 0

    # ==================== UPDATED ENRICHMENT METHODS ====================
    def create_enrich_v2(self, user_id, project_id, credible_set_id, variant, phenotype, causal_gene, go_terms, causal_graph):
        """Create enrichment entry with project and credible set references"""
        enrich_data = {
            'id': str(uuid4()),
            'user_id': user_id,
            'project_id': project_id,
            'credible_set_id': credible_set_id,
            'variant': variant,
            'phenotype': phenotype,
            'causal_gene': causal_gene,
            'GO_terms': go_terms,
            'causal_graph': causal_graph,
            'created_at': datetime.now(timezone.utc)
        }
        result = self.enrich_collection.insert_one(enrich_data)
        return enrich_data['id']

    def get_enrich_by_credible_set(self, user_id, credible_set_id, variant, phenotype):
        """Check if enrichment exists for credible set, variant, and phenotype"""
        return self.enrich_collection.find_one({
            'user_id': user_id,
            'credible_set_id': credible_set_id,
            'variant': variant,
            'phenotype': phenotype
        })

    # ==================== UPDATED HYPOTHESIS METHODS ====================
    def create_hypothesis_v2(self, user_id, project_id, enrich_id, go_id, variant, phenotype, causal_gene, graph, summary):
        """Create hypothesis with project reference"""
        hypothesis_data = {
            'id': str(uuid4()),
            'user_id': user_id,
            'project_id': project_id,
            'enrich_id': enrich_id,
            'go_id': go_id,
            'variant': variant,
            'phenotype': phenotype,
            'causal_gene': causal_gene,
            'graph': graph,
            'summary': summary,
            'biological_context': '',
            'status': 'completed',
            'task_history': [],
            'created_at': datetime.now(timezone.utc),
            'updated_at': datetime.now(timezone.utc)
        }
        result = self.hypothesis_collection.insert_one(hypothesis_data)
        return hypothesis_data['id']

    def get_hypotheses_by_project(self, user_id, project_id):
        """Get all hypotheses for a project"""
        hypotheses = list(self.hypothesis_collection.find({
            'user_id': user_id,
            'project_id': project_id
        }))
        for hypothesis in hypotheses:
            hypothesis['_id'] = str(hypothesis['_id'])
        return hypotheses

    # ==================== UTILITY METHODS ====================
    def get_project_analysis_path(self, user_id, project_id):
        """Get the analysis path for a project"""
        return f"data/projects/{user_id}/{project_id}/analysis"

    def get_analysis_state_path(self, user_id, project_id):
        """Get the analysis state file path"""
        return f"data/states/{user_id}/{project_id}/analysis_state.json"

    def save_analysis_state(self, user_id, project_id, state_data):
        """Save analysis state to file"""
        state_path = self.get_analysis_state_path(user_id, project_id)
        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        
        with open(state_path, 'w') as f:
            json.dump(state_data, f, default=str)

    def load_analysis_state(self, user_id, project_id):
        """Load analysis state from file"""
        state_path = self.get_analysis_state_path(user_id, project_id)
        if os.path.exists(state_path):
            with open(state_path, 'r') as f:
                return json.load(f)
        return None

    # ==================== CLEANUP METHODS (COMMENTED OUT) ====================
    # def cleanup_old_analysis_states(self, days_old=14):
    #     """Clean up analysis state files older than specified days"""
    #     import time
    #     cutoff_time = time.time() - (days_old * 24 * 60 * 60)
    #     
    #     states_dir = "data/states"
    #     if not os.path.exists(states_dir):
    #         return
    #     
    #     for user_dir in os.listdir(states_dir):
    #         user_path = os.path.join(states_dir, user_dir)
    #         if not os.path.isdir(user_path):
    #             continue
    #             
    #         for project_dir in os.listdir(user_path):
    #             project_path = os.path.join(user_path, project_dir)
    #             if not os.path.isdir(project_path):
    #                 continue
    #                 
    #             state_file = os.path.join(project_path, "analysis_state.json")
    #             if os.path.exists(state_file):
    #                 if os.path.getmtime(state_file) < cutoff_time:
    #                     os.remove(state_file)
    #                     # Remove empty directories
    #                     try:
    #                         os.rmdir(project_path)
    #                         os.rmdir(user_path)
    #                     except OSError:
    #                         pass  # Directory not empty

    # def cleanup_old_project_files(self, days_old=14):
    #     """Clean up project analysis files older than specified days"""
    #     import time
    #     cutoff_time = time.time() - (days_old * 24 * 60 * 60)
    #     
    #     projects_dir = "data/projects"
    #     if not os.path.exists(projects_dir):
    #         return
    #     
    #     for user_dir in os.listdir(projects_dir):
    #         user_path = os.path.join(projects_dir, user_dir)
    #         if not os.path.isdir(user_path):
    #             continue
    #             
    #         for project_dir in os.listdir(user_path):
    #             project_path = os.path.join(user_path, project_dir)
    #             if not os.path.isdir(project_path):
    #                 continue
    #                 
    #             # Check if project is older than cutoff
    #             if os.path.getmtime(project_path) < cutoff_time:
    #                 import shutil
    #                 shutil.rmtree(project_path)

    # ==================== EXISTING METHODS (KEPT FOR BACKWARD COMPATIBILITY) ====================
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
    
    def create_summary(self, user_id, hypothesis_id, summary_data):
        summary_doc = {
            "user_id": user_id,
            "hypothesis_id": hypothesis_id,
            "summary": summary_data,
        }
        result = self.summary_collection.insert_one(summary_doc)
        
        return {
            "summary_id": str(result.inserted_id),
            "user_id": user_id,
            "hypothesis_id": hypothesis_id,
            "summary": summary_data
        }, 201
    
    def check_summary(self, user_id, hypothesis_id):
        query = {}
        
        if user_id:
            query['user_id'] = user_id
        if hypothesis_id:
            query['hypothesis_id'] = hypothesis_id
        
        summary = self.summary_collection.find_one(query)

        return summary

    def check_global_summary(self, variant_input):
        query = {"variant": variant_input}
        summary = self.summary_collection.find_one(query)
        if summary:
            summary["_id"] = str(summary["_id"])
        return summary

    def create_global_summary(self, variant_input, summary_data):
        summary_doc = {
            "variant": variant_input,
            "summary": summary_data,
        }
        result = self.summary_collection.insert_one(summary_doc)
        return {
            "summary_id": str(result.inserted_id),
            "variant": variant_input,
            "summary": summary_data,
        }
    
    def get_summary(self, user_id, summary_id):
        if not user_id or not summary_id:
            print("Missing user_id or summary_id")
            return None
        
        query = {
        "user_id": user_id,
        "_id": ObjectId(summary_id) 
        }
        summary =  self.summary_collection.find_one(query)
        if summary:
            summary["_id"] = str(summary["_id"])

        return summary
    
    def check_processing_status(self, variant_input):
        return self.processing_collection.find_one({"variant": variant_input})

    def set_processing_status(self, variant_input, status):
        if status:
            self.processing_collection.update_one(
                {"variant": variant_input},
                {"$set": {"status": "processing"}},
                upsert=True
            )
        else:
            self.processing_collection.delete_one({"variant": variant_input})