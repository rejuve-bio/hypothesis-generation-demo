from bson.objectid import ObjectId
from datetime import datetime, timezone
import os
import json
import requests as _req
from datetime import timedelta

from loguru import logger
from .base_handler import BaseHandler


class ProjectHandler(BaseHandler):
    """Handler for project CRUD operations"""
    
    def __init__(self, uri, db_name):
        super().__init__(uri, db_name)
        self.projects_collection = self.db['projects']
        # Initialize collections needed for cascade deletion
        self.credible_sets_collection = self.db['credible_sets']
        self.hypothesis_collection = self.db['hypotheses']
        self.task_updates_collection = self.db['task_updates']
        self.summary_collection = self.db['summary']
        self.enrich_collection = self.db['enrich']
        self.analysis_results_collection = self.db['analysis_results']
        self.file_metadata_collection = self.db['file_metadata']
    
    def create_project(self, user_id, name, gwas_file_id, phenotype,population, ref_genome, analysis_parameters=None):
        """Create a new project"""
        project_data = {
            'user_id': user_id,
            'name': name,
            'phenotype': phenotype,
            'created_at': datetime.now(timezone.utc),
            'updated_at': datetime.now(timezone.utc),
            'status': 'active',
            'gwas_file_id': gwas_file_id,
            'population': population,
            'ref_genome': ref_genome,
            'analysis_parameters': analysis_parameters or {}
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
                project['id'] = str(project['_id'])
                del project['_id']  
            return project
        
        projects = list(self.projects_collection.find(query))
        for project in projects:
            project['id'] = str(project['_id'])
            del project['_id']
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
        """Delete a single project and all associated data"""
        try:
            return self.bulk_delete_projects(user_id, [project_id])
        except Exception as e:
            logger.error(f"Error deleting project {project_id}: {str(e)}")
            return False

    def bulk_delete_projects(self, user_id, project_ids):
        """Delete multiple projects and all associated data"""
        try:
            if not project_ids or not isinstance(project_ids, list):
                return False
            
            deleted_count = 0
            errors = []
            
            for project_id in project_ids:
                try:
                    # Verify project exists and belongs to user
                    project = self.projects_collection.find_one({
                        '_id': ObjectId(project_id),
                        'user_id': user_id
                    })
                    
                    if not project:
                        errors.append(f"Project {project_id} not found or access denied")
                        continue
                    
                    # Delete all associated data
                    self._delete_project_data(user_id, project_id)
                    
                    # Delete the project itself
                    result = self.projects_collection.delete_one({
                        '_id': ObjectId(project_id),
                        'user_id': user_id
                    })
                    
                    if result.deleted_count > 0:
                        deleted_count += 1
                        logger.info(f"Successfully deleted project {project_id} and all associated data")
                    else:
                        errors.append(f"Failed to delete project {project_id}")
                        
                except Exception as e:
                    logger.error(f"Error deleting project {project_id}: {str(e)}")
                    errors.append(f"Error deleting project {project_id}: {str(e)}")
            
            return {
                'deleted_count': deleted_count,
                'total_requested': len(project_ids),
                'errors': errors,
                'success': deleted_count == len(project_ids)
            }
            
        except Exception as e:
            logger.error(f"Error in bulk project deletion: {str(e)}")
            return False

    def _delete_project_data(self, user_id, project_id):
        """Delete all data associated with a project"""
        try:
            # 1. Delete credible sets
            credible_sets_result = self.credible_sets_collection.delete_many({
                'user_id': user_id,
                'project_id': project_id
            })
            logger.info(f"Deleted {credible_sets_result.deleted_count} credible sets for project {project_id}")
            
            # 2. Delete hypotheses and get their IDs for cascade deletion
            hypotheses = list(self.hypothesis_collection.find({
                'user_id': user_id,
                'project_id': project_id
            }))
            hypothesis_ids = [h.get('id') for h in hypotheses if h.get('id')]
            
            hypotheses_result = self.hypothesis_collection.delete_many({
                'user_id': user_id,
                'project_id': project_id
            })
            logger.info(f"Deleted {hypotheses_result.deleted_count} hypotheses for project {project_id}")
            
            # 3. Delete task updates for the hypotheses
            if hypothesis_ids:
                task_updates_result = self.task_updates_collection.delete_many({
                    'hypothesis_id': {'$in': hypothesis_ids}
                })
                logger.info(f"Deleted {task_updates_result.deleted_count} task updates for project {project_id}")
                
                # 4. Delete summaries for the hypotheses
                summaries_result = self.summary_collection.delete_many({
                    'user_id': user_id,
                    'hypothesis_id': {'$in': hypothesis_ids}
                })
                logger.info(f"Deleted {summaries_result.deleted_count} summaries for project {project_id}")
            
            # 5. Delete enrichment data
            enrich_result = self.enrich_collection.delete_many({
                'user_id': user_id,
                'project_id': project_id
            })
            logger.info(f"Deleted {enrich_result.deleted_count} enrichment records for project {project_id}")
            
            # 6. Delete analysis results
            analysis_result = self.analysis_results_collection.delete_many({
                'user_id': user_id,
                'project_id': project_id
            })
            logger.info(f"Deleted {analysis_result.deleted_count} analysis results for project {project_id}")
            
            # 7. Get file metadata for deletion
            project = self.projects_collection.find_one({
                '_id': ObjectId(project_id),
                'user_id': user_id
            })
            
            if project and project.get('gwas_file_id'):
                file_meta = self.file_metadata_collection.find_one({
                    '_id': ObjectId(project['gwas_file_id']),
                    'user_id': user_id
                })
                
                if file_meta:
                    # Delete physical file if it exists
                    file_path = file_meta.get('file_path')
                    if file_path and os.path.exists(file_path):
                        # Only delete files from uploads directory
                        if 'uploads' in file_path:
                            try:
                                os.remove(file_path)
                                logger.info(f"Deleted uploaded file: {file_path}")
                            except Exception as file_e:
                                logger.warning(f"Could not delete uploaded file {file_path}: {file_e}")
                        else:
                            logger.info(f"Skipping deletion of predefined file: {file_path}")
                    
                    # Delete file metadata
                    self.file_metadata_collection.delete_one({
                        '_id': ObjectId(project['gwas_file_id']),
                        'user_id': user_id
                    })
                    logger.info(f"Deleted file metadata for project {project_id}")
            
            # 8. Delete analysis state files
            analysis_state_path = self.get_analysis_state_path(user_id, project_id)
            if os.path.exists(analysis_state_path):
                try:
                    os.remove(analysis_state_path)
                    logger.info(f"Deleted analysis state file: {analysis_state_path}")
                except Exception as state_e:
                    logger.warning(f"Could not delete analysis state file {analysis_state_path}: {state_e}")
            
            # 9. Delete analysis results directory
            analysis_dir = self.get_project_analysis_path(user_id, project_id)
            if os.path.exists(analysis_dir):
                try:
                    import shutil
                    shutil.rmtree(analysis_dir)
                    logger.info(f"Deleted analysis directory: {analysis_dir}")
                except Exception as dir_e:
                    logger.warning(f"Could not delete analysis directory {analysis_dir}: {dir_e}")
                    
        except Exception as e:
            logger.error(f"Error deleting project data for {project_id}: {str(e)}")
            raise
    
    def get_project_analysis_path(self, user_id, project_id):
        """Get the analysis path for a project"""
        return os.path.abspath(f"data/projects/{user_id}/{project_id}/analysis")

    def get_analysis_state_path(self, user_id, project_id):
        """Get the analysis state file path"""
        return f"data/states/{user_id}/{project_id}/analysis_state.json"

    def save_analysis_state(self, user_id, project_id, state_data):
        """Save analysis state to file"""
        state_path = self.get_analysis_state_path(user_id, project_id)
        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        data = {**state_data, "state_updated_at": datetime.now(timezone.utc).isoformat()}
        with open(state_path, 'w') as f:
            json.dump(data, f, default=str)

    def load_analysis_state(self, user_id, project_id):
        """Load analysis state from file"""
        state_path = self.get_analysis_state_path(user_id, project_id)
        if not os.path.exists(state_path):
            return None
        with open(state_path, 'r') as f:
            state = json.load(f)
        if state.get("status") == "Running":
            reconciled = self._reconcile_running_state(state)
            if reconciled["status"] != "Running":
                logger.info(
                    f"[ProjectHandler] Reconciled stale Running state for project "
                    f"{project_id} → {reconciled['status']}"
                )
                self.save_analysis_state(user_id, project_id, reconciled)
                return reconciled
        return state

    def _reconcile_running_state(self, state: dict) -> dict:
        """
        Verify whether a Running state is still running.
        """
        flow_run_id = state.get("flow_run_id")
        if flow_run_id:
            prefect_url = os.getenv("PREFECT_API_URL", "http://prefect-service:4200/api")
            try:
                resp = _req.get(f"{prefect_url}/flow_runs/{flow_run_id}", timeout=3)
                if resp.status_code == 200:
                    prefect_state_type = resp.json().get("state", {}).get("type", "")
                    terminal_failed = {"FAILED", "CRASHED"}
                    terminal_ok = {"COMPLETED"}
                    terminal_stopped = {"CANCELLED"}
                    if prefect_state_type in terminal_failed:
                        return {**state, "status": "Failed",
                                "message": f"Pipeline {prefect_state_type.lower()} (confirmed via Prefect)"}
                    if prefect_state_type in terminal_ok:
                        return {**state, "status": "Completed",
                                "message": "Pipeline completed (confirmed via Prefect)"}
                    if prefect_state_type in terminal_stopped:
                        return {**state, "status": "Failed",
                                "message": "Pipeline was cancelled"}
                    return state
            except Exception as prefect_exc:
                logger.warning(f"[ProjectHandler] Could not reach Prefect API to reconcile run {flow_run_id}: {prefect_exc}")

        # time-based staleness check
        staleness_minutes = int(os.getenv("ANALYSIS_STALENESS_MINUTES", "360"))
        updated_at_str = state.get("state_updated_at")
        if updated_at_str:
            try:
                updated_at = datetime.fromisoformat(updated_at_str.replace("Z", "+00:00"))
                age = datetime.now(timezone.utc) - updated_at
                if age > timedelta(minutes=staleness_minutes):
                    hours = int(age.total_seconds() / 3600)
                    return {
                        **state,
                        "status": "Failed",
                        "message": (
                            f"Pipeline status unconfirmed: no update for {hours}h "
                            f"(process may have been terminated)"
                        ),
                    }
            except Exception:
                pass

        return state