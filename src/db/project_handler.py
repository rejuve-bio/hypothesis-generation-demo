from bson.objectid import ObjectId
from datetime import datetime, timezone, timedelta
import os
import json
import requests as _req
from typing import Callable
from uuid import uuid4

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
    
    def create_project(self, user_id, name, gwas_file_id, phenotype,population, ref_genome, analysis_parameters=None, user_email=None):
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
            'analysis_parameters': analysis_parameters or {},
            "user_email": user_email,
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

                    if project.get("is_template") or (
                        project.get("is_demo") and not project.get("source_template_id")
                    ):
                        errors.append(
                            f"Project {project_id} is a demo template and cannot be deleted"
                        )
                        continue
                    
                    # Delete all associated data
                    self._delete_project_data(user_id, project_id)
                    
                    # Delete the project itself
                    result = self.projects_collection.delete_one({
                        '_id': ObjectId(project_id),
                        'user_id': user_id
                    })
                    
                    if result.deleted_count > 0:
                        if project.get("source_template_id"):
                            fork_result = self.db["user_demo_forks"].delete_one(
                                {
                                    "user_id": user_id,
                                    "forked_project_id": project_id,
                                }
                            )
                            if fork_result.deleted_count:
                                logger.info(
                                    f"Cleared demo fork mapping for project {project_id}"
                                )
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
            # 0. Cancel any running Prefect flow before wiping data 
            state = self.load_analysis_state(user_id, project_id)
            if state and state.get("flow_run_id") and state.get("status") == "Running":
                prefect_url = os.getenv("PREFECT_API_URL", "http://prefect-service:4200/api")
                try:
                    resp = _req.post(
                        f"{prefect_url}/flow_runs/{state['flow_run_id']}/set_state",
                        json={"state": {"type": "CANCELLED"}, "force": True},
                        timeout=3,
                    )
                    if resp.status_code in (200, 201):
                        logger.info(f"Cancelled Prefect flow run {state['flow_run_id']} for project {project_id}")
                    else:
                        logger.warning(f"Could not cancel flow run {state['flow_run_id']}: {resp.status_code}")
                except Exception as cancel_e:
                    logger.warning(f"Failed to cancel Prefect flow run for {project_id}: {cancel_e}")

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

            tissue_result = self.db["tissue_selections"].delete_many({
                "user_id": user_id,
                "project_id": project_id,
            })
            logger.info(
                f"Deleted {tissue_result.deleted_count} tissue selections for project {project_id}"
            )
            
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

    def _copy_collection_docs(
        self,
        collection,
        query: dict,
        target_user_id: str | None = None,
        new_project_id: str | None = None,
        *,
        extra_fields: dict | None = None,
        transform: Callable[[dict], None] | None = None,
    ) -> list[dict]:
        docs = list(collection.find(query))
        for doc in docs:
            doc.pop("_id", None)
            if target_user_id is not None:
                doc["user_id"] = target_user_id
            if new_project_id is not None:
                doc["project_id"] = new_project_id
            if extra_fields:
                doc.update(extra_fields)
            if transform:
                transform(doc)
        if docs:
            collection.insert_many(docs)
        return docs

    def fork_project_from_template(
        self,
        template_user_id: str,
        template_project_id: str,
        target_user_id: str,
        *,
        new_name: str | None = None,
        template_slug: str | None = None,
    ) -> str:
        """Copy a demo template project into the target user's account."""
        source = self.projects_collection.find_one(
            {"_id": ObjectId(template_project_id), "user_id": template_user_id}
        )
        if not source:
            raise ValueError(
                f"Template project {template_project_id} not found for user {template_user_id}"
            )

        now = datetime.now(timezone.utc)
        new_id = ObjectId()
        new_project_id = str(new_id)

        gwas_file_id = source.get("gwas_file_id")
        if gwas_file_id:
            file_meta = self.file_metadata_collection.find_one(
                {"_id": ObjectId(gwas_file_id)}
            )
            if file_meta and file_meta.get("user_id") != target_user_id:
                cloned_file = {k: v for k, v in file_meta.items() if k != "_id"}
                cloned_file["user_id"] = target_user_id
                cloned_file["upload_date"] = now
                gwas_file_id = str(
                    self.file_metadata_collection.insert_one(cloned_file).inserted_id
                )

        new_project = {k: v for k, v in source.items() if k != "_id"}
        new_project.update(
            {
                "_id": new_id,
                "user_id": target_user_id,
                "gwas_file_id": gwas_file_id,
                "name": new_name or f"{source.get('name', 'Project')} (from sample)",
                "is_template": False,
                "is_demo": False,
                "source_template_id": template_project_id,
                "source_template_slug": template_slug,
                "created_at": now,
                "updated_at": now,
            }
        )
        new_project.pop("demo_slug", None)
        self.projects_collection.insert_one(new_project)

        project_query = {"user_id": template_user_id, "project_id": template_project_id}
        self._copy_collection_docs(
            self.credible_sets_collection,
            project_query,
            target_user_id,
            new_project_id,
        )
        self._copy_collection_docs(
            self.analysis_results_collection,
            project_query,
            target_user_id,
            new_project_id,
        )
        self._copy_collection_docs(
            self.db["tissue_selections"],
            project_query,
            target_user_id,
            new_project_id,
            transform=lambda doc: doc.update(
                {"id": str(uuid4()), "created_at": now}
            ),
        )

        run_id_map: dict[str, str] = {}

        def _remap_run(doc: dict) -> None:
            old_run_id = doc["id"]
            new_run_id = str(uuid4())
            run_id_map[old_run_id] = new_run_id
            doc["id"] = new_run_id
            doc["user_id"] = target_user_id
            doc["project_id"] = new_project_id
            doc["created_at"] = now
            doc["updated_at"] = now

        self._copy_collection_docs(
            self.db["gene_expression_runs"],
            project_query,
            transform=_remap_run,
        )

        if run_id_map:
            old_run_ids = list(run_id_map.keys())

            def _remap_run_scoped(doc: dict) -> None:
                doc["id"] = str(uuid4())
                doc["analysis_run_id"] = run_id_map[doc["analysis_run_id"]]
                doc["created_at"] = now

            run_scoped_query = {"analysis_run_id": {"$in": old_run_ids}}
            self._copy_collection_docs(
                self.db["ldsc_results"],
                run_scoped_query,
                transform=_remap_run_scoped,
            )
            self._copy_collection_docs(
                self.db["tissue_mappings"],
                run_scoped_query,
                transform=_remap_run_scoped,
            )

        source_state_path = self.get_analysis_state_path(template_user_id, template_project_id)
        if os.path.exists(source_state_path):
            with open(source_state_path, "r", encoding="utf-8") as handle:
                state_data = json.load(handle)
            self.save_analysis_state(target_user_id, new_project_id, state_data)

        logger.info(
            f"Forked demo template {template_project_id} to user {target_user_id} "
            f"as project {new_project_id}"
        )
        return new_project_id