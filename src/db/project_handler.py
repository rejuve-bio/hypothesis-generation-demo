from bson.objectid import ObjectId
from datetime import datetime, timezone, timedelta
import os
import requests as _req
from typing import Callable
from uuid import uuid4

import pymongo
from loguru import logger
from src.pipeline_stages import get_stage, resumable_from
from src.services.prefect_runs import cancel_flow_run
from src.utils import analysis_stage_label, analysis_state_for_public_api, emit_analysis_event
from .base_handler import BaseHandler

ACTIVE_ANALYSIS_STATUSES = frozenset({"Queued", "Running"})
FINISHED_ANALYSIS_STATUSES = frozenset({"Completed", "Failed"})
ANALYSIS_EVENT_TIMEOUT_SECONDS = 3


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
        self.analysis_events_collection = self.db['analysis_events']
        self._create_indexes()

    def _create_indexes(self):
        """Create indexes for chat-thread and analysis event lookups"""
        try:
            self.projects_collection.create_index([('user_id', 1), ('thread_id', 1)])
            self.analysis_events_collection.create_index([('project_id', 1), ('timestamp', 1)])
        except Exception as e:
            logger.warning(f"Could not create indexes (may already exist): {e}")
    
    def create_project(self, user_id, name, gwas_file_id, phenotype,population, ref_genome, analysis_parameters=None, user_email=None, thread_id=None):
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
            "thread_id": thread_id,
        }
        result = self.projects_collection.insert_one(project_data)
        return str(result.inserted_id)

    def get_projects(self, user_id, project_id=None, thread_id=None):
        """Get a user's projects; with thread_id, those created or used in that chat."""
        query = {'user_id': user_id}
        if thread_id and not project_id:
            used_project_ids = [
                ObjectId(pid)
                for pid in self.hypothesis_collection.distinct(
                    'project_id', {'user_id': user_id, 'thread_ids': thread_id}
                )
                if pid and ObjectId.is_valid(pid)
            ]
            query['$or'] = [
                {'thread_id': thread_id},
                {'_id': {'$in': used_project_ids}},
            ]
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
            if state and state.get("flow_run_id") and state.get("status") in ACTIVE_ANALYSIS_STATUSES:
                cancel_flow_run(state["flow_run_id"])

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

            events_result = self.analysis_events_collection.delete_many({
                'user_id': user_id,
                'project_id': project_id
            })
            logger.info(f"Deleted {events_result.deleted_count} analysis events for project {project_id}")
            
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
            
            # 8. Delete analysis results directory
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

    def save_analysis_state(self, user_id, project_id, state_data, expected_updated_at=None):
        """Merge state_data into the project's analysis_state.

        With expected_updated_at, only write if the stored state is still
        active and unchanged since it was read. A write carrying a flow_run_id
        is ignored once another run owns the state or the run was superseded by a restart.
        """
        query = {"_id": ObjectId(project_id), "user_id": user_id}
        if state_data.get("flow_run_id"):
            query["analysis_state.flow_run_id"] = {"$in": [state_data["flow_run_id"], None]}
            query["analysis_state.superseded_flow_run_ids"] = {"$ne": state_data["flow_run_id"]}
        if expected_updated_at is not None:
            query["analysis_state.status"] = {"$in": list(ACTIVE_ANALYSIS_STATUSES)}
            query["analysis_state.updated_at"] = expected_updated_at
        fields = {**state_data, "updated_at": datetime.now(timezone.utc)}
        result = self.projects_collection.update_one(
            query, {"$set": {f"analysis_state.{key}": value for key, value in fields.items()}}
        )
        saved = result.modified_count > 0
        if saved and "status" in state_data:
            self.add_analysis_event(
                user_id,
                project_id,
                stage=state_data.get("stage", "Pipeline"),
                message=analysis_state_for_public_api(state_data)["message"],
                level="error" if state_data["status"] == "Failed" else "info",
                details=state_data.get("error_detail"),
                skip_if_finished=False,
                flow_run_id=state_data.get("flow_run_id"),
            )
        return saved

    def claim_analysis_resume(self, user_id, project_id, previous_state, from_stage):
        """Atomically replace a Failed state with a fresh Queued one; None if another request won."""
        superseded = list(previous_state.get("superseded_flow_run_ids", []))
        if previous_state.get("flow_run_id"):
            superseded.append(previous_state["flow_run_id"])
        state = {
            "status": "Queued",
            "message": f"Restarting from {get_stage(from_stage)['label']}",
            "resumed_from": from_stage,
            "resume_count": previous_state.get("resume_count", 0) + 1,
            "artifacts": previous_state.get("artifacts") or {},
            "superseded_flow_run_ids": superseded,
        }
        result = self.projects_collection.update_one(
            {
                "_id": ObjectId(project_id),
                "user_id": user_id,
                "analysis_state.status": "Failed",
                "analysis_state.flow_run_id": previous_state.get("flow_run_id"),
            },
            {"$set": {"analysis_state": {**state, "updated_at": datetime.now(timezone.utc)}}},
        )
        return state if result.modified_count else None

    def update_analysis_parameters(self, user_id, project_id, changes):
        self.projects_collection.update_one(
            {"_id": ObjectId(project_id), "user_id": user_id},
            {"$set": {f"analysis_parameters.{key}": value for key, value in changes.items()}},
        )

    def has_hypotheses(self, user_id, project_id):
        return self.hypothesis_collection.count_documents(
            {"user_id": user_id, "project_id": project_id}, limit=1
        ) > 0

    def add_analysis_event(self, user_id, project_id, stage, message, level="info",
                           details=None, duration_seconds=None, skip_if_finished=True,
                           flow_run_id=None):
        """Store a timeline event and push it to the project's analysis room; never raises.

        Events arriving after the run has finished are skipped unless skip_if_finished is False.
        Events from a flow run other than the project's current one are dropped.
        """
        try:
            with pymongo.timeout(ANALYSIS_EVENT_TIMEOUT_SECONDS):
                project = self.projects_collection.find_one(
                    {"_id": ObjectId(project_id)},
                    {"analysis_state.status": 1, "analysis_state.flow_run_id": 1},
                )
                state = (project or {}).get("analysis_state") or {}
                if flow_run_id and flow_run_id != state.get("flow_run_id"):
                    return
                if skip_if_finished and state.get("status") in FINISHED_ANALYSIS_STATUSES:
                    return
                event = {
                    "project_id": project_id,
                    "user_id": user_id,
                    "flow_run_id": state.get("flow_run_id"),
                    "timestamp": datetime.now(timezone.utc),
                    "stage": analysis_stage_label(stage),
                    "level": level,
                    "message": message,
                }
                if details:
                    event["details"] = details
                if duration_seconds is not None:
                    event["duration_seconds"] = duration_seconds
                result = self.analysis_events_collection.insert_one(dict(event))
            emit_analysis_event({"id": str(result.inserted_id), **event})
        except Exception as e:
            logger.warning(f"Could not record analysis event for project {project_id}: {e}")

    def get_analysis_events(self, user_id, project_id):
        """Return the project's analysis events, oldest first, with UTC timestamps."""
        events = list(
            self.analysis_events_collection.find(
                {"user_id": user_id, "project_id": project_id}
            ).sort("timestamp", 1)
        )
        for event in events:
            event["id"] = str(event.pop("_id"))
            event["timestamp"] = event["timestamp"].replace(tzinfo=timezone.utc)
        return events

    def load_analysis_state(self, user_id, project_id):
        """Load the project's analysis state, reconciling stale active runs with Prefect."""
        project = self.projects_collection.find_one(
            {"_id": ObjectId(project_id), "user_id": user_id}, {"analysis_state": 1}
        )
        state = project.get("analysis_state") if project else None
        if not state or state.get("status") not in ACTIVE_ANALYSIS_STATUSES:
            return state
        reconciled = self._reconcile_running_state(state)
        if reconciled["status"] == state["status"]:
            return state
        logger.info(
            f"[ProjectHandler] Reconciled stale {state['status']} state for project "
            f"{project_id} → {reconciled['status']}"
        )
        changes = {"status": reconciled["status"], "message": reconciled["message"]}
        if reconciled["status"] == "Failed":
            changes["resumable_from"] = resumable_from(reconciled)
        if self.save_analysis_state(
            user_id, project_id, changes, expected_updated_at=state.get("updated_at")
        ):
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
                                "message": "The analysis stopped unexpectedly"}
                    if prefect_state_type in terminal_ok:
                        return {**state, "status": "Completed",
                                "message": "Analysis completed"}
                    if prefect_state_type in terminal_stopped:
                        return {**state, "status": "Failed",
                                "message": "The analysis was cancelled"}
                    return state
            except Exception as prefect_exc:
                logger.warning(f"[ProjectHandler] Could not reach Prefect API to reconcile run {flow_run_id}: {prefect_exc}")

        # time-based staleness check
        staleness_minutes = int(os.getenv("ANALYSIS_STALENESS_MINUTES", "360"))
        updated_at = state.get("updated_at")
        if updated_at:
            try:
                if updated_at.tzinfo is None:
                    updated_at = updated_at.replace(tzinfo=timezone.utc)
                age = datetime.now(timezone.utc) - updated_at
                if age > timedelta(minutes=staleness_minutes):
                    hours = int(age.total_seconds() / 3600)
                    return {
                        **state,
                        "status": "Failed",
                        "message": f"The analysis stopped unexpectedly (no progress for {hours}h)",
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
        # A user's fork is reused across chats, so it carries no single thread.
        new_project.pop("thread_id", None)
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

        logger.info(
            f"Forked demo template {template_project_id} to user {target_user_id} "
            f"as project {new_project_id}"
        )
        return new_project_id