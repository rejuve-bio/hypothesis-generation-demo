"""Handler for demo seed registry and MinIO bundle export/import."""

from __future__ import annotations

import json
import os
import re
import shutil
from datetime import datetime, timezone
from typing import Any, Optional

from bson import ObjectId, json_util
from loguru import logger

from .base_handler import BaseHandler

BUNDLE_VERSION = 2
DEMO_OWNER_USER_ID = "__demo__"
SLUG_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
LDSC_COMPLETED_STATUSES = frozenset({"ldsc_tissue_completed", "completed"})

_PROJECT_SCOPED_COLLECTIONS = [
    ("credible_sets.json", "credible_sets_collection", {"type": "credible_set"}),
    ("analysis_results.json", "analysis_results_collection", {}),
    ("gene_expression_runs.json", "gene_expression_runs_collection", {}),
    ("hypotheses.json", "hypotheses_collection", {}),
    ("enrich.json", "enrich_collection", {}),
    ("tissue_selections.json", "tissue_selections_collection", {}),
]

_DELETABLE_PROJECT_COLLECTIONS = (
    "credible_sets_collection",
    "analysis_results_collection",
    "gene_expression_runs_collection",
    "hypotheses_collection",
    "enrich_collection",
    "tissue_selections_collection",
)

_RUN_SCOPED_COLLECTIONS = (
    ("ldsc_results.json", "ldsc_results_collection"),
    ("tissue_mappings.json", "tissue_mappings_collection"),
)


class DemoTemplateError(Exception):
    """Raised when demo seed validation or I/O fails."""


class DemoTemplateHandler(BaseHandler):
    """Registry and MinIO bundles for shared demo projects."""

    def __init__(self, uri: str, db_name: str):
        super().__init__(uri, db_name)
        self.demo_templates_collection = self.db["demo_templates"]
        self.projects_collection = self.db["projects"]
        self.credible_sets_collection = self.db["credible_sets"]
        self.analysis_results_collection = self.db["analysis_results"]
        self.gene_expression_runs_collection = self.db["gene_expression_runs"]
        self.ldsc_results_collection = self.db["ldsc_results"]
        self.tissue_mappings_collection = self.db["tissue_mappings"]
        self.file_metadata_collection = self.db["file_metadata"]
        self.hypotheses_collection = self.db["hypotheses"]
        self.enrich_collection = self.db["enrich"]
        self.tissue_selections_collection = self.db["tissue_selections"]
        self.user_demo_forks_collection = self.db["user_demo_forks"]
        self._ensure_indexes()

    @staticmethod
    def get_owner_user_id(template: dict) -> str:
        """Return the MongoDB user_id that owns demo data."""
        owner = template.get("demo_owner_id")
        if owner:
            return owner
        legacy = template.get("template_user_id")
        if legacy:
            return legacy
        raise DemoTemplateError("Demo registry entry is missing demo_owner_id")

    def _ensure_indexes(self) -> None:
        self.demo_templates_collection.create_index("slug", unique=True)
        self.demo_templates_collection.create_index("template_project_id")
        self.user_demo_forks_collection.create_index(
            [("user_id", 1), ("template_project_id", 1)], unique=True
        )

    def get_template_by_project_id(self, project_id: str) -> Optional[dict]:
        doc = self.demo_templates_collection.find_one(
            {"template_project_id": project_id, "is_active": True}
        )
        if not doc:
            return None
        doc["id"] = str(doc.pop("_id"))
        return doc

    def get_user_fork(self, user_id: str, template_project_id: str) -> Optional[str]:
        doc = self.user_demo_forks_collection.find_one(
            {"user_id": user_id, "template_project_id": template_project_id}
        )
        return doc.get("forked_project_id") if doc else None

    def save_user_fork(
        self,
        user_id: str,
        template_project_id: str,
        forked_project_id: str,
        template_slug: str,
    ) -> None:
        now = datetime.now(timezone.utc)
        self.user_demo_forks_collection.update_one(
            {"user_id": user_id, "template_project_id": template_project_id},
            {
                "$set": {
                    "forked_project_id": forked_project_id,
                    "template_slug": template_slug,
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )

    def is_registered_template_project(self, project_id: str) -> bool:
        return self.demo_templates_collection.count_documents(
            {"template_project_id": project_id, "is_active": True},
            limit=1,
        ) > 0

    @staticmethod
    def validate_slug(slug: str) -> str:
        normalized = (slug or "").strip().lower()
        if not normalized or not SLUG_PATTERN.match(normalized):
            raise DemoTemplateError(
                "slug must be lowercase alphanumeric with optional hyphens "
                "(e.g. obesity, type-2-diabetes)"
            )
        return normalized

    @staticmethod
    def seed_minio_prefix(slug: str) -> str:
        base = (os.getenv("DEMO_SEEDS_MINIO_PREFIX") or "demo-seeds-v2").strip("/")
        return f"{base}/{slug}"

    @staticmethod
    def get_analysis_state_path(user_id: str, project_id: str) -> str:
        return f"data/states/{user_id}/{project_id}/analysis_state.json"

    def list_templates(self, active_only: bool = False) -> list[dict]:
        query: dict[str, Any] = {"is_active": True} if active_only else {}
        docs = list(self.demo_templates_collection.find(query).sort("sort_order", 1))
        for doc in docs:
            doc["id"] = str(doc.pop("_id"))
        return docs

    def get_by_slug(self, slug: str) -> Optional[dict]:
        slug = self.validate_slug(slug)
        doc = self.demo_templates_collection.find_one({"slug": slug})
        if not doc:
            return None
        doc["id"] = str(doc.pop("_id"))
        return doc

    def get_project_by_id(self, project_id: str) -> Optional[dict]:
        try:
            doc = self.projects_collection.find_one({"_id": ObjectId(project_id)})
        except Exception:
            return None
        if not doc:
            return None
        doc["id"] = str(doc.pop("_id"))
        return doc

    def load_analysis_state(self, user_id: str, project_id: str) -> Optional[dict]:
        state_path = self.get_analysis_state_path(user_id, project_id)
        if not os.path.exists(state_path):
            return None
        with open(state_path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def validate_project_for_seed(self, project_id: str) -> dict:
        project = self.get_project_by_id(project_id)
        if not project:
            raise DemoTemplateError(f"Project not found: {project_id}")

        user_id = project["user_id"]
        analysis_state = self.load_analysis_state(user_id, project_id)
        if not analysis_state:
            raise DemoTemplateError(
                f"Missing analysis state file for project {project_id} "
                f"(expected {self.get_analysis_state_path(user_id, project_id)})"
            )

        status = (analysis_state.get("status") or "").strip()
        if status != "Completed":
            raise DemoTemplateError(
                f"Project analysis is not completed (status={status!r}). "
                "Only completed analyses can be exported as demo seeds."
            )

        credible_set_count = self.credible_sets_collection.count_documents(
            {"user_id": user_id, "project_id": project_id, "type": "credible_set"}
        )
        if credible_set_count == 0:
            raise DemoTemplateError("Project has no credible sets.")

        ldsc_run = self.gene_expression_runs_collection.find_one(
            {
                "user_id": user_id,
                "project_id": project_id,
                "gene_of_interest": "project_analysis",
                "status": {"$in": list(LDSC_COMPLETED_STATUSES)},
            },
            sort=[("created_at", -1)],
        )
        if not ldsc_run:
            raise DemoTemplateError(
                "Project has no completed LDSC/tissue analysis run "
                "(gene_of_interest=project_analysis)."
            )

        ldsc_count = self.ldsc_results_collection.count_documents(
            {"analysis_run_id": ldsc_run["id"]}
        )
        if ldsc_count == 0:
            raise DemoTemplateError("LDSC run exists but has no tissue results.")

        return {
            "project": project,
            "user_id": user_id,
            "analysis_state": analysis_state,
            "credible_set_count": credible_set_count,
            "ldsc_tissue_count": ldsc_count,
        }

    def register_seed(
        self,
        project_id: str,
        slug: str,
        display_name: str,
        *,
        sort_order: int = 1,
        force: bool = False,
    ) -> dict:
        """Register a completed project in the demo catalog (local registry only)."""
        slug = self.validate_slug(slug)
        display_name = (display_name or "").strip()
        if not display_name:
            raise DemoTemplateError("display_name is required")

        validation = self.validate_project_for_seed(project_id)
        project = validation["project"]
        owner_user_id = project["user_id"]

        existing = self.get_by_slug(slug)
        if existing and not force:
            raise DemoTemplateError(
                f"Demo seed slug {slug!r} already exists. Use --force to replace."
            )

        now = datetime.now(timezone.utc)
        registry_doc = {
            "slug": slug,
            "display_name": display_name,
            "template_project_id": project_id,
            "demo_owner_id": owner_user_id,
            "phenotype": project.get("phenotype", ""),
            "sort_order": sort_order,
            "is_active": True,
            "updated_at": now,
            "bundle_version": BUNDLE_VERSION,
            "minio_prefix": self.seed_minio_prefix(slug),
        }
        if existing:
            self.demo_templates_collection.update_one(
                {"slug": slug},
                {"$set": registry_doc, "$unset": {"template_user_id": ""}},
            )
        else:
            registry_doc["created_at"] = now
            self.demo_templates_collection.insert_one(registry_doc)

        logger.info(f"Registered demo seed slug={slug} project_id={project_id}")
        return {
            "slug": slug,
            "display_name": display_name,
            "template_project_id": project_id,
            "demo_owner_id": owner_user_id,
            "credible_set_count": validation["credible_set_count"],
            "ldsc_tissue_count": validation["ldsc_tissue_count"],
        }

    def _load_project_scoped_docs(self, user_id: str, project_id: str) -> dict[str, list[dict]]:
        result: dict[str, list[dict]] = {}
        run_ids: list[str] = []

        for filename, attr, extra in _PROJECT_SCOPED_COLLECTIONS:
            docs = list(
                getattr(self, attr).find(
                    {"user_id": user_id, "project_id": project_id, **extra}
                )
            )
            result[filename] = docs
            if filename == "gene_expression_runs.json":
                run_ids = [doc["id"] for doc in docs if doc.get("id")]

        if run_ids:
            result["ldsc_results.json"] = list(
                self.ldsc_results_collection.find({"analysis_run_id": {"$in": run_ids}})
            )
            result["tissue_mappings.json"] = list(
                self.tissue_mappings_collection.find({"analysis_run_id": {"$in": run_ids}})
            )
        else:
            result["ldsc_results.json"] = []
            result["tissue_mappings.json"] = []

        return result

    def collect_seed_bundle(self, slug: str, *, seed_version: int = 1) -> dict[str, Any]:
        """Collect a demo seed bundle from live MongoDB, normalized to __demo__ owner."""
        template = self.get_by_slug(slug)
        if not template:
            raise DemoTemplateError(f"Demo seed not found for slug={slug!r}")

        project_id = template["template_project_id"]
        source_user_id = self.get_owner_user_id(template)
        demo_user_id = DEMO_OWNER_USER_ID

        project_raw = self.projects_collection.find_one({"_id": ObjectId(project_id)})
        if not project_raw:
            raise DemoTemplateError(f"Demo project {project_id} no longer exists in MongoDB.")

        analysis_state = self.load_analysis_state(source_user_id, project_id)
        if not analysis_state:
            raise DemoTemplateError(
                f"Missing analysis state file for demo project {project_id}."
            )

        file_metadata_docs: list[dict] = []
        gwas_file_id = project_raw.get("gwas_file_id")
        if gwas_file_id:
            try:
                file_doc = self.file_metadata_collection.find_one(
                    {"_id": ObjectId(gwas_file_id)}
                )
            except Exception:
                file_doc = None
            if file_doc:
                file_metadata_docs.append(file_doc)

        mongo_payload = self._load_project_scoped_docs(source_user_id, project_id)
        mongo_payload["file_metadata.json"] = file_metadata_docs

        if source_user_id != demo_user_id:
            mongo_payload = {
                filename: self._rewrite_docs_for_demo_owner(docs, source_user_id)
                for filename, docs in mongo_payload.items()
            }
            analysis_state = self._rewrite_docs_for_demo_owner(
                analysis_state, source_user_id
            )

        project_raw = dict(project_raw)
        if source_user_id != demo_user_id:
            project_raw = self._rewrite_docs_for_demo_owner(project_raw, source_user_id)
        project_raw["is_demo"] = True
        project_raw["is_template"] = False
        project_raw["demo_slug"] = slug
        project_raw["user_id"] = demo_user_id
        mongo_payload["project.json"] = [project_raw]

        manifest = {
            "bundle_version": BUNDLE_VERSION,
            "seed_version": seed_version,
            "slug": slug,
            "display_name": template["display_name"],
            "project_id": project_id,
            "demo_owner_id": demo_user_id,
            "phenotype": template.get("phenotype", project_raw.get("phenotype", "")),
            "sort_order": template.get("sort_order", 1),
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "counts": {key.replace(".json", ""): len(docs or []) for key, docs in mongo_payload.items()},
        }

        return {
            "manifest": manifest,
            "mongo": mongo_payload,
            "files": {"analysis_state.json": analysis_state},
        }

    @staticmethod
    def _serialize_bundle_value(value: Any) -> str:
        return json_util.dumps(
            value, json_options=json_util.JSONOptions(json_mode=json_util.JSONMode.RELAXED)
        )

    @staticmethod
    def _rewrite_user_scoped_strings(value: Any, source_user_id: str, demo_user_id: str) -> Any:
        if isinstance(value, str):
            return (
                value.replace(f"/{source_user_id}/", f"/{demo_user_id}/")
                .replace(f"data/projects/{source_user_id}/", f"data/projects/{demo_user_id}/")
                .replace(f"data/states/{source_user_id}/", f"data/states/{demo_user_id}/")
                .replace(f"data/temp/{source_user_id}/", f"data/temp/{demo_user_id}/")
                .replace(f"data/metadata/{source_user_id}/", f"data/metadata/{demo_user_id}/")
            )
        if isinstance(value, dict):
            return {
                k: DemoTemplateHandler._rewrite_user_scoped_strings(v, source_user_id, demo_user_id)
                for k, v in value.items()
            }
        if isinstance(value, list):
            return [
                DemoTemplateHandler._rewrite_user_scoped_strings(v, source_user_id, demo_user_id)
                for v in value
            ]
        return value

    @classmethod
    def _rewrite_docs_for_demo_owner(
        cls,
        docs: list[dict] | dict | None,
        source_user_id: str,
        *,
        demo_user_id: str = DEMO_OWNER_USER_ID,
    ) -> list[dict] | dict | None:
        if docs is None:
            return None

        is_single = isinstance(docs, dict)
        items = [docs] if is_single else docs
        out: list[dict] = []
        for item in items:
            item = cls._rewrite_user_scoped_strings(item, source_user_id, demo_user_id)
            if item.get("user_id") == source_user_id:
                item["user_id"] = demo_user_id
            out.append(item)
        return out[0] if is_single else out

    def export_seed_to_minio(self, slug: str, storage, *, seed_version: int = 1) -> dict:
        bundle = self.collect_seed_bundle(slug, seed_version=seed_version)
        prefix = self.seed_minio_prefix(slug)

        uploads: dict[str, Any] = {"manifest.json": bundle["manifest"]}
        uploads.update({f"mongo/{k}": v for k, v in bundle["mongo"].items()})
        uploads.update({f"files/{k}": v for k, v in bundle["files"].items()})

        for filename, content in uploads.items():
            key = f"{prefix}/{filename}"
            if not storage.upload_string(self._serialize_bundle_value(content), key):
                raise DemoTemplateError(f"Failed to upload {key} to MinIO")

        now = datetime.now(timezone.utc)
        self.demo_templates_collection.update_one(
            {"slug": slug},
            {
                "$set": {
                    "last_exported_at": now,
                    "minio_prefix": prefix,
                    "updated_at": now,
                    "bundle_version": BUNDLE_VERSION,
                },
                "$unset": {"template_user_id": ""},
            },
        )

        bucket = storage.bucket
        logger.info(
            f"Exported demo seed slug={slug} to s3://{bucket}/{prefix}/ "
            f"({len(uploads)} objects)"
        )
        return {
            "slug": slug,
            "minio_prefix": prefix,
            "bucket": bucket,
            "object_count": len(uploads),
            "manifest_key": f"{prefix}/manifest.json",
            "counts": bundle["manifest"]["counts"],
            "seed_version": seed_version,
            "demo_owner_id": DEMO_OWNER_USER_ID,
            "project_id": bundle["manifest"]["project_id"],
        }

    @staticmethod
    def _deserialize_bundle_value(raw: str) -> Any:
        return json_util.loads(raw)

    def _download_bundle_part(self, storage, object_key: str) -> Any:
        raw = storage.download_string(object_key)
        if raw is None:
            raise DemoTemplateError(f"Missing MinIO object: {object_key}")
        return self._deserialize_bundle_value(raw)

    def _seed_already_present(self, slug: str, project_id: str) -> bool:
        project = self.projects_collection.find_one({"_id": ObjectId(project_id)})
        if not project:
            return False
        return bool(project.get("is_demo") and project.get("demo_slug") == slug)

    def _delete_legacy_owner_data(self, legacy_user_id: str, project_id: str) -> None:
        if legacy_user_id == DEMO_OWNER_USER_ID:
            return

        legacy_runs = list(
            self.gene_expression_runs_collection.find(
                {"user_id": legacy_user_id, "project_id": project_id},
                {"id": 1},
            )
        )
        legacy_run_ids = [run["id"] for run in legacy_runs if run.get("id")]
        if legacy_run_ids:
            self.ldsc_results_collection.delete_many(
                {"analysis_run_id": {"$in": legacy_run_ids}}
            )
            self.tissue_mappings_collection.delete_many(
                {"analysis_run_id": {"$in": legacy_run_ids}}
            )

        for attr in _DELETABLE_PROJECT_COLLECTIONS:
            getattr(self, attr).delete_many(
                {"user_id": legacy_user_id, "project_id": project_id}
            )

        legacy_state_path = self.get_analysis_state_path(legacy_user_id, project_id)
        if os.path.exists(legacy_state_path):
            shutil.rmtree(os.path.dirname(legacy_state_path), ignore_errors=True)
            logger.info(f"Removed legacy analysis state at {legacy_state_path}")

    def import_seed_from_minio(
        self,
        slug: str,
        storage,
        *,
        force: bool = False,
        dry_run: bool = False,
    ) -> dict:
        slug = self.validate_slug(slug)
        prefix = self.seed_minio_prefix(slug)
        manifest_key = f"{prefix}/manifest.json"

        if not storage.exists(manifest_key):
            raise DemoTemplateError(f"Demo seed bundle not found at {manifest_key}")

        manifest = self._download_bundle_part(storage, manifest_key)
        if manifest.get("bundle_version") != BUNDLE_VERSION:
            raise DemoTemplateError(
                f"Expected bundle_version={BUNDLE_VERSION} at {manifest_key}, "
                f"got {manifest.get('bundle_version')!r}"
            )

        project_id = manifest["project_id"]
        user_id = manifest.get("demo_owner_id", DEMO_OWNER_USER_ID)
        existing_registry = self.get_by_slug(slug)

        if self._seed_already_present(slug, project_id) and not force:
            reason = "demo seed already present locally"
            logger.info(f"Demo seed slug={slug} skipped: {reason}")
            return {
                "slug": slug,
                "status": "skipped",
                "reason": reason,
                "project_id": project_id,
                "demo_owner_id": user_id,
            }

        if dry_run:
            legacy_user_id = existing_registry.get("template_user_id") if existing_registry else None
            if not legacy_user_id and existing_registry:
                legacy_user_id = existing_registry.get("demo_owner_id")
            return {
                "slug": slug,
                "status": "dry_run",
                "project_id": project_id,
                "demo_owner_id": user_id,
                "minio_prefix": prefix,
                "counts": manifest.get("counts", {}),
                "seed_version": manifest.get("seed_version", 1),
                "would_remove_legacy_user_id": legacy_user_id,
            }

        if existing_registry:
            legacy_user_id = existing_registry.get("template_user_id") or existing_registry.get(
                "demo_owner_id"
            )
            if legacy_user_id and legacy_user_id != user_id:
                self._delete_legacy_owner_data(legacy_user_id, project_id)

        project_docs = self._download_bundle_part(storage, f"{prefix}/mongo/project.json")
        if not project_docs:
            raise DemoTemplateError("project.json is empty in demo seed bundle")
        project_doc = project_docs[0]
        project_doc["is_demo"] = True
        project_doc["is_template"] = False
        project_doc["demo_slug"] = slug
        project_doc["user_id"] = user_id
        self.projects_collection.replace_one(
            {"_id": project_doc["_id"]}, project_doc, upsert=True
        )

        file_docs = self._download_bundle_part(storage, f"{prefix}/mongo/file_metadata.json")
        for doc in file_docs or []:
            self.file_metadata_collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)

        run_ids: list[str] = []
        for filename, attr, _extra in _PROJECT_SCOPED_COLLECTIONS:
            collection = getattr(self, attr)
            object_key = f"{prefix}/mongo/{filename}"
            docs = (
                self._download_bundle_part(storage, object_key)
                if storage.exists(object_key)
                else []
            )
            if not isinstance(docs, list):
                raise DemoTemplateError(f"Expected list in {object_key}")
            collection.delete_many({"user_id": user_id, "project_id": project_id})
            if docs:
                collection.insert_many(docs)
            if filename == "gene_expression_runs.json":
                run_ids = [doc["id"] for doc in docs if doc.get("id")]

        if run_ids:
            self.ldsc_results_collection.delete_many({"analysis_run_id": {"$in": run_ids}})
            self.tissue_mappings_collection.delete_many({"analysis_run_id": {"$in": run_ids}})

        for filename, attr in _RUN_SCOPED_COLLECTIONS:
            docs = self._download_bundle_part(storage, f"{prefix}/mongo/{filename}")
            if docs:
                getattr(self, attr).insert_many(docs)

        analysis_state = self._download_bundle_part(
            storage, f"{prefix}/files/analysis_state.json"
        )
        state_path = self.get_analysis_state_path(user_id, project_id)
        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        with open(state_path, "w", encoding="utf-8") as handle:
            json.dump(analysis_state, handle, default=str)

        now = datetime.now(timezone.utc)
        registry_doc = {
            "slug": slug,
            "display_name": manifest.get("display_name") or slug,
            "template_project_id": project_id,
            "demo_owner_id": user_id,
            "phenotype": manifest.get("phenotype", ""),
            "sort_order": manifest.get("sort_order", 1),
            "is_active": True,
            "updated_at": now,
            "imported_at": now,
            "bundle_version": BUNDLE_VERSION,
            "seed_version": manifest.get("seed_version", 1),
            "minio_prefix": prefix,
        }
        if existing_registry:
            self.demo_templates_collection.update_one(
                {"slug": slug},
                {"$set": registry_doc, "$unset": {"template_user_id": ""}},
            )
        else:
            registry_doc["created_at"] = now
            self.demo_templates_collection.insert_one(registry_doc)

        logger.info(f"Imported demo seed slug={slug} project_id={project_id} from MinIO")
        return {
            "slug": slug,
            "status": "imported",
            "project_id": project_id,
            "demo_owner_id": user_id,
            "counts": manifest.get("counts", {}),
            "seed_version": manifest.get("seed_version", 1),
        }

    def list_seed_slugs(self, storage) -> list[str]:
        base = (os.getenv("DEMO_SEEDS_MINIO_PREFIX") or "demo-seeds-v2").strip("/")
        return storage.list_child_prefixes(base)

    def ensure_seeds_from_minio(self, storage) -> dict:
        """Import any MinIO demo seed bundles that are missing locally."""
        slugs = self.list_seed_slugs(storage)
        results = {"slugs_found": slugs, "imported": [], "skipped": [], "failed": []}

        for slug in slugs:
            try:
                slug = self.validate_slug(slug)
                prefix = self.seed_minio_prefix(slug)
                manifest_key = f"{prefix}/manifest.json"
                if not storage.exists(manifest_key):
                    continue
                manifest = self._download_bundle_part(storage, manifest_key)
                project_id = manifest.get("project_id")
                if project_id and self._seed_already_present(slug, project_id):
                    results["skipped"].append(slug)
                    continue
                info = self.import_seed_from_minio(slug, storage)
                if info["status"] == "imported":
                    results["imported"].append(slug)
                else:
                    results["skipped"].append(slug)
            except Exception as exc:
                logger.error(f"Failed to import demo seed {slug}: {exc}")
                results["failed"].append({"slug": slug, "error": str(exc)})

        return results
