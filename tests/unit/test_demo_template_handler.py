from unittest.mock import MagicMock

from bson import ObjectId

from src.db.base_handler import BaseHandler
from src.db.demo_template_handler import BUNDLE_VERSION, DemoTemplateHandler

PREFIX = "demo-seeds-v2/obesity"


def test_import_reads_state_from_legacy_bundle_file(monkeypatch):
    monkeypatch.delenv("DEMO_SEEDS_MINIO_PREFIX", raising=False)
    monkeypatch.setattr(BaseHandler, "__init__", lambda self, *_: setattr(self, "db", MagicMock()))
    monkeypatch.setattr(DemoTemplateHandler, "_ensure_indexes", lambda self: None)
    handler = DemoTemplateHandler("uri", "db")
    handler.db["any"].find_one.return_value = None

    project_id = "64b000000000000000000001"
    bundle = {
        "manifest.json": {"bundle_version": BUNDLE_VERSION, "project_id": project_id},
        "mongo/project.json": [{"_id": ObjectId(project_id)}],
        "mongo/file_metadata.json": [],
        "mongo/ldsc_results.json": [],
        "mongo/tissue_mappings.json": [],
        "files/analysis_state.json": {"status": "Completed"},
    }
    objects = {f"{PREFIX}/{key}": handler._serialize_bundle_value(value) for key, value in bundle.items()}
    storage = MagicMock(exists=objects.__contains__, download_string=objects.get)

    handler.import_seed_from_minio("obesity", storage)

    imported = handler.projects_collection.replace_one.call_args.args[1]
    assert imported["analysis_state"] == {"status": "Completed"}
