from unittest.mock import MagicMock

import pytest
from bson import ObjectId

from src.db.demo_template_handler import BUNDLE_VERSION, DemoTemplateHandler

PROJECT_ID = "64b000000000000000000001"
PREFIX = "demo-seeds-v2/obesity"


class FakeStorage:
    def __init__(self, objects: dict):
        self.objects = {
            key: DemoTemplateHandler._serialize_bundle_value(value)
            for key, value in objects.items()
        }

    def exists(self, key):
        return key in self.objects

    def download_string(self, key):
        return self.objects.get(key)


def _handler():
    handler = object.__new__(DemoTemplateHandler)
    for name in (
        "demo_templates_collection",
        "projects_collection",
        "credible_sets_collection",
        "analysis_results_collection",
        "gene_expression_runs_collection",
        "ldsc_results_collection",
        "tissue_mappings_collection",
        "file_metadata_collection",
        "hypotheses_collection",
        "enrich_collection",
        "tissue_selections_collection",
    ):
        setattr(handler, name, MagicMock())
    handler.demo_templates_collection.find_one.return_value = None
    handler.projects_collection.find_one.return_value = None
    return handler


def _bundle(project: dict, **extra_objects) -> dict:
    return {
        f"{PREFIX}/manifest.json": {
            "bundle_version": BUNDLE_VERSION,
            "project_id": PROJECT_ID,
            "demo_owner_id": "__demo__",
        },
        f"{PREFIX}/mongo/project.json": [project],
        f"{PREFIX}/mongo/file_metadata.json": [],
        f"{PREFIX}/mongo/ldsc_results.json": [],
        f"{PREFIX}/mongo/tissue_mappings.json": [],
        **extra_objects,
    }


@pytest.fixture(autouse=True)
def _default_prefix(monkeypatch):
    monkeypatch.delenv("DEMO_SEEDS_MINIO_PREFIX", raising=False)


def test_import_reads_state_from_legacy_bundle_file():
    handler = _handler()
    storage = FakeStorage(
        _bundle(
            {"_id": ObjectId(PROJECT_ID), "name": "Obesity"},
            **{f"{PREFIX}/files/analysis_state.json": {"status": "Completed"}},
        )
    )

    handler.import_seed_from_minio("obesity", storage)

    imported = handler.projects_collection.replace_one.call_args.args[1]
    assert imported["analysis_state"] == {"status": "Completed"}


def test_import_prefers_state_embedded_in_project():
    handler = _handler()
    storage = FakeStorage(
        _bundle(
            {"_id": ObjectId(PROJECT_ID), "analysis_state": {"status": "Completed"}},
            **{f"{PREFIX}/files/analysis_state.json": {"status": "Running"}},
        )
    )

    handler.import_seed_from_minio("obesity", storage)

    imported = handler.projects_collection.replace_one.call_args.args[1]
    assert imported["analysis_state"] == {"status": "Completed"}
