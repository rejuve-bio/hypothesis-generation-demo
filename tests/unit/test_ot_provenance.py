"""Tests for recording which OpenTargets release an analysis run used."""
from unittest.mock import MagicMock

import pytest
from bson.objectid import ObjectId

import src.db.credible_sets_handler as cs_handler
import src.flows.analysis as analysis_flow
import src.tasks.analysis as analysis_tasks
from src.db.project_handler import ProjectHandler

OT_VERSION = {
    "ot_release": "25.09",
    "schema_version": 1,
    "source": "minio",
    "loaded_at": "2026-10-09T06:51:23+00:00",
}

OT_ROW = {
    "study_locus_id": "sl1",
    "study_id": "GCST1",
    "chromosome": "1",
    "position": 100,
    "beta": 0.1,
    "p_value_mantissa": 1.0,
    "p_value_exponent": -9,
    "effect_allele_frequency": 0.2,
    "finemap_method": "SuSiE-inf",
    "confidence": "high",
    "locus": [{"variantId": "1_100_A_G", "posteriorProbability": 0.9, "beta": 0.1}],
}


def test_convert_stamps_ot_release_into_metadata():
    cs = cs_handler.convert_ot_row_to_credible_set(OT_ROW, ot_version=OT_VERSION)
    assert cs["metadata"]["source"] == "opentargets"
    assert cs["metadata"]["ot_release"] == "25.09"
    assert cs["metadata"]["ot_loaded_at"] == OT_VERSION["loaded_at"]


def test_convert_without_version_leaves_release_unknown():
    cs = cs_handler.convert_ot_row_to_credible_set(OT_ROW)
    assert cs["metadata"]["ot_release"] is None


def test_project_handler_sets_opentargets_block():
    handler = object.__new__(ProjectHandler)
    handler.projects_collection = MagicMock()
    project_id = str(ObjectId())

    handler.set_opentargets_provenance("user-1", project_id, {"used": True, "ot_release": "25.09"})

    query, update = handler.projects_collection.update_one.call_args.args
    assert query == {"_id": ObjectId(project_id), "user_id": "user-1"}
    block = update["$set"]["opentargets"]
    assert block["used"] is True and block["ot_release"] == "25.09"
    assert "recorded_at" in block


class _FakeTask:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def submit(self, *args):
        self.calls.append(args)
        future = MagicMock()
        if self.fail:
            future.result.side_effect = RuntimeError("mongo down")
        return future


def test_record_ot_provenance_payload(monkeypatch):
    fake = _FakeTask()
    monkeypatch.setattr(analysis_flow, "save_opentargets_provenance_task", fake)

    analysis_flow._record_ot_provenance("u", "p", "GCST1", True, "used precomputed credible sets", OT_VERSION)

    (user_id, project_id, provenance), = fake.calls
    assert (user_id, project_id) == ("u", "p")
    assert provenance == {
        "study_id": "GCST1",
        "used": True,
        "reason": "used precomputed credible sets",
        "ot_release": "25.09",
        "ot_loaded_at": OT_VERSION["loaded_at"],
    }


def test_record_ot_provenance_never_fails_the_run(monkeypatch):
    monkeypatch.setattr(analysis_flow, "save_opentargets_provenance_task", _FakeTask(fail=True))
    analysis_flow._record_ot_provenance("u", "p", None, False, "GWAS file is not an OpenTargets study", None)


@pytest.fixture
def fake_ot_handler(monkeypatch):
    handler = MagicMock()
    monkeypatch.setattr(cs_handler, "CredibleSetsHandler", lambda: handler)
    return handler


def test_ot_task_returns_and_stamps_version(monkeypatch, fake_ot_handler):
    fake_ot_handler.get_all_credible_sets_for_study_with_version.return_value = ([OT_ROW], OT_VERSION)
    analysis = MagicMock()
    monkeypatch.setattr(analysis_tasks, "get_deps", lambda: {"analysis": analysis})

    df, version = analysis_tasks.get_results_from_opentargets_task.fn("u", "p", "GCST1")

    assert version == OT_VERSION
    assert len(df) == 1
    saved = analysis.save_credible_set.call_args.args[2]
    assert saved["metadata"]["ot_release"] == "25.09"


def test_ot_task_with_no_rows_still_returns_version(monkeypatch, fake_ot_handler):
    fake_ot_handler.get_all_credible_sets_for_study_with_version.return_value = ([], OT_VERSION)
    monkeypatch.setattr(analysis_tasks, "get_deps", lambda: {"analysis": MagicMock()})

    df, version = analysis_tasks.get_results_from_opentargets_task.fn("u", "p", "GCST1")

    assert df is None and version == OT_VERSION
