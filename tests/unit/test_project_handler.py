import asyncio
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from bson import ObjectId
from fastapi import HTTPException

from src.api.routes import projects as projects_routes
from src.api.schemas import AnalysisResumeRequest
from src import run_deployment as run_deployment_module
from src import utils
from src.services import prefect_runs
from src.tasks import project as project_tasks
from src.db.project_handler import ProjectHandler
from src.utils import analysis_state_for_public_api


def _handler():
    handler = object.__new__(ProjectHandler)
    handler.projects_collection = MagicMock()
    handler.credible_sets_collection = MagicMock()
    handler.hypothesis_collection = MagicMock()
    handler.task_updates_collection = MagicMock()
    handler.summary_collection = MagicMock()
    handler.enrich_collection = MagicMock()
    handler.analysis_results_collection = MagicMock()
    handler.file_metadata_collection = MagicMock()
    handler.analysis_events_collection = MagicMock()
    handler.db = MagicMock()
    return handler


def test_fork_project_marks_copy_as_non_demo_and_records_template():
    handler = _handler()
    template_id = "64b000000000000000000001"
    source = {
        "_id": ObjectId(template_id),
        "user_id": "owner",
        "name": "Canonical demo",
        "is_demo": True,
        "is_template": True,
        "demo_slug": "canonical",
        "analysis_state": {"status": "Completed", "progress": 100},
    }
    handler.projects_collection.find_one.return_value = source
    handler._copy_collection_docs = MagicMock(return_value=[])

    new_id = handler.fork_project_from_template(
        "owner", template_id, "viewer", template_slug="canonical"
    )

    inserted = handler.projects_collection.insert_one.call_args.args[0]
    assert str(inserted["_id"]) == new_id
    assert inserted["user_id"] == "viewer"
    assert inserted["is_demo"] is False
    assert inserted["is_template"] is False
    assert inserted["source_template_id"] == template_id
    assert inserted["source_template_slug"] == "canonical"
    assert "demo_slug" not in inserted
    assert inserted["analysis_state"] == {"status": "Completed", "progress": 100}


def test_bulk_delete_allows_fork_and_cleans_fork_mapping():
    handler = _handler()
    fork_id = "64b000000000000000000002"
    handler.projects_collection.find_one.return_value = {
        "_id": ObjectId(fork_id), "user_id": "viewer", "is_demo": False,
        "source_template_id": "64b000000000000000000001",
    }
    handler.projects_collection.delete_one.return_value.deleted_count = 1
    handler.db["user_demo_forks"].delete_one.return_value.deleted_count = 1
    handler._delete_project_data = MagicMock()

    result = handler.bulk_delete_projects("viewer", [fork_id])

    assert result == {
        "deleted_count": 1, "total_requested": 1, "errors": [], "success": True
    }
    handler._delete_project_data.assert_called_once_with("viewer", fork_id)
    handler.db["user_demo_forks"].delete_one.assert_called_once_with(
        {"user_id": "viewer", "forked_project_id": fork_id}
    )


def test_bulk_delete_protects_canonical_demo():
    handler = _handler()
    demo_id = "64b000000000000000000001"
    handler.projects_collection.find_one.return_value = {
        "_id": ObjectId(demo_id), "user_id": "owner", "is_demo": True
    }
    handler._delete_project_data = MagicMock()
    result = handler.bulk_delete_projects("owner", [demo_id])
    assert result["deleted_count"] == 0
    assert result["success"] is False
    assert "cannot be deleted" in result["errors"][0]
    handler._delete_project_data.assert_not_called()
    handler.projects_collection.delete_one.assert_not_called()


def test_delete_project_data_removes_tissue_selections_and_events(monkeypatch):
    handler = _handler()
    project_id = "64b000000000000000000001"
    handler.load_analysis_state = MagicMock(return_value=None)
    handler.hypothesis_collection.find.return_value = []
    handler.projects_collection.find_one.return_value = None
    handler.get_project_analysis_path = MagicMock(return_value="/nonexistent/analysis")
    monkeypatch.setattr("src.db.project_handler.os.path.exists", lambda _path: False)

    handler._delete_project_data("viewer", project_id)

    project_filter = {"user_id": "viewer", "project_id": project_id}
    handler.db["tissue_selections"].delete_many.assert_called_once_with(project_filter)
    handler.analysis_events_collection.delete_many.assert_called_once_with(project_filter)


PROJECT_ID = "64b000000000000000000001"


def test_save_analysis_state_sets_only_given_fields():
    handler = _handler()
    handler.projects_collection.update_one.return_value.modified_count = 1

    handler.save_analysis_state("viewer", PROJECT_ID, {"status": "Running"})

    query, update = handler.projects_collection.update_one.call_args.args
    assert query == {"_id": ObjectId(PROJECT_ID), "user_id": "viewer"}
    assert set(update["$set"]) == {"analysis_state.status", "analysis_state.updated_at"}


@pytest.mark.parametrize("modified, expected_status", [(1, "Failed"), (0, "Running")])
def test_reconcile_writes_back_only_if_state_unchanged(modified, expected_status):
    handler = _handler()
    updated_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=1)
    handler.projects_collection.find_one.return_value = {
        "analysis_state": {"status": "Running", "updated_at": updated_at}
    }
    handler.projects_collection.update_one.return_value.modified_count = modified

    assert handler.load_analysis_state("viewer", PROJECT_ID)["status"] == expected_status
    query = handler.projects_collection.update_one.call_args.args[0]
    assert query["analysis_state.updated_at"] == updated_at


def test_queued_run_is_cancelled_on_delete_and_has_public_message(monkeypatch):
    handler = _handler()
    state = {"status": "Queued", "flow_run_id": "run-1"}
    handler.load_analysis_state = MagicMock(return_value=state)
    handler.hypothesis_collection.find.return_value = []
    handler.projects_collection.find_one.return_value = None
    monkeypatch.setattr("src.db.project_handler.os.path.exists", lambda _path: False)
    post = MagicMock(return_value=MagicMock(status_code=200))
    monkeypatch.setattr("src.services.prefect_runs.requests.post", post)

    handler._delete_project_data("viewer", PROJECT_ID)

    assert post.call_args.args[0].endswith("/flow_runs/run-1/set_state")
    assert post.call_args.kwargs["json"] == {"state": {"type": "CANCELLING"}}
    assert analysis_state_for_public_api(state)["message"] == "Queued, waiting to start."


def test_add_analysis_event_stores_and_emits_with_stored_flow_run(monkeypatch):
    handler = _handler()
    handler.projects_collection.find_one.return_value = {"analysis_state": {"flow_run_id": "run-1"}}
    emit = MagicMock()
    monkeypatch.setattr("src.db.project_handler.emit_analysis_event", emit)

    handler.add_analysis_event(
        "viewer", PROJECT_ID, "Cojo", "3 independent signals", details={"independent_signals": 3}
    )

    event = handler.analysis_events_collection.insert_one.call_args.args[0]
    assert event["flow_run_id"] == "run-1"
    assert event["stage"] == "COJO"
    assert event["level"] == "info"
    assert event["details"] == {"independent_signals": 3}
    inserted_id = handler.analysis_events_collection.insert_one.return_value.inserted_id
    emit.assert_called_once_with({"id": str(inserted_id), **event})


def test_add_analysis_event_skips_events_after_the_run_finished():
    handler = _handler()
    handler.projects_collection.find_one.return_value = {"analysis_state": {"status": "Failed"}}

    handler.add_analysis_event("viewer", PROJECT_ID, "LDSC_Analysis", "Top tissues: Liver")

    handler.analysis_events_collection.insert_one.assert_not_called()


def test_add_analysis_event_never_raises():
    handler = _handler()
    handler.analysis_events_collection.insert_one.side_effect = RuntimeError("db down")

    handler.add_analysis_event("viewer", PROJECT_ID, "Cojo", "message")


@pytest.mark.parametrize("state, stage, level", [
    ({"status": "Running", "stage": "Cojo", "message": "Running COJO"}, "Cojo", "info"),
    ({"status": "Failed", "stage": "Cojo", "message": "COJO failed",
      "error_detail": {"error": "LDSC exited with an error"}}, "Cojo", "error"),
    ({"status": "Completed", "message": "Analysis completed"}, "Pipeline", "info"),
])
def test_save_analysis_state_records_an_event(state, stage, level):
    handler = _handler()
    handler.add_analysis_event = MagicMock()
    handler.projects_collection.update_one.return_value.modified_count = 1

    handler.save_analysis_state("viewer", PROJECT_ID, state)

    handler.add_analysis_event.assert_called_once_with(
        "viewer", PROJECT_ID, stage=stage, message=state["message"], level=level,
        details=state.get("error_detail"), skip_if_finished=False, flow_run_id=None,
    )


def test_save_analysis_state_without_status_records_no_event():
    handler = _handler()
    handler.add_analysis_event = MagicMock()
    handler.projects_collection.update_one.return_value.modified_count = 1

    handler.save_analysis_state("viewer", PROJECT_ID, {"flow_run_id": "run-1"})

    handler.add_analysis_event.assert_not_called()


def test_analysis_events_route_returns_owned_events_in_order():
    handler = _handler()
    handler.projects_collection.find_one.return_value = {"_id": ObjectId(PROJECT_ID)}
    handler.analysis_events_collection.find.return_value.sort.return_value = [
        {"_id": ObjectId(PROJECT_ID), "message": "first", "timestamp": datetime(2026, 1, 1)},
    ]

    response = asyncio.run(projects_routes.get_analysis_events(PROJECT_ID, "viewer", handler))

    assert json.loads(response.body) == [
        {"id": PROJECT_ID, "message": "first", "timestamp": "2026-01-01T00:00:00+00:00"}
    ]
    handler.analysis_events_collection.find.assert_called_once_with(
        {"user_id": "viewer", "project_id": PROJECT_ID}
    )
    handler.analysis_events_collection.find.return_value.sort.assert_called_once_with("timestamp", 1)


@pytest.mark.parametrize("project_id", [PROJECT_ID, "not-an-id"])
def test_analysis_events_route_hides_projects_the_user_does_not_own(project_id):
    handler = _handler()
    handler.projects_collection.find_one.return_value = None

    with pytest.raises(HTTPException) as exc:
        asyncio.run(projects_routes.get_analysis_events(project_id, "viewer", handler))

    assert exc.value.status_code == 404
    handler.analysis_events_collection.find.assert_not_called()


def test_record_analysis_event_marks_task_retries(monkeypatch):
    projects = MagicMock()
    monkeypatch.setattr(utils, "get_deps", lambda: {"projects": projects})
    monkeypatch.setattr(utils, "task_run", MagicMock(run_count=2))
    monkeypatch.setattr(utils, "flow_run", MagicMock(id="run-1"))

    utils.record_analysis_event("viewer", PROJECT_ID, "Fine_mapping", "Batch 1/2 done", details={"regions": 3})

    projects.add_analysis_event.assert_called_once_with(
        "viewer", PROJECT_ID, "Fine_mapping", "Batch 1/2 done (retry 1)",
        details={"regions": 3, "attempt": 2}, flow_run_id="run-1",
    )


def test_reconcile_to_failed_records_where_the_run_can_resume():
    handler = _handler()
    handler.projects_collection.find_one.return_value = {"analysis_state": {
        "status": "Running", "stage": "Cojo", "artifacts": {"harmonized_file": "harmonized.tsv.gz"},
        "updated_at": datetime.now(timezone.utc) - timedelta(days=1),
    }}
    handler.projects_collection.update_one.return_value.modified_count = 1

    handler.load_analysis_state("viewer", PROJECT_ID)

    update = handler.projects_collection.update_one.call_args.args[1]
    assert update["$set"]["analysis_state.resumable_from"] == ["filtering"]


@pytest.mark.parametrize("modified, claimed", [(1, True), (0, False)])
def test_claim_analysis_resume_replaces_the_failed_state_once(modified, claimed):
    handler = _handler()
    handler.projects_collection.update_one.return_value.modified_count = modified
    failed = {"status": "Failed", "stage": "LDSC_Analysis", "flow_run_id": "run-1",
              "error_detail": {"error": "LDSC exited with an error"}, "resume_count": 1,
              "artifacts": {"harmonized_file": "harmonized.tsv.gz"}}

    queued = handler.claim_analysis_resume("viewer", PROJECT_ID, failed, "ldsc")

    query, update = handler.projects_collection.update_one.call_args.args
    assert query["analysis_state.status"] == "Failed"
    assert query["analysis_state.flow_run_id"] == "run-1"
    state = update["$set"]["analysis_state"]
    assert set(state) == {
        "status", "message", "resumed_from", "resume_count", "artifacts",
        "superseded_flow_run_ids", "updated_at",
    }
    assert state["resume_count"] == 2
    assert state["superseded_flow_run_ids"] == ["run-1"]
    assert (queued is not None) == claimed


@pytest.mark.parametrize("event_run, current_run, stored", [
    ("run-1", "run-1", True),
    (None, "run-1", True),
    ("run-old", "run-1", False),
    ("run-old", None, False),
])
def test_add_analysis_event_drops_events_from_other_runs(monkeypatch, event_run, current_run, stored):
    handler = _handler()
    handler.projects_collection.find_one.return_value = {"analysis_state": {"flow_run_id": current_run}}
    monkeypatch.setattr("src.db.project_handler.emit_analysis_event", MagicMock())

    handler.add_analysis_event("viewer", PROJECT_ID, "LDSC_Analysis", "Top tissues", flow_run_id=event_run)

    assert handler.analysis_events_collection.insert_one.called == stored


FAILED_AT_FILTERING = {
    "status": "Failed", "stage": "Filtering", "flow_run_id": "run-1",
    "artifacts": {"harmonized_file": "harmonized.tsv.gz"}, "resumable_from": ["filtering"],
    "error_detail": {"error_type": "no_significant_variants", "min_p_value": 2e-7, "variants_below_1e5": 3},
}


def _resume_mocks(monkeypatch):
    projects = MagicMock()
    projects.get_projects.return_value = {
        "name": "Wrist", "user_email": "user@example.com", "population": "EUR", "ref_genome": "GRCh38",
        "analysis_parameters": {"p_value_threshold": 5e-8, "maf_threshold": 0.01, "sample_size": 1000,
                                "sample_size_source": "user_provided"},
    }
    projects.load_analysis_state.return_value = deepcopy(FAILED_AT_FILTERING)
    projects.has_hypotheses.return_value = False
    projects.claim_analysis_resume.return_value = {"status": "Queued", "artifacts": FAILED_AT_FILTERING["artifacts"]}
    mocks = SimpleNamespace(
        projects=projects,
        templates=MagicMock(**{"is_registered_template_project.return_value": False}),
        storage=MagicMock(**{"exists.return_value": False}),
        invoke=MagicMock(return_value=MagicMock(id="run-2")),
        cancel=MagicMock(),
        file_available=True,
    )
    monkeypatch.setattr(projects_routes, "invoke_analysis_pipeline_deployment", mocks.invoke)
    monkeypatch.setattr(projects_routes, "cancel_flow_run", mocks.cancel)
    monkeypatch.setattr(projects_routes, "emit_analysis_update", MagicMock())
    monkeypatch.setattr(projects_routes, "harmonized_file_available", lambda *_: mocks.file_available)
    return mocks


def _resume(mocks, **body):
    return asyncio.run(projects_routes.resume_analysis(
        PROJECT_ID, AnalysisResumeRequest(**body), "viewer",
        mocks.projects, mocks.templates, mocks.storage,
    ))


@pytest.mark.parametrize("setup, body, status_code", [
    (lambda m: m.projects.get_projects.configure_mock(return_value=None), {"from_stage": "filtering"}, 404),
    (lambda m: m.templates.is_registered_template_project.configure_mock(return_value=True),
     {"from_stage": "filtering"}, 403),
    (lambda m: None, {"from_stage": "unknown"}, 400),
    (lambda m: m.projects.load_analysis_state.return_value.update(status="Running"),
     {"from_stage": "filtering", "p_value_threshold": 1e-6}, 409),
    (lambda m: None, {"from_stage": "ldsc"}, 409),
    (lambda m: m.projects.load_analysis_state.return_value.update(resumable_from=["ldsc"], error_detail=None),
     {"from_stage": "ldsc", "p_value_threshold": 1e-6}, 400),
    (lambda m: None, {"from_stage": "filtering"}, 400),
    (lambda m: None, {"from_stage": "filtering", "p_value_threshold": 1e-7}, 400),
    (lambda m: None, {"from_stage": "filtering", "p_value_threshold": 1e-3}, 400),
    (lambda m: m.projects.has_hypotheses.configure_mock(return_value=True),
     {"from_stage": "filtering", "p_value_threshold": 1e-6}, 409),
    (lambda m: setattr(m, "file_available", False), {"from_stage": "filtering", "p_value_threshold": 1e-6}, 409),
    (lambda m: m.projects.claim_analysis_resume.configure_mock(return_value=None),
     {"from_stage": "filtering", "p_value_threshold": 1e-6}, 409),
])
def test_resume_route_rejects_invalid_requests(monkeypatch, setup, body, status_code):
    mocks = _resume_mocks(monkeypatch)
    setup(mocks)

    with pytest.raises(HTTPException) as exc:
        _resume(mocks, **body)

    assert exc.value.status_code == status_code
    mocks.invoke.assert_not_called()


def test_resume_route_restarts_the_run_with_stored_parameters(monkeypatch):
    mocks = _resume_mocks(monkeypatch)

    response = _resume(mocks, from_stage="filtering", p_value_threshold=1e-6)

    assert response.flow_run_id == "run-2"
    mocks.cancel.assert_called_once_with("run-1")
    mocks.projects.update_analysis_parameters.assert_called_once_with(
        "viewer", PROJECT_ID, {"p_value_threshold": 1e-6}
    )
    assert mocks.projects.add_analysis_event.call_args.args[3] == (
        "Restarted from filtering with p_value_threshold 1e-06 (was 5e-08)"
    )
    kwargs = mocks.invoke.call_args.kwargs
    assert kwargs["resume_from"] == "filtering"
    assert kwargs["artifacts"] == {"harmonized_file": "harmonized.tsv.gz"}
    assert (kwargs["p_value_threshold"], kwargs["maf_threshold"], kwargs["sample_size"]) == (1e-6, 0.01, 1000)
    assert "sample_size_source" not in kwargs


def test_resume_route_keeps_the_run_resumable_when_it_cannot_start(monkeypatch):
    mocks = _resume_mocks(monkeypatch)
    mocks.invoke.side_effect = RuntimeError("prefect down")

    with pytest.raises(HTTPException) as exc:
        _resume(mocks, from_stage="filtering", p_value_threshold=1e-6)

    assert exc.value.status_code == 500
    failed = mocks.projects.save_analysis_state.call_args.args[2]
    assert failed["status"] == "Failed"
    assert failed["resumable_from"] == ["filtering"]
    assert failed["error_detail"] == FAILED_AT_FILTERING["error_detail"]
    mocks.projects.update_analysis_parameters.assert_not_called()


@pytest.mark.parametrize("state_data, guarded", [
    ({"status": "Running", "flow_run_id": "run-2"}, True),
    ({"status": "Failed"}, False),
])
def test_state_writes_from_a_flow_run_are_ignored_once_another_run_owns_the_state(state_data, guarded):
    handler = _handler()
    handler.add_analysis_event = MagicMock()
    handler.projects_collection.update_one.return_value.modified_count = 0

    assert handler.save_analysis_state("viewer", PROJECT_ID, state_data) is False

    query = handler.projects_collection.update_one.call_args.args[0]
    assert ("analysis_state.flow_run_id" in query) == guarded
    if guarded:
        assert query["analysis_state.flow_run_id"] == {"$in": ["run-2", None]}
        assert query["analysis_state.superseded_flow_run_ids"] == {"$ne": "run-2"}
    handler.add_analysis_event.assert_not_called()


@pytest.mark.parametrize("state, emitted", [
    ({"status": "Running", "stage": "Cojo"}, True),
    ({"artifacts": {"harmonized_file": "harmonized.tsv.gz"}}, False),
])
def test_only_status_changes_are_pushed_to_clients(monkeypatch, state, emitted):
    projects = MagicMock(**{"save_analysis_state.return_value": True})
    emit = MagicMock()
    monkeypatch.setattr(project_tasks, "get_deps", lambda: {"projects": projects})
    monkeypatch.setattr(project_tasks, "emit_analysis_update", emit)

    project_tasks.save_analysis_state_task.fn("viewer", PROJECT_ID, state)

    assert emit.called == emitted


def test_public_analysis_state_hides_internal_fields():
    state = {"status": "Failed", "artifacts": {"harmonized_file": "harmonized.tsv.gz"},
             "superseded_flow_run_ids": ["run-1"]}

    assert set(analysis_state_for_public_api(state)) == {"status", "message"}


@pytest.mark.parametrize("resume_from, sends_resume_params", [(None, False), ("ldsc", True)])
def test_only_resumed_runs_send_resume_parameters(monkeypatch, resume_from, sends_resume_params):
    run = MagicMock()
    monkeypatch.setattr(run_deployment_module, "run_deployment", run)

    run_deployment_module.invoke_analysis_pipeline_deployment(
        "viewer", PROJECT_ID, resume_from=resume_from, artifacts={"credible_sets": True}
    )

    parameters = run.call_args.kwargs["parameters"]
    assert ("resume_from" in parameters and "artifacts" in parameters) == sends_resume_params


@pytest.mark.parametrize("status, accepted", [("ACCEPT", True), ("REJECT", False), ("ABORT", False)])
def test_cancel_flow_run_reports_whether_prefect_accepted(monkeypatch, status, accepted):
    response = MagicMock(**{"json.return_value": {"status": status, "details": {"reason": "terminal"}}})
    monkeypatch.setattr(prefect_runs.requests, "post", MagicMock(return_value=response))

    assert prefect_runs.cancel_flow_run("run-1") is accepted
