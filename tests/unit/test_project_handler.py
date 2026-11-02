import asyncio
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from bson import ObjectId
from fastapi import HTTPException

from src.api.routes import projects as projects_routes
from src import utils
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
    monkeypatch.setattr("src.db.project_handler._req.post", post)

    handler._delete_project_data("viewer", PROJECT_ID)

    assert post.call_args.args[0].endswith("/flow_runs/run-1/set_state")
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
    emit.assert_called_once_with(event)


def test_add_analysis_event_never_raises():
    handler = _handler()
    handler.analysis_events_collection.insert_one.side_effect = RuntimeError("db down")

    handler.add_analysis_event("viewer", PROJECT_ID, "Cojo", "message", flow_run_id="run-1")


@pytest.mark.parametrize("state, level", [
    ({"status": "Running", "stage": "Cojo", "message": "Running COJO"}, "info"),
    ({"status": "Failed", "stage": "Cojo", "message": "COJO failed"}, "error"),
])
def test_save_analysis_state_records_an_event(state, level):
    handler = _handler()
    handler.add_analysis_event = MagicMock()
    handler.projects_collection.update_one.return_value.modified_count = 1

    handler.save_analysis_state("viewer", PROJECT_ID, state)

    handler.add_analysis_event.assert_called_once_with(
        "viewer", PROJECT_ID, stage="Cojo", message=state["message"], level=level
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
        {"message": "first", "timestamp": datetime(2026, 1, 1)},
    ]

    response = asyncio.run(projects_routes.get_analysis_events(PROJECT_ID, "viewer", handler))

    assert json.loads(response.body) == [
        {"message": "first", "timestamp": "2026-01-01T00:00:00+00:00"}
    ]
    handler.analysis_events_collection.find.assert_called_once_with(
        {"user_id": "viewer", "project_id": PROJECT_ID}, {"_id": 0}
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

    utils.record_analysis_event("viewer", PROJECT_ID, "Fine_mapping", "Batch 1/2 done", details={"regions": 3})

    projects.add_analysis_event.assert_called_once_with(
        "viewer", PROJECT_ID, "Fine_mapping", "Batch 1/2 done (retry 1)",
        details={"regions": 3, "attempt": 2},
    )
