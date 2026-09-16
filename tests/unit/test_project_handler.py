from unittest.mock import MagicMock

from bson import ObjectId

from src.db.project_handler import ProjectHandler


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
    handler.db = MagicMock()
    return handler


def test_fork_project_marks_copy_as_non_demo_and_records_template(monkeypatch):
    handler = _handler()
    template_id = "64b000000000000000000001"
    source = {
        "_id": ObjectId(template_id),
        "user_id": "owner",
        "name": "Canonical demo",
        "is_demo": True,
        "is_template": True,
        "demo_slug": "canonical",
    }
    handler.projects_collection.find_one.return_value = source
    handler._copy_collection_docs = MagicMock(return_value=[])
    monkeypatch.setattr(handler, "get_analysis_state_path", lambda *_: "/nonexistent/state")

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


def test_delete_project_data_removes_tissue_selections(monkeypatch):
    handler = _handler()
    project_id = "64b000000000000000000001"
    handler.load_analysis_state = MagicMock(return_value=None)
    handler.hypothesis_collection.find.return_value = []
    handler.projects_collection.find_one.return_value = None
    handler.get_analysis_state_path = MagicMock(return_value="/nonexistent/state")
    handler.get_project_analysis_path = MagicMock(return_value="/nonexistent/analysis")
    monkeypatch.setattr("src.db.project_handler.os.path.exists", lambda _path: False)

    handler._delete_project_data("viewer", project_id)

    handler.db["tissue_selections"].delete_many.assert_called_once_with(
        {"user_id": "viewer", "project_id": project_id}
    )
