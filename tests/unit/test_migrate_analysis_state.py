import importlib.util
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

from bson import ObjectId

from tests.conftest import REPOSITORY_ROOT

_spec = importlib.util.spec_from_file_location(
    "migrate_analysis_state_to_mongo",
    REPOSITORY_ROOT / "scripts" / "migrate_analysis_state_to_mongo.py",
)
migration = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migration)

MISSING_STATE = "64b000000000000000000001"
HAS_STATE = "64b000000000000000000002"
NO_PROJECT = "64b000000000000000000003"
CORRUPT = "64b000000000000000000004"


def _write_state(data_dir, project_id, content):
    path = data_dir / "states" / "user-1" / project_id / "analysis_state.json"
    path.parent.mkdir(parents=True)
    path.write_text(content)


def _setup(tmp_path):
    _write_state(
        tmp_path,
        MISSING_STATE,
        json.dumps({"status": "Completed", "state_updated_at": "2026-01-01T00:00:00+00:00"}),
    )
    _write_state(tmp_path, HAS_STATE, json.dumps({"status": "Completed"}))
    _write_state(tmp_path, NO_PROJECT, json.dumps({"status": "Completed"}))
    _write_state(tmp_path, CORRUPT, "{not json")

    projects = {
        ObjectId(MISSING_STATE): {},
        ObjectId(HAS_STATE): {"analysis_state": {"status": "Running"}},
        ObjectId(CORRUPT): {},
    }
    collection = MagicMock()
    collection.find_one.side_effect = lambda query, _projection: projects.get(query["_id"])
    collection.update_one.return_value.modified_count = 1
    return collection


def test_migrate_copies_only_missing_states_and_reports_counts(tmp_path):
    collection = _setup(tmp_path)

    counts = migration.migrate(collection, tmp_path)

    assert counts == {"migrated": 1, "skipped": 1, "orphaned": 1, "unreadable": 1}
    query, update = collection.update_one.call_args.args
    assert query == {
        "_id": ObjectId(MISSING_STATE),
        "user_id": "user-1",
        "analysis_state": {"$exists": False},
    }
    assert update == {
        "$set": {
            "analysis_state": {
                "status": "Completed",
                "updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            }
        }
    }
    assert (tmp_path / "states" / "user-1" / MISSING_STATE / "analysis_state.json").exists()


def test_migrate_dry_run_writes_nothing(tmp_path):
    collection = _setup(tmp_path)

    counts = migration.migrate(collection, tmp_path, dry_run=True)

    assert counts["migrated"] == 1
    collection.update_one.assert_not_called()
