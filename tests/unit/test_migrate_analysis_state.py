import importlib.util
import json
from datetime import datetime, timezone
from types import SimpleNamespace

from bson import ObjectId

from tests.conftest import REPOSITORY_ROOT

_spec = importlib.util.spec_from_file_location(
    "migrate_analysis_state_to_mongo",
    REPOSITORY_ROOT / "scripts" / "migrate_analysis_state_to_mongo.py",
)
migration = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migration)

MISSING_STATE = "64b000000000000000000001"
CURRENT_STATE = "64b000000000000000000002"
OUTDATED_STATE = "64b000000000000000000003"
NO_PROJECT = "64b000000000000000000004"
CORRUPT = "64b000000000000000000005"

FILE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


class FakeProjects:
    """Supports only the queries the migration issues."""

    def __init__(self, docs: dict):
        self.docs = docs
        self.writes = 0

    def _match(self, query):
        doc = self.docs.get(query["_id"])
        if doc is None or doc["user_id"] != query["user_id"]:
            return None
        if "$or" not in query:
            return doc
        state = doc.get("analysis_state")
        newer = query["$or"][1]["analysis_state.updated_at"]["$lt"]
        if state is None or state.get("updated_at", newer) < newer:
            return doc
        return None

    def find_one(self, query, _projection):
        return self._match(query)

    def update_one(self, query, update):
        doc = self._match(query)
        if doc is not None:
            state = doc.setdefault("analysis_state", {})
            for key, value in update["$set"].items():
                state[key.removeprefix("analysis_state.")] = value
            self.writes += 1
        return SimpleNamespace(modified_count=int(doc is not None))


def _write_state(data_dir, project_id, content):
    path = data_dir / "states" / "user-1" / project_id / "analysis_state.json"
    path.parent.mkdir(parents=True)
    path.write_text(content)


def _setup(tmp_path):
    completed = json.dumps(
        {"status": "Completed", "state_updated_at": FILE_TIME.isoformat()}
    )
    for project_id in (MISSING_STATE, CURRENT_STATE, OUTDATED_STATE, NO_PROJECT):
        _write_state(tmp_path, project_id, completed)
    _write_state(tmp_path, CORRUPT, "{not json")

    return FakeProjects({
        ObjectId(MISSING_STATE): {"user_id": "user-1"},
        ObjectId(CURRENT_STATE): {
            "user_id": "user-1",
            "analysis_state": {"status": "Running", "updated_at": FILE_TIME},
        },
        ObjectId(OUTDATED_STATE): {
            "user_id": "user-1",
            "analysis_state": {
                "status": "Running",
                "flow_run_id": "run-1",
                "updated_at": datetime(2025, 12, 31, tzinfo=timezone.utc),
            },
        },
        ObjectId(CORRUPT): {"user_id": "user-1"},
    })


def test_migrate_copies_missing_and_outdated_states(tmp_path):
    projects = _setup(tmp_path)

    counts = migration.migrate(projects, tmp_path)

    assert counts == {"migrated": 2, "skipped": 1, "orphaned": 1, "unreadable": 1}
    assert projects.docs[ObjectId(MISSING_STATE)]["analysis_state"] == {
        "status": "Completed",
        "updated_at": FILE_TIME,
    }
    assert projects.docs[ObjectId(OUTDATED_STATE)]["analysis_state"] == {
        "status": "Completed",
        "flow_run_id": "run-1",
        "updated_at": FILE_TIME,
    }
    assert projects.docs[ObjectId(CURRENT_STATE)]["analysis_state"]["status"] == "Running"
    assert (tmp_path / "states" / "user-1" / MISSING_STATE / "analysis_state.json").exists()


def test_migrate_is_idempotent(tmp_path):
    projects = _setup(tmp_path)
    migration.migrate(projects, tmp_path)

    counts = migration.migrate(projects, tmp_path)

    assert counts["migrated"] == 0
    assert counts["skipped"] == 3


def test_migrate_dry_run_writes_nothing(tmp_path):
    projects = _setup(tmp_path)

    counts = migration.migrate(projects, tmp_path, dry_run=True)

    assert counts["migrated"] == 2
    assert projects.writes == 0
