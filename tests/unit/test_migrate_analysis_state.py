import importlib.util
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from bson import ObjectId

from tests.conftest import REPOSITORY_ROOT

_spec = importlib.util.spec_from_file_location(
    "migrate_analysis_state_to_mongo",
    REPOSITORY_ROOT / "scripts" / "migrate_analysis_state_to_mongo.py",
)
migration = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migration)

PROJECT_ID = "64b000000000000000000001"
VALID_STATE = '{"status": "Completed", "state_updated_at": "2026-01-01T00:00:00+00:00"}'


def _migrate(tmp_path, *, project=None, content=VALID_STATE, matches=True, dry_run=False):
    path = tmp_path / "states" / "user-1" / PROJECT_ID / "analysis_state.json"
    path.parent.mkdir(parents=True)
    path.write_text(content)
    projects = MagicMock()
    projects.find_one.side_effect = [project, {"_id": 1} if matches else None]
    projects.update_one.return_value.modified_count = int(matches)
    return projects, migration.migrate(projects, tmp_path, dry_run=dry_run)


@pytest.mark.parametrize(
    "kwargs, outcome",
    [
        ({"project": {}}, "migrated"),
        ({"project": {}, "matches": False}, "skipped"),
        ({"project": None}, "orphaned"),
        ({"project": {}, "content": "{not json"}, "unreadable"),
        ({"project": {}, "dry_run": True}, "migrated"),
    ],
)
def test_migrate_counts(tmp_path, kwargs, outcome):
    projects, counts = _migrate(tmp_path, **kwargs)

    assert counts[outcome] == 1 and sum(counts.values()) == 1
    if kwargs.get("dry_run"):
        projects.update_one.assert_not_called()


def test_migrate_writes_only_missing_or_older_states(tmp_path):
    projects, _ = _migrate(tmp_path, project={})

    query, update = projects.update_one.call_args.args
    file_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert query == {
        "_id": ObjectId(PROJECT_ID),
        "user_id": "user-1",
        "$or": [
            {"analysis_state": {"$exists": False}},
            {"analysis_state.updated_at": {"$lt": file_time}},
        ],
    }
    assert update == {
        "$set": {"analysis_state.status": "Completed", "analysis_state.updated_at": file_time}
    }
    assert (tmp_path / "states" / "user-1" / PROJECT_ID / "analysis_state.json").exists()
