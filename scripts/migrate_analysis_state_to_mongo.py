"""Copy analysis_state.json files into each project's `analysis_state` field.

Standalone (stdlib + pymongo) so it can run inside the API pod. Reads
MONGODB_URI and DB_NAME from the environment. Idempotent: a state is only
written when the project has none or an older one, and no file is deleted.

Usage: python migrate_analysis_state_to_mongo.py [--data-dir data] [--dry-run]
"""

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from bson import ObjectId
from pymongo import MongoClient


def _load_state(path: Path) -> dict | None:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(state, dict):
        return None
    updated_at = state.pop("state_updated_at", None)
    try:
        updated_at = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        updated_at = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
    # MongoDB keeps milliseconds; truncate so re-runs compare equal.
    state["updated_at"] = updated_at.replace(microsecond=updated_at.microsecond // 1000 * 1000)
    return state


def migrate(projects, data_dir: Path, dry_run: bool = False) -> dict:
    counts = {"migrated": 0, "skipped": 0, "orphaned": 0, "unreadable": 0}
    for path in sorted(data_dir.glob("states/*/*/analysis_state.json")):
        user_id, project_id = path.parent.parent.name, path.parent.name
        if not ObjectId.is_valid(project_id):
            counts["orphaned"] += 1
            continue
        project_query = {"_id": ObjectId(project_id), "user_id": user_id}
        if projects.find_one(project_query, {"_id": 1}) is None:
            counts["orphaned"] += 1
            continue
        state = _load_state(path)
        if state is None:
            counts["unreadable"] += 1
            continue
        # Copy when missing, or when the file moved on since an earlier run.
        query = {
            **project_query,
            "$or": [
                {"analysis_state": {"$exists": False}},
                {"analysis_state.updated_at": {"$lt": state["updated_at"]}},
            ],
        }
        if dry_run:
            changed = projects.find_one(query, {"_id": 1}) is not None
        else:
            update = {"$set": {f"analysis_state.{key}": value for key, value in state.items()}}
            changed = projects.update_one(query, update).modified_count > 0
        counts["migrated" if changed else "skipped"] += 1
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", default="data", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    client = MongoClient(os.environ["MONGODB_URI"])
    projects = client[os.environ["DB_NAME"]]["projects"]
    counts = migrate(projects, args.data_dir, dry_run=args.dry_run)
    prefix = "[dry-run] " if args.dry_run else ""
    print(prefix + " ".join(f"{key}={value}" for key, value in counts.items()))


if __name__ == "__main__":
    main()
