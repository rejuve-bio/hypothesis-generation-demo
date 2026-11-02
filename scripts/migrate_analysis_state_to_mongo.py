"""Copy analysis_state.json files into each project's `analysis_state` field.

Standalone (stdlib + pymongo) so it can run inside the API pod. Reads
MONGODB_URI and DB_NAME from the environment. Idempotent: a project that
already has `analysis_state` is left untouched, and no file is deleted.

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
        state["updated_at"] = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        state["updated_at"] = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
    return state


def migrate(projects, data_dir: Path, dry_run: bool = False) -> dict:
    counts = {"migrated": 0, "skipped": 0, "orphaned": 0, "unreadable": 0}
    for path in sorted(data_dir.glob("states/*/*/analysis_state.json")):
        user_id, project_id = path.parent.parent.name, path.parent.name
        if not ObjectId.is_valid(project_id):
            counts["orphaned"] += 1
            continue
        project = projects.find_one(
            {"_id": ObjectId(project_id), "user_id": user_id}, {"analysis_state": 1}
        )
        if project is None:
            counts["orphaned"] += 1
            continue
        if "analysis_state" in project:
            counts["skipped"] += 1
            continue
        state = _load_state(path)
        if state is None:
            counts["unreadable"] += 1
            continue
        if not dry_run:
            result = projects.update_one(
                {
                    "_id": ObjectId(project_id),
                    "user_id": user_id,
                    "analysis_state": {"$exists": False},
                },
                {"$set": {"analysis_state": state}},
            )
            if result.modified_count == 0:
                counts["skipped"] += 1
                continue
        counts["migrated"] += 1
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
