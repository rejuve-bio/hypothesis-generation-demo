#!/usr/bin/env python3
"""
Publish / list OpenTargets Postgres snapshots in MinIO.

Deployments restore these in minutes instead of re-loading from the EBI FTP
(see src/services/ot_db_bootstrap.py). Publishing is deliberately manual:

  # after a deployment has loaded OT_RELEASE (e.g. via the EBI fallback)
  uv run python scripts/pg_releases.py publish
  uv run python scripts/pg_releases.py list

Snapshots live at <OT_DB_SNAPSHOT_MINIO_PREFIX>/<release>/schema-v<N>/
{ot.dump, manifest.json}; the manifest is uploaded last.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_root))

from src.services.ot_db_bootstrap import BootstrapError, publish_snapshot  # noqa: E402
from src.services.storage import create_minio_client_from_env  # noqa: E402


def _load_env() -> None:
    env_path = _root / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)


def _prefix() -> str:
    return (os.getenv("OT_DB_SNAPSHOT_MINIO_PREFIX") or "pg-releases/opentargets").strip("/")


def _get_storage():
    storage = create_minio_client_from_env()
    if storage is None:
        raise SystemExit(
            "MinIO is not configured. Set MINIO_ENDPOINT, MINIO_ACCESS_KEY, "
            "MINIO_SECRET_KEY, and optionally MINIO_BUCKET."
        )
    return storage


def cmd_publish(args: argparse.Namespace) -> int:
    storage = _get_storage()
    try:
        manifest = publish_snapshot(storage, _prefix(), force=args.force)
    except BootstrapError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(manifest, indent=2))
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    storage = _get_storage()
    prefix = f"{_prefix()}/"
    paginator = storage.client.get_paginator("list_objects_v2")
    found = False
    for page in paginator.paginate(Bucket=storage.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            print(f"{obj['Size'] / 1e9:8.2f} GB  {obj['LastModified']:%Y-%m-%d %H:%M}  {obj['Key']}")
            found = True
    if not found:
        print(f"(no objects under s3://{storage.bucket}/{prefix})")
    return 0


def main() -> int:
    _load_env()
    parser = argparse.ArgumentParser(description="Publish / list OpenTargets Postgres snapshots in MinIO.")
    sub = parser.add_subparsers(dest="command", required=True)

    publish_parser = sub.add_parser("publish", help="Dump the loaded OT tables and upload them as a snapshot")
    publish_parser.add_argument("--force", action="store_true", help="overwrite an existing snapshot")
    publish_parser.set_defaults(func=cmd_publish)

    list_parser = sub.add_parser("list", help="List published snapshots")
    list_parser.set_defaults(func=cmd_list)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
