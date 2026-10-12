#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_root))

from src.db.demo_template_handler import DemoTemplateError, DemoTemplateHandler
from src.services.storage import create_minio_client_from_env


def _load_env() -> None:
    env_path = _root / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def _get_handler() -> DemoTemplateHandler:
    mongodb_uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("DB_NAME")
    if not mongodb_uri or not db_name:
        raise DemoTemplateError("MONGODB_URI and DB_NAME must be set in the environment.")
    return DemoTemplateHandler(mongodb_uri, db_name)


def _get_storage():
    storage = create_minio_client_from_env()
    if storage is None:
        raise DemoTemplateError(
            "MinIO is not configured. Set MINIO_ENDPOINT, MINIO_ACCESS_KEY, "
            "MINIO_SECRET_KEY, and optionally MINIO_BUCKET."
        )
    return storage

def cmd_list(args: argparse.Namespace) -> int:
    handler = _get_handler()
    templates = handler.list_templates(active_only=args.active_only)
    if not templates:
        print("No demo seeds registered.")
        return 0
    print(json.dumps(templates, indent=2, default=str))
    return 0


def cmd_register(args: argparse.Namespace) -> int:
    handler = _get_handler()
    result = handler.register_seed(
        project_id=args.project_id,
        slug=args.slug,
        display_name=args.display_name,
        sort_order=args.sort_order,
        force=args.force,
    )
    print("Demo seed registered successfully.")
    print(json.dumps(result, indent=2, default=str))
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    handler = _get_handler()
    storage = _get_storage()
    result = handler.export_seed_to_minio(
        args.slug,
        storage,
        seed_version=args.seed_version,
    )
    print("Demo seed exported successfully.")
    print(json.dumps(result, indent=2, default=str))
    return 0


def cmd_import(args: argparse.Namespace) -> int:
    handler = _get_handler()
    storage = _get_storage()
    result = handler.import_seed_from_minio(
        args.slug,
        storage,
        force=args.force,
        dry_run=args.dry_run,
    )
    label = "Demo seed dry-run finished." if args.dry_run else "Demo seed import finished."
    print(label)
    print(json.dumps(result, indent=2, default=str))
    return 0

def cmd_sync(args: argparse.Namespace) -> int:
    handler = _get_handler()
    storage = _get_storage()
    results = handler.ensure_seeds_from_minio(storage)
    print(json.dumps(results, indent=2, default=str))
    return 0 if not results["failed"] else 1

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage demo project seeds in MinIO.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List registered demo seeds")
    list_parser.add_argument("--active-only", action="store_true")
    list_parser.set_defaults(func=cmd_list)

    register_parser = subparsers.add_parser(
        "register",
        help="Register a completed project in the local demo catalog",
    )
    register_parser.add_argument("--project-id", required=True)
    register_parser.add_argument("--slug", required=True)
    register_parser.add_argument("--display-name", required=True)
    register_parser.add_argument("--sort-order", type=int, default=1)
    register_parser.add_argument("--force", action="store_true")
    register_parser.set_defaults(func=cmd_register)

    export_parser = subparsers.add_parser(
        "export",
        help="Export a registered demo seed bundle to MinIO",
    )
    export_parser.add_argument("--slug", required=True)
    export_parser.add_argument("--seed-version", type=int, default=1)
    export_parser.set_defaults(func=cmd_export)

    import_parser = subparsers.add_parser(
        "import",
        help="Import a demo seed bundle from MinIO into MongoDB",
    )
    import_parser.add_argument("--slug", required=True)
    import_parser.add_argument("--force", action="store_true")
    import_parser.add_argument("--dry-run", action="store_true")
    import_parser.set_defaults(func=cmd_import)

    sync_parser = subparsers.add_parser(
        "sync",
        help="Import any MinIO demo seed bundles missing locally",
    )
    sync_parser.set_defaults(func=cmd_sync)
    return parser

def main() -> int:
    _load_env()
    args = build_parser().parse_args()
    try:
        return args.func(args)
    except DemoTemplateError as exc:
        logger.error(str(exc))
        return 1
    except Exception as exc:
        logger.exception(f"Unexpected error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
