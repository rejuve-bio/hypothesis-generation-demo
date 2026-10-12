#!/usr/bin/env python3
"""Upload GWAS manifest and phenotype seed files to MinIO for deployment fallback."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_root))

from src.services.seed_assets import seed_asset_minio_key, seed_assets_minio_prefix
from src.services.storage import create_minio_client_from_env


def _load_env() -> None:
    env_path = _root / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def _get_storage():
    storage = create_minio_client_from_env()
    if storage is None:
        raise SystemExit(
            "MinIO is not configured. Set MINIO_ENDPOINT, MINIO_ACCESS_KEY, "
            "MINIO_SECRET_KEY, and optionally MINIO_BUCKET."
        )
    return storage


def _resolve_local_path(path: str) -> str | None:
    """Map container paths (/app/data/...) to repo data/ when running on the host."""
    if os.path.isfile(path):
        return path
    if path.startswith("/app/data/"):
        candidate = _root / "data" / os.path.basename(path)
        if candidate.is_file():
            return str(candidate)
    return None


def _default_seed_paths() -> list[str]:
    paths: list[str] = []
    for env_name in ("GWAS_MANIFEST_PATH", "PHENOTYPES_JSON_PATH", "FINNGEN_MANIFEST_PATH"):
        raw = os.getenv(env_name, "").strip()
        if not raw:
            continue
        resolved = _resolve_local_path(raw)
        if resolved:
            paths.append(resolved)
    return paths


def cmd_upload(args: argparse.Namespace) -> int:
    storage = _get_storage()
    prefix = seed_assets_minio_prefix()

    files = args.files or _default_seed_paths()
    if not files:
        raise SystemExit(
            "No files specified. Pass paths as arguments or set GWAS_MANIFEST_PATH / "
            "PHENOTYPES_JSON_PATH in the environment."
        )

    uploaded = 0
    for path in files:
        if not os.path.isfile(path):
            logger.error(f"Not a file: {path}")
            continue
        key = seed_asset_minio_key(path)
        if storage.upload_file(path, key):
            print(f"uploaded {path} -> s3://{storage.bucket}/{key}")
            uploaded += 1
        else:
            logger.error(f"Upload failed: {path}")

    print(f"Done. prefix={prefix!r} uploaded={uploaded}/{len(files)}")
    return 0 if uploaded == len(files) else 1


def cmd_list(args: argparse.Namespace) -> int:
    storage = _get_storage()
    prefix = f"{seed_assets_minio_prefix()}/"
    paginator = storage.client.get_paginator("list_objects_v2")
    found = False
    for page in paginator.paginate(Bucket=storage.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            print(obj["Key"])
            found = True
    if not found:
        print(f"(no objects under s3://{storage.bucket}/{prefix})")
    return 0


def main() -> int:
    _load_env()
    parser = argparse.ArgumentParser(
        description="Upload seed assets (GWAS manifests, phenotypes JSON) to MinIO."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    upload_parser = sub.add_parser(
        "upload",
        help="Upload seed files to MinIO (basename under seed-assets/v1/)",
    )
    upload_parser.add_argument(
        "files",
        nargs="*",
        help="Local file paths (default: GWAS_MANIFEST_PATH, PHENOTYPES_JSON_PATH from env)",
    )
    upload_parser.set_defaults(func=cmd_upload)

    list_parser = sub.add_parser("list", help="List objects under the seed assets prefix")
    list_parser.set_defaults(func=cmd_list)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
