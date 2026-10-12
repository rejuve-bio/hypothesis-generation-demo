"""Load small seed files (GWAS manifests, phenotypes JSON) from disk or MinIO."""

from __future__ import annotations

import os
from typing import Optional, Tuple

from loguru import logger

from src.services.storage import MinIOStorage


def seed_assets_minio_prefix() -> str:
    return (os.getenv("SEED_ASSETS_MINIO_PREFIX") or "seed-assets/v1").strip("/")


def seed_asset_minio_key(local_path: str) -> str:
    """MinIO object key for a seed file, derived from its basename."""
    basename = os.path.basename(local_path)
    if not basename:
        raise ValueError(f"Cannot derive MinIO key from path: {local_path!r}")
    return f"{seed_assets_minio_prefix()}/{basename}"


def _skip_local_check() -> bool:
    """Dev-only: force MinIO load to test fallback (SEED_ASSETS_SKIP_LOCAL=true)."""
    return os.getenv("SEED_ASSETS_SKIP_LOCAL", "").lower() in ("1", "true", "yes")


def load_seed_text(
    local_path: str,
    storage: Optional[MinIOStorage],
) -> Optional[Tuple[str, str]]:
    """
    Load seed file text: local disk first, then MinIO (in memory).

    Returns:
        (content, source) with source ``local`` or ``minio``, or None if unavailable.
    """
    if not _skip_local_check() and os.path.exists(local_path):
        with open(local_path, encoding="utf-8") as f:
            return f.read(), "local"

    if storage is None:
        logger.warning(
            f"Seed file not found at {local_path} and MinIO is not configured"
        )
        return None

    key = seed_asset_minio_key(local_path)
    content = storage.download_string(key)
    if content is None:
        logger.warning(
            f"Seed file not found locally or in MinIO "
            f"(s3://{storage.bucket}/{key}): {local_path}"
        )
        return None

    logger.info(f"Loaded seed asset from MinIO s3://{storage.bucket}/{key}")
    return content, "minio"
