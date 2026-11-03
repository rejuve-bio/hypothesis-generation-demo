#!/usr/bin/env python3
"""
Ensure the OpenTargets Postgres DB holds the pinned release (OT_RELEASE).

Runs as the `ot-db-bootstrap` one-shot compose service on every deployment;
the pipeline workers start only after it exits 0. Restores a pg_dump snapshot
from MinIO when one exists for the release, otherwise loads from the EBI FTP.
See src/services/ot_db_bootstrap.py for details.

Environment:
  OT_RELEASE                   pinned OpenTargets release, e.g. 25.09 (required; 'latest' rejected)
  OT_DB_BOOTSTRAP              auto (default) | skip | minio-only | ebi-only
  OT_DB_SNAPSHOT_MINIO_PREFIX  default: pg-releases/opentargets
  OT_DB_SNAPSHOT_CACHE_DIR     default: data/pg_releases
  CREDIBLE_SETS_DB_*           target database
  MINIO_*                      snapshot source

Usage:
  python scripts/bootstrap_ot_db.py [--release 25.09] [--mode ebi-only]
"""

import argparse
import dataclasses
import sys
from pathlib import Path

from dotenv import load_dotenv

_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_root))

from src.services.ot_db_bootstrap import MODES, BootstrapConfig, run_bootstrap  # noqa: E402


def main() -> int:
    # Container env (compose env_file) wins; .env only fills gaps on host runs.
    load_dotenv(_root / ".env", override=False)
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--release", help="override OT_RELEASE")
    parser.add_argument("--mode", choices=MODES, help="override OT_DB_BOOTSTRAP")
    args = parser.parse_args()

    cfg = BootstrapConfig.from_env()
    if args.release:
        cfg = dataclasses.replace(cfg, release=args.release)
    if args.mode:
        cfg = dataclasses.replace(cfg, mode=args.mode)
    return run_bootstrap(cfg)


if __name__ == "__main__":
    raise SystemExit(main())
