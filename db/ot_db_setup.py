#!/usr/bin/env python3
"""
Set up the shared OpenTargets Postgres database used by both the Python
pipeline and the Prolog backend. Standalone: it does not import the app.

  python ot_db_setup.py ensure    # make sure the pinned OT_RELEASE is loaded (run on deploy)
  python ot_db_setup.py publish   # upload the loaded DB to MinIO as a snapshot
  python ot_db_setup.py list      # list published snapshots

`ensure`:
  1. If public.ot_load_state records the pinned release + SCHEMA_VERSION and
     the live row counts match the counts recorded at load time, exit 0.
  2. Otherwise restore a pg_dump snapshot from MinIO
     (<prefix>/<release>/schema-v<N>/{manifest.json, ot.dump}), verifying its
     sha256 and row counts.
  3. If MinIO has no usable snapshot, load the release from the EBI FTP with
     pg_parquet (COPY ... FROM '<parquet url>'), then transform with SQL
     (sql/03_transform.sql).

Both paths build into the `ot_staging` schema and are swapped into `public` in
one transaction together with the ot_load_state marker, so readers never see a
partial table. A crashed run leaves `ot_staging`/`ot_raw` behind; the next run
drops them. The table definitions live in sql/.

Configuration (environment):
  OT_RELEASE                   required; pinned release, e.g. 25.09 ('latest' is
                               rejected). Must include FinnGen R12 studies.
  OT_DB_BOOTSTRAP              auto (default) | minio-only | ebi-only | skip (CI)
  OT_DB_SNAPSHOT_MINIO_PREFIX  default pg-releases/opentargets
  OT_DB_SNAPSHOT_CACHE_DIR     local snapshot download dir, default /cache
  CREDIBLE_SETS_DB_HOST/PORT/NAME/USER/PASSWORD   target database (superuser:
                               pg_parquet reads https URLs)
  MINIO_ENDPOINT/ACCESS_KEY/SECRET_KEY/BUCKET/USE_SSL   snapshot store

To force a reload: DELETE FROM ot_load_state; then run `ensure` again.
"""

import argparse
import datetime as _dt
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
from psycopg2 import sql

log = logging.getLogger("ot-db")

SCHEMA_VERSION = 2
SQL_DIR = Path(__file__).resolve().parent / "sql"

DATASET = "opentargets"
STAGING_SCHEMA = "ot_staging"
RAW_SCHEMA = "ot_raw"
OT_TABLES = (
    "study", "biosample", "variant", "enhancer",
    "credible_set", "credible_set_variants", "gwas_study_index",
)
# Tables from schema v1; dropped on swap so data isn't kept twice.
LEGACY_TABLES = ("credible_sets", "opentargets_studies")
# ot_raw table <- OpenTargets release output folder, in load order.
RAW_SOURCES = (
    ("study", "study"),
    ("biosample", "biosample"),
    ("variant", "variant"),
    ("interval", "interval"),
    ("credible_set", "credible_set"),
)

EBI_BASE_URL = "https://ftp.ebi.ac.uk/pub/databases/opentargets/platform"
HTTP_TIMEOUT_S = 300
FILE_ATTEMPTS = 3

MODES = ("auto", "skip", "minio-only", "ebi-only")
MANIFEST_FORMAT_VERSION = 1
DUMP_NAME = "ot.dump"
MANIFEST_NAME = "manifest.json"
# Arbitrary constant: serialises concurrent ensure / publish runs.
ADVISORY_LOCK_KEY = 0x07_DB_B007
FINNGEN_PREFIX = "FINNGEN_R12_"

_RELEASE_RE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._-]*$")

LOAD_STATE_DDL = """
CREATE TABLE IF NOT EXISTS public.ot_load_state (
    dataset          TEXT PRIMARY KEY,
    ot_release       TEXT NOT NULL,
    schema_version   INTEGER NOT NULL,
    source           TEXT NOT NULL,
    row_counts       JSONB NOT NULL,
    snapshot_sha256  TEXT,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


class SetupError(RuntimeError):
    pass


# ── config ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Config:
    release: str
    mode: str = "auto"
    minio_prefix: str = "pg-releases/opentargets"
    cache_dir: str = "/cache"
    restore_jobs: int = 4
    max_files: Optional[int] = None  # testing only: parquet files per folder

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            release=os.getenv("OT_RELEASE", "").strip(),
            mode=os.getenv("OT_DB_BOOTSTRAP", "auto").strip().lower() or "auto",
            minio_prefix=(os.getenv("OT_DB_SNAPSHOT_MINIO_PREFIX") or "pg-releases/opentargets").strip("/"),
            cache_dir=os.getenv("OT_DB_SNAPSHOT_CACHE_DIR", "/cache"),
            restore_jobs=int(os.getenv("OT_DB_RESTORE_JOBS", "4")),
        )


def validate_release(release: str) -> str:
    """OT_RELEASE must be an explicit, pinned release tag (e.g. '25.09')."""
    if not release:
        raise SetupError("OT_RELEASE is not set — pin an OpenTargets release, e.g. OT_RELEASE=25.09")
    if release.lower() == "latest":
        raise SetupError("OT_RELEASE=latest is not allowed — pin an explicit release, e.g. 25.09")
    if not _RELEASE_RE.match(release):
        raise SetupError(f"OT_RELEASE={release!r} is not a valid release tag")
    return release


def snapshot_keys(prefix: str, release: str, schema_version: int = SCHEMA_VERSION) -> dict:
    base = f"{prefix.strip('/')}/{release}/schema-v{schema_version}"
    return {"base": base, "manifest": f"{base}/{MANIFEST_NAME}", "dump": f"{base}/{DUMP_NAME}"}


# ── load state / freshness ───────────────────────────────────────────────────

@dataclass(frozen=True)
class LoadState:
    ot_release: str
    schema_version: int
    source: str
    row_counts: dict
    snapshot_sha256: Optional[str] = None
    loaded_at: Optional[_dt.datetime] = None


def check_current(state: Optional[LoadState], live_counts: Optional[dict], release: str,
                  schema_version: int = SCHEMA_VERSION) -> tuple:
    """
    Pure freshness check. Returns (is_current, reason).

    Current only if a marker exists for the pinned release + schema version
    AND every table's live row count equals the count recorded when the load
    completed — so a partial, truncated or hand-edited DB is reloaded.
    """
    if state is None:
        return False, "no ot_load_state marker (never loaded, or loaded before load tracking existed)"
    if state.ot_release != release:
        return False, f"loaded release {state.ot_release!r} != pinned OT_RELEASE {release!r}"
    if state.schema_version != schema_version:
        return False, f"loaded schema v{state.schema_version} != setup schema v{schema_version}"
    if live_counts is None:
        return False, "one or more OT tables are missing"
    for table in OT_TABLES:
        expected = state.row_counts.get(table)
        actual = live_counts.get(table)
        if expected is None or actual != expected:
            return False, f"{table}: {actual} rows live, {expected} recorded at load time"
    return True, f"release {release} (schema v{schema_version}, source={state.source}) is loaded and complete"


def read_load_state(conn) -> Optional[LoadState]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT ot_release, schema_version, source, row_counts, snapshot_sha256, loaded_at "
            "FROM public.ot_load_state WHERE dataset = %s",
            (DATASET,),
        )
        row = cur.fetchone()
    conn.rollback()
    return LoadState(**row) if row else None


def table_row_counts(conn, schema: str) -> Optional[dict]:
    """Exact COUNT(*) per table in *schema*, or None if any table is missing."""
    counts = {}
    with conn.cursor() as cur:
        for table in OT_TABLES:
            cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
            if cur.fetchone()[0] is None:
                conn.rollback()
                return None
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(table)))
            counts[table] = cur.fetchone()[0]
    conn.rollback()
    return counts


# ── manifest / checksum ──────────────────────────────────────────────────────

def parse_manifest(text: str, release: str, schema_version: int = SCHEMA_VERSION) -> dict:
    """Parse + sanity-check a snapshot manifest against the pinned release."""
    try:
        manifest = json.loads(text)
    except (TypeError, ValueError) as exc:
        raise SetupError(f"manifest is not valid JSON: {exc}") from exc
    if manifest.get("format_version") != MANIFEST_FORMAT_VERSION:
        raise SetupError(f"unsupported manifest format_version {manifest.get('format_version')!r}")
    if manifest.get("ot_release") != release:
        raise SetupError(f"manifest is for release {manifest.get('ot_release')!r}, expected {release!r}")
    if manifest.get("schema_version") != schema_version:
        raise SetupError(f"manifest schema v{manifest.get('schema_version')}, expected v{schema_version}")
    dump = manifest.get("dump") or {}
    if not re.fullmatch(r"[0-9a-f]{64}", str(dump.get("sha256", ""))):
        raise SetupError("manifest has no valid dump.sha256")
    if set(manifest.get("row_counts") or {}) != set(OT_TABLES):
        raise SetupError(f"manifest row_counts must cover exactly {OT_TABLES}")
    return manifest


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


# ── MinIO ────────────────────────────────────────────────────────────────────

class SnapshotStore:
    """Minimal S3/MinIO client for snapshots (same env vars as the app's MinIO client)."""

    def __init__(self, client, bucket: str):
        self.client = client
        self.bucket = bucket

    @classmethod
    def from_env(cls) -> Optional["SnapshotStore"]:
        endpoint = os.getenv("MINIO_ENDPOINT")
        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")
        if not all([endpoint, access_key, secret_key]):
            return None
        import boto3
        from botocore.client import Config as BotoConfig

        use_ssl = os.getenv("MINIO_USE_SSL", "false").lower() == "true"
        client = boto3.client(
            "s3",
            endpoint_url=f"{'https' if use_ssl else 'http'}://{endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"},
                              connect_timeout=60, read_timeout=300, retries={"max_attempts": 3}),
        )
        return cls(client, os.getenv("MINIO_BUCKET", "hypothesis"))

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def read_text(self, key: str) -> str:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read().decode("utf-8")

    def download(self, key: str, path: str) -> None:
        self.client.download_file(self.bucket, key, path)

    def upload(self, path: str, key: str) -> None:
        self.client.upload_file(path, self.bucket, key)

    def write_text(self, text: str, key: str) -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=text.encode("utf-8"),
                               ContentType="application/json")

    def list_keys(self, prefix: str):
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            yield from page.get("Contents", [])


# ── connections / subprocess ─────────────────────────────────────────────────

def connection_params() -> dict:
    return {
        "host": os.environ.get("CREDIBLE_SETS_DB_HOST", "localhost"),
        "port": int(os.environ.get("CREDIBLE_SETS_DB_PORT", "5432")),
        "dbname": os.environ.get("CREDIBLE_SETS_DB_NAME", "credible_sets"),
        "user": os.environ.get("CREDIBLE_SETS_DB_USER", "credsets"),
        "password": os.environ.get("CREDIBLE_SETS_DB_PASSWORD", ""),
    }


def connect(retries: int = 10, delay_s: float = 3.0):
    params = connection_params()
    last_exc = None
    for _ in range(retries):
        try:
            return psycopg2.connect(**params)
        except psycopg2.OperationalError as exc:
            last_exc = exc
            time.sleep(delay_s)
    raise SetupError(f"cannot connect to Postgres at {params['host']}:{params['port']}: {last_exc}")


def _pg_env() -> dict:
    p = connection_params()
    env = dict(os.environ)
    env.update(PGHOST=p["host"], PGPORT=str(p["port"]), PGDATABASE=p["dbname"],
               PGUSER=p["user"], PGPASSWORD=p["password"])
    return env


def _run(cmd: list) -> str:
    log.info("$ %s", " ".join(cmd))
    proc = subprocess.run(cmd, env=_pg_env(), capture_output=True, text=True)
    if proc.returncode != 0:
        raise SetupError(f"{cmd[0]} failed (exit {proc.returncode}): {proc.stderr.strip()[-2000:]}")
    return proc.stdout


def run_sql_file(conn, name: str, search_path: Optional[str] = None) -> None:
    with conn.cursor() as cur:
        if search_path:
            cur.execute(sql.SQL("SET LOCAL search_path = {}").format(sql.Identifier(search_path)))
        cur.execute((SQL_DIR / name).read_text())
    conn.commit()


def prepare_database(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_parquet")
        cur.execute(LOAD_STATE_DDL)
    conn.commit()


def acquire_lock(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
        if not cur.fetchone()[0]:
            log.info("Another ensure/publish holds the lock — waiting")
            cur.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
    conn.commit()


def drop_work_schemas(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {STAGING_SCHEMA} CASCADE")
        cur.execute(f"DROP SCHEMA IF EXISTS {RAW_SCHEMA} CASCADE")
    conn.commit()


def create_staging_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {STAGING_SCHEMA}")
    conn.commit()
    run_sql_file(conn, "02_tables.sql", search_path=STAGING_SCHEMA)


def build_indexes(conn) -> None:
    run_sql_file(conn, "04_indexes.sql", search_path=STAGING_SCHEMA)
    old = conn.autocommit
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for table in OT_TABLES:
                cur.execute(f"ANALYZE {STAGING_SCHEMA}.{table}")
    finally:
        conn.autocommit = old


# ── validate + swap ──────────────────────────────────────────────────────────

def validate_staging(conn, expected_counts: Optional[dict] = None) -> dict:
    counts = table_row_counts(conn, STAGING_SCHEMA)
    if counts is None:
        raise SetupError("staging is missing one or more OT tables")
    empty = [t for t in OT_TABLES if counts[t] <= 0]
    if empty:
        raise SetupError(f"staging tables are empty: {empty}")
    if expected_counts is not None and counts != expected_counts:
        raise SetupError(f"staging row counts {counts} != manifest {expected_counts}")
    with conn.cursor() as cur:
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM {STAGING_SCHEMA}.study WHERE studyid LIKE %s)",
                    (FINNGEN_PREFIX + "%",))
        has_finngen = cur.fetchone()[0]
    conn.rollback()
    if not has_finngen:
        log.warning("No %s* studies in this release — FinnGen library entries (mapped to R12) "
                    "will never match OT credible sets", FINNGEN_PREFIX)
    return counts


def swap_into_public(conn, release: str, source: str, counts: dict, snapshot_sha256: Optional[str]) -> None:
    """Replace the public tables with ot_staging's and record the load — atomically."""
    with conn.cursor() as cur:
        for table in OT_TABLES + LEGACY_TABLES:
            cur.execute(f"DROP TABLE IF EXISTS public.{table} CASCADE")
        for table in OT_TABLES:
            cur.execute(f"ALTER TABLE {STAGING_SCHEMA}.{table} SET SCHEMA public")
        cur.execute(f"DROP SCHEMA {STAGING_SCHEMA}")
        cur.execute(
            """
            INSERT INTO public.ot_load_state
                (dataset, ot_release, schema_version, source, row_counts, snapshot_sha256, loaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (dataset) DO UPDATE SET
                ot_release = EXCLUDED.ot_release,
                schema_version = EXCLUDED.schema_version,
                source = EXCLUDED.source,
                row_counts = EXCLUDED.row_counts,
                snapshot_sha256 = EXCLUDED.snapshot_sha256,
                loaded_at = EXCLUDED.loaded_at
            """,
            (DATASET, release, SCHEMA_VERSION, source, json.dumps(counts), snapshot_sha256),
        )
    conn.commit()
    log.info("Swapped release %s (%s) into public: %s", release, source, counts)


# ── MinIO restore ────────────────────────────────────────────────────────────

def _fetch_dump(store: SnapshotStore, key: str, dest: Path, sha256: str) -> None:
    """Download to a tmp file, verify sha256, then atomically move into place."""
    if dest.exists() and sha256_file(str(dest)) == sha256:
        log.info("Reusing cached snapshot %s", dest)
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f"{dest.name}.tmp-{uuid.uuid4().hex}")
    try:
        store.download(key, str(tmp))
        actual = sha256_file(str(tmp))
        if actual != sha256:
            raise SetupError(f"snapshot checksum mismatch: got {actual}, manifest says {sha256}")
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)


def restore_from_minio(conn, cfg: Config, store: SnapshotStore) -> None:
    keys = snapshot_keys(cfg.minio_prefix, cfg.release)
    if not store.exists(keys["manifest"]):
        raise SetupError(f"no snapshot at s3://{store.bucket}/{keys['manifest']}")
    manifest = parse_manifest(store.read_text(keys["manifest"]), cfg.release)
    sha = manifest["dump"]["sha256"]

    dest = Path(cfg.cache_dir) / cfg.release / f"schema-v{SCHEMA_VERSION}" / DUMP_NAME
    t0 = time.time()
    _fetch_dump(store, keys["dump"], dest, sha)
    log.info("Snapshot downloaded + verified in %.0fs", time.time() - t0)

    drop_work_schemas(conn)
    t0 = time.time()
    _run(["pg_restore", "--no-owner", "--no-privileges", "--exit-on-error",
          "--jobs", str(cfg.restore_jobs), "--dbname", connection_params()["dbname"], str(dest)])
    log.info("pg_restore finished in %.1f min", (time.time() - t0) / 60)

    counts = validate_staging(conn, expected_counts=manifest["row_counts"])
    swap_into_public(conn, cfg.release, "minio", counts, sha)
    dest.unlink(missing_ok=True)


# ── EBI load (pg_parquet) ────────────────────────────────────────────────────

def list_parquet_urls(release: str, folder: str) -> list:
    url = f"{EBI_BASE_URL}/{release}/output/{folder}/"
    with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT_S) as resp:
        html = resp.read().decode()
    files = re.findall(r'href="([^"]+\.parquet)"', html)
    if not files:
        raise SetupError(f"No parquet files found at {url}")
    return sorted(f"{url}{f}" for f in files)


def copy_parquet(conn, raw_table: str, url: str) -> int:
    """Load one parquet file into ot_raw.<raw_table> with pg_parquet, in its own transaction."""
    statement = sql.SQL("COPY {}.{} FROM {} WITH (format 'parquet', match_by 'name')").format(
        sql.Identifier(RAW_SCHEMA), sql.Identifier(raw_table), sql.Literal(url))
    with conn.cursor() as cur:
        cur.execute(statement)
        rows = cur.rowcount
    conn.commit()
    return rows


def load_raw_table(conn, cfg: Config, raw_table: str, folder: str):
    """COPY every parquet file of an OT folder; retry a file, fail the load if it keeps failing."""
    urls = list_parquet_urls(cfg.release, folder)
    if cfg.max_files:
        urls = urls[: cfg.max_files]
    log.info("ot_raw.%s <- %s: %d parquet file(s)", raw_table, folder, len(urls))
    total = 0
    for i, url in enumerate(urls, 1):
        for attempt in range(1, FILE_ATTEMPTS + 1):
            t0 = time.time()
            try:
                rows = copy_parquet(conn, raw_table, url)
                total += rows
                log.info("ot_raw.%s [%d/%d]: %s rows (%.1fs)", raw_table, i, len(urls), f"{rows:,}", time.time() - t0)
                break
            except psycopg2.Error as exc:
                conn.rollback()
                log.warning("ot_raw.%s [%d/%d] attempt %d/%d failed: %s",
                            raw_table, i, len(urls), attempt, FILE_ATTEMPTS, str(exc).strip())
                if attempt == FILE_ATTEMPTS:
                    raise SetupError(f"Giving up on {url} after {FILE_ATTEMPTS} attempts") from exc
                time.sleep(min(60, 5 * attempt))
    return total


def load_from_ebi(conn, cfg: Config) -> None:
    t_start = time.time()
    drop_work_schemas(conn)
    run_sql_file(conn, "01_raw.sql")
    create_staging_tables(conn)
    for raw_table, folder in RAW_SOURCES:
        load_raw_table(conn, cfg, raw_table, folder)

    log.info("Transforming ot_raw -> %s", STAGING_SCHEMA)
    t0 = time.time()
    run_sql_file(conn, "03_transform.sql", search_path=STAGING_SCHEMA)
    log.info("Transform done in %.1f min; building indexes", (time.time() - t0) / 60)
    build_indexes(conn)
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA {RAW_SCHEMA} CASCADE")
    conn.commit()

    counts = validate_staging(conn)
    swap_into_public(conn, cfg.release, "ebi", counts, None)
    log.info("Release %s loaded from EBI in %.1f min", cfg.release, (time.time() - t_start) / 60)


# ── commands ─────────────────────────────────────────────────────────────────

def ensure(cfg: Config, store_factory=SnapshotStore.from_env) -> int:
    """Make sure the pinned release is fully loaded. Returns a process exit code."""
    if cfg.mode not in MODES:
        log.error("OT_DB_BOOTSTRAP=%r — expected one of %s", cfg.mode, MODES)
        return 2
    if cfg.mode == "skip":
        log.warning("OT_DB_BOOTSTRAP=skip — not loading OpenTargets data")
        return 0
    try:
        validate_release(cfg.release)
    except SetupError as exc:
        log.error("%s", exc)
        return 2

    conn = connect()
    try:
        prepare_database(conn)
        acquire_lock(conn)

        current, reason = check_current(read_load_state(conn), table_row_counts(conn, "public"), cfg.release)
        if current:
            log.info("Up to date: %s", reason)
            return 0
        log.warning("Reload needed: %s", reason)

        if cfg.mode in ("auto", "minio-only"):
            try:
                store = store_factory()
                if store is None:
                    raise SetupError("MinIO is not configured (MINIO_ENDPOINT/ACCESS_KEY/SECRET_KEY)")
                restore_from_minio(conn, cfg, store)
                return 0
            except Exception as exc:
                conn.rollback()
                if cfg.mode == "minio-only":
                    log.error("MinIO restore failed and OT_DB_BOOTSTRAP=minio-only: %s", exc)
                    return 1
                log.warning("MinIO restore unavailable (%s) — loading from EBI; this can take hours. "
                            "Run `ot_db_setup.py publish` afterwards.", exc)

        load_from_ebi(conn, cfg)
        return 0
    except Exception:
        log.exception("Setup failed")
        return 1
    finally:
        conn.close()


def publish(store: SnapshotStore, prefix: str, force: bool = False) -> dict:
    """
    Dump the loaded public tables to MinIO as a pinned snapshot. The live
    tables are copied into ot_staging first (readers are never touched), and
    manifest.json is uploaded last so a half-finished upload is never used.
    """
    conn = connect()
    try:
        prepare_database(conn)
        acquire_lock(conn)
        state = read_load_state(conn)
        if state is None:
            raise SetupError("nothing to publish: ot_load_state is empty — run `ensure` first")
        live = table_row_counts(conn, "public")
        current, reason = check_current(state, live, state.ot_release)
        if not current:
            raise SetupError(f"refusing to publish an incomplete DB: {reason}")

        keys = snapshot_keys(prefix, state.ot_release)
        if store.exists(keys["manifest"]) and not force:
            raise SetupError(f"s3://{store.bucket}/{keys['manifest']} already exists (use --force)")

        drop_work_schemas(conn)
        create_staging_tables(conn)
        with conn.cursor() as cur:
            for table in OT_TABLES:
                cur.execute(f"INSERT INTO {STAGING_SCHEMA}.{table} SELECT * FROM public.{table}")
        conn.commit()
        build_indexes(conn)

        with tempfile.TemporaryDirectory(prefix="ot-snapshot-") as tmpdir:
            dump_path = os.path.join(tmpdir, DUMP_NAME)
            _run(["pg_dump", "--format=custom", "--no-owner", "--no-privileges",
                  "--schema", STAGING_SCHEMA, "--file", dump_path])
            sha = sha256_file(dump_path)
            size = os.path.getsize(dump_path)
            store.upload(dump_path, keys["dump"])

        manifest = {
            "format_version": MANIFEST_FORMAT_VERSION,
            "dataset": DATASET,
            "ot_release": state.ot_release,
            "schema_version": SCHEMA_VERSION,
            "row_counts": live,
            "dump": {"key": keys["dump"], "sha256": sha, "size_bytes": size},
            "pg_dump_version": _run(["pg_dump", "--version"]).strip(),
            "loaded_from": state.source,
            "created_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        }
        store.write_text(json.dumps(manifest, indent=2), keys["manifest"])
        log.info("Published s3://%s/%s/ (%.2f GB)", store.bucket, keys["base"], size / 1e9)
        return manifest
    finally:
        try:
            drop_work_schemas(conn)
        except Exception:
            pass
        conn.close()


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [ot-db] %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p_ensure = sub.add_parser("ensure", help="make sure the pinned OT_RELEASE is loaded")
    p_ensure.add_argument("--release", help="override OT_RELEASE")
    p_ensure.add_argument("--mode", choices=MODES, help="override OT_DB_BOOTSTRAP")
    p_ensure.add_argument("--max-files", type=int, help="testing only: parquet files per OT folder")
    p_publish = sub.add_parser("publish", help="upload the loaded DB to MinIO as a snapshot")
    p_publish.add_argument("--force", action="store_true", help="overwrite an existing snapshot")
    sub.add_parser("list", help="list published snapshots")
    args = parser.parse_args(argv)

    cfg = Config.from_env()
    if args.command == "ensure":
        cfg = replace(cfg, release=args.release or cfg.release, mode=args.mode or cfg.mode,
                      max_files=args.max_files)
        return ensure(cfg)

    store = SnapshotStore.from_env()
    if store is None:
        log.error("MinIO is not configured (MINIO_ENDPOINT/ACCESS_KEY/SECRET_KEY)")
        return 2
    if args.command == "publish":
        try:
            print(json.dumps(publish(store, cfg.minio_prefix, force=args.force), indent=2))
            return 0
        except SetupError as exc:
            log.error("%s", exc)
            return 1
    for obj in store.list_keys(f"{cfg.minio_prefix}/"):
        print(f"{obj['Size'] / 1e9:8.2f} GB  {obj['LastModified']:%Y-%m-%d %H:%M}  {obj['Key']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
