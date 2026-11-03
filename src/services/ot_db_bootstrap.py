"""
Make sure the OpenTargets Postgres DB (credible_sets, gwas_study_index,
opentargets_studies) holds a complete copy of the pinned release OT_RELEASE.

Run on every deployment by the `ot-db-bootstrap` one-shot service
(scripts/bootstrap_ot_db.py); the pipeline workers wait for it to exit 0.

  1. If public.ot_load_state says the pinned release + SCHEMA_VERSION is
     loaded AND the live row counts match the counts recorded at load time,
     exit immediately.
  2. Otherwise restore a pg_dump snapshot from MinIO
     (<prefix>/<release>/schema-v<N>/{manifest.json, ot.dump}), verifying its
     sha256 and row counts.
  3. If MinIO has no usable snapshot, load the release from the EBI FTP.

Both load paths build into the `ot_staging` schema and are swapped into
`public` in one transaction together with the ot_load_state marker, so readers
never see a partial table and the marker always describes data that exists.
A crashed run just leaves `ot_staging` behind, which the next run drops.

Snapshots are published manually with scripts/pg_releases.py (publish_snapshot).
"""

import datetime as _dt
import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
from loguru import logger

from src.db import credible_sets_schema as cs_schema
from src.services import ot_ebi_loader

MODES = ("auto", "skip", "minio-only", "ebi-only")
MANIFEST_FORMAT_VERSION = 1
DUMP_NAME = "ot.dump"
MANIFEST_NAME = "manifest.json"
# Arbitrary constant: serialises concurrent bootstrap / publish runs.
ADVISORY_LOCK_KEY = 0x07_DB_B007
FINNGEN_PREFIX = "FINNGEN_R12_"

_RELEASE_RE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._-]*$")


class BootstrapError(RuntimeError):
    pass


# ── config ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class BootstrapConfig:
    release: str
    mode: str = "auto"
    minio_prefix: str = "pg-releases/opentargets"
    cache_dir: str = "data/pg_releases"
    restore_jobs: int = 4

    @classmethod
    def from_env(cls) -> "BootstrapConfig":
        return cls(
            release=os.getenv("OT_RELEASE", "").strip(),
            mode=os.getenv("OT_DB_BOOTSTRAP", "auto").strip().lower() or "auto",
            minio_prefix=(os.getenv("OT_DB_SNAPSHOT_MINIO_PREFIX") or "pg-releases/opentargets").strip("/"),
            cache_dir=os.getenv("OT_DB_SNAPSHOT_CACHE_DIR", "data/pg_releases"),
            restore_jobs=int(os.getenv("OT_DB_RESTORE_JOBS", "4")),
        )


def validate_release(release: str) -> str:
    """OT_RELEASE must be an explicit, pinned release tag (e.g. '25.09')."""
    if not release:
        raise BootstrapError("OT_RELEASE is not set — pin an OpenTargets release, e.g. OT_RELEASE=25.09")
    if release.lower() == "latest":
        raise BootstrapError("OT_RELEASE=latest is not allowed — pin an explicit release, e.g. 25.09")
    if not _RELEASE_RE.match(release):
        raise BootstrapError(f"OT_RELEASE={release!r} is not a valid release tag")
    return release


def snapshot_keys(prefix: str, release: str, schema_version: int = cs_schema.SCHEMA_VERSION) -> dict:
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


def check_current(
    state: Optional[LoadState],
    live_counts: Optional[dict],
    release: str,
    schema_version: int = cs_schema.SCHEMA_VERSION,
) -> tuple:
    """
    Pure freshness check. Returns (is_current, reason).

    Current only if a marker exists for the pinned release + schema version
    AND every OT table's live row count equals the count recorded when the
    load completed — so a partial, truncated or hand-edited DB is reloaded.
    """
    if state is None:
        return False, "no ot_load_state marker (never loaded, or loaded before load tracking existed)"
    if state.ot_release != release:
        return False, f"loaded release {state.ot_release!r} != pinned OT_RELEASE {release!r}"
    if state.schema_version != schema_version:
        return False, f"loaded schema v{state.schema_version} != code schema v{schema_version}"
    if live_counts is None:
        return False, "one or more OT tables are missing"
    for table in cs_schema.OT_TABLES:
        expected = state.row_counts.get(table)
        actual = live_counts.get(table)
        if expected is None or actual != expected:
            return False, f"{table}: {actual} rows live, {expected} recorded at load time"
    return True, f"release {release} (schema v{schema_version}, source={state.source}) is loaded and complete"


def ensure_load_state_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(cs_schema.LOAD_STATE_DDL)
    conn.commit()


def read_load_state(conn) -> Optional[LoadState]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT to_regclass('public.ot_load_state') IS NOT NULL AS ok")
        if not cur.fetchone()["ok"]:
            conn.rollback()
            return None
        cur.execute(
            "SELECT ot_release, schema_version, source, row_counts, snapshot_sha256, loaded_at "
            "FROM public.ot_load_state WHERE dataset = %s",
            (cs_schema.DATASET,),
        )
        row = cur.fetchone()
    conn.rollback()
    return LoadState(**row) if row else None


def table_row_counts(conn, schema: str) -> Optional[dict]:
    """Exact COUNT(*) per OT table in *schema*, or None if any table is missing."""
    counts = {}
    with conn.cursor() as cur:
        for table in cs_schema.OT_TABLES:
            cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
            if cur.fetchone()[0] is None:
                conn.rollback()
                return None
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            counts[table] = cur.fetchone()[0]
    conn.rollback()
    return counts


# ── manifest ─────────────────────────────────────────────────────────────────

def parse_manifest(text: str, release: str, schema_version: int = cs_schema.SCHEMA_VERSION) -> dict:
    """Parse + sanity-check a snapshot manifest against the pinned release."""
    try:
        manifest = json.loads(text)
    except (TypeError, ValueError) as exc:
        raise BootstrapError(f"manifest is not valid JSON: {exc}") from exc
    if manifest.get("format_version") != MANIFEST_FORMAT_VERSION:
        raise BootstrapError(f"unsupported manifest format_version {manifest.get('format_version')!r}")
    if manifest.get("ot_release") != release:
        raise BootstrapError(f"manifest is for release {manifest.get('ot_release')!r}, expected {release!r}")
    if manifest.get("schema_version") != schema_version:
        raise BootstrapError(f"manifest schema v{manifest.get('schema_version')}, expected v{schema_version}")
    dump = manifest.get("dump") or {}
    if not re.fullmatch(r"[0-9a-f]{64}", str(dump.get("sha256", ""))):
        raise BootstrapError("manifest has no valid dump.sha256")
    counts = manifest.get("row_counts") or {}
    if set(counts) != set(cs_schema.OT_TABLES):
        raise BootstrapError(f"manifest row_counts must cover exactly {cs_schema.OT_TABLES}")
    return manifest


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


# ── connections / subprocess env ─────────────────────────────────────────────

def connect(retries: int = 10, delay_s: float = 3.0):
    params = cs_schema.connection_params_from_env()
    last_exc = None
    for _ in range(retries):
        try:
            return psycopg2.connect(**params)
        except psycopg2.OperationalError as exc:
            last_exc = exc
            time.sleep(delay_s)
    raise BootstrapError(f"cannot connect to credible-sets Postgres at {params['host']}:{params['port']}: {last_exc}")


def _pg_env() -> dict:
    p = cs_schema.connection_params_from_env()
    env = dict(os.environ)
    env.update(
        PGHOST=p["host"], PGPORT=str(p["port"]), PGDATABASE=p["dbname"],
        PGUSER=p["user"], PGPASSWORD=p["password"],
    )
    return env


def _run(cmd: list) -> str:
    logger.info(f"[OT-DB] $ {' '.join(cmd)}")
    proc = subprocess.run(cmd, env=_pg_env(), capture_output=True, text=True)
    if proc.returncode != 0:
        raise BootstrapError(f"{cmd[0]} failed (exit {proc.returncode}): {proc.stderr.strip()[-2000:]}")
    return proc.stdout


# ── staging + swap ───────────────────────────────────────────────────────────

def drop_staging(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {cs_schema.STAGING_SCHEMA} CASCADE")
    conn.commit()


def validate_staging(conn, expected_counts: Optional[dict] = None) -> dict:
    counts = table_row_counts(conn, cs_schema.STAGING_SCHEMA)
    if counts is None:
        raise BootstrapError("staging is missing one or more OT tables")
    for table in ("credible_sets", "gwas_study_index", "opentargets_studies"):
        if counts[table] <= 0:
            raise BootstrapError(f"staging {table} is empty")
    if expected_counts is not None and counts != expected_counts:
        raise BootstrapError(f"staging row counts {counts} != manifest {expected_counts}")
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT EXISTS (SELECT 1 FROM {cs_schema.STAGING_SCHEMA}.opentargets_studies "
            "WHERE study_id LIKE %s)",
            (FINNGEN_PREFIX + "%",),
        )
        has_finngen = cur.fetchone()[0]
    conn.rollback()
    if not has_finngen:
        logger.warning(
            f"[OT-DB] No {FINNGEN_PREFIX}* studies in this release — FinnGen library entries "
            "(mapped to R12 in gwas_library_handler) will never match OT credible sets"
        )
    return counts


def swap_into_public(conn, release: str, source: str, counts: dict, snapshot_sha256: Optional[str]) -> None:
    """Replace the public OT tables with ot_staging's and record the load — atomically."""
    with conn.cursor() as cur:
        for table in cs_schema.OT_TABLES:
            cur.execute(f"DROP TABLE IF EXISTS public.{table} CASCADE")
        for table in cs_schema.OT_TABLES:
            cur.execute(f"ALTER TABLE {cs_schema.STAGING_SCHEMA}.{table} SET SCHEMA public")
        cur.execute(f"DROP SCHEMA {cs_schema.STAGING_SCHEMA}")
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
            (cs_schema.DATASET, release, cs_schema.SCHEMA_VERSION, source,
             json.dumps(counts), snapshot_sha256),
        )
    conn.commit()
    logger.info(f"[OT-DB] Swapped release {release} ({source}) into public: {counts}")


# ── MinIO restore ────────────────────────────────────────────────────────────

def _fetch_dump(storage, key: str, dest: Path, sha256: str) -> None:
    """Download to a tmp file, verify sha256, then atomically move into place."""
    if dest.exists() and sha256_file(str(dest)) == sha256:
        logger.info(f"[OT-DB] Reusing cached snapshot {dest}")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f"{dest.name}.tmp-{uuid.uuid4().hex}")
    try:
        if not storage.download_file(key, str(tmp)):
            raise BootstrapError(f"download of s3://{storage.bucket}/{key} failed")
        actual = sha256_file(str(tmp))
        if actual != sha256:
            raise BootstrapError(f"snapshot checksum mismatch: got {actual}, manifest says {sha256}")
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)


def restore_from_minio(conn, cfg: BootstrapConfig, storage) -> None:
    keys = snapshot_keys(cfg.minio_prefix, cfg.release)
    if not storage.exists(keys["manifest"]):
        raise BootstrapError(f"no snapshot at s3://{storage.bucket}/{keys['manifest']}")
    text = storage.download_string(keys["manifest"])
    if text is None:
        raise BootstrapError(f"could not read s3://{storage.bucket}/{keys['manifest']}")
    manifest = parse_manifest(text, cfg.release)
    sha = manifest["dump"]["sha256"]

    dest = Path(cfg.cache_dir) / cfg.release / f"schema-v{cs_schema.SCHEMA_VERSION}" / DUMP_NAME
    t0 = time.time()
    _fetch_dump(storage, keys["dump"], dest, sha)
    logger.info(f"[OT-DB] Snapshot downloaded + verified in {time.time() - t0:.0f}s")

    drop_staging(conn)
    t0 = time.time()
    _run([
        "pg_restore", "--no-owner", "--no-privileges", "--exit-on-error",
        "--jobs", str(cfg.restore_jobs), "--dbname", cs_schema.connection_params_from_env()["dbname"],
        str(dest),
    ])
    logger.info(f"[OT-DB] pg_restore finished in {(time.time() - t0) / 60:.1f} min")

    counts = validate_staging(conn, expected_counts=manifest["row_counts"])
    swap_into_public(conn, cfg.release, "minio", counts, sha)
    dest.unlink(missing_ok=True)


# ── EBI load ─────────────────────────────────────────────────────────────────

def load_from_ebi(conn, cfg: BootstrapConfig):
    drop_staging(conn)
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {cs_schema.STAGING_SCHEMA}")
        cur.execute(cs_schema.create_tables_sql(cs_schema.STAGING_SCHEMA))
    conn.commit()
    conn = ot_ebi_loader.load_release_into_schema(_connect_locked, conn, cs_schema.STAGING_SCHEMA, cfg.release)
    counts = validate_staging(conn)
    swap_into_public(conn, cfg.release, "ebi", counts, None)
    return conn


# ── orchestration ────────────────────────────────────────────────────────────

def _acquire_lock(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
        if not cur.fetchone()[0]:
            logger.info("[OT-DB] Another bootstrap/publish holds the lock — waiting")
            cur.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
    conn.commit()


def _connect_locked():
    """Reconnect after a dropped connection, re-taking the session-level lock."""
    conn = connect()
    _acquire_lock(conn)
    return conn


def run_bootstrap(cfg: Optional[BootstrapConfig] = None, storage_factory=None) -> int:
    """Entry point for scripts/bootstrap_ot_db.py. Returns a process exit code."""
    cfg = cfg or BootstrapConfig.from_env()
    if cfg.mode not in MODES:
        logger.error(f"[OT-DB] OT_DB_BOOTSTRAP={cfg.mode!r} — expected one of {MODES}")
        return 2
    if cfg.mode == "skip":
        logger.warning("[OT-DB] OT_DB_BOOTSTRAP=skip — not loading OpenTargets data; "
                       "the pipeline will fall back to full fine-mapping")
        return 0
    try:
        validate_release(cfg.release)
    except BootstrapError as exc:
        logger.error(f"[OT-DB] {exc}")
        return 2

    conn = connect()
    try:
        ensure_load_state_table(conn)
        _acquire_lock(conn)

        current, reason = check_current(
            read_load_state(conn), table_row_counts(conn, "public"), cfg.release
        )
        if current:
            logger.info(f"[OT-DB] Up to date: {reason}")
            return 0
        logger.warning(f"[OT-DB] Reload needed: {reason}")

        if cfg.mode in ("auto", "minio-only"):
            try:
                if storage_factory is None:
                    from src.services.storage import create_minio_client_from_env as storage_factory
                storage = storage_factory()
                if storage is None:
                    raise BootstrapError("MinIO is not configured (MINIO_ENDPOINT/ACCESS_KEY/SECRET_KEY)")
                restore_from_minio(conn, cfg, storage)
                return 0
            except Exception as exc:
                conn.rollback()
                if cfg.mode == "minio-only":
                    logger.error(f"[OT-DB] MinIO restore failed and OT_DB_BOOTSTRAP=minio-only: {exc}")
                    return 1
                logger.warning(f"[OT-DB] MinIO restore unavailable ({exc}) — falling back to EBI FTP; "
                               "this can take hours. Publish a snapshot with scripts/pg_releases.py afterwards.")

        conn = load_from_ebi(conn, cfg)
        return 0
    except Exception as exc:
        logger.exception(f"[OT-DB] Bootstrap failed: {exc}")
        return 1
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ── publish (manual, scripts/pg_releases.py) ─────────────────────────────────

def publish_snapshot(storage, prefix: str, force: bool = False) -> dict:
    """
    Dump the currently-loaded public OT tables to MinIO as a pinned snapshot.
    The live tables are copied into ot_staging first (readers are never
    touched), and manifest.json is uploaded last so a half-finished upload is
    never picked up by a deployment. Returns the manifest.
    """
    conn = connect()
    try:
        ensure_load_state_table(conn)
        _acquire_lock(conn)
        state = read_load_state(conn)
        live = table_row_counts(conn, "public")
        if state is None:
            raise BootstrapError("nothing to publish: public.ot_load_state is empty — run the bootstrap first")
        current, reason = check_current(state, live, state.ot_release)
        if not current:
            raise BootstrapError(f"refusing to publish an incomplete DB: {reason}")

        keys = snapshot_keys(prefix, state.ot_release)
        if storage.exists(keys["manifest"]) and not force:
            raise BootstrapError(f"s3://{storage.bucket}/{keys['manifest']} already exists (use --force)")

        # Rebuild with the canonical DDL (not LIKE ... INCLUDING ALL, which
        # auto-renames indexes) so a restored snapshot matches an EBI load.
        staging = cs_schema.STAGING_SCHEMA
        drop_staging(conn)
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA {staging}")
            cur.execute(cs_schema.create_tables_sql(staging))
            for table in cs_schema.OT_TABLES:
                cols = ", ".join(cs_schema.TABLE_COLUMNS[table])
                cur.execute(f"INSERT INTO {staging}.{table} ({cols}) SELECT {cols} FROM public.{table}")
            cur.execute(cs_schema.create_indexes_sql(staging))
        conn.commit()
        logger.info(f"[OT-DB] Copied live tables into {cs_schema.STAGING_SCHEMA}")

        with tempfile.TemporaryDirectory(prefix="ot-snapshot-") as tmpdir:
            dump_path = os.path.join(tmpdir, DUMP_NAME)
            _run(["pg_dump", "--format=custom", "--no-owner", "--no-privileges",
                  "--schema", cs_schema.STAGING_SCHEMA, "--file", dump_path])
            sha = sha256_file(dump_path)
            size = os.path.getsize(dump_path)
            pg_dump_version = _run(["pg_dump", "--version"]).strip()

            if not storage.upload_file(dump_path, keys["dump"]):
                raise BootstrapError(f"upload of {keys['dump']} failed")

        manifest = {
            "format_version": MANIFEST_FORMAT_VERSION,
            "dataset": cs_schema.DATASET,
            "ot_release": state.ot_release,
            "schema_version": cs_schema.SCHEMA_VERSION,
            "row_counts": live,
            "dump": {"key": keys["dump"], "sha256": sha, "size_bytes": size},
            "pg_dump_version": pg_dump_version,
            "loaded_from": state.source,
            "created_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        }
        if not storage.upload_string(json.dumps(manifest, indent=2), keys["manifest"]):
            raise BootstrapError(f"upload of {keys['manifest']} failed")
        logger.info(f"[OT-DB] Published s3://{storage.bucket}/{keys['base']}/ ({size / 1e9:.2f} GB)")
        return manifest
    finally:
        try:
            drop_staging(conn)
        except Exception:
            pass
        conn.close()
