"""
Load an OpenTargets release from the EBI FTP into a target Postgres schema —
normally `ot_staging`, which the bootstrap then validates and swaps into
`public` (see ot_db_bootstrap.py).

What gets loaded is driven by credible_sets_schema.LOADED_TABLES: for each
table, only the listed parquet fields are read and converted, then COPYed in.
Derived tables (credible_set_variants, gwas_study_index) are built afterwards
with SQL inside Postgres.

Fail-fast by design: each parquet file is loaded in its own transaction and
retried a few times; if a file still fails, the whole load raises. A partial
release is never reported as success.
"""

import csv
import io
import json
import math
import re
import shutil
import tempfile
import time
import urllib.request
from typing import Callable

import pyarrow.parquet as pq
from loguru import logger

from src.db import credible_sets_schema as cs_schema

BASE_URL = "https://ftp.ebi.ac.uk/pub/databases/opentargets/platform"

# Rows per parquet read batch: files are streamed instead of loaded whole
# (a single file can be a few hundred MB, enough to OOM the loader).
CHUNK_ROWS = 1_000
# Rows vary wildly in size — some credible sets carry large `locus` JSONB
# blobs. Each COPY is capped by serialized bytes so no single statement gets
# large enough to kill the Postgres connection.
MAX_COPY_BYTES = 8 * 1024 * 1024
# Attempts per parquet file (download + load) before the whole load fails.
FILE_ATTEMPTS = 3
HTTP_TIMEOUT_S = 300

# COPY ... (FORMAT csv, NULL '\N') — distinguishes NULL from empty string.
_NULL = "\\N"


class EbiLoadError(RuntimeError):
    pass


# ── value conversion ─────────────────────────────────────────────────────────
# Values come from pyarrow's to_pylist(): None for nulls, lists for list
# columns, dicts for structs. Floats can still be NaN.

def _is_nan(v) -> bool:
    return isinstance(v, float) and math.isnan(v)


def _clean_json(v):
    """Replace NaN with None so the value is valid JSON for a jsonb column."""
    if _is_nan(v):
        return None
    if isinstance(v, list):
        return [_clean_json(x) for x in v]
    if isinstance(v, dict):
        return {k: _clean_json(x) for k, x in v.items()}
    return v


def convert_value(value, kind: str):
    """Convert one parquet value to what COPY expects for the column kind."""
    if value is None or _is_nan(value):
        return None
    if kind == "text":
        return str(value)
    if kind == "int":
        return int(value)
    if kind == "float":
        return float(value)
    if kind == "bool":
        return bool(value)
    if kind == "text_array":
        return list(value) or None
    if kind == "json":
        return json.dumps(_clean_json(value), allow_nan=False)
    raise ValueError(f"unknown column kind {kind!r}")


def table_row(spec: cs_schema.LoadedTable, record: dict) -> tuple:
    return tuple(convert_value(record.get(c.source), c.kind) for c in spec.columns)


def pg_text_array(values) -> str:
    """Render a Python list as a Postgres TEXT[] literal, e.g. {"a","b"}."""
    parts = []
    for item in values:
        if item is None:
            parts.append("NULL")
        else:
            escaped = str(item).replace("\\", "\\\\").replace('"', '\\"')
            parts.append(f'"{escaped}"')
    return "{" + ",".join(parts) + "}"


def _copy_field(v) -> str:
    if v is None:
        return _NULL
    if isinstance(v, bool):
        return "t" if v else "f"
    if isinstance(v, list):
        return pg_text_array(v)
    return str(v)


# ── FTP helpers ──────────────────────────────────────────────────────────────

def list_parquet_urls(release: str, folder: str) -> list:
    url = f"{BASE_URL}/{release}/output/{folder}/"
    with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT_S) as resp:
        html = resp.read().decode()
    files = re.findall(r'href="([^"]+\.parquet)"', html)
    if not files:
        raise EbiLoadError(f"No parquet files found at {url}")
    return sorted(f"{url}{f}" for f in files)


def _download(url: str, dest: str) -> None:
    with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT_S) as resp, open(dest, "wb") as out:
        shutil.copyfileobj(resp, out, length=1024 * 1024)


# ── COPY ─────────────────────────────────────────────────────────────────────

class _CopyBuffer:
    """Accumulates rows as CSV and flushes them with COPY once MAX_COPY_BYTES is hit."""

    def __init__(self, cur, schema: str, table: str):
        self._cur = cur
        self._sql = (
            f"COPY {schema}.{table} ({cs_schema.column_list(table)}) "
            f"FROM STDIN WITH (FORMAT csv, NULL '{_NULL}')"
        )
        self._buf = io.StringIO()
        self._writer = csv.writer(self._buf, lineterminator="\n")
        self.rows = 0

    def add(self, record: tuple) -> None:
        self._writer.writerow([_copy_field(v) for v in record])
        self.rows += 1
        if self._buf.tell() >= MAX_COPY_BYTES:
            self.flush()

    def flush(self) -> None:
        if self._buf.tell() == 0:
            return
        self._buf.seek(0)
        self._cur.copy_expert(self._sql, self._buf)
        self._buf = io.StringIO()
        self._writer = csv.writer(self._buf, lineterminator="\n")


def _load_file(cur, schema: str, spec: cs_schema.LoadedTable, path: str) -> int:
    buf = _CopyBuffer(cur, schema, spec.name)
    fields = [c.source for c in spec.columns]
    for batch in pq.ParquetFile(path).iter_batches(batch_size=CHUNK_ROWS, columns=fields):
        for record in batch.to_pylist():
            buf.add(table_row(spec, record))
    buf.flush()
    return buf.rows


def _load_table(connect: Callable, conn, schema: str, release: str, spec: cs_schema.LoadedTable):
    """
    Load every parquet file in the table's EBI folder. Each file is one
    transaction: on failure it is rolled back (so a retry never double-inserts)
    and retried; after FILE_ATTEMPTS failures the whole load raises. Returns
    (conn, rows) — conn may be a fresh connection if the old one died.
    """
    urls = list_parquet_urls(release, spec.folder)
    logger.info(f"[OT-EBI] {spec.name} <- {spec.folder}: {len(urls)} parquet file(s) for release {release}")
    total = 0
    for i, url in enumerate(urls, 1):
        fname = url.rsplit("/", 1)[-1]
        for attempt in range(1, FILE_ATTEMPTS + 1):
            t0 = time.time()
            try:
                with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                    _download(url, tmp.name)
                    with conn.cursor() as cur:
                        rows = _load_file(cur, schema, spec, tmp.name)
                conn.commit()
                total += rows
                logger.info(
                    f"[OT-EBI] {spec.name} [{i}/{len(urls)}] {fname}: "
                    f"{rows:,} rows ({time.time() - t0:.1f}s)"
                )
                break
            except Exception as exc:
                logger.warning(
                    f"[OT-EBI] {spec.name} [{i}/{len(urls)}] {fname} attempt "
                    f"{attempt}/{FILE_ATTEMPTS} failed: {exc}"
                )
                try:
                    conn.rollback()
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = connect()
                if attempt == FILE_ATTEMPTS:
                    raise EbiLoadError(f"Giving up on {url} after {FILE_ATTEMPTS} attempts") from exc
                time.sleep(min(60, 5 * attempt))
    return conn, total


def load_release_into_schema(connect: Callable, conn, schema: str, release: str):
    """
    Load every table in LOADED_TABLES for *release* into *schema* (whose bare
    tables must already exist), de-duplicate, build the derived tables, then
    add PKs and indexes. Returns the (possibly replaced) connection.
    """
    t_start = time.time()
    for spec in cs_schema.LOADED_TABLES:
        conn, rows = _load_table(connect, conn, schema, release, spec)
        logger.info(f"[OT-EBI] {spec.name}: {rows:,} rows loaded")

    with conn.cursor() as cur:
        for spec in cs_schema.LOADED_TABLES:
            if spec.name not in cs_schema.PRIMARY_KEYS:
                continue
            cur.execute(cs_schema.duplicate_count_sql(schema, spec.name))
            dupes = cur.fetchone()[0]
            if dupes:
                logger.warning(f"[OT-EBI] {spec.name}: removing {dupes:,} duplicate primary-key rows")
                cur.execute(cs_schema.dedupe_sql(schema, spec.name))
        logger.info("[OT-EBI] Building derived tables (credible_set_variants, gwas_study_index)")
        for statement in cs_schema.derive_sql(schema):
            cur.execute(statement)
        logger.info("[OT-EBI] Building primary keys and indexes")
        cur.execute(cs_schema.create_indexes_sql(schema))
    conn.commit()

    old_autocommit = conn.autocommit
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for table in cs_schema.OT_TABLES:
                cur.execute(f"ANALYZE {schema}.{table}")
    finally:
        conn.autocommit = old_autocommit

    logger.info(f"[OT-EBI] Release {release} loaded into {schema} in {(time.time() - t_start) / 60:.1f} min")
    return conn
