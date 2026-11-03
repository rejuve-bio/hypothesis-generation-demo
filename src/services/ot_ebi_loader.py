"""
Load an OpenTargets release (credible_set/ + study/ parquet from the EBI FTP)
into a target Postgres schema — normally `ot_staging`, which the bootstrap
then validates and swaps into `public` (see ot_db_bootstrap.py).

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
from typing import Callable, Iterable

import numpy as np
import pyarrow.parquet as pq
from loguru import logger

from src.db import credible_sets_schema as cs_schema

BASE_URL = "https://ftp.ebi.ac.uk/pub/databases/opentargets/platform"
CREDIBLE_SET_FOLDER = "credible_set"
STUDY_FOLDER = "study"

# Row-group read batch size for streaming a parquet file instead of loading it
# whole into memory (a single file can be ~235MB, enough to OOM the loader).
CHUNK_ROWS = 1_000
# Rows vary wildly in size — some credible sets carry huge `locus`/`ld_set`
# JSONB blobs. Each COPY is capped by serialized bytes so no single statement
# gets large enough to kill the Postgres connection.
MAX_COPY_BYTES = 8 * 1024 * 1024
# Attempts per parquet file (download + load) before the whole load fails.
FILE_ATTEMPTS = 3
HTTP_TIMEOUT_S = 300

# COPY ... (FORMAT csv, NULL '\N') — distinguishes NULL from empty string.
_NULL = "\\N"


class EbiLoadError(RuntimeError):
    pass


# ── value conversion ─────────────────────────────────────────────────────────

def _float(v):
    if v is None:
        return None
    if isinstance(v, (np.floating, float)):
        return None if math.isnan(v) else float(v)
    return float(v)


def _int(v):
    if v is None:
        return None
    if isinstance(v, (np.floating, float)):
        return None if math.isnan(v) else int(v)
    return int(v)


def _bool(v):
    if v is None:
        return None
    return bool(v)


def _list(v):
    if v is None:
        return None
    if isinstance(v, np.ndarray):
        v = v.tolist()
    return list(v)


def _json(v):
    if v is None:
        return None
    if isinstance(v, np.ndarray):
        v = v.tolist()

    def default(obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return None if math.isnan(obj) else float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return str(obj)

    return json.dumps(v, default=default)


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


def credible_set_row(row: dict) -> tuple:
    return (
        row["studyLocusId"],
        row["studyId"],
        row["variantId"],
        row.get("chromosome"),
        _int(row.get("position")),
        row.get("region"),
        _float(row.get("beta")),
        _float(row.get("zScore")),
        _float(row.get("pValueMantissa")),
        _int(row.get("pValueExponent")),
        _float(row.get("effectAlleleFrequencyFromSource")),
        _float(row.get("standardError")),
        row.get("subStudyDescription"),
        _list(row.get("qualityControls")),
        row.get("finemappingMethod"),
        _int(row.get("credibleSetIndex")),
        _float(row.get("credibleSetlog10BF")),
        _float(row.get("purityMeanR2")),
        _float(row.get("purityMinR2")),
        _int(row.get("locusStart")),
        _int(row.get("locusEnd")),
        _int(row.get("sampleSize")),
        _json(row.get("ldSet")),
        _json(row.get("locus")),
        row.get("confidence"),
        row.get("studyType"),
        _bool(row.get("isTransQtl")),
    )


def study_row(row: dict) -> tuple:
    efo_ids = _list(row.get("traitFromSourceMappedIds"))
    return (
        row["studyId"],
        row.get("projectId"),
        row.get("traitFromSource"),
        efo_ids or None,
        _bool(row.get("hasSumstats")),
        row.get("summarystatsLocation"),
        _int(row.get("nSamples")),
    )


STUDY_PARQUET_COLUMNS = [
    "studyId", "projectId", "studyType", "traitFromSource",
    "traitFromSourceMappedIds", "hasSumstats", "summarystatsLocation", "nSamples",
]


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

    def __init__(self, cur, schema: str, table: str, columns: Iterable[str]):
        self._cur = cur
        self._sql = (
            f"COPY {schema}.{table} ({', '.join(columns)}) "
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


def _load_credible_set_file(cur, schema: str, path: str) -> int:
    buf = _CopyBuffer(cur, schema, "credible_sets", cs_schema.CREDIBLE_SETS_COLUMNS)
    for batch in pq.ParquetFile(path).iter_batches(batch_size=CHUNK_ROWS):
        df = batch.to_pandas()
        df = df[df["studyType"] == "gwas"]
        for row in df.to_dict("records"):
            buf.add(credible_set_row(row))
    buf.flush()
    return buf.rows


def _load_study_file(cur, schema: str, path: str) -> int:
    buf = _CopyBuffer(cur, schema, "opentargets_studies", cs_schema.OPENTARGETS_STUDIES_COLUMNS)
    df = pq.read_table(path, columns=STUDY_PARQUET_COLUMNS).to_pandas()
    df = df[df["studyType"] == "gwas"]
    for row in df.to_dict("records"):
        buf.add(study_row(row))
    buf.flush()
    return buf.rows


def _load_folder(
    connect: Callable,
    conn,
    schema: str,
    release: str,
    folder: str,
    load_file: Callable,
):
    """
    Load every parquet file in an EBI folder. Each file is one transaction:
    on failure it is rolled back (so a retry never double-inserts) and retried;
    after FILE_ATTEMPTS failures the whole load raises. Returns (conn, rows) —
    conn may be a fresh connection if the old one died.
    """
    urls = list_parquet_urls(release, folder)
    logger.info(f"[OT-EBI] {folder}: {len(urls)} parquet file(s) for release {release}")
    total = 0
    for i, url in enumerate(urls, 1):
        fname = url.rsplit("/", 1)[-1]
        for attempt in range(1, FILE_ATTEMPTS + 1):
            t0 = time.time()
            try:
                with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                    _download(url, tmp.name)
                    with conn.cursor() as cur:
                        rows = load_file(cur, schema, tmp.name)
                conn.commit()
                total += rows
                logger.info(
                    f"[OT-EBI] {folder} [{i}/{len(urls)}] {fname}: "
                    f"{rows:,} gwas rows ({time.time() - t0:.1f}s)"
                )
                break
            except Exception as exc:
                logger.warning(
                    f"[OT-EBI] {folder} [{i}/{len(urls)}] {fname} attempt "
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
    Load studies + credible sets for *release* into *schema* (whose bare
    tables must already exist), de-duplicate, build gwas_study_index, then
    add PKs and indexes. Returns the (possibly replaced) connection.
    """
    t_start = time.time()
    conn, n_studies = _load_folder(connect, conn, schema, release, STUDY_FOLDER, _load_study_file)
    conn, n_cs = _load_folder(connect, conn, schema, release, CREDIBLE_SET_FOLDER, _load_credible_set_file)
    logger.info(f"[OT-EBI] Loaded {n_studies:,} studies and {n_cs:,} credible sets")

    with conn.cursor() as cur:
        for table in ("opentargets_studies", "credible_sets"):
            cur.execute(cs_schema.duplicate_count_sql(schema, table))
            dupes = cur.fetchone()[0]
            if dupes:
                logger.warning(f"[OT-EBI] {table}: removing {dupes:,} duplicate primary-key rows")
                cur.execute(cs_schema.dedupe_sql(schema, table))
        logger.info("[OT-EBI] Building gwas_study_index, primary keys and indexes")
        cur.execute(cs_schema.build_study_index_sql(schema))
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
