#!/usr/bin/env python3
"""
Load OpenTargets credible sets (Parquet) into PostgreSQL.

Schema fields come from:
  https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/latest/output/credible_set/

Connection is configured via environment variables:
  CREDIBLE_SETS_DB_HOST     (default: localhost)
  CREDIBLE_SETS_DB_PORT     (default: 5411)
  CREDIBLE_SETS_DB_NAME     (default: credible_sets)
  CREDIBLE_SETS_DB_USER     (default: credsets)
  CREDIBLE_SETS_DB_PASSWORD (required)

Usage:
  python scripts/load_credible_sets_to_postgres.py [--release 25.06] [--resume]
"""

import argparse
import json
import math
import os
import re
import sys
import tempfile
import time
import urllib.request

import numpy as np
import psycopg2
import psycopg2.extras
import pyarrow.parquet as pq

# ── configuration ────────────────────────────────────────────────────────────

BASE_URL = "https://ftp.ebi.ac.uk/pub/databases/opentargets/platform"
FOLDER = "credible_set"
BATCH_SIZE = 5000
# Row-group read batch size for streaming a parquet file instead of loading it
# whole into memory (a single file can be ~235MB, enough to OOM the loader).
CHUNK_ROWS = 1_000
# Rows vary wildly in size — some credible sets carry huge `locus`/`ld_set`
# JSONB blobs (one file had a row group averaging ~27KB/row). Capping a
# single execute_values() call by row count alone still let a ~135MB insert
# through, which is what actually killed the Postgres connection. So each
# insert is also capped by estimated serialized bytes.
MAX_INSERT_BYTES = 8 * 1024 * 1024
# Retries per chunk insert before giving up on that chunk and moving on.
CHUNK_RETRIES = 2

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS credible_sets (
    study_locus_id          TEXT PRIMARY KEY,
    study_id                TEXT NOT NULL,
    variant_id              TEXT NOT NULL,
    chromosome              TEXT,
    position                INTEGER,
    region                  TEXT,
    beta                    DOUBLE PRECISION,
    z_score                 DOUBLE PRECISION,
    p_value_mantissa        REAL,
    p_value_exponent        INTEGER,
    effect_allele_frequency REAL,
    standard_error          DOUBLE PRECISION,
    sub_study_description   TEXT,
    quality_controls        TEXT[],
    finemap_method          TEXT,
    credible_set_index      INTEGER,
    credible_set_log10bf    DOUBLE PRECISION,
    purity_mean_r2          DOUBLE PRECISION,
    purity_min_r2           DOUBLE PRECISION,
    locus_start             INTEGER,
    locus_end               INTEGER,
    sample_size             INTEGER,
    ld_set                  JSONB,
    locus                   JSONB,
    confidence              TEXT,
    study_type              TEXT,
    is_trans_qtl            BOOLEAN
);
CREATE INDEX IF NOT EXISTS idx_cs_study_id    ON credible_sets(study_id);
CREATE INDEX IF NOT EXISTS idx_cs_variant_id  ON credible_sets(variant_id);
CREATE INDEX IF NOT EXISTS idx_cs_chr_pos     ON credible_sets(chromosome, position);
CREATE INDEX IF NOT EXISTS idx_cs_study_type  ON credible_sets(study_type);
CREATE INDEX IF NOT EXISTS idx_cs_region      ON credible_sets(region);

CREATE TABLE IF NOT EXISTS gwas_study_index (
    study_id            TEXT PRIMARY KEY,
    credible_set_count  INTEGER NOT NULL,
    chromosomes         TEXT[],
    finemap_methods     TEXT[]
);
"""

BUILD_STUDY_INDEX_SQL = """
INSERT INTO gwas_study_index (study_id, credible_set_count, chromosomes, finemap_methods)
SELECT
    study_id,
    COUNT(*)                                                    AS credible_set_count,
    ARRAY_AGG(DISTINCT chromosome ORDER BY chromosome)          AS chromosomes,
    ARRAY_AGG(DISTINCT finemap_method ORDER BY finemap_method)  AS finemap_methods
FROM credible_sets
GROUP BY study_id
ON CONFLICT (study_id) DO UPDATE SET
    credible_set_count = EXCLUDED.credible_set_count,
    chromosomes        = EXCLUDED.chromosomes,
    finemap_methods    = EXCLUDED.finemap_methods;
"""

INSERT_SQL = """
INSERT INTO credible_sets (
    study_locus_id, study_id, variant_id, chromosome, position,
    region, beta, z_score, p_value_mantissa, p_value_exponent,
    effect_allele_frequency, standard_error, sub_study_description,
    quality_controls, finemap_method, credible_set_index,
    credible_set_log10bf, purity_mean_r2, purity_min_r2,
    locus_start, locus_end, sample_size, ld_set, locus,
    confidence, study_type, is_trans_qtl
) VALUES %s
ON CONFLICT (study_locus_id) DO NOTHING
"""


# ── type helpers ─────────────────────────────────────────────────────────────

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
    if isinstance(v, np.integer):
        return int(v)
    return int(v) if v is not None else None


def _bool(v):
    if v is None:
        return None
    if isinstance(v, (np.bool_,)):
        return bool(v)
    return v


def _list(v):
    if v is None:
        return None
    if isinstance(v, np.ndarray):
        return v.tolist()
    return list(v) if v is not None else None


def _json(v):
    if v is None:
        return None
    if isinstance(v, np.ndarray):
        v = v.tolist()

    def default(obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return None if math.isnan(obj) else float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return str(obj)

    return json.dumps(v, default=default)


def row_to_tuple(row: dict) -> tuple:
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


# ── database / FTP helpers ───────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=os.environ.get("CREDIBLE_SETS_DB_HOST", "localhost"),
        port=int(os.environ.get("CREDIBLE_SETS_DB_PORT", "5411")),
        dbname=os.environ.get("CREDIBLE_SETS_DB_NAME", "credible_sets"),
        user=os.environ.get("CREDIBLE_SETS_DB_USER", "credsets"),
        password=os.environ.get("CREDIBLE_SETS_DB_PASSWORD", ""),
    )


def list_parquet_urls(release: str) -> list:
    url = f"{BASE_URL}/{release}/output/{FOLDER}/"
    with urllib.request.urlopen(url) as resp:
        html = resp.read().decode()
    files = re.findall(r'href="([^"]+\.parquet)"', html)
    if not files:
        raise RuntimeError(f"No parquet files found at {url}")
    return sorted(f"{BASE_URL}/{release}/output/{FOLDER}/{f}" for f in files)


def already_loaded_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM credible_sets")
        return cur.fetchone()[0]


def insert_chunk_with_retry(conn, records: list):
    """
    Insert one chunk of rows, reconnecting and retrying if the connection
    died server-side (e.g. OOM-killed) mid-insert. Returns the (possibly new)
    connection object — callers must keep using the returned conn.
    """
    last_exc = None
    for attempt in range(CHUNK_RETRIES + 1):
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, INSERT_SQL, records, page_size=BATCH_SIZE)
            conn.commit()
            return conn
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            last_exc = exc
            print(f"    DB connection lost ({exc}); reconnecting (attempt {attempt + 1})", file=sys.stderr)
            try:
                conn.close()
            except Exception:
                pass
            conn = get_conn()
    raise last_exc


def _estimate_row_bytes(record: tuple) -> int:
    """Cheap size estimate dominated by the locus/ld_set JSONB text fields."""
    size = 200  # fixed-width columns overhead
    for v in record:
        if isinstance(v, str):
            size += len(v)
    return size


def process_file(conn, url: str, file_num: int, total: int):
    """
    Stream one parquet file in row-group-sized reads, then flush inserts in
    byte-capped sub-batches (see MAX_INSERT_BYTES) so memory usage AND the
    size of any single execute_values() call stay bounded, regardless of how
    large individual rows are. Returns (conn, rows_inserted) — conn may be a
    new connection object if a reconnect happened mid-file.
    """
    fname = url.split("/")[-1]
    print(f"  [{file_num}/{total}] {fname} ... ", end="", flush=True)

    inserted = 0
    pending, pending_bytes = [], 0
    t0 = time.time()

    def flush():
        nonlocal conn, inserted, pending, pending_bytes
        if not pending:
            return
        conn = insert_chunk_with_retry(conn, pending)
        inserted += len(pending)
        pending, pending_bytes = [], 0

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
        urllib.request.urlretrieve(url, tmp.name)
        parquet_file = pq.ParquetFile(tmp.name)

        for batch in parquet_file.iter_batches(batch_size=CHUNK_ROWS):
            df = batch.to_pandas()
            df = df[df["studyType"] == "gwas"]
            if df.empty:
                continue
            for row in df.to_dict("records"):
                record = row_to_tuple(row)
                record_bytes = _estimate_row_bytes(record)
                if pending and pending_bytes + record_bytes > MAX_INSERT_BYTES:
                    flush()
                pending.append(record)
                pending_bytes += record_bytes
        flush()

    print(f"{inserted:,} gwas rows  ({time.time()-t0:.1f}s)", flush=True)
    return conn, inserted


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--release",
        default="latest",
        help="OpenTargets release tag, e.g. '25.06' (default: latest)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip files whose rows are already present (uses ON CONFLICT DO NOTHING)",
    )
    args = parser.parse_args()

    print(f"\nOpenTargets credible sets → PostgreSQL  (release={args.release})\n")

    conn = get_conn()
    print(f"Connected to PostgreSQL.")

    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    print("Schema created/verified.")

    # Exit early if data is already fully loaded (avoid re-downloading on every restart)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM gwas_study_index")
        study_count = cur.fetchone()[0]
    if study_count > 0 and not args.resume:
        print(f"Data already loaded ({study_count:,} studies in gwas_study_index). Skipping.")
        conn.close()
        return

    if args.resume:
        n = already_loaded_count(conn)
        if n > 0:
            print(f"Resuming: {n:,} rows already in table (duplicates skipped).")

    urls = list_parquet_urls(args.release)
    print(f"Found {len(urls)} parquet files.\n")

    total_rows = 0
    t_start = time.time()

    for i, url in enumerate(urls, 1):
        try:
            conn, rows = process_file(conn, url, i, len(urls))
            total_rows += rows
        except Exception as exc:
            print(f"ERROR on {url}: {exc}", file=sys.stderr)
            try:
                conn.rollback()
            except Exception:
                # Connection is dead beyond the retries already attempted in
                # process_file — reconnect so the loop can continue to the
                # remaining files instead of exiting.
                try:
                    conn.close()
                except Exception:
                    pass
                conn = get_conn()

    elapsed = time.time() - t_start
    final_count = already_loaded_count(conn)

    print(f"\nBuilding gwas_study_index ...", end=" ", flush=True)
    with conn.cursor() as cur:
        cur.execute(BUILD_STUDY_INDEX_SQL)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM gwas_study_index")
        study_count = cur.fetchone()[0]
    print(f"{study_count:,} studies indexed.")

    conn.close()

    print(f"\n{'─'*60}")
    print(f"Loaded {total_rows:,} gwas rows in {elapsed/60:.1f} min")
    print(f"Total rows in credible_sets:  {final_count:,}")
    print(f"Total rows in gwas_study_index: {study_count:,}")


if __name__ == "__main__":
    main()
