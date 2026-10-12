#!/usr/bin/env python3
"""
Load OpenTargets GWAS study metadata (Parquet) into PostgreSQL.

This is the study-level index OpenTargets publishes separately from
credible_set/ (see load_credible_sets_to_postgres.py). It carries fields the
credible-sets dataset doesn't: projectId (authoritative source tag, e.g.
"GCST" or "FINNGEN_R12"), traitFromSource / traitFromSourceMappedIds (EFO/MONDO
ids), and summarystatsLocation (the raw sumstats file path). Use it to find
which OpenTargets study, if any, corresponds to a trait a user is analyzing,
independent of whether that study happens to already be in our own gwas_library.

Schema fields come from:
  https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/latest/output/study/

Connection is configured via environment variables:
  CREDIBLE_SETS_DB_HOST     (default: localhost)
  CREDIBLE_SETS_DB_PORT     (default: 5411)
  CREDIBLE_SETS_DB_NAME     (default: credible_sets)
  CREDIBLE_SETS_DB_USER     (default: credsets)
  CREDIBLE_SETS_DB_PASSWORD (required)

Usage:
  python scripts/load_opentargets_studies.py [--release latest]
"""

import argparse
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

BASE_URL = "https://ftp.ebi.ac.uk/pub/databases/opentargets/platform"
FOLDER = "study"
BATCH_SIZE = 5000

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS opentargets_studies (
    study_id                TEXT PRIMARY KEY,
    project_id              TEXT,
    trait_from_source       TEXT,
    trait_efo_ids           TEXT[],
    has_sumstats            BOOLEAN,
    summarystats_location   TEXT,
    n_samples               INTEGER
);
CREATE INDEX IF NOT EXISTS idx_ots_project_id ON opentargets_studies(project_id);
CREATE INDEX IF NOT EXISTS idx_ots_trait_efo  ON opentargets_studies USING GIN(trait_efo_ids);
"""

INSERT_SQL = """
INSERT INTO opentargets_studies (
    study_id, project_id, trait_from_source, trait_efo_ids,
    has_sumstats, summarystats_location, n_samples
) VALUES %s
ON CONFLICT (study_id) DO UPDATE SET
    project_id             = EXCLUDED.project_id,
    trait_from_source      = EXCLUDED.trait_from_source,
    trait_efo_ids          = EXCLUDED.trait_efo_ids,
    has_sumstats           = EXCLUDED.has_sumstats,
    summarystats_location  = EXCLUDED.summarystats_location,
    n_samples              = EXCLUDED.n_samples
"""


def _int(v):
    if v is None:
        return None
    if isinstance(v, (np.floating, float)):
        return None if math.isnan(v) else int(v)
    if isinstance(v, np.integer):
        return int(v)
    return int(v)


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
        v = v.tolist()
    return list(v) if len(v) else None


def row_to_tuple(row: dict) -> tuple:
    return (
        row["studyId"],
        row.get("projectId"),
        row.get("traitFromSource"),
        _list(row.get("traitFromSourceMappedIds")),
        _bool(row.get("hasSumstats")),
        row.get("summarystatsLocation"),
        _int(row.get("nSamples")),
    )


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


def process_file(conn, url: str, file_num: int, total: int) -> int:
    fname = url.split("/")[-1]
    print(f"  [{file_num}/{total}] {fname} ... ", end="", flush=True)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
        t0 = time.time()
        urllib.request.urlretrieve(url, tmp.name)
        table = pq.read_table(
            tmp.name,
            columns=[
                "studyId", "projectId", "studyType", "traitFromSource",
                "traitFromSourceMappedIds", "hasSumstats",
                "summarystatsLocation", "nSamples",
            ],
        )

    df = table.to_pandas()
    df = df[df["studyType"] == "gwas"]
    records = [row_to_tuple(r) for r in df.to_dict("records")]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, INSERT_SQL, records, page_size=BATCH_SIZE)
    conn.commit()

    print(f"{len(records):,} gwas studies  ({time.time()-t0:.1f}s)", flush=True)
    return len(records)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--release", default="latest",
        help="OpenTargets release tag, e.g. '25.06' (default: latest)",
    )
    args = parser.parse_args()

    print(f"\nOpenTargets GWAS studies -> PostgreSQL  (release={args.release})\n")

    conn = get_conn()
    print("Connected to PostgreSQL.")

    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    print("Schema created/verified.")

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM opentargets_studies")
        existing = cur.fetchone()[0]
    if existing > 0:
        print(f"Data already loaded ({existing:,} studies). Skipping.")
        conn.close()
        return

    urls = list_parquet_urls(args.release)
    print(f"Found {len(urls)} parquet file(s).\n")

    total = 0
    t_start = time.time()
    for i, url in enumerate(urls, 1):
        try:
            total += process_file(conn, url, i, len(urls))
        except Exception as exc:
            print(f"ERROR on {url}: {exc}", file=sys.stderr)
            conn.rollback()

    elapsed = time.time() - t_start
    conn.close()

    print(f"\n{'-'*60}")
    print(f"Loaded {total:,} GWAS studies in {elapsed/60:.1f} min")


if __name__ == "__main__":
    main()
