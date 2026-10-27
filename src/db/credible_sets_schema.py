"""
Single source of truth for the OpenTargets Postgres schema (credible_sets,
gwas_study_index, opentargets_studies) plus the ot_load_state marker table.

Every DDL helper takes a target schema so the bootstrap can build a complete
copy in `ot_staging` and swap it into `public` atomically (see
src/services/ot_db_bootstrap.py). Table DDL and index/PK DDL are separate so
bulk loads run against bare tables and indexes are built once at the end.

Bump SCHEMA_VERSION on any DDL change: deployments then treat the loaded data
as stale and reload it (and look for a matching `schema-v<N>` snapshot in MinIO).
"""

import os

SCHEMA_VERSION = 1

PUBLIC_SCHEMA = "public"
STAGING_SCHEMA = "ot_staging"
_ALLOWED_SCHEMAS = {PUBLIC_SCHEMA, STAGING_SCHEMA}

DATASET = "opentargets"

# Order matters for the swap only in that all three move together.
OT_TABLES = ("credible_sets", "gwas_study_index", "opentargets_studies")

CREDIBLE_SETS_COLUMNS = (
    "study_locus_id", "study_id", "variant_id", "chromosome", "position",
    "region", "beta", "z_score", "p_value_mantissa", "p_value_exponent",
    "effect_allele_frequency", "standard_error", "sub_study_description",
    "quality_controls", "finemap_method", "credible_set_index",
    "credible_set_log10bf", "purity_mean_r2", "purity_min_r2",
    "locus_start", "locus_end", "sample_size", "ld_set", "locus",
    "confidence", "study_type", "is_trans_qtl",
)

OPENTARGETS_STUDIES_COLUMNS = (
    "study_id", "project_id", "trait_from_source", "trait_efo_ids",
    "has_sumstats", "summarystats_location", "n_samples",
)

GWAS_STUDY_INDEX_COLUMNS = ("study_id", "credible_set_count", "chromosomes", "finemap_methods")

TABLE_COLUMNS = {
    "credible_sets": CREDIBLE_SETS_COLUMNS,
    "gwas_study_index": GWAS_STUDY_INDEX_COLUMNS,
    "opentargets_studies": OPENTARGETS_STUDIES_COLUMNS,
}

# Primary-key column per table, used to de-duplicate before adding the PK.
PRIMARY_KEYS = {
    "credible_sets": "study_locus_id",
    "gwas_study_index": "study_id",
    "opentargets_studies": "study_id",
}

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


def _check_schema(schema: str) -> str:
    if schema not in _ALLOWED_SCHEMAS:
        raise ValueError(f"Unexpected target schema {schema!r}")
    return schema


def create_tables_sql(schema: str) -> str:
    """Bare tables (no PK, no indexes) — fast to bulk-load into."""
    s = _check_schema(schema)
    return f"""
CREATE TABLE {s}.credible_sets (
    study_locus_id          TEXT NOT NULL,
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

-- Lightweight study-level summary used for fast "has credible sets?" checks.
CREATE TABLE {s}.gwas_study_index (
    study_id            TEXT NOT NULL,
    credible_set_count  INTEGER NOT NULL,
    chromosomes         TEXT[],
    finemap_methods     TEXT[]
);

-- OpenTargets study-level index. Used to confirm a GWAS file's OpenTargets
-- provenance independent of whether it already has precomputed credible sets.
CREATE TABLE {s}.opentargets_studies (
    study_id                TEXT NOT NULL,
    project_id              TEXT,
    trait_from_source       TEXT,
    trait_efo_ids           TEXT[],
    has_sumstats            BOOLEAN,
    summarystats_location   TEXT,
    n_samples               INTEGER
);
"""


def build_study_index_sql(schema: str) -> str:
    s = _check_schema(schema)
    return f"""
INSERT INTO {s}.gwas_study_index (study_id, credible_set_count, chromosomes, finemap_methods)
SELECT
    study_id,
    COUNT(*)                                                    AS credible_set_count,
    ARRAY_AGG(DISTINCT chromosome ORDER BY chromosome)          AS chromosomes,
    ARRAY_AGG(DISTINCT finemap_method ORDER BY finemap_method)  AS finemap_methods
FROM {s}.credible_sets
GROUP BY study_id;
"""


def dedupe_sql(schema: str, table: str) -> str:
    """Drop all but one physical row per primary key (keeps the last-loaded)."""
    s = _check_schema(schema)
    pk = PRIMARY_KEYS[table]
    return f"""
DELETE FROM {s}.{table} a
USING {s}.{table} b
WHERE a.{pk} = b.{pk} AND a.ctid < b.ctid;
"""


def duplicate_count_sql(schema: str, table: str) -> str:
    s = _check_schema(schema)
    pk = PRIMARY_KEYS[table]
    return f"SELECT COUNT(*) - COUNT(DISTINCT {pk}) FROM {s}.{table};"


def create_indexes_sql(schema: str) -> str:
    """Primary keys + secondary indexes, built after the bulk load."""
    s = _check_schema(schema)
    return f"""
ALTER TABLE {s}.credible_sets       ADD CONSTRAINT credible_sets_pkey       PRIMARY KEY (study_locus_id);
ALTER TABLE {s}.gwas_study_index    ADD CONSTRAINT gwas_study_index_pkey    PRIMARY KEY (study_id);
ALTER TABLE {s}.opentargets_studies ADD CONSTRAINT opentargets_studies_pkey PRIMARY KEY (study_id);

CREATE INDEX idx_cs_study_id    ON {s}.credible_sets(study_id);
CREATE INDEX idx_cs_variant_id  ON {s}.credible_sets(variant_id);
CREATE INDEX idx_cs_chr_pos     ON {s}.credible_sets(chromosome, position);
CREATE INDEX idx_cs_study_type  ON {s}.credible_sets(study_type);
CREATE INDEX idx_cs_region      ON {s}.credible_sets(region);

CREATE INDEX idx_ots_project_id ON {s}.opentargets_studies(project_id);
CREATE INDEX idx_ots_trait_efo  ON {s}.opentargets_studies USING GIN(trait_efo_ids);
"""


def connection_params_from_env() -> dict:
    """psycopg2.connect() kwargs for the credible-sets database."""
    return {
        "host": os.environ.get("CREDIBLE_SETS_DB_HOST", "localhost"),
        "port": int(os.environ.get("CREDIBLE_SETS_DB_PORT", "5411")),
        "dbname": os.environ.get("CREDIBLE_SETS_DB_NAME", "credible_sets"),
        "user": os.environ.get("CREDIBLE_SETS_DB_USER", "credsets"),
        "password": os.environ.get("CREDIBLE_SETS_DB_PASSWORD", ""),
    }
