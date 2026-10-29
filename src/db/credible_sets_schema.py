"""
Single source of truth for the shared OpenTargets Postgres schema, used by both
the Python pipeline and the Prolog backend (pl/queries.pl), plus the
ot_load_state marker table.

Table and column names follow the Prolog backend: OpenTargets' camelCase field
names lowercased (studyLocusId -> studylocusid). Columns are chosen by what the
consumers query, not by what the parquet files contain:

  table                  source (OT folder)  used by
  credible_set           credible_set        Python (GWAS rows only), Prolog
  study                  study               Python (GWAS rows only), Prolog
  variant                variant             Prolog
  biosample              biosample           Prolog
  enhancer               interval            Prolog
  credible_set_variants  derived (locus)     Prolog
  gwas_study_index       derived             Python

All study types are loaded (Prolog needs QTL credible sets and studies); the
Python handler filters on studytype = 'gwas' at query time.

Every DDL helper takes a target schema so the bootstrap can build a complete
copy in `ot_staging` and swap it into `public` atomically (see
src/services/ot_db_bootstrap.py). Table DDL and index/PK DDL are separate so
bulk loads run against bare tables and indexes are built once at the end.

Bump SCHEMA_VERSION on any DDL change: deployments then treat the loaded data
as stale and reload it (and look for a matching `schema-v<N>` snapshot in MinIO).
"""

import os
from dataclasses import dataclass

SCHEMA_VERSION = 2

PUBLIC_SCHEMA = "public"
STAGING_SCHEMA = "ot_staging"
_ALLOWED_SCHEMAS = {PUBLIC_SCHEMA, STAGING_SCHEMA}

DATASET = "opentargets"


@dataclass(frozen=True)
class Column:
    name: str             # Postgres column name
    pg_type: str
    source: str = ""      # parquet field it is loaded from ("" for derived tables)
    kind: str = "text"    # value conversion: text | int | float | bool | text_array | json


@dataclass(frozen=True)
class LoadedTable:
    name: str
    folder: str           # OpenTargets release output folder
    columns: tuple


def _col(name, pg_type, source, kind="text"):
    return Column(name, pg_type, source, kind)


# Tables loaded straight from an OpenTargets release, in load order.
LOADED_TABLES = (
    LoadedTable("study", "study", (
        _col("studyid", "TEXT NOT NULL", "studyId"),
        _col("studytype", "TEXT", "studyType"),
        _col("projectid", "TEXT", "projectId"),
        _col("traitfromsource", "TEXT", "traitFromSource"),
        _col("traitfromsourcemappedids", "TEXT[]", "traitFromSourceMappedIds", "text_array"),
        _col("hassumstats", "BOOLEAN", "hasSumstats", "bool"),
        _col("summarystatslocation", "TEXT", "summarystatsLocation"),
        _col("nsamples", "INTEGER", "nSamples", "int"),
        _col("geneid", "TEXT", "geneId"),
        _col("biosampleid", "TEXT", "biosampleId"),
        _col("biosamplefromsourceid", "TEXT", "biosampleFromSourceId"),
    )),
    LoadedTable("biosample", "biosample", (
        _col("biosampleid", "TEXT NOT NULL", "biosampleId"),
        _col("biosamplename", "TEXT", "biosampleName"),
    )),
    LoadedTable("variant", "variant", (
        _col("variantid", "TEXT NOT NULL", "variantId"),
        _col("chromosome", "TEXT", "chromosome"),
        _col("position", "INTEGER", "position", "int"),
        _col("referenceallele", "TEXT", "referenceAllele"),
        _col("alternateallele", "TEXT", "alternateAllele"),
        # varchar[]: Prolog matches with `rsids @> ARRAY[...]::varchar[]`
        _col("rsids", "VARCHAR[]", "rsIds", "text_array"),
    )),
    LoadedTable("enhancer", "interval", (
        _col("intervalid", "TEXT", "intervalId"),
        _col("chromosome", "TEXT", "chromosome"),
        _col("start", "BIGINT", "start", "int"),
        _col("end", "BIGINT", "end", "int"),
    )),
    LoadedTable("credible_set", "credible_set", (
        _col("studylocusid", "TEXT NOT NULL", "studyLocusId"),
        _col("studyid", "TEXT NOT NULL", "studyId"),
        _col("variantid", "TEXT NOT NULL", "variantId"),
        _col("studytype", "TEXT", "studyType"),
        _col("chromosome", "TEXT", "chromosome"),
        _col("position", "INTEGER", "position", "int"),
        _col("beta", "DOUBLE PRECISION", "beta", "float"),
        _col("pvaluemantissa", "REAL", "pValueMantissa", "float"),
        _col("pvalueexponent", "INTEGER", "pValueExponent", "int"),
        _col("effectallelefrequencyfromsource", "REAL", "effectAlleleFrequencyFromSource", "float"),
        _col("standarderror", "DOUBLE PRECISION", "standardError", "float"),
        _col("finemappingmethod", "TEXT", "finemappingMethod"),
        _col("confidence", "TEXT", "confidence"),
        # Keeps OT's camelCase keys (variantId, posteriorProbability, is95CredibleSet)
        _col("locus", "JSONB", "locus", "json"),
    )),
)

# Tables built from the loaded ones with SQL (see derive_sql).
DERIVED_TABLES = {
    "credible_set_variants": (
        Column("studylocusid", "TEXT NOT NULL"),
        Column("studyid", "TEXT NOT NULL"),
        Column("variantid", "TEXT"),
        Column("is95credibleset", "BOOLEAN"),
    ),
    "gwas_study_index": (
        Column("studyid", "TEXT NOT NULL"),
        Column("credible_set_count", "INTEGER NOT NULL"),
        Column("chromosomes", "TEXT[]"),
        Column("finemapping_methods", "TEXT[]"),
    ),
}

TABLE_COLUMNS = {
    **{t.name: tuple(c.name for c in t.columns) for t in LOADED_TABLES},
    **{name: tuple(c.name for c in cols) for name, cols in DERIVED_TABLES.items()},
}

# All tables the bootstrap loads, validates, counts and swaps together.
OT_TABLES = tuple(TABLE_COLUMNS)

# Tables from SCHEMA_VERSION 1; dropped on swap so data isn't kept twice.
LEGACY_TABLES = ("credible_sets", "opentargets_studies")

# Primary-key column per table, used to de-duplicate before adding the PK.
# Tables not listed (enhancer, credible_set_variants) have no natural key.
PRIMARY_KEYS = {
    "study": "studyid",
    "biosample": "biosampleid",
    "variant": "variantid",
    "credible_set": "studylocusid",
    "gwas_study_index": "studyid",
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


def quote_ident(name: str) -> str:
    """Quote a column name ("end" is reserved; quoting lowercase names is a no-op otherwise)."""
    return '"' + name.replace('"', '""') + '"'


def column_list(table: str) -> str:
    return ", ".join(quote_ident(c) for c in TABLE_COLUMNS[table])


def _columns_of(table: str) -> tuple:
    for t in LOADED_TABLES:
        if t.name == table:
            return t.columns
    return DERIVED_TABLES[table]


def create_tables_sql(schema: str) -> str:
    """Bare tables (no PK, no indexes) — fast to bulk-load into."""
    s = _check_schema(schema)
    statements = []
    for table in OT_TABLES:
        cols = ",\n    ".join(f"{quote_ident(c.name)} {c.pg_type}" for c in _columns_of(table))
        statements.append(f"CREATE TABLE {s}.{table} (\n    {cols}\n);")
    return "\n".join(statements)


def derive_sql(schema: str) -> list:
    """INSERTs that build the derived tables from the loaded ones."""
    s = _check_schema(schema)
    return [
        # One row per variant in each credible set (Prolog: qtl_association).
        f"""
INSERT INTO {s}.credible_set_variants (studylocusid, studyid, variantid, is95credibleset)
SELECT cs.studylocusid, cs.studyid, e->>'variantId', (e->>'is95CredibleSet')::boolean
FROM {s}.credible_set cs
CROSS JOIN LATERAL jsonb_array_elements(cs.locus) e
WHERE jsonb_typeof(cs.locus) = 'array';
""",
        # Fast "does this GWAS study have credible sets?" lookups (Python).
        f"""
INSERT INTO {s}.gwas_study_index (studyid, credible_set_count, chromosomes, finemapping_methods)
SELECT
    studyid,
    COUNT(*),
    ARRAY_AGG(DISTINCT chromosome ORDER BY chromosome),
    ARRAY_AGG(DISTINCT finemappingmethod ORDER BY finemappingmethod)
FROM {s}.credible_set
WHERE studytype = 'gwas'
GROUP BY studyid;
""",
    ]


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
    pks = "\n".join(
        f"ALTER TABLE {s}.{table} ADD CONSTRAINT {table}_pkey PRIMARY KEY ({pk});"
        for table, pk in PRIMARY_KEYS.items()
    )
    return pks + f"""

CREATE INDEX idx_credible_set_studyid   ON {s}.credible_set(studyid);
CREATE INDEX idx_credible_set_variantid ON {s}.credible_set(variantid);
CREATE INDEX idx_credible_set_chr_pos   ON {s}.credible_set(chromosome, position);

CREATE INDEX idx_study_projectid ON {s}.study(projectid);
CREATE INDEX idx_study_studytype ON {s}.study(studytype);
CREATE INDEX idx_study_trait_ids ON {s}.study USING GIN(traitfromsourcemappedids);

CREATE INDEX idx_variant_chr_pos ON {s}.variant(chromosome, position);
CREATE INDEX idx_variant_rsids   ON {s}.variant USING GIN(rsids);

CREATE INDEX idx_enhancer_chr_start ON {s}.enhancer(chromosome, start, "end");

CREATE INDEX idx_csv_variantid ON {s}.credible_set_variants(variantid);
CREATE INDEX idx_csv_studyid   ON {s}.credible_set_variants(studyid);
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
