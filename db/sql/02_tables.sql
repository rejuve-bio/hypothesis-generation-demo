-- Final tables, shared by the Python pipeline and the Prolog backend
-- (pl/queries.pl). Names follow the Prolog backend: OpenTargets field names
-- lowercased (studyLocusId -> studylocusid). Columns are the ones the two
-- consumers query.
--
--   table                  source (OT folder)  used by
--   study                  study               Python (GWAS rows only), Prolog
--   biosample              biosample           Prolog
--   variant                variant             Prolog
--   enhancer               interval            Prolog
--   credible_set           credible_set        Python (GWAS rows only), Prolog
--   credible_set_variants  derived (locus)     Prolog
--   gwas_study_index       derived             Python
--
-- Created unqualified: the setup script sets search_path to the target schema
-- (ot_staging), then swaps the finished tables into public.
-- Bump SCHEMA_VERSION in ot_db_setup.py on any change here.

CREATE TABLE study (
    studyid                   TEXT NOT NULL,
    studytype                 TEXT,
    projectid                 TEXT,
    traitfromsource           TEXT,
    traitfromsourcemappedids  TEXT[],
    hassumstats               BOOLEAN,
    summarystatslocation      TEXT,
    nsamples                  INTEGER,
    geneid                    TEXT,
    biosampleid               TEXT,
    biosamplefromsourceid     TEXT
);

CREATE TABLE biosample (
    biosampleid    TEXT NOT NULL,
    biosamplename  TEXT
);

CREATE TABLE variant (
    variantid        TEXT NOT NULL,
    chromosome       TEXT,
    position         INTEGER,
    referenceallele  TEXT,
    alternateallele  TEXT,
    -- varchar[]: Prolog matches with `rsids @> ARRAY[...]::varchar[]`
    rsids            VARCHAR[]
);

CREATE TABLE enhancer (
    intervalid  TEXT,
    chromosome  TEXT,
    start       BIGINT,
    "end"       BIGINT
);

CREATE TABLE credible_set (
    studylocusid                     TEXT NOT NULL,
    studyid                          TEXT NOT NULL,
    variantid                        TEXT NOT NULL,
    studytype                        TEXT,
    chromosome                       TEXT,
    position                         INTEGER,
    beta                             DOUBLE PRECISION,
    pvaluemantissa                   REAL,
    pvalueexponent                   INTEGER,
    effectallelefrequencyfromsource  REAL,
    standarderror                    DOUBLE PRECISION,
    finemappingmethod                TEXT,
    confidence                       TEXT,
    -- keeps OT's camelCase keys (variantId, posteriorProbability, is95CredibleSet)
    locus                            JSONB
);

CREATE TABLE credible_set_variants (
    studylocusid     TEXT NOT NULL,
    studyid          TEXT NOT NULL,
    variantid        TEXT,
    is95credibleset  BOOLEAN
);

CREATE TABLE gwas_study_index (
    studyid              TEXT NOT NULL,
    credible_set_count   INTEGER NOT NULL,
    chromosomes          TEXT[],
    finemapping_methods  TEXT[]
);
