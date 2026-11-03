-- Landing tables for pg_parquet: COPY ot_raw.<table> FROM '<parquet url>'
-- WITH (format 'parquet', match_by 'name').
--
-- Column names are OpenTargets' parquet field names (quoted camelCase) because
-- match_by 'name' matches on them. Each table lists only the fields we keep;
-- extra fields in the file are ignored. 03_transform.sql renames them into the
-- final tables.

CREATE SCHEMA ot_raw;

-- pg_parquet loads a parquet list<struct> only into an array of a composite
-- type with exactly the same fields (names and count) as the struct. This
-- mirrors OpenTargets credible_set.locus; to_jsonb() later keeps these names
-- as the JSON keys the Prolog backend reads (variantId, posteriorProbability,
-- is95CredibleSet).
CREATE TYPE ot_raw.locus_item AS (
    "is95CredibleSet"       BOOLEAN,
    "is99CredibleSet"       BOOLEAN,
    "logBF"                 DOUBLE PRECISION,
    "posteriorProbability"  DOUBLE PRECISION,
    "variantId"             TEXT,
    "pValueMantissa"        REAL,
    "pValueExponent"        INTEGER,
    "beta"                  DOUBLE PRECISION,
    "standardError"         DOUBLE PRECISION,
    "r2Overall"             DOUBLE PRECISION
);

CREATE TABLE ot_raw.study (
    "studyId"                   TEXT,
    "studyType"                 TEXT,
    "projectId"                 TEXT,
    "traitFromSource"           TEXT,
    "traitFromSourceMappedIds"  TEXT[],
    "hasSumstats"               BOOLEAN,
    "summarystatsLocation"      TEXT,
    "nSamples"                  INTEGER,
    "geneId"                    TEXT,
    "biosampleId"               TEXT,
    "biosampleFromSourceId"     TEXT
);

CREATE TABLE ot_raw.biosample (
    "biosampleId"    TEXT,
    "biosampleName"  TEXT
);

CREATE TABLE ot_raw.variant (
    "variantId"        TEXT,
    "chromosome"       TEXT,
    "position"         INTEGER,
    "referenceAllele"  TEXT,
    "alternateAllele"  TEXT,
    "rsIds"            TEXT[]
);

CREATE TABLE ot_raw.interval (
    "intervalId"  TEXT,
    "chromosome"  TEXT,
    "start"       BIGINT,
    "end"         BIGINT
);

CREATE TABLE ot_raw.credible_set (
    "studyLocusId"                     TEXT,
    "studyId"                          TEXT,
    "variantId"                        TEXT,
    "studyType"                        TEXT,
    "chromosome"                       TEXT,
    "position"                         INTEGER,
    "beta"                             DOUBLE PRECISION,
    "pValueMantissa"                   REAL,
    "pValueExponent"                   INTEGER,
    "effectAlleleFrequencyFromSource"  REAL,
    "standardError"                    DOUBLE PRECISION,
    "finemappingMethod"                TEXT,
    "confidence"                       TEXT,
    "locus"                            ot_raw.locus_item[]
);
