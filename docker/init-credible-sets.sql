-- Initialisation script for credible-sets-postgres.
-- Runs automatically on first container startup (empty data volume).
-- Subsequent starts skip this file.

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

CREATE INDEX IF NOT EXISTS idx_cs_study_id   ON credible_sets(study_id);
CREATE INDEX IF NOT EXISTS idx_cs_variant_id ON credible_sets(variant_id);
CREATE INDEX IF NOT EXISTS idx_cs_chr_pos    ON credible_sets(chromosome, position);
CREATE INDEX IF NOT EXISTS idx_cs_study_type ON credible_sets(study_type);
CREATE INDEX IF NOT EXISTS idx_cs_region     ON credible_sets(region);

-- Lightweight study-level summary used for fast "has credible sets?" checks.
CREATE TABLE IF NOT EXISTS gwas_study_index (
    study_id            TEXT PRIMARY KEY,
    credible_set_count  INTEGER NOT NULL,
    chromosomes         TEXT[],
    finemap_methods     TEXT[]
);

-- OpenTargets study-level index (populated by scripts/load_opentargets_studies.py).
-- Used to confirm a GWAS file's OpenTargets provenance independent of whether it
-- already has precomputed credible sets.
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
