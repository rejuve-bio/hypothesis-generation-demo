-- ot_raw (parquet field names) -> final tables in the current search_path.
-- DISTINCT ON keeps one row per primary key in case a release repeats one.

INSERT INTO study
SELECT DISTINCT ON ("studyId")
       "studyId", "studyType", "projectId", "traitFromSource",
       NULLIF("traitFromSourceMappedIds", '{}'), "hasSumstats",
       "summarystatsLocation", "nSamples", "geneId", "biosampleId",
       "biosampleFromSourceId"
FROM ot_raw.study
WHERE "studyId" IS NOT NULL
ORDER BY "studyId";

INSERT INTO biosample
SELECT DISTINCT ON ("biosampleId") "biosampleId", "biosampleName"
FROM ot_raw.biosample
WHERE "biosampleId" IS NOT NULL
ORDER BY "biosampleId";

INSERT INTO variant
SELECT DISTINCT ON ("variantId")
       "variantId", "chromosome", "position", "referenceAllele",
       "alternateAllele", NULLIF("rsIds", '{}')::VARCHAR[]
FROM ot_raw.variant
WHERE "variantId" IS NOT NULL
ORDER BY "variantId";

INSERT INTO enhancer
SELECT "intervalId", "chromosome", "start", "end"
FROM ot_raw.interval;

INSERT INTO credible_set
SELECT DISTINCT ON ("studyLocusId")
       "studyLocusId", "studyId", "variantId", "studyType", "chromosome",
       "position", "beta", "pValueMantissa", "pValueExponent",
       "effectAlleleFrequencyFromSource", "standardError",
       "finemappingMethod", "confidence", to_jsonb("locus")
FROM ot_raw.credible_set
WHERE "studyLocusId" IS NOT NULL
ORDER BY "studyLocusId";

-- One row per variant in each credible set (Prolog: qtl_association).
INSERT INTO credible_set_variants (studylocusid, studyid, variantid, is95credibleset)
SELECT cs.studylocusid, cs.studyid, e->>'variantId', (e->>'is95CredibleSet')::BOOLEAN
FROM credible_set cs
CROSS JOIN LATERAL jsonb_array_elements(cs.locus) e
WHERE jsonb_typeof(cs.locus) = 'array';

-- Fast "does this GWAS study have credible sets?" lookups (Python).
INSERT INTO gwas_study_index (studyid, credible_set_count, chromosomes, finemapping_methods)
SELECT studyid,
       COUNT(*),
       ARRAY_AGG(DISTINCT chromosome ORDER BY chromosome),
       ARRAY_AGG(DISTINCT finemappingmethod ORDER BY finemappingmethod)
FROM credible_set
WHERE studytype = 'gwas'
GROUP BY studyid;
