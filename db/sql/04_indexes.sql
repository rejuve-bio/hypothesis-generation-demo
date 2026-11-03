-- Primary keys and indexes, built after the bulk load (current search_path).

ALTER TABLE study            ADD CONSTRAINT study_pkey            PRIMARY KEY (studyid);
ALTER TABLE biosample        ADD CONSTRAINT biosample_pkey        PRIMARY KEY (biosampleid);
ALTER TABLE variant          ADD CONSTRAINT variant_pkey          PRIMARY KEY (variantid);
ALTER TABLE credible_set     ADD CONSTRAINT credible_set_pkey     PRIMARY KEY (studylocusid);
ALTER TABLE gwas_study_index ADD CONSTRAINT gwas_study_index_pkey PRIMARY KEY (studyid);

CREATE INDEX idx_credible_set_studyid   ON credible_set (studyid);
CREATE INDEX idx_credible_set_variantid ON credible_set (variantid);
CREATE INDEX idx_credible_set_chr_pos   ON credible_set (chromosome, position);

CREATE INDEX idx_study_projectid ON study (projectid);
CREATE INDEX idx_study_studytype ON study (studytype);
CREATE INDEX idx_study_trait_ids ON study USING GIN (traitfromsourcemappedids);

CREATE INDEX idx_variant_chr_pos ON variant (chromosome, position);
CREATE INDEX idx_variant_rsids   ON variant USING GIN (rsids);

CREATE INDEX idx_enhancer_chr_start ON enhancer (chromosome, start, "end");

CREATE INDEX idx_csv_variantid ON credible_set_variants (variantid);
CREATE INDEX idx_csv_studyid   ON credible_set_variants (studyid);
