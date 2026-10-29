"""Contract tests for the shared OpenTargets schema (src/db/credible_sets_schema.py):
the tables/columns both consumers query must exist, values convert correctly,
and the Python handler only ever reads GWAS rows."""
import json
from unittest.mock import MagicMock

import pytest

from src.db import credible_sets_handler as handler_mod
from src.db import credible_sets_schema as cs_schema
from src.services.ot_ebi_loader import convert_value, table_row

# Columns the Prolog backend queries (pl/queries.pl on the wip branch) from
# the OpenTargets tables. Non-OT tables (gencode_gene, catlas_*, ...) and
# coloc are not part of this schema yet.
PROLOG_COLUMNS = {
    "variant": {"variantid", "chromosome", "position", "referenceallele", "alternateallele", "rsids"},
    "credible_set": {"studylocusid", "studyid", "variantid", "locus"},
    "credible_set_variants": {"variantid", "studyid", "is95credibleset"},
    "study": {"studyid", "geneid", "studytype", "biosampleid", "biosamplefromsourceid"},
    "biosample": {"biosampleid", "biosamplename"},
    "enhancer": {"intervalid", "chromosome", "start", "end"},
}

# Columns the Python handler queries.
PYTHON_COLUMNS = {
    "credible_set": {
        "studylocusid", "studyid", "variantid", "chromosome", "position", "beta",
        "pvaluemantissa", "pvalueexponent", "effectallelefrequencyfromsource",
        "standarderror", "finemappingmethod", "confidence", "locus", "studytype",
    },
    "study": {
        "studyid", "studytype", "projectid", "traitfromsource", "traitfromsourcemappedids",
        "hassumstats", "summarystatslocation", "nsamples",
    },
    "gwas_study_index": {"studyid", "credible_set_count"},
}


@pytest.mark.parametrize("consumer", [PROLOG_COLUMNS, PYTHON_COLUMNS], ids=["prolog", "python"])
def test_schema_has_every_column_consumers_query(consumer):
    for table, columns in consumer.items():
        assert table in cs_schema.TABLE_COLUMNS, table
        missing = columns - set(cs_schema.TABLE_COLUMNS[table])
        assert not missing, f"{table} missing {missing}"


def test_rsids_is_varchar_array_for_prolog_containment_query():
    variant = next(t for t in cs_schema.LOADED_TABLES if t.name == "variant")
    rsids = next(c for c in variant.columns if c.name == "rsids")
    assert rsids.pg_type == "VARCHAR[]"


def test_reserved_column_names_are_quoted():
    assert '"end" BIGINT' in cs_schema.create_tables_sql("ot_staging")
    assert '"end"' in cs_schema.column_list("enhancer")


def test_legacy_tables_are_not_reused():
    assert not set(cs_schema.LEGACY_TABLES) & set(cs_schema.OT_TABLES)


def test_gwas_study_index_only_counts_gwas():
    gwas_sql = cs_schema.derive_sql("ot_staging")[1]
    assert "gwas_study_index" in gwas_sql and "studytype = 'gwas'" in gwas_sql


# ── value conversion ───────────────────────────────────────────────────────

def test_convert_value_handles_nulls_nan_and_empty_lists():
    assert convert_value(None, "text") is None
    assert convert_value(float("nan"), "float") is None
    assert convert_value([], "text_array") is None
    assert convert_value(["rs1", "rs2"], "text_array") == ["rs1", "rs2"]
    assert convert_value(True, "bool") is True
    assert convert_value(12.0, "int") == 12


def test_convert_json_keeps_ot_keys_and_drops_nan():
    locus = [{"variantId": "1_100_A_G", "posteriorProbability": 0.9,
              "is95CredibleSet": True, "beta": float("nan")}]
    out = json.loads(convert_value(locus, "json"))
    assert out == [{"variantId": "1_100_A_G", "posteriorProbability": 0.9,
                    "is95CredibleSet": True, "beta": None}]


def test_table_row_reads_listed_parquet_fields_in_column_order():
    variant = next(t for t in cs_schema.LOADED_TABLES if t.name == "variant")
    record = {"variantId": "1_100_A_G", "chromosome": "1", "position": 100,
              "referenceAllele": "A", "alternateAllele": "G", "rsIds": ["rs1"],
              "alleleFrequencies": [{"ignored": 1}]}
    assert table_row(variant, record) == ("1_100_A_G", "1", 100, "A", "G", ["rs1"])


# ── the Python handler only reads GWAS ─────────────────────────────────────

@pytest.fixture
def recorded_sql(monkeypatch):
    cur = MagicMock()
    cur.fetchone.return_value = None
    cur.fetchall.return_value = []
    conn = MagicMock(closed=False)
    conn.cursor.return_value.__enter__.return_value = cur
    h = handler_mod.CredibleSetsHandler()
    h._conn = conn
    return h, cur


@pytest.mark.parametrize(
    "call",
    [
        lambda h: h.get_study_by_id("GCST1"),
        lambda h: h.search_studies_by_trait("asthma"),
        lambda h: h.get_all_credible_sets_for_study("GCST1"),
        lambda h: h.get_credible_sets_for_region("GCST1", "1", 1, 2),
    ],
    ids=["get_study_by_id", "search_studies_by_trait", "get_all_credible_sets", "region"],
)
def test_handler_queries_filter_to_gwas(recorded_sql, call):
    h, cur = recorded_sql
    call(h)
    sql = cur.execute.call_args.args[0]
    assert "studytype = 'gwas'" in sql


def test_has_credible_sets_uses_gwas_only_index(recorded_sql):
    h, cur = recorded_sql
    h.study_has_credible_sets("GCST1")
    assert "FROM gwas_study_index" in cur.execute.call_args.args[0]
