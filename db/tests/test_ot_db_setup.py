"""Tests for db/ot_db_setup.py and the SQL in db/sql — no database needed."""
import json
import re
import sys
from pathlib import Path

import pytest

DB_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DB_DIR))

import ot_db_setup as setup  # noqa: E402

COUNTS = {table: 10 * (i + 1) for i, table in enumerate(setup.OT_TABLES)}


def _sql(name: str) -> str:
    return (DB_DIR / "sql" / name).read_text()


def _tables(sql_text: str) -> dict:
    """{table: {column, ...}} parsed from CREATE TABLE statements."""
    tables = {}
    for name, body in re.findall(r"CREATE TABLE\s+([\w.]+)\s*\((.*?)\n\);", sql_text, re.S):
        cols = set()
        for line in body.splitlines():
            line = line.split("--")[0].strip()
            if line:
                cols.add(line.split()[0].strip('"'))
        tables[name.split(".")[-1]] = cols
    return tables


# ── schema contract: what each consumer queries must exist ──────────────────

# Columns the Prolog backend queries (pl/queries.pl on the wip branch) from the
# OpenTargets tables. Non-OT tables (gencode_gene, catlas_*, ...) and coloc are
# not part of this database yet.
PROLOG_COLUMNS = {
    "variant": {"variantid", "chromosome", "position", "referenceallele", "alternateallele", "rsids"},
    "credible_set": {"studylocusid", "studyid", "variantid", "locus"},
    "credible_set_variants": {"variantid", "studyid", "is95credibleset"},
    "study": {"studyid", "geneid", "studytype", "biosampleid", "biosamplefromsourceid"},
    "biosample": {"biosampleid", "biosamplename"},
    "enhancer": {"intervalid", "chromosome", "start", "end"},
}

# Columns src/db/credible_sets_handler.py queries.
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
def test_final_tables_have_every_column_consumers_query(consumer):
    tables = _tables(_sql("02_tables.sql"))
    for table, columns in consumer.items():
        assert table in tables, table
        assert not columns - tables[table], f"{table} missing {columns - tables[table]}"


def test_setup_counts_exactly_the_final_tables():
    assert set(_tables(_sql("02_tables.sql"))) == set(setup.OT_TABLES)


def test_raw_tables_cover_every_load_source():
    raw = _tables(_sql("01_raw.sql"))
    assert {t for t, _ in setup.RAW_SOURCES} == set(raw)


def test_locus_composite_matches_opentargets_struct():
    # pg_parquet needs the same field names and count as the parquet struct.
    fields = re.search(r"CREATE TYPE ot_raw.locus_item AS \((.*?)\);", _sql("01_raw.sql"), re.S).group(1)
    names = [line.split()[0].strip('"') for line in fields.splitlines() if line.strip()]
    assert names == [
        "is95CredibleSet", "is99CredibleSet", "logBF", "posteriorProbability", "variantId",
        "pValueMantissa", "pValueExponent", "beta", "standardError", "r2Overall",
    ]


def test_rsids_is_varchar_array_for_prolog_containment_query():
    assert re.search(r"^\s*rsids\s+VARCHAR\[\]", _sql("02_tables.sql"), re.M)


def test_gwas_study_index_only_counts_gwas():
    transform = _sql("03_transform.sql")
    gwas_insert = transform[transform.index("INSERT INTO gwas_study_index"):]
    assert "WHERE studytype = 'gwas'" in gwas_insert


def test_legacy_tables_are_not_reused():
    assert not set(setup.LEGACY_TABLES) & set(setup.OT_TABLES)


# ── freshness check ─────────────────────────────────────────────────────────

def _state(**overrides):
    fields = dict(ot_release="25.09", schema_version=setup.SCHEMA_VERSION, source="minio",
                  row_counts=dict(COUNTS))
    fields.update(overrides)
    return setup.LoadState(**fields)


def test_current_when_marker_and_counts_match():
    current, reason = setup.check_current(_state(), dict(COUNTS), "25.09")
    assert current, reason


def test_not_current_without_marker():
    assert not setup.check_current(None, dict(COUNTS), "25.09")[0]


def test_not_current_on_release_change():
    assert not setup.check_current(_state(ot_release="25.06"), dict(COUNTS), "25.09")[0]


def test_not_current_on_schema_version_change():
    assert not setup.check_current(_state(schema_version=setup.SCHEMA_VERSION - 1), dict(COUNTS), "25.09")[0]


def test_not_current_when_tables_missing():
    assert not setup.check_current(_state(), None, "25.09")[0]


@pytest.mark.parametrize("table", setup.OT_TABLES)
def test_not_current_on_partial_table(table):
    live = dict(COUNTS)
    live[table] -= 1
    current, reason = setup.check_current(_state(), live, "25.09")
    assert not current and table in reason


# ── release pinning / manifest / checksum ───────────────────────────────────

@pytest.mark.parametrize("release", ["", "latest", "LATEST", "../25.09", "25 09"])
def test_validate_release_rejects_unpinned_or_bad(release):
    with pytest.raises(setup.SetupError):
        setup.validate_release(release)


def test_snapshot_keys_are_release_and_schema_scoped():
    keys = setup.snapshot_keys("pg-releases/opentargets/", "25.09", 3)
    assert keys["manifest"] == "pg-releases/opentargets/25.09/schema-v3/manifest.json"


def _manifest(**overrides):
    m = {"format_version": setup.MANIFEST_FORMAT_VERSION, "ot_release": "25.09",
         "schema_version": setup.SCHEMA_VERSION, "row_counts": dict(COUNTS),
         "dump": {"key": "x/ot.dump", "sha256": "a" * 64, "size_bytes": 1}}
    m.update(overrides)
    return json.dumps(m)


def test_parse_manifest_ok():
    assert setup.parse_manifest(_manifest(), "25.09")["row_counts"] == COUNTS


@pytest.mark.parametrize("text", [
    "not json",
    _manifest(ot_release="25.06"),
    _manifest(schema_version=setup.SCHEMA_VERSION + 1),
    _manifest(format_version=99),
    _manifest(dump={"sha256": "nothex"}),
    _manifest(row_counts={"study": 1}),
])
def test_parse_manifest_rejects_mismatch(text):
    with pytest.raises(setup.SetupError):
        setup.parse_manifest(text, "25.09")


def test_fetch_dump_checksum_mismatch_leaves_no_file(tmp_path):
    class FakeStore:
        bucket = "b"

        def download(self, key, path):
            Path(path).write_bytes(b"corrupt")

    with pytest.raises(setup.SetupError, match="checksum mismatch"):
        setup._fetch_dump(FakeStore(), "k", tmp_path / "ot.dump", "0" * 64)
    assert list(tmp_path.iterdir()) == []


# ── ensure short-circuits ───────────────────────────────────────────────────

def test_skip_mode_exits_zero_without_touching_db(monkeypatch):
    monkeypatch.setattr(setup, "connect", lambda *a, **k: pytest.fail("must not connect"))
    assert setup.ensure(setup.Config(release="", mode="skip")) == 0


@pytest.mark.parametrize("cfg", [setup.Config(release="latest"), setup.Config(release="25.09", mode="bogus")])
def test_bad_config_fails_before_connecting(monkeypatch, cfg):
    monkeypatch.setattr(setup, "connect", lambda *a, **k: pytest.fail("must not connect"))
    assert setup.ensure(cfg) != 0
