"""Tests for the OpenTargets Postgres bootstrap (src/services/ot_db_bootstrap.py)
and its EBI loader helpers — the pure decision logic, no database needed."""
import json

import pytest

from src.db import credible_sets_schema as cs_schema
from src.services import ot_db_bootstrap as boot
from src.services.ot_ebi_loader import _copy_field, pg_text_array

COUNTS = {table: 10 * (i + 1) for i, table in enumerate(cs_schema.OT_TABLES)}


def _state(**overrides):
    fields = dict(
        ot_release="25.09",
        schema_version=cs_schema.SCHEMA_VERSION,
        source="minio",
        row_counts=dict(COUNTS),
    )
    fields.update(overrides)
    return boot.LoadState(**fields)


# ── freshness check ─────────────────────────────────────────────────────────

def test_current_when_marker_and_counts_match():
    current, reason = boot.check_current(_state(), dict(COUNTS), "25.09")
    assert current, reason


def test_not_current_without_marker():
    current, reason = boot.check_current(None, dict(COUNTS), "25.09")
    assert not current
    assert "no ot_load_state marker" in reason


def test_not_current_on_release_change():
    current, reason = boot.check_current(_state(ot_release="25.06"), dict(COUNTS), "25.09")
    assert not current
    assert "25.06" in reason


def test_not_current_on_schema_version_change():
    current, reason = boot.check_current(
        _state(schema_version=cs_schema.SCHEMA_VERSION - 1), dict(COUNTS), "25.09"
    )
    assert not current
    assert "schema" in reason


def test_not_current_when_tables_missing():
    current, _ = boot.check_current(_state(), None, "25.09")
    assert not current


@pytest.mark.parametrize("table", cs_schema.OT_TABLES)
def test_not_current_on_partial_or_tampered_table(table):
    live = dict(COUNTS)
    live[table] -= 1
    current, reason = boot.check_current(_state(), live, "25.09")
    assert not current
    assert table in reason


def test_not_current_when_marker_lacks_a_table_count():
    counts = dict(COUNTS)
    del counts[cs_schema.OT_TABLES[-1]]
    current, _ = boot.check_current(_state(row_counts=counts), dict(COUNTS), "25.09")
    assert not current


# ── release pinning ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("release", ["", "latest", "LATEST", "../25.09", "25 09"])
def test_validate_release_rejects_unpinned_or_bad(release):
    with pytest.raises(boot.BootstrapError):
        boot.validate_release(release)


@pytest.mark.parametrize("release", ["25.09", "24.12", "25.09.1"])
def test_validate_release_accepts_pinned(release):
    assert boot.validate_release(release) == release


def test_snapshot_keys_are_release_and_schema_scoped():
    keys = boot.snapshot_keys("pg-releases/opentargets/", "25.09", 3)
    assert keys["manifest"] == "pg-releases/opentargets/25.09/schema-v3/manifest.json"
    assert keys["dump"] == "pg-releases/opentargets/25.09/schema-v3/ot.dump"


# ── manifest ────────────────────────────────────────────────────────────────

def _manifest(**overrides):
    m = {
        "format_version": boot.MANIFEST_FORMAT_VERSION,
        "ot_release": "25.09",
        "schema_version": cs_schema.SCHEMA_VERSION,
        "row_counts": dict(COUNTS),
        "dump": {"key": "x/ot.dump", "sha256": "a" * 64, "size_bytes": 1},
    }
    m.update(overrides)
    return json.dumps(m)


def test_parse_manifest_ok():
    assert boot.parse_manifest(_manifest(), "25.09")["row_counts"] == COUNTS


@pytest.mark.parametrize(
    "text",
    [
        "not json",
        _manifest(ot_release="25.06"),
        _manifest(schema_version=cs_schema.SCHEMA_VERSION + 1),
        _manifest(format_version=99),
        _manifest(dump={"sha256": "nothex"}),
        _manifest(row_counts={"credible_sets": 1}),
    ],
)
def test_parse_manifest_rejects_mismatch(text):
    with pytest.raises(boot.BootstrapError):
        boot.parse_manifest(text, "25.09")


def test_fetch_dump_checksum_mismatch_leaves_no_file(tmp_path):
    class FakeStorage:
        bucket = "b"

        def download_file(self, key, path):
            with open(path, "wb") as f:
                f.write(b"corrupt")
            return True

    dest = tmp_path / "ot.dump"
    with pytest.raises(boot.BootstrapError, match="checksum mismatch"):
        boot._fetch_dump(FakeStorage(), "k", dest, "0" * 64)
    assert list(tmp_path.iterdir()) == []


# ── orchestration short-circuits ────────────────────────────────────────────

def test_skip_mode_exits_zero_without_touching_db(monkeypatch):
    monkeypatch.setattr(boot, "connect", lambda *a, **k: pytest.fail("must not connect"))
    assert boot.run_bootstrap(boot.BootstrapConfig(release="", mode="skip")) == 0


def test_latest_release_fails_before_connecting(monkeypatch):
    monkeypatch.setattr(boot, "connect", lambda *a, **k: pytest.fail("must not connect"))
    assert boot.run_bootstrap(boot.BootstrapConfig(release="latest")) != 0


def test_unknown_mode_fails(monkeypatch):
    monkeypatch.setattr(boot, "connect", lambda *a, **k: pytest.fail("must not connect"))
    assert boot.run_bootstrap(boot.BootstrapConfig(release="25.09", mode="bogus")) != 0


# ── COPY serialization ──────────────────────────────────────────────────────

def test_pg_text_array_escapes():
    assert pg_text_array(["a", 'b"c', "d\\e", None]) == '{"a","b\\"c","d\\\\e",NULL}'
    assert pg_text_array([]) == "{}"


def test_copy_field_null_and_bool():
    assert _copy_field(None) == "\\N"
    assert _copy_field(True) == "t"
    assert _copy_field(False) == "f"
    assert _copy_field("") == ""
    assert _copy_field(1.5) == "1.5"
