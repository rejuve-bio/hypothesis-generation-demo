"""The Python handler reads the shared OpenTargets tables (set up by db/,
which holds every study type for the Prolog backend) but only ever returns GWAS.
Schema contract tests live in db/tests/test_ot_db_setup.py."""
from unittest.mock import MagicMock

import pytest

from src.db import credible_sets_handler as handler_mod


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
