from unittest.mock import MagicMock, call

import pytest

from src.services.status_tracker import TaskState
from src.tasks import hypothesis as tasks


def test_extract_probability_prefers_hypothesis_graph():
    enrichment = MagicMock()
    result = tasks.extract_probability(
        {"graph": {"probability": 0.8}, "enrich_id": "enrich-1"},
        enrichment,
        "user-1",
    )
    assert result == 0.8
    enrichment.get_enrich.assert_not_called()


def test_extract_probability_falls_back_to_enrichment(sample_enrichment):
    enrichment = MagicMock()
    enrichment.get_enrich.return_value = sample_enrichment
    assert tasks.extract_probability(
        {"id": "hyp-1", "enrich_id": "enrich-1"}, enrichment, "user-1"
    ) == 0.3


@pytest.mark.parametrize(
    ("task", "dependency", "method", "args", "expected"),
    [
        (tasks.get_enrich, "enrichment", "get_enrich", ("user-1", "enrich-1", "hyp-1"), {"id": "enrich-1"}),
        (tasks.get_gene_ids, "prolog_query", "get_gene_ids", (["stat1"], "hyp-1"), ["ENSG1"]),
        (tasks.execute_gene_query, "prolog_query", "execute_query", ("gene-query", "hyp-1"), ["STAT1"]),
        (tasks.execute_variant_query, "prolog_query", "execute_query", ("variant-query", "hyp-1"), ["var_1"]),
    ],
)
def test_query_tasks_emit_started_and_completed(
    monkeypatch, task, dependency, method, args, expected
):
    service = MagicMock()
    getattr(service, method).return_value = expected
    updates = MagicMock()
    monkeypatch.setattr(tasks, "get_deps", lambda: {dependency: service})
    monkeypatch.setattr(tasks, "emit_task_update", updates)

    assert task.fn(*args) == expected
    getattr(service, method).assert_called_once_with(*args[:-1])
    assert updates.call_args_list[0].kwargs["state"] == TaskState.STARTED
    assert updates.call_args_list[-1].kwargs["state"] == TaskState.COMPLETED


def test_execute_phenotype_query_constructs_term_name_query(monkeypatch):
    prolog = MagicMock()
    prolog.execute_query.return_value = ["EFO_0000729"]
    updates = MagicMock()
    monkeypatch.setattr(tasks, "get_deps", lambda: {"prolog_query": prolog})
    monkeypatch.setattr(tasks, "emit_task_update", updates)

    assert tasks.execute_phenotype_query.fn("Ulcerative colitis", "hyp-1") == [
        "EFO_0000729"
    ]
    prolog.execute_query.assert_called_once_with(
        "term_name(efo(X), 'Ulcerative colitis')"
    )


def test_summarize_graph_failure_emits_failed_and_reraises(monkeypatch):
    llm = MagicMock()
    llm.summarize_graph.side_effect = RuntimeError("LLM unavailable")
    updates = MagicMock()
    monkeypatch.setattr(tasks, "get_deps", lambda: {"llm": llm})
    monkeypatch.setattr(tasks, "emit_task_update", updates)

    with pytest.raises(RuntimeError, match="LLM unavailable"):
        tasks.summarize_graph.fn({"nodes": [], "edges": []}, "hyp-1")

    assert updates.call_args_list[-1] == call(
        hypothesis_id="hyp-1",
        task_name="Generating graph summary",
        state=TaskState.FAILED,
        error="LLM unavailable",
    )
