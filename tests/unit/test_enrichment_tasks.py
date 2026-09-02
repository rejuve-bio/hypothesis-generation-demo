import json
from unittest.mock import MagicMock, call

import pytest

from src.services.status_tracker import TaskState
from src.tasks import enrichment as tasks


def test_parse_prolog_graphs_accepts_json_and_dict_and_skips_bad_json(
    sample_graph, monkeypatch
):
    log_error = MagicMock()
    monkeypatch.setattr(tasks.logger, "error", log_error)

    result = tasks.parse_prolog_graphs(
        {"response": [json.dumps(sample_graph), sample_graph, "{not-json"]}
    )

    assert result == [sample_graph, sample_graph]
    assert log_error.call_count == 2
    assert "Failed to parse graph 2" in log_error.call_args_list[0].args[0]


@pytest.mark.parametrize(
    ("graph", "expected"),
    [
        ({"nodes": [{"id": "rs1", "type": "snp"}], "edges": []}, (None, None)),
        (
            {
                "nodes": [
                    {"id": "rs1", "type": "snp"},
                    {"id": "ENSG1", "type": "gene", "name": "GENE1"},
                ],
                "edges": [],
            },
            (None, None),
        ),
        (
            {
                "nodes": [
                    {"id": "rs1", "type": "snp"},
                    {"id": "enh1", "type": "enhancer"},
                    {"id": "ENSG1", "type": "gene", "name": "GENE1"},
                ],
                "edges": [
                    {"source": "rs1", "target": "enh1"},
                    {"source": "enh1", "target": "ENSG1"},
                ],
            },
            (None, None),
        ),
    ],
)
def test_extract_causal_gene_returns_none_without_direct_snp_gene_edge(graph, expected):
    variants = [node for node in graph["nodes"] if node["type"] == "snp"]
    assert tasks.extract_causal_gene_from_graph(graph, variants) == expected


def test_extract_causal_gene_uses_first_directly_connected_gene(sample_graph):
    sample_graph["nodes"][0]["name"] = "IRF8"
    sample_graph["nodes"].insert(1, {"id": "ENSG2", "type": "gene", "name": "OTHER"})
    sample_graph["edges"].append({"source": "rs16940186", "target": "ENSG2"})

    assert tasks.extract_causal_gene_from_graph(
        sample_graph, [sample_graph["nodes"][2]]
    ) == ("ENSG00000140968", "IRF8")


def test_get_candidate_genes_emits_started_and_completed(monkeypatch, mock_prolog):
    updates = MagicMock()
    monkeypatch.setattr(tasks, "get_deps", lambda: {"prolog_query": mock_prolog})
    monkeypatch.setattr(tasks, "emit_task_update", updates)

    result = tasks.get_candidate_genes.fn("rs16940186", "hyp-1")

    assert result == ["ENSG00000140968"]
    mock_prolog.get_candidate_genes.assert_called_once_with("rs16940186")
    assert updates.call_args_list == [
        call(
            hypothesis_id="hyp-1",
            task_name="Getting candidate genes",
            state=TaskState.STARTED,
            next_task="Predicting causal gene",
        ),
        call(
            hypothesis_id="hyp-1",
            task_name="Getting candidate genes",
            state=TaskState.COMPLETED,
            details={"genes_count": 1},
        ),
    ]


def test_get_candidate_genes_emits_failed_and_reraises(monkeypatch, mock_prolog):
    mock_prolog.get_candidate_genes.side_effect = RuntimeError("prolog down")
    updates = MagicMock()
    monkeypatch.setattr(tasks, "get_deps", lambda: {"prolog_query": mock_prolog})
    monkeypatch.setattr(tasks, "emit_task_update", updates)

    with pytest.raises(RuntimeError, match="prolog down"):
        tasks.get_candidate_genes.fn("rs16940186", "hyp-1")

    assert updates.call_args_list[-1] == call(
        hypothesis_id="hyp-1",
        task_name="Getting candidate genes",
        state=TaskState.FAILED,
        error="prolog down",
    )


def test_get_relevant_gene_proof_parses_graphs_and_emits_updates(
    monkeypatch, mock_prolog, sample_graph
):
    mock_prolog.get_relevant_gene_proof.return_value = {
        "response": [json.dumps(sample_graph)]
    }
    updates = MagicMock()
    monkeypatch.setattr(tasks, "get_deps", lambda: {"prolog_query": mock_prolog})
    monkeypatch.setattr(tasks, "emit_task_update", updates)

    result = tasks.get_relevant_gene_proof.fn("rs16940186", "hyp-1", 7)

    assert result == [sample_graph]
    mock_prolog.get_relevant_gene_proof.assert_called_once_with(
        "rs16940186", 7, samples=10
    )
    assert [entry.kwargs["state"] for entry in updates.call_args_list] == [
        TaskState.STARTED,
        TaskState.COMPLETED,
    ]
    assert updates.call_args_list[-1].kwargs["details"] == {
        "relevant_gene_proof": [sample_graph],
        "num_graphs": 1,
    }


def test_get_relevant_gene_proof_emits_failed_and_reraises(monkeypatch, mock_prolog):
    mock_prolog.get_relevant_gene_proof.side_effect = TimeoutError("timeout")
    updates = MagicMock()
    monkeypatch.setattr(tasks, "get_deps", lambda: {"prolog_query": mock_prolog})
    monkeypatch.setattr(tasks, "emit_task_update", updates)

    with pytest.raises(TimeoutError, match="timeout"):
        tasks.get_relevant_gene_proof.fn("rs16940186", "hyp-1", 7)

    assert updates.call_args_list[-1].kwargs == {
        "hypothesis_id": "hyp-1",
        "task_name": "Getting relevant gene proof",
        "state": TaskState.FAILED,
        "next_task": "Retrying to predict causal gene",
        "error": "timeout",
    }
