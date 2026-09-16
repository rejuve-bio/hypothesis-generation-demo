from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import sys
from unittest.mock import MagicMock

import pytest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))


SAMPLE_GRAPH = {
    "nodes": [
        {"id": "ENSG00000140968", "type": "gene"},
        {"id": "rs16940186", "type": "snp"},
    ],
    "edges": [
        {
            "source": "rs16940186",
            "target": "ENSG00000140968",
            "label": "pqtl_association",
        }
    ],
    "prob": {"value": 0.3},
}


class ImmediateResult:
    def __init__(self, value=None, error: Exception | None = None):
        self._value = value
        self._error = error

    def result(self):
        if self._error is not None:
            raise self._error
        return self._value


class ImmediateTask:
    """Small Prefect task stand-in that evaluates work during ``submit``."""

    def __init__(self, function):
        self.function = function
        self.calls: list[tuple[tuple, dict]] = []

    def submit(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        try:
            return ImmediateResult(self.function(*args, **kwargs))
        except Exception as exc:  # preserve Future.result() failure semantics
            return ImmediateResult(error=exc)


@pytest.fixture
def immediate_task_factory():
    return ImmediateTask


@pytest.fixture
def sample_graph():
    return deepcopy(SAMPLE_GRAPH)


@pytest.fixture
def sample_enrichment(sample_graph):
    return {
        "id": "enrich-1",
        "user_id": "user-1",
        "project_id": "project-1",
        "variant": "rs16940186",
        "phenotype": "Ulcerative colitis",
        "causal_gene": "ENSG00000140968",
        "GO_terms": [
            {
                "id": "GO:0006954",
                "name": "inflammatory response",
                "genes": ["STAT1", "IRF1"],
            }
        ],
        "causal_graph": {
            "graph": sample_graph,
            "graph_index": 0,
            "total_graphs": 1,
        },
    }


@pytest.fixture
def sample_hypothesis():
    return {
        "id": "hypothesis-1",
        "user_id": "user-1",
        "project_id": "project-1",
        "enrich_id": "enrich-1",
        "variant": "rs16940186",
        "phenotype": "Ulcerative colitis",
        "status": "pending",
    }


@pytest.fixture
def mock_prolog(sample_graph):
    prolog = MagicMock()
    prolog.get_candidate_genes.return_value = ["ENSG00000140968"]
    prolog.get_relevant_gene_proof.return_value = {
        "response": [deepcopy(sample_graph)]
    }
    prolog.get_gene_ids.return_value = ["ensg00000115415", "ensg00000125347"]
    return prolog


@pytest.fixture
def mock_llm():
    llm = MagicMock()
    llm.get_relevant_go.return_value = [
        {
            "id": "GO:0006954",
            "name": "inflammatory response",
            "genes": ["STAT1", "IRF1"],
        }
    ]
    llm.summarize_graph.return_value = "A testable mechanistic hypothesis."
    return llm


@pytest.fixture
def mock_enrichr():
    enrichr = MagicMock()
    enrichr.to_symbol.side_effect = lambda value: {
        "ENSG00000140968": "IRF8",
        "ensg00000140968": "IRF8",
    }.get(value, str(value).strip("'\"").upper())
    enrichr.to_ensembl_id.side_effect = lambda value: {
        "IRF8": "ensg00000140968",
        "STAT1": "ensg00000115415",
        "IRF1": "ensg00000125347",
    }.get(str(value).upper())
    enrichr.is_ensembl_id.side_effect = lambda value: str(value).upper().startswith(
        "ENSG"
    )
    enrichr.annotate_graph_gene_names.side_effect = deepcopy
    return enrichr
