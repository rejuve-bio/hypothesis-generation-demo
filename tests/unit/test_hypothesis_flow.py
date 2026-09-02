from copy import deepcopy
from unittest.mock import MagicMock

import pytest

from src.flows import hypothesis as flow_module


def _configure_flow(
    monkeypatch,
    immediate_task_factory,
    sample_enrichment,
    *,
    variant_result=None,
    phenotype_result=None,
    summary="A testable mechanism.",
):
    hypotheses = MagicMock()
    hypotheses.get_hypotheses.return_value = {"id": "hyp-1"}
    enrichr = MagicMock()
    enrichr.to_symbol.side_effect = lambda value: {
        "ENSG00000140968": "IRF8",
        "STAT1": "STAT1",
        "IRF1": "IRF1",
        "quoted_gene": "QUOTED_GENE",
    }.get(value, value)
    enrichr.to_ensembl_id.return_value = "ENSG00000140968"
    enrichr.is_ensembl_id.side_effect = lambda value: str(value).upper().startswith("ENSG")
    deps = {"hypotheses": hypotheses, "enrichr": enrichr}
    monkeypatch.setattr(flow_module.Config, "from_env", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(flow_module, "create_dependencies", lambda _config: deps)
    monkeypatch.setattr(flow_module, "check_hypothesis", immediate_task_factory(lambda *_: None))
    monkeypatch.setattr(flow_module, "get_enrich", immediate_task_factory(lambda *_: deepcopy(sample_enrichment)))
    monkeypatch.setattr(flow_module, "get_gene_ids", immediate_task_factory(lambda *_: ["ENSG-STAT1", "ENSG-IRF1"]))
    monkeypatch.setattr(flow_module, "execute_gene_query", immediate_task_factory(lambda *_: ["quoted_gene"]))
    if isinstance(variant_result, Exception):
        monkeypatch.setattr(flow_module, "execute_variant_query", immediate_task_factory(lambda *_: (_ for _ in ()).throw(variant_result)))
    else:
        monkeypatch.setattr(flow_module, "execute_variant_query", immediate_task_factory(lambda *_: ["variant_internal"] if variant_result is None else variant_result))
    monkeypatch.setattr(flow_module, "execute_phenotype_query", immediate_task_factory(lambda *_: ["EFO_0000729"] if phenotype_result is None else phenotype_result))
    if isinstance(summary, Exception):
        monkeypatch.setattr(flow_module, "summarize_graph", immediate_task_factory(lambda *_: (_ for _ in ()).throw(summary)))
    else:
        monkeypatch.setattr(flow_module, "summarize_graph", immediate_task_factory(lambda *_: summary))
    saved = []
    monkeypatch.setattr(flow_module, "create_hypothesis", immediate_task_factory(lambda *args: saved.append(args) or "hyp-1"))
    return deps, saved


def test_happy_path_assembles_graph_and_saves(
    monkeypatch, immediate_task_factory, sample_enrichment
):
    _, saved = _configure_flow(monkeypatch, immediate_task_factory, sample_enrichment)

    response, status = flow_module.hypothesis_flow.fn(
        "user-1", "hyp-1", "enrich-1", "GO:0006954"
    )

    assert status == 201
    assert response["summary"] == "A testable mechanism."
    graph = response["graph"]
    assert {node["id"] for node in graph["nodes"]} >= {
        "variant_internal", "GO:0006954", "EFO_0000729", "ENSG-STAT1", "ENSG-IRF1"
    }
    assert {edge["label"] for edge in graph["edges"]} >= {
        "involved_in", "enriched_in", "coexpressed_with"
    }
    assert saved[0][0:2] == ("enrich-1", "GO:0006954")
    assert saved[0][2] == "rs16940186"


@pytest.mark.parametrize("variant_result", [[], RuntimeError("variant lookup failed")])
def test_variant_lookup_falls_back_to_rsid(
    monkeypatch, immediate_task_factory, sample_enrichment, variant_result
):
    _, saved = _configure_flow(
        monkeypatch,
        immediate_task_factory,
        sample_enrichment,
        variant_result=variant_result,
    )

    response, status = flow_module.hypothesis_flow.fn(
        "user-1", "hyp-1", "enrich-1", "GO:0006954"
    )

    assert status == 201
    assert any(node["id"] == "rs16940186" for node in response["graph"]["nodes"])
    assert saved[0][2] == "rs16940186"
    warning_codes = {warning["code"] for warning in saved[0][9]}
    expected_code = (
        "variant_id_lookup_failed"
        if isinstance(variant_result, Exception)
        else "variant_id_fallback"
    )
    assert expected_code in warning_codes


def test_non_ensembl_gene_uses_enrichr_symbol_without_prolog_lookup(
    monkeypatch, immediate_task_factory, sample_enrichment
):
    sample_enrichment["causal_graph"]["graph"]["nodes"][0] = {
        "id": "IRF8", "type": "gene"
    }
    deps, _ = _configure_flow(monkeypatch, immediate_task_factory, sample_enrichment)

    response, _ = flow_module.hypothesis_flow.fn(
        "user-1", "hyp-1", "enrich-1", "GO:0006954"
    )

    gene = next(node for node in response["graph"]["nodes"] if node["id"] == "IRF8")
    assert gene["name"] == "IRF8"
    deps["enrichr"].to_symbol.assert_any_call("IRF8")


def test_empty_phenotype_lookup_uses_raw_phenotype(
    monkeypatch, immediate_task_factory, sample_enrichment
):
    _, _ = _configure_flow(
        monkeypatch, immediate_task_factory, sample_enrichment, phenotype_result=[]
    )
    response, status = flow_module.hypothesis_flow.fn(
        "user-1", "hyp-1", "enrich-1", "GO:0006954"
    )
    assert status == 201
    assert any(
        node == {"id": "Ulcerative colitis", "type": "phenotype", "name": "Ulcerative colitis"}
        for node in response["graph"]["nodes"]
    )
    assert saved_warning_codes(flow_module) == {"phenotype_id_fallback"}


def saved_warning_codes(module):
    return {warning["code"] for warning in module.create_hypothesis.calls[0][0][9]}


def test_existing_hypothesis_returns_early(
    monkeypatch, immediate_task_factory, sample_enrichment
):
    deps, saved = _configure_flow(monkeypatch, immediate_task_factory, sample_enrichment)
    monkeypatch.setattr(
        flow_module,
        "check_hypothesis",
        immediate_task_factory(lambda *_: {"summary": "existing", "graph": {"nodes": []}}),
    )
    assert flow_module.hypothesis_flow.fn(
        "user-1", "hyp-1", "enrich-1", "GO:0006954"
    ) == ({"summary": "existing", "graph": {"nodes": []}}, 200)
    assert saved == []
    deps["hypotheses"].get_hypotheses.assert_not_called()


def test_summarize_failure_propagates(
    monkeypatch, immediate_task_factory, sample_enrichment
):
    _configure_flow(
        monkeypatch,
        immediate_task_factory,
        sample_enrichment,
        summary=RuntimeError("LLM unavailable"),
    )
    with pytest.raises(RuntimeError, match="LLM unavailable"):
        flow_module.hypothesis_flow.fn(
            "user-1", "hyp-1", "enrich-1", "GO:0006954"
        )
