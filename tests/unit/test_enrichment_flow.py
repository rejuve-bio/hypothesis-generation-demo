from copy import deepcopy
from unittest.mock import MagicMock, call

import pytest

from src.catlas_census_mapping import CatlasMappingError
from src.flows import enrichment as flow_module
from src.services.enrich import EnrichrAPIUnavailableError
from src.services.prolog import PrologNoEvidenceError, PrologServiceError


def _configure_flow(monkeypatch, immediate_task_factory, graphs, *, enrich_table=None):
    hypotheses = MagicMock()
    gene_expression = MagicMock()
    gene_expression.get_tissue_selection.return_value = None
    gene_expression.get_ldsc_results_for_project.return_value = []
    enrichr = MagicMock()
    enrichr.to_symbol.side_effect = lambda value: {
        "ENSG00000140968": "IRF8",
        "ENSG2": "GENE2",
    }.get(value, value)
    enrichr.to_ensembl_id.side_effect = lambda value: {
        "IRF8": "ENSG00000140968",
        "GENE2": "ENSG2",
    }.get(value)
    enrichr.run.return_value = (
        [{"Term": "inflammatory response"}]
        if enrich_table is None
        else enrich_table
    )
    enrichr.annotate_graph_gene_names.side_effect = deepcopy
    llm = MagicMock()
    llm.get_relevant_go.return_value = [{"id": "GO:1", "name": "response", "genes": ["STAT1"]}]
    deps = {
        "tasks": MagicMock(),
        "redis_url": "redis://unused",
        "enrichr": enrichr,
        "llm": llm,
        "hypotheses": hypotheses,
        "gene_expression": gene_expression,
    }

    monkeypatch.setattr(flow_module.Config, "from_env", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(flow_module, "create_dependencies", lambda _config: deps)
    monkeypatch.setattr("src.services.status_tracker.StatusTracker", MagicMock())
    monkeypatch.setattr(flow_module, "emit_task_update", MagicMock())
    monkeypatch.setattr(flow_module, "check_enrich", immediate_task_factory(lambda *_: None))
    monkeypatch.setattr(flow_module, "get_candidate_genes", immediate_task_factory(lambda *_: ["ENSG00000140968"]))
    monkeypatch.setattr(flow_module, "get_relevant_gene_proof", immediate_task_factory(lambda *_: deepcopy(graphs)))
    monkeypatch.setattr(flow_module, "retry_get_relevant_gene_proof", immediate_task_factory(lambda *_: []))
    monkeypatch.setattr(flow_module, "get_coexpression_matrix_for_tissue", immediate_task_factory(lambda *_args, **_kwargs: "coexpression"))
    created = []

    def save(*args):
        created.append(args)
        return f"enrich-{len(created)}"

    monkeypatch.setattr(flow_module, "create_enrich_data", immediate_task_factory(save))
    return deps, created


def test_happy_path_runs_enrichr_filters_go_and_saves(
    monkeypatch, immediate_task_factory, sample_graph
):
    deps, created = _configure_flow(monkeypatch, immediate_task_factory, [sample_graph])

    result = flow_module.enrichment_flow.fn(
        "user-1", "Ulcerative colitis", "rs16940186", "hyp-1", "project-1", 3
    )

    assert result == ({"id": "enrich-1"}, 200)
    deps["enrichr"].run.assert_called_once_with("IRF8")
    deps["llm"].get_relevant_go.assert_called_once_with(
        "Ulcerative colitis", [{"Term": "inflammatory response"}]
    )
    assert created[0][:6] == (
        "user-1", "project-1", "rs16940186", "Ulcerative colitis", "IRF8",
        [{"id": "GO:1", "name": "response", "genes": ["STAT1"]}],
    )
    deps["hypotheses"].update_hypothesis.assert_has_calls(
        [
            call("hyp-1", {"causal_gene": "IRF8", "enrichment_stage": "enrichment_running"}),
            call(
                "hyp-1",
                {
                    "enrich_id": "enrich-1",
                    "child_enrich_ids": [],
                    "skipped_enrich_ids": [],
                    "status": "pending",
                    "enrichment_effective_mode": "non_tissue",
                    "non_tissue_specific_fallback": False,
                    "attempted_ldsc_cell_type": None,
                },
            ),
        ]
    )


def test_five_graphs_create_five_enrichments(
    monkeypatch, immediate_task_factory, sample_graph
):
    graphs = []
    for index in range(5):
        graph = deepcopy(sample_graph)
        graph["nodes"][0]["id"] = f"ENSG{index}"
        graph["edges"][0]["target"] = f"ENSG{index}"
        graph["prob"]["value"] = index / 10
        graphs.append(graph)
    deps, created = _configure_flow(monkeypatch, immediate_task_factory, graphs)
    deps["enrichr"].to_symbol.side_effect = lambda value: f"GENE-{value}"

    result = flow_module.enrichment_flow.fn(
        "user-1", "Trait", "rs16940186", "hyp-1", "project-1", 3
    )

    assert result == ({"id": "enrich-1"}, 200)
    assert len(created) == 5
    assert [entry[6]["graph_index"] for entry in created] == [4, 3, 2, 1, 0]


def test_graph_without_direct_causal_gene_is_skipped_while_valid_graph_proceeds(
    monkeypatch, immediate_task_factory, sample_graph
):
    invalid = deepcopy(sample_graph)
    invalid["edges"] = [{"source": "rs16940186", "target": "enhancer-1"}]
    invalid["nodes"].append({"id": "enhancer-1", "type": "enhancer"})
    invalid["prob"]["value"] = 0.9
    _, created = _configure_flow(
        monkeypatch, immediate_task_factory, [invalid, sample_graph]
    )

    result = flow_module.enrichment_flow.fn(
        "user-1", "Trait", "rs16940186", "hyp-1", "project-1", 3
    )

    assert result == ({"id": "enrich-2"}, 200)
    assert len(created) == 2
    skipped, successful = created
    assert skipped[4:6] == (None, [])
    assert skipped[8:] == (
        "skipped", "No direct SNP-gene edge found in causal graph."
    )
    assert successful[4] == "IRF8"
    final_patch = flow_module.create_dependencies(None)["hypotheses"].update_hypothesis.call_args.args[1]
    assert final_patch["child_enrich_ids"] == []
    assert final_patch["skipped_enrich_ids"] == ["enrich-1"]


def test_empty_enrichr_result_saves_graph_with_empty_go_terms(
    monkeypatch, immediate_task_factory, sample_graph
):
    deps, created = _configure_flow(
        monkeypatch, immediate_task_factory, [sample_graph], enrich_table=[]
    )
    result = flow_module.enrichment_flow.fn(
        "user-1", "Trait", "rs16940186", "hyp-1", "project-1", 3
    )
    assert result == ({"id": "enrich-1"}, 200)
    assert created[0][5] == []
    deps["llm"].get_relevant_go.assert_not_called()


def test_enrichr_failure_for_one_graph_does_not_abort_other_graphs(
    monkeypatch, immediate_task_factory, sample_graph
):
    second = deepcopy(sample_graph)
    second["nodes"][0]["id"] = "ENSG2"
    second["edges"][0]["target"] = "ENSG2"
    deps, created = _configure_flow(
        monkeypatch, immediate_task_factory, [sample_graph, second]
    )
    deps["enrichr"].run.side_effect = [
        EnrichrAPIUnavailableError("Enrichr unavailable"),
        [{"Term": "ok"}],
    ]

    result = flow_module.enrichment_flow.fn(
        "user-1", "Trait", "rs16940186", "hyp-1", "project-1", 3
    )

    assert result[1] == 200
    assert len(created) == 2
    assert created[0][8] == "skipped"
    assert "Enrichr API unavailable after retries" in created[0][9]
    final_patch = deps["hypotheses"].update_hypothesis.call_args.args[1]
    assert final_patch["child_enrich_ids"] == []
    assert final_patch["skipped_enrich_ids"] == ["enrich-1"]


def test_existing_enrichment_returns_without_other_work(
    monkeypatch, immediate_task_factory, sample_graph
):
    deps, created = _configure_flow(monkeypatch, immediate_task_factory, [sample_graph])
    existing = immediate_task_factory(lambda *_: {"id": "existing"})
    monkeypatch.setattr(flow_module, "check_enrich", existing)

    assert flow_module.enrichment_flow.fn(
        "user-1", "Trait", "rs16940186", "hyp-1", "project-1", 3
    ) == ({"id": "existing"}, 200)
    assert created == []
    deps["enrichr"].run.assert_not_called()


def test_tissue_selection_uses_coexpression_background(
    monkeypatch, immediate_task_factory, sample_graph
):
    deps, _ = _configure_flow(monkeypatch, immediate_task_factory, [sample_graph])
    deps["gene_expression"].get_tissue_selection.return_value = {"tissue_name": "Liver"}

    flow_module.enrichment_flow.fn(
        "user-1", "Trait", "rs16940186", "hyp-1", "project-1", 3
    )

    deps["enrichr"].run.assert_called_once_with(
        "IRF8", tissue_name="Liver", coexpression_data="coexpression"
    )


def test_tissue_empty_result_falls_back_to_standard_enrichment(
    monkeypatch, immediate_task_factory, sample_graph
):
    deps, created = _configure_flow(monkeypatch, immediate_task_factory, [sample_graph])
    deps["gene_expression"].get_tissue_selection.return_value = {"tissue_name": "Liver"}
    deps["enrichr"].run.side_effect = [[], [{"Term": "fallback"}]]

    flow_module.enrichment_flow.fn(
        "user-1", "Trait", "rs16940186", "hyp-1", "project-1", 3
    )

    assert deps["enrichr"].run.call_args_list == [
        call("IRF8", tissue_name="Liver", coexpression_data="coexpression"),
        call("IRF8"),
    ]
    assert created[0][6]["non_tissue_specific_fallback"] is True


def test_zero_graphs_after_retry_raises_no_evidence_error(
    monkeypatch, immediate_task_factory
):
    deps, created = _configure_flow(monkeypatch, immediate_task_factory, [])
    # _configure_flow already wires retry_get_relevant_gene_proof to return [].

    with pytest.raises(PrologNoEvidenceError, match="No causal-gene evidence"):
        flow_module.enrichment_flow.fn(
            "user-1", "Trait", "rs16940186", "hyp-1", "project-1", 3
        )

    assert created == []
    assert deps["hypotheses"].update_hypothesis.call_count == 1
    fail_call = deps["hypotheses"].update_hypothesis.call_args
    assert fail_call.args[0] == "hyp-1"
    assert fail_call.args[1]["status"] == "failed"
    assert "No causal-gene evidence" in fail_call.args[1]["error"]
    assert fail_call.args[1]["error_detail"] == {
        "error_type": "prolog_no_evidence",
        "message": fail_call.args[1]["error"],
        "variant": "rs16940186",
    }


def test_prolog_service_unavailable_is_distinguished_from_no_evidence(
    monkeypatch, immediate_task_factory
):
    """A Prolog outage (PrologServiceError) must not be reported the same
    way as a clean "no evidence found" result (PrologNoEvidenceError)."""
    deps, created = _configure_flow(monkeypatch, immediate_task_factory, [])
    monkeypatch.setattr(
        flow_module,
        "get_relevant_gene_proof",
        immediate_task_factory(
            lambda *_: (_ for _ in ()).throw(
                PrologServiceError("get_relevant_gene_proof failed. Prolog server is unreachable")
            )
        ),
    )

    with pytest.raises(PrologServiceError, match="unreachable"):
        flow_module.enrichment_flow.fn(
            "user-1", "Trait", "rs16940186", "hyp-1", "project-1", 3
        )

    assert created == []
    fail_call = deps["hypotheses"].update_hypothesis.call_args
    assert fail_call.args[1]["error_detail"]["error_type"] == "prolog_service_unavailable"


def test_all_graphs_skipped_raises_and_persists_skip_reason(
    monkeypatch, immediate_task_factory, sample_graph
):
    def _invalid(prob):
        graph = deepcopy(sample_graph)
        graph["edges"] = [{"source": "rs16940186", "target": "enhancer-1"}]
        graph["nodes"] = graph["nodes"] + [{"id": "enhancer-1", "type": "enhancer"}]
        graph["prob"]["value"] = prob
        return graph

    deps, created = _configure_flow(
        monkeypatch, immediate_task_factory, [_invalid(0.9), _invalid(0.1)]
    )

    with pytest.raises(PrologNoEvidenceError, match="all causal graphs were skipped"):
        flow_module.enrichment_flow.fn(
            "user-1", "Trait", "rs16940186", "hyp-1", "project-1", 3
        )

    assert len(created) == 2
    for entry in created:
        assert entry[8:] == (
            "skipped", "No direct SNP-gene edge found in causal graph."
        )
    deps["hypotheses"].update_hypothesis.assert_any_call(
        "hyp-1", {"skipped_enrich_ids": ["enrich-1", "enrich-2"]}
    )
    fail_call = deps["hypotheses"].update_hypothesis.call_args
    assert fail_call.args[1]["error_detail"]["error_type"] == "prolog_no_evidence"
    assert fail_call.args[1]["error_detail"]["variant"] == "rs16940186"


def test_all_graphs_enrichr_failed_raises_unavailable_error(
    monkeypatch, immediate_task_factory, sample_graph
):
    second = deepcopy(sample_graph)
    second["nodes"][0]["id"] = "ENSG2"
    second["edges"][0]["target"] = "ENSG2"
    deps, created = _configure_flow(
        monkeypatch, immediate_task_factory, [sample_graph, second]
    )
    deps["enrichr"].run.side_effect = EnrichrAPIUnavailableError("Enrichr down")

    with pytest.raises(EnrichrAPIUnavailableError, match="No enrichment could be completed"):
        flow_module.enrichment_flow.fn(
            "user-1", "Trait", "rs16940186", "hyp-1", "project-1", 3
        )

    assert len(created) == 2
    assert all(entry[8] == "skipped" for entry in created)
    deps["hypotheses"].update_hypothesis.assert_any_call(
        "hyp-1", {"skipped_enrich_ids": ["enrich-1", "enrich-2"]}
    )
    failed_calls = [
        c for c in deps["hypotheses"].update_hypothesis.call_args_list
        if c.args[1].get("status") == "failed"
    ]
    assert len(failed_calls) == 1
    assert failed_calls[0].args[1]["error_detail"] == {
        "error_type": "enrichr_service_unavailable",
        "message": failed_calls[0].args[1]["error"],
        "variant": "rs16940186",
    }


def test_shared_causal_gene_reuses_cached_enrichment(
    monkeypatch, immediate_task_factory, sample_graph
):
    second = deepcopy(sample_graph)
    second["prob"]["value"] = 0.1
    deps, created = _configure_flow(
        monkeypatch, immediate_task_factory, [sample_graph, second]
    )

    result = flow_module.enrichment_flow.fn(
        "user-1", "Trait", "rs16940186", "hyp-1", "project-1", 3
    )

    assert result == ({"id": "enrich-1"}, 200)
    deps["enrichr"].run.assert_called_once_with("IRF8")
    assert len(created) == 2
    assert created[0][5] == created[1][5]


def test_no_tissue_selection_falls_back_to_top_ldsc_tissue(
    monkeypatch, immediate_task_factory, sample_graph
):
    deps, _ = _configure_flow(monkeypatch, immediate_task_factory, [sample_graph])
    deps["gene_expression"].get_tissue_selection.return_value = None
    deps["gene_expression"].get_ldsc_results_for_project.return_value = [
        {"tissue_name": "Liver"}
    ]

    flow_module.enrichment_flow.fn(
        "user-1", "Trait", "rs16940186", "hyp-1", "project-1", 3
    )

    deps["gene_expression"].get_ldsc_results_for_project.assert_called_once_with(
        "user-1", "project-1", limit=1, format="selection"
    )
    deps["enrichr"].run.assert_called_once_with(
        "IRF8", tissue_name="Liver", coexpression_data="coexpression"
    )


def test_catlas_mapping_error_persists_structured_error_detail(
    monkeypatch, immediate_task_factory, sample_graph
):
    deps, _ = _configure_flow(monkeypatch, immediate_task_factory, [sample_graph])
    deps["gene_expression"].get_tissue_selection.return_value = {
        "tissue_name": "Weird_Tissue"
    }
    error = CatlasMappingError("Unknown LDSC cell type", ldsc_name="Weird_Tissue")
    deps["enrichr"].run.side_effect = error

    with pytest.raises(CatlasMappingError):
        flow_module.enrichment_flow.fn(
            "user-1", "Trait", "rs16940186", "hyp-1", "project-1", 3
        )

    fail_call = deps["hypotheses"].update_hypothesis.call_args
    assert fail_call.args[0] == "hyp-1"
    patch = fail_call.args[1]
    assert patch["status"] == "failed"
    assert patch["error_detail"] == {
        "error_type": "catlas_mapping",
        "message": "Unknown LDSC cell type",
        "ldsc_name": "Weird_Tissue",
    }
