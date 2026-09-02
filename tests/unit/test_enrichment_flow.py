from copy import deepcopy
from unittest.mock import MagicMock, call

from src.flows import enrichment as flow_module
from src.services.enrich import EnrichrAPIUnavailableError


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
