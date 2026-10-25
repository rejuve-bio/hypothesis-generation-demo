from copy import deepcopy
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.services import enrich as service


def _enrich():
    instance = object.__new__(service.Enrich)
    instance.ensembl_hgnc_map = {"ENSG1": "GENE1", "ENSG2": "GENE2"}
    instance.hgnc_ensembl_map = {"GENE1": "ENSG1"}
    instance.go_map = {"GO:1": {"desc": "description"}}
    instance.config = MagicMock(data_dir="/unused")
    return instance


def test_gene_identifier_normalization_and_mapping():
    enrich = _enrich()
    assert enrich.is_ensembl_id(" ENSG123 ") is True
    assert enrich.to_symbol("'ENSG1'") == "GENE1"
    assert enrich.to_symbol("gene1") == "GENE1"
    assert enrich.to_ensembl_id('"GENE1"') == "ensg1"
    assert enrich.to_ensembl_id("unknown") is None


def test_annotate_graph_gene_names_returns_copy(sample_graph):
    enrich = _enrich()
    original = deepcopy(sample_graph)
    annotated = enrich.annotate_graph_gene_names(sample_graph)
    assert annotated["nodes"][0]["name"] == "ENSG00000140968"
    assert sample_graph == original


def test_run_uses_tissue_gene_list_and_caps_background(monkeypatch):
    enrich = _enrich()
    enrich.get_coexpression_net = MagicMock(
        return_value=(["ENSG1", "ENSG2"], ["ENSG1"] * 6000)
    )
    processed = pd.DataFrame({"ID": ["GO:1"]})
    enrich._process_enrichment_results = MagicMock(return_value=processed)
    api = MagicMock()
    api.return_value.results = pd.DataFrame()
    monkeypatch.setattr(service.gp, "enrichr", api)

    result = enrich.run("GENE1", tissue_name="Liver", coexpression_data="matrix")

    assert result is processed
    assert len(api.call_args.kwargs["background"]) == 5000
    assert api.call_args.kwargs["gene_list"] == ["GENE1", "GENE2"]
    assert api.call_args.kwargs["outdir"] is None


@pytest.mark.parametrize(
    "coexpression, reason, message",
    [
        (([], [], []), "tissue_data_unavailable",
         "No CellxGene expression data could be loaded for 'Liver'"),
        (([], [], ["ENSG2"]), "gene_not_in_tissue_data",
         "GENE1 is not measured in the CellxGene expression data for 'Liver'"),
        (([], [], ["ENSG1", "ENSG2"]), "no_coexpressed_genes",
         "No genes are significantly co-expressed with GENE1 in 'Liver'"),
        (([("ENSG_UNMAPPED", 0.9)], [], ["ENSG1", "ENSG_UNMAPPED"]),
         "coexpressed_genes_unmapped",
         "none of them could be mapped to an HGNC gene symbol"),
    ],
)
def test_empty_coexpression_raises_with_the_specific_reason(
    monkeypatch, coexpression, reason, message
):
    enrich = _enrich()
    api = MagicMock()
    monkeypatch.setattr(service.gp, "enrichr", api)

    with pytest.raises(service.NoEnrichmentFoundError) as exc:
        enrich.run("GENE1", tissue_name="Liver", coexpression_data=coexpression)

    assert exc.value.reason == reason
    assert message in str(exc.value)
    api.assert_not_called()


def test_no_significant_go_terms_raises_with_gene_count(monkeypatch):
    enrich = _enrich()
    api = MagicMock()
    api.return_value.results = pd.DataFrame(
        {
            "Gene_set": ["GO_Biological_Process_2023"],
            "Term": ["some process (GO:1)"],
            "Adjusted P-value": [0.4],
            "Genes": ["GENE1;GENE2"],
        }
    )
    monkeypatch.setattr(service.gp, "enrichr", api)

    with pytest.raises(service.NoEnrichmentFoundError) as exc:
        enrich.run(
            "GENE1", tissue_name="Liver",
            coexpression_data=([("ENSG1", 0.9), ("ENSG2", 0.8)], [], ["ENSG1", "ENSG2"]),
        )

    assert exc.value.reason == "no_significant_go_terms"
    assert "2 genes are co-expressed with GENE1 in 'Liver'" in str(exc.value)
    assert "'some process' (adjusted p = 0.4)" in str(exc.value)
    assert exc.value.as_detail()["min_adjusted_p"] == 0.4


def test_p_value_threshold_controls_which_go_terms_pass():
    enrich = _enrich()
    results = pd.DataFrame(
        {
            "Gene_set": ["GO_Biological_Process_2023"],
            "Term": ["some process (GO:1)"],
            "Adjusted P-value": [0.08],
            "Genes": ["GENE1;GENE2"],
        }
    )

    assert enrich._process_enrichment_results(results).empty
    assert enrich._process_enrichment_results(results, 0.1)["ID"].tolist() == ["GO:1"]


def test_retry_api_contract_is_available():
    assert hasattr(service, "EnrichrAPIUnavailableError")
    assert hasattr(service.Enrich, "_run_enrichr_with_retry")


def test_enrichr_retry_shrinks_background_until_success(monkeypatch):
    enrich = _enrich()
    api = MagicMock()
    success = MagicMock(results="results")
    api.side_effect = [RuntimeError("first"), RuntimeError("second"), success]
    monkeypatch.setattr(service.gp, "enrichr", api)

    result = enrich._run_enrichr_with_retry(
        gene_list=["GENE1"],
        gene_sets="GO_Biological_Process_2023",
        background=[f"GENE-{index}" for index in range(6000)],
        organism="human",
        outdir=None,
    )

    assert result == "results"
    assert [len(entry.kwargs["background"]) for entry in api.call_args_list] == [
        5000, 2500, 1000
    ]


def test_enrichr_retry_raises_typed_error_after_exhaustion(monkeypatch):
    enrich = _enrich()
    api = MagicMock(side_effect=RuntimeError("offline"))
    monkeypatch.setattr(service.gp, "enrichr", api)

    with pytest.raises(
        service.EnrichrAPIUnavailableError,
        match=r"background sizes \[5000, 2500, 1000\]",
    ):
        enrich._run_enrichr_with_retry(
            gene_list=["GENE1"],
            gene_sets="GO_Biological_Process_2023",
            background=[f"GENE-{index}" for index in range(6000)],
            organism="human",
        )
