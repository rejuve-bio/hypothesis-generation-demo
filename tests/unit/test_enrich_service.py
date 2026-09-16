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
    enrich._process_enrichment_results = MagicMock(return_value="processed")
    api = MagicMock()
    api.return_value.results = pd.DataFrame()
    monkeypatch.setattr(service.gp, "enrichr", api)

    result = enrich.run("GENE1", tissue_name="Liver", coexpression_data="matrix")

    assert result == "processed"
    assert len(api.call_args.kwargs["background"]) == 5000
    assert api.call_args.kwargs["gene_list"] == ["GENE1", "GENE2"]
    assert api.call_args.kwargs["outdir"] is None


def test_run_returns_empty_frame_without_calling_api():
    enrich = _enrich()
    enrich.get_coexpression_net = MagicMock(return_value=[])
    enrich._load_fallback_background_data = MagicMock(return_value=["BG"])
    original = service.gp.enrichr
    service.gp.enrichr = MagicMock()
    try:
        result = enrich.run("GENE1")
        assert list(result.columns) == ["ID", "Term", "Desc", "Adjusted P-value", "Genes"]
        service.gp.enrichr.assert_not_called()
    finally:
        service.gp.enrichr = original


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
