import json
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


def _run_with_background(monkeypatch, query, tissue_genes):
    enrich = _enrich()
    enrich.ensembl_hgnc_map = {
        gid: f"SYM{gid}" for gid in set(list(query) + list(tissue_genes))
    }
    enrich.get_coexpression_net = MagicMock(return_value=(query, tissue_genes))
    enrich._process_enrichment_results = MagicMock(
        return_value=pd.DataFrame({"ID": ["GO:1"], "Adjusted P-value": [0.001]})
    )
    api = MagicMock()
    api.return_value.results = pd.DataFrame()
    monkeypatch.setattr(service.gp, "enrichr", api)
    enrich.run("GENE1", tissue_name="Liver", coexpression_data="matrix")
    return api.call_args.kwargs


def test_run_uses_tissue_gene_list_without_truncating_it(monkeypatch):
    """The tissue-filtered set (~15k) must survive intact."""
    tissue = [f"ENSG{i:05d}" for i in range(15000)]
    kwargs = _run_with_background(monkeypatch, ["ENSG1", "ENSG2"], tissue)

    assert len(kwargs["background"]) >= 15000
    assert kwargs["gene_list"] == ["SYMENSG1", "SYMENSG2"]
    assert kwargs["outdir"] is None


def test_run_always_includes_the_query_genes_in_the_background(monkeypatch):
    """Enrichr silently drops query genes missing from the background."""
    tissue = [f"ENSG{i:05d}" for i in range(100)]
    query = ["ENSGQUERY1", "ENSGQUERY2"]  # deliberately outside the tissue set
    kwargs = _run_with_background(monkeypatch, query, tissue)

    background = set(kwargs["background"])
    assert set(kwargs["gene_list"]) <= background, "query genes missing from background"


def test_run_caps_a_pathological_background(monkeypatch):
    """A genome-scale background makes Enrichr return all-NaN p-values."""
    monkeypatch.setenv("GO_MAX_BACKGROUND_SIZE", "1000")
    tissue = [f"ENSG{i:05d}" for i in range(60000)]
    kwargs = _run_with_background(monkeypatch, ["ENSG1"], tissue)

    # cap applies, plus the unioned query genes
    assert len(kwargs["background"]) <= 1000 + 2


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


def test_no_significant_go_terms_message_reports_the_configured_floor(monkeypatch):
    monkeypatch.setenv("GO_SIGNIFICANCE_MAX_P", "0.01")
    enrich = _enrich()
    api = MagicMock()
    api.return_value.results = pd.DataFrame(
        {
            "Gene_set": ["GO_Biological_Process_2023"],
            "Term": ["some process (GO:1)"],
            "Adjusted P-value": [0.03],
            "Genes": ["GENE1;GENE2"],
        }
    )
    monkeypatch.setattr(service.gp, "enrichr", api)

    with pytest.raises(service.NoEnrichmentFoundError) as exc:
        enrich.run(
            "GENE1", tissue_name="Liver",
            coexpression_data=([("ENSG1", 0.9), ("ENSG2", 0.8)], [], ["ENSG1", "ENSG2"]),
        )

    assert "adjusted p < 0.01" in str(exc.value)


def test_run_returns_only_the_significant_terms(monkeypatch):
    enrich = _enrich()
    enrich.get_coexpression_net = MagicMock(return_value=(["ENSG1"], ["ENSG1", "ENSG2"]))
    raw = pd.DataFrame(
        {
            "Gene_set": ["s", "s"],
            "Term": ["significant (GO:1)", "not significant (GO:2)"],
            "Adjusted P-value": [0.01, 0.5],
            "Genes": ["GENE1;GENE2", "GENE1"],
        }
    )
    monkeypatch.setattr(service.gp, "enrichr", MagicMock(return_value=MagicMock(results=raw)))

    filtered = enrich.run("GENE1", tissue_name="Liver")

    assert list(filtered["ID"]) == ["GO:1"]


def test_process_results_falls_back_to_NA_for_terms_missing_from_go_map():
    enrich = _enrich()
    raw = pd.DataFrame(
        {
            "Gene_set": ["s", "s"],
            "Term": ["known (GO:1)", "unknown (GO:2)"],
            "Adjusted P-value": [0.01, 0.5],
            "Genes": ["GENE1;GENE2", "GENE1"],
        }
    )

    out = enrich._process_enrichment_results(raw)

    # go_map only knows GO:1; GO:2 must fall back to "NA", not raise.
    assert dict(zip(out["ID"], out["Desc"])) == {"GO:1": "description", "GO:2": "NA"}


def test_run_raises_when_no_coexpressed_genes():
    """No co-expression is reported, not silently returned as empty tables."""
    enrich = _enrich()
    enrich.get_coexpression_net = MagicMock(return_value=([], []))
    original = service.gp.enrichr
    service.gp.enrichr = MagicMock()
    try:
        with pytest.raises(service.NoEnrichmentFoundError):
            enrich.run("GENE1", tissue_name="Liver")
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
    # full background first, then shrink; nothing below 2000, where the
    # Enrichr endpoint cannot succeed. Each rung carries the query genes,
    # hence the +1 over the plain slice sizes.
    assert [len(entry.kwargs["background"]) for entry in api.call_args_list] == [
        6001, 5001, 2001
    ]
    # a shrinking background must never drop the genes being tested
    for entry in api.call_args_list:
        assert "GENE1" in entry.kwargs["background"]


def test_enrichr_retry_raises_typed_error_after_exhaustion(monkeypatch):
    enrich = _enrich()
    api = MagicMock(side_effect=RuntimeError("offline"))
    monkeypatch.setattr(service.gp, "enrichr", api)

    with pytest.raises(
        service.EnrichrAPIUnavailableError,
        match=r"background sizes \[6000, 5000, 2000\]",
    ):
        enrich._run_enrichr_with_retry(
            gene_list=["GENE1"],
            gene_sets="GO_Biological_Process_2023",
            background=[f"GENE-{index}" for index in range(6000)],
            organism="human",
        )


@pytest.fixture(autouse=True)
def _reset_go_term_size_cache():
    """Keep the module-level size memo from leaking between tests."""
    service._GO_TERM_SIZES = None
    yield
    service._GO_TERM_SIZES = None


def test_go_term_sizes_parses_library_and_caches_to_disk(monkeypatch, tmp_path):
    cache = tmp_path / "sizes.json"
    monkeypatch.setenv("GO_TERM_SIZES_CACHE", str(cache))
    library = MagicMock(return_value={
        "Regulation Of DNA-templated Transcription (GO:0006355)": ["A", "B", "C"],
        "Regulation Of miRNA-mediated Gene Silencing (GO:0060964)": ["A"],
        "Term without an id": ["A", "B"],
    })
    monkeypatch.setattr(service.gp, "get_library", library)

    sizes = service.go_term_sizes()

    assert sizes == {"GO:0006355": 3, "GO:0060964": 1}
    cached = json.loads(cache.read_text())
    assert cached["sizes"] == {"GO:0006355": 3, "GO:0060964": 1}
    # distinct genes across the whole library, the IC denominator
    assert cached["corpus_genes"] == 3


def test_go_term_sizes_reads_cache_without_hitting_the_network(monkeypatch, tmp_path):
    cache = tmp_path / "sizes.json"
    cache.write_text(json.dumps({"GO:0006355": 1922}))
    monkeypatch.setenv("GO_TERM_SIZES_CACHE", str(cache))
    library = MagicMock(side_effect=AssertionError("network should not be used"))
    monkeypatch.setattr(service.gp, "get_library", library)

    assert service.go_term_sizes() == {"GO:0006355": 1922}
    library.assert_not_called()


def test_go_term_sizes_degrades_to_empty_when_library_unavailable(monkeypatch, tmp_path):
    monkeypatch.setenv("GO_TERM_SIZES_CACHE", str(tmp_path / "missing.json"))
    monkeypatch.setattr(
        service.gp, "get_library", MagicMock(side_effect=RuntimeError("offline"))
    )

    assert service.go_term_sizes() == {}


def test_process_results_falls_back_to_library_sizes_without_overlap(monkeypatch, tmp_path):
    """The background-corrected Enrichr endpoint omits Overlap entirely."""
    monkeypatch.setenv("GO_TERM_SIZES_CACHE", str(tmp_path / "sizes.json"))
    monkeypatch.setattr(service.gp, "get_library", MagicMock(return_value={
        "Regulation Of DNA-templated Transcription (GO:0006355)": ["G"] * 1922,
        "Regulation Of miRNA-mediated Gene Silencing (GO:0060964)": ["G"] * 15,
    }))
    enrich = _enrich()
    enrich.go_map = {
        "GO:0006355": {"desc": "broad"}, "GO:0060964": {"desc": "narrow"}
    }
    raw = pd.DataFrame([
        {
            "Gene_set": "GO_Biological_Process_2023",
            "Term": "Regulation Of DNA-templated Transcription (GO:0006355)",
            "Adjusted P-value": 1e-9,
            "Genes": "A;B",
        },
        {
            "Gene_set": "GO_Biological_Process_2023",
            "Term": "Regulation Of miRNA-mediated Gene Silencing (GO:0060964)",
            "Adjusted P-value": 1e-4,
            "Genes": "C",
        },
    ])

    out = enrich._process_enrichment_results(raw)

    assert out["Term Size"].tolist() == [1922, 15]


def test_shortlist_honours_the_shared_significance_floor(monkeypatch):
    monkeypatch.setenv("GO_SIGNIFICANCE_MAX_P", "0.01")
    enrich = _enrich()
    enrich.go_map = {"GO:1": {"desc": "a"}, "GO:2": {"desc": "b"}}
    enrich.get_coexpression_net = MagicMock(return_value=(["ENSG1"], ["ENSG1", "ENSG2"]))
    raw = pd.DataFrame([
        {"Gene_set": "GO", "Term": "kept (GO:1)", "Adjusted P-value": 0.005,
         "Genes": "A", "Overlap": "1/10"},
        {"Gene_set": "GO", "Term": "dropped (GO:2)", "Adjusted P-value": 0.03,
         "Genes": "B", "Overlap": "1/20"},
    ])
    monkeypatch.setattr(service.gp, "enrichr", MagicMock(return_value=MagicMock(results=raw)))

    filtered = enrich.run("GENE1", tissue_name="Liver")

    assert list(filtered["ID"]) == ["GO:1"]


# --- GO id parsing: names contain their own parentheses -------------------------


@pytest.mark.parametrize("label,expected_id,expected_name", [
    ("Regulation Of Transcription By RNA Polymerase II (GO:0006357)",
     "GO:0006357", "Regulation Of Transcription By RNA Polymerase II"),
    # "Poly(A)" used to yield id "A" and a name truncated at "Poly"
    ("Regulation Of Nuclear-Transcribed mRNA Poly(A) Tail Shortening (GO:0060211)",
     "GO:0060211", "Regulation Of Nuclear-Transcribed mRNA Poly(A) Tail Shortening"),
    # "(C-X-C Motif)" used to yield id "C-X-C Motif"
    ("Regulation Of Chemokine (C-X-C Motif) Ligand 2 Production (GO:2000342)",
     "GO:2000342", "Regulation Of Chemokine (C-X-C Motif) Ligand 2 Production"),
    ("Maturation Of SSU-rRNA (SSU-rRNA, 5.8S rRNA, LSU-rRNA) (GO:0000462)",
     "GO:0000462", "Maturation Of SSU-rRNA (SSU-rRNA, 5.8S rRNA, LSU-rRNA)"),
])
def test_split_term_and_go_id_anchors_to_the_trailing_id(label, expected_id, expected_name):
    name, go_id = service.split_term_and_go_id(label)
    assert go_id == expected_id
    assert name == expected_name


def test_split_term_and_go_id_degrades_on_an_unexpected_label():
    name, go_id = service.split_term_and_go_id("no identifier here")
    assert name == "no identifier here"
    assert go_id == "no identifier here"


def test_process_results_never_emits_a_malformed_go_id(monkeypatch):
    monkeypatch.setenv("GO_TERM_SIZES_CACHE", "/nonexistent/sizes.json")
    monkeypatch.setattr(service.gp, "get_library", MagicMock(return_value={}))
    enrich = _enrich()
    enrich.go_map = {}
    raw = pd.DataFrame([
        {"Gene_set": "GO", "Adjusted P-value": 0.001, "Genes": "A", "Overlap": "1/9",
         "Term": "Regulation Of Nuclear-Transcribed mRNA Poly(A) Tail Shortening (GO:0060211)"},
        {"Gene_set": "GO", "Adjusted P-value": 0.002, "Genes": "B", "Overlap": "1/9",
         "Term": "Regulation Of Chemokine (C-X-C Motif) Ligand 2 Production (GO:2000342)"},
    ])

    out = enrich._process_enrichment_results(raw)

    assert list(out["ID"]) == ["GO:0060211", "GO:2000342"]


def test_go_term_sizes_retries_after_a_failed_load(monkeypatch):
    monkeypatch.setattr(service, "_GO_TERM_SIZES", None)
    loader = MagicMock(
        side_effect=[
            {"sizes": {}, "corpus_genes": 0},
            {"sizes": {"GO:1": 42}, "corpus_genes": 100},
        ]
    )
    monkeypatch.setattr(service, "_load_go_term_sizes", loader)

    assert service.go_term_sizes() == {}
    assert service.go_term_sizes() == {"GO:1": 42}
    assert loader.call_count == 2
