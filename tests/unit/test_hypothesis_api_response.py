from src.api.routes.hypothesis import _response_from_hypothesis_document
from src.api.schemas.hypothesis import HypothesisGraphResponse


def test_hypothesis_response_exposes_persisted_warnings():
    warning = {
        "code": "variant_id_fallback",
        "message": "The original rsID was retained.",
        "variant": "rs16940186",
    }
    response = _response_from_hypothesis_document(
        "hyp-1",
        {"summary": "summary", "graph": {"nodes": []}, "warnings": [warning]},
        enrich_id="enrich-1",
        project_id="project-1",
    )

    validated = HypothesisGraphResponse.model_validate(response)
    assert validated.warnings == [warning]


def test_hypothesis_response_defaults_warnings_to_empty_list():
    response = _response_from_hypothesis_document("hyp-1", {})
    assert HypothesisGraphResponse.model_validate(response).warnings == []


def test_hypothesis_response_lists_skipped_graph_warnings_first():
    skipped = {
        "code": "causal_graphs_skipped",
        "message": "1 of 2 causal graphs for rs16940186 were skipped and have no hypothesis.",
        "skipped": [{"graph_index": 1, "causal_gene": "GENE2", "reason": "no_coexpressed_genes"}],
    }
    built = {"code": "variant_id_fallback", "message": "The original rsID was retained."}
    response = _response_from_hypothesis_document(
        "hyp-1",
        {"summary": "s", "graph": {}, "enrichment_warnings": [skipped], "warnings": [built]},
    )

    assert HypothesisGraphResponse.model_validate(response).warnings == [skipped, built]
