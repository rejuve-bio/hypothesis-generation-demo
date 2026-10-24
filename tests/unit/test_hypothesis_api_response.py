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
