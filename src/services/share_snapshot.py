"""Frozen public snapshot behind a share link, copied field by field so no
internal ids or owner data leak; malformed legacy items are dropped or coerced."""

from __future__ import annotations

import math

from src.utils import normalize_status_responses

MAX_GO_TERMS = 10
MAX_RELATED = 20
_SKIPPED_HINT = " See 'skipped' for why."


def is_completed(hypothesis: dict) -> bool:
    return (
        normalize_status_responses(hypothesis.get("status")) == "Completed"
        and bool(hypothesis.get("summary"))
        and isinstance(hypothesis.get("graph"), dict)
    )


def _number(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _text(value) -> str | None:
    return None if value is None else str(value)


def _dicts(value) -> list[dict]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _graph(graph: dict) -> dict:
    return {
        "nodes": [
            {"id": node["id"], "type": _text(node.get("type")), "name": _text(node.get("name"))}
            for node in _dicts(graph.get("nodes"))
            if isinstance(node.get("id"), str)
        ],
        "edges": [
            {"source": edge["source"], "target": edge["target"], "label": _text(edge.get("label"))}
            for edge in _dicts(graph.get("edges"))
            if isinstance(edge.get("source"), str) and isinstance(edge.get("target"), str)
        ],
        "probability": _number(graph.get("probability")),
    }


def _go_term(term: dict) -> dict:
    rank = term.get("rank")
    return {
        "id": term["id"],
        "name": _text(term.get("name")),
        "p": _number(term.get("p")),
        "rank": rank if isinstance(rank, int) else None,
        "genes": [str(gene) for gene in term.get("genes") or []],
    }


def _warning(warning: dict) -> dict:
    message = _text(warning.get("message"))
    return {
        "code": _text(warning.get("code")),
        "message": message.replace(_SKIPPED_HINT, "") if message else None,
    }


def _probability(hypothesis: dict, enrich: dict) -> float | None:
    probability = _number((hypothesis.get("graph") or {}).get("probability"))
    if probability is None:
        causal_graph = (enrich.get("causal_graph") or {}).get("graph") or {}
        prob = causal_graph.get("prob")
        probability = _number(prob.get("value")) if isinstance(prob, dict) else None
    return probability


def _entry(hypothesis: dict, enrichment, user_id: str) -> dict:
    enrich_id = hypothesis.get("enrich_id")
    enrich = enrichment.get_enrich(user_id, enrich_id) if enrich_id else None
    enrich = enrich if isinstance(enrich, dict) else {}
    entry = {
        "causal_gene": _text(hypothesis.get("causal_gene")),
        "probability": _probability(hypothesis, enrich),
        "status": normalize_status_responses(hypothesis.get("status")),
    }
    if not is_completed(hypothesis):
        return entry

    go_terms = [
        _go_term(term)
        for term in _dicts(enrich.get("GO_terms"))
        if isinstance(term.get("id"), str)
    ]
    return {
        **entry,
        "summary": str(hypothesis["summary"]),
        "graph": _graph(hypothesis["graph"]),
        "selected_go_term": next(
            (term for term in go_terms if term["id"] == hypothesis.get("go_id")), None
        ),
        "go_terms": go_terms[:MAX_GO_TERMS],
    }


def build_share_snapshot(hypothesis: dict, hypotheses, enrichment, gene_expression) -> dict:
    """Snapshot a completed hypothesis and its same-variant siblings."""
    user_id = hypothesis["user_id"]
    project_id = hypothesis.get("project_id")
    variant = hypothesis.get("variant") or hypothesis.get("variant_id")
    variant_rsid = hypothesis.get("variant_rsid") or variant

    siblings = []
    tissue = None
    if project_id and variant:
        siblings = [
            h
            for h in hypotheses.get_variant_hypotheses(user_id, project_id, variant)
            if h.get("id") != hypothesis["id"]
        ]
        selection = gene_expression.get_tissue_selection(user_id, project_id, variant_rsid)
        tissue = _text(selection.get("tissue_name")) if selection else None

    related = sorted(
        (_entry(h, enrichment, user_id) for h in siblings),
        key=lambda entry: entry["probability"] or 0,
        reverse=True,
    )
    for index, entry in enumerate(related, start=1):
        entry["key"] = f"h{index}"

    warnings = [*_dicts(hypothesis.get("enrichment_warnings")), *_dicts(hypothesis.get("warnings"))]
    return {
        "hypothesis": {
            **_entry(hypothesis, enrichment, user_id),
            "key": "h0",
            "variant": _text(variant),
            "variant_rsid": _text(variant_rsid),
            "phenotype": _text(hypothesis.get("phenotype")),
            "tissue": tissue,
            "warnings": [_warning(warning) for warning in warnings],
            "generated_at": hypothesis.get("created_at"),
        },
        "related": related[:MAX_RELATED],
        "related_truncated": len(related) > MAX_RELATED,
    }
