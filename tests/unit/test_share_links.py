import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from src.api import router
from src.api.routes.share import create_share_link
from src.api.routes.share_public import router as public_router
from src.api.schemas.share import ShareLinkCreateRequest, SharedSnapshot
from src.db.demo_template_handler import _DELETABLE_PROJECT_COLLECTIONS, DemoTemplateHandler
from src.db.hypothesis_handler import HypothesisHandler
from src.db.project_handler import ProjectHandler
from src.db.share_link_handler import ShareLinkHandler
from src.logging_config import ShareTokenRedactingFilter
from src.services.share_snapshot import build_share_snapshot

TOKEN = "A" * 43
PROJECT_ID = "64b000000000000000000001"
SENTINELS = [
    "SENTINEL_USER",
    "SENTINEL_PROJECT",
    "SENTINEL_HYPOTHESIS",
    "SENTINEL_ENRICH",
    "SENTINEL_THREAD",
    "sentinel@example.org",
    "SENTINEL_ERROR",
    "SENTINEL_FILE_KEY",
]
FORBIDDEN_KEYS = {
    "_id", "user_id", "project_id", "enrich_id", "child_enrich_ids", "thread_ids",
    "task_history", "error_detail", "user_email", "token",
}


def _hypothesis(**overrides):
    hypothesis = {
        "_id": "SENTINEL_HYPOTHESIS",
        "id": "SENTINEL_HYPOTHESIS",
        "user_id": "SENTINEL_USER",
        "project_id": "SENTINEL_PROJECT",
        "enrich_id": "SENTINEL_ENRICH",
        "child_enrich_ids": ["SENTINEL_ENRICH"],
        "thread_ids": ["SENTINEL_THREAD"],
        "task_history": [{"task_name": "SENTINEL_ERROR"}],
        "error_detail": {"message": "SENTINEL_ERROR"},
        "user_email": "sentinel@example.org",
        "gwas_file_id": "SENTINEL_FILE_KEY",
        "variant": "rs16940186",
        "phenotype": "Ulcerative colitis",
        "causal_gene": "IRF8",
        "go_id": "GO:1",
        "status": "Completed",
        "summary": "IRF8 drives inflammation.",
        "graph": {
            "nodes": [{"id": "rs16940186", "type": "snp", "name": "rs16940186", "owner": "SENTINEL_USER"}],
            "edges": [{"source": "rs16940186", "target": "IRF8", "label": "eqtl", "run": "SENTINEL_ENRICH"}],
            "probability": 0.5,
        },
        "warnings": [{"code": "variant_id_fallback", "message": "kept rsID", "variants": ["SENTINEL_ERROR"]}],
        "enrichment_warnings": [{
            "code": "causal_graphs_skipped",
            "message": "1 of 2 causal graphs were skipped and have no hypothesis. See 'skipped' for why.",
            "skipped": [{"reason": "SENTINEL_ERROR"}],
        }],
        "created_at": "2026-11-01T10:00:00.000+00:00Z",
    }
    return {**hypothesis, **overrides}


def _enrich():
    return {
        "_id": "SENTINEL_ENRICH",
        "id": "SENTINEL_ENRICH",
        "user_id": "SENTINEL_USER",
        "project_id": "SENTINEL_PROJECT",
        "GO_terms": [
            {"id": f"GO:{rank}", "name": "inflammation", "p": 0.001, "rank": rank, "genes": ["STAT1"], "similarity": 0.9}
            for rank in range(1, 13)
        ],
    }


def _snapshot(main=None, siblings=(), enrich=None):
    main = main or _hypothesis()
    hypotheses = MagicMock()
    hypotheses.get_variant_hypotheses.return_value = [main, *siblings]
    enrichment = MagicMock()
    enrichment.get_enrich.return_value = enrich or _enrich()
    gene_expression = MagicMock()
    gene_expression.get_tissue_selection.return_value = {
        "tissue_name": "Colon", "user_id": "SENTINEL_USER",
    }
    snapshot = build_share_snapshot(main, hypotheses, enrichment, gene_expression)
    return SharedSnapshot.model_validate(snapshot).model_dump(mode="json")


def _keys(value):
    if isinstance(value, dict):
        for key, item in value.items():
            yield key
            yield from _keys(item)
    elif isinstance(value, list):
        for item in value:
            yield from _keys(item)


def test_snapshot_contains_no_internal_data():
    pending_sibling = _hypothesis(
        id="SENTINEL_HYPOTHESIS_2", status="failed", summary=None, graph=None,
        causal_gene="FOXF1", error="SENTINEL_ERROR",
    )
    payload = json.dumps(_snapshot(siblings=[pending_sibling]))

    assert not [s for s in SENTINELS if s in payload]
    assert not FORBIDDEN_KEYS & set(_keys(json.loads(payload)))


def test_snapshot_shape():
    completed = _hypothesis(id="h-2", causal_gene="FOXF1", graph={"nodes": [], "edges": [], "probability": 0.9})
    pending = _hypothesis(id="h-3", causal_gene="COTL1", status="pending", summary=None, graph=None)

    snapshot = _snapshot(siblings=[pending, completed])

    main = snapshot["hypothesis"]
    assert (main["key"], main["tissue"], main["selected_go_term"]["id"]) == ("h0", "Colon", "GO:1")
    assert main["generated_at"].startswith("2026-11-01T10:00:00")
    assert len(main["go_terms"]) == 10
    assert main["go_terms"][0] == {"id": "GO:1", "name": "inflammation", "p": 0.001, "rank": 1, "genes": ["STAT1"]}
    assert main["warnings"] == [
        {"code": "causal_graphs_skipped", "message": "1 of 2 causal graphs were skipped and have no hypothesis."},
        {"code": "variant_id_fallback", "message": "kept rsID"},
    ]
    assert [(r["key"], r["causal_gene"], r["status"]) for r in snapshot["related"]] == [
        ("h1", "FOXF1", "Completed"),
        ("h2", "COTL1", "Running"),
    ]
    assert snapshot["related"][1] == {"key": "h2", "causal_gene": "COTL1", "probability": None, "status": "Running"}


def test_snapshot_drops_malformed_legacy_items():
    graph = {
        "nodes": [{"id": "IRF8", "type": "gene"}, {"id": 7}, "junk"],
        "edges": [{"source": "rs1", "target": "IRF8"}, {"source": None, "target": "IRF8"}],
        "probability": float("nan"),
    }
    enrich = {"GO_terms": [{"id": "GO:1", "p": float("nan"), "rank": "1"}, {"name": "no id"}]}

    main = _snapshot(main=_hypothesis(graph=graph), enrich=enrich)["hypothesis"]

    assert [node["id"] for node in main["graph"]["nodes"]] == ["IRF8"]
    assert len(main["graph"]["edges"]) == 1
    assert main["probability"] is None
    assert main["go_terms"] == [{"id": "GO:1", "name": None, "p": None, "rank": None, "genes": []}]


@pytest.fixture
def share_links():
    links = MagicMock()
    links.count_active_links.return_value = 0
    links.create_link.side_effect = lambda **kwargs: {
        **kwargs, "id": "link-1", "token": TOKEN, "created_at": datetime.now(timezone.utc),
    }
    return links


def _create(share_links, hypothesis, **body):
    hypotheses = MagicMock()
    hypotheses.get_hypotheses.return_value = hypothesis
    hypotheses.get_variant_hypotheses.return_value = []
    return asyncio.run(
        create_share_link(
            ShareLinkCreateRequest(hypothesis_id="h-1", **body),
            current_user_id="SENTINEL_USER",
            share_links=share_links,
            hypotheses=hypotheses,
            enrichment=MagicMock(get_enrich=MagicMock(return_value=_enrich())),
            gene_expression=MagicMock(get_tissue_selection=MagicMock(return_value=None)),
        )
    )


@pytest.mark.parametrize(
    "hypothesis, active_links, status_code, code",
    [
        (None, 0, 404, None),
        (_hypothesis(status="pending"), 0, 409, "hypothesis_not_completed"),
        (_hypothesis(summary=None), 0, 409, "hypothesis_not_completed"),
        (_hypothesis(), 20, 409, "too_many_links"),
    ],
)
def test_create_rejects(share_links, hypothesis, active_links, status_code, code):
    share_links.count_active_links.return_value = active_links
    with pytest.raises(HTTPException) as exc:
        _create(share_links, hypothesis)
    assert exc.value.status_code == status_code
    if code:
        assert exc.value.detail["code"] == code
    share_links.create_link.assert_not_called()


@pytest.mark.parametrize("body, expected_days", [({}, 30), ({"expires_in_days": 7}, 7), ({"expires_in_days": None}, None)])
def test_create_sets_expiry(share_links, body, expected_days):
    link = _create(share_links, _hypothesis(), **body)

    assert link.state == "active" and link.url_path == f"/s/{TOKEN}"
    if expected_days is None:
        assert link.expires_at is None
    else:
        remaining = link.expires_at - datetime.now(timezone.utc)
        assert timedelta(days=expected_days - 1) < remaining <= timedelta(days=expected_days)


def test_open_link_only_matches_active_links_and_counts_the_view():
    handler = object.__new__(ShareLinkHandler)
    handler.collection = MagicMock()

    handler.open_link(TOKEN)

    query, update = handler.collection.find_one_and_update.call_args.args
    assert query["token"] == TOKEN and query["revoked_at"] is None
    assert query["$or"][0] == {"expires_at": None}
    assert update["$inc"] == {"view_count": 1}


@pytest.fixture
def container(monkeypatch):
    container = MagicMock()
    monkeypatch.setattr("src.api.dependencies._container", container)
    return container


@pytest.fixture
def public_client(container):
    app = FastAPI()
    app.include_router(public_router)
    return TestClient(app)


def _assert_public_headers(response):
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-robots-tag"] == "noindex, nofollow"
    assert response.headers["referrer-policy"] == "no-referrer"


@pytest.mark.parametrize("token", [TOKEN, "short", "A" * 42 + "!"])
def test_public_read_unavailable_link_is_one_uniform_404(public_client, container, token):
    share_links = container.share_link_handler.return_value
    share_links.open_link.return_value = None

    response = public_client.get(f"/public/share/{token}")

    assert response.status_code == 404
    assert response.json() == {"detail": "This link is no longer available."}
    _assert_public_headers(response)
    assert share_links.open_link.called == (token == TOKEN)


def test_public_read_serves_only_the_snapshot(public_client, container):
    container.share_link_handler.return_value.open_link.return_value = {
        "snapshot": _snapshot(),
        "schema_version": 1,
        "created_at": datetime(2026, 11, 1),
        "expires_at": None,
    }

    response = public_client.get(f"/public/share/{TOKEN}")

    assert response.status_code == 200
    assert response.json()["hypothesis"]["causal_gene"] == "IRF8"
    assert response.json()["shared_at"] == "2026-11-01T00:00:00Z"
    _assert_public_headers(response)
    container.hypothesis_handler.assert_not_called()
    container.enrichment_handler.assert_not_called()


def test_public_routes_are_read_only_and_unauthenticated():
    app = FastAPI()
    app.include_router(router)
    paths = app.openapi()["paths"]

    public = [path for path in paths if path.startswith("/public")]
    assert public == [path for path in paths if "{token}" in path] == ["/public/share/{token}"]
    assert list(paths["/public/share/{token}"]) == ["get"]
    assert "security" not in paths["/public/share/{token}"]["get"]


def _bare(cls, *attributes):
    handler = object.__new__(cls)
    for name in ("db", *attributes):
        setattr(handler, name, MagicMock())
    handler.get_analysis_state_path = MagicMock(return_value="/nonexistent/state")
    handler.get_project_analysis_path = MagicMock(return_value="/nonexistent/analysis")
    handler.load_analysis_state = MagicMock(return_value=None)
    return handler


def _hypothesis_handler():
    handler = _bare(HypothesisHandler, "hypothesis_collection", "enrich_collection")
    handler.hypothesis_collection.find_one.return_value = {"id": "h-1"}
    handler.hypothesis_collection.find.return_value = [{"id": "h-1"}]
    handler.hypothesis_collection.delete_one.return_value.deleted_count = 1
    return handler


def _project_handler():
    handler = _bare(
        ProjectHandler, "projects_collection", "credible_sets_collection", "hypothesis_collection",
        "task_updates_collection", "summary_collection", "enrich_collection",
        "analysis_results_collection", "file_metadata_collection",
    )
    handler.hypothesis_collection.find.return_value = []
    handler.projects_collection.find_one.return_value = None
    return handler


def _demo_template_handler():
    handler = _bare(DemoTemplateHandler, *_DELETABLE_PROJECT_COLLECTIONS)
    handler.gene_expression_runs_collection.find.return_value = []
    return handler


@pytest.mark.parametrize(
    "make_handler, delete, expected_filter",
    [
        (_hypothesis_handler, lambda h: h.delete_hypothesis("u-1", "h-1"),
         {"owner_user_id": "u-1", "hypothesis_id": "h-1"}),
        (_hypothesis_handler, lambda h: h.bulk_delete_hypotheses("u-1", ["h-1"]),
         {"owner_user_id": "u-1", "hypothesis_id": {"$in": ["h-1"]}}),
        (_project_handler, lambda h: h._delete_project_data("u-1", PROJECT_ID),
         {"owner_user_id": "u-1", "project_id": PROJECT_ID}),
        (_demo_template_handler, lambda h: h._delete_legacy_owner_data("u-1", PROJECT_ID),
         {"owner_user_id": "u-1", "project_id": PROJECT_ID}),
    ],
)
def test_deletes_cascade_to_share_links(monkeypatch, make_handler, delete, expected_filter):
    monkeypatch.setattr("os.path.exists", lambda _path: False)
    handler = make_handler()

    delete(handler)

    handler.db["share_links"].delete_many.assert_any_call(expected_filter)


def test_access_log_redacts_share_token():
    record = logging.LogRecord(
        "uvicorn.access", logging.INFO, __file__, 0, '%s - "%s %s HTTP/%s" %d',
        ("10.0.0.1:5000", "GET", f"/public/share/{TOKEN}?x=1", "1.1", 200), None,
    )

    ShareTokenRedactingFilter().filter(record)

    assert TOKEN not in record.getMessage()
    assert "/public/share/<redacted>?x=1" in record.getMessage()
