import asyncio
from unittest.mock import MagicMock

import pytest
from bson import ObjectId
from fastapi import HTTPException

from src.api.dependencies import parse_thread_id
from src.api.routes import enrichment as enrichment_routes
from src.api.routes import hypothesis as hypothesis_routes
from src.db.hypothesis_handler import HypothesisHandler
from src.db.project_handler import ProjectHandler
from src.utils import MAX_THREAD_ID_LENGTH, normalize_thread_id


def _project_handler():
    handler = object.__new__(ProjectHandler)
    for name in (
        "projects_collection",
        "credible_sets_collection",
        "hypothesis_collection",
        "task_updates_collection",
        "summary_collection",
        "enrich_collection",
        "analysis_results_collection",
        "file_metadata_collection",
        "db",
    ):
        setattr(handler, name, MagicMock())
    return handler


def _hypothesis_handler():
    handler = object.__new__(HypothesisHandler)
    handler.hypothesis_collection = MagicMock()
    return handler


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        ("   ", None),
        (" thread-1 \n", "thread-1"),
        ("a" * MAX_THREAD_ID_LENGTH, "a" * MAX_THREAD_ID_LENGTH),
    ],
)
def test_normalize_thread_id(value, expected):
    assert normalize_thread_id(value) == expected


def test_overlong_thread_id_returns_400():
    with pytest.raises(HTTPException) as exc_info:
        parse_thread_id("a" * (MAX_THREAD_ID_LENGTH + 1))
    assert exc_info.value.status_code == 400


def test_create_project_stores_thread_id():
    handler = _project_handler()

    handler.create_project(
        "user-1", "Project", "file-1", "T2D", "EUR", "GRCh38", thread_id="thread-1"
    )

    assert handler.projects_collection.insert_one.call_args.args[0]["thread_id"] == "thread-1"


def test_get_projects_by_thread_includes_projects_used_in_thread():
    handler = _project_handler()
    used_project_id = "64b000000000000000000002"
    handler.hypothesis_collection.distinct.return_value = [used_project_id, None]
    handler.projects_collection.find.return_value = []

    handler.get_projects("user-1")
    handler.get_projects("user-1", thread_id="thread-1")

    unfiltered, by_thread = (c.args[0] for c in handler.projects_collection.find.call_args_list)
    assert unfiltered == {"user_id": "user-1"}
    handler.hypothesis_collection.distinct.assert_called_once_with(
        "project_id", {"user_id": "user-1", "thread_ids": "thread-1"}
    )
    assert by_thread["$or"] == [
        {"thread_id": "thread-1"},
        {"_id": {"$in": [ObjectId(used_project_id)]}},
    ]


def test_fork_does_not_inherit_template_thread_id(monkeypatch):
    handler = _project_handler()
    template_id = "64b000000000000000000001"
    handler.projects_collection.find_one.return_value = {
        "_id": ObjectId(template_id),
        "user_id": "owner",
        "name": "Demo",
        "thread_id": "owner-thread",
    }
    handler._copy_collection_docs = MagicMock(return_value=[])
    monkeypatch.setattr(handler, "get_analysis_state_path", lambda *_: "/nonexistent/state")

    handler.fork_project_from_template("owner", template_id, "viewer")

    assert "thread_id" not in handler.projects_collection.insert_one.call_args.args[0]


def test_tag_hypothesis_thread_adds_to_set_and_skips_missing_thread():
    handler = _hypothesis_handler()

    handler.tag_hypothesis_thread("hyp-1", "thread-1")
    handler.tag_hypothesis_thread("hyp-1", None)

    handler.hypothesis_collection.update_one.assert_called_once_with(
        {"id": "hyp-1"}, {"$addToSet": {"thread_ids": "thread-1"}}
    )


def test_get_hypotheses_filters_by_thread():
    handler = _hypothesis_handler()
    handler.hypothesis_collection.find.return_value = []

    handler.get_hypotheses("user-1", thread_id="thread-1")

    handler.hypothesis_collection.find.assert_called_once_with(
        {"user_id": "user-1", "thread_ids": "thread-1"}
    )


def test_hypothesis_copy_drops_source_threads(sample_hypothesis):
    handler = _hypothesis_handler()
    handler.get_hypothesis_by_phenotype_and_variant_in_project = MagicMock(return_value=None)
    handler.get_hypothesis_by_enrich = MagicMock(return_value=None)

    handler.ensure_hypothesis_copy_for_user(
        {**sample_hypothesis, "thread_ids": ["owner-thread"]}, "viewer", "enrich-2", "fork-1"
    )

    assert "thread_ids" not in handler.hypothesis_collection.insert_one.call_args.args[0]


def test_get_hypothesis_list_filters_by_thread_and_exposes_links(monkeypatch):
    hypotheses = MagicMock()
    hypotheses.get_hypotheses.return_value = [
        {"id": "hyp-1", "project_id": "project-1", "thread_ids": ["thread-1"]}
    ]
    monkeypatch.setattr(hypothesis_routes.status_tracker, "get_history", lambda _id: [])

    result = asyncio.run(
        hypothesis_routes.get_hypothesis(
            id=None,
            thread_id=" thread-1 ",
            current_user_id="user-1",
            hypotheses=hypotheses,
            enrichment=MagicMock(),
            gene_expression=MagicMock(),
            demo_templates=MagicMock(),
        )
    )

    hypotheses.get_hypotheses.assert_called_once_with(user_id="user-1", thread_id="thread-1")
    entry = result.model_dump()[0]
    assert entry["project_id"] == "project-1"
    assert entry["thread_ids"] == ["thread-1"]


def _run_post_enrich(monkeypatch, hypotheses, thread_id):
    monkeypatch.setattr(enrichment_routes, "invoke_enrichment_deployment", lambda **_: None)
    monkeypatch.setattr(enrichment_routes.Config, "from_env", MagicMock)
    monkeypatch.setattr(enrichment_routes, "validate_ldsc_tissue_mapping", lambda *_, **__: None)
    monkeypatch.setattr(
        enrichment_routes,
        "resolve_project_access",
        lambda *_: MagicMock(template=None, is_demo_read=False),
    )
    projects = MagicMock()
    projects.get_projects.return_value = {"id": "project-1", "phenotype": "T2D"}
    gene_expression = MagicMock()
    gene_expression.get_ldsc_results_for_project.return_value = [{"tissue_name": "Liver"}]
    body = enrichment_routes.EnrichPostBody(
        variant="rs1", project_id="project-1", tissue_name="Liver", thread_id=thread_id
    )

    asyncio.run(
        enrichment_routes.post_enrich(
            request=MagicMock(),
            body=body,
            variant=None,
            project_id=None,
            tissue_name=None,
            thread_id=None,
            current_user_id="user-1",
            projects=projects,
            hypotheses=hypotheses,
            gene_expression=gene_expression,
            demo_templates=MagicMock(),
        )
    )


def test_post_enrich_tags_new_hypothesis_with_thread(monkeypatch):
    hypotheses = MagicMock()
    hypotheses.get_hypothesis_by_phenotype_and_variant_in_project.return_value = None

    _run_post_enrich(monkeypatch, hypotheses, "thread-1")

    assert hypotheses.create_hypothesis.call_args.args[1]["thread_ids"] == ["thread-1"]


def test_post_enrich_tags_reused_hypothesis_with_thread(monkeypatch):
    hypotheses = MagicMock()
    hypotheses.get_hypothesis_by_phenotype_and_variant_in_project.return_value = {"id": "hyp-1"}

    _run_post_enrich(monkeypatch, hypotheses, "thread-2")

    hypotheses.tag_hypothesis_thread.assert_called_once_with("hyp-1", "thread-2")
    hypotheses.create_hypothesis.assert_not_called()
