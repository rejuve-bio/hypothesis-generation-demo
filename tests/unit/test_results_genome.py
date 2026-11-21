import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from bson import ObjectId

from src.api.routes import analysis as analysis_routes
from src.api.routes import projects as projects_routes
from src.config import RESULTS_GENOME_BUILD, resolve_results_genome
from src.db.project_handler import ProjectHandler
from src.services.demo import projects as demo_service
from src.tasks.project import get_project_with_full_data

PROJECT_ID = "64b000000000000000000001"


@pytest.mark.parametrize(
    "project, has_results, expected",
    [
        ({"results_genome": "GRCh38"}, False, "GRCh38"),
        ({"ref_genome": "GRCh37"}, True, RESULTS_GENOME_BUILD),
        ({"ref_genome": "GRCh37"}, False, None),
        (None, False, None),
    ],
)
def test_resolve_results_genome(project, has_results, expected):
    assert resolve_results_genome(project, has_results) == expected


def test_set_results_genome_updates_the_users_project():
    handler = object.__new__(ProjectHandler)
    handler.projects_collection = MagicMock()

    handler.set_results_genome("user-1", PROJECT_ID, "GRCh38")

    query, update = handler.projects_collection.update_one.call_args.args
    assert query == {"_id": ObjectId(PROJECT_ID), "user_id": "user-1"}
    assert update["$set"]["results_genome"] == "GRCh38"
    assert "updated_at" in update["$set"]


# Each case: (project document, credible sets, expected results_genome)
CASES = [
    pytest.param(
        {"ref_genome": "GRCh37", "results_genome": "GRCh38"}, [{"variants_count": 2}], "GRCh38",
        id="stored",
    ),
    pytest.param({"ref_genome": "GRCh37"}, [{"variants_count": 2}], "GRCh38", id="older-with-results"),
    pytest.param({"ref_genome": "GRCh37"}, [], None, id="older-without-results"),
]


def _handlers(project_doc, credible_sets):
    projects = MagicMock()
    projects.get_projects.return_value = {"id": PROJECT_ID, "name": "P", "gwas_file_id": "f", **project_doc}
    projects.load_analysis_state.return_value = {"status": "Completed"}
    analysis = MagicMock()
    analysis.get_credible_sets_for_project.return_value = credible_sets
    hypotheses = MagicMock(**{"get_hypotheses.return_value": []})
    return projects, analysis, hypotheses


@pytest.mark.parametrize("project_doc, credible_sets, expected", CASES)
def test_project_detail_returns_results_genome_next_to_ref_genome(project_doc, credible_sets, expected):
    projects, analysis, hypotheses = _handlers(project_doc, credible_sets)

    response, status = get_project_with_full_data(
        projects, analysis, hypotheses, MagicMock(), "user-1", PROJECT_ID
    )

    assert status == 200
    assert response["analysis_parameters"]["ref_genome"] == "GRCh37"
    assert response["analysis_parameters"]["results_genome"] == expected


@pytest.mark.parametrize("project_doc, credible_sets, expected", CASES)
def test_demo_project_summary_returns_results_genome(project_doc, credible_sets, expected):
    projects, analysis, hypotheses = _handlers(project_doc, credible_sets)

    summary = demo_service.build_project_summary(
        project_id=PROJECT_ID, name="P", phenotype="Trait", created_at=None,
        data_user_id="owner", projects=projects, analysis=analysis,
        hypotheses=hypotheses, files=MagicMock(),
    )

    assert summary["ref_genome"] == "GRCh37"
    assert summary["results_genome"] == expected


@pytest.mark.parametrize("project_doc, credible_sets, expected", CASES)
def test_project_list_returns_results_genome(project_doc, credible_sets, expected):
    projects, analysis, hypotheses = _handlers(project_doc, credible_sets)
    projects.get_projects.return_value = [projects.get_projects.return_value]
    demo_templates = MagicMock(**{
        "list_templates.return_value": [],
        "get_template_by_project_id.return_value": None,
    })

    response = asyncio.run(projects_routes.get_projects(
        id=None, thread_id=None, current_user_id="user-1", projects=projects,
        analysis=analysis, hypotheses=hypotheses, enrichment=MagicMock(),
        gene_expression=MagicMock(), files=MagicMock(), demo_templates=demo_templates,
    ))

    [project] = response.projects
    assert project["ref_genome"] == "GRCh37"
    assert project["results_genome"] == expected


@pytest.mark.parametrize(
    "project_doc, expected",
    [({"results_genome": "GRCh38"}, "GRCh38"), ({"ref_genome": "GRCh37"}, RESULTS_GENOME_BUILD)],
)
def test_credible_set_endpoint_returns_results_genome(monkeypatch, project_doc, expected):
    monkeypatch.setattr(
        analysis_routes, "resolve_project_access",
        lambda *_: SimpleNamespace(owner_user_id="owner"),
    )
    analysis = MagicMock(**{"get_credible_set_by_id.return_value": {
        "variants_data": {"data": {"variant": ["1:785910:G:C"], "PIP": [0.9]}},
    }})
    projects = MagicMock(**{"get_projects.return_value": project_doc})

    response = asyncio.run(analysis_routes.get_credible_sets(
        project_id=PROJECT_ID, credible_set_id="cs-1", current_user_id="user-1",
        analysis=analysis, demo_templates=MagicMock(), projects=projects,
    ))

    projects.get_projects.assert_called_once_with("owner", PROJECT_ID)
    assert response.results_genome == expected
    assert response.variants
