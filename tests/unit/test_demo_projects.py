from types import SimpleNamespace
from unittest.mock import MagicMock

from src.services.demo import projects as service


def _services():
    return {
        "demo_templates": MagicMock(),
        "projects": MagicMock(),
        "enrichment": MagicMock(),
        "hypotheses": MagicMock(),
        "gene_expression": MagicMock(),
    }


def _source_docs(services):
    enrich = {
        "id": "enrich-source", "user_id": "owner", "project_id": "template-1",
        "variant": "rs1", "phenotype": "Trait"
    }
    hypothesis = {
        "id": "hyp-source", "user_id": "owner", "project_id": "template-1",
        "enrich_id": "enrich-source", "variant": "rs1", "phenotype": "Trait"
    }
    services["enrichment"].get_enrich.side_effect = [None, enrich]
    services["hypotheses"].get_hypothesis_by_enrich.return_value = hypothesis
    return enrich, hypothesis


def test_owner_path_returns_owned_context_without_copy():
    services = _services()
    services["enrichment"].get_enrich.return_value = {
        "id": "enrich-1", "project_id": "project-1"
    }
    services["hypotheses"].get_hypothesis_by_enrich.return_value = {
        "id": "hyp-1", "project_id": "project-1"
    }
    result = service.resolve_enrich_and_hypothesis_for_write(
        **services, current_user_id="owner", enrich_id="enrich-1"
    )
    assert result == service.HypothesisWriteContext(
        "owner", "enrich-1", "hyp-1", "project-1", False
    )
    services["enrichment"].ensure_enrich_copy_for_user.assert_not_called()


def test_demo_first_fork_copies_enrich_hypothesis_and_tissue(monkeypatch):
    services = _services()
    enrich, hypothesis = _source_docs(services)
    access = SimpleNamespace(
        mode="demo_read", owner_user_id="owner",
        template={"template_project_id": "template-1", "display_name": "Demo", "slug": "demo", "demo_owner_id": "owner"},
    )
    monkeypatch.setattr(service, "resolve_project_access_or_none", lambda *_: access)
    services["demo_templates"].get_user_fork.return_value = None
    services["projects"].fork_project_from_template.return_value = "fork-1"
    services["enrichment"].ensure_enrich_copy_for_user.return_value = "enrich-copy"
    services["hypotheses"].get_hypothesis_by_phenotype_and_variant_in_project.return_value = None
    services["hypotheses"].ensure_hypothesis_copy_for_user.return_value = "hyp-copy"

    result = service.resolve_enrich_and_hypothesis_for_write(
        **services, current_user_id="viewer", enrich_id="enrich-source"
    )

    assert result == service.HypothesisWriteContext(
        "viewer", "enrich-copy", "hyp-copy", "fork-1", True
    )
    services["projects"].fork_project_from_template.assert_called_once()
    services["enrichment"].ensure_enrich_copy_for_user.assert_called_once_with(
        enrich, "viewer", "fork-1"
    )
    services["hypotheses"].ensure_hypothesis_copy_for_user.assert_called_once_with(
        hypothesis, "viewer", "enrich-copy", "fork-1"
    )
    services["gene_expression"].ensure_tissue_selection_copy.assert_called_once_with(
        "owner", "template-1", "viewer", "fork-1", "rs1"
    )


def test_demo_existing_fork_is_reused(monkeypatch):
    services = _services()
    _source_docs(services)
    access = SimpleNamespace(
        mode="demo_read", owner_user_id="owner",
        template={"template_project_id": "template-1", "display_name": "Demo", "slug": "demo", "demo_owner_id": "owner"},
    )
    monkeypatch.setattr(service, "resolve_project_access_or_none", lambda *_: access)
    services["demo_templates"].get_user_fork.return_value = "fork-existing"
    services["enrichment"].ensure_enrich_copy_for_user.return_value = "enrich-copy"
    services["hypotheses"].ensure_hypothesis_copy_for_user.return_value = "hyp-copy"

    result = service.resolve_enrich_and_hypothesis_for_write(
        **services, current_user_id="viewer", enrich_id="enrich-source"
    )
    assert result.project_id == "fork-existing"
    assert result.forked is False
    services["projects"].fork_project_from_template.assert_not_called()


def test_existing_hypothesis_in_fork_is_reused_and_relinked(monkeypatch):
    services = _services()
    _source_docs(services)
    access = SimpleNamespace(
        mode="demo_read", owner_user_id="owner",
        template={"template_project_id": "template-1", "display_name": "Demo", "slug": "demo", "demo_owner_id": "owner"},
    )
    monkeypatch.setattr(service, "resolve_project_access_or_none", lambda *_: access)
    services["demo_templates"].get_user_fork.return_value = "fork-1"
    services["enrichment"].ensure_enrich_copy_for_user.return_value = "new-enrich"
    services["hypotheses"].get_hypothesis_by_phenotype_and_variant_in_project.return_value = {
        "id": "existing-hyp", "enrich_id": "old-enrich"
    }
    result = service.resolve_enrich_and_hypothesis_for_write(
        **services, current_user_id="viewer", enrich_id="enrich-source"
    )
    assert result.hypothesis_id == "existing-hyp"
    services["hypotheses"].update_hypothesis.assert_called_once_with(
        "existing-hyp", {"enrich_id": "new-enrich"}
    )
    services["hypotheses"].ensure_hypothesis_copy_for_user.assert_not_called()


def test_missing_enrichment_returns_none():
    services = _services()
    services["enrichment"].get_enrich.return_value = None
    assert service.resolve_enrich_and_hypothesis_for_write(
        **services, current_user_id="viewer", enrich_id="missing"
    ) is None


def test_access_denied_returns_none(monkeypatch):
    services = _services()
    _source_docs(services)
    monkeypatch.setattr(service, "resolve_project_access_or_none", lambda *_: None)
    assert service.resolve_enrich_and_hypothesis_for_write(
        **services, current_user_id="viewer", enrich_id="enrich-source"
    ) is None


def test_owner_mismatch_returns_none(monkeypatch):
    services = _services()
    _source_docs(services)
    monkeypatch.setattr(
        service, "resolve_project_access_or_none",
        lambda *_: SimpleNamespace(owner_user_id="different-owner", mode="demo_read", template={}),
    )
    assert service.resolve_enrich_and_hypothesis_for_write(
        **services, current_user_id="viewer", enrich_id="enrich-source"
    ) is None


def test_owned_docs_without_project_id_return_none(monkeypatch):
    services = _services()
    services["enrichment"].get_enrich.return_value = {"id": "enrich-1"}
    services["hypotheses"].get_hypothesis_by_enrich.return_value = {"id": "hyp-1"}
    warning = MagicMock()
    monkeypatch.setattr(service.logger, "warning", warning)
    assert service.resolve_enrich_and_hypothesis_for_write(
        **services, current_user_id="owner", enrich_id="enrich-1"
    ) is None
    assert "both have no project_id" in warning.call_args.args[0]


def test_resolve_hypothesis_data_user_id_via_template_access(monkeypatch):
    demo_templates = MagicMock()
    hypotheses = MagicMock()
    # Not the current user's own hypothesis.
    hypotheses.get_hypotheses.return_value = None
    hypotheses.get_hypothesis_by_id.return_value = {
        "id": "hyp-shared", "user_id": "owner", "project_id": "template-1"
    }
    access = SimpleNamespace(owner_user_id="owner", mode="demo_read", template={})
    monkeypatch.setattr(service, "resolve_project_access_or_none", lambda *_: access)

    result = service.resolve_hypothesis_data_user_id(
        demo_templates, hypotheses, "viewer", "hyp-shared"
    )

    assert result == "owner"


def test_resolve_hypothesis_data_user_id_returns_none_when_owner_mismatched(monkeypatch):
    demo_templates = MagicMock()
    hypotheses = MagicMock()
    hypotheses.get_hypotheses.return_value = None
    hypotheses.get_hypothesis_by_id.return_value = {
        "id": "hyp-shared", "user_id": "someone-else", "project_id": "template-1"
    }
    access = SimpleNamespace(owner_user_id="owner", mode="demo_read", template={})
    monkeypatch.setattr(service, "resolve_project_access_or_none", lambda *_: access)

    result = service.resolve_hypothesis_data_user_id(
        demo_templates, hypotheses, "viewer", "hyp-shared"
    )

    assert result is None


def test_build_project_summary_includes_credible_sets_hypotheses_and_state():
    projects = MagicMock()
    analysis = MagicMock()
    hypotheses = MagicMock()
    files = MagicMock()
    projects.get_projects.return_value = {
        "gwas_file_id": "file-1", "population": "EUR", "ref_genome": "GRCh38"
    }
    files.get_file_metadata.return_value = {
        "download_url": "/download/1", "record_count": 42
    }
    projects.load_analysis_state.return_value = {"status": "Completed"}
    analysis.get_credible_sets_for_project.return_value = [
        {"variants_count": 3}, {"variants_count": 5}
    ]
    hypotheses.get_hypotheses.return_value = [
        {"project_id": "project-1"}, {"project_id": "other"}
    ]

    summary = service.build_project_summary(
        project_id="project-1",
        name="Demo Project",
        phenotype="Trait",
        created_at="2026-01-01",
        data_user_id="owner",
        projects=projects,
        analysis=analysis,
        hypotheses=hypotheses,
        files=files,
    )

    assert summary["gwas_file"] == "/download/1"
    assert summary["gwas_records_count"] == 42
    assert summary["status"] == "Completed"
    assert summary["total_credible_sets_count"] == 2
    assert summary["total_variants_count"] == 8
    assert summary["hypothesis_count"] == 1
    assert summary["population"] == "European"
    assert summary["ref_genome"] == "GRCh38"


def test_build_project_summary_degrades_gracefully_when_a_sub_call_raises():
    """build_project_summary has no top-level try/except: each optional
    sub-section (file metadata, analysis state, credible sets, hypothesis
    count) swallows its own exception and degrades to a safe default. It
    never returns None -- confirmed by reading the source before writing
    this test."""
    projects = MagicMock()
    analysis = MagicMock()
    hypotheses = MagicMock()
    files = MagicMock()
    projects.get_projects.return_value = {}
    projects.load_analysis_state.side_effect = RuntimeError("state file corrupt")
    analysis.get_credible_sets_for_project.return_value = []
    hypotheses.get_hypotheses.return_value = []

    summary = service.build_project_summary(
        project_id="project-1",
        name="Demo Project",
        phenotype="Trait",
        created_at="2026-01-01",
        data_user_id="owner",
        projects=projects,
        analysis=analysis,
        hypotheses=hypotheses,
        files=files,
    )

    assert summary is not None
    assert summary["status"] == "Completed"
    assert summary["running_task"] == "Analysis completed successfully."
    assert summary["total_credible_sets_count"] == 0
