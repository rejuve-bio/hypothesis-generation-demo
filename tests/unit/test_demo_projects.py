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
