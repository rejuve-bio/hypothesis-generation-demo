from unittest.mock import MagicMock

from src.db.enrichment_handler import EnrichmentHandler
from src.db.gene_expression_handler import GeneExpressionHandler
from src.db.hypothesis_handler import HypothesisHandler


def _bare(cls):
    return object.__new__(cls)


def test_ensure_enrich_copy_reuses_matching_project(sample_enrichment):
    handler = _bare(EnrichmentHandler)
    handler.enrich_collection = MagicMock()
    handler.get_enrich_by_phenotype_and_variant = MagicMock(
        return_value={"id": "existing", "project_id": "fork-1"}
    )
    assert handler.ensure_enrich_copy_for_user(
        sample_enrichment, "viewer", "fork-1"
    ) == "existing"
    handler.enrich_collection.insert_one.assert_not_called()


def test_ensure_enrich_copy_creates_owned_project_copy(sample_enrichment):
    handler = _bare(EnrichmentHandler)
    handler.enrich_collection = MagicMock()
    handler.get_enrich_by_phenotype_and_variant = MagicMock(return_value=None)
    copied_id = handler.ensure_enrich_copy_for_user(
        sample_enrichment, "viewer", "fork-1"
    )
    copied = handler.enrich_collection.insert_one.call_args.args[0]
    assert copied["id"] == copied_id
    assert copied["id"] != sample_enrichment["id"]
    assert copied["user_id"] == "viewer"
    assert copied["project_id"] == "fork-1"
    assert "_id" not in copied


def test_create_enrich_persists_skip_tracking_fields():
    handler = _bare(EnrichmentHandler)
    handler.enrich_collection = MagicMock()
    enrich_id = handler.create_enrich(
        "user-1", "project-1", "rs1", "Trait", None, [], {"graph": {}},
        status="skipped", skip_reason="No direct SNP-gene edge found in causal graph."
    )
    inserted = handler.enrich_collection.insert_one.call_args.args[0]
    assert inserted["id"] == enrich_id
    assert inserted["status"] == "skipped"
    assert inserted["skip_reason"] == "No direct SNP-gene edge found in causal graph."


def test_hypothesis_copy_reuses_variant_and_updates_enrich(sample_hypothesis):
    handler = _bare(HypothesisHandler)
    handler.hypothesis_collection = MagicMock()
    handler.get_hypothesis_by_phenotype_and_variant_in_project = MagicMock(
        return_value={"id": "existing-hyp", "enrich_id": "old-enrich"}
    )
    handler.update_hypothesis = MagicMock()
    assert handler.ensure_hypothesis_copy_for_user(
        sample_hypothesis, "viewer", "new-enrich", "fork-1"
    ) == "existing-hyp"
    handler.update_hypothesis.assert_called_once_with(
        "existing-hyp", {"enrich_id": "new-enrich"}
    )
    handler.hypothesis_collection.insert_one.assert_not_called()


def test_hypothesis_copy_reuses_existing_by_enrich(sample_hypothesis):
    handler = _bare(HypothesisHandler)
    handler.hypothesis_collection = MagicMock()
    handler.get_hypothesis_by_phenotype_and_variant_in_project = MagicMock(return_value=None)
    handler.get_hypothesis_by_enrich = MagicMock(return_value={"id": "by-enrich"})
    assert handler.ensure_hypothesis_copy_for_user(
        sample_hypothesis, "viewer", "enrich-1", "fork-1"
    ) == "by-enrich"
    handler.hypothesis_collection.insert_one.assert_not_called()


def test_hypothesis_copy_strips_generated_fields(sample_hypothesis):
    source = {
        **sample_hypothesis,
        "summary": "old",
        "graph": {"nodes": []},
        "go_id": "GO:old",
        "_id": "mongo-id",
    }
    handler = _bare(HypothesisHandler)
    handler.hypothesis_collection = MagicMock()
    handler.get_hypothesis_by_phenotype_and_variant_in_project = MagicMock(return_value=None)
    handler.get_hypothesis_by_enrich = MagicMock(return_value=None)
    copied_id = handler.ensure_hypothesis_copy_for_user(
        source, "viewer", "copied-enrich", "fork-1"
    )
    copied = handler.hypothesis_collection.insert_one.call_args.args[0]
    assert copied["id"] == copied_id
    assert copied["status"] == "pending"
    assert copied["user_id"] == "viewer"
    assert copied["enrich_id"] == "copied-enrich"
    assert copied["project_id"] == "fork-1"
    assert not ({"summary", "graph", "go_id", "_id"} & copied.keys())


def test_tissue_copy_does_nothing_when_target_exists():
    handler = _bare(GeneExpressionHandler)
    handler.get_tissue_selection = MagicMock(return_value={"tissue_name": "Liver"})
    handler.save_tissue_selection = MagicMock()
    handler.ensure_tissue_selection_copy("owner", "template", "viewer", "fork", "rs1")
    handler.get_tissue_selection.assert_called_once_with("viewer", "fork", "rs1")
    handler.save_tissue_selection.assert_not_called()


def test_tissue_copy_copies_source_when_target_missing():
    handler = _bare(GeneExpressionHandler)
    handler.get_tissue_selection = MagicMock(
        side_effect=[None, {"tissue_name": "Liver"}]
    )
    handler.save_tissue_selection = MagicMock()
    handler.ensure_tissue_selection_copy("owner", "template", "viewer", "fork", "rs1")
    handler.save_tissue_selection.assert_called_once_with(
        "viewer", "fork", "rs1", "Liver"
    )


def test_tissue_copy_does_nothing_without_source():
    handler = _bare(GeneExpressionHandler)
    handler.get_tissue_selection = MagicMock(side_effect=[None, None])
    handler.save_tissue_selection = MagicMock()
    handler.ensure_tissue_selection_copy("owner", "template", "viewer", "fork", "rs1")
    handler.save_tissue_selection.assert_not_called()
