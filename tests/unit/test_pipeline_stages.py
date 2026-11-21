import pytest

from src import pipeline_stages
from src.pipeline_stages import cleanup_targets, rerun_params, resumable_from

HARMONIZED = {"harmonized_file": "harmonized.tsv.gz"}
FINE_MAPPED = {**HARMONIZED, "credible_sets": True, "credible_sets_source": "finemapping"}
FROM_OPENTARGETS = {**HARMONIZED, "credible_sets": True, "credible_sets_source": "opentargets"}


def _no_significant_variants(below_1e5):
    return {"error_type": "no_significant_variants", "min_p_value": 1e-6, "variants_below_1e5": below_1e5}


@pytest.mark.parametrize("state, expected", [
    ({"status": "Running", "stage": "Cojo", "artifacts": HARMONIZED}, []),
    ({"status": "Failed", "stage": "Harmonization"}, []),
    ({"status": "Failed", "stage": "Filtering", "artifacts": HARMONIZED,
      "error_detail": _no_significant_variants(3)}, ["filtering"]),
    ({"status": "Failed", "stage": "Filtering", "artifacts": HARMONIZED,
      "error_detail": _no_significant_variants(0)}, []),
    ({"status": "Failed", "stage": "Cojo", "artifacts": HARMONIZED}, ["filtering"]),
    ({"status": "Failed", "stage": "Fine_mapping", "artifacts": HARMONIZED}, ["filtering"]),
    ({"status": "Failed", "stage": "LDSC_Analysis", "artifacts": FINE_MAPPED}, ["ldsc"]),
    ({"status": "Failed", "stage": "LDSC_Tissue_Analysis", "artifacts": FINE_MAPPED}, ["ldsc"]),
    ({"status": "Failed", "stage": "LDSC_Analysis", "artifacts": FROM_OPENTARGETS}, ["ldsc"]),
    ({"status": "Failed", "stage": "OpenTargets_credible_sets",
      "artifacts": {**HARMONIZED, "credible_sets_source": "opentargets"}}, []),
    ({"status": "Failed", "stage": "Cojo"}, []),
    ({"status": "Failed", "stage": "Unknown", "resumed_from": "ldsc", "artifacts": FINE_MAPPED}, ["ldsc"]),
])
def test_resumable_from(state, expected):
    assert resumable_from(state) == expected


def test_enabling_a_stage_makes_it_resumable(monkeypatch):
    stages = [
        {**stage, "resumable": True} if stage["key"] == "cojo" else stage
        for stage in pipeline_stages.PIPELINE_STAGES
    ]
    monkeypatch.setattr(pipeline_stages, "PIPELINE_STAGES", stages)
    state = {"status": "Failed", "stage": "Cojo", "artifacts": HARMONIZED}

    assert resumable_from(state) == ["cojo"]


@pytest.mark.parametrize("stage, cleanup, params", [
    ("filtering", ["credible_sets", "analysis_results"], {"p_value_threshold"}),
    ("ldsc", [], set()),
])
def test_rerunning_from_a_stage_cleans_and_accepts_what_later_stages_need(stage, cleanup, params):
    assert cleanup_targets(stage) == cleanup
    assert rerun_params(stage) == params


def test_every_resumable_stage_defines_its_resume_state():
    assert all("resume_state" in stage for stage in pipeline_stages.PIPELINE_STAGES if stage["resumable"])
