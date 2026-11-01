import pickle
from unittest.mock import MagicMock

import pandas as pd
import pytest
from fastapi import HTTPException

from src.api.routes.projects import _gwas_p_value_threshold
from src.flows import analysis as flow_module
from src.tasks.analysis import NoSignificantVariantsError, filter_significant_variants


def _sumstats(p_values):
    return pd.DataFrame(
        {"p_value": p_values},
        index=pd.Index([f"1:{i}:A:G" for i in range(len(p_values))], name="variant_id"),
    )


def test_filter_keeps_variants_below_threshold(tmp_path):
    significant_df, _ = filter_significant_variants.fn(_sumstats([1e-9, 1e-3]), str(tmp_path))

    assert list(significant_df.index) == ["1:0:A:G"]


def test_filter_reports_p_value_range_when_nothing_is_significant(tmp_path):
    sumstats = _sumstats([3.2e-7, 5e-7, 2e-6, 8e-6, 0.5, None])

    with pytest.raises(NoSignificantVariantsError) as exc:
        filter_significant_variants.fn(sumstats, str(tmp_path))

    assert str(exc.value) == (
        "No variants reach p < 5e-08. Most significant: 1:0:A:G (p = 3.2e-07). "
        "2 variants pass p < 1e-06 and 4 pass p < 1e-05; "
        "rerun with a higher p_value_threshold to include them."
    )
    assert exc.value.as_detail() == {
        "error_type": "no_significant_variants",
        "reason": "no_significant_variants",
        "message": str(exc.value),
        "p_value_threshold": 5e-8,
        "min_p_value": 3.2e-7,
        "min_p_variant": "1:0:A:G",
        "variants_below_1e6": 2,
        "variants_below_1e5": 4,
    }


def test_filter_does_not_suggest_rerun_when_nothing_passes_the_maximum_threshold(tmp_path):
    with pytest.raises(NoSignificantVariantsError) as exc:
        filter_significant_variants.fn(_sumstats([0.01, 0.5]), str(tmp_path), p_threshold=1e-5)

    assert str(exc.value) == "No variants reach p < 1e-05. Most significant: 1:0:A:G (p = 0.01)."


def test_no_significant_variants_error_survives_pickling():
    error = NoSignificantVariantsError(
        "No variants reach p < 5e-08.", p_value_threshold=5e-8, min_p_value=1e-6,
        min_p_variant="1:0:A:G", variants_below_1e6=0, variants_below_1e5=3,
    )

    assert pickle.loads(pickle.dumps(error)).as_detail() == error.as_detail()


@pytest.mark.parametrize("value", ["5e-8", "1e-5", 1e-6])
def test_gwas_p_value_threshold_accepts_valid_values(value):
    assert _gwas_p_value_threshold(value) == float(value)


@pytest.mark.parametrize("value", [0, -1e-8, 2e-5, "", "abc", float("nan")])
def test_gwas_p_value_threshold_rejects_invalid_values(value):
    with pytest.raises(HTTPException) as exc:
        _gwas_p_value_threshold(value)
    assert exc.value.status_code == 400


def _configure_flow(monkeypatch, immediate_task_factory):
    saved_states = []
    ldsc_future = MagicMock()
    notify = MagicMock()

    monkeypatch.setattr(flow_module, "get_project_analysis_path_task", immediate_task_factory(lambda *_: "/out"))
    monkeypatch.setattr(flow_module, "harmonize_sumstats_with_nextflow", immediate_task_factory(lambda *_, **__: "/out/harmonized.tsv.gz"))
    monkeypatch.setattr(flow_module, "run_combined_ldsc_tissue_analysis", MagicMock(**{"submit.return_value": ldsc_future}))
    monkeypatch.setattr(flow_module, "save_analysis_state_task", immediate_task_factory(lambda _u, _p, state: saved_states.append(state)))
    monkeypatch.setattr(flow_module, "send_pipeline_notification", notify)
    monkeypatch.setattr(flow_module.Config, "from_env", MagicMock(return_value=MagicMock()))
    return saved_states, ldsc_future, notify


def _run_flow(p_value_threshold=5e-8):
    return flow_module.analysis_pipeline_flow.fn(
        "user-1", "project-1", p_value_threshold=p_value_threshold,
        user_email="user@example.com", project_name="Wrist",
    )


def test_no_significant_variants_fails_at_filtering_without_waiting_for_ldsc(
    monkeypatch, immediate_task_factory
):
    saved_states, ldsc_future, notify = _configure_flow(monkeypatch, immediate_task_factory)
    error = NoSignificantVariantsError("No variants reach p < 1e-06.", p_value_threshold=1e-6)

    def filter_variants(*_args, **_kwargs):
        raise error

    filter_task = immediate_task_factory(filter_variants)
    monkeypatch.setattr(flow_module, "filter_significant_variants", filter_task)

    with pytest.raises(NoSignificantVariantsError):
        _run_flow(p_value_threshold=1e-6)

    assert filter_task.calls[0][1] == {"p_threshold": 1e-6}
    assert saved_states[-1] == {
        "status": "Failed",
        "stage": "Filtering",
        "progress": 30,
        "message": "No variants reach p < 1e-06.",
        "error_detail": error.as_detail(),
    }
    notify.assert_called_once_with(
        "user@example.com", "Wrist", "project-1",
        success=False, detail="No variants reach p < 1e-06.",
    )
    ldsc_future.result.assert_not_called()


def test_unexpected_failure_keeps_the_stage_it_happened_in(monkeypatch, immediate_task_factory):
    saved_states, _, notify = _configure_flow(monkeypatch, immediate_task_factory)
    monkeypatch.setattr(flow_module, "filter_significant_variants", immediate_task_factory(lambda *_, **__: (_sumstats([1e-9]), None)))

    def cojo(*_args, **_kwargs):
        raise RuntimeError("COJO analysis failed for all chromosomes")

    monkeypatch.setattr(flow_module, "run_cojo_per_chromosome", immediate_task_factory(cojo))

    with pytest.raises(RuntimeError):
        _run_flow()

    assert saved_states[-1] == {
        "status": "Failed",
        "stage": "Cojo",
        "progress": 50,
        "message": "Analysis pipeline failed: COJO analysis failed for all chromosomes",
    }
    notify.assert_called_once()


def test_specific_failed_state_is_not_overwritten(monkeypatch, immediate_task_factory):
    saved_states, _, notify = _configure_flow(monkeypatch, immediate_task_factory)
    monkeypatch.setattr(flow_module, "filter_significant_variants", immediate_task_factory(lambda *_, **__: (_sumstats([1e-9]), None)))
    monkeypatch.setattr(flow_module, "run_cojo_per_chromosome", immediate_task_factory(lambda *_, **__: (_sumstats([1e-9]), None)))
    monkeypatch.setattr(flow_module, "create_region_batches", lambda *_, **__: [["region"]])
    monkeypatch.setattr(flow_module, "save_sumstats_for_workers", lambda *_: "/out/sumstats.pkl")
    monkeypatch.setattr(flow_module, "cleanup_sumstats_file", MagicMock())
    monkeypatch.setattr(flow_module, "finemap_region_batch_worker", MagicMock(**{"map.return_value": []}))

    with pytest.raises(RuntimeError, match="All fine-mapping batches failed"):
        _run_flow()

    assert saved_states[-1]["stage"] == "Fine_mapping"
    assert saved_states[-1]["message"] == "Fine-mapping failed - no results generated"
    notify.assert_called_once()
