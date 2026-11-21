import asyncio
import pickle
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from fastapi import HTTPException

from src.api.routes import projects as projects_routes
from src.flows import analysis as flow_module
from src.tasks import analysis as analysis_tasks
from src.tasks.analysis import NoSignificantVariantsError, filter_significant_variants
from src.tasks import project as project_tasks
from src.tasks.gene_expression import LdscExitError
from src.tasks.project import HarmonizedFileMissingError
from src.pipeline_stages import runs_stage


def _sumstats(p_values):
    return pd.DataFrame(
        {"p_value": p_values},
        index=pd.Index([f"1:{i}:A:G" for i in range(len(p_values))], name="variant_id"),
    )


def test_filter_keeps_variants_below_threshold_and_records_the_count(monkeypatch, tmp_path):
    record = MagicMock()
    monkeypatch.setattr(analysis_tasks, "record_analysis_event", record)

    significant_df, _ = filter_significant_variants.fn(
        "user", "project", _sumstats([1e-9, 1e-3]), str(tmp_path)
    )

    assert list(significant_df.index) == ["1:0:A:G"]
    record.assert_called_once_with(
        "user", "project", "Filtering", "1 of 2 variants pass p < 5e-08",
        details={"significant_variants": 1, "total_variants": 2, "p_value_threshold": 5e-8},
    )


def test_filter_reports_p_value_range_when_nothing_is_significant(tmp_path):
    sumstats = _sumstats([3.2e-7, 5e-7, 2e-6, 8e-6, 0.5, None])

    with pytest.raises(NoSignificantVariantsError) as exc:
        filter_significant_variants.fn("user", "project", sumstats, str(tmp_path))

    assert str(exc.value) == (
        "No variants reach p < 5e-08. Most significant: 1:0:A:G (p = 3.2e-07). "
        "Variants passing p < 1e-06: 2; p < 1e-05: 4; "
        "rerun with a higher p_value_threshold to include them."
    )
    assert exc.value.as_detail() == {
        "error_type": "no_significant_variants",
        "message": str(exc.value),
        "p_value_threshold": 5e-8,
        "min_p_value": 3.2e-7,
        "min_p_variant": "1:0:A:G",
        "variants_below_1e6": 2,
        "variants_below_1e5": 4,
    }


def test_filter_does_not_suggest_rerun_when_nothing_passes_the_maximum_threshold(tmp_path):
    with pytest.raises(NoSignificantVariantsError) as exc:
        filter_significant_variants.fn(
            "user", "project", _sumstats([0.01, 0.5]), str(tmp_path), p_threshold=1e-5
        )

    assert str(exc.value) == "No variants reach p < 1e-05. Most significant: 1:0:A:G (p = 0.01)."


@pytest.mark.parametrize("results, recorded", [([pd.DataFrame({"cs": [1]})], False), ([], True)])
def test_finemap_batch_warns_only_when_it_fails(monkeypatch, results, recorded):
    record = MagicMock()
    monkeypatch.setattr(analysis_tasks, "record_analysis_event", record)
    monkeypatch.setattr(analysis_tasks, "_finemap_region_batch", lambda _batch_data: results)
    params = {"user_id": "user", "project_id": "project", "batch_number": 1, "batch_count": 3}

    analysis_tasks.finemap_region_batch_worker.fn((["r1", "r2"], "batch_0", "/sumstats", params))

    assert record.called == recorded
    if recorded:
        assert record.call_args.args[3] == "Batch 1/3 failed: none of its 2 regions could be fine-mapped"


def test_cojo_runs_only_on_chromosomes_present_in_the_input(monkeypatch, tmp_path):
    record = MagicMock()
    monkeypatch.setattr(analysis_tasks, "record_analysis_event", record)
    monkeypatch.setattr(analysis_tasks, "get_shared_temp_dir", lambda prefix: str(tmp_path))
    monkeypatch.setattr(analysis_tasks.os.path, "exists", lambda _path: True)
    gcta_chromosomes = []

    def gcta(cmd, **_kwargs):
        chrom = cmd[cmd.index("--chr") + 1]
        gcta_chromosomes.append(chrom)
        prefix = cmd[cmd.index("--out") + 1]
        pd.DataFrame({"SNP": ["7:100:A:G"], "p": [1e-9]}).to_csv(f"{prefix}.jma.cojo", sep="\t", index=False)
        return MagicMock(returncode=0)

    monkeypatch.setattr(analysis_tasks.subprocess, "run", gcta)
    significant = pd.DataFrame(
        {"effect_allele": ["G"], "other_allele": ["A"], "effect_allele_frequency": [0.2],
         "beta": [0.1], "standard_error": [0.01], "p_value": [1e-9], "N": [1000]},
        index=pd.Index(["7:100:A:G"], name="variant_id"),
    )

    analysis_tasks.run_cojo_per_chromosome.fn("user", "project", significant, "/plink", str(tmp_path))

    assert gcta_chromosomes == ["7"]
    assert [call.kwargs.get("level", "info") for call in record.call_args_list] == ["info"]


def test_no_significant_variants_error_survives_pickling():
    error = NoSignificantVariantsError(
        "No variants reach p < 5e-08.", p_value_threshold=5e-8, min_p_value=1e-6,
        min_p_variant="1:0:A:G", variants_below_1e6=0, variants_below_1e5=3,
    )

    assert pickle.loads(pickle.dumps(error)).as_detail() == error.as_detail()


@pytest.mark.parametrize("value", ["5e-8", "1e-5", 1e-6])
def test_p_value_threshold_accepts_valid_values(value):
    assert projects_routes._parse_p_value_threshold(value) == float(value)


@pytest.mark.parametrize("value", [0, -1e-8, 2e-5, "", "abc", float("nan")])
def test_p_value_threshold_rejects_invalid_values(value):
    with pytest.raises(HTTPException) as exc:
        projects_routes._parse_p_value_threshold(value)
    assert exc.value.status_code == 400


def test_route_passes_p_value_threshold_to_project_and_pipeline(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    invoke = MagicMock()
    monkeypatch.setattr(projects_routes, "invoke_analysis_pipeline_deployment", invoke)
    projects = MagicMock(**{"create_project.return_value": "project-1"})
    files = MagicMock(**{"get_file_metadata.return_value": {"storage_key": "k", "filename": "f.tsv"}})
    request = MagicMock(form=AsyncMock(return_value={}))

    asyncio.run(projects_routes.post_analysis_pipeline(
        request, project_name="Wrist", population="EUR", max_workers="3",
        is_uploaded="false", gwas_file_input="file-1", maf_threshold="0.01",
        p_value_threshold="1e-6", seed="42", window="2000", L="-1", coverage="0.95",
        min_abs_corr="0.5", batch_size="5", sample_size=None, phenotype="Wrist",
        ref_genome="GRCh38", thread_id=None, current_user_id="user-1",
        projects=projects, files=files, config=MagicMock(), storage=MagicMock(),
        gwas_library=None, current_user_email=None,
    ))

    assert projects.create_project.call_args.kwargs["analysis_parameters"]["p_value_threshold"] == 1e-6
    assert invoke.call_args.kwargs["p_value_threshold"] == 1e-6


def _configure_flow(monkeypatch, immediate_task_factory):
    saved_states = []
    ldsc_task = MagicMock()
    notify = MagicMock()

    monkeypatch.setattr(flow_module, "get_project_analysis_path_task", immediate_task_factory(lambda *_: "/out"))
    monkeypatch.setattr(flow_module, "harmonize_sumstats_with_nextflow", immediate_task_factory(lambda *_, **__: "/out/harmonized.tsv.gz"))
    monkeypatch.setattr(flow_module, "run_combined_ldsc_tissue_analysis", ldsc_task)
    monkeypatch.setattr(flow_module, "save_analysis_state_task", immediate_task_factory(lambda _u, _p, state: saved_states.append(state) or True))
    monkeypatch.setattr(flow_module, "send_pipeline_notification", notify)
    monkeypatch.setattr(flow_module.Config, "from_env", MagicMock(return_value=MagicMock()))
    return saved_states, ldsc_task, notify


def _run_flow(p_value_threshold=5e-8):
    return flow_module.analysis_pipeline_flow.fn(
        "user-1", "project-1", p_value_threshold=p_value_threshold,
        user_email="user@example.com", project_name="Wrist",
    )


def test_no_significant_variants_fails_at_filtering_without_starting_ldsc(
    monkeypatch, immediate_task_factory
):
    saved_states, ldsc_task, notify = _configure_flow(monkeypatch, immediate_task_factory)
    error = NoSignificantVariantsError(
        "No variants reach p < 1e-06.", p_value_threshold=1e-6, min_p_value=None,
        min_p_variant=None, variants_below_1e6=0, variants_below_1e5=0,
    )

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
        "flow_run_id": None,
        "resumable_from": [],
    }
    notify.assert_called_once_with(
        "user@example.com", "Wrist", "project-1",
        success=False, detail="No variants reach p < 1e-06.", restarted=False,
    )
    ldsc_task.submit.assert_not_called()


def test_unexpected_failure_keeps_the_stage_it_happened_in(monkeypatch, immediate_task_factory):
    saved_states, ldsc_task, notify = _configure_flow(monkeypatch, immediate_task_factory)
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
        "message": "Analysis failed in the COJO step",
        "flow_run_id": None,
        "resumable_from": ["filtering"],
    }
    assert notify.call_args.kwargs["detail"] == "Analysis failed in the COJO step"
    ldsc_task.submit.assert_called_once_with("/out/harmonized.tsv.gz", "/out", "project-1", "user-1")


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
    assert saved_states[-1]["resumable_from"] == ["filtering"]
    notify.assert_called_once()


def test_opentargets_returning_nothing_fails_without_starting_ldsc(monkeypatch, immediate_task_factory):
    saved_states, ldsc_task, _ = _configure_flow(monkeypatch, immediate_task_factory)
    monkeypatch.setattr(flow_module, "CredibleSetsHandler", MagicMock(**{"return_value.study_has_credible_sets.return_value": True}))
    monkeypatch.setattr(flow_module, "get_results_from_opentargets_task", immediate_task_factory(lambda *_, **__: None))

    with pytest.raises(RuntimeError):
        flow_module.analysis_pipeline_flow.fn("user-1", "project-1", opentargets_study_id="FINNGEN_R12_X")

    assert saved_states[-1]["status"] == "Failed"
    assert saved_states[-1]["stage"] == "OpenTargets_credible_sets"
    ldsc_task.submit.assert_not_called()


def test_opentargets_study_starts_ldsc_and_skips_filtering(monkeypatch, immediate_task_factory):
    saved_states, ldsc_task, notify = _configure_flow(monkeypatch, immediate_task_factory)
    ldsc_task.submit.return_value.result.return_value = {"analysis_run_id": "run-1"}
    filter_task = MagicMock()
    monkeypatch.setattr(flow_module, "filter_significant_variants", filter_task)
    monkeypatch.setattr(flow_module, "CredibleSetsHandler", MagicMock(**{"return_value.study_has_credible_sets.return_value": True}))
    monkeypatch.setattr(flow_module, "get_results_from_opentargets_task", immediate_task_factory(lambda *_, **__: pd.DataFrame({"PIP": [0.9]})))
    monkeypatch.setattr(flow_module, "create_analysis_result_task", immediate_task_factory(lambda *_: "/out/analysis_results.csv"))

    result = flow_module.analysis_pipeline_flow.fn(
        "user-1", "project-1", opentargets_study_id="FINNGEN_R12_X",
        user_email="user@example.com", project_name="Wrist",
    )

    assert result["results_file"] == "/out/analysis_results.csv"
    assert saved_states[-1]["status"] == "Completed"
    ldsc_task.submit.assert_called_once()
    filter_task.submit.assert_not_called()
    notify.assert_called_once_with("user@example.com", "Wrist", "project-1", success=True, restarted=False)


def _raise_no_significant_variants(*_args, **_kwargs):
    raise NoSignificantVariantsError(
        "No variants reach p < 1e-06.", p_value_threshold=1e-6, min_p_value=1e-6,
        min_p_variant="1:1:A:G", variants_below_1e6=0, variants_below_1e5=2,
    )


def test_full_run_records_the_harmonized_file_name(monkeypatch, immediate_task_factory):
    saved_states, _, _ = _configure_flow(monkeypatch, immediate_task_factory)
    monkeypatch.setattr(flow_module, "filter_significant_variants", immediate_task_factory(_raise_no_significant_variants))

    with pytest.raises(NoSignificantVariantsError):
        _run_flow()

    assert {"artifacts": {"harmonized_file": "harmonized.tsv.gz"}, "flow_run_id": None} in saved_states


def _configure_resume(monkeypatch, immediate_task_factory, ensure=lambda *_: "/out/harmonized.tsv.gz"):
    saved_states, ldsc_task, notify = _configure_flow(monkeypatch, immediate_task_factory)
    harmonize = MagicMock()
    reset = immediate_task_factory(lambda *_: None)
    monkeypatch.setattr(flow_module, "harmonize_sumstats_with_nextflow", harmonize)
    monkeypatch.setattr(flow_module, "ensure_harmonized_file_task", immediate_task_factory(ensure))
    monkeypatch.setattr(flow_module, "reset_analysis_outputs_task", reset)
    return saved_states, ldsc_task, notify, harmonize, reset


def _resume(resume_from, artifacts, **kwargs):
    return flow_module.analysis_pipeline_flow.fn(
        "user-1", "project-1", resume_from=resume_from, artifacts=artifacts,
        user_email="user@example.com", project_name="Wrist", **kwargs,
    )


def _configure_successful_pipeline(monkeypatch, immediate_task_factory):
    saved_states, ldsc_task, notify, harmonize, _ = _configure_resume(monkeypatch, immediate_task_factory)
    harmonize.submit.return_value.result.return_value = "/out/harmonized.tsv.gz"
    ldsc_task.submit.return_value.result.return_value = {"analysis_run_id": "run-2"}
    fine_mapped = pd.DataFrame({"PIP": [0.9], "cs": [1], "credible_set": [1]})
    tasks = {
        "harmonization": harmonize,
        "filtering": MagicMock(**{"submit.return_value.result.return_value": (_sumstats([1e-9]), None)}),
        "cojo": MagicMock(**{"submit.return_value.result.return_value": (_sumstats([1e-9]), None)}),
        "fine_mapping": MagicMock(**{"map.return_value": [MagicMock(**{"result.return_value": [fine_mapped]})]}),
        "ldsc": ldsc_task,
    }
    monkeypatch.setattr(flow_module, "filter_significant_variants", tasks["filtering"])
    monkeypatch.setattr(flow_module, "run_cojo_per_chromosome", tasks["cojo"])
    monkeypatch.setattr(flow_module, "finemap_region_batch_worker", tasks["fine_mapping"])
    monkeypatch.setattr(flow_module, "create_region_batches", lambda *_, **__: [["region"]])
    monkeypatch.setattr(flow_module, "save_sumstats_for_workers", lambda *_: "/out/sumstats.pkl")
    monkeypatch.setattr(flow_module, "cleanup_sumstats_file", MagicMock())
    monkeypatch.setattr(flow_module, "record_analysis_event_task", MagicMock())
    monkeypatch.setattr(flow_module, "create_analysis_result_task", immediate_task_factory(lambda *_: "/out/analysis_results.csv"))
    return saved_states, tasks, notify


@pytest.mark.parametrize("resume_from", [None, "filtering", "ldsc"])
def test_flow_runs_exactly_the_stages_from_the_resume_point(monkeypatch, immediate_task_factory, resume_from):
    saved_states, tasks, notify = _configure_successful_pipeline(monkeypatch, immediate_task_factory)
    artifacts = {"harmonized_file": "harmonized.tsv.gz", "credible_sets": True}

    result = flow_module.analysis_pipeline_flow.fn(
        "user-1", "project-1", resume_from=resume_from, artifacts=artifacts,
        user_email="user@example.com", project_name="Wrist",
    )

    for key, task in tasks.items():
        assert (task.submit.called or task.map.called) == runs_stage(key, resume_from), key
    assert set(result) == set(flow_module.RESULT_KEYS)
    assert saved_states[-1]["status"] == "Completed"
    assert notify.call_args.kwargs["restarted"] == bool(resume_from)


def test_ldsc_resume_that_fails_again_stays_resumable(monkeypatch, immediate_task_factory):
    saved_states, ldsc_task, _, _, _ = _configure_resume(monkeypatch, immediate_task_factory)
    ldsc_task.submit.return_value.result.side_effect = LdscExitError("ldsc failed")

    with pytest.raises(RuntimeError):
        _resume("ldsc", {"harmonized_file": "harmonized.tsv.gz", "credible_sets": True})

    assert [state.get("stage") for state in saved_states] == ["LDSC_Tissue_Analysis", "LDSC_Analysis"]
    assert saved_states[-1]["resumable_from"] == ["ldsc"]


def test_filtering_resume_clears_results_and_uses_the_new_threshold(monkeypatch, immediate_task_factory):
    saved_states, _, _, harmonize, reset = _configure_resume(monkeypatch, immediate_task_factory)
    filter_task = immediate_task_factory(_raise_no_significant_variants)
    ot_handler = MagicMock()
    monkeypatch.setattr(flow_module, "filter_significant_variants", filter_task)
    monkeypatch.setattr(flow_module, "CredibleSetsHandler", ot_handler)

    with pytest.raises(NoSignificantVariantsError):
        _resume("filtering", {"harmonized_file": "harmonized.tsv.gz"},
                p_value_threshold=1e-6, opentargets_study_id="GCST1")

    assert reset.calls[0][0] == ("user-1", "project-1", ["credible_sets", "analysis_results"])
    assert filter_task.calls[0][1] == {"p_threshold": 1e-6}
    assert saved_states[-1]["resumable_from"] == ["filtering"]
    harmonize.submit.assert_not_called()
    ot_handler.assert_not_called()


def test_resume_without_the_harmonized_file_is_no_longer_resumable(monkeypatch, immediate_task_factory):
    def missing(*_args):
        raise HarmonizedFileMissingError("gone")

    saved_states, ldsc_task, _, _, reset = _configure_resume(monkeypatch, immediate_task_factory, ensure=missing)

    with pytest.raises(HarmonizedFileMissingError):
        _resume("ldsc", {"harmonized_file": "harmonized.tsv.gz", "credible_sets": True})

    assert saved_states[-1]["status"] == "Failed"
    assert saved_states[-1]["resumable_from"] == []
    assert saved_states[-1]["artifacts"] == {"credible_sets": True}
    assert reset.calls == []
    ldsc_task.submit.assert_not_called()


def _ensure_harmonized_file(monkeypatch, local, downloaded):
    storage = MagicMock(**{"download_file.return_value": downloaded})
    monkeypatch.setattr(project_tasks, "get_deps", lambda: {"storage": storage})
    monkeypatch.setattr(project_tasks.os.path, "exists", lambda _path: local)
    path = project_tasks.ensure_harmonized_file_task.fn("user-1", "project-1", "/out", "harmonized.tsv.gz")
    return path, storage


@pytest.mark.parametrize("local", [True, False])
def test_ensure_harmonized_file_uses_the_local_copy_or_downloads_it(monkeypatch, local):
    path, storage = _ensure_harmonized_file(monkeypatch, local=local, downloaded=True)

    assert path == "/out/harmonized.tsv.gz"
    assert storage.download_file.called == (not local)


def test_ensure_harmonized_file_fails_when_both_copies_are_gone(monkeypatch):
    with pytest.raises(HarmonizedFileMissingError):
        _ensure_harmonized_file(monkeypatch, local=False, downloaded=False)


def test_superseded_run_stops_without_writing_a_failed_state(monkeypatch, immediate_task_factory):
    saved_states, _, notify = _configure_flow(monkeypatch, immediate_task_factory)
    monkeypatch.setattr(
        flow_module, "save_analysis_state_task",
        immediate_task_factory(lambda _u, _p, state: saved_states.append(state) or state.get("stage") != "Filtering"),
    )

    with pytest.raises(flow_module.RunSupersededError):
        _run_flow()

    assert saved_states[-1]["stage"] == "Filtering"
    notify.assert_not_called()
