"""Analysis pipeline stages, in order, and the rules for resuming a failed run from one.

Each entry has:
key: name used by the API (from_stage) and the flow (resume_from).
label: user-facing name.
state_stages: analysis_state stage names written while the stage runs.
needs: artifacts recorded in analysis_state.artifacts that a run starting here requires.
resumable: whether a failed run may restart here.
cleanup (optional): stored outputs deleted before rerunning this stage or an earlier one.
params (optional): request overrides consumed by this stage.
skipped_on_opentargets (optional): the stage does not run when OpenTargets supplied the credible sets.
can_retry (optional): whether the stored failure could succeed on a rerun from here.
resume_state (resumable stages): the Running state a run resumed here starts in.
"""


def _some_variants_can_pass(state):
    detail = state.get("error_detail") or {}
    return detail.get("error_type") != "no_significant_variants" or bool(detail.get("variants_below_1e5"))


PIPELINE_STAGES = [
    {"key": "harmonization", "label": "harmonization",
     "state_stages": {"File_upload", "Harmonization"}, "needs": [], "resumable": False},
    {"key": "filtering", "label": "filtering", "state_stages": {"Filtering"},
     "needs": ["harmonized_file"], "params": ["p_value_threshold"],
     "skipped_on_opentargets": True, "can_retry": _some_variants_can_pass,
     "resume_state": {"stage": "Filtering", "progress": 30, "message": "Filtering significant variants"},
     "resumable": True},
    {"key": "cojo", "label": "COJO", "state_stages": {"Cojo"},
     "needs": ["harmonized_file"], "skipped_on_opentargets": True, "resumable": False},
    {"key": "fine_mapping", "label": "fine-mapping",
     "state_stages": {"Fine_mapping", "OpenTargets_credible_sets"},
     "needs": ["harmonized_file"], "cleanup": ["credible_sets", "analysis_results"],
     "skipped_on_opentargets": True, "resumable": False},
    {"key": "ldsc", "label": "LDSC tissue analysis",
     "state_stages": {"LDSC_Analysis", "LDSC_Tissue_Analysis"},
     "needs": ["harmonized_file", "credible_sets"],
     "resume_state": {"stage": "LDSC_Tissue_Analysis", "progress": 85,
                      "message": "Waiting for LDSC tissue analysis to finish"},
     "resumable": True},
]

STAGE_INDEX = {stage["key"]: index for index, stage in enumerate(PIPELINE_STAGES)}


def get_stage(key):
    return PIPELINE_STAGES[STAGE_INDEX[key]]


def stage_key(state_stage):
    """Registry key of a stored analysis_state stage name, or None."""
    return next(
        (stage["key"] for stage in PIPELINE_STAGES if state_stage in stage["state_stages"]),
        None,
    )


def runs_stage(key, resume_from):
    """Whether a run resumed from resume_from (None for a full run) executes the stage."""
    return resume_from is None or STAGE_INDEX[key] >= STAGE_INDEX[resume_from]


def stages_from(key):
    return PIPELINE_STAGES[STAGE_INDEX[key]:]


def cleanup_targets(key):
    """Stored outputs to delete before rerunning from the stage."""
    return [target for stage in stages_from(key) for target in stage.get("cleanup", [])]


def rerun_params(key):
    """Request overrides allowed when rerunning from the stage."""
    return {param for stage in stages_from(key) for param in stage.get("params", [])}


def _can_start_at(stage, state):
    artifacts = state.get("artifacts") or {}
    if not stage["resumable"] or not all(artifacts.get(need) for need in stage["needs"]):
        return False
    if stage.get("skipped_on_opentargets") and artifacts.get("credible_sets_source") == "opentargets":
        return False
    return stage.get("can_retry", lambda _state: True)(state)


def resumable_from(state):
    """Stages a failed run can be restarted from: the latest startable one up to the failure."""
    if (state or {}).get("status") != "Failed":
        return []
    failed = stage_key(state.get("stage")) or state.get("resumed_from")
    if failed not in STAGE_INDEX:
        return []
    for stage in reversed(PIPELINE_STAGES[:STAGE_INDEX[failed] + 1]):
        if _can_start_at(stage, state):
            return [stage["key"]]
    return []
