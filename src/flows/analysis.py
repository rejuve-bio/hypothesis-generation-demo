import os
import shutil as _shutil

import pandas as pd
from datetime import datetime, timezone
from loguru import logger
from prefect import flow
from prefect.runtime import flow_run as _prefect_flow_run
from prefect_dask import DaskTaskRunner
from src.pipeline_stages import cleanup_targets, get_stage, resumable_from, runs_stage
from src.utils import analysis_stage_label, get_deps
from src.services.mail import send_pipeline_notification

from src.config import Config
from src.tasks import (
    get_project_analysis_path_task,
    ensure_harmonized_file_task,
    reset_analysis_outputs_task,
    prepare_gwas_file_task,
    harmonize_sumstats_with_nextflow,
    run_combined_ldsc_tissue_analysis,
    filter_significant_variants,
    run_cojo_per_chromosome,
    create_region_batches,
    save_sumstats_for_workers,
    finemap_region_batch_worker,
    cleanup_sumstats_file,
    save_analysis_state_task,
    record_analysis_event_task,
    create_analysis_result_task,
    get_results_from_opentargets_task,
)
from src.tasks.analysis import NoSignificantVariantsError, count_credible_sets
from src.tasks.gene_expression import LdscExitError
from src.tasks.project import HarmonizedFileMissingError
from src.db.credible_sets_handler import CredibleSetsHandler


def _ldsc_failure_cause(error):
    """Short user-facing cause of an LDSC failure; the raw error stays in the logs."""
    if isinstance(error, FileNotFoundError):
        return "LDSC reference files are missing on the server"
    if isinstance(error, LdscExitError):
        return "LDSC exited with an error"
    return "Unexpected error during LDSC"


RESULT_KEYS = ("results_file", "total_variants", "high_pip_variants", "total_credible_sets")


class RunSupersededError(RuntimeError):
    """The project's analysis state now belongs to another run, so this one stops."""


@flow(log_prints=True,
    persist_result=False,
    task_runner=DaskTaskRunner(address=os.getenv("DASK_ADDRESS"))
)
def analysis_pipeline_flow(user_id, project_id, gwas_file_path=None, ref_genome="GRCh38",
                           population="EUR", batch_size=5, max_workers=3,
                           maf_threshold=0.01, p_value_threshold=5e-8,
                           seed=42, window=2000, L=-1,
                           coverage=0.95, min_abs_corr=0.5, sample_size=None,
                           user_email: str | None = None, project_name: str | None = None,
                           file_metadata_id=None, file_needs_processing=False,
                           file_storage_key=None, file_id_new=None,
                           file_source_minio_path=None, file_source_download_url=None,
                           file_minio_cache_key=None, file_gwas_library_id=None,
                           opentargets_study_id=None, resume_from=None, artifacts=None):
    """
    Complete analysis pipeline flow using Prefect for orchestration
    but multiprocessing for fine-mapping batches (R safety).

    With resume_from, stages before it are skipped and their recorded artifacts reused.
    """

    logger.info(f"[PIPELINE] Starting Prefect analysis pipeline with multiprocessing fine-mapping")
    logger.info(f"[PIPELINE] Project: {project_id}, User: {user_id}")
    logger.info(f"[PIPELINE] File: {gwas_file_path}")
    logger.info(f"[PIPELINE] Batch size: {batch_size} regions per worker process")
    logger.info(f"[PIPELINE] Max workers: {max_workers}")
    logger.info(f"[PIPELINE] Parameters: maf={maf_threshold}, p={p_value_threshold}, seed={seed}, window={window}kb, L={L}, coverage={coverage}, min_abs_corr={min_abs_corr}, N={sample_size}")

    flow_run_id = _prefect_flow_run.id
    started_at = datetime.now(timezone.utc).isoformat()
    artifacts = dict(artifacts or {})
    last_state = {}

    def save_state(state):
        nonlocal last_state
        state = {**state, "flow_run_id": flow_run_id}
        if state.get("status") == "Failed":
            state["resumable_from"] = resumable_from(
                {**state, "artifacts": artifacts, "resumed_from": resume_from}
            )
        save_or_stop(state)
        last_state = state

    def record_artifacts(**recorded):
        artifacts.update(recorded)
        save_or_stop({"artifacts": dict(artifacts), "flow_run_id": flow_run_id})

    def save_or_stop(state):
        if not save_analysis_state_task.submit(user_id, project_id, state).result():
            raise RunSupersededError(f"Flow run {flow_run_id} no longer owns project {project_id}")

    def enter_stage(key, state):
        """Save a stage's Running state; a resumed run already saved it for its first stage."""
        if key != resume_from:
            save_state({"status": "Running", **state})

    def start_ldsc_tissue_analysis(harmonized_file, output_dir):
        logger.info("[PIPELINE] Starting LDSC + tissue analysis in background")
        return run_combined_ldsc_tissue_analysis.submit(
            harmonized_file, output_dir, project_id, user_id
        )

    def harmonize(output_dir):
        nonlocal gwas_file_path
        if file_needs_processing:
            logger.info(f"[PIPELINE] Stage 0: preparing GWAS file")
            gwas_file_path = prepare_gwas_file_task.submit(
                user_id, project_id, file_metadata_id, gwas_file_path,
                storage_key=file_storage_key,
                file_id_new=file_id_new,
                source_minio_path=file_source_minio_path,
                source_download_url=file_source_download_url,
                minio_cache_key=file_minio_cache_key,
                gwas_library_id=file_gwas_library_id,
                output_dir=output_dir,
            ).result()
            logger.info(f"[PIPELINE] Stage 0 complete: file ready at {gwas_file_path}")

        # Save initial analysis state
        save_state({
            "status": "Running",
            "stage": "Harmonization",
            "progress": 10,
            "message": "Harmonizing summary statistics",
            "started_at": started_at,
        })

        logger.info(f"[PIPELINE] Stage 1: Harmonization (Nextflow, or pass-through if opentargets_study_id is confirmed in OpenTargets' own tables)")
        harmonized_file_result = harmonize_sumstats_with_nextflow.submit(
            gwas_file_path, output_dir, ref_genome=ref_genome, sample_size=sample_size,
            user_id=user_id, project_id=project_id, opentargets_study_id=opentargets_study_id
        ).result()

        # harmonize_sumstats_with_nextflow now returns just the file path.
        # Keep tuple handling for backward compatibility with older task versions.
        if isinstance(harmonized_file_result, tuple):
            harmonized_file = harmonized_file_result[1]
        else:
            harmonized_file = harmonized_file_result
        record_artifacts(harmonized_file=os.path.basename(harmonized_file))

        # Clean up the raw GWAS input file. For direct uploads it lives in its own
        # data/uploads/<user_id>/ dir (safe to rmtree). But cache/library downloads
        # (source_minio_path / source_download_url — e.g. OT-confirmed FinnGen
        # studies) land the raw file directly in output_dir alongside the
        # harmonized/remapped output, so rmtree-ing its parent would delete that
        # output too. Only rmtree when the raw file's directory isn't output_dir
        # (or the harmonized output itself); otherwise just remove the raw file.
        if (
            gwas_file_path
            and os.path.exists(gwas_file_path)
            and os.path.abspath(gwas_file_path) != os.path.abspath(harmonized_file)
        ):
            try:
                parent_dir = os.path.dirname(gwas_file_path)
                if os.path.abspath(parent_dir) == os.path.abspath(output_dir):
                    os.remove(gwas_file_path)
                    logger.info(f"[PIPELINE] Cleaned up raw GWAS input file: {gwas_file_path}")
                else:
                    _shutil.rmtree(parent_dir, ignore_errors=True)
                    logger.info(f"[PIPELINE] Cleaned up raw GWAS temp dir: {parent_dir}")
            except Exception as _cleanup_e:
                logger.warning(f"[PIPELINE] Could not clean up {gwas_file_path}: {_cleanup_e}")
        return harmonized_file

    def prepare_resume(output_dir):
        stage = get_stage(resume_from)
        logger.info(f"[PIPELINE] Resuming from {resume_from}")
        save_state({"status": "Running", **stage["resume_state"], "started_at": started_at})
        try:
            harmonized_file = ensure_harmonized_file_task.submit(
                user_id, project_id, output_dir, artifacts["harmonized_file"]
            ).result()
        except HarmonizedFileMissingError:
            artifacts.pop("harmonized_file")
            save_state({
                **last_state,
                "status": "Failed",
                "message": "Intermediate files are no longer available, start a new analysis",
                "artifacts": artifacts,
            })
            raise
        targets = cleanup_targets(resume_from)
        reset_analysis_outputs_task.submit(user_id, project_id, targets).result()
        for target in targets:
            artifacts.pop(target, None)
        return harmonized_file

    def find_credible_sets(output_dir, harmonized_file):
        """Credible sets from OpenTargets or fine-mapping; returns (summary, LDSC future)."""
        ldsc_tissue_future = None

        # Check whether OpenTargets already has full study-level credible sets.
        # If so, skip filtering + COJO + SuSiE fine-mapping entirely and use OT
        # results directly — no need to load/filter the harmonized sumstats at all.
        # A resumed run keeps the source chosen by the original run.
        has_ot_results = False
        if opentargets_study_id and not resume_from:
            _ot_check = CredibleSetsHandler()
            try:
                has_ot_results = _ot_check.study_has_credible_sets(opentargets_study_id)
            finally:
                _ot_check.close()

        cojo_output_path = None
        region_batches = []
        successful_batches = 0
        combined_results = None

        if has_ot_results:
            logger.info(
                f"[PIPELINE] OpenTargets credible sets found for study "
                f"{opentargets_study_id} — skipping filtering, COJO, and fine-mapping"
            )

            ot_state = {
                "status": "Running",
                "stage": "OpenTargets_credible_sets",
                "progress": 70,
                "message": (
                    f"Using OpenTargets fine-mapped credible sets for {opentargets_study_id}; "
                    "COJO and fine-mapping skipped"
                ),
                "started_at": started_at,
            }
            save_state(ot_state)
            record_artifacts(credible_sets_source="opentargets")

            combined_results = get_results_from_opentargets_task.submit(
                user_id, project_id, opentargets_study_id, coverage=coverage,
                harmonized_file=harmonized_file
            ).result()
            if combined_results is not None and len(combined_results) > 0:
                successful_batches = 1
                ldsc_tissue_future = start_ldsc_tissue_analysis(harmonized_file, output_dir)
        else:
            record_artifacts(credible_sets_source="finemapping")

            if runs_stage("filtering", resume_from):
                enter_stage("filtering", {
                    "stage": "Filtering",
                    "progress": 30,
                    "message": "Harmonization completed, filtering significant variants",
                    "started_at": started_at,
                })

                logger.info(f"[PIPELINE] Stage 2: Loading and filtering variants")
                try:
                    significant_df_result = filter_significant_variants.submit(
                        user_id, project_id, harmonized_file, output_dir, p_threshold=p_value_threshold
                    ).result()
                except NoSignificantVariantsError as e:
                    save_state({
                        "status": "Failed",
                        "stage": "Filtering",
                        "progress": 30,
                        "message": str(e),
                        "error_detail": e.as_detail(),
                    })
                    raise

                # Start LDSC only once the study has significant variants to fine-map
                ldsc_tissue_future = start_ldsc_tissue_analysis(harmonized_file, output_dir)

                # Extract the actual DataFrame
                if isinstance(significant_df_result, tuple):
                    significant_df, sig_output_path = significant_df_result
                else:
                    significant_df = significant_df_result
                    sig_output_path = None

            if runs_stage("cojo", resume_from):
                enter_stage("cojo", {
                    "stage": "Cojo",
                    "progress": 50,
                    "message": "Filtering completed, running COJO analysis"
                })

                logger.info(f"[PIPELINE] Stage 3: COJO analysis")

                config = Config.from_env()
                plink_dir = config.plink_dir_38
                cojo_result = run_cojo_per_chromosome.submit(user_id, project_id, significant_df, plink_dir, output_dir, maf_threshold=maf_threshold, population=population, ref_genome=ref_genome).result()

                # Extract the actual DataFrame
                if isinstance(cojo_result, tuple):
                    cojo_results, cojo_output_path = cojo_result
                else:
                    cojo_results = cojo_result
                    cojo_output_path = None

                # Cleanup
                if sig_output_path and os.path.exists(sig_output_path):
                    try:
                        os.remove(sig_output_path)
                        logger.info(f"[PIPELINE] Cleaned up temporary file: {sig_output_path}")
                    except Exception as cleanup_e:
                        logger.warning(f"[PIPELINE] Could not cleanup {sig_output_path}: {cleanup_e}")

            enter_stage("fine_mapping", {
                "stage": "Fine_mapping",
                "progress": 70,
                "message": "COJO analysis completed, starting fine-mapping"
            })

            logger.info(f"[PIPELINE] Stage 4: Multiprocessing fine-mapping")
            logger.info(f"[PIPELINE] Processing {len(cojo_results)} regions with {batch_size} regions per batch")

            region_batches = create_region_batches(cojo_results, batch_size=batch_size)
            logger.info(f"[PIPELINE] Created {len(region_batches)} batches for {max_workers} worker processes")

            sumstats_temp_file = save_sumstats_for_workers(significant_df, output_dir)

            # Prepare batch data for multiprocessing
            batch_data_list = []
            for i, batch in enumerate(region_batches):
                batch_data = (batch, f"batch_{i}", sumstats_temp_file, {
                    'user_id': user_id,
                    'project_id': project_id,
                    'batch_number': i + 1,
                    'batch_count': len(region_batches),
                    'finemap_params': {
                        'seed': seed,
                        'window': window,
                        'L': L,
                        'coverage': coverage,
                        'min_abs_corr': min_abs_corr,
                        'population': population,
                        'ref_genome': ref_genome,
                        'maf_threshold': maf_threshold,
                        'plink_dir': plink_dir
                    }
                })
                batch_data_list.append(batch_data)

            logger.info(f"[PIPELINE] Submitting {len(batch_data_list)} batches to Dask Cluster...")

            all_results = []

            try:
                futures = finemap_region_batch_worker.map(batch_data_list)

                batch_results_list = [f.result() for f in futures]

                for i, batch_results in enumerate(batch_results_list):
                    if batch_results and len(batch_results) > 0:
                        all_results.extend(batch_results)
                        successful_batches += 1
                        logger.info(f"[PIPELINE] Batch {i} completed with {len(batch_results)} regions")
                    else:
                        logger.warning(f"[PIPELINE] Batch {i} failed or returned no results")

            except Exception as e:
                logger.error(f"[PIPELINE] Error in Dask mapping: {str(e)}")
                raise
            finally:
                cleanup_sumstats_file.submit(sumstats_temp_file)

            if all_results:
                logger.info(f"[PIPELINE] Combining results from {successful_batches} successful batches")
                combined_results = pd.concat(all_results, ignore_index=True)
                try:
                    credible_sets = count_credible_sets(all_results)
                    high_pip = int((combined_results["PIP"] > 0.5).sum())
                    empty_regions = len(cojo_results) - sum(
                        count_credible_sets([result]) > 0 for result in all_results
                    )
                    message = (
                        f"Fine-mapping found {credible_sets} credible sets, "
                        f"{high_pip} variants with PIP > 0.5"
                    )
                    if empty_regions:
                        message += f"; {empty_regions} of {len(cojo_results)} regions produced no credible set"
                    record_analysis_event_task.submit(
                        user_id, project_id, "Fine_mapping", message,
                        details={
                            "credible_sets": credible_sets,
                            "high_pip_variants": high_pip,
                            "regions": len(cojo_results),
                            "regions_without_credible_set": empty_regions,
                        },
                    ).result()
                except Exception as summary_e:
                    logger.warning(f"[PIPELINE] Could not record fine-mapping summary: {summary_e}")

        if combined_results is None or len(combined_results) == 0:
            if has_ot_results:
                logger.error("[PIPELINE] No OpenTargets credible sets could be loaded")
                save_state({
                    "status": "Failed",
                    "stage": "OpenTargets_credible_sets",
                    "progress": 70,
                    "message": "No OpenTargets credible sets could be loaded for this study",
                })
                raise RuntimeError("No OpenTargets credible sets loaded")
            logger.error("[PIPELINE]  No fine-mapping results generated")
            # Save failed state for fine-mapping
            failed_finemap_state = {
                "status": "Failed",
                "stage": "Fine_mapping",
                "progress": 70,
                "message": "Fine-mapping failed - no results generated",
            }
            save_state(failed_finemap_state)
            raise RuntimeError("All fine-mapping batches failed")

        # Cleanup
        if cojo_output_path and os.path.exists(cojo_output_path):
            try:
                os.remove(cojo_output_path)
                logger.info(f"[PIPELINE] Cleaned up temporary file: {cojo_output_path}")
            except Exception as cleanup_e:
                logger.warning(f"[PIPELINE] Could not cleanup {cojo_output_path}: {cleanup_e}")

        # Save results using Prefect tasks
        results_file = create_analysis_result_task.submit(user_id, project_id, combined_results, output_dir).result()
        record_artifacts(credible_sets=True)
        logger.info(f"[PIPELINE] - Successful batches: {successful_batches}/{len(region_batches)}")

        summary = {
            "results_file": results_file,
            "total_variants": len(combined_results),
            "high_pip_variants": len(combined_results[combined_results['PIP'] > 0.5]),
            "total_credible_sets": combined_results.get('credible_set', pd.Series([0])).max(),
        }
        return summary, ldsc_tissue_future

    def finish(ldsc_tissue_future):
        logger.info(f"[PIPELINE] Stage 5: Waiting for LDSC + Tissue Analysis to complete")
        enter_stage("ldsc", {
            "stage": "LDSC_Tissue_Analysis",
            "progress": 85,
            "message": "Waiting for LDSC tissue analysis to finish"
        })

        try:
            # Wait for the parallel LDSC + tissue analysis task to complete
            ldsc_tissue_result = ldsc_tissue_future.result()
            logger.info(f"[PIPELINE] LDSC + tissue analysis completed successfully!")
            logger.info(f"[PIPELINE] - Analysis run ID: {ldsc_tissue_result['analysis_run_id']}")
        except Exception as ldsc_e:
            logger.error(f"[PIPELINE] LDSC + tissue analysis failed: {str(ldsc_e)}")
            # LDSC results are required for tissue selection and enrichment - fail the pipeline
            save_state({
                "status": "Failed",
                "stage": "LDSC_Analysis",
                "progress": 90,
                "message": "LDSC tissue analysis failed, so tissue-specific enrichment is not available",
                "error_detail": {"error": _ldsc_failure_cause(ldsc_e)},
            })
            raise RuntimeError(f"LDSC tissue analysis failed - required for enrichment: {str(ldsc_e)}")

        save_state({
            "status": "Completed",
            "progress": 100,
            "message": "Analysis completed successfully",
            "ldsc_status": "completed"
        })
        send_pipeline_notification(
            user_email, project_name, project_id, success=True, restarted=bool(resume_from)
        )

    try:
        # Get project-specific output directory (using Prefect task)
        output_dir = get_project_analysis_path_task.submit(user_id, project_id).result()
        logger.info(f"[PIPELINE] Using output directory: {output_dir}")

        if runs_stage("harmonization", resume_from):
            harmonized_file = harmonize(output_dir)
        else:
            harmonized_file = prepare_resume(output_dir)

        summary = dict.fromkeys(RESULT_KEYS)
        ldsc_tissue_future = None
        if runs_stage("fine_mapping", resume_from):
            summary, ldsc_tissue_future = find_credible_sets(output_dir, harmonized_file)

        if runs_stage("ldsc", resume_from):
            finish(ldsc_tissue_future or start_ldsc_tissue_analysis(harmonized_file, output_dir))

        logger.info(f"[PIPELINE] Analysis completed successfully: {summary}")
        return summary

    except RunSupersededError as e:
        logger.warning(f"[PIPELINE] Stopping: {e}")
        raise
    except Exception as e:
        logger.error(f"[PIPELINE]  Analysis pipeline failed: {str(e)}")
        # Keep a specific failed state already saved by the failing stage
        if last_state.get("status") == "Failed":
            failure_message = last_state["message"]
        else:
            stage = last_state.get("stage")
            failure_message = (
                f"Analysis failed in the {analysis_stage_label(stage)} step" if stage
                else "Analysis failed"
            )
            try:
                save_state({
                    "status": "Failed",
                    "stage": stage or "Unknown",
                    "progress": last_state.get("progress", 0),
                    "message": failure_message,
                })
            except Exception as state_e:
                logger.error(f"[PIPELINE] Failed to save error state: {str(state_e)}")
        send_pipeline_notification(
            user_email, project_name, project_id,
            success=False, detail=failure_message, restarted=bool(resume_from),
        )
        raise
