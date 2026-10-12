import os
import shutil as _shutil

import pandas as pd
from datetime import datetime, timezone
from loguru import logger
from prefect import flow
from prefect.runtime import flow_run as _prefect_flow_run
from prefect_dask import DaskTaskRunner
from src.utils import get_deps
from src.services.mail import send_pipeline_notification

from src.config import Config
from src.tasks import (
    get_project_analysis_path_task,
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
    create_analysis_result_task,
    get_results_from_opentargets_task,
)
from src.db.credible_sets_handler import CredibleSetsHandler



@flow(log_prints=True,
    persist_result=False,
    task_runner=DaskTaskRunner(address=os.getenv("DASK_ADDRESS"))
)
def analysis_pipeline_flow(user_id, project_id, gwas_file_path=None, ref_genome="GRCh38",
                           population="EUR", batch_size=5, max_workers=3,
                           maf_threshold=0.01, seed=42, window=2000, L=-1,
                           coverage=0.95, min_abs_corr=0.5, sample_size=None,
                           user_email: str | None = None, project_name: str | None = None,
                           file_metadata_id=None, file_needs_processing=False,
                           file_storage_key=None, file_id_new=None,
                           file_source_minio_path=None, file_source_download_url=None,
                           file_minio_cache_key=None, file_gwas_library_id=None,
                           opentargets_study_id=None):
    """
    Complete analysis pipeline flow using Prefect for orchestration
    but multiprocessing for fine-mapping batches (R safety)
    """

    logger.info(f"[PIPELINE] Starting Prefect analysis pipeline with multiprocessing fine-mapping")
    logger.info(f"[PIPELINE] Project: {project_id}, User: {user_id}")
    logger.info(f"[PIPELINE] File: {gwas_file_path}")
    logger.info(f"[PIPELINE] Batch size: {batch_size} regions per worker process")
    logger.info(f"[PIPELINE] Max workers: {max_workers}")
    logger.info(f"[PIPELINE] Parameters: maf={maf_threshold}, seed={seed}, window={window}kb, L={L}, coverage={coverage}, min_abs_corr={min_abs_corr}, N={sample_size}")

    try:
        # Get project-specific output directory (using Prefect task)
        output_dir = get_project_analysis_path_task.submit(user_id, project_id).result()
        logger.info(f"[PIPELINE] Using output directory: {output_dir}")

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
        initial_state = {
            "status": "Running",
            "stage": "Harmonization",
            "progress": 10,
            "message": "Harmonizing summary statistics (skipped automatically for confirmed OpenTargets studies)",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "flow_run_id": _prefect_flow_run.id,
        }
        save_analysis_state_task.submit(user_id, project_id, initial_state).result()

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

        # Start LDSC + tissue analysis immediately after harmonization (runs in parallel)
        logger.info(f"[PIPELINE] Starting LDSC + tissue analysis in parallel after harmonization")
        ldsc_tissue_future = run_combined_ldsc_tissue_analysis.submit(
            harmonized_file, output_dir, project_id, user_id
        )
        logger.info(f"[PIPELINE] LDSC + tissue analysis started in background")

        # Check whether OpenTargets already has full study-level credible sets.
        # If so, skip filtering + COJO + SuSiE fine-mapping entirely and use OT
        # results directly — no need to load/filter the harmonized sumstats at all.
        has_ot_results = False
        if opentargets_study_id:
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
                "message": "Using OpenTargets pre-computed credible sets, skipping COJO and fine-mapping",
                "started_at": initial_state["started_at"],
            }
            save_analysis_state_task.submit(user_id, project_id, ot_state).result()

            combined_results = get_results_from_opentargets_task.submit(
                user_id, project_id, opentargets_study_id, coverage=coverage,
                harmonized_file=harmonized_file
            ).result()
            successful_batches = 1 if combined_results is not None and len(combined_results) > 0 else 0
        else:
            # Update analysis state after harmonization
            harmonization_state = {
                "status": "Running",
                "stage": "Filtering",
                "progress": 30,
                "message": "Harmonization completed, filtering significant variants",
                "started_at": initial_state["started_at"]
            }
            save_analysis_state_task.submit(user_id, project_id, harmonization_state).result()

            logger.info(f"[PIPELINE] Stage 2: Loading and filtering variants")
            significant_df_result = filter_significant_variants.submit(harmonized_file, output_dir).result()

            # Extract the actual DataFrame
            if isinstance(significant_df_result, tuple):
                significant_df, sig_output_path = significant_df_result
            else:
                significant_df = significant_df_result
                sig_output_path = None

            # Update analysis state after filtering
            filtering_state = {
                "status": "Running",
                "stage": "Cojo",
                "progress": 50,
                "message": "Filtering completed, running COJO analysis"
            }
            save_analysis_state_task.submit(user_id, project_id, filtering_state).result()

            logger.info(f"[PIPELINE] Stage 3: COJO analysis")

            config = Config.from_env()
            plink_dir = config.plink_dir_38
            cojo_result = run_cojo_per_chromosome.submit(significant_df, plink_dir, output_dir, maf_threshold=maf_threshold, population=population, ref_genome=ref_genome).result()

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

            if cojo_results is None or len(cojo_results) == 0:
                logger.error("[PIPELINE] No COJO results to process")
                # Save failed state
                failed_state = {
                    "status": "Failed",
                    "stage": "Cojo",
                    "progress": 50,
                    "message": "COJO analysis failed - no independent signals found",
                }
                save_analysis_state_task.submit(user_id, project_id, failed_state).result()
                send_pipeline_notification(
                    user_email, project_name, project_id,
                    success=False, detail=failed_state["message"],
                )
                return None

            # Update analysis state after COJO
            cojo_state = {
                "status": "Running",
                "stage": "Fine_mapping",
                "progress": 70,
                "message": "COJO analysis completed, starting fine-mapping"
            }
            save_analysis_state_task.submit(user_id, project_id, cojo_state).result()

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

        # Combine and save results
        if combined_results is not None and len(combined_results) > 0:
            # Cleanup
            if cojo_output_path and os.path.exists(cojo_output_path):
                try:
                    os.remove(cojo_output_path)
                    logger.info(f"[PIPELINE] Cleaned up temporary file: {cojo_output_path}")
                except Exception as cleanup_e:
                    logger.warning(f"[PIPELINE] Could not cleanup {cojo_output_path}: {cleanup_e}")

            # Save results using Prefect tasks
            results_file = create_analysis_result_task.submit(user_id, project_id, combined_results, output_dir).result()

            # Summary statistics
            total_variants = len(combined_results)
            high_pip_variants = len(combined_results[combined_results['PIP'] > 0.5])
            total_credible_sets = combined_results.get('credible_set', pd.Series([0])).max()

            # Wait for parallel LDSC + tissue analysis to complete
            logger.info(f"[PIPELINE] Stage 5: Waiting for LDSC + Tissue Analysis to complete")

            # Update analysis state for waiting on LDSC + tissue analysis
            ldsc_tissue_state = {
                "status": "Running",
                "stage": "LDSC_Tissue_Analysis",
                "progress": 85,
                "message": "Fine-mapping completed, waiting for LDSC and tissue analysis"
            }
            save_analysis_state_task.submit(user_id, project_id, ldsc_tissue_state).result()

            try:
                # Wait for the parallel LDSC + tissue analysis task to complete
                ldsc_tissue_result = ldsc_tissue_future.result()

                logger.info(f"[PIPELINE] LDSC + tissue analysis completed successfully!")
                logger.info(f"[PIPELINE] - Analysis run ID: {ldsc_tissue_result['analysis_run_id']}")
                ldsc_status = "completed"

            except Exception as ldsc_e:
                logger.error(f"[PIPELINE] LDSC + tissue analysis failed: {str(ldsc_e)}")
                # LDSC results are required for tissue selection and enrichment - fail the pipeline
                failed_ldsc_state = {
                    "status": "Failed",
                    "stage": "LDSC_Analysis",
                    "progress": 90,
                    "message": f"LDSC tissue analysis failed: {str(ldsc_e)}. Tissue-specific enrichment will not be available.",
                }
                save_analysis_state_task.submit(user_id, project_id, failed_ldsc_state).result()
                raise RuntimeError(f"LDSC tissue analysis failed - required for enrichment: {str(ldsc_e)}")

            # Save completed analysis state
            completed_state = {
                "status": "Completed",
                "progress": 100,
                "message": "Analysis completed successfully",
                "ldsc_status": ldsc_status
            }
            save_analysis_state_task.submit(user_id, project_id, completed_state).result()

            send_pipeline_notification(user_email, project_name, project_id, success=True)

            logger.info(f"[PIPELINE] Analysis completed successfully!")
            logger.info(f"[PIPELINE] - Total variants: {total_variants}")
            logger.info(f"[PIPELINE] - High-confidence variants (PIP > 0.5): {high_pip_variants}")
            logger.info(f"[PIPELINE] - Total credible sets: {total_credible_sets}")
            logger.info(f"[PIPELINE] - Successful batches: {successful_batches}/{len(region_batches)}")
            logger.info(f"[PIPELINE] - Results saved: {results_file}")

            return {
                "results_file": results_file,
                "total_variants": total_variants,
                "high_pip_variants": high_pip_variants,
                "total_credible_sets": total_credible_sets
            }
        else:
            logger.error("[PIPELINE]  No fine-mapping results generated")
            # Save failed state for fine-mapping
            failed_finemap_state = {
                "status": "Failed",
                "stage": "Fine_mapping",
                "progress": 70,
                "message": "Fine-mapping failed - no results generated",
            }
            save_analysis_state_task.submit(user_id, project_id, failed_finemap_state).result()
            raise RuntimeError("All fine-mapping batches failed")

    except Exception as e:
        logger.error(f"[PIPELINE]  Analysis pipeline failed: {str(e)}")
        # Save failed analysis state
        try:
            failed_state = {
                "status": "Failed",
                "stage": "Unknown",
                "progress": 0,
                "message": f"Analysis pipeline failed: {str(e)}",
            }
            save_analysis_state_task.submit(user_id, project_id, failed_state).result()
        except Exception as state_e:
            logger.error(f"[PIPELINE] Failed to save error state: {str(state_e)}")
        send_pipeline_notification(
            user_email, project_name, project_id,
            success=False, detail=str(e),
        )
        raise