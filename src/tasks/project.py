import os
import pandas as pd
from prefect import task
from loguru import logger
import gzip
import re
from src.utils import (
    analysis_state_for_public_api,
    emit_analysis_update,
    get_deps,
    normalize_status_responses,
)


@task()
def prepare_gwas_file_task(
    user_id: str,
    project_id: str,
    file_metadata_id: str | None,
    gwas_file_path: str | None = None,
    storage_key: str | None = None,
    file_id_new: str | None = None,
    source_minio_path: str | None = None,
    source_download_url: str | None = None,
    minio_cache_key: str | None = None,
    gwas_library_id: str | None = None,
    output_dir: str | None = None,
) -> str:
    """
    Ensure the GWAS file is available locally.  
    """
    import shutil
    import requests as _req

    deps = get_deps()
    files_handler = deps["files"]
    storage = deps.get("storage")
    projects_handler = deps["projects"]
    gwas_library = deps.get("gwas_library")

    uploading_state = {
        "status": "Running",
        "stage": "File_upload",
        "progress": 3,
        "message": "Uploading or preparing GWAS file...",
    }
    projects_handler.save_analysis_state(user_id, project_id, uploading_state)
    emit_analysis_update(user_id, project_id, uploading_state)

    final_path = gwas_file_path
    dest_dir = output_dir or os.path.join("data", "temp", str(user_id))

    if source_minio_path and storage:
        os.makedirs(dest_dir, exist_ok=True)
        filename = os.path.basename(source_minio_path)
        final_path = os.path.join(dest_dir, filename)
        if not storage.download_file(source_minio_path, final_path):
            raise RuntimeError(f"MinIO download failed: {source_minio_path}")

    elif source_download_url:
        os.makedirs(dest_dir, exist_ok=True)
        raw_name = source_download_url.split("/")[-1].split("?")[0] or "gwas_file"
        final_path = os.path.join(dest_dir, raw_name)
        with _req.get(source_download_url, stream=True, timeout=600) as r:
            r.raise_for_status()
            with open(final_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        file_size = os.path.getsize(final_path)
        if storage and minio_cache_key:
            if storage.upload_file(final_path, minio_cache_key):
                if gwas_library and gwas_library_id:
                    gwas_library.mark_as_downloaded(gwas_library_id, minio_cache_key, file_size)

    elif gwas_file_path and storage and storage_key:
        if not storage.upload_file(gwas_file_path, storage_key):
            raise RuntimeError(f"MinIO upload failed: {storage_key}")
        final_path = gwas_file_path

    elif gwas_file_path and file_id_new:
        filename = os.path.basename(gwas_file_path)
        user_upload_dir = os.path.join("data", "uploads", str(user_id))
        os.makedirs(user_upload_dir, exist_ok=True)
        final_path = os.path.join(user_upload_dir, f"{file_id_new}_{filename}")
        shutil.move(gwas_file_path, final_path)

    if not final_path or not os.path.exists(final_path):
        raise RuntimeError(f"GWAS file not available at: {final_path!r}")

    record_count = count_gwas_records(final_path)

    if file_metadata_id:
        metadata_updates: dict = {"record_count": record_count, "file_path": final_path}
        effective_storage_key = minio_cache_key or storage_key
        if effective_storage_key:
            metadata_updates["storage_key"] = effective_storage_key
        files_handler.update_file_metadata(file_metadata_id, metadata_updates)

    logger.info(
        f"[FILE_PREP] Done: {record_count} records, path={final_path}, project={project_id}"
    )
    return final_path


@task()
def save_analysis_state_task(user_id, project_id, state_data):
    """Save analysis state to file system"""
    try:
        deps = get_deps()
        projects_handler = deps["projects"]
        projects_handler.save_analysis_state(user_id, project_id, state_data)
        logger.info(f"Saved analysis state for project {project_id}")
        emit_analysis_update(user_id, project_id, state_data)
        return True
    except Exception as e:
        logger.info(f"Error saving analysis state: {str(e)}")
        raise


@task()
def create_analysis_result_task(user_id, project_id, combined_results, output_dir):
    """Create and save analysis results"""
    try:
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Save to output directory
        results_file = os.path.join(output_dir, "analysis_results.csv")
        combined_results.to_csv(results_file, index=False)
        
        # Save to database
        deps = get_deps()
        analysis_handler = deps["analysis"]
        analysis_handler.save_analysis_results(user_id, project_id, combined_results.to_dict('records'))
        
        logger.info(f"Analysis results saved: {results_file}")
        return results_file
    except Exception as e:
        logger.error(f"Error saving analysis results: {str(e)}")
        raise



@task()
def get_project_analysis_path_task(user_id, project_id):
    """Get the analysis path for a project"""
    try:
        deps = get_deps()
        projects_handler = deps["projects"]
        analysis_path = projects_handler.get_project_analysis_path(user_id, project_id)
        
        # Create directory structure if it doesn't exist
        os.makedirs(analysis_path, exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "preprocessed_data"), exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "plink_binary"), exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "cojo"), exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "expanded_regions"), exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "ld"), exist_ok=True)
        
        logger.info(f"Project analysis path: {analysis_path}")
        return analysis_path
    except Exception as e:
        logger.info(f"Error getting project analysis path: {str(e)}")
        raise


def count_gwas_records(file_path):
    """Count the number of records in a GWAS file"""
    try:
        
        if not os.path.exists(file_path):
            logger.warning(f"GWAS file not found: {file_path}")
            return 0
        
        count = 0
        
        # Handle gzipped files
        if file_path.endswith('.gz') or file_path.endswith('.bgz'):
            with gzip.open(file_path, 'rt') as f:
                # Skip header
                next(f, None)
                for line in f:
                    if line.strip():  # Skip empty lines
                        count += 1
        else:
            with open(file_path, 'r') as f:
                # Skip header
                next(f, None)
                for line in f:
                    if line.strip():  # Skip empty lines
                        count += 1
        
        return count
    except Exception as e:
        logger.warning(f"Error counting GWAS records in {file_path}: {str(e)}")
        return 0


def get_project_with_full_data(projects_handler, analysis_handler, hypotheses_handler, enrichment_handler, user_id, project_id, gene_expression_handler=None):
    """Get comprehensive project data including state, hypotheses, and credible sets"""
    try:
        # Get basic project info
        project = projects_handler.get_projects(user_id, project_id)
        if not project:
            return {"error": "Project not found"}, 404
        
        # Get credible sets with simplified metadata
        credible_sets_data = []
        total_credible_sets_count = 0
        total_variants_count = 0
        analysis_parameters = project.get("analysis_parameters", {})
        
        try:
            credible_sets_data = analysis_handler.get_credible_sets_for_project(user_id, project_id)
            if credible_sets_data:
                total_credible_sets_count = len(credible_sets_data)
                total_variants_count = sum(cs.get("variants_count", 0) for cs in credible_sets_data)
            else:
                credible_sets_data = []
        except Exception as cs_e:
            logger.warning(f"Could not load credible sets for project {project_id}: {cs_e}")
            credible_sets_data = []
        
        # Get analysis state
        analysis_state = projects_handler.load_analysis_state(user_id, project_id)
        if not analysis_state:
            # Infer status from whether results exist
            if credible_sets_data and len(credible_sets_data) > 0:
                analysis_state = {
                    "status": "Completed",
                    "message": "Analysis completed successfully.",
                }
            else:
                analysis_state = {
                    "status": "Running",
                    "message": "Waiting to start analysis.",
                }
        
        # Get hypotheses for this project
        project_hypotheses = []
        try:
            all_hypotheses = hypotheses_handler.get_hypotheses(user_id)
            if isinstance(all_hypotheses, list):
                project_hypotheses = []
                for h in all_hypotheses:
                    if h.get('project_id') == project_id:
                        # Extract probability from hypothesis graph OR enrichment data
                        probability = None
                        
                        # First try to get from hypothesis graph (if hypothesis is fully generated)
                        if h.get("graph") and isinstance(h["graph"], dict):
                            probability = h["graph"].get("probability")
                        
                        # If no probability in hypothesis, try to get from enrichment data
                        if probability is None and h.get("enrich_id"):
                            try:
                                enrich_data = enrichment_handler.get_enrich(user_id, h["enrich_id"])
                                if enrich_data and enrich_data.get("causal_graph"):
                                    causal_graph = enrich_data["causal_graph"]
                                    if isinstance(causal_graph, dict) and causal_graph.get("graph"):
                                        graph = causal_graph["graph"]
                                        if isinstance(graph, dict):
                                            probability = graph.get('prob', {}).get('value') if isinstance(graph.get('prob'), dict) else None
                            except Exception as e:
                                logger.warning(f"Could not get enrichment data for hypothesis {h['id']}: {e}")
                        
                        hypothesis_data = {
                            "id": h["id"], 
                            "variant": h.get("variant") or h.get("variant_id"),
                            "status": normalize_status_responses(
                                h.get("status", "pending")
                            ),
                            "causal_gene": h.get("causal_gene"),
                            "created_at": h.get("created_at"),
                            "probability": probability  # Add confidence/probability score
                        }
                        
                        # Get tissue selection from tissue_selections collection
                        selected_tissue = None
                        if gene_expression_handler:
                            try:
                                variant_id = h.get("variant_rsid") or h.get("variant") or h.get("variant_id")
                                if variant_id:
                                    tissue_selection = gene_expression_handler.get_tissue_selection(
                                        user_id, project_id, variant_id
                                    )
                                    if tissue_selection:
                                        selected_tissue = tissue_selection.get('tissue_name')
                                        logger.info(f"Retrieved tissue selection from DB for hypothesis {h['id']} using variant_id={variant_id}: {selected_tissue}")
                                    else:
                                        logger.info(f"No tissue selection found for hypothesis {h['id']} with variant_id={variant_id}")
                            except Exception as ts_e:
                                logger.warning(f"Could not get tissue selection for hypothesis {h['id']}: {ts_e}")
                        
                        hypothesis_data["tissue_selected"] = selected_tissue
                        
                        project_hypotheses.append(hypothesis_data)
        except Exception as hyp_e:
            logger.warning(f"Could not load hypotheses for project {project_id}: {hyp_e}")
            project_hypotheses = []
        
        # Get LDSC data
        ldsc_data = None
        if gene_expression_handler:
            try:
                ldsc_data = gene_expression_handler.get_ldsc_results_for_project(user_id, project_id, limit=10, format='summary')
                if ldsc_data:
                    logger.info(f"Retrieved LDSC summary from MongoDB for project {project_id}")
            except Exception as ldsc_e:
                logger.warning(f"Could not load LDSC data: {ldsc_e}")
        
        # Build comprehensive response
        response = {
            "id": project["id"],
            "name": project["name"],
            "phenotype": project.get("phenotype", ""),
            "gwas_file_id": project["gwas_file_id"],
            "created_at": project.get("created_at"),
            
            # Summary counts at top level
            "total_credible_sets_count": total_credible_sets_count,
            "total_variants_count": total_variants_count,
            
            # Analysis state and parameters  
            "analysis_state": analysis_state_for_public_api(analysis_state),
            "analysis_parameters": analysis_parameters,
            
            # Credible sets data
            "credible_sets": credible_sets_data,
            
            # Hypotheses information
            "hypotheses": project_hypotheses
        }
        
        if ldsc_data:
            response["ldsc"] = ldsc_data
        
        return response, 200
        
    except Exception as e:
        logger.error(f"Error getting comprehensive project data for {project_id}: {str(e)}")
        return {"error": f"Error retrieving project data: {str(e)}"}, 500