from __future__ import annotations

import asyncio
import json
import os
import re
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import JSONResponse
from loguru import logger
from werkzeug.utils import secure_filename

from api.dependencies import _deps
from api.auth import get_current_user_id
from tasks.project import count_gwas_records, get_project_with_full_data
from src.run_deployment import invoke_analysis_pipeline_deployment
from src.utils import allowed_file, compute_file_md5, get_shared_temp_dir, serialize_datetime_fields

router = APIRouter()


@router.get("/projects")
async def get_projects(
    id: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
):
    projects = _deps["projects"]
    analysis = _deps["analysis"]
    hypotheses = _deps["hypotheses"]
    enrichment = _deps["enrichment"]
    gene_expression = _deps.get("gene_expression")

    if id:
        response_data, status_code = get_project_with_full_data(
            projects,
            analysis,
            hypotheses,
            enrichment,
            current_user_id,
            id,
            gene_expression_handler=gene_expression,
        )
        if status_code == 200:
            response_data = serialize_datetime_fields(response_data)
        return JSONResponse(content=response_data, status_code=status_code)

    raw_projects = projects.get_projects(current_user_id)
    files = _deps["files"]
    enhanced_projects: list[dict] = []

    for project in raw_projects:
        enhanced: dict = {
            "id": project["id"],
            "name": project["name"],
            "phenotype": project.get("phenotype", ""),
            "created_at": project.get("created_at"),
        }

        file_metadata = files.get_file_metadata(current_user_id, project["gwas_file_id"])
        enhanced["gwas_file"] = file_metadata["download_url"]
        enhanced["gwas_records_count"] = file_metadata["record_count"]

        try:
            analysis_state = projects.load_analysis_state(current_user_id, project["id"])
            enhanced["status"] = (
                analysis_state.get("status", "Not_started")
                if analysis_state
                else "Not_started"
            )
        except Exception as state_e:
            logger.warning(f"Could not load analysis state for project {project['id']}: {state_e}")
            enhanced["status"] = "Completed"

        enhanced["population"] = project.get("population")
        enhanced["ref_genome"] = project.get("ref_genome")

        total_credible_sets = 0
        total_variants = 0
        try:
            credible_sets_raw = analysis.get_credible_sets_for_project(
                current_user_id, project["id"]
            )
            if credible_sets_raw and isinstance(credible_sets_raw, list):
                total_credible_sets = len(credible_sets_raw)
                total_variants = sum(
                    cs.get("variants_count", 0) for cs in credible_sets_raw
                )
        except Exception as cs_e:
            logger.warning(f"Could not load credible sets for {project['id']}: {cs_e}")

        enhanced["total_credible_sets_count"] = total_credible_sets
        enhanced["total_variants_count"] = total_variants

        hypothesis_count = 0
        try:
            all_hyp = hypotheses.get_hypotheses(current_user_id)
            if isinstance(all_hyp, list):
                hypothesis_count = sum(
                    1 for h in all_hyp if h.get("project_id") == project["id"]
                )
            elif all_hyp and all_hyp.get("project_id") == project["id"]:
                hypothesis_count = 1
        except Exception as hyp_e:
            logger.warning(f"Could not count hypotheses for {project['id']}: {hyp_e}")

        enhanced["hypothesis_count"] = hypothesis_count
        enhanced_projects.append(enhanced)

    return {"projects": serialize_datetime_fields(enhanced_projects)}


@router.delete("/projects")
async def delete_project(
    id: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
):
    if not id:
        raise HTTPException(status_code=400, detail="Project ID is required")
    projects = _deps["projects"]
    success = projects.delete_project(current_user_id, id)
    if success:
        return {"message": "Project deleted successfully"}
    raise HTTPException(status_code=404, detail="Project not found or access denied")


@router.post("/projects/delete")
async def bulk_delete_projects(
    data: dict = Body(...),
    current_user_id: str = Depends(get_current_user_id),
):
    projects = _deps["projects"]
    project_ids = data.get("project_ids")

    if not project_ids:
        raise HTTPException(
            status_code=400, detail="project_ids is required in request body"
        )
    if not isinstance(project_ids, list):
        raise HTTPException(status_code=400, detail="project_ids must be a list")
    if not project_ids:
        raise HTTPException(
            status_code=400, detail="project_ids list cannot be empty"
        )

    result = projects.bulk_delete_projects(current_user_id, project_ids)

    if result and isinstance(result, dict):
        if result["success"]:
            return {
                "message": f"Successfully deleted {result['deleted_count']} project(s)",
                "deleted_count": result["deleted_count"],
                "total_requested": result["total_requested"],
            }
        return JSONResponse(
            content={
                "message": (
                    f"Partially deleted {result['deleted_count']}/{result['total_requested']}"
                    " project(s)"
                ),
                "deleted_count": result["deleted_count"],
                "total_requested": result["total_requested"],
                "errors": result.get("errors"),
            },
            status_code=207,
        )
    raise HTTPException(status_code=500, detail="Failed to delete projects")


@router.post("/analysis-pipeline", status_code=202)
async def post_analysis_pipeline(
    request: Request,
    current_user_id: str = Depends(get_current_user_id),
):
    try:
        form = await request.form()

        project_name: str | None = form.get("project_name")
        population: str = form.get("population", "EUR")
        max_workers: int = int(form.get("max_workers", 3))
        is_uploaded: bool = form.get("is_uploaded", "false").lower() == "true"

        gwas_file = form.get("gwas_file") if is_uploaded else None

        maf_threshold: float = float(form.get("maf_threshold", 0.01))
        seed: int = int(form.get("seed", 42))
        window: int = int(form.get("window", 2000))
        L: int = int(form.get("L", -1))
        coverage: float = float(form.get("coverage", 0.95))
        min_abs_corr: float = float(form.get("min_abs_corr", 0.5))
        batch_size: int = int(form.get("batch_size", 5))
        sample_size: int = int(form.get("sample_size", 10000))

        projects = _deps["projects"]
        files = _deps["files"]
        analysis = _deps["analysis"]
        gene_expression = _deps.get("gene_expression")
        config = _deps["config"]
        storage = _deps.get("storage")
        gwas_library = _deps.get("gwas_library")

        gwas_entry = None
        file_id_param: str | None = form.get("gwas_file") if not is_uploaded else None

        if not is_uploaded and file_id_param and gwas_library:
            gwas_entry = gwas_library.get_gwas_entry(file_id=file_id_param)

        phenotype: str | None = form.get("phenotype")
        if not phenotype and gwas_entry:
            phenotype = gwas_entry.get("description") or gwas_entry.get("phenotype_code")
            # Clean up leading '#' if it exists in the library description
            if isinstance(phenotype, str) and phenotype.startswith("#"):
                phenotype = phenotype.lstrip("#").strip()

        raw_ref_genome = form.get("ref_genome")
        ref_genome: str = raw_ref_genome or "GRCh37"

        if gwas_entry and (not raw_ref_genome or raw_ref_genome == "GRCh37"):
            inferred_build = gwas_entry.get("genome_build")
            if inferred_build:
                # Normalize common library build string formats
                if "38" in inferred_build:
                    ref_genome = "GRCh38"
                elif "37" in inferred_build or "19" in inferred_build:
                    ref_genome = "GRCh37"

        if not project_name:
            raise HTTPException(status_code=400, detail="project_name is required")
        if not phenotype:
            raise HTTPException(status_code=400, detail="phenotype is required (could not be inferred from library)")

        if is_uploaded and gwas_file and not allowed_file(gwas_file.filename):
            raise HTTPException(
                status_code=400,
                detail="Invalid file format. Supported: .tsv, .txt, .csv, .gz, .bgz",
            )
        if ref_genome not in ("GRCh37", "GRCh38"):
            raise HTTPException(
                status_code=400, detail="Reference genome must be GRCh37 or GRCh38"
            )
        if population not in ("EUR", "AFR", "AMR", "EAS", "SAS"):
            raise HTTPException(
                status_code=400,
                detail="Population must be one of: EUR, AFR, AMR, EAS, SAS",
            )
        if not (1 <= max_workers <= 16):
            raise HTTPException(
                status_code=400, detail="Max workers must be between 1-16"
            )
        if not (0.001 <= maf_threshold <= 0.5):
            raise HTTPException(
                status_code=400, detail="MAF threshold must be between 0.001-0.5"
            )
        if not (1 <= seed <= 999999):
            raise HTTPException(
                status_code=400, detail="Seed must be between 1-999999"
            )
        if window > 10000:
            raise HTTPException(
                status_code=400,
                detail="Fine-mapping window shouldn't be greater than 10000 kb",
            )
        if L != -1 and not (1 <= L <= 50):
            raise HTTPException(
                status_code=400, detail="L must be -1 (auto) or between 1-50"
            )
        if not (0.5 <= coverage <= 0.999):
            raise HTTPException(
                status_code=400, detail="Coverage must be between 0.5-0.999"
            )
        if not (0.5 <= min_abs_corr <= 1.0):
            raise HTTPException(
                status_code=400,
                detail="Min absolute correlation must be between 0.5-1.0",
            )
        if not (1 <= batch_size <= 20):
            raise HTTPException(
                status_code=400, detail="Batch size must be between 1-20"
            )

        start_time = datetime.now()
        file_metadata_id = None
        file_path: str | None = None
        filename: str | None = None
        file_size: int = 0
        gwas_records_count: int = 0
        original_filename: str | None = None
        file_id_new: str | None = None
        object_key: str | None = None
        _file_needs_processing: bool = False
        _source_minio_path: str | None = None
        _source_download_url: str | None = None
        _minio_cache_key: str | None = None
        _gwas_library_id: str | None = None

        if not is_uploaded:
            file_id_param: str | None = form.get("gwas_file")
            if not file_id_param:
                raise HTTPException(
                    status_code=400,
                    detail="gwas_file parameter is required when is_uploaded=false",
                )

            logger.info(f"[API] Auto-detecting source for file ID: {file_id_param}")

            file_meta = None
            try:
                file_meta = files.get_file_metadata(current_user_id, file_id_param)
            except Exception as exc:
                logger.info(f"[API] Not a valid user file ID, checking library: {exc}")

            if file_meta:
                storage_key = file_meta.get("storage_key")
                filename = file_meta.get("filename")
                file_size = file_meta.get("file_size", 0)
                gwas_records_count = file_meta.get("record_count", 0)
                original_filename = filename

                if not storage_key:
                    raise HTTPException(
                        status_code=400,
                        detail="File does not have storage information.",
                    )
                if not storage:
                    raise HTTPException(
                        status_code=500, detail="Storage service not available"
                    )

                file_path = ""
                file_metadata_id = file_id_param
                _file_needs_processing = True
                _source_minio_path = storage_key
            else:
                if not gwas_entry:
                    raise HTTPException(
                        status_code=404,
                        detail=f"File not found in user library or system library: {file_id_param}",
                    )

                filename = gwas_entry.get("filename", file_id_param)
                original_filename = filename
                minio_path_lib = f"gwas_cache/{filename}"
                file_size = gwas_entry.get("file_size", 0)

                if storage and gwas_entry.get("downloaded") and gwas_entry.get("minio_path"):
                    minio_path = gwas_entry["minio_path"]
                    loop = asyncio.get_running_loop()
                    file_exists = await loop.run_in_executor(None, storage.exists, minio_path)
                    if file_exists:
                        # Cached in MinIO — Prefect will download it.
                        file_path = ""
                        _file_needs_processing = True
                        _source_minio_path = minio_path
                    else:
                        gwas_library.update_gwas_entry(
                            file_id_param, {"downloaded": False, "minio_path": None}
                        )

                if not _source_minio_path:
                    # Check local raw directory
                    raw_data_path = os.path.join(config.data_dir, "raw")
                    for ext in (".tsv", ".tsv.gz", ".tsv.bgz", ".txt", ".txt.gz", ".csv", ".csv.gz"):
                        candidate = os.path.join(raw_data_path, f"{file_id_param}{ext}")
                        if os.path.exists(candidate):
                            file_path = candidate
                            filename = f"{file_id_param}{ext}"
                            file_size = os.path.getsize(file_path)
                            _file_needs_processing = True
                            break

                if not _source_minio_path and not file_path:
                    # Not cached anywhere — Prefect will download from external URL.
                    download_url = (
                        gwas_entry.get("aws_url")
                        or (
                            re.search(r"(https?://[^\s]+)", gwas_entry["wget_command"]).group(1)
                            if gwas_entry.get("wget_command")
                            else None
                        )
                        or gwas_entry.get("dropbox_url")
                    )
                    if not download_url:
                        raise HTTPException(
                            status_code=404,
                            detail=f"No download URL available for {file_id_param}",
                        )
                    file_path = ""
                    _file_needs_processing = True
                    _source_download_url = download_url
                    _minio_cache_key = minio_path_lib if storage else None
                    _gwas_library_id = file_id_param

                if not _source_minio_path and not _source_download_url and not file_path:
                    raise HTTPException(
                        status_code=404,
                        detail=f"GWAS file not found: {file_id_param}",
                    )

                gwas_records_count = 0  # filled in by prepare_gwas_file_task

        else:
            # Uploaded file
            if not gwas_file or gwas_file.filename == "":
                raise HTTPException(status_code=400, detail="No GWAS file uploaded")
            if not allowed_file(gwas_file.filename):
                raise HTTPException(
                    status_code=400,
                    detail="Invalid file format. Supported: .tsv, .txt, .csv, .gz, .bgz",
                )

            original_filename = gwas_file.filename
            filename = secure_filename(gwas_file.filename)
            file_id_new = str(uuid.uuid4())
            temp_dir = get_shared_temp_dir(user_id=current_user_id, prefix="upload")
            temp_file_path = os.path.join(temp_dir, filename)

            with open(temp_file_path, "wb") as fh:
                while chunk := await gwas_file.read(1024 * 1024):
                    fh.write(chunk)

            file_size = os.path.getsize(temp_file_path)
            md5_hash = compute_file_md5(temp_file_path)

            existing_file = None
            if md5_hash and storage:
                existing_file = files.find_file_by_md5(current_user_id, md5_hash)

            if existing_file:
                storage_key = existing_file.get("storage_key")
                if storage_key and storage.exists(storage_key):
                    storage.download_file(storage_key, temp_file_path)
                    file_path = temp_file_path
                    file_metadata_id = existing_file.get("_id")
                    gwas_records_count = existing_file.get(
                        "record_count", count_gwas_records(file_path)
                    )
                else:
                    raise HTTPException(
                        status_code=500,
                        detail="Failed to retrieve existing file from storage",
                    )
            else:
                file_path = temp_file_path
                gwas_records_count = 0
                object_key = (
                    f"uploads/{current_user_id}/{file_id_new}/{filename}"
                    if storage else None
                )
                md5_hash_param: str | None = md5_hash if storage else None
                _file_needs_processing = True

        if not file_metadata_id:
            source = "gwas_library" if not is_uploaded else "user_upload"
            storage_key_arg = object_key if is_uploaded and storage else None
            md5_arg = md5_hash_param if is_uploaded else None

            file_metadata_id = files.create_file_metadata(
                user_id=current_user_id,
                filename=filename,
                original_filename=original_filename,
                file_path=file_path,
                file_type="gwas",
                file_size=file_size,
                record_count=gwas_records_count,
                download_url=f"/download/{str(uuid.uuid4())}",
                md5_hash=md5_arg,
                storage_key=storage_key_arg,
                source=source,
            )

        analysis_parameters = {
            "maf_threshold": maf_threshold,
            "seed": seed,
            "window": window,
            "L": L,
            "coverage": coverage,
            "min_abs_corr": min_abs_corr,
            "batch_size": batch_size,
            "max_workers": max_workers,
        }

        project_id = projects.create_project(
            user_id=current_user_id,
            name=project_name,
            gwas_file_id=file_metadata_id,
            phenotype=phenotype,
            population=population,
            ref_genome=ref_genome,
            analysis_parameters=analysis_parameters,
        )

        metadata_dir = os.path.join("data", "metadata", str(current_user_id))
        os.makedirs(metadata_dir, exist_ok=True)
        metadata = {
            "file_id": file_metadata_id,
            "user_id": current_user_id,
            "filename": filename,
            "original_filename": original_filename,
            "file_path": file_path,
            "file_type": "gwas",
            "upload_date": str(datetime.now()),
            "file_size": file_size,
            "project_id": project_id,
        }
        with open(os.path.join(metadata_dir, f"{file_metadata_id}.json"), "w") as fh:
            json.dump(metadata, fh)

        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[API] Project {project_id} ready in {total_time:.1f}s, firing Prefect")

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: invoke_analysis_pipeline_deployment(
                user_id=current_user_id,
                project_id=project_id,
                gwas_file_path=file_path,
                ref_genome=ref_genome,
                population=population,
                batch_size=batch_size,
                max_workers=max_workers,
                maf_threshold=maf_threshold,
                seed=seed,
                window=window,
                L=L,
                coverage=coverage,
                min_abs_corr=min_abs_corr,
                sample_size=sample_size,
                file_metadata_id=file_metadata_id if _file_needs_processing else None,
                file_needs_processing=_file_needs_processing,
                file_storage_key=object_key if _file_needs_processing else None,
                file_id_new=file_id_new if _file_needs_processing else None,
                file_source_minio_path=_source_minio_path,
                file_source_download_url=_source_download_url,
                file_minio_cache_key=_minio_cache_key,
                file_gwas_library_id=_gwas_library_id,
            ),
        )

        return {
            "status": "started",
            "project_id": project_id,
            "file_id": file_metadata_id,
            "message": "Analysis pipeline started successfully",
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[API] Error starting analysis pipeline: {exc}")
        raise HTTPException(
            status_code=500,
            detail=f"Error starting analysis pipeline: {exc}",
        )
