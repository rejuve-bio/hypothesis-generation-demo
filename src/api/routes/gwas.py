from __future__ import annotations

import asyncio
import os
import re
import tempfile
import traceback

import requests as _http
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from loguru import logger

from src.api.auth import get_current_user_id
from src.api.dependencies import get_file_handler, get_gwas_library_handler, get_storage
from src.api.schemas import (
    GwasDownloadUrlResponse,
    GwasFilesListResponse,
    GwasSourcesResponse,
    SampleSizeInfoResponse,
)
from src.db import FileHandler, GWASLibraryHandler

router = APIRouter(tags=["gwas_library"])


def _download_to_path_sync(url: str, path: str) -> int:
    with _http.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return os.path.getsize(path)


@router.get(
    "/gwas-files/sources",
    response_model=GwasSourcesResponse,
    summary="GWAS catalog source counts",
)
async def get_gwas_file_sources(
    gwas_library: GWASLibraryHandler = Depends(get_gwas_library_handler),
):
    try:
        sources = gwas_library.get_source_counts()
        return GwasSourcesResponse(sources=sources)
    except Exception as exc:
        logger.error(f"Error fetching GWAS source counts: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch GWAS sources: {exc}"
        )


@router.get(
    "/gwas-files",
    response_model=GwasFilesListResponse,
    summary="Search GWAS library catalog",
)
async def get_gwas_files(
    search: str | None = Query(None),
    sex: str | None = Query(None),
    source: str | None = Query(None),
    limit: int | None = Query(None),
    skip: int = Query(0),
    gwas_library: GWASLibraryHandler = Depends(get_gwas_library_handler),
):
    try:
        if limit is None:
            limit = 100

        entries = gwas_library.get_all_gwas_entries(
            search_term=search, sex_filter=sex, source_filter=source, limit=limit, skip=skip
        )

        gwas_files: list[dict] = []
        for entry in entries:
            file_id = entry.get("file_id") or entry.get("filename")
            desc = entry.get("description") or ""
            if isinstance(desc, str) and desc.startswith("#"):
                desc = desc.lstrip("#").strip()
            resolution = GWASLibraryHandler.resolve_sample_size_info(entry=entry)
            gwas_file_entry: dict = {
                "id": file_id,
                "phenotype": desc,
                "phenotype_code": entry.get("phenotype_code"),
                "filename": entry.get("filename"),
                "sex": entry.get("sex"),
                "source": entry.get("source"),
                "downloaded": entry.get("downloaded", False),
                "download_count": entry.get("download_count", 0),
                "url": f"/gwas-files/download/{file_id}",
                "showcase_link": entry.get("showcase_link", ""),
                "genome_build": entry.get("genome_build"),
                **GWASLibraryHandler.sample_size_fields_for_library_entry(
                    entry, resolution
                ),
            }
            if entry.get("category"):
                gwas_file_entry["category"] = entry.get("category")
            if entry.get("file_size"):
                gwas_file_entry["file_size_mb"] = round(
                    entry["file_size"] / (1024 * 1024), 2
                )
            gwas_files.append(gwas_file_entry)

        total_count = gwas_library.get_entry_count(
            search_term=search, sex_filter=sex, source_filter=source
        )
        return GwasFilesListResponse(
            gwas_files=gwas_files,
            total_files=total_count,
            returned=len(gwas_files),
            skip=skip,
            limit=limit,
        )

    except Exception as exc:
        logger.error(f"Error fetching GWAS files: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch GWAS files: {exc}"
        )


@router.get(
    "/gwas-files/{file_id}/sample-size-info",
    response_model=SampleSizeInfoResponse,
    summary="Preview GWAS sample size",
)
async def get_sample_size_info(
    file_id: str,
    gwas_source: Literal["library", "upload"] | None = Query(
        None,
        description="Optional. If set, only that source is checked. If omitted, library is tried first, then user uploads.",
    ),
    current_user_id: str = Depends(get_current_user_id),
    gwas_library: GWASLibraryHandler = Depends(get_gwas_library_handler),
    files: FileHandler = Depends(get_file_handler),
):
    """
    Preview sample size after the user selects a file.
    """
    try:
        if gwas_source in (None, "library"):
            entry = gwas_library.get_gwas_entry(file_id=file_id)
            if entry:
                resolution = GWASLibraryHandler.resolve_sample_size_info(entry=entry)
                return SampleSizeInfoResponse(
                    **GWASLibraryHandler.sample_size_fields_for_library_entry(
                        entry, resolution
                    ),
                    gwas_source="library",
                )
            if gwas_source == "library":
                raise HTTPException(
                    status_code=404, detail="GWAS file not found in library"
                )

        file_meta = files.get_file_metadata(current_user_id, file_id)
        if file_meta:
            resolution = GWASLibraryHandler.resolve_upload_sample_size_info(
                file_meta.get("file_path")
            )
            return SampleSizeInfoResponse(
                **resolution.to_api_dict(),
                gwas_source="upload",
            )

        raise HTTPException(status_code=404, detail="GWAS file not found")
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error resolving sample-size info for {file_id}: {exc}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to resolve sample-size info: {exc}",
        )


@router.get(
    "/gwas-files/download/{file_id}",
    responses={
        200: {
            "description": "Presigned download URL when cached, otherwise TSV file stream.",
            "content": {
                "application/json": {"schema": GwasDownloadUrlResponse.model_json_schema()},
                "text/tab-separated-values": {"schema": {"type": "string", "format": "binary"}},
            },
        }
    },
    summary="Download GWAS file",
)
async def download_gwas_file(
    file_id: str,
    gwas_library: GWASLibraryHandler = Depends(get_gwas_library_handler),
    storage=Depends(get_storage),
):

    try:
        entry = gwas_library.get_gwas_entry(file_id=file_id)
        if not entry:
            raise HTTPException(
                status_code=404, detail="GWAS file not found in library"
            )

        filename = entry.get("filename", file_id)
        minio_path = f"gwas_cache/{filename}"

        if storage and entry.get("downloaded") and entry.get("minio_path"):
            cached_path = entry["minio_path"]
            if storage.exists(cached_path):
                gwas_library.increment_download_count(file_id)
                download_url = storage.generate_presigned_url(cached_path, expiration=3600)
                if download_url:
                    return GwasDownloadUrlResponse(download_url=download_url, cached=True)

                with tempfile.NamedTemporaryFile(
                    delete=False, suffix=f"_{filename}"
                ) as tmp:
                    if storage.download_file(cached_path, tmp.name):
                        return FileResponse(
                            tmp.name,
                            media_type="text/tab-separated-values",
                            filename=filename,
                        )
            else:
                gwas_library.update_gwas_entry(
                    file_id, {"downloaded": False, "minio_path": None}
                )

        # Determine download URL
        download_url = None
        if entry.get("aws_url"):
            download_url = entry["aws_url"]
        elif entry.get("wget_command"):
            m = re.search(r"(https?://[^\s]+)", entry["wget_command"])
            if m:
                download_url = m.group(1)
        elif entry.get("dropbox_url"):
            download_url = entry["dropbox_url"]

        if not download_url:
            raise HTTPException(
                status_code=404, detail="No download URL available for this file"
            )

        with tempfile.NamedTemporaryFile(
            delete=False, suffix=f"_{filename}"
        ) as tmp:
            temp_path = tmp.name
            try:
                loop = asyncio.get_running_loop()
                file_size = await loop.run_in_executor(
                    None, _download_to_path_sync, download_url, temp_path
                )

                if storage:
                    if storage.upload_file(temp_path, minio_path):
                        gwas_library.mark_as_downloaded(file_id, minio_path, file_size)
                        gwas_library.increment_download_count(file_id)

                return FileResponse(
                    temp_path,
                    media_type="text/tab-separated-values",
                    filename=filename,
                )
            except Exception as exc:
                logger.error(f"[GWAS DOWNLOAD] Download failed: {exc}")
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                raise HTTPException(
                    status_code=500, detail=f"Failed to download file: {exc}"
                )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"[GWAS DOWNLOAD] Error: {exc}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Download failed: {exc}")
