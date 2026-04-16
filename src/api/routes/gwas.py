from __future__ import annotations

import asyncio
import os
import re
import tempfile
import traceback

import requests as _http
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from loguru import logger

from src.api.dependencies import _deps

router = APIRouter()


def _download_to_path_sync(url: str, path: str) -> int:
    with _http.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return os.path.getsize(path)


@router.get("/gwas-files")
async def get_gwas_files(
    search: str | None = Query(None),
    sex: str | None = Query(None),
    limit: int | None = Query(None),
    skip: int = Query(0),
):
    gwas_library = _deps.get("gwas_library")
    try:
        if limit is None:
            limit = 100

        entries = gwas_library.get_all_gwas_entries(
            search_term=search, sex_filter=sex, limit=limit, skip=skip
        )

        gwas_files: list[dict] = []
        for entry in entries:
            file_id = entry.get("file_id") or entry.get("filename")
            desc = entry.get("description") or ""
            if isinstance(desc, str) and desc.startswith("#"):
                desc = desc.lstrip("#").strip()
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
                "sample_size": entry.get("sample_size"),
                "genome_build": entry.get("genome_build"),
            }
            if entry.get("file_size"):
                gwas_file_entry["file_size_mb"] = round(
                    entry["file_size"] / (1024 * 1024), 2
                )
            gwas_files.append(gwas_file_entry)

        total_count = gwas_library.get_entry_count(search_term=search, sex_filter=sex)
        return {
            "gwas_files": gwas_files,
            "total_files": total_count,
            "returned": len(gwas_files),
            "skip": skip,
            "limit": limit,
        }

    except Exception as exc:
        logger.error(f"Error fetching GWAS files: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch GWAS files: {exc}"
        )


@router.get("/gwas-files/download/{file_id}")
async def download_gwas_file(file_id: str):
    gwas_library = _deps.get("gwas_library")
    storage = _deps.get("storage")

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
                    return {"download_url": download_url, "cached": True}

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
