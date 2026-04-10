from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from loguru import logger

from api.dependencies import _deps
from api.auth import get_current_user_id
from src.utils import serialize_datetime_fields

router = APIRouter()


@router.get("/user-files")
async def get_user_files(current_user_id: str = Depends(get_current_user_id)):
    files = _deps["files"]
    try:
        all_files = files.get_file_metadata(current_user_id)
        if not all_files:
            return {"files": [], "total_files": 0}

        if not isinstance(all_files, list):
            all_files = [all_files]

        user_files: list[dict] = []
        for file_meta in all_files:
            if file_meta.get("source") not in (None, "user_upload"):
                continue
            user_files.append(
                {
                    "id": file_meta.get("_id"),
                    "display_name": file_meta.get(
                        "original_filename", file_meta.get("filename")
                    ),
                    "filename": file_meta.get("filename"),
                    "file_size": file_meta.get("file_size", 0),
                    "file_size_mb": round(
                        file_meta.get("file_size", 0) / (1024 * 1024), 2
                    ),
                    "record_count": file_meta.get("record_count"),
                    "upload_date": file_meta.get("upload_date"),
                    "source": "user_upload",
                }
            )

        user_files.sort(key=lambda x: x.get("upload_date", ""), reverse=True)
        user_files = serialize_datetime_fields(user_files)
        return {"files": user_files, "total_files": len(user_files)}

    except Exception as exc:
        logger.error(f"Error fetching user files: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch user files: {exc}"
        )
