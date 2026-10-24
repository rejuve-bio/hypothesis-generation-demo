from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class UserFileItem(BaseModel):
    id: Any | None = None
    display_name: str | None = None
    filename: str | None = None
    file_size: int = 0
    file_size_mb: float = 0
    record_count: int | None = None
    upload_date: Any | None = None
    source: str = "user_upload"


class UserFilesResponse(BaseModel):
    files: list[UserFileItem | dict[str, Any]]
    total_files: int
