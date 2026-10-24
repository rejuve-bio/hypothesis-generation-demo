from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


class GwasSourcesResponse(BaseModel):
    sources: list[dict[str, Any]]


class GwasFileListItem(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str | None = None
    phenotype: str | None = None
    phenotype_code: str | None = None
    filename: str | None = None
    sex: str | None = None
    source: str | None = None
    downloaded: bool = False
    download_count: int = 0
    url: str | None = None
    showcase_link: str = ""
    genome_build: str | None = None


class GwasFilesListResponse(BaseModel):
    gwas_files: list[GwasFileListItem | dict[str, Any]]
    total_files: int
    returned: int
    skip: int
    limit: int


class SampleSizeInfoResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    sample_size: int | None = None
    sample_size_source: str | None = None
    sample_size_message: str | None = None
    sample_size_is_user_provided: bool | None = None
    sample_size_editable: bool | None = None
    sample_size_prefill: int | None = None
    gwas_source: Literal["library", "upload"] | None = None
    default_sample_size: int | None = None


class GwasDownloadUrlResponse(BaseModel):
    download_url: str
    cached: bool = True
