from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class PhenotypeBulkItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str = ""
    name: str = ""


class PhenotypeBulkResponse(BaseModel):
    message: str
    inserted_count: int
    skipped_count: int
    total_provided: int


class PhenotypeSingleWrapResponse(BaseModel):
    phenotype: dict[str, Any]


class PhenotypeListResponse(BaseModel):
    phenotypes: list[Any]
    total_count: int
    skip: int
    limit: int
    has_more: bool
    next_skip: int | None = None
    search_term: str | None = None
