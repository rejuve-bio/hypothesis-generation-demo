from __future__ import annotations

from typing import Any

from pydantic import BaseModel, field_validator


class EnrichmentsListResponse(BaseModel):
    enrichments: list[dict[str, Any]]


class EnrichPostBody(BaseModel):
    """Optional JSON body for POST /enrich (fields also accepted as query params)."""

    variant: str | None = None
    project_id: str | None = None
    tissue_name: str | None = None
    seed: int = 42

    @field_validator("seed", mode="before")
    @classmethod
    def coerce_seed(cls, value: object) -> int:
        if value is None or value == "":
            return 42
        return int(value)  # type: ignore[arg-type]


class EnrichPostAcceptedResponse(BaseModel):
    hypothesis_id: str
    project_id: str
    forked: bool = False
