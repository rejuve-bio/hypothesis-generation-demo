from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CredibleSetsResponse(BaseModel):
    variants: list[dict[str, Any]] = Field(
        ...,
        description="Variant rows for the requested credible set.",
    )
