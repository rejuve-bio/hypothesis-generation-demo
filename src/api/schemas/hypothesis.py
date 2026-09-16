from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class HypothesisGraphResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    hypothesis_id: str
    summary: Any | None = None
    graph: Any | None = None
    enrich_id: str | None = None
    project_id: str | None = None
    forked: bool = False
    warnings: list[dict[str, Any]] = Field(default_factory=list)


class HypothesisChatResponse(BaseModel):
    response: Any | None = None


class BulkDeleteHypothesesRequest(BaseModel):
    model_config = ConfigDict(json_schema_extra={"required": ["hypothesis_ids"]})

    hypothesis_ids: Any = Field(
        default_factory=lambda: None,
        description="Non-empty list of hypothesis IDs.",
        json_schema_extra={
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1,
        },
    )


class BulkDeleteHypothesisFailure(BaseModel):
    id: str
    reason: str


class BulkDeleteHypothesesResponse(BaseModel):
    message: str
    deleted_count: int
    enrichments_deleted: int
    successful: list[str]
    failed: list[BulkDeleteHypothesisFailure]


class HypothesisChatForm(BaseModel):
    query: str = Field(..., min_length=1)
    hypothesis_id: str = Field(..., min_length=1)

    @classmethod
    def from_form(cls, form: Any) -> HypothesisChatForm:
        query = form.get("query")
        hypothesis_id = form.get("hypothesis_id")
        if not query or not hypothesis_id:
            raise ValueError("query and hypothesis_id are required")
        return cls(query=str(query), hypothesis_id=str(hypothesis_id))
