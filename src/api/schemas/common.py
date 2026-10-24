from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, RootModel


class FlexibleDict(RootModel[dict[str, Any]]):
    """JSON object with arbitrary keys (Mongo documents, merged API payloads)."""


class FlexibleList(RootModel[list[Any]]):
    """JSON array with arbitrary elements."""


class ErrorResponse(BaseModel):
    detail: str = Field(..., description="Human-readable error message.")


class MessageResponse(BaseModel):
    message: str
