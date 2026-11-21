from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StringConstraints,
    computed_field,
)


_OFFSET_THEN_Z = re.compile(r"[+-]\d{2}:\d{2}Z$")


def _strip_redundant_z(value):
    # Stored stamps are often isoformat() + "Z", i.e. "...+00:00Z".
    if isinstance(value, str) and _OFFSET_THEN_Z.search(value):
        return value[:-1]
    return value


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


UtcDatetime = Annotated[
    datetime, BeforeValidator(_strip_redundant_z), AfterValidator(_as_utc)
]


class ShareLinkCreateRequest(BaseModel):
    hypothesis_id: str
    label: Annotated[str, StringConstraints(strip_whitespace=True, max_length=100)] | None = None
    expires_in_days: int | None = Field(
        30, ge=1, le=365, description="Days until the link expires; null never expires."
    )


class ShareLinkResponse(BaseModel):
    id: str
    token: str
    hypothesis_id: str
    label: str | None = None
    created_at: UtcDatetime
    expires_at: UtcDatetime | None = None
    revoked_at: UtcDatetime | None = None
    view_count: int = 0
    last_viewed_at: UtcDatetime | None = None

    @computed_field
    @property
    def url_path(self) -> str:
        return f"/s/{self.token}"

    @computed_field
    @property
    def state(self) -> Literal["active", "expired", "revoked"]:
        if self.revoked_at:
            return "revoked"
        if self.expires_at and self.expires_at <= datetime.now(timezone.utc):
            return "expired"
        return "active"


class ShareLinkListResponse(BaseModel):
    links: list[ShareLinkResponse]


class _Shared(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SharedGraphNode(_Shared):
    id: str
    type: str | None = None
    name: str | None = None


class SharedGraphEdge(_Shared):
    source: str
    target: str
    label: str | None = None


class SharedGraph(_Shared):
    nodes: list[SharedGraphNode] = Field(default_factory=list)
    edges: list[SharedGraphEdge] = Field(default_factory=list)
    probability: float | None = None


class SharedGoTerm(_Shared):
    id: str
    name: str | None = None
    p: float | None = None
    rank: int | None = None
    genes: list[str] = Field(default_factory=list)


class SharedWarning(_Shared):
    code: str | None = None
    message: str | None = None


class SharedHypothesisStub(_Shared):
    """A related hypothesis that has not completed."""

    key: str
    causal_gene: str | None = None
    probability: float | None = None
    status: str


class SharedHypothesis(SharedHypothesisStub):
    summary: str
    graph: SharedGraph
    selected_go_term: SharedGoTerm | None = None
    go_terms: list[SharedGoTerm] = Field(default_factory=list)


class SharedMainHypothesis(SharedHypothesis):
    variant: str | None = None
    variant_rsid: str | None = None
    phenotype: str | None = None
    tissue: str | None = None
    warnings: list[SharedWarning] = Field(default_factory=list)
    generated_at: UtcDatetime | None = None


class SharedSnapshot(_Shared):
    hypothesis: SharedMainHypothesis
    related: list[SharedHypothesis | SharedHypothesisStub] = Field(default_factory=list)
    related_truncated: bool = False


class SharedHypothesisResponse(SharedSnapshot):
    schema_version: int
    shared_at: UtcDatetime
    expires_at: UtcDatetime | None = None
