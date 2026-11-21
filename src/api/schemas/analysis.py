from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CredibleSetsResponse(BaseModel):
    variants: list[dict[str, Any]] = Field(
        ...,
        description="Variant rows for the requested credible set.",
    )
    results_genome: str | None = Field(
        None,
        description=(
            "Genome build of the variant coordinates (e.g. GRCh38). This can differ "
            "from the project's ref_genome, which is the build of the uploaded file."
        ),
    )
