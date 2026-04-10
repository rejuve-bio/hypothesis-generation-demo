from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger

from api.dependencies import _deps
from api.auth import get_current_user_id
from src.utils import convert_variants_to_object_array, serialize_datetime_fields

router = APIRouter()


@router.get("/credible-sets")
async def get_credible_sets(
    project_id: str | None = Query(None),
    credible_set_id: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
):
    analysis = _deps["analysis"]

    if not project_id:
        raise HTTPException(status_code=400, detail="project_id is required")
    if not credible_set_id:
        raise HTTPException(status_code=400, detail="Credible_set_id is required")

    try:
        credible_set = analysis.get_credible_set_by_id(
            current_user_id, project_id, credible_set_id
        )
        if not credible_set:
            raise HTTPException(
                status_code=404, detail="No credible set found with this ID"
            )

        variants_data = credible_set.get("variants_data", {})
        if not variants_data:
            raise HTTPException(
                status_code=404,
                detail="No variants data found for this credible set",
            )

        variants = variants_data.get("data", {})
        variants_array = convert_variants_to_object_array(variants)
        variants_array = serialize_datetime_fields(variants_array)
        return {"variants": variants_array}

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error fetching credible set: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch credible set: {exc}"
        )
