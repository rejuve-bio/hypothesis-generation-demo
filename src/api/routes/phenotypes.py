from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query
from loguru import logger

from src.api.dependencies import _deps
from src.utils import serialize_datetime_fields

router = APIRouter()


@router.get("/phenotypes")
async def get_phenotypes(
    id: str | None = Query(None),
    search: str | None = Query(None),
    limit: int | None = Query(None),
    skip: int = Query(0),
):
    phenotypes = _deps["phenotypes"]
    try:
        if id:
            phenotype = phenotypes.get_phenotypes(phenotype_id=id)
            if not phenotype:
                raise HTTPException(status_code=404, detail="Phenotype not found")
            return serialize_datetime_fields({"phenotype": phenotype})

        if limit is None:
            limit = 100

        all_phenotypes = phenotypes.get_phenotypes(
            limit=limit, skip=skip, search_term=search
        )
        total_count = phenotypes.count_phenotypes(search_term=search)

        response: dict = {
            "phenotypes": all_phenotypes,
            "total_count": total_count,
            "skip": skip,
            "limit": limit,
            "has_more": (skip + len(all_phenotypes)) < total_count,
            "next_skip": (
                skip + len(all_phenotypes)
                if (skip + len(all_phenotypes)) < total_count
                else None
            ),
        }
        if search:
            response["search_term"] = search

        return serialize_datetime_fields(response)

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error getting phenotypes: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to get phenotypes: {exc}"
        )


@router.post("/phenotypes", status_code=201)
async def post_phenotypes(data: list = Body(...)):
    phenotypes = _deps["phenotypes"]
    try:
        if not isinstance(data, list):
            raise HTTPException(
                status_code=400, detail="Expected JSON array of phenotypes"
            )

        phenotypes_data = []
        for item in data:
            if not isinstance(item, dict):
                continue
            phenotype = {"id": item.get("id", ""), "phenotype_name": item.get("name", "")}
            if phenotype["id"] and phenotype["phenotype_name"]:
                phenotypes_data.append(phenotype)
            else:
                logger.warning(f"Skipping invalid phenotype entry: {item}")

        if not phenotypes_data:
            raise HTTPException(
                status_code=400, detail="No valid phenotypes found in JSON data"
            )

        result = phenotypes.bulk_create_phenotypes(phenotypes_data)
        return {
            "message": "Phenotypes loaded successfully",
            "inserted_count": result["inserted_count"],
            "skipped_count": result["skipped_count"],
            "total_provided": len(phenotypes_data),
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"Error loading phenotypes: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to load phenotypes: {exc}"
        )
