from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from loguru import logger

from src.api.dependencies import (
    get_enrichment_handler,
    get_gene_expression_handler,
    get_hypothesis_handler,
    get_project_handler,
)
from src.api.auth import get_current_user_id
from src.db import EnrichmentHandler, GeneExpressionHandler, HypothesisHandler, ProjectHandler
from src.run_deployment import invoke_enrichment_deployment
from src.utils import serialize_datetime_fields

router = APIRouter()


@router.get("/enrich")
async def get_enrich(
    id: str | None = Query(None),
    project_id: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
    enrichment: EnrichmentHandler = Depends(get_enrichment_handler),
):

    if id:
        enrich = enrichment.get_enrich(current_user_id, id)
        if not enrich:
            raise HTTPException(status_code=404, detail="Enrich not found or access denied.")
        return serialize_datetime_fields(enrich)

    if project_id:
        enrichments = enrichment.get_enrich(user_id=current_user_id)
        if isinstance(enrichments, list):
            project_enrichments = [
                e for e in enrichments if e.get("project_id") == project_id
            ]
            return {"enrichments": serialize_datetime_fields(project_enrichments)}
        else:
            if enrichments and enrichments.get("project_id") == project_id:
                return {"enrichments": [serialize_datetime_fields(enrichments)]}
            return {"enrichments": []}

    enrich = enrichment.get_enrich(user_id=current_user_id)
    return serialize_datetime_fields(enrich)


@router.post("/enrich", status_code=202)
async def post_enrich(
    request: Request,
    current_user_id: str = Depends(get_current_user_id),
    projects: ProjectHandler = Depends(get_project_handler),
    hypotheses: HypothesisHandler = Depends(get_hypothesis_handler),
    gene_expression: GeneExpressionHandler = Depends(get_gene_expression_handler),
):
    body: dict = {}
    try:
        body = await request.json()
    except Exception:
        pass

    variant = request.query_params.get("variant") or body.get("variant")
    project_id = request.query_params.get("project_id") or body.get("project_id")
    seed = int(body.get("seed", 42))

    if not project_id:
        raise HTTPException(status_code=400, detail="project_id is required")
    if not variant:
        raise HTTPException(status_code=400, detail="variant is required")

    project = projects.get_projects(current_user_id, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found or access denied")

    phenotype = project["phenotype"]

    tissue_name = request.query_params.get("tissue_name") or body.get("tissue_name")
    if not tissue_name:
        raise HTTPException(status_code=400, detail="tissue_name is required")

    try:
        available_tissues = gene_expression.get_ldsc_results_for_project(
            current_user_id, project_id, limit=20, format="selection"
        )
        tissue_names = [t.get("tissue_name") for t in (available_tissues or [])]
        if tissue_name not in tissue_names:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid tissue selection. Available tissues: {tissue_names}",
            )
        gene_expression.save_tissue_selection(
            current_user_id, project_id, variant, tissue_name
        )
        logger.info(f"Saved tissue selection in /enrich: {tissue_name} for variant {variant}")
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(f"Failed to save/validate tissue selection: {exc}")

    existing_hypothesis = hypotheses.get_hypothesis_by_phenotype_and_variant_in_project(
        current_user_id, project_id, phenotype, variant
    )

    if existing_hypothesis:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: invoke_enrichment_deployment(
                current_user_id=current_user_id,
                phenotype=phenotype,
                variant=variant,
                hypothesis_id=existing_hypothesis["id"],
                project_id=project_id,
                seed=seed,
            ),
        )
        return {"hypothesis_id": existing_hypothesis["id"], "project_id": project_id}

    hypothesis_id = str(uuid4())
    hypothesis_data = {
        "id": hypothesis_id,
        "project_id": project_id,
        "phenotype": phenotype,
        "variant": variant,
        "variant_rsid": variant,
        "status": "pending",
        "created_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds") + "Z",
        "task_history": [],
    }

    hypotheses.create_hypothesis(current_user_id, hypothesis_data)

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        lambda: invoke_enrichment_deployment(
            current_user_id=current_user_id,
            phenotype=phenotype,
            variant=variant,
            hypothesis_id=hypothesis_id,
            project_id=project_id,
            seed=seed,
        ),
    )
    return {"hypothesis_id": hypothesis_id, "project_id": project_id}


@router.delete("/enrich")
async def delete_enrich(
    id: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
    enrichment: EnrichmentHandler = Depends(get_enrichment_handler),
):
    if id:
        result = enrichment.delete_enrich(current_user_id, id)
        return result
    raise HTTPException(status_code=400, detail="enrich id is required!")
