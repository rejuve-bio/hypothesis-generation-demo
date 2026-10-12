from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Union
from uuid import uuid4

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from loguru import logger

from src.api.dependencies import (
    get_demo_template_handler,
    get_enrichment_handler,
    get_gene_expression_handler,
    get_hypothesis_handler,
    get_project_handler,
)
from src.api.auth import get_current_user_id
from src.api.schemas import (
    EnrichPostAcceptedResponse,
    EnrichPostBody,
    EnrichmentsListResponse,
    FlexibleDict,
    FlexibleList,
    MessageResponse,
)
from src.db import (
    DemoTemplateHandler,
    EnrichmentHandler,
    GeneExpressionHandler,
    HypothesisHandler,
    ProjectHandler,
)
from src.services.demo import (
    resolve_fork_project_id,
    resolve_project_access,
    resolve_project_access_or_none,
)
from src.run_deployment import invoke_enrichment_deployment
from src.utils import serialize_datetime_fields
from src.config import Config
from src.catlas_census_mapping import CatlasMappingError, validate_ldsc_tissue_mapping

router = APIRouter(tags=["enrichment"])


@router.get(
    "/enrich",
    response_model=Union[FlexibleDict, FlexibleList, EnrichmentsListResponse],
    summary="Get enrichment record(s)",
)
async def get_enrich(
    id: str | None = Query(None),
    project_id: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
    enrichment: EnrichmentHandler = Depends(get_enrichment_handler),
    demo_templates: DemoTemplateHandler = Depends(get_demo_template_handler),
):

    if id:
        enrich = enrichment.get_enrich(current_user_id, id)
        if not enrich:
            enrich = enrichment.get_enrich(enrich_id=id)
            if enrich and enrich.get("project_id"):
                access = resolve_project_access_or_none(
                    demo_templates, current_user_id, enrich["project_id"]
                )
                if not access or enrich.get("user_id") != access.owner_user_id:
                    enrich = None
        if not enrich:
            raise HTTPException(status_code=404, detail="Enrich not found or access denied.")
        return FlexibleDict.model_validate(serialize_datetime_fields(enrich))

    if project_id:
        access = resolve_project_access(demo_templates, current_user_id, project_id)
        data_user_id = access.owner_user_id
        enrichments = enrichment.get_enrich(user_id=data_user_id)
        if isinstance(enrichments, list):
            project_enrichments = [
                e for e in enrichments if e.get("project_id") == project_id
            ]
            return EnrichmentsListResponse(
                enrichments=serialize_datetime_fields(project_enrichments)
            )
        else:
            if enrichments and enrichments.get("project_id") == project_id:
                return EnrichmentsListResponse(
                    enrichments=[serialize_datetime_fields(enrichments)]
                )
            return EnrichmentsListResponse(enrichments=[])

    enrich = enrichment.get_enrich(user_id=current_user_id)
    return FlexibleList.model_validate(serialize_datetime_fields(enrich))


@router.post(
    "/enrich",
    status_code=202,
    response_model=EnrichPostAcceptedResponse,
    summary="Start enrichment for a variant",
)
async def post_enrich(
    request: Request,
    body: EnrichPostBody | None = Body(None),
    variant: str | None = Query(None),
    project_id: str | None = Query(None),
    tissue_name: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
    projects: ProjectHandler = Depends(get_project_handler),
    hypotheses: HypothesisHandler = Depends(get_hypothesis_handler),
    gene_expression: GeneExpressionHandler = Depends(get_gene_expression_handler),
    demo_templates: DemoTemplateHandler = Depends(get_demo_template_handler),
):
    payload = body.model_dump() if body else {}
    if not payload:
        try:
            payload = await request.json()
        except Exception:
            payload = {}

    variant = variant or payload.get("variant")
    project_id = project_id or payload.get("project_id")
    seed = int(payload.get("seed", 42))

    if not project_id:
        raise HTTPException(status_code=400, detail="project_id is required")
    if not variant:
        raise HTTPException(status_code=400, detail="variant is required")

    access = resolve_project_access(demo_templates, current_user_id, project_id)
    forked = False
    if access.template and access.is_demo_read:
        project_id, forked = resolve_fork_project_id(
            demo_templates=demo_templates,
            projects=projects,
            current_user_id=current_user_id,
            project_id=project_id,
            template=access.template,
        )

    project = projects.get_projects(current_user_id, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found or access denied")

    phenotype = project["phenotype"]

    tissue_name = tissue_name or payload.get("tissue_name")
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
        config = Config.from_env()
        try:
            validate_ldsc_tissue_mapping(
                tissue_name,
                repo_root=config.repo_root,
                mapping_json_rel=config.catlas_celltype_cl_mapping_json,
                catlas_aliases_rel=config.catlas_abc_aliases_tsv,
            )
        except CatlasMappingError as exc:
            raise HTTPException(status_code=422, detail=exc.as_detail()) from exc
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
        return EnrichPostAcceptedResponse(
            hypothesis_id=existing_hypothesis["id"],
            project_id=project_id,
            forked=forked,
        )

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
    return EnrichPostAcceptedResponse(
        hypothesis_id=hypothesis_id,
        project_id=project_id,
        forked=forked,
    )


@router.delete(
    "/enrich",
    response_model=MessageResponse,
    responses={404: {"model": MessageResponse}},
)
async def delete_enrich(
    id: str | None = Query(None),
    current_user_id: str = Depends(get_current_user_id),
    enrichment: EnrichmentHandler = Depends(get_enrichment_handler),
):
    if id:
        result, status_code = enrichment.delete_enrich(current_user_id, id)
        return JSONResponse(content=result, status_code=status_code)
    raise HTTPException(status_code=400, detail="enrich id is required!")
