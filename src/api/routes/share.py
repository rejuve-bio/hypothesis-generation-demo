from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from src.api.auth import get_current_user_id
from src.api.dependencies import (
    get_enrichment_handler,
    get_gene_expression_handler,
    get_hypothesis_handler,
    get_share_link_handler,
)
from src.api.schemas import ErrorResponse
from src.api.schemas.share import (
    ShareLinkCreateRequest,
    ShareLinkListResponse,
    ShareLinkResponse,
    SharedSnapshot,
)
from src.db import (
    EnrichmentHandler,
    GeneExpressionHandler,
    HypothesisHandler,
    ShareLinkHandler,
)
from src.services.share_snapshot import build_share_snapshot, is_completed

router = APIRouter(prefix="/share-links", tags=["share"])

MAX_ACTIVE_LINKS = 20


def _conflict(code: str, message: str) -> HTTPException:
    return HTTPException(status_code=409, detail={"code": code, "message": message})


@router.post(
    "",
    status_code=201,
    response_model=ShareLinkResponse,
    responses={
        404: {"model": ErrorResponse},
        409: {"description": "`hypothesis_not_completed` or `too_many_links` in `detail.code`."},
    },
    summary="Create a public share link for a completed hypothesis",
)
async def create_share_link(
    body: ShareLinkCreateRequest,
    current_user_id: str = Depends(get_current_user_id),
    share_links: ShareLinkHandler = Depends(get_share_link_handler),
    hypotheses: HypothesisHandler = Depends(get_hypothesis_handler),
    enrichment: EnrichmentHandler = Depends(get_enrichment_handler),
    gene_expression: GeneExpressionHandler = Depends(get_gene_expression_handler),
):
    hypothesis = hypotheses.get_hypotheses(current_user_id, body.hypothesis_id)
    if not hypothesis:
        raise HTTPException(status_code=404, detail="Hypothesis not found or access denied.")
    if not is_completed(hypothesis):
        raise _conflict("hypothesis_not_completed", "Only completed hypotheses can be shared.")
    if share_links.count_active_links(current_user_id, body.hypothesis_id) >= MAX_ACTIVE_LINKS:
        raise _conflict(
            "too_many_links",
            f"A hypothesis can have at most {MAX_ACTIVE_LINKS} active share links.",
        )

    expires_at = (
        datetime.now(timezone.utc) + timedelta(days=body.expires_in_days)
        if body.expires_in_days
        else None
    )
    link = share_links.create_link(
        owner_user_id=current_user_id,
        hypothesis_id=body.hypothesis_id,
        project_id=hypothesis.get("project_id"),
        label=body.label or None,
        expires_at=expires_at,
        snapshot=SharedSnapshot.model_validate(
            build_share_snapshot(hypothesis, hypotheses, enrichment, gene_expression)
        ).model_dump(),
    )
    return ShareLinkResponse.model_validate(link)


@router.get(
    "",
    response_model=ShareLinkListResponse,
    summary="List a hypothesis's share links",
)
async def list_share_links(
    hypothesis_id: str = Query(...),
    current_user_id: str = Depends(get_current_user_id),
    share_links: ShareLinkHandler = Depends(get_share_link_handler),
):
    return ShareLinkListResponse(links=share_links.list_links(current_user_id, hypothesis_id))


@router.delete(
    "/{link_id}",
    response_model=ShareLinkResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Revoke a share link",
)
async def revoke_share_link(
    link_id: str,
    current_user_id: str = Depends(get_current_user_id),
    share_links: ShareLinkHandler = Depends(get_share_link_handler),
):
    link = share_links.revoke_link(current_user_id, link_id)
    if not link:
        raise HTTPException(status_code=404, detail="Share link not found.")
    return ShareLinkResponse.model_validate(link)
