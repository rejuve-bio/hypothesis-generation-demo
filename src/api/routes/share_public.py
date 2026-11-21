from __future__ import annotations

import re

from fastapi import APIRouter, Depends, HTTPException, Response

from src.api.dependencies import get_share_link_handler
from src.api.schemas import ErrorResponse
from src.api.schemas.share import SharedHypothesisResponse
from src.db import ShareLinkHandler

router = APIRouter(prefix="/public", tags=["share"])

_TOKEN = re.compile(r"[A-Za-z0-9_-]{43}")
_PUBLIC_HEADERS = {
    "Cache-Control": "no-store",
    "X-Robots-Tag": "noindex, nofollow",
    "Referrer-Policy": "no-referrer",
}


@router.get(
    "/share/{token}",
    response_model=SharedHypothesisResponse,
    responses={404: {"model": ErrorResponse}},
    summary="View a shared hypothesis (no authentication)",
)
async def get_shared_hypothesis(
    token: str,
    response: Response,
    share_links: ShareLinkHandler = Depends(get_share_link_handler),
):
    link = share_links.open_link(token) if _TOKEN.fullmatch(token) else None
    if not link:
        raise HTTPException(
            status_code=404,
            detail="This link is no longer available.",
            headers=_PUBLIC_HEADERS,
        )
    response.headers.update(_PUBLIC_HEADERS)
    return SharedHypothesisResponse(
        **link["snapshot"],
        schema_version=link["schema_version"],
        shared_at=link["created_at"],
        expires_at=link["expires_at"],
    )
