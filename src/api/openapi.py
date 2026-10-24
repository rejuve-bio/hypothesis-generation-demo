"""OpenAPI metadata, tag definitions, and security scheme configuration."""

from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from src.api.openapi_websocket import build_websocket_api_docs

OPENAPI_TAGS: list[dict[str, str]] = [
    {
        "name": "health",
        "description": "Liveness and readiness checks.",
    },
    {
        "name": "projects",
        "description": (
            "Project CRUD, bulk delete, and GWAS analysis pipeline kickoff "
            "(harmonization, fine-mapping)."
        ),
    },
    {
        "name": "hypothesis",
        "description": (
            "Hypothesis generation from enrichment results, retrieval, deletion, "
            "and LLM chat over a hypothesis."
        ),
    },
    {
        "name": "enrichment",
        "description": (
            "Gene-set enrichment analysis for a project locus. Long-running jobs "
            "return 202; subscribe via Socket.IO for progress."
        ),
    },
    {
        "name": "analysis",
        "description": "Post-GWAS analysis outputs such as credible sets.",
    },
    {
        "name": "gwas_library",
        "description": (
            "Public GWAS catalog (sources, file listing, download). "
            "Sample-size metadata requires authentication."
        ),
    },
    {
        "name": "phenotypes",
        "description": "Phenotype lookup and creation (public endpoints).",
    },
    {
        "name": "user_files",
        "description": "User-uploaded GWAS and related files.",
    },
]

_AUTH_DOCS = """\
## Authentication

Most endpoints require a **JWT Bearer token**. Click **Authorize** above and enter:

```
<your-jwt-token>
```

Or send the header:

```
Authorization: Bearer <JWT>
```

The token must be signed with HS256 using `JWT_SECRET` and include a `user_id` claim.
Optional claims: `email`.
"""

def get_api_servers() -> list[dict[str, str]]:
    """OpenAPI `servers` list.

    Empty by default so Swagger UI resolves requests against the origin the docs page
    was loaded from (works behind nginx, a published port, or localhost with no config).
    Set OPENAPI_SERVER_URL only when the docs must point at a different base URL.
    """
    explicit = os.getenv("OPENAPI_SERVER_URL", "").strip().rstrip("/")
    if not explicit:
        return []
    return [{"url": explicit, "description": "Hypothesis Generation API"}]


def _ws_origin_from_http(url: str) -> str:
    url = url.rstrip("/")
    if url.startswith("https://"):
        return "wss://" + url[len("https://") :]
    if url.startswith("http://"):
        return "ws://" + url[len("http://") :]
    return url


def build_api_description() -> str:
    """Compose the OpenAPI description (authentication + WebSocket summary)."""
    servers = get_api_servers()
    ws_origin = _ws_origin_from_http(servers[0]["url"]) if servers else None
    return (
        "Hypothesis Generation API for GWAS-driven gene hypothesis and enrichment workflows.\n\n"
        "Integrates enrichment analysis, knowledge-graph reasoning (Prolog), and LLM summarization "
        "to produce hypothesis graphs and narratives from genetic association data.\n\n"
        + _AUTH_DOCS
        + "\n"
        + build_websocket_api_docs(ws_origin)
    )


_BEARER_DESCRIPTION = (
    "JWT access token with a required `user_id` claim, signed with HS256 using the "
    "server `JWT_SECRET`. Paste the raw token here; Swagger adds the `Bearer ` prefix."
)


def configure_openapi(app: FastAPI) -> None:
    """Register a custom OpenAPI generator that documents the Bearer JWT scheme.

    Per-operation security requirements are left to FastAPI, which derives them from the
    `HTTPBearer` dependency on each route, so protected and public endpoints stay accurate
    without a hand-maintained list.
    """

    def custom_openapi() -> dict:
        if app.openapi_schema:
            return app.openapi_schema

        schema = get_openapi(
            title=app.title,
            version=app.version,
            openapi_version=app.openapi_version,
            description=app.description,
            routes=app.routes,
            tags=app.openapi_tags,
            servers=app.servers,
        )

        security_schemes = schema.setdefault("components", {}).setdefault(
            "securitySchemes", {}
        )
        for scheme in security_schemes.values():
            if scheme.get("type") == "http" and scheme.get("scheme") == "bearer":
                scheme.setdefault("bearerFormat", "JWT")
                scheme["description"] = _BEARER_DESCRIPTION

        app.openapi_schema = schema
        return schema

    app.openapi = custom_openapi  # type: ignore[method-assign]
