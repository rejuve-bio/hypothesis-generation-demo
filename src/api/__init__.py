from fastapi import APIRouter

from api import socketio  # noqa: F401 — registers @sio.on handlers
from api.routes.analysis import router as analysis_router
from api.routes.enrichment import router as enrichment_router
from api.routes.files import router as files_router
from api.routes.gwas import router as gwas_router
from api.routes.hypothesis import router as hypothesis_router
from api.routes.internal import router as internal_router
from api.routes.phenotypes import router as phenotypes_router
from api.routes.projects import router as projects_router

router = APIRouter()
router.include_router(enrichment_router)
router.include_router(hypothesis_router)
router.include_router(projects_router)
router.include_router(phenotypes_router)
router.include_router(gwas_router)
router.include_router(files_router)
router.include_router(analysis_router)
router.include_router(internal_router)
