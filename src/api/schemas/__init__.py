"""Pydantic request/response models for OpenAPI schemas and validation."""

from src.api.schemas.analysis import CredibleSetsResponse
from src.api.schemas.common import ErrorResponse, FlexibleDict, FlexibleList, MessageResponse
from src.api.schemas.enrichment import (
    EnrichPostAcceptedResponse,
    EnrichPostBody,
    EnrichmentsListResponse,
)
from src.api.schemas.files import UserFilesResponse
from src.api.schemas.gwas import (
    GwasDownloadUrlResponse,
    GwasFilesListResponse,
    GwasSourcesResponse,
    SampleSizeInfoResponse,
)
from src.api.schemas.hypothesis import (
    BulkDeleteHypothesesResponse,
    BulkDeleteHypothesesRequest,
    HypothesisChatForm,
    HypothesisChatResponse,
    HypothesisGraphResponse,
)
from src.api.schemas.internal import HealthResponse
from src.api.schemas.phenotypes import (
    PhenotypeBulkResponse,
    PhenotypeListResponse,
    PhenotypeSingleWrapResponse,
)
from src.api.schemas.projects import (
    AnalysisPipelineStartResponse,
    BulkDeleteProjectsOkResponse,
    BulkDeleteProjectsPartialResponse,
    BulkDeleteProjectsRequest,
    ProjectDeleteMessage,
    ProjectsListResponse,
)

__all__ = [
    "AnalysisPipelineStartResponse",
    "BulkDeleteHypothesesRequest",
    "BulkDeleteHypothesesResponse",
    "BulkDeleteProjectsOkResponse",
    "BulkDeleteProjectsPartialResponse",
    "BulkDeleteProjectsRequest",
    "CredibleSetsResponse",
    "EnrichPostAcceptedResponse",
    "EnrichPostBody",
    "EnrichmentsListResponse",
    "ErrorResponse",
    "FlexibleDict",
    "FlexibleList",
    "GwasDownloadUrlResponse",
    "GwasFilesListResponse",
    "GwasSourcesResponse",
    "HealthResponse",
    "HypothesisChatForm",
    "HypothesisChatResponse",
    "HypothesisGraphResponse",
    "MessageResponse",
    "PhenotypeBulkResponse",
    "PhenotypeListResponse",
    "PhenotypeSingleWrapResponse",
    "ProjectDeleteMessage",
    "ProjectsListResponse",
    "SampleSizeInfoResponse",
    "UserFilesResponse",
]
