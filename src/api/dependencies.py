from __future__ import annotations

from dependency_injector.wiring import Provide, inject

from src.config import Config
from src.container import Container
from src.db import (
    AnalysisHandler,
    EnrichmentHandler,
    FileHandler,
    GeneExpressionHandler,
    GWASLibraryHandler,
    HypothesisHandler,
    PhenotypeHandler,
    ProjectHandler,
    SummaryHandler,
    TaskHandler,
    UserHandler,
)
from src.services.llm import LLM
from src.services.prolog import PrologQuery


@inject
def get_config(config: Config = Provide[Container.config]) -> Config:
    return config


@inject
def get_llm(llm: LLM = Provide[Container.llm]) -> LLM:
    return llm


@inject
def get_prolog_query(pq: PrologQuery = Provide[Container.prolog_query]) -> PrologQuery:
    return pq


@inject
def get_enrichr(enrichr=Provide[Container.enrichr]):
    return enrichr


@inject
def get_storage(storage=Provide[Container.storage]):
    return storage


@inject
def get_user_handler(handler: UserHandler = Provide[Container.user_handler]) -> UserHandler:
    return handler


@inject
def get_project_handler(handler: ProjectHandler = Provide[Container.project_handler]) -> ProjectHandler:
    return handler


@inject
def get_file_handler(handler: FileHandler = Provide[Container.file_handler]) -> FileHandler:
    return handler


@inject
def get_analysis_handler(handler: AnalysisHandler = Provide[Container.analysis_handler]) -> AnalysisHandler:
    return handler


@inject
def get_enrichment_handler(handler: EnrichmentHandler = Provide[Container.enrichment_handler]) -> EnrichmentHandler:
    return handler


@inject
def get_hypothesis_handler(handler: HypothesisHandler = Provide[Container.hypothesis_handler]) -> HypothesisHandler:
    return handler


@inject
def get_summary_handler(handler: SummaryHandler = Provide[Container.summary_handler]) -> SummaryHandler:
    return handler


@inject
def get_task_handler(handler: TaskHandler = Provide[Container.task_handler]) -> TaskHandler:
    return handler


@inject
def get_gene_expression_handler(handler: GeneExpressionHandler = Provide[Container.gene_expression_handler]) -> GeneExpressionHandler:
    return handler


@inject
def get_phenotype_handler(handler: PhenotypeHandler = Provide[Container.phenotype_handler]) -> PhenotypeHandler:
    return handler


@inject
def get_gwas_library_handler(handler: GWASLibraryHandler = Provide[Container.gwas_library_handler]) -> GWASLibraryHandler:
    return handler
