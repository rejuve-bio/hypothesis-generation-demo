from __future__ import annotations

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
    DemoTemplateHandler,
)
from src.services.llm import LLM
from src.services.prolog import PrologQuery

_container: Container | None = None


def init_container(container: Container) -> None:
    """Called once at application startup to register the DI container."""
    global _container
    _container = container


def get_config() -> Config:
    return _container.config()


def get_llm() -> LLM:
    return _container.llm()


def get_prolog_query() -> PrologQuery:
    return _container.prolog_query()


def get_enrichr():
    return _container.enrichr()


def get_storage():
    return _container.storage()


def get_user_handler() -> UserHandler:
    return _container.user_handler()


def get_project_handler() -> ProjectHandler:
    return _container.project_handler()


def get_file_handler() -> FileHandler:
    return _container.file_handler()


def get_analysis_handler() -> AnalysisHandler:
    return _container.analysis_handler()


def get_enrichment_handler() -> EnrichmentHandler:
    return _container.enrichment_handler()


def get_hypothesis_handler() -> HypothesisHandler:
    return _container.hypothesis_handler()


def get_summary_handler() -> SummaryHandler:
    return _container.summary_handler()


def get_task_handler() -> TaskHandler:
    return _container.task_handler()


def get_gene_expression_handler() -> GeneExpressionHandler:
    return _container.gene_expression_handler()


def get_phenotype_handler() -> PhenotypeHandler:
    return _container.phenotype_handler()


def get_gwas_library_handler() -> GWASLibraryHandler:
    return _container.gwas_library_handler()


def get_demo_template_handler() -> DemoTemplateHandler:
    return _container.demo_template_handler()
