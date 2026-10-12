from __future__ import annotations

from dependency_injector import containers, providers

from src.config import Config
from src.services.llm import LLM
from src.services.prolog import PrologQuery
from src.services.storage import create_minio_client_from_env
from src.db import (
    UserHandler,
    ProjectHandler,
    FileHandler,
    AnalysisHandler,
    EnrichmentHandler,
    HypothesisHandler,
    SummaryHandler,
    TaskHandler,
    GeneExpressionHandler,
    GWASLibraryHandler,
    PhenotypeHandler,
    DemoTemplateHandler,
)


def _create_enrich(ensembl_hgnc_map, hgnc_ensembl_map, go_map):
    from src.services.enrich import Enrich
    return Enrich(ensembl_hgnc_map, hgnc_ensembl_map, go_map)


class Container(containers.DeclarativeContainer):
    wiring_config = containers.WiringConfiguration(
        modules=["src.api.socketio"]
    )

    config = providers.Dependency(instance_of=Config)

    llm = providers.ThreadSafeSingleton(LLM)

    prolog_query = providers.ThreadSafeSingleton(
        PrologQuery,
        host=config.provided.swipl_host,
        port=config.provided.swipl_port,
    )

    enrichr = providers.ThreadSafeSingleton(
        _create_enrich,
        config.provided.ensembl_hgnc_map,
        config.provided.hgnc_ensembl_map,
        config.provided.go_map,
    )

    storage = providers.ThreadSafeSingleton(create_minio_client_from_env)

    user_handler = providers.ThreadSafeSingleton(
        UserHandler, config.provided.mongodb_uri, config.provided.db_name
    )
    project_handler = providers.ThreadSafeSingleton(
        ProjectHandler, config.provided.mongodb_uri, config.provided.db_name
    )
    file_handler = providers.ThreadSafeSingleton(
        FileHandler, config.provided.mongodb_uri, config.provided.db_name
    )
    analysis_handler = providers.ThreadSafeSingleton(
        AnalysisHandler, config.provided.mongodb_uri, config.provided.db_name
    )
    enrichment_handler = providers.ThreadSafeSingleton(
        EnrichmentHandler, config.provided.mongodb_uri, config.provided.db_name
    )
    hypothesis_handler = providers.ThreadSafeSingleton(
        HypothesisHandler, config.provided.mongodb_uri, config.provided.db_name
    )
    summary_handler = providers.ThreadSafeSingleton(
        SummaryHandler, config.provided.mongodb_uri, config.provided.db_name
    )
    task_handler = providers.ThreadSafeSingleton(
        TaskHandler, config.provided.mongodb_uri, config.provided.db_name
    )
    gene_expression_handler = providers.ThreadSafeSingleton(
        GeneExpressionHandler, config.provided.mongodb_uri, config.provided.db_name
    )
    phenotype_handler = providers.ThreadSafeSingleton(
        PhenotypeHandler, config.provided.mongodb_uri, config.provided.db_name
    )
    gwas_library_handler = providers.ThreadSafeSingleton(
        GWASLibraryHandler, config.provided.mongodb_uri, config.provided.db_name
    )
    demo_template_handler = providers.ThreadSafeSingleton(
        DemoTemplateHandler, config.provided.mongodb_uri, config.provided.db_name
    )
