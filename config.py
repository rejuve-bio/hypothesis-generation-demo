import os
import argparse
from llm import LLM
from query_swipl import PrologQuery
from db import (
    UserHandler, ProjectHandler, FileHandler, AnalysisHandler,
    EnrichmentHandler, HypothesisHandler, SummaryHandler, TaskHandler
)

class Config:
    """Centralized configuration for the application"""
    
    def __init__(self):
        self.ensembl_hgnc_map = None
        self.hgnc_ensembl_map = None
        self.go_map = None
        self.swipl_host = "localhost"
        self.swipl_port = 4242
        self.mongodb_uri = None
        self.db_name = None
        self.embedding_model = "w601sxs/b1ade-embed-kd"
        self.plink_dir = "./data/1000Genomes_phase3/plink_format_b37"
        self.data_dir = "./data"
        self.host = "0.0.0.0"
        self.port = 5000

    @classmethod
    def from_args(cls, args):
        """Create config from command line arguments"""
        config = cls()
        config.ensembl_hgnc_map = args.ensembl_hgnc_map
        config.hgnc_ensembl_map = args.hgnc_ensembl_map
        config.go_map = args.go_map
        config.swipl_host = args.swipl_host
        config.swipl_port = args.swipl_port
        config.embedding_model = getattr(args, 'embedding_model', config.embedding_model)
        # Flask-specific arguments (if present)
        config.host = getattr(args, 'host', config.host)
        config.port = getattr(args, 'port', config.port)
        # Also load MongoDB config from environment
        config.mongodb_uri = os.getenv("MONGODB_URI")
        config.db_name = os.getenv("DB_NAME")
        return config

    @classmethod
    def from_env(cls):
        """Create config from environment variables"""
        config = cls()
        config.ensembl_hgnc_map = os.getenv("ENSEMBL_HGNC_MAP")
        config.hgnc_ensembl_map = os.getenv("HGNC_ENSEMBL_MAP")
        config.go_map = os.getenv("GO_MAP")
        config.swipl_host = os.getenv("SWIPL_HOST", "localhost")
        config.swipl_port = int(os.getenv("SWIPL_PORT", "4242"))
        config.mongodb_uri = os.getenv("MONGODB_URI")
        config.db_name = os.getenv("DB_NAME")
        config.embedding_model = os.getenv("EMBEDDING_MODEL", "w601sxs/b1ade-embed-kd")
        config.plink_dir = os.getenv("PLINK_DIR", "./data/1000Genomes_phase3/plink_format_b37")
        config.data_dir = os.getenv("DATA_DIR", "./data")
        return config


def create_dependencies(config):
    """Factory function to create all dependencies from config"""
    # Import here to avoid circular dependency
    from enrich import Enrich
    
    enrichr = Enrich(
        config.ensembl_hgnc_map,
        config.hgnc_ensembl_map,
        config.go_map
    )
    
    llm = LLM()
    
    prolog_query = PrologQuery(
        host=config.swipl_host,
        port=config.swipl_port
    )
    
    # Use environment variables for MongoDB connection
    mongodb_uri = config.mongodb_uri 
    db_name = config.db_name
    
    return {
        'enrichr': enrichr,
        'llm': llm,
        'prolog_query': prolog_query,
        'users': UserHandler(mongodb_uri, db_name),
        'projects': ProjectHandler(mongodb_uri, db_name),
        'files': FileHandler(mongodb_uri, db_name),
        'analysis': AnalysisHandler(mongodb_uri, db_name),
        'enrichment': EnrichmentHandler(mongodb_uri, db_name),
        'hypotheses': HypothesisHandler(mongodb_uri, db_name),
        'summaries': SummaryHandler(mongodb_uri, db_name),
        'tasks': TaskHandler(mongodb_uri, db_name)
    }
