import os
import argparse
from llm import LLM
from query_swipl import PrologQuery
from db import (
    UserHandler, ProjectHandler, FileHandler, AnalysisHandler,
    EnrichmentHandler, HypothesisHandler, SummaryHandler, TaskHandler,
    GeneExpressionHandler,
    PhenotypeHandler
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
        self.plink_dir_37 = "./data/1000Genomes_phase3/plink_format_b37"
        self.plink_dir_38 = "./data/1000Genomes_phase3/plink_format_b38"
        self.data_dir = "./data"
        self.ontology_cache_dir = "./data/ontology"
        self.host = "0.0.0.0"
        self.port = 5000
        # Harmonization workflow configuration
        self.harmonizer_ref_dir_37 = "/data/harmonizer_ref/b37"
        self.harmonizer_ref_dir_38 = "/data/harmonizer_ref/b38"
        self.harmonizer_code_repo = "./gwas-sumstats-harmoniser"  # Nextflow workflow
        self.harmonizer_script_dir = "./scripts/1000Genomes_phase3"  # Shell scripts

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
        config.plink_dir_37 = os.getenv("PLINK_DIR_37", "./data/1000Genomes_phase3/plink_format_b37")
        config.plink_dir_38 = os.getenv("PLINK_DIR_38", "./data/1000Genomes_phase3/plink_format_b38")
        config.data_dir = os.getenv("DATA_DIR", "./data")
        config.ontology_cache_dir = os.getenv("ONTOLOGY_CACHE_DIR", "./data/ontology")
        # Harmonization workflow configuration
        config.harmonizer_ref_dir_37 = os.getenv("HARMONIZER_REF_DIR_37", "/data/harmonizer_ref/b37")
        config.harmonizer_ref_dir_38 = os.getenv("HARMONIZER_REF_DIR_38", "/data/harmonizer_ref/b38")
        config.harmonizer_code_repo = os.getenv("HARMONIZER_CODE_REPO", "./gwas-sumstats-harmoniser")  # Nextflow workflow
        config.harmonizer_script_dir = os.getenv("HARMONIZER_SCRIPT_DIR", "./scripts/1000Genomes_phase3")  # Shell scripts
        return config

    def get_plink_dir(self, ref_genome):
        """Get the PLINK directory for the specified genome build"""
        if ref_genome == "GRCh38":
            return self.plink_dir_38
        elif ref_genome == "GRCh37":
            return self.plink_dir_37
        else:
            raise ValueError(f"Unsupported reference genome: {ref_genome}. Must be 'GRCh37' or 'GRCh38'")

    def get_harmonizer_ref_dir(self, ref_genome):
        """Get the harmonizer reference directory for the specified genome build"""
        if ref_genome == "GRCh38":
            return self.harmonizer_ref_dir_38
        elif ref_genome == "GRCh37":
            return self.harmonizer_ref_dir_37
        else:
            raise ValueError(f"Unsupported reference genome: {ref_genome}. Must be 'GRCh37' or 'GRCh38'")

    def get_plink_file_pattern(self, ref_genome, population, chrom):
        """
        Get the PLINK file pattern for the specified genome build.
        """
        if ref_genome == "GRCh38":
            return f"{population}.chr{chrom}.1KG.GRCh38"
        elif ref_genome == "GRCh37":
            return f"{population}.{chrom}.1000Gp3.20130502"
        else:
            raise ValueError(f"Unsupported reference genome: {ref_genome}. Must be 'GRCh37' or 'GRCh38'")


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
    
    # Validate MongoDB configuration
    if not mongodb_uri or not db_name:
        raise ValueError("Missing required MongoDB configuration: MONGODB_URI and DB_NAME environment variables must be set")
    
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
        'tasks': TaskHandler(mongodb_uri, db_name),
        'gene_expression': GeneExpressionHandler(mongodb_uri, db_name),
        'phenotypes': PhenotypeHandler(mongodb_uri, db_name)
    }
