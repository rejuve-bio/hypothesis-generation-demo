import os
from pathlib import Path

from src.services.llm import LLM
from src.services.prolog import PrologQuery
from src.db import (
    UserHandler, ProjectHandler, FileHandler, AnalysisHandler,
    EnrichmentHandler, HypothesisHandler, SummaryHandler, TaskHandler,
    GeneExpressionHandler, GWASLibraryHandler,
    PhenotypeHandler
)
from src.services.storage import create_minio_client_from_env

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
        self.redis_url = None
        self.embedding_model = "w601sxs/b1ade-embed-kd"
        self.plink_dir_38 = "./data/1000Genomes_phase3/plink_format_b38"
        self.data_dir = "./data"
        self.ontology_cache_dir = "./data/ontology"
        # Disk cache for downloaded CellxGene Census co-expression matrices,
        # keyed by tissue/cell-type — avoids re-downloading the same
        # (cells x genes) raw-count matrix from Census for every gene queried
        # against the same tissue.
        self.census_cache_dir = "./data/census_cache"
        # Size cap for census_cache_dir, in bytes — oldest (by mtime) cached
        # tissues are evicted once the directory exceeds this. Default 20 GB.
        self.census_cache_max_bytes = 20 * 1024 * 1024 * 1024
        self.host = "0.0.0.0"
        self.port = 5000
        # Harmonization workflow configuration
        self.harmonizer_ref_dir_37 = "/app/data/harmonizer_ref/b37"
        self.harmonizer_ref_dir_38 = "/app/data/harmonizer_ref/b38"
        self.harmonizer_code_repo = "./gwas-sumstats-harmoniser"  # Nextflow workflow
        self.harmonizer_script_dir = "./scripts/1000Genomes_phase3"  # Shell scripts
        # Zhang cell-type LDSC (GRCh38 + precomputed ldscores); paths relative to repo_root
        _repo = Path(__file__).resolve().parent.parent
        self.repo_root = str(_repo)
        self.ldsc_baseline_ld_prefix = "data/OSF/baseline_v1.2/baseline."
        self.ldsc_merged_ccre_prefix = "data/ldscores/all_merged_cCREs/all_merged_cCREs."
        self.ldsc_w_ld_prefix = "data/OSF/weights/weights.hm3_noMHC."
        self.ldsc_w_hm3_snplist = "data/ldsc/data/w_hm3.snplist"
        self.ldsc_hm3_no_mhc_list = "data/1000Genomes_phase3/hm3_no_MHC.list.txt"
        self.ldsc_cts_file = "data/cts/cell_types_available.cts"
        self.cell_ontology_tsv = "data/Cell_ontology.tsv"
        self.catlas_abc_aliases_tsv = "data/catlas_abc_cell_type_aliases.tsv"
        self.catlas_celltype_cl_mapping_json = (
            "data/catlas_celltype_cl_mapping.json"
        )
        self.mail_username = ""
        self.mail_password = ""
        self.mail_from = ""
        self.mail_server = ""
        self.mail_port = 587
        self.mail_tls = True
        self.mail_ssl = False
        

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
        config.redis_url = os.getenv("REDIS_URL")

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
        config.plink_dir_38 = os.getenv("PLINK_DIR_38", "./data/1000Genomes_phase3/plink_format_b38")
        config.data_dir = os.getenv("DATA_DIR", "./data")
        config.ontology_cache_dir = os.getenv("ONTOLOGY_CACHE_DIR", "./data/ontology")
        config.census_cache_dir = os.getenv("CENSUS_CACHE_DIR", "./data/census_cache")
        config.census_cache_max_bytes = int(
            os.getenv("CENSUS_CACHE_MAX_BYTES", str(20 * 1024 * 1024 * 1024))
        )
        # Harmonization workflow configuration
        config.harmonizer_ref_dir_37 = os.getenv("HARMONIZER_REF_DIR_37", "/app/data/harmonizer_ref/b37")
        config.harmonizer_ref_dir_38 = os.getenv("HARMONIZER_REF_DIR_38", "/app/data/harmonizer_ref/b38")
        config.harmonizer_code_repo = os.getenv("HARMONIZER_CODE_REPO", "./gwas-sumstats-harmoniser")  # Nextflow workflow
        config.harmonizer_script_dir = os.getenv("HARMONIZER_SCRIPT_DIR", "./scripts/1000Genomes_phase3")  # Shell scripts
        config.redis_url = os.getenv("REDIS_URL")
        _repo = Path(__file__).resolve().parent.parent
        config.repo_root = os.getenv("REPO_ROOT", str(_repo))
        config.ldsc_baseline_ld_prefix = os.getenv(
            "LDSC_BASELINE_LD_PREFIX", "data/OSF/baseline_v1.2/baseline."
        )
        config.ldsc_merged_ccre_prefix = os.getenv(
            "LDSC_MERGED_CCRE_PREFIX", "data/ldscores/all_merged_cCREs/all_merged_cCREs."
        )
        config.ldsc_w_ld_prefix = os.getenv(
            "LDSC_W_LD_PREFIX", "data/OSF/weights/weights.hm3_noMHC."
        )
        config.ldsc_w_hm3_snplist = os.getenv(
            "LDSC_W_HM3_SNPLIST", "data/ldsc/data/w_hm3.snplist"
        )
        config.ldsc_hm3_no_mhc_list = os.getenv(
            "LDSC_HM3_NO_MHC_LIST", "data/1000Genomes_phase3/hm3_no_MHC.list.txt"
        )
        config.ldsc_cts_file = os.getenv("LDSC_CTS_FILE", "data/cts/cell_types_available.cts")
        config.cell_ontology_tsv = os.getenv("CELL_ONTOLOGY_TSV", "data/Cell_ontology.tsv")
        config.catlas_abc_aliases_tsv = os.getenv(
            "CATLAS_ABC_ALIASES_TSV", "data/catlas_abc_cell_type_aliases.tsv"
        )
        config.catlas_celltype_cl_mapping_json = os.getenv(
            "CATLAS_CELLTYPE_CL_MAPPING_JSON",
            "data/catlas_celltype_cl_mapping.json",
        )
        config.mail_username = os.getenv("MAIL_USERNAME", "")
        config.mail_password = os.getenv("MAIL_PASSWORD", "")
        config.mail_from = os.getenv("MAIL_FROM", "")
        config.mail_server = os.getenv("MAIL_SERVER", "")
        config.mail_port = int(os.getenv("MAIL_PORT", "587"))
        config.mail_tls = os.getenv("MAIL_TLS", "true").lower() == "true"
        config.mail_ssl = os.getenv("MAIL_SSL", "false").lower() == "true"
        return config

    def get_harmonizer_ref_dir(self, ref_genome):
        """Get the harmonizer reference directory for the specified genome build"""
        if ref_genome == "GRCh38":
            return self.harmonizer_ref_dir_38
        elif ref_genome == "GRCh37":
            return self.harmonizer_ref_dir_37
        else:
            raise ValueError(f"Unsupported reference genome: {ref_genome}. Must be 'GRCh37' or 'GRCh38'")

    def get_plink_file_pattern(self, *, population, chrom):
        return f"{population}.chr{chrom}.1KG.GRCh38"

    def get_ldsc_ref_ld_chr(self) -> str:
        """Comma-separated chromosome-prefix pair for ldsc.py --h2-cts (baseline + merged cCRE)."""
        return f"{self.ldsc_baseline_ld_prefix},{self.ldsc_merged_ccre_prefix}"

    def resolve_ldsc_path(self, rel_path: str) -> str:
        """Absolute path under repo_root for subprocess cwd-relative LDSC IO."""
        p = Path(rel_path)
        if p.is_absolute():
            return str(p)
        return str(Path(self.repo_root) / rel_path)


def create_dependencies(config):
    """Factory function to create all dependencies from config"""
    # Import here to avoid circular dependency
    from src.services.enrich import Enrich
    
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
    
    # Initialize MinIO storage
    minio_storage = create_minio_client_from_env()
    
    return {
        'config': config,
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
        'phenotypes': PhenotypeHandler(mongodb_uri, db_name),
        'gwas_library': GWASLibraryHandler(mongodb_uri, db_name),
        'storage': minio_storage,
        'redis_url': config.redis_url,
    }
