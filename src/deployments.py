#!/usr/bin/env python

import os
import argparse
import logging
from src.flows import (
    enrichment_flow,
    analysis_pipeline_flow,
    child_enrichment_batch_flow,
    hypothesis_flow,
)
from src.config import Config
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_deployment_arguments():
    """Parse arguments for deployment service"""
    parser = argparse.ArgumentParser(description="Prefect Deployment Service")
    parser.add_argument("--ensembl-hgnc-map", type=str, required=True)
    parser.add_argument("--hgnc-ensembl-map", type=str, required=True)
    parser.add_argument("--go-map", type=str, required=True)
    parser.add_argument("--swipl-host", type=str, default="localhost")
    parser.add_argument("--swipl-port", type=int, default=4242)
    parser.add_argument("--embedding-model", type=str, default="w601sxs/b1ade-embed-kd")
    return parser.parse_args()

def setup_deployments(config):
    """Create and configure Prefect deployments"""
    
    os.environ.update({
        "ENSEMBL_HGNC_MAP": config.ensembl_hgnc_map,
        "HGNC_ENSEMBL_MAP": config.hgnc_ensembl_map,
        "GO_MAP": config.go_map,
        "SWIPL_HOST": config.swipl_host,
        "SWIPL_PORT": str(config.swipl_port),
    })
    
    job_vars = {"working_dir": "/app"}
    
    # 1. Enrichment Deployment (interactive-pool: lighter workloads)
    enrich_deploy = enrichment_flow.to_deployment(
        name="enrichment-flow-deployment",
        work_pool_name="interactive-pool",
        tags=["background_enrichment", "production"],
        description="Background enrichment processing for gene hypothesis generation",
        version="1.0.0",
        job_variables=job_vars
    )

    # 2. Analysis Pipeline Deployment (analysis-pool: heavy workloads, limit 3)
    analysis_deploy = analysis_pipeline_flow.to_deployment(
        name="analysis-pipeline-deployment",
        work_pool_name="analysis-pool",
        tags=["analysis", "production"],
        description="GWAS Analysis Pipeline (Harmonization -> Fine-mapping)",
        version="1.0.0",
        job_variables=job_vars
    )
    
    # 3. Child Enrichment Batch Deployment (interactive-pool)
    child_batch_deploy = child_enrichment_batch_flow.to_deployment(
        name="child-batch-deployment",
        work_pool_name="interactive-pool",
        tags=["background_hypothesis", "production"],
        description="Background processing for child enrichment hypotheses",
        version="1.0.0",
        job_variables=job_vars
    )

    # 4. Hypothesis generation
    hypothesis_deploy = hypothesis_flow.to_deployment(
        name="hypothesis-generation-deployment",
        work_pool_name="interactive-pool",
        tags=["hypothesis", "production"],
        description="Build hypothesis summary and graph from enrichment + GO term",
        version="1.0.0",
        job_variables=job_vars,
    )

    return [enrich_deploy, analysis_deploy, child_batch_deploy, hypothesis_deploy]

def main():
    """Main deployment service entry point"""
    load_dotenv()
    
    try:
        args = parse_deployment_arguments()
        config = Config.from_args(args)
        logger.info("Configuration loaded from command line arguments")
    except SystemExit:
        config = Config.from_env()
        logger.info("Configuration loaded from environment variables")
    
    if not all([config.ensembl_hgnc_map, config.hgnc_ensembl_map, config.go_map]):
        raise ValueError("Missing required configuration: ensembl_hgnc_map, hgnc_ensembl_map, go_map")
    
    print(f" Starting Prefect deployment service...")
    logger.info(f"- SWIPL Host: {config.swipl_host}:{config.swipl_port}")
    logger.info(f"- Data files: {config.ensembl_hgnc_map}")
    
    deployments = setup_deployments(config)
    
    logger.info(f"Registering {len(deployments)} deployments with Work Pools...")
    for deployment in deployments:
        deployment.apply()
    
    logger.info("Deployments registered successfully. Workers can now pick up runs.")

if __name__ == "__main__":
    main()