#!/usr/bin/env python

import os
import argparse
from prefect import serve
from flows import enrichment_flow
from config import Config
from dotenv import load_dotenv

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
    
    # Set environment variables from config for the deployment
    os.environ.update({
        "ENSEMBL_HGNC_MAP": config.ensembl_hgnc_map,
        "HGNC_ENSEMBL_MAP": config.hgnc_ensembl_map,
        "GO_MAP": config.go_map,
        "SWIPL_HOST": config.swipl_host,
        "SWIPL_PORT": str(config.swipl_port),
    })
    
    # Create deployment
    enrich_deploy = enrichment_flow.to_deployment(
        name="enrichment-flow-deployment",
        tags=["background_enrichment", "production"],
        description="Background enrichment processing for gene hypothesis generation",
        version="1.0.0"
    )
    
    return [enrich_deploy]

def main():
    """Main deployment service entry point"""
    load_dotenv()
    
    # Try to get config from arguments first, fallback to environment
    try:
        args = parse_deployment_arguments()
        config = Config.from_args(args)
        logger.info("✅ Configuration loaded from command line arguments")
    except SystemExit:
        # If no args provided, use environment variables
        config = Config.from_env()
        logger.info("✅ Configuration loaded from environment variables")
    
    # Validate critical configuration
    if not all([config.ensembl_hgnc_map, config.hgnc_ensembl_map, config.go_map]):
        raise ValueError("Missing required configuration: ensembl_hgnc_map, hgnc_ensembl_map, go_map")
    
    print(f" Starting Prefect deployment service...")
    logger.info(f"- SWIPL Host: {config.swipl_host}:{config.swipl_port}")
    logger.info(f"- Data files: {config.ensembl_hgnc_map}")
    
    deployments = setup_deployments(config)
    
    # Start serving deployments
    logger.info("Serving deployments...")
    serve(*deployments)

if __name__ == "__main__":
    main()