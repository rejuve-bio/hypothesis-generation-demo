#!/usr/bin/env python3
"""
Database seeding script for GWAS library and phenotypes

This script runs on container startup to populate the database with
initial data if collections are empty.
"""

import os
import sys
from pathlib import Path
from loguru import logger

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from db import GWASLibraryHandler, PhenotypeHandler
from scripts.gwas_manifest_parser import GWASManifestParser


def check_if_should_seed():
    """Check if seeding should be skipped"""
    skip_seed = os.getenv('SKIP_SEED', 'false').lower() == 'true'
    
    if skip_seed:
        logger.info("SKIP_SEED is set to true. Skipping database seeding.")
        return False
    
    return True


def seed_gwas_library(handler: GWASLibraryHandler, manifest_path: str) -> bool:
    """
    Seed the GWAS library collection from a manifest file
    """
    try:
        # Check if collection is already populated
        count = handler.get_entry_count()
        
        if count > 0:
            logger.info(f"GWAS library already populated with {count} entries. Skipping.")
            return True
        
        # Check if manifest file exists
        if not os.path.exists(manifest_path):
            logger.warning(f"GWAS manifest file not found: {manifest_path}")
            logger.warning("GWAS library will remain empty.")
            logger.warning("To populate, provide a manifest file and set GWAS_MANIFEST_PATH")
            return False
        
        logger.info(f"Seeding GWAS library from manifest: {manifest_path}")
        
        # Parse manifest
        parser = GWASManifestParser(manifest_path)
        entries = parser.parse()
        
        logger.info(f"Parsed {len(entries)} entries from manifest")
        
        # Validate entries
        valid_entries, invalid_entries, report = parser.validate_entries(entries)
        
        logger.info(f"Valid entries: {report['valid_entries']}")
        logger.info(f"Invalid entries: {report['invalid_entries']}")
        
        if report['valid_entries'] == 0:
            logger.error("No valid entries found in manifest. GWAS library will remain empty.")
            return False
        
        # Insert into database
        result = handler.bulk_create_gwas_entries(valid_entries)
        
        logger.info(f"GWAS library seeded successfully!")
        logger.info(f"Inserted: {result['inserted_count']}")
        logger.info(f"Skipped: {result['skipped_count']}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error seeding GWAS library: {e}")
        return False


def seed_phenotypes(handler: PhenotypeHandler, phenotypes_json_path: str) -> bool:
    """
    Seed phenotypes collection from a JSON file
    """
    try:
        # Check if collection is already populated
        count = handler.count_phenotypes()
        
        if count > 0:
            logger.info(f"Phenotypes already populated with {count} entries. Skipping.")
            return True
        
        # Check if phenotypes file exists
        if not os.path.exists(phenotypes_json_path):
            logger.warning(f"Phenotypes file not found: {phenotypes_json_path}")
            logger.warning("Phenotypes collection will remain empty.")
            return False
        
        logger.info(f"Seeding phenotypes from: {phenotypes_json_path}")
        
        # Load phenotypes from JSON file
        import json
        
        with open(phenotypes_json_path, 'r', encoding='utf-8') as f:
            phenotypes_data = json.load(f)
        
        if not isinstance(phenotypes_data, list):
            logger.error("Phenotypes file must contain a JSON array")
            return False
        
        logger.info(f"Loaded {len(phenotypes_data)} phenotypes from file")
        
        # Transform data to match database schema
        # Input format: {"name": "...", "id": "..."}
        # Database format: {"phenotype_name": "...", "id": "..."}
        transformed_phenotypes = []
        skipped_invalid = 0
        
        for item in phenotypes_data:
            if not isinstance(item, dict):
                skipped_invalid += 1
                continue
            
            # Map "name" to "phenotype_name" for database
            phenotype = {
                "id": item.get("id", ""),
                "phenotype_name": item.get("name", "")
            }
            
            # Validate that both fields exist
            if phenotype["id"] and phenotype["phenotype_name"]:
                transformed_phenotypes.append(phenotype)
            else:
                skipped_invalid += 1
                logger.debug(f"Skipping invalid entry: {item}")
        
        if not transformed_phenotypes:
            logger.error("No valid phenotypes found in file")
            return False
        
        if skipped_invalid > 0:
            logger.warning(f"qSkipped {skipped_invalid} invalid entries")
        
        # Bulk insert phenotypes
        result = handler.bulk_create_phenotypes(transformed_phenotypes)
        
        logger.info(f"Phenotypes seeded successfully!")
        logger.info(f"Inserted: {result['inserted_count']}")
        logger.info(f"Skipped: {result['skipped_count']} (duplicates)")
        
        return True
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in phenotypes file: {e}")
        return False
    except Exception as e:
        logger.error(f"Error seeding phenotypes: {e}")
        return False


def main():
    """Main seeding function"""
    logger.info("DATABASE SEEDING STARTED")
    
    # Check if seeding should be skipped
    if not check_if_should_seed():
        return 0
    
    # Get MongoDB configuration
    mongodb_uri = os.getenv('MONGODB_URI')
    db_name = os.getenv('DB_NAME')
    
    if not mongodb_uri or not db_name:
        logger.error("Missing MongoDB configuration!")
        return 1
    
    # Initialize handlers
    try:
        gwas_handler = GWASLibraryHandler(mongodb_uri, db_name)
        phenotype_handler = PhenotypeHandler(mongodb_uri, db_name)
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return 1
    
    # Seed GWAS library
    gwas_manifest_path = os.getenv(
        'GWAS_MANIFEST_PATH',
        '/app/data/gwas_manifest.tsv'
    )
    
    logger.info("1. GWAS LIBRARY")
    gwas_success = seed_gwas_library(gwas_handler, gwas_manifest_path)
    
    # Seed phenotypes
    phenotypes_json_path = os.getenv(
        'PHENOTYPES_JSON_PATH',
        '/app/data/phenotypes.json'
    )
    
    logger.info("2. PHENOTYPES")
    phenotype_success = seed_phenotypes(phenotype_handler, phenotypes_json_path)

    
    # Summary
    logger.info("DATABASE SEEDING COMPLETED")
    logger.info(f"   GWAS Library: {'✓ SUCCESS' if gwas_success else 'SKIPPED/FAILED'}")
    logger.info(f"   Phenotypes:   {'✓ SUCCESS' if phenotype_success else 'SKIPPED/FAILED'}")
    
    # Return 0 even if some seeding failed (non-critical)
    return 0


if __name__ == '__main__':
    exit(main())
