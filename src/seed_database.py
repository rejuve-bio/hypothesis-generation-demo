#!/usr/bin/env python3
"""
Database seeding script for GWAS library and phenotypes

This script runs on container startup to populate the database with
initial data if collections are empty.

GWAS boot modes (GWAS_LIBRARY_BOOT_MODE): auto (default), refresh, incremental.
See GWAS_SHOWCASE_DELAY_SEC for UKB showcase throttling.
"""

import json
import os
import sys
from pathlib import Path
from typing import Optional
from loguru import logger

_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_root))

from src.db import GWASLibraryHandler, PhenotypeHandler, DemoTemplateHandler
from scripts.finngen_manifest_parser import FINNGEN_SOURCE, FinnGenManifestParser
from scripts.gwas_manifest_parser import GWASManifestParser
from src.services.seed_assets import load_seed_text
from src.services.storage import MinIOStorage, create_minio_client_from_env

_VALID_GWAS_BOOT_MODES = frozenset({"auto", "refresh", "incremental"})
UKB_SOURCE = "UK Biobank"


def _parse_gwas_library_boot_mode() -> str:
    raw = (os.getenv("GWAS_LIBRARY_BOOT_MODE") or "auto").strip().lower()
    if raw not in _VALID_GWAS_BOOT_MODES:
        logger.warning(
            f"Invalid GWAS_LIBRARY_BOOT_MODE={raw!r}; using 'auto'. "
            f"Valid: {sorted(_VALID_GWAS_BOOT_MODES)}"
        )
        return "auto"
    return raw


def _parse_showcase_delay_sec() -> float:
    try:
        return float(os.getenv("GWAS_SHOWCASE_DELAY_SEC", "0"))
    except (TypeError, ValueError):
        logger.warning("Invalid GWAS_SHOWCASE_DELAY_SEC; using 0")
        return 0.0


def check_if_should_seed():
    """Check if seeding should be skipped"""
    skip_seed = os.getenv('SKIP_SEED', 'false').lower() == 'true'
    
    if skip_seed:
        logger.info("SKIP_SEED is set to true. Skipping database seeding.")
        return False
    
    return True


def seed_gwas_library(
    handler: GWASLibraryHandler,
    manifest_path: str,
    *,
    storage: Optional[MinIOStorage] = None,
    boot_mode: str = "auto",
    showcase_delay_sec: float = 0.0,
) -> bool:
    """
    Seed the GWAS library collection from a UK Biobank manifest file.
    """
    try:
        count = handler.get_entry_count(source_filter=UKB_SOURCE)
        logger.info(
            f"UK Biobank GWAS boot_mode={boot_mode!r}, showcase_delay_sec={showcase_delay_sec}, "
            f"current_ukb_entries={count}"
        )

        if boot_mode == "auto" and count > 0:
            logger.info(
                f"UK Biobank GWAS library already populated with {count} entries. "
                "Skipping (auto mode)."
            )
            return True

        loaded = load_seed_text(manifest_path, storage)
        if loaded is None:
            logger.warning(f"UK Biobank GWAS manifest not available: {manifest_path}")
            logger.warning("UK Biobank GWAS entries will not be seeded.")
            logger.warning(
                "Provide a local file (GWAS_MANIFEST_PATH) or upload to MinIO "
                "under seed-assets/v1/<filename>"
            )
            return False

        manifest_text, asset_source = loaded
        skip_existing = boot_mode == "incremental"
        logger.info(
            f"Seeding UK Biobank GWAS library from {asset_source}: {manifest_path} "
            f"(skip_existing={skip_existing})"
        )

        parser = GWASManifestParser(
            manifest_path,
            manifest_text=manifest_text,
            showcase_request_delay_sec=showcase_delay_sec,
        )
        entries = parser.parse()

        logger.info(f"Parsed {len(entries)} entries from manifest")

        valid_entries, _, report = parser.validate_entries(entries)

        logger.info(f"Valid entries: {report['valid_entries']}")
        logger.info(f"Invalid entries: {report['invalid_entries']}")

        if report["valid_entries"] == 0:
            logger.error(
                "No valid entries found in manifest. GWAS library will remain empty."
            )
            return False

        result = handler.bulk_create_gwas_entries(
            valid_entries, skip_existing=skip_existing
        )

        logger.info("UK Biobank GWAS library seed/refresh step finished.")
        logger.info(f"Inserted: {result['inserted_count']}")
        logger.info(f"Updated: {result['updated_count']}")
        logger.info(f"Skipped (already existed): {result['skipped_existing_count']}")

        return True

    except Exception as e:
        logger.error(f"Error seeding UK Biobank GWAS library: {e}")
        return False


def seed_finngen_gwas_library(
    handler: GWASLibraryHandler,
    manifest_path: str,
    *,
    storage: Optional[MinIOStorage] = None,
) -> bool:
    """Seed FinnGen entries into the GWAS library collection."""
    try:
        count = handler.get_entry_count(source_filter=FINNGEN_SOURCE)
        if count > 0:
            logger.info(
                f"FinnGen GWAS library already populated with {count} entries. Skipping."
            )
            return True

        loaded = load_seed_text(manifest_path, storage)
        if loaded is None:
            logger.warning(f"FinnGen manifest not available: {manifest_path}")
            logger.warning("FinnGen GWAS entries will not be seeded.")
            logger.warning(
                "Provide a local file (FINNGEN_MANIFEST_PATH) or upload to MinIO "
                "under seed-assets/v1/<filename>"
            )
            return False

        manifest_text, asset_source = loaded
        logger.info(
            f"Seeding FinnGen GWAS library from {asset_source}: {manifest_path}"
        )

        parser = FinnGenManifestParser(manifest_path, manifest_text=manifest_text)
        entries = parser.parse()
        logger.info(f"Parsed {len(entries)} FinnGen entries from manifest")

        valid_entries, _, report = parser.validate_entries(entries)
        logger.info(f"Valid FinnGen entries: {report['valid_entries']}")
        logger.info(f"Invalid FinnGen entries: {report['invalid_entries']}")

        if report["valid_entries"] == 0:
            logger.error("No valid FinnGen entries found in manifest.")
            return False

        result = handler.bulk_create_gwas_entries(valid_entries, skip_existing=True)
        logger.info("FinnGen GWAS library seed step finished.")
        logger.info(f"Inserted: {result['inserted_count']}")
        logger.info(f"Updated: {result['updated_count']}")
        logger.info(f"Skipped (already existed): {result['skipped_existing_count']}")
        return True

    except Exception as e:
        logger.error(f"Error seeding FinnGen GWAS library: {e}")
        return False


def seed_phenotypes(
    handler: PhenotypeHandler,
    phenotypes_json_path: str,
    *,
    storage: Optional[MinIOStorage] = None,
) -> bool:
    """
    Seed phenotypes collection from a JSON file
    """
    try:
        count = handler.count_phenotypes()

        if count > 0:
            logger.info(f"Phenotypes already populated with {count} entries. Skipping.")
            return True

        loaded = load_seed_text(phenotypes_json_path, storage)
        if loaded is None:
            logger.warning(f"Phenotypes file not available: {phenotypes_json_path}")
            logger.warning("Phenotypes collection will remain empty.")
            return False

        raw_json, source = loaded
        logger.info(f"Seeding phenotypes from {source}: {phenotypes_json_path}")

        phenotypes_data = json.loads(raw_json)
        
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


def seed_demo_seeds(handler: DemoTemplateHandler, storage) -> bool:
    """Import demo seed bundles from MinIO (demo-seeds-v2/)."""
    if storage is None:
        logger.warning(
            "MinIO not configured; skipping demo seed import. "
            "Set MINIO_* env vars to enable automatic demo seed seeding."
        )
        return False

    try:
        results = handler.ensure_seeds_from_minio(storage)
        logger.info(
            "Demo seeds step finished: "
            f"imported={results['imported']} skipped={results['skipped']} "
            f"failed={len(results['failed'])}"
        )
        if results["failed"]:
            for failure in results["failed"]:
                logger.error(f"Demo seed import failed: {failure}")
        return len(results["failed"]) == 0
    except Exception as exc:
        logger.error(f"Error seeding demo projects: {exc}")
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
        demo_template_handler = DemoTemplateHandler(mongodb_uri, db_name)
        storage = create_minio_client_from_env()
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return 1
    
    # Seed GWAS library
    gwas_manifest_path = os.getenv(
        'GWAS_MANIFEST_PATH',
        '/app/data/gwas_manifest.tsv'
    )
    
    gwas_boot_mode = _parse_gwas_library_boot_mode()
    showcase_delay = _parse_showcase_delay_sec()

    logger.info("1. GWAS LIBRARY (UK BIOBANK)")
    gwas_success = seed_gwas_library(
        gwas_handler,
        gwas_manifest_path,
        storage=storage,
        boot_mode=gwas_boot_mode,
        showcase_delay_sec=showcase_delay,
    )

    finngen_manifest_path = os.getenv(
        "FINNGEN_MANIFEST_PATH",
        "/app/data/finngen_R12_manifest.tsv",
    )

    logger.info("2. GWAS LIBRARY (FINNGEN)")
    finngen_success = seed_finngen_gwas_library(
        gwas_handler,
        finngen_manifest_path,
        storage=storage,
    )
    
    # Seed phenotypes
    phenotypes_json_path = os.getenv(
        'PHENOTYPES_JSON_PATH',
        '/app/data/phenotypes.json'
    )
    
    logger.info("3. PHENOTYPES")
    phenotype_success = seed_phenotypes(
        phenotype_handler, phenotypes_json_path, storage=storage
    )

    logger.info("4. DEMO SEEDS")
    demo_success = seed_demo_seeds(demo_template_handler, storage)

    
    # Summary
    logger.info("DATABASE SEEDING COMPLETED")
    logger.info(f"   UK Biobank GWAS: {'✓ SUCCESS' if gwas_success else 'SKIPPED/FAILED'}")
    logger.info(f"   FinnGen GWAS:    {'✓ SUCCESS' if finngen_success else 'SKIPPED/FAILED'}")
    logger.info(f"   Phenotypes:      {'✓ SUCCESS' if phenotype_success else 'SKIPPED/FAILED'}")
    logger.info(f"   Demo Seeds:      {'✓ SUCCESS' if demo_success else 'SKIPPED/FAILED'}")
    
    # Return 0 even if some seeding failed (non-critical)
    return 0


if __name__ == '__main__':
    exit(main())
