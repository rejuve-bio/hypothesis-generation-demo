#!/usr/bin/env python3
"""
Script to populate the GWAS library collection from a manifest file

This script reads a UK Biobank GWAS manifest file (TSV or CSV format)
and populates the gwas_library MongoDB collection with the metadata.

Usage:
    python populate_gwas_library.py <manifest_file> [options]

Options:
    --validate-only    Only validate the manifest, don't insert into database
    --dry-run         Validate and show preview, but don't insert
    --clear           Clear existing collection before populating (destructive!)
    --sample N        Show N sample entries (default: 5)
    --skip-existing   Skip DB rows that already exist (no upsert for those file_ids)
    --showcase-delay SEC   Sleep SEC before each uncached showcase HTTP request (0 default)

Examples:
    # Validate manifest only
    python populate_gwas_library.py manifest.tsv --validate-only
    
    # Dry run with preview
    python populate_gwas_library.py manifest.tsv --dry-run
    
    # Populate database
    python populate_gwas_library.py manifest.tsv
    
    # Clear and repopulate
    python populate_gwas_library.py manifest.tsv --clear
"""

import sys
import os
import argparse
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_root))

from scripts.finngen_manifest_parser import (
    FinnGenManifestParser,
    detect_manifest_format,
)
from scripts.gwas_manifest_parser import GWASManifestParser
from src.db import GWASLibraryHandler
from loguru import logger
from dotenv import load_dotenv


def _resolve_manifest_source(manifest_path: str, source: str) -> str:
    if source != "auto":
        return source
    detected = detect_manifest_format(manifest_path)
    print(f"Auto-detected manifest format: {detected}")
    return detected


def _create_parser(manifest_path: str, source: str, showcase_delay_sec: float = 0.0):
    if source == "finngen":
        return FinnGenManifestParser(manifest_path)
    return GWASManifestParser(
        manifest_path, showcase_request_delay_sec=showcase_delay_sec
    )


def validate_manifest(
    manifest_path: str,
    sample_size: int = 5,
    showcase_delay_sec: float = 0.0,
    *,
    source: str = "auto",
):
    """
    Validate a manifest file

    Args:
        manifest_path (str): Path to manifest file
        sample_size (int): Number of sample entries to display
        showcase_delay_sec: Seconds to sleep before each uncached showcase HTTP request

    Returns:
        tuple: (valid_entries, invalid_entries, report)
    """
    resolved_source = _resolve_manifest_source(manifest_path, source)

    print(f"\n{'='*80}")
    print(f"VALIDATING MANIFEST: {manifest_path}")
    print(f"Source: {resolved_source}")
    print(f"{'='*80}\n")

    parser = _create_parser(manifest_path, resolved_source, showcase_delay_sec)
    entries = parser.parse()
    
    print(f"✓ Parsed {len(entries)} entries from manifest\n")
    
    # Validate entries
    valid_entries, invalid_entries, report = parser.validate_entries(entries)
    
    print(f"{'='*80}")
    print("VALIDATION RESULTS")
    print(f"{'='*80}")
    print(f"Total entries:     {report['total_entries']}")
    print(f"Valid entries:     {report['valid_entries']} ✓")
    print(f"Invalid entries:   {report['invalid_entries']} ✗")
    print(f"{'='*80}\n")
    
    # Show validation issues
    if report['issues']:
        print(f"❌ VALIDATION ISSUES ({len(report['issues'])} entries):\n")
        for issue in report['issues'][:10]:  # Show first 10
            filename = issue.get('filename', 'Unknown')
            phenotype = issue.get('phenotype_code', 'N/A')
            print(f"  - {filename} (phenotype: {phenotype}): {', '.join(issue['issues'])}")
        
        if len(report['issues']) > 10:
            print(f"\n  ... and {len(report['issues']) - 10} more issues\n")
    
    # Show sample valid entries
    if valid_entries:
        print(f"\n{'='*80}")
        print(f"SAMPLE VALID ENTRIES (first {min(sample_size, len(valid_entries))})")
        print(f"{'='*80}\n")
        
        for i, entry in enumerate(valid_entries[:sample_size], 1):
            print(f"[{i}] File: {entry.get('filename', 'Unknown')}")
            print(f"    Phenotype Code: {entry.get('phenotype_code', 'N/A')}")
            print(f"    Display Name: {entry['display_name']}")
            print(f"    Description: {entry['description'][:80]}...")
            print(f"    Source: {entry.get('source', '—')}")
            print(f"    Sex: {entry['sex']}")
            print(f"    sample_size: {entry.get('sample_size', '—')}")
            print(f"    default_sample_size: {entry.get('default_sample_size')}")
            print(f"    Has AWS URL: {bool(entry.get('aws_url'))}")
            print(f"    Has wget: {bool(entry.get('wget_command'))}")
            print(f"    Has Dropbox: {bool(entry.get('dropbox_url'))}")
            print()
    
    return valid_entries, invalid_entries, report


def populate_database(
    valid_entries,
    clear_existing=False,
    *,
    skip_existing=False,
):
    """
    Populate the database with valid entries
    
    Args:
        valid_entries (list): List of valid GWAS entries
        clear_existing (bool): Whether to clear existing collection first
        skip_existing (bool): If True, do not upsert rows whose file_id is already in the DB
    """
    # Load environment variables
    load_dotenv()
    
    mongodb_uri = os.getenv('MONGODB_URI')
    db_name = os.getenv('DB_NAME')
    
    if not mongodb_uri or not db_name:
        print("❌ Error: MONGODB_URI and DB_NAME environment variables must be set")
        print("\nPlease set them in your .env file or environment:")
        print("  export MONGODB_URI='mongodb://localhost:27017'")
        print("  export DB_NAME='hypothesis_db'")
        sys.exit(1)
    
    print(f"\n{'='*80}")
    print("DATABASE OPERATION")
    print(f"{'='*80}")
    print(f"MongoDB URI: {mongodb_uri}")
    print(f"Database: {db_name}")
    print(f"Collection: gwas_library")
    print(f"{'='*80}\n")
    
    # Initialize handler
    try:
        handler = GWASLibraryHandler(mongodb_uri, db_name)
        print("✓ Connected to MongoDB\n")
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {e}")
        sys.exit(1)
    
    # Clear existing collection if requested
    if clear_existing:
        print("⚠️  WARNING: Clearing existing GWAS library collection...")
        confirm = input("Are you sure? This will delete all existing entries! (yes/no): ")
        
        if confirm.lower() == 'yes':
            deleted_count = handler.clear_collection()
            print(f"✓ Cleared {deleted_count} existing entries\n")
        else:
            print("❌ Aborted: Collection not cleared")
            sys.exit(1)
    
    # Insert entries
    mode = "skip-existing" if skip_existing else "upsert-all"
    print(f"Writing {len(valid_entries)} entries ({mode})...")

    try:
        result = handler.bulk_create_gwas_entries(
            valid_entries, skip_existing=skip_existing
        )

        print(f"\n{'='*80}")
        print("INSERTION RESULTS")
        print(f"{'='*80}")
        print(f"Inserted (new):     {result['inserted_count']} ✓")
        print(f"Updated (existing): {result['updated_count']}")
        print(f"Skipped (existing): {result['skipped_existing_count']}")
        print(f"Total in manifest:  {len(valid_entries)}")
        print(f"{'='*80}\n")
        
        # Get final count
        total_in_db = handler.get_entry_count()
        print(f"Total entries in collection: {total_in_db}")
        
        print("\n✓ Database population complete!\n")
        
    except Exception as e:
        print(f"❌ Error inserting entries: {e}")
        sys.exit(1)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Populate GWAS library from manifest file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        'manifest_file',
        help='Path to GWAS manifest file (UK Biobank CSV or FinnGen TSV)',
    )

    parser.add_argument(
        '--source',
        choices=['auto', 'ukbb', 'finngen'],
        default='auto',
        help='Manifest format: auto-detect (default), ukbb, or finngen',
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only validate the manifest, do not insert into database'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate and show preview, but do not insert into database'
    )
    
    parser.add_argument(
        '--clear',
        action='store_true',
        help='Clear existing collection before populating (destructive!)'
    )
    
    parser.add_argument(
        '--sample',
        type=int,
        default=5,
        help='Number of sample entries to display (default: 5)'
    )
    
    parser.add_argument(
        '--skip-existing',
        action='store_true',
        help='Do not update rows whose file_id is already in the database (faster re-runs)',
    )

    parser.add_argument(
        '--showcase-delay',
        type=float,
        default=0.0,
        metavar='SEC',
        help='Seconds to sleep before each uncached UKB showcase HTTP request (0 = no delay)',
    )

    args = parser.parse_args()
    
    # Validate manifest file exists
    if not os.path.exists(args.manifest_file):
        print(f"❌ Error: Manifest file not found: {args.manifest_file}")
        sys.exit(1)
    
    # Validate manifest
    valid_entries, invalid_entries, report = validate_manifest(
        args.manifest_file,
        sample_size=args.sample,
        showcase_delay_sec=args.showcase_delay,
        source=args.source,
    )
    
    # Check if validation passed
    if report['valid_entries'] == 0:
        print("❌ No valid entries found in manifest. Cannot proceed.")
        sys.exit(1)
    
    # If validate-only, stop here
    if args.validate_only:
        print("✓ Validation complete (--validate-only mode)")
        print("No database changes made.\n")
        sys.exit(0)
    
    # If dry-run, stop here
    if args.dry_run:
        print("✓ Dry run complete (--dry-run mode)")
        print("No database changes made.\n")
        sys.exit(0)
    
    # Populate database
    populate_database(
        valid_entries,
        clear_existing=args.clear,
        skip_existing=args.skip_existing,
    )


if __name__ == '__main__':
    main()
