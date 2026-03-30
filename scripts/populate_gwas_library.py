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

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.gwas_manifest_parser import GWASManifestParser
from db import GWASLibraryHandler
from loguru import logger
from dotenv import load_dotenv


def validate_manifest(manifest_path: str, sample_size: int = 5):
    """
    Validate a manifest file
    
    Args:
        manifest_path (str): Path to manifest file
        sample_size (int): Number of sample entries to display
        
    Returns:
        tuple: (valid_entries, invalid_entries, report)
    """
    print(f"\n{'='*80}")
    print(f"VALIDATING MANIFEST: {manifest_path}")
    print(f"{'='*80}\n")
    
    # Parse manifest
    parser = GWASManifestParser(manifest_path)
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
            print(f"    Sex: {entry['sex']}")
            print(f"    Has AWS URL: {bool(entry.get('aws_url'))}")
            print(f"    Has wget: {bool(entry.get('wget_command'))}")
            print(f"    Has Dropbox: {bool(entry.get('dropbox_url'))}")
            print()
    
    return valid_entries, invalid_entries, report


def populate_database(valid_entries, clear_existing=False):
    """
    Populate the database with valid entries
    
    Args:
        valid_entries (list): List of valid GWAS entries
        clear_existing (bool): Whether to clear existing collection first
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
    print(f"Inserting {len(valid_entries)} entries into database...")
    
    try:
        result = handler.bulk_create_gwas_entries(valid_entries)
        
        print(f"\n{'='*80}")
        print("INSERTION RESULTS")
        print(f"{'='*80}")
        print(f"Inserted:  {result['inserted_count']} ✓")
        print(f"Skipped:   {result['skipped_count']} (duplicates)")
        print(f"Total:     {len(valid_entries)}")
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
        help='Path to UK Biobank GWAS manifest file (TSV or CSV)'
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
    
    args = parser.parse_args()
    
    # Validate manifest file exists
    if not os.path.exists(args.manifest_file):
        print(f"❌ Error: Manifest file not found: {args.manifest_file}")
        sys.exit(1)
    
    # Validate manifest
    valid_entries, invalid_entries, report = validate_manifest(
        args.manifest_file,
        sample_size=args.sample
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
    populate_database(valid_entries, clear_existing=args.clear)


if __name__ == '__main__':
    main()
