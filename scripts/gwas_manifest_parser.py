"""
Parser for UK Biobank GWAS manifest files (Neale lab round 2).

This module parses manifest files from UK Biobank GWAS results and
extracts metadata for storage in the GWAS library collection.

Expected manifest format (CSV):
- Phenotype Code              — UKB field ID (e.g. 100010)
- Phenotype Description       — trait label
- UK Biobank Data Showcase Link
- Sex                         — both_sexes | male | female
- File                        — summary-stat filename
- wget command / AWS File / Dropbox File / md5s

Note: UK Biobank manifests have no category column (unlike FinnGen).

Sample input row (comma-separated):
    Phenotype Code,Phenotype Description,UK Biobank Data Showcase Link,Sex,File,...
    100010,Portion size,http://biobank.ctsu.ox.ac.uk/crystal/field.cgi?id=100010,both_sexes,100010.gwas.imputed_v3.both_sexes.tsv.bgz,...

Sample parsed entry (subset):
    {
        "phenotype_code": "100010",
        "description": "Portion size",
        "sex": "both_sexes",
        "source": "UK Biobank",
        "aws_url": "https://broad-ukb-sumstats-us-east-1.s3.amazonaws.com/round2/additive-tsvs/100010.gwas.imputed_v3.both_sexes.tsv.bgz",
        ...
    }
"""

import csv
import os
import re
import time
from io import StringIO

import requests
from loguru import logger
from typing import List, Dict, Optional

# Neale lab UK Biobank round 2 — reference totals by sex (not field-level N).
UKB_NEALE_ROUND2_N_BY_SEX = {
    "both_sexes": 361_194,
    "male": 168_962,
    "female": 194_174,
}


class GWASManifestParser:
    """Parser for UK Biobank GWAS manifest files"""
    
    def __init__(
        self,
        manifest_path: str,
        *,
        manifest_text: Optional[str] = None,
        showcase_request_delay_sec: float = 0.0,
    ):
        """
        Args:
            manifest_path: Path to the manifest TSV/CSV file (used for logging and
                showcase resolution when manifest_text is not provided).
            manifest_text: Optional in-memory manifest body (e.g. loaded from MinIO).
            showcase_request_delay_sec: Sleep this many seconds before each *uncached*
                showcase HTTP request (reduces rate limiting). 0 disables.
        """
        self.manifest_path = manifest_path
        self.manifest_text = manifest_text
        self.showcase_request_delay_sec = float(showcase_request_delay_sec)
        self._showcase_n_cache: dict[str, Optional[int]] = {}

        if manifest_text is None and not os.path.exists(manifest_path):
            raise FileNotFoundError(f"Manifest file not found: {manifest_path}")
    
    def parse(self) -> List[Dict]:
        """
        Parse the manifest file and return a list of GWAS entries
        
        Returns:
            List[Dict]: List of dictionaries containing GWAS metadata
        """
        entries = []

        try:
            if self.manifest_text is not None:
                sample = self.manifest_text[:1024]
                f = StringIO(self.manifest_text)
            else:
                f = open(self.manifest_path, "r", encoding="utf-8")
                sample = f.read(1024)
                f.seek(0)

            try:
                delimiter = "\t" if "\t" in sample else ","
                reader = csv.DictReader(f, delimiter=delimiter)

                headers = reader.fieldnames
                if not headers:
                    raise ValueError("Manifest file has no headers")

                logger.info(f"Found headers: {headers}")

                for row_num, row in enumerate(reader, start=2):
                    try:
                        entry = self._parse_row(row)
                        if entry:
                            entries.append(entry)
                    except Exception as e:
                        logger.warning(f"Error parsing row {row_num}: {e}")
                        continue
            finally:
                if self.manifest_text is None:
                    f.close()

            logger.info(f"Successfully parsed {len(entries)} GWAS entries from manifest")
            return entries

        except Exception as e:
            logger.error(f"Error reading manifest file: {e}")
            raise
    
    def _parse_row(self, row: Dict) -> Optional[Dict]:
        """
        Parse a single row from the manifest
        
        Args:
            row (dict): Dictionary representing one row from the CSV/TSV
        
        Returns:
            dict: Parsed GWAS entry or None if row is invalid
        """
        # Normalize header names - handle various column name formats
        normalized_row = {self._normalize_key(k): v for k, v in row.items()}
        
        # Extract phenotype code - optional field (can be N/A)
        phenotype_code = (
            normalized_row.get('phenotype_code') or 
            normalized_row.get('phenotype') or 
            normalized_row.get('code') or
            normalized_row.get('field_id') or
            normalized_row.get('field') or
            'N/A'
        )
        
        phenotype_code = phenotype_code.strip()
        
        # Extract phenotype description - required field
        description = (
            normalized_row.get('phenotype_description') or 
            normalized_row.get('description') or 
            normalized_row.get('trait') or
            normalized_row.get('phenotype_name')
        )
        
        if not description:
            description = f"Phenotype {phenotype_code}"
        
        description = description.strip()
        
        # Create display name (shorter version for UI)
        display_name = self._create_display_name(description)
        
        # Extract sex category
        sex = (
            normalized_row.get('sex') or 
            normalized_row.get('gender') or 
            'both_sexes'
        )
        sex = sex.strip().lower().replace(' ', '_')
        if sex not in ['both_sexes', 'male', 'female', 'males', 'females']:
            sex = 'both_sexes'
        # Normalize to singular
        if sex == 'males':
            sex = 'male'
        elif sex == 'females':
            sex = 'female'

        default_sample_size = UKB_NEALE_ROUND2_N_BY_SEX.get(
            sex, UKB_NEALE_ROUND2_N_BY_SEX["both_sexes"]
        )

        # Extract UK Biobank showcase link
        showcase_link = (
            normalized_row.get('uk_biobank_data_showcase_link') or 
            normalized_row.get('showcase_link') or 
            normalized_row.get('data_showcase_link') or
            normalized_row.get('link') or
            ''
        )
        showcase_link = showcase_link.strip()
        
        # Extract filename - REQUIRED (this is our unique identifier)
        filename = (
            normalized_row.get('file') or 
            normalized_row.get('filename') or 
            ''
        )
        filename = filename.strip()
        
        # Skip row if no filename
        if not filename:
            logger.debug(f"Skipping row with missing filename: {row}")
            return None
        
        # Extract wget command
        wget_command = (
            normalized_row.get('wget_command') or 
            normalized_row.get('wget') or 
            ''
        )
        wget_command = wget_command.strip()
        
        # Extract AWS URL
        aws_url = (
            normalized_row.get('aws_file') or 
            normalized_row.get('aws_url') or 
            normalized_row.get('aws') or
            normalized_row.get('s3_url') or
            ''
        )
        aws_url = aws_url.strip()
        
        # Extract Dropbox URL
        dropbox_url = (
            normalized_row.get('dropbox_file') or 
            normalized_row.get('dropbox_url') or 
            normalized_row.get('dropbox') or
            ''
        )
        dropbox_url = dropbox_url.strip()
        
        # Extract MD5 checksum
        md5 = (
            normalized_row.get('md5s') or 
            normalized_row.get('md5') or 
            normalized_row.get('checksum') or
            ''
        )
        md5 = md5.strip()
        
        # Try to extract file size from wget command or filename
        file_size = self._extract_file_size(wget_command, filename, aws_url)
        
        # Extract sample_size 
        sample_size_raw = (
            normalized_row.get('sample_size') or
            normalized_row.get('n') or
            normalized_row.get('n_complete_samples') or
            normalized_row.get('n_cases') or
            ''
        )
        sample_size = self._parse_sample_size(sample_size_raw)

        if sample_size is None and showcase_link:
            scraped = self._fetch_n_from_showcase(showcase_link)
            if scraped is not None:
                sample_size = scraped
                logger.info(
                    f"[Showcase] sample_size={sample_size} from {showcase_link}"
                )

        genome_build = (
            normalized_row.get('ref_genome') or
            normalized_row.get('genome_build') or
            normalized_row.get('build') or
            normalized_row.get('reference_genome') or
            ''
        )
        genome_build = genome_build.strip()
        if not genome_build:
            genome_build = self._infer_genome_build_from_filename(filename)
        
        # Build the entry (filename is the unique ID)
        entry = {
            'file_id': filename,  # Use filename as unique ID
            'phenotype_code': phenotype_code,
            'display_name': display_name,
            'description': description,
            'showcase_link': showcase_link,
            'sex': sex,
            'filename': filename,
            'wget_command': wget_command,
            'aws_url': aws_url,
            'dropbox_url': dropbox_url,
            'md5': md5,
            'source': 'UK Biobank',
            'default_sample_size': default_sample_size,
        }
        
        if file_size:
            entry['file_size'] = file_size
        if sample_size is not None:
            entry['sample_size'] = sample_size
        if genome_build:
            entry['genome_build'] = genome_build
        
        return entry
    
    def _normalize_key(self, key: str) -> str:
        """
        Normalize a column header key
        
        Args:
            key (str): Original header name
        
        Returns:
            str: Normalized key name
        """
        if not key:
            return ''
        
        # Convert to lowercase, replace spaces and special chars with underscores
        normalized = key.lower().strip()
        normalized = re.sub(r'[^\w]+', '_', normalized)
        normalized = re.sub(r'_+', '_', normalized)
        normalized = normalized.strip('_')
        
        return normalized
    
    def _create_display_name(self, description: str) -> str:
        """
        Create a shorter display name from description
        
        Args:
            description (str): Full phenotype description        
        Returns:
            str: Display name for UI
        """
        # If description is short enough, use it
        if len(description) <= 60:
            return description
        
        # Try to create a shorter version
        # Remove common prefixes
        display = description
        prefixes_to_remove = [
            'Diagnoses - main ICD10: ',
            'Diagnoses - secondary ICD10: ',
            'Treatment/medication code: ',
        ]
        
        for prefix in prefixes_to_remove:
            if display.startswith(prefix):
                display = display[len(prefix):]
                break
        
        # If still too long, truncate with ellipsis
        if len(display) > 60:
            display = display[:57] + '...'
        
        return display
    
    def _parse_sample_size(self, raw: str) -> Optional[int]:
        """
        Parse sample size from manifest column. Returns int or None if invalid/missing.
        """
        if not raw or not str(raw).strip():
            return None
        try:
            val = int(float(str(raw).strip().replace(',', '')))
            return val if val > 0 else None
        except (ValueError, TypeError):
            return None
    
    def _infer_genome_build_from_filename(self, filename: str) -> str:
        """
        Infer genome build from filename. UK Biobank imputed_v3 = GRCh37.
        """
        fn_lower = filename.lower()
        if 'hg38' in fn_lower or 'grch38' in fn_lower or 'b38' in fn_lower:
            return 'GRCh38'
        if 'hg19' in fn_lower or 'grch37' in fn_lower or 'b37' in fn_lower:
            return 'GRCh37'
        # UK Biobank imputed_v3 = GRCh37
        if 'imputed_v3' in fn_lower:
            return 'GRCh37'
        # Default for UK Biobank round2
        return 'GRCh37'
    
    def _extract_file_size(self, wget_command: str, filename: str, url: str) -> Optional[int]:
        """
        Try to extract file size from available information
        
        This is a best-effort attempt. Real file size will be updated when downloaded.
        
        Args:
            wget_command (str): wget command string
            filename (str): Filename
            url (str): URL to file
        
        Returns:
            Optional[int]: File size in bytes if found, None otherwise
        """
        # Look for common file size patterns in wget command or URL
        # This is difficult without actually querying the server
        # Return None for now - size will be updated on download
        return None

    def _fetch_n_from_showcase(self, showcase_url: str) -> Optional[int]:
        if showcase_url in self._showcase_n_cache:
            return self._showcase_n_cache[showcase_url]

        result: Optional[int] = None
        try:
            if self.showcase_request_delay_sec > 0:
                time.sleep(self.showcase_request_delay_sec)
            response = requests.get(showcase_url, timeout=10)
            response.raise_for_status()
            html = response.text
            for pattern in (
                r"(\d[\d,]+)\s*participants",
                r"N\s*=\s*([\d,]+)",
                r"sample\s+size[^>]*?(\d[\d,]+)",
            ):
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    result = int(match.group(1).replace(",", ""))
                    break
        except Exception as e:
            logger.debug(f"[Showcase] Could not fetch N from {showcase_url}: {e}")

        self._showcase_n_cache[showcase_url] = result
        return result

    def validate_entries(self, entries: List[Dict]) -> tuple:
        """
        Validate parsed entries
        
        Args:
            entries (List[Dict]): List of parsed entries
        
        Returns:
            tuple: (valid_entries, invalid_entries, validation_report)
        """
        valid_entries = []
        invalid_entries = []
        issues = []
        
        seen_files = set()
        
        for entry in entries:
            entry_issues = []
            
            # Check required field: filename (this is the unique identifier)
            if not entry.get('filename'):
                entry_issues.append("Missing filename")
            elif entry['filename'] in seen_files:
                entry_issues.append(f"Duplicate filename: {entry['filename']}")
            else:
                seen_files.add(entry['filename'])
            
            # Phenotype code is optional (can be N/A)
            # Description and display_name are always generated, so no need to check
            
            # Check that at least one download method is available
            has_download_method = any([
                entry.get('wget_command'),
                entry.get('aws_url'),
                entry.get('dropbox_url')
            ])
            
            if not has_download_method:
                entry_issues.append("No download method available (wget_command, aws_url, or dropbox_url)")
            
            if entry_issues:
                invalid_entries.append(entry)
                issues.append({
                    'filename': entry.get('filename', 'Unknown'),
                    'phenotype_code': entry.get('phenotype_code', 'N/A'),
                    'issues': entry_issues
                })
            else:
                valid_entries.append(entry)
        
        report = {
            'total_entries': len(entries),
            'valid_entries': len(valid_entries),
            'invalid_entries': len(invalid_entries),
            'issues': issues
        }
        
        return valid_entries, invalid_entries, report


def parse_manifest_file(
    manifest_path: str, *, showcase_request_delay_sec: float = 0.0
) -> List[Dict]:
    parser = GWASManifestParser(
        manifest_path, showcase_request_delay_sec=showcase_request_delay_sec
    )
    return parser.parse()


if __name__ == "__main__":
    # Example usage for testing
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python gwas_manifest_parser.py <manifest_file_path>")
        sys.exit(1)
    
    manifest_path = sys.argv[1]
    
    try:
        parser = GWASManifestParser(manifest_path)
        entries = parser.parse()
        
        print(f"\n=== Parsing Results ===")
        print(f"Total entries parsed: {len(entries)}")
        
        # Validate
        valid, invalid, report = parser.validate_entries(entries)
        print(f"\nValid entries: {report['valid_entries']}")
        print(f"Invalid entries: {report['invalid_entries']}")
        
        if report['issues']:
            print("\nValidation Issues:")
            for issue in report['issues'][:10]:  # Show first 10
                print(f"  - {issue['phenotype_code']}: {', '.join(issue['issues'])}")
        
        # Show sample entries
        if valid:
            print(f"\n=== Sample Valid Entries (first 3) ===")
            for entry in valid[:3]:
                print(f"\nPhenotype: {entry['phenotype_code']}")
                print(f"  Display Name: {entry['display_name']}")
                print(f"  Description: {entry['description'][:100]}...")
                print(f"  Sex: {entry['sex']}")
                print(f"  Has Download URL: {bool(entry.get('aws_url') or entry.get('wget_command'))}")
        
    except Exception as e:
        logger.error(f"Failed to parse manifest: {e}")
        sys.exit(1)

