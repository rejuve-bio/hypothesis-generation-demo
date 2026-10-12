"""
Parser for FinnGen GWAS manifest files (R12 public release).

Expected manifest format (TSV):
- phenocode       — FinnGen phenotype identifier (e.g. K11_UC_STRICT2)
- phenotype       — human-readable trait name
- category        — ICD-10 chapter / FinnGen disease grouping for UI browse/filter
                    (e.g. "XI Digestive", "I Certain infectious and parasitic diseases (AB1_)")
- num_cases       — case count for this GWAS
- num_controls    — control count for this GWAS
- path_bucket     — gs:// URL to the summary-stat file in FinnGen's public bucket
- path_https      — HTTPS download URL (preferred when present)

Sample input row (tab-separated):
    phenocode    phenotype              category                                          num_cases  num_controls  path_https
    K11_UC_STRICT2  Ulcerative colitis strict  XI Digestive                                  7220       492160        https://storage.googleapis.com/finngen-public-data-r12/summary_stats/release/finngen_R12_K11_UC_STRICT2.gz

Sample parsed entry (subset):
    {
        "phenotype_code": "K11_UC_STRICT2",
        "description": "Ulcerative colitis strict",
        "category": "XI Digestive",
        "source": "FinnGen",
        "sample_size": 499380,
        "aws_url": "https://storage.googleapis.com/finngen-public-data-r12/.../finngen_R12_K11_UC_STRICT2.gz",
        ...
    }
"""

from __future__ import annotations

import csv
import os
import re
from io import StringIO
from typing import Dict, List, Optional
from urllib.parse import urlparse

from loguru import logger

FINNGEN_SOURCE = "FinnGen"
FINNGEN_GENOME_BUILD = "GRCh38"
MISSING_VALUE = "N/A"


def create_display_name(description: str, max_len: int = 60) -> str:
    if len(description) <= max_len:
        return description
    return description[: max_len - 3] + "..."


def detect_manifest_format(manifest_path: str) -> str:
    """
    Detect manifest format from headers.

    Returns:
        'finngen' | 'ukbb'
    """
    with open(manifest_path, "r", encoding="utf-8") as f:
        sample = f.read(4096)
        f.seek(0)
        delimiter = "\t" if "\t" in sample.splitlines()[0] else ","
        reader = csv.DictReader(f, delimiter=delimiter)
        headers = {_normalize_key(h) for h in (reader.fieldnames or [])}

    if {"phenocode", "path_https"} <= headers or {"phenocode", "path_bucket"} <= headers:
        return "finngen"
    return "ukbb"


def _normalize_key(key: str) -> str:
    if not key:
        return ""
    normalized = key.lower().strip()
    normalized = re.sub(r"[^\w]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_")


def _parse_int(raw: str) -> Optional[int]:
    if not raw or not str(raw).strip():
        return None
    try:
        val = int(float(str(raw).strip().replace(",", "")))
        return val if val >= 0 else None
    except (ValueError, TypeError):
        return None


class FinnGenManifestParser:
    """Parser for FinnGen GWAS manifest files."""

    def __init__(
        self,
        manifest_path: str,
        *,
        manifest_text: Optional[str] = None,
    ):
        self.manifest_path = manifest_path
        self.manifest_text = manifest_text
        if manifest_text is None and not os.path.exists(manifest_path):
            raise FileNotFoundError(f"Manifest file not found: {manifest_path}")

    def parse(self) -> List[Dict]:
        entries: List[Dict] = []

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

            if not reader.fieldnames:
                raise ValueError("Manifest file has no headers")

            logger.info(f"FinnGen manifest headers: {reader.fieldnames}")

            for row_num, row in enumerate(reader, start=2):
                try:
                    entry = self._parse_row(row)
                    if entry:
                        entries.append(entry)
                except Exception as exc:
                    logger.warning(f"Error parsing FinnGen row {row_num}: {exc}")
        finally:
            if self.manifest_text is None:
                f.close()

        logger.info(f"Parsed {len(entries)} FinnGen GWAS entries from manifest")
        return entries

    def _parse_row(self, row: Dict) -> Optional[Dict]:
        normalized_row = {_normalize_key(k): (v or "").strip() for k, v in row.items()}

        phenocode = normalized_row.get("phenocode", "")
        description = normalized_row.get("phenotype", "")
        category = normalized_row.get("category", "")
        path_https = normalized_row.get("path_https", "")
        path_bucket = normalized_row.get("path_bucket", "")

        download_url = path_https or self._gs_to_https(path_bucket)
        if not download_url:
            logger.debug(f"Skipping row with no download URL: {row}")
            return None

        filename = os.path.basename(urlparse(download_url).path)
        if not filename or not phenocode:
            logger.debug(f"Skipping row with missing filename/phenocode: {row}")
            return None

        num_cases = _parse_int(normalized_row.get("num_cases", ""))
        num_controls = _parse_int(normalized_row.get("num_controls", ""))
        sample_size = None
        if num_cases is not None and num_controls is not None:
            sample_size = num_cases + num_controls

        if not description:
            description = phenocode

        entry: Dict = {
            "file_id": filename,
            "phenotype_code": phenocode,
            "display_name": create_display_name(description),
            "description": description,
            "sex": MISSING_VALUE,
            "filename": filename,
            "aws_url": download_url,
            "wget_command": "",
            "dropbox_url": "",
            "md5": "",
            "source": FINNGEN_SOURCE,
            "genome_build": FINNGEN_GENOME_BUILD,
            "default_sample_size": MISSING_VALUE,
        }

        if category:
            entry["category"] = category
        if sample_size is not None:
            entry["sample_size"] = sample_size

        return entry

    @staticmethod
    def _gs_to_https(path_bucket: str) -> str:
        if not path_bucket.startswith("gs://"):
            return ""
        return "https://storage.googleapis.com/" + path_bucket[5:]

    def validate_entries(self, entries: List[Dict]) -> tuple:
        valid_entries: List[Dict] = []
        invalid_entries: List[Dict] = []
        issues: List[Dict] = []
        seen_files: set[str] = set()

        for entry in entries:
            entry_issues: List[str] = []

            if not entry.get("filename"):
                entry_issues.append("Missing filename")
            elif entry["filename"] in seen_files:
                entry_issues.append(f"Duplicate filename: {entry['filename']}")
            else:
                seen_files.add(entry["filename"])

            if not entry.get("aws_url"):
                entry_issues.append("No download URL (path_https)")

            if entry.get("source") != FINNGEN_SOURCE:
                entry_issues.append(f"Unexpected source: {entry.get('source')}")

            if entry_issues:
                invalid_entries.append(entry)
                issues.append(
                    {
                        "filename": entry.get("filename", "Unknown"),
                        "phenotype_code": entry.get("phenotype_code", "N/A"),
                        "issues": entry_issues,
                    }
                )
            else:
                valid_entries.append(entry)

        report = {
            "total_entries": len(entries),
            "valid_entries": len(valid_entries),
            "invalid_entries": len(invalid_entries),
            "issues": issues,
        }
        return valid_entries, invalid_entries, report
