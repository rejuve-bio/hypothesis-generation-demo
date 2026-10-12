"""
Fetch GWAS summary statistics from Open Targets Platform for studies matching a trait.

Workflow for each study:
  1. Check if harmonized output already exists locally or in MinIO — skip if found.
  2. Download raw summary stats from summarystatsLocation (gs:// or https://).
  3. Inspect columns: if already in GWAS SSF format (Open Targets pre-harmonizes),
     mark as ready and skip the Nextflow harmonization pipeline.
  4. Otherwise flag the raw file as needing harmonization.

Requirements:
    pip install google-cloud-bigquery google-auth requests tqdm pandas

    For GCS-hosted files (gs:// URLs):
        Install the gcloud SDK so `gsutil` is on PATH, OR:
        pip install google-cloud-storage

Usage — via BigQuery (requires GCP credentials):
    python scripts/fetch_opentargets_sumstats.py \\
        --trait "ulcerative colitis" \\
        --output-dir data/ot_uc_sumstats \\
        --gcp-project my-gcp-project

Usage — via GWAS Catalog REST API (no credentials needed, same underlying data):
    python scripts/fetch_opentargets_sumstats.py \\
        --trait "ulcerative colitis" \\
        --source gwas-catalog \\
        --output-dir data/ot_uc_sumstats

Usage — from a pre-fetched CSV (columns: studyId, summarystatsLocation):
    python scripts/fetch_opentargets_sumstats.py \\
        --studies-csv studies.csv \\
        --output-dir data/ot_uc_sumstats

Results written to {output_dir}/manifest.csv with columns:
    studyId, summarystatsLocation, status, local_path, needs_harmonization
"""

import argparse
import gzip
import io
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from loguru import logger

# GWAS SSF standard column names emitted by Open Targets / EMBL-EBI harmonisation
_SSF_REQUIRED_COLS = {
    "chromosome",
    "base_pair_location",
    "effect_allele",
    "other_allele",
    "beta",
    "standard_error",
    "p_value",
}

# MinIO object-key prefix for Open Targets harmonised files
_MINIO_PREFIX = "harmonized/opentargets"


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def _bq_query(trait: str, gcp_project: str) -> pd.DataFrame:
    """Run the Open Targets study query against BigQuery and return a DataFrame."""
    try:
        from google.cloud import bigquery  # type: ignore
    except ImportError:
        logger.error(
            "google-cloud-bigquery is not installed. "
            "Run: pip install google-cloud-bigquery google-auth"
        )
        sys.exit(1)

    sql = """
        SELECT s.studyId, s.summarystatsLocation
        FROM `open-targets-prod.platform.studies` s
        WHERE LOWER(s.traitFromSource) LIKE @trait_pattern
          AND s.studyType = 'gwas'
          AND s.summarystatsLocation IS NOT NULL
    """
    client = bigquery.Client(project=gcp_project)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "trait_pattern", "STRING", f"%{trait.lower()}%"
            )
        ]
    )
    logger.info(f"[BQ] Querying Open Targets for trait matching '{trait}' ...")
    result = client.query(sql, job_config=job_config).to_dataframe()
    logger.info(f"[BQ] Found {len(result)} studies with summary stats.")
    return result


def _gwas_catalog_query(trait: str, max_results: int = 50) -> pd.DataFrame:
    """
    Query the GWAS Catalog REST API for studies matching *trait* that have
    summary statistics available.  Returns the same (studyId, summarystatsLocation)
    schema as the BigQuery path, so the rest of the pipeline is identical.

    The GWAS Catalog is the primary data source for Open Targets GWAS studies;
    the `summarystatsLocation` column in Open Targets BigQuery is derived from
    the same FTP paths returned here.
    """
    base = "https://www.ebi.ac.uk/gwas/rest/api"
    page, page_size = 0, 100
    all_studies: list[dict] = []

    logger.info(f"[GWAS-Catalog] Searching for studies matching '{trait}' …")
    while True:
        params = {
            "efoTrait": trait,
            "page": page,
            "size": page_size,
        }
        resp = requests.get(
            f"{base}/studies/search/findByEfoTrait",
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        embedded = data.get("_embedded", {}).get("studies", [])
        if not embedded:
            break

        all_studies.extend(embedded)
        logger.info(f"[GWAS-Catalog] Fetched page {page}: {len(embedded)} studies (total so far: {len(all_studies)})")

        page_info = data.get("page", {})
        total_pages = page_info.get("totalPages", 1)
        if page >= total_pages - 1:
            break
        page += 1

    logger.info(f"[GWAS-Catalog] Total studies found: {len(all_studies)}")

    rows: list[dict] = []
    for s in all_studies:
        if not s.get("fullPvalueSet", False):
            continue  # no summary stats available
        accession: str = s.get("accessionId", "")
        if not accession:
            continue

        # Build the canonical EBI FTP HTTPS URL for this study's harmonised folder.
        # Open Targets `summarystatsLocation` resolves to exactly this location.
        bucket_start = (int(accession.replace("GCST", "")) // 1000) * 1000 + 1
        bucket_end   = bucket_start + 999
        ftp_dir = (
            f"https://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/"
            f"GCST{bucket_start:06d}-GCST{bucket_end:06d}/{accession}/harmonised/"
        )
        rows.append({"studyId": accession, "summarystatsLocation": ftp_dir})
        if len(rows) >= max_results:
            break

    df = pd.DataFrame(rows, columns=["studyId", "summarystatsLocation"])
    logger.info(f"[GWAS-Catalog] {len(df)} studies with summary statistics.")
    return df


def _load_studies_csv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    required = {"studyId", "summarystatsLocation"}
    missing = required - set(df.columns)
    if missing:
        logger.error(f"CSV is missing required columns: {missing}")
        sys.exit(1)
    return df[["studyId", "summarystatsLocation"]].dropna()


# ---------------------------------------------------------------------------
# Existence checks
# ---------------------------------------------------------------------------

def _harmonized_local_path(output_dir: str, study_id: str) -> str:
    return os.path.join(output_dir, f"{study_id}_harmonized.tsv.gz")


def _raw_local_path(output_dir: str, study_id: str, url: str) -> str:
    ext = _url_extension(url)
    return os.path.join(output_dir, f"{study_id}_raw{ext}")


def _url_extension(url: str) -> str:
    """Best-effort file extension from a URL."""
    name = url.rstrip("/").split("/")[-1].split("?")[0]
    for ext in (".tsv.gz", ".vcf.gz", ".tsv", ".vcf", ".gz", ".parquet"):
        if name.endswith(ext):
            return ext
    return ".tsv.gz"


def _check_minio(study_id: str) -> Optional[str]:
    """Return MinIO object key if the harmonised file exists, else None."""
    try:
        from dotenv import load_dotenv

        load_dotenv()
        from src.services.storage import create_minio_client_from_env

        storage = create_minio_client_from_env()
        if storage is None:
            return None
        key = f"{_MINIO_PREFIX}/{study_id}_harmonized.tsv.gz"
        if storage.exists(key):
            return key
    except Exception as exc:
        logger.debug(f"[MinIO] Check skipped ({exc})")
    return None


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def _resolve_directory_url(dir_url: str) -> Optional[str]:
    """
    If *dir_url* is an HTTPS directory listing (ends with /), find the best
    summary-stats file inside it by parsing the HTML index.

    Preference order:
      1. *.h.tsv.gz  — EMBL-EBI harmonised (already in SSF format, no re-harmonisation needed)
      2. *.tsv.gz    — any other gzip TSV
      3. *.vcf.gz    — VCF fallback

    Returns the resolved absolute file URL, or None if nothing suitable is found.
    """
    try:
        import re

        resp = requests.get(dir_url, timeout=20)
        resp.raise_for_status()
        candidates = re.findall(r'href="([^"]+)"', resp.text)

        def _abs(name: str) -> str:
            return name if name.startswith("http") else dir_url.rstrip("/") + "/" + name.lstrip("/")

        # Prefer harmonised files first
        for name in candidates:
            if name.endswith(".h.tsv.gz"):
                return _abs(name)
        for name in candidates:
            if name.endswith(".tsv.gz"):
                return _abs(name)
        for name in candidates:
            if name.endswith(".vcf.gz"):
                return _abs(name)
    except Exception as exc:
        logger.warning(f"[DOWNLOAD] Could not resolve directory URL {dir_url}: {exc}")
    return None


def _download_https(url: str, dest_path: str) -> None:
    # If url is a directory, resolve to the actual file first
    if url.endswith("/"):
        resolved = _resolve_directory_url(url)
        if not resolved:
            raise RuntimeError(
                f"Could not find a .tsv.gz/.vcf.gz file in directory: {url}"
            )
        logger.info(f"[DOWNLOAD] Resolved directory → {resolved}")
        url = resolved

    logger.info(f"[DOWNLOAD] HTTP → {dest_path}")
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)
    logger.info(f"[DOWNLOAD] Done ({os.path.getsize(dest_path) / 1e6:.1f} MB)")


def _download_gcs(gcs_url: str, dest_path: str) -> None:
    """Download a gs:// URL using gsutil (preferred) or google-cloud-storage."""
    # Try gsutil first (simplest, honours application-default credentials)
    try:
        result = subprocess.run(
            ["gsutil", "cp", gcs_url, dest_path],
            capture_output=True,
            text=True,
            timeout=600,
        )
        if result.returncode == 0:
            logger.info(f"[DOWNLOAD] gsutil OK → {dest_path}")
            return
        logger.warning(f"[DOWNLOAD] gsutil failed: {result.stderr.strip()}")
    except FileNotFoundError:
        logger.warning("[DOWNLOAD] gsutil not found, trying google-cloud-storage SDK …")

    # Fall back to the Python SDK
    try:
        from google.cloud import storage as gcs  # type: ignore

        # gs://bucket/blob
        without_prefix = gcs_url[len("gs://"):]
        bucket_name, _, blob_name = without_prefix.partition("/")
        client = gcs.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(dest_path)
        logger.info(f"[DOWNLOAD] GCS SDK OK → {dest_path}")
    except Exception as exc:
        raise RuntimeError(
            f"Cannot download {gcs_url}: gsutil unavailable and google-cloud-storage failed ({exc}). "
            "Install the gcloud SDK or: pip install google-cloud-storage"
        ) from exc


def _download(url: str, dest_path: str) -> None:
    if url.startswith("gs://"):
        _download_gcs(url, dest_path)
    elif url.startswith(("http://", "https://")):
        _download_https(url, dest_path)
    else:
        raise ValueError(f"Unsupported URL scheme: {url}")


# ---------------------------------------------------------------------------
# SSF format detection
# ---------------------------------------------------------------------------

def _is_ssf_format(file_path: str) -> bool:
    """
    Return True if the file already contains GWAS SSF standard columns.
    Reads only the header line so this is fast even for large files.
    """
    try:
        opener = gzip.open if file_path.endswith(".gz") else open
        with opener(file_path, "rt", encoding="utf-8", errors="replace") as fh:
            header_line = fh.readline().rstrip("\n")
        cols = {c.strip().lower() for c in header_line.split("\t")}
        found = _SSF_REQUIRED_COLS & cols
        logger.debug(f"[SSF] {file_path}: matched {len(found)}/{len(_SSF_REQUIRED_COLS)} required cols")
        return len(found) == len(_SSF_REQUIRED_COLS)
    except Exception as exc:
        logger.warning(f"[SSF] Could not inspect {file_path}: {exc}")
        return False


# ---------------------------------------------------------------------------
# Main processing loop
# ---------------------------------------------------------------------------

def process_studies(studies: pd.DataFrame, output_dir: str) -> pd.DataFrame:
    """
    For each study:
      - skip if harmonized output already exists
      - download raw file
      - detect if already in SSF format
      - report status

    Returns a manifest DataFrame.
    """
    os.makedirs(output_dir, exist_ok=True)

    records = []

    for _, row in studies.iterrows():
        study_id: str = str(row["studyId"]).strip()
        sumstats_loc: str = str(row["summarystatsLocation"]).strip()

        record: dict = {
            "studyId": study_id,
            "summarystatsLocation": sumstats_loc,
            "status": None,
            "local_path": None,
            "needs_harmonization": False,
            "minio_key": None,
        }

        logger.info(f"[{study_id}] Processing …")

        # ── 1. Check for existing harmonized output ──────────────────────────
        harmonized_path = _harmonized_local_path(output_dir, study_id)
        if os.path.exists(harmonized_path):
            logger.info(f"[{study_id}] Harmonized file exists locally → skipping.")
            record.update(
                status="already_harmonized",
                local_path=harmonized_path,
                needs_harmonization=False,
            )
            records.append(record)
            continue

        minio_key = _check_minio(study_id)
        if minio_key:
            logger.info(f"[{study_id}] Harmonized file found in MinIO ({minio_key}) → skipping.")
            record.update(
                status="already_harmonized_minio",
                minio_key=minio_key,
                needs_harmonization=False,
            )
            records.append(record)
            continue

        # ── 2. Check for already-downloaded raw file ──────────────────────────
        raw_path = _raw_local_path(output_dir, study_id, sumstats_loc)
        if os.path.exists(raw_path):
            logger.info(f"[{study_id}] Raw file already present at {raw_path} → skipping download.")
        else:
            # ── 3. Download ──────────────────────────────────────────────────
            try:
                _download(sumstats_loc, raw_path)
            except Exception as exc:
                logger.error(f"[{study_id}] Download failed: {exc}")
                record.update(status="download_failed")
                records.append(record)
                continue

        # ── 4. Detect if already in SSF format ────────────────────────────────
        already_harmonized = _is_ssf_format(raw_path)

        if already_harmonized:
            # Rename to the harmonized path so downstream code finds it
            os.rename(raw_path, harmonized_path)
            logger.info(
                f"[{study_id}] File is already in SSF format → "
                f"renamed to {harmonized_path}, harmonization skipped."
            )
            record.update(
                status="ssf_ready",
                local_path=harmonized_path,
                needs_harmonization=False,
            )
        else:
            logger.info(
                f"[{study_id}] File is NOT in SSF format → "
                f"needs Nextflow harmonization."
            )
            record.update(
                status="needs_harmonization",
                local_path=raw_path,
                needs_harmonization=True,
            )

        records.append(record)

    manifest = pd.DataFrame(records)
    manifest_path = os.path.join(output_dir, "manifest.csv")
    manifest.to_csv(manifest_path, index=False)
    logger.info(f"\nManifest written to {manifest_path}")
    _print_summary(manifest)
    return manifest


def _print_summary(manifest: pd.DataFrame) -> None:
    counts = manifest["status"].value_counts()
    logger.info("── Summary ──────────────────────────────────")
    for status, n in counts.items():
        logger.info(f"  {status:<35} {n}")
    needs = manifest["needs_harmonization"].sum()
    if needs:
        logger.info(
            f"\n{needs} file(s) still need Nextflow harmonization.\n"
            "Pass the paths in the 'local_path' column to analysis_pipeline_flow()."
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--trait",
        help=(
            'Trait name to search (e.g. "ulcerative colitis"). '
            "Source depends on --source flag (default: bigquery)."
        ),
    )
    source.add_argument(
        "--studies-csv",
        metavar="PATH",
        help="CSV file with columns studyId and summarystatsLocation (skips API queries).",
    )
    p.add_argument(
        "--source",
        choices=["bigquery", "gwas-catalog"],
        default="bigquery",
        help=(
            "Where to query study metadata. "
            "'gwas-catalog' uses the public GWAS Catalog REST API (no GCP credentials needed). "
            "'bigquery' uses Open Targets BigQuery (requires --gcp-project). "
            "Default: bigquery."
        ),
    )
    p.add_argument(
        "--output-dir",
        required=True,
        metavar="DIR",
        help="Directory where downloaded / harmonized files will be stored.",
    )
    p.add_argument(
        "--gcp-project",
        metavar="PROJECT_ID",
        help="Google Cloud project to bill BigQuery queries to (required with --source bigquery).",
    )
    p.add_argument(
        "--max-studies",
        type=int,
        default=50,
        metavar="N",
        help="Maximum number of studies to process (default: 50).",
    )
    return p


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.studies_csv:
        studies = _load_studies_csv(args.studies_csv)
    elif args.source == "gwas-catalog":
        studies = _gwas_catalog_query(args.trait, max_results=args.max_studies)
    else:
        if not args.gcp_project:
            parser.error("--gcp-project is required when using --source bigquery.")
        studies = _bq_query(args.trait, args.gcp_project)

    if studies.empty:
        logger.warning("No studies found. Nothing to do.")
        sys.exit(0)

    if args.max_studies and len(studies) > args.max_studies:
        studies = studies.head(args.max_studies)

    logger.info(f"Processing {len(studies)} studies → {args.output_dir}")
    process_studies(studies, args.output_dir)


if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(_root))
    main()
