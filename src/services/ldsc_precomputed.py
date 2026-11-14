"""
Precomputed cell-type LDSC results for OpenTargets studies, stored in MinIO.

`ldsc --h2-cts` output depends only on a study's summary statistics, the sample
size N written into them, and the LDSC reference files — nothing about the user
or project. For GWAS library studies that OpenTargets has fine-mapped, the
ldsc_precompute_flow runs it once per study and stores the raw
`.cell_type_results.txt` here; the analysis pipeline fetches it instead of
running LDSC, then ranks/maps/saves it exactly as it would a live result.

Layout (shared MinIO bucket, so every deployment reuses it):

  ldsc-precomputed/<fingerprint>/<study_id>/cell_type_results.txt
  ldsc-precomputed/<fingerprint>/<study_id>/manifest.json

<fingerprint> hashes the LDSC reference set (see reference_fingerprint), so a
changed CTS file or reference panel never reads stale results.
"""

import hashlib
import json
from datetime import datetime, timezone
from typing import Optional

from loguru import logger

# Bump when sumstats munging or the ldsc invocation changes.
LDSC_PRECOMPUTE_VERSION = 1
PREFIX = "ldsc-precomputed"
RESULTS_NAME = "cell_type_results.txt"
MANIFEST_NAME = "manifest.json"


def reference_fingerprint(config) -> str:
    """Short hash of everything besides the sumstats that determines LDSC output."""
    h = hashlib.sha256()
    h.update(f"v{LDSC_PRECOMPUTE_VERSION}\n".encode())
    for part in (
        config.get_ldsc_ref_ld_chr(),
        config.ldsc_w_ld_prefix,
        config.ldsc_w_hm3_snplist,
    ):
        h.update(f"{part}\n".encode())
    with open(config.resolve_ldsc_path(config.ldsc_cts_file), "rb") as fh:
        h.update(fh.read())
    return h.hexdigest()[:16]


def _keys(study_id: str, fingerprint: str) -> tuple:
    base = f"{PREFIX}/{fingerprint}/{study_id}"
    return f"{base}/{RESULTS_NAME}", f"{base}/{MANIFEST_NAME}"


def has_precomputed(storage, study_id: str, fingerprint: str) -> bool:
    _, manifest_key = _keys(study_id, fingerprint)
    return storage.exists(manifest_key)


def fetch_precomputed(storage, study_id: str, n: Optional[int], fingerprint: str,
                      dest_path: str) -> bool:
    """
    Download the precomputed results for *study_id* to *dest_path*. Returns
    False (caller runs LDSC live) if there is none, if it was computed with a
    different N (e.g. the user overrode the sample size), or on any error.
    """
    results_key, manifest_key = _keys(study_id, fingerprint)
    try:
        if not storage.exists(manifest_key):
            logger.info(f"[LDSC-PRE] No precomputed LDSC for {study_id} ({fingerprint})")
            return False
        manifest = json.loads(storage.download_string(manifest_key))
        if n is not None and int(manifest.get("n", -1)) != int(n):
            logger.info(
                f"[LDSC-PRE] Precomputed LDSC for {study_id} used N={manifest.get('n')}, "
                f"pipeline N={n} — running LDSC live"
            )
            return False
        if not storage.download_file(results_key, dest_path):
            return False
        logger.info(f"[LDSC-PRE] Using precomputed LDSC for {study_id} ({fingerprint})")
        return True
    except Exception as e:
        logger.warning(f"[LDSC-PRE] Could not fetch precomputed LDSC for {study_id}: {e}")
        return False


def publish_precomputed(storage, study_id: str, n: int, fingerprint: str,
                        results_path: str, ot_release: Optional[str] = None) -> bool:
    """Upload results, then the manifest — readers key off the manifest, so they
    never see one without its data."""
    results_key, manifest_key = _keys(study_id, fingerprint)
    if not storage.upload_file(results_path, results_key):
        return False
    with open(results_path) as fh:
        row_count = max(sum(1 for line in fh if line.strip()) - 1, 0)
    manifest = {
        "study_id": study_id,
        "n": int(n),
        "fingerprint": fingerprint,
        "ldsc_precompute_version": LDSC_PRECOMPUTE_VERSION,
        "ot_release": ot_release,
        "row_count": row_count,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    return storage.upload_string(json.dumps(manifest), manifest_key)
