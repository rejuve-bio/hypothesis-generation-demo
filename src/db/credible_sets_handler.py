"""
Handler for querying OpenTargets pre-computed credible sets from PostgreSQL.

Used to check whether a GWAS study already has credible sets available so the
pipeline can skip local SuSiE fine-mapping and return OpenTargets results directly.
"""

import json
import math
import os
from typing import Optional

from loguru import logger

from src.db.credible_sets_schema import DATASET, connection_params_from_env

try:
    import psycopg2
    import psycopg2.extras
    _PSYCOPG2_OK = True
except ImportError:
    _PSYCOPG2_OK = False


def _parse_variant_id(variant_id: str) -> dict:
    """Parse 'chr_pos_ref_alt' into component fields."""
    parts = variant_id.split("_")
    if len(parts) >= 4:
        return {
            "chromosome": parts[0],
            "position": int(parts[1]),
            "ref_allele": parts[2],
            "minor_allele": parts[3],
        }
    return {"chromosome": None, "position": None, "ref_allele": None, "minor_allele": None}


def _log_pvalue(mantissa, exponent) -> Optional[float]:
    """Convert mantissa/exponent p-value to -log10(p)."""
    if mantissa is None or exponent is None:
        return None
    try:
        return -(math.log10(float(mantissa)) + int(exponent))
    except (ValueError, TypeError, ZeroDivisionError):
        return None


def build_rsid_lookup_from_harmonized_file(harmonized_file: str) -> dict:
    """
    Build a {(chromosome, position): rsid} lookup from a harmonized sumstats
    file, so OpenTargets credible sets (which carry no rsID) can be enriched
    with the rsIDs the harmonization step already resolved.

    Returns an empty dict if the file has no rsid/RS_ID column — callers must
    treat missing entries as "no rsid available", not an error.
    """
    import pandas as pd

    if not harmonized_file or not os.path.exists(harmonized_file):
        return {}

    try:
        header = pd.read_csv(harmonized_file, sep='\t', compression='gzip', nrows=0, low_memory=False)
    except Exception as exc:
        logger.warning(f"[CredibleSets] could not read harmonized file header for rsid lookup: {exc}")
        return {}

    rsid_col = next((c for c in ('rsid', 'RS_ID') if c in header.columns), None)
    if rsid_col is None or 'chromosome' not in header.columns or 'base_pair_location' not in header.columns:
        logger.info("[CredibleSets] harmonized file has no rsid column — OT credible sets will have null rs_id")
        return {}

    df = pd.read_csv(
        harmonized_file, sep='\t', compression='gzip', low_memory=False,
        usecols=['chromosome', 'base_pair_location', rsid_col],
    )
    lookup = {}
    for chrom, pos, rsid in zip(df['chromosome'], df['base_pair_location'], df[rsid_col]):
        if pd.notna(rsid):
            lookup[(str(chrom), int(pos))] = rsid
    return lookup


def convert_ot_row_to_credible_set(ot_row: dict, coverage: float = 0.95, rsid_lookup: Optional[dict] = None) -> dict:
    """
    Convert one OpenTargets credible set row (from the credible_set table) into the
    format expected by AnalysisHandler.save_credible_set().

    *rsid_lookup*, if given, is a {(chromosome, position): rsid} map (see
    build_rsid_lookup_from_harmonized_file) used to enrich OT variants, which
    otherwise carry no rsID at all.
    """
    rsid_lookup = rsid_lookup or {}
    locus = ot_row.get("locus") or []
    if isinstance(locus, str):
        locus = json.loads(locus)

    variants, posterior_probs, betas, chromosomes = [], [], [], []
    log_pvalues, positions, ref_alleles, minor_alleles, rs_ids = [], [], [], [], []
    ref_allele_freqs = []

    lead_eaf = ot_row.get("effect_allele_frequency")
    lead_log_p = _log_pvalue(ot_row.get("p_value_mantissa"), ot_row.get("p_value_exponent"))

    for v in locus:
        vid = v.get("variantId", "")
        parsed = _parse_variant_id(vid)

        variants.append(vid)
        posterior_probs.append(float(v.get("posteriorProbability") or 0))
        betas.append(float(v.get("beta") or ot_row.get("beta") or 0))
        chromosomes.append(parsed["chromosome"] or ot_row.get("chromosome"))
        positions.append(parsed["position"] or ot_row.get("position"))
        ref_alleles.append(parsed["ref_allele"])
        minor_alleles.append(parsed["minor_allele"])
        chrom = parsed["chromosome"] or ot_row.get("chromosome")
        pos = parsed["position"] or ot_row.get("position")
        rs_ids.append(rsid_lookup.get((str(chrom), int(pos))) if chrom is not None and pos is not None else None)
        ref_allele_freqs.append(float(lead_eaf) if lead_eaf is not None else None)

        lv = _log_pvalue(v.get("pValueMantissa"), v.get("pValueExponent"))
        log_pvalues.append(lv if lv is not None else lead_log_p)

    variants_data = {
        "variant": variants,
        "posterior_prob": posterior_probs,
        "beta": betas,
        "chromosome": chromosomes,
        "log_pvalue": log_pvalues,
        "position": positions,
        "ref_allele": ref_alleles,
        "minor_allele": minor_alleles,
        "ref_allele_freq": ref_allele_freqs,
        "rs_id": rs_ids,
    }

    return {
        "coverage": coverage,
        "variants": {"data": variants_data},
        "metadata": {
            "source": "opentargets",
            "study_locus_id": ot_row.get("study_locus_id"),
            "finemap_method": ot_row.get("finemap_method"),
            "confidence": ot_row.get("confidence"),
            "chr": ot_row.get("chromosome"),
            "position": ot_row.get("position"),
        },
    }


# Columns returned for a credible set, aliased to the keys
# convert_ot_row_to_credible_set() expects (the tables use Prolog's naming).
_CREDIBLE_SET_SELECT = """
    SELECT studylocusid AS study_locus_id, studyid AS study_id, variantid AS variant_id,
           chromosome, position,
           beta, pvaluemantissa AS p_value_mantissa, pvalueexponent AS p_value_exponent,
           effectallelefrequencyfromsource AS effect_allele_frequency,
           standarderror AS standard_error,
           finemappingmethod AS finemap_method, confidence, locus
    FROM credible_set
"""

# Study fields returned to callers, aliased to the keys they already use.
_STUDY_SELECT_FIELDS = """
    s.studyid AS study_id, s.projectid AS project_id, s.traitfromsource AS trait_from_source,
    s.traitfromsourcemappedids AS trait_efo_ids, s.hassumstats AS has_sumstats,
    s.summarystatslocation AS summarystats_location, s.nsamples AS n_samples
"""


class CredibleSetsHandler:
    """
    Query OpenTargets data from the shared PostgreSQL database (see
    src/db/credible_sets_schema.py). The tables hold every study type because
    the Prolog backend needs QTL data; this handler only ever returns GWAS.
    """

    def __init__(self):
        self._conn = None

    def _get_conn(self):
        if not _PSYCOPG2_OK:
            raise RuntimeError("psycopg2 is not installed")
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(**connection_params_from_env())
        return self._conn

    def get_load_state(self) -> Optional[dict]:
        """
        The ot_load_state marker written by the OT DB bootstrap (release,
        schema version, source, row counts), or None if the DB has never been
        fully loaded or is unreachable.
        """
        if not _PSYCOPG2_OK:
            return None
        try:
            conn = self._get_conn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT ot_release, schema_version, source, row_counts, loaded_at "
                    "FROM ot_load_state WHERE dataset = %s",
                    (DATASET,),
                )
                row = cur.fetchone()
                return dict(row) if row else None
        except Exception as exc:
            logger.warning(f"[CredibleSets] get_load_state: {exc}")
            try:
                self._conn.rollback()
            except Exception:
                pass
            return None

    def study_has_credible_sets(self, study_id: str) -> bool:
        """Fast check: does this GWAS study have credible sets in OpenTargets?"""
        if not study_id or not _PSYCOPG2_OK:
            return False
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM gwas_study_index WHERE studyid = %s LIMIT 1",
                    (study_id,),
                )
                return cur.fetchone() is not None
        except Exception as exc:
            logger.warning(f"[CredibleSets] study_has_credible_sets({study_id}): {exc}")
            try:
                self._conn.rollback()
            except Exception:
                pass
            return False

    def get_study_by_id(self, study_id: str) -> Optional[dict]:
        """
        Look up a GWAS study's own OpenTargets record (study_id, project_id,
        trait, has_sumstats, summarystats_location, n_samples) from the
        `study` table. QTL studies are never returned.

        Returns None if the study isn't known to OpenTargets, the table hasn't
        been loaded yet, or the lookup otherwise fails — callers should treat
        this as "provenance not confirmed", not a hard error.
        """
        if not study_id or not _PSYCOPG2_OK:
            return None
        try:
            conn = self._get_conn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT {_STUDY_SELECT_FIELDS}
                    FROM study s
                    WHERE s.studyid = %s AND s.studytype = 'gwas'
                    LIMIT 1
                    """,
                    (study_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None
        except Exception as exc:
            logger.warning(f"[CredibleSets] get_study_by_id({study_id}): {exc}")
            try:
                self._conn.rollback()
            except Exception:
                pass
            return None

    def is_study_known_to_opentargets(self, study_id: str) -> bool:
        """
        True if OpenTargets has any record of this GWAS study — either its own
        study record (study) or precomputed credible sets (gwas_study_index) —
        confirming the file the user selected genuinely
        originates from OpenTargets (and is therefore already pre-harmonized),
        rather than just happening to look like SSF format.
        """
        return self.get_study_by_id(study_id) is not None or self.study_has_credible_sets(study_id)

    def get_credible_sets_for_region(
        self,
        study_id: str,
        chromosome: str,
        position_start: int,
        position_end: int,
    ) -> list:
        """
        Fetch all credible sets for a study within a genomic window.
        Returns raw rows as dicts — pass each through convert_ot_row_to_credible_set()
        before saving.
        """
        if not study_id or not _PSYCOPG2_OK:
            return []
        try:
            conn = self._get_conn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    _CREDIBLE_SET_SELECT + """
                    WHERE studyid = %s
                      AND studytype = 'gwas'
                      AND chromosome = %s
                      AND position BETWEEN %s AND %s
                    ORDER BY position
                    """,
                    (study_id, str(chromosome), position_start, position_end),
                )
                return [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.warning(f"[CredibleSets] get_credible_sets_for_region({study_id}): {exc}")
            return []

    def search_studies_by_trait(self, trait: str, limit: int = 20) -> list:
        """
        Search OpenTargets GWAS studies by trait text (against the `study`
        table, GWAS only), joined with whether each study already has
        precomputed credible sets in gwas_study_index.

        Lets a user pick a real OpenTargets study directly by trait, rather than
        needing an existing gwas_library entry to already carry a matching
        study_id.
        """
        if not trait or not _PSYCOPG2_OK:
            return []
        try:
            conn = self._get_conn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT {_STUDY_SELECT_FIELDS},
                           (g.studyid IS NOT NULL) AS has_credible_sets,
                           g.credible_set_count
                    FROM study s
                    LEFT JOIN gwas_study_index g ON g.studyid = s.studyid
                    WHERE s.studytype = 'gwas'
                      AND s.traitfromsource ILIKE %s
                    ORDER BY has_credible_sets DESC, s.hassumstats DESC
                    LIMIT %s
                    """,
                    (f"%{trait}%", limit),
                )
                return [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.warning(f"[CredibleSets] search_studies_by_trait({trait}): {exc}")
            return []

    def get_all_credible_sets_for_study(self, study_id: str) -> list:
        """
        Fetch every credible set for a study, regardless of genomic region.
        Used to bypass COJO + fine-mapping entirely when OpenTargets already
        has full study-level credible sets. Returns raw rows as dicts — pass
        each through convert_ot_row_to_credible_set() before saving.
        """
        if not study_id or not _PSYCOPG2_OK:
            return []
        try:
            conn = self._get_conn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    _CREDIBLE_SET_SELECT + """
                    WHERE studyid = %s
                      AND studytype = 'gwas'
                    ORDER BY chromosome, position
                    """,
                    (study_id,),
                )
                return [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.warning(f"[CredibleSets] get_all_credible_sets_for_study({study_id}): {exc}")
            return []

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()
