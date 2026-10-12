from __future__ import annotations

import csv
import json
import os
import re
from dataclasses import dataclass, field
from typing import Optional

from loguru import logger


class CatlasMappingError(ValueError):
    """Raised when an LDSC cell type cannot be mapped to Census filters."""

    def __init__(self, message: str, *, ldsc_name: str | None = None) -> None:
        super().__init__(message)
        self.ldsc_name = ldsc_name

    def as_detail(self) -> dict[str, str]:
        detail = {
            "error_type": "catlas_mapping",
            "message": str(self),
        }
        if self.ldsc_name:
            detail["ldsc_name"] = self.ldsc_name
        return detail


@dataclass
class ResolvedCensusCellFilter:
    """How to query Census ``obs`` for one LDSC cell-type name."""

    ldsc_name: str
    #: Try these `cell_type` string filters in order (already lowercase where needed).
    cell_type_labels: list[str] = field(default_factory=list)
    #: Try these CL ids in order after labels exhausted.
    cl_ids: list[str] = field(default_factory=list)
    source: str = "direct_json"
    skip_coexpression: bool = False
    skip_reason: Optional[str] = None


def _escape_soma_string_literal(value: str) -> str:
    """Escape single quotes for SOMA value_filter string literals."""
    return value.replace("'", "''")


def _normalize_lookup_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


class DirectClJsonMapper:
    """
    Resolve LDSC ``Name`` (underscore form) to Census filters via
    ``catlas_celltype_cl_mapping.json``, using the aliases TSV only as a
    cre_key → Catlas label bridge.
    """

    def __init__(self, mapping_json_path: str, catlas_aliases_path: str) -> None:
        self._mapping_json_path = mapping_json_path
        self._catlas_aliases_path = catlas_aliases_path
        self._mapping: dict[str, dict] = {}
        self._by_norm_key: dict[str, str] = {}
        self._cre_key_to_stem: dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        with open(self._mapping_json_path, encoding="utf-8") as f:
            self._mapping = json.load(f)
        self._by_norm_key = {
            _normalize_lookup_key(key): key for key in self._mapping
        }

        with open(self._catlas_aliases_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                cre = (row.get("cre_key") or "").strip()
                stem = (row.get("abc_stem") or "").strip()
                if cre and stem:
                    self._cre_key_to_stem[cre] = stem

        logger.info(
            f"[CatlasCensus] loaded {len(self._mapping)} JSON mappings, "
            f"{len(self._cre_key_to_stem)} cre_key aliases"
        )

    def _lookup_json_key(self, ldsc_name: str) -> str:
        if ldsc_name in self._mapping:
            return ldsc_name

        spaced = ldsc_name.replace("_", " ")
        if spaced in self._mapping:
            return spaced

        stem = self._cre_key_to_stem.get(ldsc_name)
        if stem and stem in self._mapping:
            return stem

        for candidate in (ldsc_name, spaced, stem):
            if not candidate:
                continue
            norm_key = _normalize_lookup_key(candidate)
            hit = self._by_norm_key.get(norm_key)
            if hit:
                return hit

        raise CatlasMappingError(
            f"No catlas mapping entry for LDSC cell type {ldsc_name!r}",
            ldsc_name=ldsc_name,
        )

    def resolve(self, ldsc_name: str) -> ResolvedCensusCellFilter:
        json_key = self._lookup_json_key(ldsc_name)
        entry = self._mapping[json_key]

        if entry.get("match_method") == "no_match" or not entry.get("cl_id"):
            logger.info(
                f"[CatlasCensus] no Census mapping for LDSC {ldsc_name!r} "
                f"({json_key!r}, match_method={entry.get('match_method')!r}) — skipping coexpression"
            )
            return ResolvedCensusCellFilter(
                ldsc_name=ldsc_name,
                skip_coexpression=True,
                skip_reason=f"no_match (match_method={entry.get('match_method')!r})",
            )

        labels: list[str] = []
        census_name = entry.get("matched_census_name")
        if census_name:
            low = census_name.strip().lower()
            if low:
                labels.append(low)

        cl_ids: list[str] = [entry["cl_id"]]
        parent_cl = entry.get("cellxgene_parent_cl_id")
        if parent_cl and parent_cl not in cl_ids:
            cl_ids.append(parent_cl)

        return ResolvedCensusCellFilter(
            ldsc_name=ldsc_name,
            cell_type_labels=labels,
            cl_ids=cl_ids,
            source="direct_json",
        )


_mapper_instance: DirectClJsonMapper | None = None


def resolve_ldsc_for_census(
    ldsc_name: str,
    *,
    repo_root: str,
    mapping_json_rel: str,
    catlas_aliases_rel: str,
) -> ResolvedCensusCellFilter:
    """
    Resolve LDSC name to Census filters.

    Returns a ResolvedCensusCellFilter with skip_coexpression=True (not a raise)
    when the LDSC cell type has no Census/Catlas mapping (match_method="no_match"
    or missing cl_id) — that's an expected, non-fatal outcome for some LDSC cell
    types. Still raises CatlasMappingError for genuinely fatal problems: the
    mapping JSON/aliases file missing entirely, or ldsc_name having no entry in
    the mapping JSON at all (a data/config problem, not just "unmapped").
    """
    mapping_path = os.path.normpath(os.path.join(repo_root, mapping_json_rel))
    aliases_path = os.path.normpath(os.path.join(repo_root, catlas_aliases_rel))
    if not os.path.isfile(mapping_path):
        raise CatlasMappingError(
            f"Catlas mapping JSON not found: {mapping_path}"
        )
    if not os.path.isfile(aliases_path):
        raise CatlasMappingError(
            f"Catlas aliases TSV not found: {aliases_path}"
        )
    return get_direct_cl_json_mapper(mapping_path, aliases_path).resolve(ldsc_name)


def get_direct_cl_json_mapper(
    mapping_json_path: str,
    catlas_aliases_path: str,
) -> DirectClJsonMapper:
    global _mapper_instance
    if _mapper_instance is None:
        _mapper_instance = DirectClJsonMapper(
            mapping_json_path=mapping_json_path,
            catlas_aliases_path=catlas_aliases_path,
        )
    return _mapper_instance


def reset_catlas_census_mapper_for_tests() -> None:
    global _mapper_instance
    _mapper_instance = None


def validate_ldsc_tissue_mapping(
    ldsc_name: str,
    *,
    repo_root: str,
    mapping_json_rel: str,
    catlas_aliases_rel: str,
) -> ResolvedCensusCellFilter:
    """Resolve LDSC tissue mapping or raise ``CatlasMappingError``."""
    return resolve_ldsc_for_census(
        ldsc_name,
        repo_root=repo_root,
        mapping_json_rel=mapping_json_rel,
        catlas_aliases_rel=catlas_aliases_rel,
    )
