"""Semantic ranking of enriched GO terms against a GWAS phenotype.

Primary strategy: **hybrid** (GO_SEMANTIC_STRATEGY=llm or hybrid).
    1. Score the full enrichment table (embedding similarity + p-value hybrid score).
    2. Build a shortlist via `prefilter_go_candidates_union`: top-N by adjusted
       p-value, topped up with the highest-scoring remaining terms that also
       clear the significance floor. The cap is a ceiling, not a quota -- a gene
       with 18 real hits yields a shortlist of 18, never padded out with noise.
    3. Send the shortlist to an LLM (local gemma4 or OpenAI) for a single rerank call.
    4. Enforce two guarantees in code, independent of the model: no sub-floor
       term survives, and the highest-scoring candidates keep a share of the
       returned slots.
Switch model backend with GO_LLM_BACKEND=local|openai; no code change required.

`baseline` / `improved` / `embedding` and the legacy `llm-local` / `llm-openai`
(full-table batched map-reduce) strategies are kept for benchmarking via
scripts/replay_go_semantic_search.py but are not the production default.
"""

from __future__ import annotations

import math
import json
import os
import re
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import numpy as np
import openai
import pandas as pd
import scipy.spatial
from loguru import logger

DEFAULT_EMBEDDING_MODEL = "text-embedding-3-small"
DEFAULT_HYBRID_ALPHA = 0.7
MAX_GENES_IN_DOC = 20
DEFAULT_LLM_MAX_CANDIDATES = 250  # 0 in env = auto-cap; see _resolve_llm_candidate_cap
DEFAULT_LLM_PREFILTER_K = 250
DEFAULT_LLM_PREFILTER_PVALUE_K = 150
DEFAULT_LLM_MODEL = "gemma4"
DEFAULT_OPENAI_LLM_MODEL = "gpt-4o"
_SPECIFICITY_SCALE: tuple[int, float, float] | None = None

ALL_AB_STRATEGIES = (
    "baseline",
    "improved",
    "embedding",
    "hybrid-local",
    "hybrid-openai",
    "llm-local",
    "llm-openai",
)

_FIXTURE_LINE_RE = re.compile(
    r"^(?P<term>.+?)\s+\((?P<go_id>GO:\d+)\)\s+\|\s+adj_p=(?P<adj_p>[\d.eE+-]+)\s+\|\s+genes:\s*(?P<genes>.+)$"
)
_GO_ID_RE = re.compile(r"GO:\d+")


class GoSemanticStrategy(str, Enum):
    BASELINE = "baseline"
    IMPROVED = "improved"
    EMBEDDING = "embedding"
    HYBRID = "hybrid"  # alias: hybrid + GO_LLM_BACKEND
    HYBRID_LOCAL = "hybrid-local"
    HYBRID_OPENAI = "hybrid-openai"
    LLM = "llm"  # alias for hybrid-local (production default)
    LLM_LOCAL = "llm-local"
    LLM_OPENAI = "llm-openai"


def parse_go_semantic_strategy(value: str | None) -> GoSemanticStrategy:
    raw = (value or os.getenv("GO_SEMANTIC_STRATEGY", "llm")).strip().lower()
    if raw == "llm":
        return GoSemanticStrategy.HYBRID_LOCAL
    if raw == "hybrid":
        backend = os.getenv("GO_LLM_BACKEND", "local").strip().lower()
        if backend == "openai":
            return GoSemanticStrategy.HYBRID_OPENAI
        return GoSemanticStrategy.HYBRID_LOCAL
    try:
        return GoSemanticStrategy(raw)
    except ValueError:
        logger.warning(f"Unknown GO_SEMANTIC_STRATEGY={raw!r}; defaulting to hybrid-local")
        return GoSemanticStrategy.HYBRID_LOCAL


def llm_backend_for_strategy(strategy: GoSemanticStrategy) -> str:
    if strategy in (GoSemanticStrategy.LLM_OPENAI, GoSemanticStrategy.HYBRID_OPENAI):
        return "openai"
    return "local"


def is_hybrid_strategy(strategy: GoSemanticStrategy) -> bool:
    return strategy in (
        GoSemanticStrategy.HYBRID,
        GoSemanticStrategy.HYBRID_LOCAL,
        GoSemanticStrategy.HYBRID_OPENAI,
    )


def build_go_search_query(
    phenotype: str,
    causal_gene: Optional[str] = None,
    strategy: GoSemanticStrategy = GoSemanticStrategy.IMPROVED,
) -> str:
    if strategy == GoSemanticStrategy.BASELINE:
        return phenotype.strip()
    parts = [f"GWAS phenotype: {phenotype.strip()}."]
    if causal_gene:
        parts.append(f"Causal gene at locus: {causal_gene.strip()}.")
    parts.append(
        "Identify GO biological processes most relevant to this disease mechanism."
    )
    return " ".join(parts)


def build_document_text(row: pd.Series, strategy: GoSemanticStrategy) -> str:
    term = str(row["Term"]).strip()
    desc = str(row.get("Desc", "")).strip()
    if not desc or desc == "NA" or desc == "GO":
        desc = term
    text = f"{term} [SEP] {desc}"
    if strategy == GoSemanticStrategy.IMPROVED:
        genes = str(row.get("Genes", "")).strip()
        if genes:
            gene_list = genes.replace(",", ";").split(";")[:MAX_GENES_IN_DOC]
            gene_text = "; ".join(g.strip() for g in gene_list if g.strip())
            if gene_text:
                text += f" [SEP] Genes: {gene_text}"
    return text


def normalize_neg_log_p(pvalues: pd.Series) -> pd.Series:
    p = pvalues.astype(float).clip(lower=1e-300)
    neg_log = -np.log10(p)
    min_v, max_v = neg_log.min(), neg_log.max()
    if max_v == min_v:
        return pd.Series(1.0, index=pvalues.index)
    return (neg_log - min_v) / (max_v - min_v)


def normalize_specificity(term_sizes: pd.Series) -> pd.Series:
    """Information content from GO term size, on a fixed [0, 1] scale.

    Resnik information content, IC(t) = -log(p(t)) with p(t) the share of the
    annotation corpus the term covers, divided by log(N) to land in [0, 1].

    The scale is deliberately fixed to the corpus rather than min-maxed over
    the candidate pool. Pool-relative scaling made a term's specificity depend
    on its neighbours: in a pool of uniformly broad terms the least-broad one
    scored 1.0 and looked specific, and the same term scored differently from
    one run to the next. Against the corpus, 1922 genes always scores 0.21 and
    15 genes always scores 0.72.

    Terms of unknown size are treated as the broadest the library knows of,
    not as average. The asymmetry is deliberate: a term Enrichr scores but
    does not publish a gene set for is unmeasurable here, and guessing
    "average" hands it a reserved slot. GO:0006357 "regulation of
    transcription by RNA polymerase II" -- absent from every published
    Enrichr GO library -- took the top scored slot in both recorded cases on
    nothing but that default. Wrongly demoting an unmeasurable term costs one
    candidate; wrongly promoting one puts a truism in a slot reserved for
    specific biology, which is the failure this weighting exists to prevent.

    QuickGO was evaluated as a way to fill the gap properly and rejected: its
    counts agree with the library for small terms but diverge unpredictably
    for the rest (0.1x to 4.9x across sampled terms), so the two cannot be
    mixed on one scale.
    """
    sizes = pd.to_numeric(term_sizes, errors="coerce")
    if sizes.notna().sum() == 0:
        return pd.Series(0.5, index=term_sizes.index)

    corpus, lo_ic, hi_ic = _specificity_scale()
    clipped = sizes.clip(lower=1, upper=corpus).astype(float)
    ic = np.log(corpus / clipped) / np.log(corpus)
    if hi_ic > lo_ic:
        ic = (ic - lo_ic) / (hi_ic - lo_ic)
    unmeasured = int(ic.isna().sum())
    if unmeasured:
        logger.info(
            f"{unmeasured} of {len(ic)} terms have no known size; scoring them "
            "as the broadest rather than average"
        )
    return ic.fillna(0.0).clip(lower=0.0, upper=1.0)


def _specificity_scale() -> tuple[int, float, float]:
    """(corpus size, IC of the broadest term, IC of the narrowest term).

    The endpoints come from the library's own size distribution, so they are
    constant for a given library. Scaling to them keeps a term's specificity
    independent of its pool while still using the full [0, 1] range -- raw IC
    only spans about 0.21 to 0.83, which left specificity too compressed to
    weigh against similarity and significance.
    """
    global _SPECIFICITY_SCALE
    if _SPECIFICITY_SCALE is not None:
        return _SPECIFICITY_SCALE

    from src.services.enrich import go_corpus_gene_count, go_term_sizes

    corpus = max(int(go_corpus_gene_count()), 2)
    observed = [s for s in go_term_sizes().values() if s and s > 0]
    if observed:
        widest = min(max(observed), corpus)
        narrowest = max(min(observed), 1)
    else:
        widest, narrowest = corpus, 1
    lo = np.log(corpus / widest) / np.log(corpus)
    hi = np.log(corpus / narrowest) / np.log(corpus)
    _SPECIFICITY_SCALE = (corpus, float(lo), float(hi))
    return _SPECIFICITY_SCALE


def hybrid_score(
    similarity: pd.Series,
    pvalues: pd.Series,
    alpha: float = DEFAULT_HYBRID_ALPHA,
    term_sizes: pd.Series | None = None,
    weights: tuple[float, float, float] | None = None,
) -> pd.Series:
    """Blend semantic similarity, statistical signal and term specificity.
    """
    significance = normalize_neg_log_p(pvalues)
    if term_sizes is None:
        return alpha * similarity + (1.0 - alpha) * significance
    w_sim, w_sig, w_spec = weights or _resolve_score_weights()
    return (
        w_sim * similarity
        + w_sig * significance
        + w_spec * normalize_specificity(term_sizes)
    )


_DEFAULT_SCORE_WEIGHTS = (0.40, 0.20, 0.40)  # similarity, significance, specificity


def _resolve_score_weights() -> tuple[float, float, float]:
    """(similarity, significance, specificity) weights, normalized to sum 1.
    """
    raw = (
        float(os.getenv("GO_SCORE_W_SIMILARITY", str(_DEFAULT_SCORE_WEIGHTS[0]))),
        float(os.getenv("GO_SCORE_W_SIGNIFICANCE", str(_DEFAULT_SCORE_WEIGHTS[1]))),
        float(os.getenv("GO_SCORE_W_SPECIFICITY", str(_DEFAULT_SCORE_WEIGHTS[2]))),
    )
    total = sum(raw)
    if total <= 0:
        return _DEFAULT_SCORE_WEIGHTS
    return tuple(w / total for w in raw)


def significance_floor() -> float:
    """Max adjusted p-value for a term to count as a real enrichment hit.

    Delegates to the enrichment layer so the shortlist filter and this floor
    cannot drift apart. Imported lazily, as the other cross-service imports in
    this module are, to keep import cost off the module path.
    """
    from src.services.enrich import significance_floor as _floor

    return _floor()


EMBEDDING_BATCH_SIZE = 1024


def _embed_texts(texts: list[str], model: str) -> list[list[float]]:
    """Embed in batches; the API rejects arrays longer than 2048."""
    client = openai.Client()
    vectors: list[list[float]] = []
    for start in range(0, len(texts), EMBEDDING_BATCH_SIZE):
        chunk = texts[start : start + EMBEDDING_BATCH_SIZE]
        response = client.embeddings.create(input=chunk, model=model)
        vectors.extend(item.embedding for item in response.data)
    return vectors


def _rows_to_results(ranked: pd.DataFrame, score_column: str) -> list[dict]:
    results = []
    for rank, (_, row) in enumerate(ranked.iterrows(), start=1):
        genes_raw = str(row.get("Genes", ""))
        genes = [g.strip() for g in genes_raw.replace(",", ";").split(";") if g.strip()]
        entry = {
            "id": str(row["ID"]).strip(),
            "name": str(row["Term"]).strip(),
            "genes": genes,
            "p": float(row["Adjusted P-value"]),
            "rank": rank,
            "similarity": float(row.get("similarity", 0.0)),
        }
        if score_column in row:
            entry["score"] = float(row[score_column])
        results.append(entry)
    return results


def rank_go_terms(
    phenotype: str,
    enrich_tbl: pd.DataFrame,
    k: int = 10,
    strategy: GoSemanticStrategy = GoSemanticStrategy.IMPROVED,
    causal_gene: Optional[str] = None,
    embedding_model: Optional[str] = None,
    hybrid_alpha: float = DEFAULT_HYBRID_ALPHA,
) -> list[dict]:
    if enrich_tbl is None or len(enrich_tbl) == 0:
        logger.warning("Empty enrichment table provided for GO semantic search")
        return []

    data = _score_enrichment_table(
        phenotype,
        enrich_tbl,
        causal_gene=causal_gene,
        embedding_model=embedding_model,
        hybrid_alpha=hybrid_alpha,
        strategy=strategy,
    )
    if len(data) == 0:
        return []

    score_column = "score" if strategy == GoSemanticStrategy.IMPROVED else "similarity"
    if score_column == "similarity":
        data["score"] = data["similarity"]
    ranked = data.sort_values(score_column, ascending=False).head(k)
    return _rows_to_results(ranked, score_column)


def _score_enrichment_table(
    phenotype: str,
    enrich_tbl: pd.DataFrame,
    causal_gene: Optional[str] = None,
    embedding_model: Optional[str] = None,
    hybrid_alpha: float = DEFAULT_HYBRID_ALPHA,
    strategy: GoSemanticStrategy = GoSemanticStrategy.IMPROVED,
) -> pd.DataFrame:
    """Attach similarity and hybrid score columns to an enrichment table."""
    model = embedding_model or os.getenv("GO_EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL)
    data = enrich_tbl.copy()
    texts = [build_document_text(row, strategy) for _, row in data.iterrows()]
    if not texts:
        return data.iloc[0:0]

    query = build_go_search_query(phenotype, causal_gene=causal_gene, strategy=strategy)
    doc_embeddings = _embed_texts(texts, model)
    query_embedding = _embed_texts([query], model)[0]

    data["similarity"] = [
        1.0 - scipy.spatial.distance.cosine(emb, query_embedding)
        for emb in doc_embeddings
    ]
    data["score"] = hybrid_score(
        data["similarity"],
        data["Adjusted P-value"],
        alpha=hybrid_alpha,
        term_sizes=data["Term Size"] if "Term Size" in data.columns else None,
    )
    return data


def prefilter_go_candidates_union(
    phenotype: str,
    enrich_tbl: pd.DataFrame,
    top_n: int,
    causal_gene: Optional[str] = None,
    embedding_model: Optional[str] = None,
    hybrid_alpha: float = DEFAULT_HYBRID_ALPHA,
    pvalue_k: Optional[int] = None,
) -> pd.DataFrame:
    """Union shortlist: guaranteed top enrichment terms plus semantic top-ups.

    Two arms are merged so the LLM sees both worlds:
      - `pvalue_k` terms with the strongest raw enrichment signal (adj p-value),
        guaranteed regardless of how well they embed against the phenotype text.
      - The remaining slots (up to `top_n`) filled by highest hybrid score
        (embedding similarity blended with p-value) among terms not already picked.
    """
    if enrich_tbl is None or len(enrich_tbl) == 0:
        return enrich_tbl

    top_n = min(top_n, len(enrich_tbl))
    if pvalue_k is None:
        default_pvalue_k = max(top_n // 2, top_n - 100)
        pvalue_k = int(
            os.getenv("GO_LLM_PREFILTER_PVALUE_K", str(default_pvalue_k))
        )
    pvalue_k = min(max(pvalue_k, 0), top_n)

    data = _score_enrichment_table(
        phenotype,
        enrich_tbl,
        causal_gene=causal_gene,
        embedding_model=embedding_model,
        hybrid_alpha=hybrid_alpha,
    )
    if len(data) == 0:
        return data

    floor = significance_floor()
    above = data[data["Adjusted P-value"] < floor]
    pool = above if len(above) > 0 else data
    top_n = min(top_n, len(pool))

    by_pvalue = pool.sort_values("Adjusted P-value", ascending=True).head(
        min(pvalue_k, len(pool))
    )
    selected_ids = {str(row["ID"]).strip() for _, row in by_pvalue.iterrows()}
    remaining = top_n - len(by_pvalue)

    extras = []
    if remaining > 0:
        for _, row in pool.sort_values("score", ascending=False).iterrows():
            go_id = str(row["ID"]).strip()
            if go_id in selected_ids:
                continue
            extras.append(row)
            selected_ids.add(go_id)
            if len(extras) >= remaining:
                break

    if extras:
        shortlisted = pd.concat([by_pvalue, pd.DataFrame(extras)], ignore_index=True)
    else:
        shortlisted = by_pvalue.reset_index(drop=True)

    n_below = int((shortlisted["Adjusted P-value"] >= floor).sum())
    logger.info(
        f"Hybrid prefilter union: {len(by_pvalue)} by adj p-value + "
        f"{len(shortlisted) - len(by_pvalue)} by semantic score = "
        f"{len(shortlisted)} total (cap {top_n}); {len(above)} of {len(data)} "
        f"pool terms pass p<{floor}"
        + (
            f"; no term cleared the floor, falling back to the full pool "
            f"({n_below} sub-floor terms shortlisted)"
            if len(above) == 0
            else ""
        )
    )
    return shortlisted


def _go_eval_log_enabled() -> bool:
    return os.getenv("GO_EVAL_LOG", "").strip().lower() in {"1", "true", "yes"}


def _go_eval_log_dir() -> Path:
    return Path(os.getenv("GO_EVAL_LOG_DIR", "data/go_eval"))


def _maybe_log_go_eval_snapshot(
    *,
    phenotype: str,
    causal_gene: Optional[str],
    full_tbl: pd.DataFrame,
    shortlisted_tbl: pd.DataFrame,
    results: list[dict],
    error: Optional[str] = None,
) -> None:
    """Opt-in (GO_EVAL_LOG=1) audit trail for judging ranking quality.

    Writes the full candidate pool, the shortlist sent to the LLM and what it
    picked to one JSON file per call. Off by default, so it costs nothing in
    production. It also fires on the failure path, which is what makes a silent
    fallback to embedding-only ranking visible after the fact.
    """
    if not _go_eval_log_enabled():
        return
    out_dir = _go_eval_log_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = _safe_snapshot_part(f"{phenotype}_{causal_gene or 'nogene'}")
    ts = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%S")
    path = out_dir / f"{ts}_{stem}.json"
    base_cols = ["ID", "Term", "Desc", "Adjusted P-value", "Genes"]
    diag_cols = ["Term Size", "similarity", "score"]

    def _rows(df: pd.DataFrame) -> list[dict]:
        if not len(df):
            return []
        cols = base_cols + [c for c in diag_cols if c in df.columns]
        return df[cols].to_dict(orient="records")

    payload = {
        "phenotype": phenotype,
        "causal_gene": causal_gene,
        "full_pool_size": len(full_tbl),
        "shortlist_size": len(shortlisted_tbl),
        "full_pool": _rows(full_tbl),
        "shortlist": _rows(shortlisted_tbl),
        "llm_results": results,
        "error": error,
    }
    if error:
        path = path.with_name(f"{path.stem}_FAILED{path.suffix}")
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    status = f"FAILED ({error})" if error else "ok"
    logger.info(f"[GO_EVAL_LOG] wrote {path} (full_pool={len(full_tbl)}, shortlist={len(shortlisted_tbl)}, status={status})")


def _safe_snapshot_part(value: str | None, fallback: str = "unknown") -> str:
    if not value or not str(value).strip():
        return fallback
    return re.sub(r"[^\w.-]", "_", str(value).strip()).strip("_") or fallback


def enforce_significance_floor(
    results: list[dict], shortlisted: pd.DataFrame, k: int
) -> list[dict]:
    """Replace sub-floor picks with the best unused real hits.
    """
    if not results or shortlisted is None or len(shortlisted) == 0:
        return results
    if "Adjusted P-value" not in shortlisted.columns:
        return results

    floor = significance_floor()
    picked_ids = {str(r.get("id", "")).strip() for r in results}
    sort_col = "score" if "score" in shortlisted.columns else "Adjusted P-value"
    ascending = sort_col == "Adjusted P-value"
    spare = [
        row
        for _, row in shortlisted.sort_values(sort_col, ascending=ascending).iterrows()
        if str(row["ID"]).strip() not in picked_ids
        and float(row["Adjusted P-value"]) < floor
    ]

    repaired, swaps = [], 0
    for entry in results:
        p = entry.get("p")
        if p is not None and float(p) >= floor and spare:
            row = spare.pop(0)
            genes_raw = str(row.get("Genes", ""))
            repaired.append(
                {
                    **entry,
                    "id": str(row["ID"]).strip(),
                    "name": str(row["Term"]).strip(),
                    "genes": [
                        g.strip()
                        for g in genes_raw.replace(",", ";").split(";")
                        if g.strip()
                    ],
                    "p": float(row["Adjusted P-value"]),
                    "reason": (
                        f"Substituted by significance floor (p<{floor}): the model's "
                        f"pick had adjusted p={float(p):.3g} with no enrichment signal, "
                        "while this candidate does."
                    ),
                    "floor_substituted": True,
                }
            )
            swaps += 1
        else:
            if p is not None and float(p) >= floor:
                entry = {**entry, "below_significance_floor": True}
            repaired.append(entry)

    repaired = [{**entry, "rank": rank} for rank, entry in enumerate(repaired[:k], start=1)]
    if swaps:
        logger.info(
            f"Significance floor: replaced {swaps} of {len(results)} LLM picks "
            f"that had adj p >= {floor} with unused candidates that pass it"
        )
    return repaired


def specificity_quota(k: int) -> int:
    """Reserved slots for top-scored terms; off unless configured.

    Promotes on score alone, so without a redundancy check it seats nested
    variants of the same term. Re-enable via GO_SPECIFICITY_QUOTA_FRACTION.
    """
    raw = os.getenv("GO_SPECIFICITY_QUOTA_FRACTION", "0")
    try:
        fraction = float(raw)
    except ValueError:
        fraction = 0.4
    fraction = min(max(fraction, 0.0), 1.0)
    return int(math.floor(k * fraction))


def enforce_specificity_quota(
    results: list[dict], shortlisted: pd.DataFrame, k: int
) -> list[dict]:
    """Guarantee the highest-scoring candidates a share of the returned slots.
    """
    if not results or shortlisted is None or len(shortlisted) == 0:
        return results
    if "score" not in shortlisted.columns:
        return results

    quota = specificity_quota(k)
    if quota <= 0:
        return results

    ranked = shortlisted.sort_values("score", ascending=False)
    picked_ids = {str(r.get("id", "")).strip() for r in results[:k]}
    missing = [
        row
        for _, row in ranked.head(quota).iterrows()
        if str(row["ID"]).strip() not in picked_ids
    ]
    if not missing:
        return results

    # Drop the model's lowest-scored picks to make room, never its best ones.
    score_by_id = {
        str(row["ID"]).strip(): float(row["score"]) for _, row in ranked.iterrows()
    }
    kept = sorted(
        results[:k],
        key=lambda e: score_by_id.get(str(e.get("id", "")).strip(), float("-inf")),
        reverse=True,
    )[: k - len(missing)]

    promoted = []
    for row in missing:
        genes_raw = str(row.get("Genes", ""))
        promoted.append(
            {
                "id": str(row["ID"]).strip(),
                "name": str(row["Term"]).strip(),
                "genes": [
                    g.strip()
                    for g in genes_raw.replace(",", ";").split(";")
                    if g.strip()
                ],
                "p": float(row["Adjusted P-value"]),
                "reason": (
                    "Promoted by specificity quota: among the most specific "
                    "significantly-enriched terms for this gene, which the "
                    "ranker passed over in favour of broader ones."
                ),
                "quota_promoted": True,
            }
        )

    merged = sorted(
        kept + promoted,
        key=lambda e: score_by_id.get(str(e.get("id", "")).strip(), float("-inf")),
        reverse=True,
    )
    merged = [{**entry, "rank": rank} for rank, entry in enumerate(merged, start=1)]
    logger.info(
        f"Specificity quota: promoted {len(promoted)} of the top {quota} "
        f"highest-scoring candidates the ranker had omitted"
    )
    return merged


def go_llm_fallback_backend() -> Optional[str]:
    """Opt-in second-tier model, used only if the primary backend is unreachable.

    Off by default: a local backend is usually chosen for cost or privacy
    reasons, and silently spending on a hosted model every time that host
    blinks is not a decision this code should make on its own.
    """
    raw = os.getenv("GO_LLM_FALLBACK_BACKEND", "").strip().lower()
    return raw or None


def rank_go_terms_degraded(
    phenotype: str,
    enrich_tbl: pd.DataFrame,
    k: int = 10,
    causal_gene: Optional[str] = None,
    embedding_model: Optional[str] = None,
    hybrid_alpha: float = DEFAULT_HYBRID_ALPHA,
) -> list[dict]:
    """Rank without any LLM, keeping every guarantee the normal path gives.

    The significance floor and the specificity quota are arithmetic; they do
    not need a model. Only the *ordering within* the real hits, and the written
    reasons, are lost when no model is reachable.

    This exists because the previous fallback ranked the FULL unfiltered pool
    by embedding similarity alone -- no floor, no quota. On a live ulcerative
    colitis run that returned ten terms with adjusted p from 0.28 to 0.88,
    including TNF and NF-kB signalling: flawless-sounding disease biology with
    no statistical support whatsoever, reported as a successful run.

    Results are marked `ranking_degraded` so a caller can tell this apart from
    a real ranking.
    """
    if enrich_tbl is None or len(enrich_tbl) == 0:
        return []
    try:
        shortlisted = prefilter_go_candidates_union(
            phenotype,
            enrich_tbl,
            top_n=min(
                int(os.getenv("GO_LLM_PREFILTER_K", str(DEFAULT_LLM_PREFILTER_K))),
                len(enrich_tbl),
            ),
            causal_gene=causal_gene,
            embedding_model=embedding_model,
            hybrid_alpha=hybrid_alpha,
        )
    except Exception as exc:
        # Embeddings are a separate service and may be down too.
        logger.warning(
            f"Embedding scoring unavailable during degraded ranking ({exc}); "
            "falling back to significance and specificity only."
        )
        shortlisted = _score_without_embeddings(enrich_tbl)
    if shortlisted is None or len(shortlisted) == 0:
        return []

    ranked = shortlisted.sort_values("score", ascending=False).head(k)
    results = _rows_to_results(ranked, "score")
    results = enforce_significance_floor(results, shortlisted, k=k)
    results = enforce_specificity_quota(results, shortlisted, k=k)
    logger.info(
        f"Degraded GO ranking: {len(results)} terms chosen by score from "
        f"{len(shortlisted)} candidates, no LLM involved"
    )
    return [
        {
            **entry,
            "ranking_degraded": True,
            "reason": entry.get("reason")
            or "Selected by enrichment statistics and term specificity; "
            "no language model was reachable to rank these.",
        }
        for entry in results
    ]


def _score_without_embeddings(enrich_tbl: pd.DataFrame) -> pd.DataFrame:
    """Shortlist scored on statistics and specificity, with no embedding call."""
    data = enrich_tbl.copy()
    pvalues = pd.to_numeric(data["Adjusted P-value"], errors="coerce")
    floor = significance_floor()
    above = data[pvalues < floor]
    pool = (above if len(above) > 0 else data).copy()
    pool["similarity"] = 0.0
    pool["score"] = hybrid_score(
        pool["similarity"],
        pd.to_numeric(pool["Adjusted P-value"], errors="coerce"),
        term_sizes=pool["Term Size"] if "Term Size" in pool.columns else None,
    )
    return pool


def rank_go_terms_resilient(
    phenotype: str,
    enrich_tbl: pd.DataFrame,
    k: int = 10,
    strategy=None,
    causal_gene: Optional[str] = None,
    max_candidates: Optional[int] = None,
    llm=None,
) -> list[dict]:
    """Normal ranking, then an opt-in second model, then a model-free ranking.

    Every tier keeps the significance floor and the specificity quota, so a
    degraded answer is weaker but never statistically unsupported.
    """
    try:
        return rank_go_terms_by_strategy(
            phenotype,
            enrich_tbl,
            k=k,
            strategy=strategy,
            causal_gene=causal_gene,
            max_candidates=max_candidates,
            llm=llm,
        )
    except Exception as exc:
        logger.warning(f"GO ranking failed (strategy={strategy}): {exc}")

    fallback = go_llm_fallback_backend()
    if fallback:
        parsed = parse_go_semantic_strategy(
            strategy if isinstance(strategy, str) else None
        )
        if fallback != llm_backend_for_strategy(parsed):
            alt = (
                GoSemanticStrategy.HYBRID_OPENAI
                if fallback == "openai"
                else GoSemanticStrategy.HYBRID_LOCAL
            )
            try:
                logger.info(f"Retrying GO ranking on fallback backend {fallback}")
                return rank_go_terms_by_strategy(
                    phenotype,
                    enrich_tbl,
                    k=k,
                    strategy=alt,
                    causal_gene=causal_gene,
                    max_candidates=max_candidates,
                    llm=llm,
                )
            except Exception as exc:
                logger.warning(f"Fallback backend {fallback} also failed: {exc}")

    return rank_go_terms_degraded(
        phenotype, enrich_tbl, k=k, causal_gene=causal_gene
    )


def rank_go_terms_hybrid_llm(
    phenotype: str,
    enrich_tbl: pd.DataFrame,
    k: int = 10,
    causal_gene: Optional[str] = None,
    prefilter_k: Optional[int] = None,
    backend: str = "local",
    model: Optional[str] = None,
    embedding_model: Optional[str] = None,
    hybrid_alpha: float = DEFAULT_HYBRID_ALPHA,
) -> list[dict]:
    """Embedding+p-value union prefilter over the full pool, then one LLM rerank.

    This is the production strategy behind GO_SEMANTIC_STRATEGY=llm/hybrid.
    """
    if enrich_tbl is None or len(enrich_tbl) == 0:
        logger.warning("Empty enrichment table provided for hybrid LLM GO ranking")
        return []

    from src.services.llm import LLM

    if prefilter_k is None:
        prefilter_k = int(
            os.getenv("GO_LLM_PREFILTER_K", str(DEFAULT_LLM_PREFILTER_K))
        )
    prefilter_k = max(prefilter_k, k)
    pvalue_k = int(
        os.getenv("GO_LLM_PREFILTER_PVALUE_K", str(DEFAULT_LLM_PREFILTER_PVALUE_K))
    )

    pool_size = len(enrich_tbl)
    shortlisted = prefilter_go_candidates_union(
        phenotype,
        enrich_tbl,
        top_n=min(prefilter_k, pool_size),
        causal_gene=causal_gene,
        embedding_model=embedding_model,
        hybrid_alpha=hybrid_alpha,
        pvalue_k=pvalue_k,
    )
    logger.info(
        f"Hybrid LLM GO ranking: union prefilter kept {len(shortlisted)} of "
        f"{pool_size} terms for LLM rerank"
    )

    if model is None:
        if backend == "openai":
            model = os.getenv("GO_OPENAI_LLM_MODEL", DEFAULT_OPENAI_LLM_MODEL)
        else:
            model = os.getenv("GO_LLM_MODEL", DEFAULT_LLM_MODEL)

    llm = LLM()
    try:
        results = llm.rank_relevant_go_by_llm(
            phenotype=phenotype,
            enrich_tbl=shortlisted,
            k=k,
            causal_gene=causal_gene,
            max_candidates=len(shortlisted),
            model=model,
            backend=backend,
            prefiltered=True,
        )
    except Exception as exc:
        _maybe_log_go_eval_snapshot(
            phenotype=phenotype,
            causal_gene=causal_gene,
            full_tbl=enrich_tbl,
            shortlisted_tbl=shortlisted,
            results=[],
            error=str(exc),
        )
        raise

    results = enforce_significance_floor(results, shortlisted, k=k)
    results = enforce_specificity_quota(results, shortlisted, k=k)
    for entry in results:
        entry["prefilter"] = "pvalue_embedding_union"
        entry["prefilter_pool"] = pool_size
    _maybe_log_go_eval_snapshot(
        phenotype=phenotype,
        causal_gene=causal_gene,
        full_tbl=enrich_tbl,
        shortlisted_tbl=shortlisted,
        results=results,
    )
    return results


def rank_go_terms_llm(
    phenotype: str,
    enrich_tbl: pd.DataFrame,
    k: int = 10,
    causal_gene: Optional[str] = None,
    max_candidates: Optional[int] = None,
    backend: str = "local",
    model: Optional[str] = None,
) -> list[dict]:
    """Rank GO terms via an LLM reading the enrichment candidate pool."""
    if enrich_tbl is None or len(enrich_tbl) == 0:
        logger.warning("Empty enrichment table provided for LLM GO ranking")
        return []

    from src.services.llm import LLM

    if max_candidates is None:
        cap = int(os.getenv("GO_LLM_MAX_CANDIDATES", str(DEFAULT_LLM_MAX_CANDIDATES)))
    else:
        cap = max_candidates
    if model is None:
        if backend == "openai":
            model = os.getenv("GO_OPENAI_LLM_MODEL", DEFAULT_OPENAI_LLM_MODEL)
        else:
            model = os.getenv("GO_LLM_MODEL", DEFAULT_LLM_MODEL)
    llm = LLM()
    return llm.rank_relevant_go_by_llm(
        phenotype=phenotype,
        enrich_tbl=enrich_tbl,
        k=k,
        causal_gene=causal_gene,
        max_candidates=cap,
        model=model,
        backend=backend,
    )


def rank_go_terms_by_strategy(
    phenotype: str,
    enrich_tbl: pd.DataFrame,
    k: int = 10,
    strategy: GoSemanticStrategy | str | None = None,
    causal_gene: Optional[str] = None,
    embedding_model: Optional[str] = None,
    hybrid_alpha: float = DEFAULT_HYBRID_ALPHA,
    max_candidates: Optional[int] = None,
    llm=None,
) -> list[dict]:
    if isinstance(strategy, str) or strategy is None:
        strategy = parse_go_semantic_strategy(strategy)

    if is_hybrid_strategy(strategy):
        return rank_go_terms_hybrid_llm(
            phenotype,
            enrich_tbl,
            k=k,
            causal_gene=causal_gene,
            prefilter_k=max_candidates,
            backend=llm_backend_for_strategy(strategy),
            embedding_model=embedding_model,
            hybrid_alpha=hybrid_alpha,
        )
    if strategy in (
        GoSemanticStrategy.LLM,
        GoSemanticStrategy.LLM_LOCAL,
        GoSemanticStrategy.LLM_OPENAI,
    ):
        return rank_go_terms_llm(
            phenotype,
            enrich_tbl,
            k=k,
            causal_gene=causal_gene,
            max_candidates=max_candidates,
            backend=llm_backend_for_strategy(strategy),
        )
    if strategy == GoSemanticStrategy.EMBEDDING:
        if llm is None:
            from src.services.llm import LLM

            llm = LLM()
        return llm.get_relevant_go(phenotype, enrich_tbl, k=k)
    return rank_go_terms(
        phenotype,
        enrich_tbl,
        k=k,
        strategy=strategy,
        causal_gene=causal_gene,
        embedding_model=embedding_model,
        hybrid_alpha=hybrid_alpha,
    )


def parse_fixture_go_terms(path: str) -> pd.DataFrame:
    rows = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("Total GO terms:"):
                continue
            match = _FIXTURE_LINE_RE.match(line)
            if not match:
                continue
            rows.append(
                {
                    "ID": match.group("go_id"),
                    "Term": match.group("term").strip(),
                    "Desc": match.group("term").strip(),
                    "Adjusted P-value": float(match.group("adj_p")),
                    "Genes": match.group("genes").strip(),
                }
            )
    if not rows:
        raise ValueError(f"No GO terms parsed from fixture: {path}")
    return pd.DataFrame(rows)


def load_enrich_table(path: str) -> pd.DataFrame:
    if path.lower().endswith(".csv"):
        table = pd.read_csv(path)
        required = {"ID", "Term", "Desc", "Adjusted P-value", "Genes"}
        missing = required - set(table.columns)
        if missing:
            raise ValueError(f"CSV missing columns: {sorted(missing)}")
        return table
    return parse_fixture_go_terms(path)


def load_snapshot_metadata(path: str) -> dict[str, Any]:
    csv_path = Path(path)
    if csv_path.suffix == ".json":
        meta_path = csv_path
    elif csv_path.name.endswith("_enrich_tbl.csv"):
        meta_path = csv_path.with_name(
            csv_path.name[: -len("_enrich_tbl.csv")] + "_metadata.json"
        )
    else:
        meta_path = csv_path.with_suffix(".json")
    if not meta_path.is_file():
        raise FileNotFoundError(f"Metadata not found: {meta_path}")
    return json.loads(meta_path.read_text(encoding="utf-8"))


def parse_expected_go_ids(path: str) -> list[str]:
    with open(path, encoding="utf-8") as handle:
        content = handle.read()
    seen: set[str] = set()
    ordered: list[str] = []
    for go_id in _GO_ID_RE.findall(content):
        if go_id not in seen:
            seen.add(go_id)
            ordered.append(go_id)
    return ordered


def overlap_at_k(result_ids: list[str], expected_ids: list[str], k: int = 10) -> int:
    return len(set(result_ids[:k]) & set(expected_ids))
