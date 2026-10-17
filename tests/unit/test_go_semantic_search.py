from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.services import go_semantic_search as service
from src.services.go_semantic_search import GoSemanticStrategy


def _enrich_tbl(rows):
    """rows: list of (go_id, term, adj_p, genes)"""
    return pd.DataFrame(
        [
            {"ID": go_id, "Term": term, "Desc": term, "Adjusted P-value": adj_p, "Genes": genes}
            for go_id, term, adj_p, genes in rows
        ]
    )


# --- strategy parsing / dispatch -------------------------------------------------


def test_parse_go_semantic_strategy_llm_alias_maps_to_hybrid_local():
    assert service.parse_go_semantic_strategy("llm") == GoSemanticStrategy.HYBRID_LOCAL


def test_parse_go_semantic_strategy_hybrid_respects_backend_env(monkeypatch):
    monkeypatch.setenv("GO_LLM_BACKEND", "openai")
    assert service.parse_go_semantic_strategy("hybrid") == GoSemanticStrategy.HYBRID_OPENAI
    monkeypatch.setenv("GO_LLM_BACKEND", "local")
    assert service.parse_go_semantic_strategy("hybrid") == GoSemanticStrategy.HYBRID_LOCAL


def test_parse_go_semantic_strategy_unknown_value_falls_back_to_hybrid_local():
    assert service.parse_go_semantic_strategy("not-a-real-strategy") == GoSemanticStrategy.HYBRID_LOCAL


def test_parse_go_semantic_strategy_explicit_enum_names_roundtrip():
    assert service.parse_go_semantic_strategy("baseline") == GoSemanticStrategy.BASELINE
    assert service.parse_go_semantic_strategy("embedding") == GoSemanticStrategy.EMBEDDING


@pytest.mark.parametrize(
    "strategy,expected_backend",
    [
        (GoSemanticStrategy.LLM_OPENAI, "openai"),
        (GoSemanticStrategy.HYBRID_OPENAI, "openai"),
        (GoSemanticStrategy.LLM_LOCAL, "local"),
        (GoSemanticStrategy.HYBRID_LOCAL, "local"),
        (GoSemanticStrategy.BASELINE, "local"),
    ],
)
def test_llm_backend_for_strategy(strategy, expected_backend):
    assert service.llm_backend_for_strategy(strategy) == expected_backend


def test_is_hybrid_strategy():
    assert service.is_hybrid_strategy(GoSemanticStrategy.HYBRID_LOCAL)
    assert service.is_hybrid_strategy(GoSemanticStrategy.HYBRID_OPENAI)
    assert not service.is_hybrid_strategy(GoSemanticStrategy.LLM_LOCAL)
    assert not service.is_hybrid_strategy(GoSemanticStrategy.BASELINE)


def test_rank_go_terms_by_strategy_dispatches_hybrid_to_hybrid_llm(monkeypatch):
    hybrid_mock = MagicMock(return_value=["hybrid-result"])
    monkeypatch.setattr(service, "rank_go_terms_hybrid_llm", hybrid_mock)
    tbl = _enrich_tbl([("GO:1", "term", 0.01, "GENE1")])

    result = service.rank_go_terms_by_strategy(
        "phenotype", tbl, k=5, strategy="llm", causal_gene="GENE1", max_candidates=100
    )

    assert result == ["hybrid-result"]
    hybrid_mock.assert_called_once_with(
        "phenotype", tbl, k=5, causal_gene="GENE1", prefilter_k=100,
        backend="local", embedding_model=None, hybrid_alpha=service.DEFAULT_HYBRID_ALPHA,
    )


def test_rank_go_terms_by_strategy_dispatches_plain_llm_strategy(monkeypatch):
    llm_mock = MagicMock(return_value=["llm-result"])
    monkeypatch.setattr(service, "rank_go_terms_llm", llm_mock)
    tbl = _enrich_tbl([("GO:1", "term", 0.01, "GENE1")])

    result = service.rank_go_terms_by_strategy(
        "phenotype", tbl, k=5, strategy=GoSemanticStrategy.LLM_OPENAI, causal_gene="GENE1"
    )

    assert result == ["llm-result"]
    llm_mock.assert_called_once_with(
        "phenotype", tbl, k=5, causal_gene="GENE1", max_candidates=None, backend="openai"
    )


def test_rank_go_terms_by_strategy_embedding_uses_provided_llm(monkeypatch):
    llm = MagicMock()
    llm.get_relevant_go.return_value = ["embedding-result"]
    tbl = _enrich_tbl([("GO:1", "term", 0.01, "GENE1")])

    result = service.rank_go_terms_by_strategy(
        "phenotype", tbl, k=3, strategy=GoSemanticStrategy.EMBEDDING, llm=llm
    )

    assert result == ["embedding-result"]
    llm.get_relevant_go.assert_called_once_with("phenotype", tbl, k=3)


def test_rank_go_terms_by_strategy_falls_back_to_plain_rank_go_terms(monkeypatch):
    rank_mock = MagicMock(return_value=["baseline-result"])
    monkeypatch.setattr(service, "rank_go_terms", rank_mock)
    tbl = _enrich_tbl([("GO:1", "term", 0.01, "GENE1")])

    result = service.rank_go_terms_by_strategy(
        "phenotype", tbl, k=5, strategy=GoSemanticStrategy.BASELINE
    )

    assert result == ["baseline-result"]
    rank_mock.assert_called_once()


# --- pure scoring math ------------------------------------------------------------


def test_normalize_neg_log_p_maps_smallest_p_to_one():
    p = pd.Series([1e-10, 1e-2, 0.5])
    normalized = service.normalize_neg_log_p(p)
    assert normalized.iloc[0] == pytest.approx(1.0)
    assert normalized.iloc[-1] == pytest.approx(0.0)
    assert (normalized >= 0).all() and (normalized <= 1).all()


def test_normalize_neg_log_p_constant_column_returns_all_ones():
    p = pd.Series([0.01, 0.01, 0.01])
    normalized = service.normalize_neg_log_p(p)
    assert (normalized == 1.0).all()


def test_hybrid_score_blends_similarity_and_pvalue_by_alpha():
    similarity = pd.Series([1.0, 0.0])
    pvalues = pd.Series([0.5, 0.5])  # normalize_neg_log_p -> [1.0, 0.0] or [0.0, 1.0]? constant->1.0
    score = service.hybrid_score(similarity, pvalues, alpha=1.0)
    # alpha=1.0 means pure similarity
    assert list(score) == [1.0, 0.0]

    score_alpha_0 = service.hybrid_score(similarity, pvalues, alpha=0.0)
    # alpha=0.0 means pure p-value signal; constant p-values normalize to 1.0 for both
    assert list(score_alpha_0) == [1.0, 1.0]


# --- prefilter union: the core "surface the most significant terms" fix ----------


def _patch_score_enrichment_table(monkeypatch, similarity_by_id):
    """Avoid real embedding calls: stub _score_enrichment_table to attach a
    caller-controlled similarity/score column instead of calling OpenAI."""

    def fake_score(phenotype, enrich_tbl, causal_gene=None, embedding_model=None,
                    hybrid_alpha=service.DEFAULT_HYBRID_ALPHA, strategy=None):
        data = enrich_tbl.copy()
        data["similarity"] = [similarity_by_id.get(row["ID"], 0.0) for _, row in data.iterrows()]
        data["score"] = service.hybrid_score(data["similarity"], data["Adjusted P-value"], alpha=hybrid_alpha)
        return data

    monkeypatch.setattr(service, "_score_enrichment_table", fake_score)


def test_prefilter_union_guarantees_top_pvalue_terms_even_with_low_similarity(monkeypatch):
    # GO:1 has the strongest p-value but the worst semantic similarity to the
    # phenotype text -- exactly the case the old embedding-only ranking would
    # drop. The union prefilter must still keep it.
    _patch_score_enrichment_table(monkeypatch, {"GO:1": 0.0, "GO:2": 0.9, "GO:3": 0.8})
    tbl = _enrich_tbl(
        [
            ("GO:1", "strong signal, semantically distant", 1e-8, "GENE1"),
            ("GO:2", "weak signal, semantically close", 0.02, "GENE2"),
            ("GO:3", "mid signal, semantically close", 0.01, "GENE3"),
        ]
    )

    shortlist = service.prefilter_go_candidates_union(
        "phenotype", tbl, top_n=2, pvalue_k=1
    )

    assert "GO:1" in set(shortlist["ID"])
    assert len(shortlist) == 2


def test_prefilter_union_deduplicates_between_pvalue_and_semantic_arms(monkeypatch):
    # GO:1 is both the strongest p-value AND the most similar term -- it must
    # not be double-counted, leaving room for a genuine second pick.
    _patch_score_enrichment_table(monkeypatch, {"GO:1": 0.9, "GO:2": 0.1, "GO:3": 0.5})
    tbl = _enrich_tbl(
        [
            ("GO:1", "best of both", 1e-8, "GENE1"),
            ("GO:2", "weak everything", 0.03, "GENE2"),
            ("GO:3", "second best semantic", 0.02, "GENE3"),
        ]
    )

    shortlist = service.prefilter_go_candidates_union(
        "phenotype", tbl, top_n=2, pvalue_k=1
    )

    assert list(shortlist["ID"]) == ["GO:1", "GO:3"]


def test_prefilter_union_caps_at_available_rows():
    result = service.prefilter_go_candidates_union(
        "phenotype", _enrich_tbl([]), top_n=10
    )
    assert result.empty


def test_prefilter_union_returns_input_unchanged_when_empty():
    empty = _enrich_tbl([])
    assert service.prefilter_go_candidates_union("phenotype", empty, top_n=5) is empty


# --- fixture / replay parsing utilities -------------------------------------------


def test_parse_expected_go_ids_dedupes_and_preserves_order(tmp_path):
    path = tmp_path / "gold.md"
    path.write_text("GO:0006954 inflammatory response\nGO:0002250 immune\nGO:0006954 repeat")

    ids = service.parse_expected_go_ids(str(path))

    assert ids == ["GO:0006954", "GO:0002250"]


def test_overlap_at_k_counts_intersection_within_top_k():
    result_ids = ["GO:1", "GO:2", "GO:3", "GO:4"]
    expected_ids = ["GO:2", "GO:4", "GO:9"]
    assert service.overlap_at_k(result_ids, expected_ids, k=2) == 1
    assert service.overlap_at_k(result_ids, expected_ids, k=4) == 2


def test_load_enrich_table_from_csv_requires_expected_columns(tmp_path):
    path = tmp_path / "table.csv"
    pd.DataFrame({"ID": ["GO:1"]}).to_csv(path, index=False)
    with pytest.raises(ValueError, match="missing columns"):
        service.load_enrich_table(str(path))


# --- specificity weighting: stop broad terms winning on size alone ---------------


def test_normalize_specificity_favours_narrow_terms():
    # 20-gene term is far more informative than a 5000-gene one.
    spec = service.normalize_specificity(pd.Series([20, 500, 5000]))
    assert spec.iloc[0] > spec.iloc[1] > spec.iloc[2]
    assert (spec >= 0).all() and (spec <= 1).all()


def test_normalize_specificity_is_independent_of_the_rest_of_the_pool():
    """A term's specificity must not depend on what it is ranked beside.

    Min-maxing over the candidate pool made the narrowest term in any pool
    score 1.0 -- so a 525-gene term looked maximally specific whenever it
    happened to sit among broader ones, and the same term scored differently
    from one run to the next.
    """
    alone = service.normalize_specificity(pd.Series([500]))
    with_narrow = service.normalize_specificity(pd.Series([500, 5, 10]))
    with_broad = service.normalize_specificity(pd.Series([500, 4000, 9000]))

    assert alone.iloc[0] == pytest.approx(with_narrow.iloc[0])
    assert alone.iloc[0] == pytest.approx(with_broad.iloc[0])


def test_normalize_specificity_anchors_to_the_library_size_range(monkeypatch):
    """Endpoints are the library's own widest and narrowest terms.

    Raw information content only spans ~0.21-0.83, too compressed to weigh
    against similarity and significance. Anchoring to the library's size range
    restores the full [0, 1] scale while keeping it fixed across pools.
    """
    monkeypatch.setattr(service, "_SPECIFICITY_SCALE", None)
    monkeypatch.setattr(
        "src.services.enrich.go_corpus_gene_count", lambda *a, **k: 10000
    )
    monkeypatch.setattr(
        "src.services.enrich.go_term_sizes",
        lambda *a, **k: {"GO:wide": 1000, "GO:mid": 100, "GO:narrow": 10},
    )

    spec = service.normalize_specificity(pd.Series([1000, 100, 10]))

    assert spec.iloc[0] == pytest.approx(0.0)   # widest term in the library
    assert spec.iloc[-1] == pytest.approx(1.0)  # narrowest term in the library
    assert 0.0 < spec.iloc[1] < 1.0


def test_normalize_specificity_handles_missing_sizes():
    spec = service.normalize_specificity(pd.Series([None, None]))
    assert (spec == 0.5).all()


def test_hybrid_score_without_term_sizes_keeps_original_two_term_behaviour():
    similarity = pd.Series([1.0, 0.0])
    pvalues = pd.Series([0.5, 0.5])
    assert list(service.hybrid_score(similarity, pvalues, alpha=1.0)) == [1.0, 0.0]


def test_specificity_lifts_narrow_terms_above_broad_ones_ranked_by_pvalue():
    """The DHODH failure in miniature.

    Broad "regulation of transcription"-style terms annotate thousands of genes
    and win on raw p-value in almost any coexpression network, crowding out
    narrower, more mechanistically informative hits. Specificity weighting must
    move the narrow term up relative to pure p-value ordering.
    """
    similarity = pd.Series([0.5, 0.5, 0.5, 0.5])
    pvalues = pd.Series([1e-15, 1e-12, 1e-9, 1e-3])
    term_sizes = pd.Series([5200, 4100, 3800, 22])  # last one is the specific hit

    pvalue_rank = pvalues.rank().iloc[-1]          # 4th of 4 by p-value
    score = service.hybrid_score(similarity, pvalues, term_sizes=term_sizes)
    new_rank = score.rank(ascending=False).iloc[-1]

    # The significance floor already establishes that every candidate here is
    # a real hit, so specificity is weighted to out-argue raw p-value extremity
    # and the narrow term takes the lead outright.
    assert new_rank < pvalue_rank
    assert new_rank == 1


# --- significance floor: the model-independent guarantee ------------------------


def _shortlist(rows):
    """rows: list of (go_id, term, adj_p, genes, score)"""
    return pd.DataFrame(
        [
            {
                "ID": go_id, "Term": term, "Desc": term,
                "Adjusted P-value": adj_p, "Genes": genes, "score": score,
            }
            for go_id, term, adj_p, genes, score in rows
        ]
    )


def test_enforce_floor_swaps_null_pick_for_unused_significant_candidate():
    shortlisted = _shortlist([
        ("GO:1", "real hit", 1e-8, "GENE1;GENE2", 0.9),
        ("GO:2", "also real", 1e-4, "GENE3", 0.8),
        ("GO:3", "statistical noise", 0.68, "GENE4", 0.7),
    ])
    llm_results = [
        {"id": "GO:1", "name": "real hit", "p": 1e-8, "rank": 1, "genes": ["GENE1", "GENE2"]},
        {"id": "GO:3", "name": "statistical noise", "p": 0.68, "rank": 2, "genes": ["GENE4"]},
    ]

    repaired = service.enforce_significance_floor(llm_results, shortlisted, k=2)

    assert [r["id"] for r in repaired] == ["GO:1", "GO:2"]
    assert repaired[1]["floor_substituted"] is True
    assert repaired[1]["p"] == 1e-4
    assert repaired[1]["genes"] == ["GENE3"]
    assert [r["rank"] for r in repaired] == [1, 2]


def test_enforce_floor_keeps_null_pick_when_no_real_candidate_is_left():
    # Gene genuinely has only one significant term -- padding is unavoidable,
    # but it must be marked rather than silently passed off as a real hit.
    shortlisted = _shortlist([
        ("GO:1", "only real hit", 1e-8, "GENE1", 0.9),
        ("GO:3", "noise", 0.68, "GENE4", 0.7),
    ])
    llm_results = [
        {"id": "GO:1", "name": "only real hit", "p": 1e-8, "rank": 1, "genes": ["GENE1"]},
        {"id": "GO:3", "name": "noise", "p": 0.68, "rank": 2, "genes": ["GENE4"]},
    ]

    repaired = service.enforce_significance_floor(llm_results, shortlisted, k=2)

    assert [r["id"] for r in repaired] == ["GO:1", "GO:3"]
    assert repaired[1]["below_significance_floor"] is True
    assert "floor_substituted" not in repaired[1]


def test_enforce_floor_leaves_all_significant_results_untouched():
    shortlisted = _shortlist([
        ("GO:1", "a", 1e-8, "G1", 0.9),
        ("GO:2", "b", 1e-4, "G2", 0.8),
    ])
    llm_results = [
        {"id": "GO:1", "name": "a", "p": 1e-8, "rank": 1, "genes": ["G1"]},
        {"id": "GO:2", "name": "b", "p": 1e-4, "rank": 2, "genes": ["G2"]},
    ]

    repaired = service.enforce_significance_floor(llm_results, shortlisted, k=2)

    assert repaired == llm_results
    assert not any("floor_substituted" in r for r in repaired)


def test_enforce_floor_respects_configurable_threshold(monkeypatch):
    monkeypatch.setenv("GO_SIGNIFICANCE_MAX_P", "0.001")
    shortlisted = _shortlist([
        ("GO:1", "very strong", 1e-8, "G1", 0.9),
        ("GO:2", "strong enough at 0.05 but not 0.001", 1e-2, "G2", 0.8),
    ])
    llm_results = [{"id": "GO:2", "name": "x", "p": 1e-2, "rank": 1, "genes": ["G2"]}]

    repaired = service.enforce_significance_floor(llm_results, shortlisted, k=1)

    # Under the stricter floor, p=1e-2 is no longer acceptable and gets swapped.
    assert repaired[0]["id"] == "GO:1"


def test_prefilter_excludes_sub_floor_terms_when_real_hits_exist(monkeypatch):
    _patch_score_enrichment_table(
        monkeypatch, {"GO:1": 0.1, "GO:2": 0.1, "GO:noise": 0.99}
    )
    tbl = _enrich_tbl([
        ("GO:1", "real hit", 1e-8, "G1"),
        ("GO:2", "real hit two", 1e-3, "G2"),
        ("GO:noise", "semantically perfect but null", 0.8, "G3"),
    ])

    shortlist = service.prefilter_go_candidates_union("phenotype", tbl, top_n=2, pvalue_k=1)

    # The null term must not make the shortlist while real hits are available,
    # no matter how well it embeds against the phenotype text.
    assert "GO:noise" not in set(shortlist["ID"])


def test_prefilter_does_not_pad_shortlist_with_sub_floor_terms(monkeypatch):
    """top_n is a cap, not a quota.

    A gene with two real hits must yield a shortlist of two, not two hits
    padded with statistical noise. Padding is actively harmful: a narrow but
    insignificant term outranks a broad but real one on specificity, so the
    ranker is handed noise that reads as a more satisfying answer.
    """
    _patch_score_enrichment_table(
        monkeypatch, {"GO:1": 0.1, "GO:2": 0.2, "GO:3": 0.99, "GO:4": 0.98}
    )
    tbl = _enrich_tbl(
        [
            ("GO:1", "real hit", 1e-8, "GENE1"),
            ("GO:2", "real hit", 0.001, "GENE2"),
            ("GO:3", "noise, semantically irresistible", 0.44, "GENE3"),
            ("GO:4", "noise, semantically irresistible", 0.51, "GENE4"),
        ]
    )

    shortlist = service.prefilter_go_candidates_union(
        "phenotype", tbl, top_n=50, pvalue_k=1
    )

    assert set(shortlist["ID"]) == {"GO:1", "GO:2"}


def test_prefilter_falls_back_to_full_pool_when_nothing_is_significant(monkeypatch):
    """With no real hits at all, returning nothing would be worse than noise."""
    _patch_score_enrichment_table(monkeypatch, {"GO:3": 0.9, "GO:4": 0.1})
    tbl = _enrich_tbl(
        [
            ("GO:3", "noise", 0.44, "GENE3"),
            ("GO:4", "noise", 0.51, "GENE4"),
        ]
    )

    shortlist = service.prefilter_go_candidates_union(
        "phenotype", tbl, top_n=50, pvalue_k=1
    )

    assert set(shortlist["ID"]) == {"GO:3", "GO:4"}


# --- specificity quota: the model-independent ordering guarantee -----------------


def _quota_shortlist():
    """Four candidates, all significant; the two highest-scoring are specific."""
    return pd.DataFrame(
        [
            {"ID": "GO:spec1", "Term": "ER-associated degradation", "Desc": "d",
             "Adjusted P-value": 0.02, "Genes": "A;B", "score": 0.95},
            {"ID": "GO:spec2", "Term": "miRNA-mediated silencing", "Desc": "d",
             "Adjusted P-value": 0.001, "Genes": "C", "score": 0.90},
            {"ID": "GO:broad1", "Term": "regulation of gene expression", "Desc": "d",
             "Adjusted P-value": 1e-9, "Genes": "D", "score": 0.30},
            {"ID": "GO:broad2", "Term": "regulation of transcription", "Desc": "d",
             "Adjusted P-value": 1e-10, "Genes": "E", "score": 0.20},
        ]
    )


def test_specificity_quota_promotes_top_scored_terms_the_model_skipped(monkeypatch):
    monkeypatch.setenv("GO_SPECIFICITY_QUOTA_FRACTION", "0.5")
    results = [
        {"id": "GO:broad1", "name": "regulation of gene expression", "p": 1e-9},
        {"id": "GO:broad2", "name": "regulation of transcription", "p": 1e-10},
    ]

    out = service.enforce_specificity_quota(results, _quota_shortlist(), k=2)

    # quota = floor(2 * 0.5) = 1, so the single best-scoring term must appear.
    assert "GO:spec1" in {e["id"] for e in out}
    assert len(out) == 2
    assert next(e for e in out if e["id"] == "GO:spec1")["quota_promoted"] is True


def test_specificity_quota_drops_the_models_worst_pick_not_its_best(monkeypatch):
    monkeypatch.setenv("GO_SPECIFICITY_QUOTA_FRACTION", "0.5")
    results = [
        {"id": "GO:broad1", "name": "keeps: better score", "p": 1e-9},
        {"id": "GO:broad2", "name": "dropped: worst score", "p": 1e-10},
    ]

    out = service.enforce_specificity_quota(results, _quota_shortlist(), k=2)

    ids = {e["id"] for e in out}
    assert "GO:broad1" in ids
    assert "GO:broad2" not in ids


def test_specificity_quota_is_a_noop_when_model_already_picked_top_terms(monkeypatch):
    monkeypatch.setenv("GO_SPECIFICITY_QUOTA_FRACTION", "0.5")
    results = [
        {"id": "GO:spec1", "name": "already top", "p": 0.02},
        {"id": "GO:broad1", "name": "broad", "p": 1e-9},
    ]

    out = service.enforce_specificity_quota(results, _quota_shortlist(), k=2)

    assert out == results


def test_specificity_quota_disabled_by_zero_fraction(monkeypatch):
    monkeypatch.setenv("GO_SPECIFICITY_QUOTA_FRACTION", "0")
    results = [{"id": "GO:broad1", "name": "broad", "p": 1e-9}]

    assert service.enforce_specificity_quota(results, _quota_shortlist(), k=10) == results


def test_specificity_quota_orders_final_results_by_score(monkeypatch):
    monkeypatch.setenv("GO_SPECIFICITY_QUOTA_FRACTION", "0.5")
    results = [
        {"id": "GO:broad1", "name": "broad", "p": 1e-9},
        {"id": "GO:broad2", "name": "broader", "p": 1e-10},
    ]

    out = service.enforce_specificity_quota(results, _quota_shortlist(), k=2)

    assert [e["id"] for e in out] == ["GO:spec1", "GO:broad1"]
    assert [e["rank"] for e in out] == [1, 2]


# --- composed guarantees: the property that actually ships ----------------------


def test_significance_floor_does_not_mutate_the_callers_results():
    """The enforce_* helpers are called on the model's own list; stamping rank
    onto those dicts in place corrupts the caller's copy."""
    shortlist = _quota_shortlist()
    results = [{"id": "GO:broad1", "name": "broad", "p": 1e-9}]

    service.enforce_significance_floor(results, shortlist, k=1)
    service.enforce_specificity_quota(results, shortlist, k=1)

    assert results == [{"id": "GO:broad1", "name": "broad", "p": 1e-9}]


def test_full_chain_returns_only_real_hits_and_honours_the_quota(monkeypatch):
    """End-to-end property, over a pool that is mostly noise.

    Unit tests cover each guarantee alone; this pins the composition, which is
    what production actually runs: 4 real hits buried in 40 null ones, a ranker
    that picks badly, and an output that must still be all-real and contain the
    top-scored candidates.
    """
    monkeypatch.setenv("GO_SPECIFICITY_QUOTA_FRACTION", "0.5")
    real = [
        ("GO:r1", "narrow real", 0.001, "A;B"),
        ("GO:r2", "narrow real", 0.004, "C"),
        ("GO:r3", "broad real", 1e-12, "D"),
        ("GO:r4", "broad real", 1e-10, "E"),
    ]
    noise = [(f"GO:n{i}", "null but narrow", 0.4 + i / 1000, "Z") for i in range(40)]
    sims = {go_id: 0.9 for go_id, *_ in noise}
    sims.update({"GO:r1": 0.8, "GO:r2": 0.7, "GO:r3": 0.1, "GO:r4": 0.1})
    _patch_score_enrichment_table(monkeypatch, sims)

    shortlist = service.prefilter_go_candidates_union(
        "phenotype", _enrich_tbl(real + noise), top_n=20, pvalue_k=2
    )
    assert set(shortlist["ID"]) == {"GO:r1", "GO:r2", "GO:r3", "GO:r4"}

    # a ranker that returns the two broad terms plus two sub-floor inventions
    picks = [
        {"id": "GO:r3", "name": "broad real", "p": 1e-12},
        {"id": "GO:r4", "name": "broad real", "p": 1e-10},
        {"id": "GO:n0", "name": "null but narrow", "p": 0.4},
        {"id": "GO:n1", "name": "null but narrow", "p": 0.401},
    ]
    out = service.enforce_specificity_quota(
        service.enforce_significance_floor(picks, shortlist, k=4), shortlist, k=4
    )

    assert all(entry["p"] < 0.05 for entry in out), "a sub-floor term survived"
    top_two = set(shortlist.sort_values("score", ascending=False).head(2)["ID"])
    assert top_two <= {str(e["id"]) for e in out}, "quota did not seat the top-scored"
    assert [e["rank"] for e in out] == [1, 2, 3, 4]


def test_significance_floor_is_shared_with_the_enrichment_filter(monkeypatch):
    """One env var must move both, or the shortlist filter and the floor drift."""
    from src.services import enrich as enrich_service

    monkeypatch.setenv("GO_SIGNIFICANCE_MAX_P", "0.01")
    assert service.significance_floor() == 0.01
    assert enrich_service.significance_floor() == 0.01


# --- degraded ranking: the fallback must keep the guarantees --------------------


def _noisy_pool():
    """4 real hits buried in 40 null ones whose names scream the phenotype."""
    real = [
        ("GO:r1", "narrow real", 0.001, "A;B"),
        ("GO:r2", "narrow real", 0.004, "C"),
        ("GO:r3", "broad real", 1e-12, "D"),
        ("GO:r4", "broad real", 1e-10, "E"),
    ]
    noise = [
        (f"GO:n{i}", "tumor necrosis factor signalling", 0.3 + i / 100, "Z")
        for i in range(40)
    ]
    return _enrich_tbl(real + noise)


def test_degraded_ranking_never_returns_sub_floor_terms(monkeypatch):
    """The defect this replaced: the old fallback ranked the FULL pool by
    embedding similarity, no floor and no quota. A live UC run returned ten
    terms with adjusted p from 0.28 to 0.88 -- TNF and NF-kB signalling, which
    read as perfect disease biology and had no support at all."""
    sims = {f"GO:n{i}": 0.99 for i in range(40)}
    sims.update({"GO:r1": 0.1, "GO:r2": 0.1, "GO:r3": 0.05, "GO:r4": 0.05})
    _patch_score_enrichment_table(monkeypatch, sims)

    out = service.rank_go_terms_degraded("phenotype", _noisy_pool(), k=10)

    assert out, "degraded ranking returned nothing"
    assert all(e["p"] < 0.05 for e in out), "a statistically null term survived"
    assert all(e["ranking_degraded"] for e in out)


def test_degraded_ranking_works_when_embeddings_are_down_too(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("embedding service unreachable")

    monkeypatch.setattr(service, "_score_enrichment_table", boom)

    out = service.rank_go_terms_degraded("phenotype", _noisy_pool(), k=5)

    assert out
    assert all(e["p"] < 0.05 for e in out)


def test_degraded_ranking_returns_empty_for_empty_input():
    assert service.rank_go_terms_degraded("phenotype", _enrich_tbl([]), k=5) == []


def test_fallback_backend_is_off_unless_configured(monkeypatch):
    monkeypatch.delenv("GO_LLM_FALLBACK_BACKEND", raising=False)
    assert service.go_llm_fallback_backend() is None
    monkeypatch.setenv("GO_LLM_FALLBACK_BACKEND", "openai")
    assert service.go_llm_fallback_backend() == "openai"


def test_resilient_uses_normal_ranking_when_it_works(monkeypatch):
    ok = MagicMock(return_value=[{"id": "GO:1", "name": "fine", "p": 0.01}])
    monkeypatch.setattr(service, "rank_go_terms_by_strategy", ok)

    out = service.rank_go_terms_resilient("phenotype", _noisy_pool(), k=5)

    assert out == [{"id": "GO:1", "name": "fine", "p": 0.01}]
    ok.assert_called_once()


def test_resilient_degrades_when_no_fallback_backend_is_set(monkeypatch):
    monkeypatch.delenv("GO_LLM_FALLBACK_BACKEND", raising=False)
    monkeypatch.setattr(
        service, "rank_go_terms_by_strategy",
        MagicMock(side_effect=RuntimeError("endpoint unreachable")),
    )
    degraded = MagicMock(return_value=[{"id": "GO:x", "ranking_degraded": True}])
    monkeypatch.setattr(service, "rank_go_terms_degraded", degraded)

    out = service.rank_go_terms_resilient("phenotype", _noisy_pool(), k=5)

    assert out[0]["ranking_degraded"] is True
    degraded.assert_called_once()


def test_resilient_tries_the_opt_in_backend_before_degrading(monkeypatch):
    monkeypatch.setenv("GO_LLM_FALLBACK_BACKEND", "openai")
    calls = []

    def ranker(phenotype, tbl, **kw):
        calls.append(kw.get("strategy"))
        if len(calls) == 1:
            raise RuntimeError("local endpoint unreachable")
        return [{"id": "GO:via-openai", "p": 0.01}]

    monkeypatch.setattr(service, "rank_go_terms_by_strategy", ranker)
    degraded = MagicMock()
    monkeypatch.setattr(service, "rank_go_terms_degraded", degraded)

    out = service.rank_go_terms_resilient(
        "phenotype", _noisy_pool(), k=5, strategy="llm"
    )

    assert out == [{"id": "GO:via-openai", "p": 0.01}]
    assert calls[1] == GoSemanticStrategy.HYBRID_OPENAI
    degraded.assert_not_called()


def test_unknown_size_is_scored_as_broad_not_average(monkeypatch):
    """GO:0006357 is scored by Enrichr but absent from every published
    library. Treating unknown as average handed it a reserved slot in both
    recorded cases; it must be treated as broad instead."""
    monkeypatch.setattr(service, "_SPECIFICITY_SCALE", None)
    monkeypatch.setattr(
        "src.services.enrich.go_corpus_gene_count", lambda *a, **k: 10000
    )
    monkeypatch.setattr(
        "src.services.enrich.go_term_sizes",
        lambda *a, **k: {"GO:wide": 1000, "GO:narrow": 10},
    )

    spec = service.normalize_specificity(pd.Series([10, None, 1000]))

    assert spec.iloc[0] == pytest.approx(1.0)   # narrowest
    assert spec.iloc[1] == pytest.approx(0.0)   # unknown -> broad, not 0.5
    assert spec.iloc[2] == pytest.approx(0.0)   # broadest


def test_all_sizes_unknown_stays_neutral():
    """With nothing measurable at all, flat 0.5 is right -- demoting every
    term equally is the same ranking, but 0.0 would distort the blend."""
    assert (service.normalize_specificity(pd.Series([None, None])) == 0.5).all()


def test_specificity_quota_is_off_by_default(monkeypatch):
    """Default off; promoting on score alone seats redundant sibling terms."""
    monkeypatch.delenv("GO_SPECIFICITY_QUOTA_FRACTION", raising=False)
    assert service.specificity_quota(10) == 0

    results = [{"id": "GO:broad1", "name": "broad", "p": 1e-9}]
    assert service.enforce_specificity_quota(results, _quota_shortlist(), k=10) == results

    monkeypatch.setenv("GO_SPECIFICITY_QUOTA_FRACTION", "0.4")
    assert service.specificity_quota(10) == 4


def test_embed_texts_batches_above_the_api_limit(monkeypatch):
    """The API rejects arrays longer than 2048; the pool can exceed that."""
    calls = []

    class _Resp:
        def __init__(self, n):
            self.data = [MagicMock(embedding=[0.0]) for _ in range(n)]

    class _Client:
        def __init__(self):
            self.embeddings = self

        def create(self, input, model):
            calls.append(len(input))
            return _Resp(len(input))

    monkeypatch.setattr(service.openai, "Client", lambda *a, **k: _Client())

    out = service._embed_texts([f"t{i}" for i in range(2591)], "m")

    assert len(out) == 2591
    assert max(calls) <= 2048, f"a batch exceeded the API limit: {calls}"
    assert sum(calls) == 2591
