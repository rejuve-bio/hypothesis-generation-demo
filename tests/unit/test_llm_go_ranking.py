import json
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.services import llm as service


def _llm():
    return object.__new__(service.LLM)


def _candidates():
    return pd.DataFrame(
        [
            {"ID": "GO:1", "Term": "inflammatory response", "Desc": "desc1", "Adjusted P-value": 0.001, "Genes": "GENE1;GENE2"},
            {"ID": "GO:2", "Term": "cell cycle", "Desc": "desc2", "Adjusted P-value": 0.2, "Genes": "GENE3"},
        ]
    )


def _chat_response(content: str):
    message = MagicMock(content=content)
    choice = MagicMock(message=message)
    return MagicMock(choices=[choice])


# --- _make_go_llm_client: the hardcoded-IP fix -----------------------------------


def test_make_go_llm_client_raises_clearly_when_url_unset(monkeypatch):
    monkeypatch.delenv("GO_LLM_URL", raising=False)
    with pytest.raises(RuntimeError, match="GO_LLM_URL is not set"):
        service._make_go_llm_client()


def test_make_go_llm_client_normalizes_base_url_with_and_without_v1(monkeypatch):
    monkeypatch.setenv("GO_LLM_URL", "http://example.local:8001")
    client = service._make_go_llm_client()
    assert client.base_url == "http://example.local:8001/v1/"

    monkeypatch.setenv("GO_LLM_URL", "http://example.local:8001/v1")
    client = service._make_go_llm_client()
    assert client.base_url == "http://example.local:8001/v1/"


# --- JSON fence stripping ---------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ('{"terms": []}', '{"terms": []}'),
        ('```json\n{"terms": []}\n```', '{"terms": []}'),
        ('```\n{"terms": []}\n```', '{"terms": []}'),
    ],
)
def test_strip_json_fence(raw, expected):
    assert service._strip_json_fence(raw) == expected


# --- candidate cap resolution ------------------------------------------------------


def test_resolve_llm_candidate_cap_caps_to_pool_size():
    assert service._resolve_llm_candidate_cap(max_candidates=0, pool_size=10) <= 10


def test_resolve_llm_candidate_cap_respects_explicit_max_candidates():
    assert service._resolve_llm_candidate_cap(max_candidates=5, pool_size=100) == 5


# --- single-call ranking: retry-on-malformed-JSON, unknown-term skip -------------


def test_rank_relevant_go_by_llm_once_retries_once_on_malformed_json():
    llm = _llm()
    client = MagicMock()
    client.chat.completions.create.side_effect = [
        _chat_response("not json"),
        _chat_response(json.dumps({"terms": [{"rank": 1, "go_id": "GO:1", "name": "inflammatory response", "reason": "ok"}]})),
    ]

    results = llm._rank_relevant_go_by_llm_once(
        "phenotype", _candidates(), k=1, causal_gene=None,
        llm_model="gemma4", client=client, llm_backend="local",
    )

    assert client.chat.completions.create.call_count == 2
    assert len(results) == 1
    assert results[0]["id"] == "GO:1"
    assert results[0]["genes"] == ["GENE1", "GENE2"]
    assert results[0]["p"] == 0.001


def test_rank_relevant_go_by_llm_once_skips_unknown_go_id_without_crashing():
    llm = _llm()
    client = MagicMock()
    client.chat.completions.create.return_value = _chat_response(
        json.dumps(
            {
                "terms": [
                    {"rank": 1, "go_id": "GO:999", "name": "made up term", "reason": "hallucinated"},
                    {"rank": 2, "go_id": "GO:2", "name": "cell cycle", "reason": "ok"},
                ]
            }
        )
    )

    results = llm._rank_relevant_go_by_llm_once(
        "phenotype", _candidates(), k=2, causal_gene=None,
        llm_model="gemma4", client=client, llm_backend="local",
    )

    assert [r["id"] for r in results] == ["GO:2"]


def test_rank_relevant_go_by_llm_once_wraps_client_errors():
    llm = _llm()
    client = MagicMock()
    client.chat.completions.create.side_effect = ConnectionError("host unreachable")

    with pytest.raises(RuntimeError, match="GO term ranking service error"):
        llm._rank_relevant_go_by_llm_once(
            "phenotype", _candidates(), k=1, causal_gene=None,
            llm_model="gemma4", client=client, llm_backend="local",
        )


# --- term size in the prompt: the model cannot prefer specific terms blind -------


def _sized_candidates():
    return pd.DataFrame(
        [
            {"ID": "GO:1", "Term": "ER-associated degradation", "Desc": "d",
             "Adjusted P-value": 0.02, "Genes": "GENE1", "Term Size": 8},
            {"ID": "GO:2", "Term": "regulation of gene expression", "Desc": "d",
             "Adjusted P-value": 1e-9, "Genes": "GENE2", "Term Size": 1127},
        ]
    )


def _sent_user_prompt(client):
    messages = client.chat.completions.create.call_args.kwargs["messages"]
    return next(m["content"] for m in messages if m["role"] == "user")


def test_candidate_lines_state_each_terms_gene_count():
    llm = _llm()
    client = MagicMock()
    client.chat.completions.create.return_value = _chat_response(
        json.dumps({"terms": [{"rank": 1, "go_id": "GO:1", "name": "x", "reason": "ok"}]})
    )

    llm._rank_relevant_go_by_llm_once(
        "phenotype", _sized_candidates(), k=1, causal_gene=None,
        llm_model="gemma4", client=client, llm_backend="local",
    )

    prompt = _sent_user_prompt(client)
    assert "annotates 8 genes" in prompt
    assert "annotates 1127 genes" in prompt


def test_candidate_lines_omit_gene_count_when_unavailable():
    """Term Size is absent when Enrichr's library lookup failed; no crash, no noise."""
    llm = _llm()
    client = MagicMock()
    client.chat.completions.create.return_value = _chat_response(
        json.dumps({"terms": [{"rank": 1, "go_id": "GO:1", "name": "x", "reason": "ok"}]})
    )

    llm._rank_relevant_go_by_llm_once(
        "phenotype", _candidates(), k=1, causal_gene=None,
        llm_model="gemma4", client=client, llm_backend="local",
    )

    assert "annotates" not in _sent_user_prompt(client)


def test_system_prompt_states_the_specificity_rule():
    prompt = service._go_llm_system_prompt(10)
    assert "fewer than 100 genes" in prompt
    assert "more than 500 genes" in prompt
