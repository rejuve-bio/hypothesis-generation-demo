"""Unit tests for src.services.prolog.PrologQuery's HTTP-facing methods."""

from unittest.mock import MagicMock

import pytest
import requests

from src.services.prolog import PrologQuery, PrologServiceError


class _FakeResponse:
    """Minimal stand-in for a requests.Response used by PrologQuery."""

    def __init__(self, *, ok=True, status_code=200, json_data=None, json_error=None, text=""):
        self.ok = ok
        self.status_code = status_code
        self.text = text
        self._json_data = json_data
        self._json_error = json_error

    def json(self):
        if self._json_error is not None:
            raise self._json_error
        return self._json_data


@pytest.fixture
def prolog():
    return PrologQuery(host="prolog-service", port=4242)


def test_get_candidate_genes_happy_path_returns_bare_ensembl_ids(monkeypatch, prolog):
    response = _FakeResponse(
        json_data={"candidate_genes": ["ensg00000140968", "ensg00000125347"]}
    )
    monkeypatch.setattr("src.services.prolog.requests.get", MagicMock(return_value=response))

    result = prolog.get_candidate_genes("rs16940186")

    assert result == ["ENSG00000140968", "ENSG00000125347"]


def test_get_candidate_genes_non_2xx_raises_service_error_with_response_text(monkeypatch, prolog):
    response = _FakeResponse(ok=False, status_code=500, text="Internal Prolog error")
    monkeypatch.setattr("src.services.prolog.requests.get", MagicMock(return_value=response))

    with pytest.raises(PrologServiceError, match="Internal Prolog error"):
        prolog.get_candidate_genes("rs16940186")


def test_get_candidate_genes_invalid_json_raises_service_error(monkeypatch, prolog):
    json_error = requests.exceptions.JSONDecodeError("Expecting value", "", 0)
    response = _FakeResponse(
        ok=True, json_error=json_error, text="<html>not json</html>"
    )
    monkeypatch.setattr("src.services.prolog.requests.get", MagicMock(return_value=response))

    with pytest.raises(PrologServiceError, match="Invalid JSON response"):
        prolog.get_candidate_genes("rs16940186")


def test_get_relevant_gene_proof_connection_failure_raises_service_error(monkeypatch, prolog):
    """A downed/unreachable Prolog server (connection refused, DNS failure,
    timeout, ...) must surface as PrologServiceError, not a raw
    requests.exceptions.ConnectionError leaking out of the service layer."""
    monkeypatch.setattr(
        "src.services.prolog.requests.get",
        MagicMock(side_effect=requests.exceptions.ConnectionError("Connection refused")),
    )

    with pytest.raises(PrologServiceError, match="unreachable"):
        prolog.get_relevant_gene_proof("rs16940186", seed=1, samples=10)


def test_execute_query_prolog_side_error_raises_distinctly_from_transport_failure(
    monkeypatch, prolog
):
    response = _FakeResponse(ok=True, json_data={"error": "some prolog error"})
    monkeypatch.setattr("src.services.prolog.requests.get", MagicMock(return_value=response))

    with pytest.raises(PrologServiceError, match="Prolog query failed: some prolog error"):
        prolog.execute_query("gene_id('IRF8', X)")


def test_get_gene_ids_passes_through_execute_query_result_unwrapped(monkeypatch, prolog):
    # NOTE: verified against the current source (src/services/prolog.py) --
    # get_gene_ids does NOT strip a `gene(...)` wrapper. It appends
    # execute_query's result[0] verbatim. This test documents that actual
    # behavior rather than the wrapper-stripping behavior an earlier task
    # description assumed.
    monkeypatch.setattr(
        prolog, "execute_query", MagicMock(return_value=["gene(ENSG00000140968)"])
    )

    result = prolog.get_gene_ids(["IRF8"])

    assert result == ["gene(ENSG00000140968)"]


def test_get_gene_ids_falls_back_to_gene_name_on_empty_result(monkeypatch, prolog):
    monkeypatch.setattr(prolog, "execute_query", MagicMock(return_value=[]))

    result = prolog.get_gene_ids(["IRF8"])

    assert result == ["IRF8"]
