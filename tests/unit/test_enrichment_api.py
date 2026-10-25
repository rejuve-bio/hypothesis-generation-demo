import pytest
from fastapi import HTTPException

from src.api.routes.enrichment import _p_value_threshold


def test_p_value_threshold_defaults_and_accepts_valid_values():
    assert _p_value_threshold(None) == 0.05
    assert _p_value_threshold("0.1") == 0.1
    assert _p_value_threshold(1) == 1.0


@pytest.mark.parametrize("value", [0, -0.1, 1.5, "abc", float("nan")])
def test_p_value_threshold_rejects_out_of_range_values(value):
    with pytest.raises(HTTPException) as exc:
        _p_value_threshold(value)
    assert exc.value.status_code == 400
