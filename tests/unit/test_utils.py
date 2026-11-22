import json

import numpy as np
import pandas as pd

from src.utils import transform_credible_sets_to_locuszoom


def test_locuszoom_transform_maps_missing_allele_freq_to_none():
    df = pd.DataFrame({
        "BETA": [0.1, 0.2],
        "CHR": [1, 1],
        "P": [1e-9, 1e-8],
        "BP": [100, 200],
        "A1": ["A", "C"],
        "A2": ["G", "T"],
        "FRQ": [0.3, np.nan],
        "PIP": [0.9, 0.1],
        "cs": [1, 1],
    })

    result = transform_credible_sets_to_locuszoom(df)

    assert result["data"]["ref_allele_freq"] == [0.3, None]
    json.dumps(result, allow_nan=False)
