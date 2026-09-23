import numpy as np
import pandas as pd
import pytest

from credit_pipeline.db import pg_type, to_python


@pytest.mark.parametrize(
    ("series", "expected"),
    [
        (pd.Series([True]), "BOOLEAN"),
        (pd.Series([1], dtype="Int32"), "BIGINT"),
        (pd.Series([1.5]), "DOUBLE PRECISION"),
        (pd.Series(pd.to_datetime(["2025-01-01"])), "TIMESTAMP"),
        (pd.Series(["SP"]), "TEXT"),
    ],
)
def test_pg_type(series, expected):
    assert pg_type(series.dtype) == expected


def test_to_python_converte_nulos_e_tipos_numpy():
    assert to_python(np.nan) is None
    assert to_python(pd.NA) is None
    assert to_python(pd.NaT) is None
    assert to_python(np.int64(3)) == 3 and type(to_python(np.int64(3))) is int
    assert to_python(pd.Timestamp("2025-01-01")).year == 2025
