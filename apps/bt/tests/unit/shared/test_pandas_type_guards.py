from collections.abc import Hashable

import pandas as pd
import pytest

from src.shared.utils.pandas_type_guards import (
    ensure_str_key,
    finite_float_or_none,
    int_or_none,
    is_missing_scalar,
    normalize_bool_frame,
    normalize_bool_series,
    records_with_str_keys,
    numeric_series_or_empty,
    str_or_none,
)


def test_is_missing_scalar_treats_pandas_na_and_nan_as_missing() -> None:
    assert is_missing_scalar(pd.NA)
    assert is_missing_scalar(float("nan"))
    assert is_missing_scalar(None)
    assert not is_missing_scalar("7203")
    assert not is_missing_scalar(0)


def test_scalar_extractors_return_none_for_missing_values() -> None:
    assert str_or_none(pd.NA) is None
    assert int_or_none(pd.NA) is None
    assert finite_float_or_none(pd.NA) is None
    assert finite_float_or_none(float("inf")) is None


def test_scalar_extractors_normalize_present_values() -> None:
    assert str_or_none(7203) == "7203"
    assert int_or_none("42") == 42
    assert finite_float_or_none("12.5") == 12.5


def test_ensure_str_key_rejects_missing_pandas_keys() -> None:
    assert ensure_str_key("sector") == "sector"
    assert ensure_str_key(2025) == "2025"

    with pytest.raises(ValueError, match="missing pandas key"):
        ensure_str_key(pd.NA)


def test_records_with_str_keys_normalizes_dataframe_record_keys() -> None:
    raw_records: list[dict[Hashable, object]] = [
        {"code": 7203, "event_count": 2},
        {2025: "calendar_year", "mean_return": 0.12},
    ]

    assert records_with_str_keys(raw_records) == [
        {"code": 7203, "event_count": 2},
        {"2025": "calendar_year", "mean_return": 0.12},
    ]


def test_numeric_series_or_empty_coerces_iterables_and_drops_missing_values() -> None:
    series = numeric_series_or_empty(["1.0", pd.NA, "bad", 2])

    assert series.tolist() == [1.0, 2.0]
    assert numeric_series_or_empty(None).empty


def test_numeric_series_or_empty_preserves_series_index_and_returns_float_values() -> None:
    series = numeric_series_or_empty(
        pd.Series(["1.0", pd.NA, "bad", 2], index=["a", "b", "c", "d"])
    )

    assert series.tolist() == [1.0, 2.0]
    assert series.index.tolist() == ["a", "d"]
    assert str(series.dtype) == "float64"


def test_normalize_bool_series_coerces_missing_values_without_changing_index() -> None:
    series = normalize_bool_series(pd.Series([True, pd.NA, 1, 0], index=["a", "b", "c", "d"]))

    assert series.tolist() == [True, False, True, False]
    assert series.index.tolist() == ["a", "b", "c", "d"]
    assert str(series.dtype) == "bool"


def test_normalize_bool_frame_coerces_missing_values_without_changing_shape() -> None:
    frame = normalize_bool_frame(
        pd.DataFrame(
            {
                "a": [True, pd.NA],
                "b": [0, 1],
            },
            index=["x", "y"],
        )
    )

    assert frame.to_dict(orient="list") == {"a": [True, False], "b": [False, True]}
    assert frame.index.tolist() == ["x", "y"]
    assert list(frame.columns) == ["a", "b"]
