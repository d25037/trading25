from __future__ import annotations

import math
from collections.abc import Hashable, Iterable, Mapping
from typing import Any, SupportsFloat, SupportsIndex, SupportsInt

import numpy as np
import pandas as pd


def is_missing_scalar(value: object) -> bool:
    """Return True when a scalar-like pandas value is missing.

    `pd.isna` can also return arrays for array-like inputs. These helpers are
    for row/group scalar extraction, so array-like results are treated as
    present and should be handled by the caller.
    """

    if value is None or value is pd.NA or value is pd.NaT:
        return True
    if isinstance(value, float):
        return math.isnan(value)
    if isinstance(value, np.floating):
        return bool(np.isnan(value))
    return False


def str_or_none(value: object) -> str | None:
    if is_missing_scalar(value):
        return None
    return str(value)


def required_str(value: object, *, field: str) -> str:
    normalized = str_or_none(value)
    if normalized is None:
        raise ValueError(f"missing pandas scalar: {field}")
    return normalized


def int_or_none(value: object) -> int | None:
    if is_missing_scalar(value):
        return None
    if isinstance(value, str | bytes | bytearray | SupportsInt | SupportsIndex):
        return int(value)
    if isinstance(value, SupportsFloat):
        return int(float(value))
    raise TypeError(f"cannot normalize pandas scalar to int: {type(value).__name__}")


def required_int(value: object, *, field: str) -> int:
    normalized = int_or_none(value)
    if normalized is None:
        raise ValueError(f"missing pandas scalar: {field}")
    return normalized


def float_or_none(value: object) -> float | None:
    if is_missing_scalar(value):
        return None
    if isinstance(value, str | bytes | bytearray | SupportsFloat | SupportsIndex):
        return float(value)
    if isinstance(value, SupportsInt):
        return float(int(value))
    raise TypeError(f"cannot normalize pandas scalar to float: {type(value).__name__}")


def finite_float_or_none(value: object) -> float | None:
    normalized = float_or_none(value)
    if normalized is None or not math.isfinite(normalized):
        return None
    return normalized


def required_float(value: object, *, field: str) -> float:
    normalized = float_or_none(value)
    if normalized is None:
        raise ValueError(f"missing pandas scalar: {field}")
    return normalized


def ensure_str_key(key: Hashable, *, field: str = "pandas key") -> str:
    if is_missing_scalar(key):
        raise ValueError(f"missing pandas key: {field}")
    return str(key)


def record_with_str_keys(record: Mapping[Hashable, Any]) -> dict[str, Any]:
    return {ensure_str_key(key): value for key, value in record.items()}


def records_with_str_keys(records: Iterable[Mapping[Hashable, Any]]) -> list[dict[str, Any]]:
    return [record_with_str_keys(record) for record in records]


def normalize_bool_series(series: pd.Series) -> pd.Series:
    """Coerce boolean-like Series with NA to bool without silent downcasting warnings."""
    with pd.option_context("future.no_silent_downcasting", True):
        return series.fillna(False).infer_objects(copy=False).astype(bool)


def normalize_bool_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Coerce boolean-like DataFrame with NA to bool without silent downcasting warnings."""
    with pd.option_context("future.no_silent_downcasting", True):
        return frame.fillna(False).infer_objects(copy=False).astype(bool)


def numeric_series_or_empty(values: Iterable[Any] | pd.Series | None) -> pd.Series:
    if values is None:
        return pd.Series(dtype="float64")
    series = values if isinstance(values, pd.Series) else pd.Series(list(values))
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return pd.Series(dtype="float64")
    return numeric.astype(float)
