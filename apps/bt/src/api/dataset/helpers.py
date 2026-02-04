"""Helper functions for DataFrame conversion in API responses."""

from typing import Any, Sequence

import pandas as pd

# APIレスポンスの型（リストまたは辞書）
APIResponse = dict[str, Any] | list[dict[str, Any]]


def build_date_params(
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, str]:
    """Build date filter parameters for API requests."""
    params: dict[str, str] = {}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    return params


def convert_ohlcv_response(
    data: APIResponse | None,
    columns: Sequence[str] = ("Open", "High", "Low", "Close", "Volume"),
) -> pd.DataFrame:
    """Convert API OHLCV response to DataFrame with DatetimeIndex.

    Args:
        data: API response data (list of records)
        columns: Column names to assign

    Returns:
        DataFrame with DatetimeIndex and specified columns
    """
    if not data or not isinstance(data, list):
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df.columns = pd.Index(columns)
    return df


def convert_index_response(data: APIResponse | None) -> pd.DataFrame:
    """Convert API index response to DataFrame with DatetimeIndex."""
    return convert_ohlcv_response(data, columns=("Open", "High", "Low", "Close"))


def convert_dated_response(
    data: APIResponse | None,
    date_column: str = "date",
) -> pd.DataFrame:
    """Convert API response with date column to DataFrame with DatetimeIndex.

    Args:
        data: API response data
        date_column: Name of the date column

    Returns:
        DataFrame with DatetimeIndex
    """
    if not data or not isinstance(data, list):
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df[date_column] = pd.to_datetime(df[date_column])
    df.set_index(date_column, inplace=True)
    return df
