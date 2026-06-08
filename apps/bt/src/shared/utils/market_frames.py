"""Build dated market DataFrames from row-like records."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

import pandas as pd


class RowLike(Protocol):
    def __getitem__(self, key: str, /) -> Any: ...


OHLC_COLUMNS = ("Open", "High", "Low", "Close")
OHLCV_COLUMNS = (*OHLC_COLUMNS, "Volume")
_SOURCE_COLUMNS_BY_OUTPUT = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
}


def _empty_datetime_index_frame(columns: Sequence[str], *, index_name: str = "date") -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns), index=pd.DatetimeIndex([], name=index_name))


def rows_to_datetime_index_frame(
    rows: Sequence[RowLike],
    *,
    columns: Sequence[str],
    source_columns: Mapping[str, str] | None = None,
    date_column: str = "date",
) -> pd.DataFrame:
    if not rows:
        return _empty_datetime_index_frame(columns, index_name=date_column)

    source_by_output = source_columns or {column: column for column in columns}
    records = [
        {
            date_column: row[date_column],
            **{
                output_column: row[source_by_output[output_column]]
                for output_column in columns
            },
        }
        for row in rows
    ]
    frame = pd.DataFrame.from_records(records, columns=[date_column, *columns])
    frame[date_column] = pd.to_datetime(frame[date_column])
    indexed = frame.set_index(date_column).sort_index()
    return indexed.loc[:, list(columns)]


def rows_to_ohlcv_frame(rows: Sequence[RowLike]) -> pd.DataFrame:
    return rows_to_datetime_index_frame(
        rows,
        columns=OHLCV_COLUMNS,
        source_columns=_SOURCE_COLUMNS_BY_OUTPUT,
    )


def rows_to_ohlc_frame(rows: Sequence[RowLike]) -> pd.DataFrame:
    return rows_to_datetime_index_frame(
        rows,
        columns=OHLC_COLUMNS,
        source_columns=_SOURCE_COLUMNS_BY_OUTPUT,
    )
