"""Point-in-time guards for dataframe and record-based analytics flows."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TypeVar

import pandas as pd


T = TypeVar("T")


def _normalize_group_columns(group_cols: str | Sequence[str]) -> list[str]:
    if isinstance(group_cols, str):
        return [group_cols]
    return [str(column) for column in group_cols]


def _coerce_date_series(frame: pd.DataFrame, *, date_col: str) -> pd.Series:
    if date_col not in frame.columns:
        raise ValueError(f"Missing required date column: {date_col}")
    return frame[date_col].astype(str)


def slice_frame_as_of(
    frame: pd.DataFrame,
    *,
    as_of_date: str,
    date_col: str = "date",
) -> pd.DataFrame:
    """Return rows whose date column is on or before the as-of date."""
    if frame.empty:
        return frame.copy()

    coerced_dates = _coerce_date_series(frame, date_col=date_col)
    sliced = frame.loc[coerced_dates <= str(as_of_date)].copy()
    if date_col in sliced.columns:
        sliced[date_col] = sliced[date_col].astype(str)
    return sliced


def assert_no_future_rows(
    frame: pd.DataFrame,
    *,
    as_of_date: str,
    date_col: str = "date",
    frame_name: str = "dataframe",
) -> None:
    """Raise when the frame still contains rows after the as-of date."""
    if frame.empty:
        return

    coerced_dates = _coerce_date_series(frame, date_col=date_col)
    future_mask = coerced_dates > str(as_of_date)
    if not future_mask.any():
        return

    future_dates = sorted(coerced_dates[future_mask].drop_duplicates().tolist())
    sample_dates = ", ".join(future_dates[:3])
    if len(future_dates) > 3:
        sample_dates = f"{sample_dates}, ..."
    raise ValueError(
        f"{frame_name} contains rows after as_of_date {as_of_date}: {sample_dates}"
    )


def latest_rows_per_group_as_of(
    frame: pd.DataFrame,
    *,
    group_cols: str | Sequence[str],
    as_of_date: str | None = None,
    date_col: str = "date",
    tie_breaker_cols: Sequence[str] = (),
) -> pd.DataFrame:
    """Return the latest row per group, optionally after as-of slicing."""
    if frame.empty:
        return frame.copy()

    normalized_group_cols = _normalize_group_columns(group_cols)
    working_df = frame.copy()
    if as_of_date is not None:
        working_df = slice_frame_as_of(
            working_df,
            as_of_date=as_of_date,
            date_col=date_col,
        )
    if working_df.empty:
        return working_df

    working_df[date_col] = _coerce_date_series(working_df, date_col=date_col)
    ordered_columns = [*normalized_group_cols, date_col, *tie_breaker_cols]
    latest_df = (
        working_df.sort_values(ordered_columns, kind="stable")
        .groupby(normalized_group_cols, observed=True, sort=False)
        .tail(1)
        .reset_index(drop=True)
    )
    return latest_df


def filter_records_as_of(
    records: Sequence[T],
    *,
    as_of_date: str,
    date_getter: Callable[[T], str],
) -> list[T]:
    """Return records whose logical date is on or before the as-of date."""
    cutoff = str(as_of_date)
    return [record for record in records if str(date_getter(record)) <= cutoff]
