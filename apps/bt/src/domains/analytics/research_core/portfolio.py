"""Reusable portfolio-curve helpers for runner-first analytics research."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PricePath:
    dates: np.ndarray
    closes: np.ndarray

    def slice_inclusive(self, start_date: str, end_date: str) -> tuple[np.ndarray, np.ndarray]:
        start_idx = int(np.searchsorted(self.dates, start_date, side="left"))
        end_idx = int(np.searchsorted(self.dates, end_date, side="right"))
        if end_idx <= start_idx:
            return np.array([], dtype=str), np.array([], dtype=float)
        return self.dates[start_idx:end_idx], self.closes[start_idx:end_idx]


def build_price_path_lookup(
    price_df: pd.DataFrame,
    *,
    code_column: str = "code",
    date_column: str = "date",
    close_column: str = "close",
) -> dict[str, PricePath]:
    """Build per-code sorted close arrays for repeated event path lookups."""
    if price_df.empty:
        return {}
    required = {code_column, date_column, close_column}
    missing = required.difference(price_df.columns)
    if missing:
        raise ValueError(f"price_df missing required columns: {sorted(missing)}")

    working = price_df[[code_column, date_column, close_column]].copy()
    working["_date_key"] = _date_key_series(working[date_column])
    working["_close"] = pd.to_numeric(working[close_column], errors="coerce")
    working = working.dropna(subset=[code_column, "_date_key", "_close"])
    if working.empty:
        return {}

    lookup: dict[str, PricePath] = {}
    for code, frame in working.groupby(code_column, sort=False):
        ordered = frame.sort_values("_date_key", kind="stable")
        lookup[str(code)] = PricePath(
            dates=ordered["_date_key"].astype(str).to_numpy(),
            closes=ordered["_close"].astype(float).to_numpy(dtype=float),
        )
    return lookup


def build_event_portfolio_daily_df(
    selected_event_df: pd.DataFrame,
    price_df: pd.DataFrame,
    *,
    group_columns: Sequence[str],
    code_column: str = "code",
    entry_date_column: str = "entry_date",
    exit_date_column: str = "exit_date",
    entry_open_column: str = "entry_open",
) -> pd.DataFrame:
    """Build equal-weight daily portfolio curves from selected event windows.

    This helper centralizes a common research-runner fast path: price paths are
    grouped once by code, then each event window is resolved with binary search
    instead of repeatedly filtering the full price frame.
    """
    columns = [
        *group_columns,
        "date",
        "active_positions",
        "mean_daily_return",
        "mean_daily_return_pct",
        "portfolio_value",
        "drawdown_pct",
    ]
    if selected_event_df.empty or price_df.empty:
        return pd.DataFrame(columns=columns)
    required_event_columns = {
        *group_columns,
        code_column,
        entry_date_column,
        exit_date_column,
        entry_open_column,
    }
    missing_event_columns = required_event_columns.difference(selected_event_df.columns)
    if missing_event_columns:
        raise ValueError(f"selected_event_df missing required columns: {sorted(missing_event_columns)}")

    price_paths = build_price_path_lookup(price_df, code_column=code_column)
    aggregate: dict[tuple[Any, ...], list[float]] = defaultdict(lambda: [0.0, 0.0])
    for event in selected_event_df.to_dict(orient="records"):
        price_path = price_paths.get(str(event[code_column]))
        if price_path is None:
            continue
        entry_date = _date_key(event.get(entry_date_column))
        exit_date = _date_key(event.get(exit_date_column))
        if entry_date is None or exit_date is None:
            continue
        path_dates, closes = price_path.slice_inclusive(entry_date, exit_date)
        if len(path_dates) == 0:
            continue
        entry_open = _finite_float(event.get(entry_open_column))
        if entry_open is None or entry_open <= 0:
            continue
        if not np.isfinite(closes).all():
            continue
        previous_close = np.concatenate(([entry_open], closes[:-1]))
        daily_returns = closes / previous_close - 1.0
        group_key = tuple(event[column] for column in group_columns)
        for date_value, daily_return in zip(path_dates, daily_returns, strict=True):
            key = (*group_key, str(date_value))
            aggregate[key][0] += float(daily_return)
            aggregate[key][1] += 1.0

    return _portfolio_daily_frame_from_aggregate(
        aggregate,
        group_columns=group_columns,
        columns=columns,
    )


def _portfolio_daily_frame_from_aggregate(
    aggregate: Mapping[tuple[Any, ...], list[float]],
    *,
    group_columns: Sequence[str],
    columns: Sequence[str],
) -> pd.DataFrame:
    if not aggregate:
        return pd.DataFrame(columns=list(columns))
    records = [
        {
            **dict(zip(group_columns, key[:-1], strict=True)),
            "date": key[-1],
            "active_positions": int(values[1]),
            "mean_daily_return": float(values[0] / values[1]),
            "mean_daily_return_pct": float(values[0] / values[1] * 100.0),
        }
        for key, values in aggregate.items()
    ]
    daily_df = pd.DataFrame(records).sort_values([*group_columns, "date"], kind="stable").reset_index(
        drop=True
    )
    daily_df["portfolio_value"] = np.nan
    daily_df["drawdown_pct"] = np.nan
    for _, group in daily_df.groupby(list(group_columns), observed=True, sort=False):
        idx = list(group.index)
        values = (1.0 + daily_df.loc[idx, "mean_daily_return"]).cumprod()
        peaks = values.cummax()
        daily_df.loc[idx, "portfolio_value"] = values.to_numpy()
        daily_df.loc[idx, "drawdown_pct"] = ((values / peaks - 1.0) * 100.0).to_numpy()
    return daily_df[list(columns)]


def _date_key_series(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    return parsed.dt.strftime("%Y-%m-%d")


def _date_key(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    parsed = pd.to_datetime(str(value), errors="coerce")
    if pd.isna(parsed):
        return None
    return str(parsed.strftime("%Y-%m-%d"))


def _finite_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result
