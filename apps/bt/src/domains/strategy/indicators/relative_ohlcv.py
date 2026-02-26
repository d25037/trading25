"""Relative OHLCV transformation domain logic."""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd


def _compute_relative_ohlc_column(
    stock_col: pd.Series[float],
    bench_col: pd.Series[float],
    handle_zero_division: Literal["skip", "zero", "null"],
) -> pd.Series[float]:
    """Compute relative OHLC column with configurable zero-division behavior."""
    if handle_zero_division == "skip":
        return stock_col / bench_col

    fill_value = 0.0 if handle_zero_division == "zero" else np.nan
    return pd.Series(
        np.where(bench_col == 0, fill_value, stock_col / bench_col),
        index=stock_col.index,
    )


def calculate_relative_ohlcv(
    stock_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
    handle_zero_division: Literal["skip", "zero", "null"] = "skip",
) -> pd.DataFrame:
    """Calculate relative OHLCV (stock / benchmark)."""
    common_dates = stock_df.index.intersection(benchmark_df.index)
    if len(common_dates) == 0:
        raise ValueError("銘柄とベンチマークに共通する日付がありません")

    stock_aligned = stock_df.loc[common_dates]
    bench_aligned = benchmark_df.loc[common_dates]

    ohlc_cols = ["Open", "High", "Low", "Close"]
    bench_has_zero = (bench_aligned[ohlc_cols] == 0).any(axis=1)

    if handle_zero_division == "skip":
        valid_mask = ~bench_has_zero
        stock_aligned = stock_aligned.loc[valid_mask]
        bench_aligned = bench_aligned.loc[valid_mask]

    if stock_aligned.empty:
        raise ValueError("相対計算可能なデータがありません（全日がゼロ除算）")

    result = pd.DataFrame(index=stock_aligned.index)
    for col in ohlc_cols:
        result[col] = _compute_relative_ohlc_column(
            stock_aligned[col],
            bench_aligned[col],
            handle_zero_division,
        )
    result["Volume"] = stock_aligned["Volume"]
    return result
