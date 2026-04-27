"""Falling-knife reversal study from daily market OHLC.

The study treats "do not catch a falling knife" as a testable timing question:
buy the next session after a stress signal, or wait until a simple stabilization
confirmation appears.  Signal features use only data available at the signal
date close; trade entry is the next session open.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pandas as pd

from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    date_where_clause,
    fetch_date_range,
    normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)
from src.domains.strategy.indicators.calculations import compute_risk_adjusted_return

RatioType = Literal["sharpe", "sortino"]

FALLING_KNIFE_REVERSAL_STUDY_EXPERIMENT_ID = (
    "market-behavior/falling-knife-reversal-study"
)
DEFAULT_MARKET_CODES: tuple[str, ...] = ("0111", "0112", "0113")
DEFAULT_FORWARD_HORIZONS: tuple[int, ...] = (5, 20, 60)
DEFAULT_RISK_ADJUSTED_LOOKBACK = 60
DEFAULT_FIVE_DAY_DROP_THRESHOLD = -0.10
DEFAULT_TWENTY_DAY_DROP_THRESHOLD = -0.20
DEFAULT_SIXTY_DAY_DRAWDOWN_THRESHOLD = -0.25
DEFAULT_RISK_ADJUSTED_THRESHOLD = 0.0
DEFAULT_MIN_CONDITION_COUNT = 2
DEFAULT_MAX_WAIT_DAYS = 10
DEFAULT_SIGNAL_COOLDOWN_DAYS = 20
DEFAULT_SEVERE_LOSS_THRESHOLD = -0.10
_PREFER_4DIGIT_ORDER_SQL = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"

_BASE_EVENT_TABLE_COLUMNS: tuple[str, ...] = (
    "signal_date",
    "code",
    "company_name",
    "market_code",
    "market_name",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "next_date",
    "next_open",
    "return_5d",
    "return_20d",
    "drawdown_from_60d_high",
    "sma20",
    "sma60",
    "sma20_slope_5d",
    "atr20_pct",
    "condition_count",
    "sharp_5d_drop",
    "deep_20d_drop",
    "deep_60d_drawdown",
    "downtrend_sma",
    "poor_risk_adjusted_return",
    "risk_adjusted_bucket",
    "wait_signal_date",
    "wait_entry_date",
    "wait_days",
)
_TRADE_SUMMARY_COLUMNS: tuple[str, ...] = (
    "strategy_family",
    "horizon_days",
    "market_code",
    "market_name",
    "risk_adjusted_bucket",
    "sample_count",
    "mean_return_pct",
    "median_return_pct",
    "hit_rate_pct",
    "p10_return_pct",
    "severe_loss_rate_pct",
)
_PAIRED_DELTA_COLUMNS: tuple[str, ...] = (
    "horizon_days",
    "market_code",
    "market_name",
    "risk_adjusted_bucket",
    "paired_count",
    "mean_wait_minus_catch_pct",
    "median_wait_minus_catch_pct",
    "wait_better_rate_pct",
)
_CONDITION_PROFILE_COLUMNS: tuple[str, ...] = (
    "condition_name",
    "event_count",
    "event_rate_pct",
    "catch_return_column",
    "mean_catch_return_pct",
    "median_catch_return_pct",
    "severe_loss_rate_pct",
)


@dataclass(frozen=True)
class FallingKnifeReversalStudyResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    market_codes: tuple[str, ...]
    forward_horizons: tuple[int, ...]
    risk_adjusted_lookback: int
    condition_ratio_type: RatioType
    five_day_drop_threshold: float
    twenty_day_drop_threshold: float
    sixty_day_drawdown_threshold: float
    risk_adjusted_threshold: float
    min_condition_count: int
    max_wait_days: int
    signal_cooldown_days: int
    severe_loss_threshold: float
    source_row_count: int
    event_count: int
    wait_candidate_count: int
    research_note: str
    event_df: pd.DataFrame
    trade_summary_df: pd.DataFrame
    paired_delta_df: pd.DataFrame
    condition_profile_df: pd.DataFrame


def _empty_df(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _event_table_columns(risk_adjusted_lookback: int) -> list[str]:
    columns = list(_BASE_EVENT_TABLE_COLUMNS)
    insert_at = columns.index("condition_count")
    columns[insert_at:insert_at] = [
        f"risk_adjusted_return_{risk_adjusted_lookback}_sharpe",
        f"risk_adjusted_return_{risk_adjusted_lookback}_sortino",
    ]
    return columns


def _fmt_pct(value: object, digits: int = 2) -> str:
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return "-"
    if not math.isfinite(number):
        return "-"
    return f"{number * 100.0:.{digits}f}%"


def _fmt_ratio(value: object, digits: int = 2) -> str:
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return "-"
    if not math.isfinite(number):
        return "-"
    return f"{number:.{digits}f}"


def _normalize_forward_horizons(values: Sequence[int]) -> tuple[int, ...]:
    normalized: list[int] = []
    for raw_value in values:
        value = int(raw_value)
        if value < 1:
            raise ValueError("forward horizons must be positive integers")
        if value not in normalized:
            normalized.append(value)
    if not normalized:
        raise ValueError("at least one forward horizon is required")
    return tuple(sorted(normalized))


def _normalize_market_codes(values: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for raw_value in values:
        value = str(raw_value).strip()
        if not value:
            continue
        if value not in normalized:
            normalized.append(value)
    return tuple(normalized)


def _query_daily_price_df(
    conn: Any,
    *,
    market_codes: Sequence[str],
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    normalized_code_sql = normalize_code_sql("code")
    date_where_sql, params = date_where_clause("date", start_date, end_date)
    normalized_market_codes = _normalize_market_codes(market_codes)
    market_filter_sql = ""
    if normalized_market_codes:
        placeholders = ", ".join("?" for _ in normalized_market_codes)
        market_filter_sql = f"WHERE market_code IN ({placeholders})"
        params.extend(normalized_market_codes)
    sql = f"""
        WITH stocks_snapshot AS (
            SELECT
                normalized_code,
                company_name,
                market_code,
                market_name
            FROM (
                SELECT
                    {normalized_code_sql} AS normalized_code,
                    company_name,
                    market_code,
                    market_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized_code_sql}
                        ORDER BY {_PREFER_4DIGIT_ORDER_SQL}, code
                    ) AS row_priority
                FROM stocks
                {market_filter_sql}
            )
            WHERE row_priority = 1
        ),
        stock_daily AS (
            SELECT
                date,
                normalized_code AS code,
                open,
                high,
                low,
                close,
                volume
            FROM (
                SELECT
                    date,
                    {normalized_code_sql} AS normalized_code,
                    CAST(open AS DOUBLE) AS open,
                    CAST(high AS DOUBLE) AS high,
                    CAST(low AS DOUBLE) AS low,
                    CAST(close AS DOUBLE) AS close,
                    CAST(volume AS DOUBLE) AS volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized_code_sql}, date
                        ORDER BY {_PREFER_4DIGIT_ORDER_SQL}, code
                    ) AS row_priority
                FROM stock_data
                {date_where_sql}
            )
            WHERE row_priority = 1
                AND open IS NOT NULL
                AND high IS NOT NULL
                AND low IS NOT NULL
                AND close IS NOT NULL
        )
        SELECT
            stock_daily.date,
            stock_daily.code,
            stocks_snapshot.company_name,
            COALESCE(stocks_snapshot.market_code, 'unmapped_latest_stocks') AS market_code,
            COALESCE(stocks_snapshot.market_name, 'UNMAPPED_LATEST_STOCKS') AS market_name,
            stock_daily.open,
            stock_daily.high,
            stock_daily.low,
            stock_daily.close,
            stock_daily.volume
        FROM stock_daily
        LEFT JOIN stocks_snapshot
            ON stocks_snapshot.normalized_code = stock_daily.code
        WHERE stocks_snapshot.normalized_code IS NOT NULL
        ORDER BY stock_daily.code, stock_daily.date
    """
    return conn.execute(sql, params).fetchdf()


def _compute_feature_frame(
    price_df: pd.DataFrame,
    *,
    forward_horizons: Sequence[int],
    risk_adjusted_lookback: int,
) -> pd.DataFrame:
    if price_df.empty:
        return _empty_df(_event_table_columns(risk_adjusted_lookback))

    horizon_values = _normalize_forward_horizons(forward_horizons)
    frames: list[pd.DataFrame] = []
    for _, group in price_df.groupby("code", sort=False):
        frame = group.sort_values("date", kind="stable").reset_index(drop=True).copy()
        frame["code_pos"] = np.arange(len(frame), dtype=int)
        close = pd.to_numeric(frame["close"], errors="coerce").astype(float)
        high = pd.to_numeric(frame["high"], errors="coerce").astype(float)
        low = pd.to_numeric(frame["low"], errors="coerce").astype(float)

        frame["prev_close"] = close.shift(1)
        frame["next_date"] = frame["date"].shift(-1)
        frame["next_open"] = frame["open"].shift(-1)
        frame["return_5d"] = close / close.shift(5) - 1.0
        frame["return_20d"] = close / close.shift(20) - 1.0
        frame["rolling_60d_high"] = close.rolling(60, min_periods=20).max()
        frame["drawdown_from_60d_high"] = close / frame["rolling_60d_high"] - 1.0
        frame["sma5"] = close.rolling(5, min_periods=5).mean()
        frame["sma20"] = close.rolling(20, min_periods=20).mean()
        frame["sma60"] = close.rolling(60, min_periods=40).mean()
        frame["sma20_slope_5d"] = frame["sma20"] / frame["sma20"].shift(5) - 1.0
        true_range = pd.concat(
            [
                high - low,
                (high - frame["prev_close"]).abs(),
                (low - frame["prev_close"]).abs(),
            ],
            axis=1,
        ).max(axis=1)
        frame["atr20_pct"] = true_range.rolling(20, min_periods=10).mean() / close
        frame[f"risk_adjusted_return_{risk_adjusted_lookback}_sharpe"] = (
            compute_risk_adjusted_return(
                close=close,
                lookback_period=risk_adjusted_lookback,
                ratio_type="sharpe",
            ).to_numpy()
        )
        frame[f"risk_adjusted_return_{risk_adjusted_lookback}_sortino"] = (
            compute_risk_adjusted_return(
                close=close,
                lookback_period=risk_adjusted_lookback,
                ratio_type="sortino",
            ).to_numpy()
        )
        frame["prior_3d_low"] = low.shift(1).rolling(3, min_periods=3).min()
        frame["stabilized"] = (close > frame["sma5"]) & (low >= frame["prior_3d_low"])
        for horizon in horizon_values:
            frame[f"future_close_{horizon}d"] = close.shift(-horizon)
            frame[f"catch_return_{horizon}d"] = (
                frame[f"future_close_{horizon}d"] / frame["next_open"] - 1.0
            )
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def _assign_risk_adjusted_bucket(
    event_df: pd.DataFrame,
    *,
    risk_adjusted_lookback: int,
    condition_ratio_type: RatioType,
) -> pd.Series:
    if event_df.empty:
        return pd.Series(dtype="object")
    column = f"risk_adjusted_return_{risk_adjusted_lookback}_{condition_ratio_type}"
    values = pd.to_numeric(event_df[column], errors="coerce")
    bucket = pd.Series("unbucketed", index=event_df.index, dtype="object")
    valid = values.dropna()
    if len(valid) < 5 or valid.nunique(dropna=True) < 2:
        return bucket
    ranks = valid.rank(method="first")
    qcut = pd.qcut(
        ranks,
        q=5,
        labels=("Q1_lowest", "Q2", "Q3", "Q4", "Q5_highest"),
        duplicates="drop",
    )
    bucket.loc[qcut.index] = qcut.astype(str)
    return bucket


def _build_event_df(
    feature_df: pd.DataFrame,
    *,
    forward_horizons: Sequence[int],
    risk_adjusted_lookback: int,
    condition_ratio_type: RatioType,
    five_day_drop_threshold: float,
    twenty_day_drop_threshold: float,
    sixty_day_drawdown_threshold: float,
    risk_adjusted_threshold: float,
    min_condition_count: int,
    max_wait_days: int,
    signal_cooldown_days: int,
) -> pd.DataFrame:
    if feature_df.empty:
        return _empty_df(_event_table_columns(risk_adjusted_lookback))
    if risk_adjusted_lookback <= 1:
        raise ValueError("risk_adjusted_lookback must be greater than 1")
    if condition_ratio_type not in ("sharpe", "sortino"):
        raise ValueError("condition_ratio_type must be 'sharpe' or 'sortino'")
    if min_condition_count < 1:
        raise ValueError("min_condition_count must be at least 1")
    if max_wait_days < 1:
        raise ValueError("max_wait_days must be at least 1")
    if signal_cooldown_days < 0:
        raise ValueError("signal_cooldown_days must be non-negative")

    frame = feature_df.copy()
    risk_column = f"risk_adjusted_return_{risk_adjusted_lookback}_{condition_ratio_type}"
    frame["sharp_5d_drop"] = frame["return_5d"] <= five_day_drop_threshold
    frame["deep_20d_drop"] = frame["return_20d"] <= twenty_day_drop_threshold
    frame["deep_60d_drawdown"] = (
        frame["drawdown_from_60d_high"] <= sixty_day_drawdown_threshold
    )
    frame["downtrend_sma"] = (
        (frame["close"] < frame["sma20"])
        & (frame["sma20"] < frame["sma60"])
        & (frame["sma20_slope_5d"] < 0.0)
    )
    frame["poor_risk_adjusted_return"] = frame[risk_column] <= risk_adjusted_threshold
    condition_columns = [
        "sharp_5d_drop",
        "deep_20d_drop",
        "deep_60d_drawdown",
        "downtrend_sma",
        "poor_risk_adjusted_return",
    ]
    frame["condition_count"] = frame[condition_columns].fillna(False).sum(axis=1)
    event_mask = (frame["condition_count"] >= min_condition_count) & frame["next_open"].notna()
    event_df = frame.loc[event_mask].copy()
    event_df = _apply_signal_cooldown(event_df, signal_cooldown_days=signal_cooldown_days)
    if event_df.empty:
        return _empty_df([*_event_table_columns(risk_adjusted_lookback), *_return_columns(forward_horizons)])

    wait_rows = _find_wait_entries(
        frame,
        event_df,
        forward_horizons=forward_horizons,
        max_wait_days=max_wait_days,
    )
    wait_df = pd.DataFrame(wait_rows).set_index("event_index")
    event_df["wait_signal_date"] = wait_df["wait_signal_date"]
    event_df["wait_entry_date"] = wait_df["wait_entry_date"]
    event_df["wait_days"] = wait_df["wait_days"]
    for horizon in _normalize_forward_horizons(forward_horizons):
        event_df[f"wait_return_{horizon}d"] = wait_df[f"wait_return_{horizon}d"]
    event_df["risk_adjusted_bucket"] = _assign_risk_adjusted_bucket(
        event_df,
        risk_adjusted_lookback=risk_adjusted_lookback,
        condition_ratio_type=condition_ratio_type,
    )
    event_df = event_df.rename(columns={"date": "signal_date"})
    output_columns = [
        *_event_table_columns(risk_adjusted_lookback),
        *_return_columns(forward_horizons),
    ]
    for column in output_columns:
        if column not in event_df.columns:
            event_df[column] = np.nan
    return event_df[output_columns].sort_values(
        ["signal_date", "code"],
        kind="stable",
    ).reset_index(drop=True)


def _apply_signal_cooldown(
    event_df: pd.DataFrame,
    *,
    signal_cooldown_days: int,
) -> pd.DataFrame:
    if event_df.empty or signal_cooldown_days <= 0:
        return event_df
    keep_indices: list[Any] = []
    for _, group in event_df.sort_values(["code", "date"], kind="stable").groupby(
        "code",
        sort=False,
    ):
        last_kept_pos: int | None = None
        for index, row in group.iterrows():
            code_pos = int(row["code_pos"])
            if last_kept_pos is None or code_pos - last_kept_pos > signal_cooldown_days:
                keep_indices.append(index)
                last_kept_pos = code_pos
    return event_df.loc[sorted(keep_indices)].copy()


def _return_columns(forward_horizons: Sequence[int]) -> list[str]:
    columns: list[str] = []
    for horizon in _normalize_forward_horizons(forward_horizons):
        columns.extend([f"catch_return_{horizon}d", f"wait_return_{horizon}d"])
    return columns


def _find_wait_entries(
    feature_df: pd.DataFrame,
    event_df: pd.DataFrame,
    *,
    forward_horizons: Sequence[int],
    max_wait_days: int,
) -> list[dict[str, object]]:
    horizon_values = _normalize_forward_horizons(forward_horizons)
    frame_by_code = {
        str(code): group.reset_index().sort_values("date", kind="stable").reset_index(drop=True)
        for code, group in feature_df.groupby("code", sort=False)
    }
    rows: list[dict[str, object]] = []
    for event_index, event in event_df.iterrows():
        code = str(event["code"])
        code_frame = frame_by_code[code]
        source_indices = code_frame.index[code_frame["index"] == event_index].tolist()
        row: dict[str, object] = {
            "event_index": event_index,
            "wait_signal_date": None,
            "wait_entry_date": None,
            "wait_days": np.nan,
        }
        for horizon in horizon_values:
            row[f"wait_return_{horizon}d"] = np.nan
        if not source_indices:
            rows.append(row)
            continue
        event_pos = int(source_indices[0])
        max_pos = min(len(code_frame) - 1, event_pos + max_wait_days)
        for wait_pos in range(event_pos + 1, max_pos + 1):
            candidate = code_frame.iloc[wait_pos]
            if not bool(candidate.get("stabilized", False)):
                continue
            entry_pos = wait_pos + 1
            if entry_pos >= len(code_frame):
                continue
            entry_open = candidate.get("next_open")
            if pd.isna(entry_open) or float(entry_open) <= 0.0:
                continue
            row["wait_signal_date"] = str(candidate["date"])
            row["wait_entry_date"] = str(candidate["next_date"])
            row["wait_days"] = int(wait_pos - event_pos)
            for horizon in horizon_values:
                exit_pos = wait_pos + horizon
                if exit_pos >= len(code_frame):
                    continue
                exit_close = code_frame.iloc[exit_pos]["close"]
                if pd.notna(exit_close):
                    row[f"wait_return_{horizon}d"] = float(exit_close) / float(entry_open) - 1.0
            break
        rows.append(row)
    return rows


def _trade_long_df(event_df: pd.DataFrame, forward_horizons: Sequence[int]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for event in event_df.to_dict(orient="records"):
        for horizon in _normalize_forward_horizons(forward_horizons):
            for strategy_family, column in (
                ("catch_next_open", f"catch_return_{horizon}d"),
                ("wait_for_stabilization", f"wait_return_{horizon}d"),
            ):
                value = event.get(column)
                if value is None or pd.isna(value):
                    continue
                rows.append(
                    {
                        "strategy_family": strategy_family,
                        "horizon_days": horizon,
                        "market_code": event["market_code"],
                        "market_name": event["market_name"],
                        "risk_adjusted_bucket": event["risk_adjusted_bucket"],
                        "trade_return": float(value),
                    }
                )
    return pd.DataFrame(rows)


def _summarize_trade_returns(
    values: pd.Series,
    *,
    severe_loss_threshold: float,
) -> dict[str, float | int]:
    returns = pd.to_numeric(values, errors="coerce").dropna()
    if returns.empty:
        return {
            "sample_count": 0,
            "mean_return_pct": math.nan,
            "median_return_pct": math.nan,
            "hit_rate_pct": math.nan,
            "p10_return_pct": math.nan,
            "severe_loss_rate_pct": math.nan,
        }
    return {
        "sample_count": int(len(returns)),
        "mean_return_pct": float(returns.mean() * 100.0),
        "median_return_pct": float(returns.median() * 100.0),
        "hit_rate_pct": float((returns > 0.0).mean() * 100.0),
        "p10_return_pct": float(returns.quantile(0.10) * 100.0),
        "severe_loss_rate_pct": float((returns <= severe_loss_threshold).mean() * 100.0),
    }


def _build_trade_summary_df(
    event_df: pd.DataFrame,
    *,
    forward_horizons: Sequence[int],
    severe_loss_threshold: float,
) -> pd.DataFrame:
    trade_df = _trade_long_df(event_df, forward_horizons)
    if trade_df.empty:
        return _empty_df(_TRADE_SUMMARY_COLUMNS)
    rows: list[dict[str, object]] = []
    group_columns = [
        "strategy_family",
        "horizon_days",
        "market_code",
        "market_name",
        "risk_adjusted_bucket",
    ]
    for keys, group in trade_df.groupby(group_columns, dropna=False, sort=False):
        row = dict(zip(group_columns, keys, strict=True))
        row.update(
            _summarize_trade_returns(
                group["trade_return"],
                severe_loss_threshold=severe_loss_threshold,
            )
        )
        rows.append(row)
    return pd.DataFrame(rows, columns=list(_TRADE_SUMMARY_COLUMNS)).sort_values(
        ["horizon_days", "strategy_family", "market_code", "risk_adjusted_bucket"],
        kind="stable",
    ).reset_index(drop=True)


def _build_paired_delta_df(
    event_df: pd.DataFrame,
    *,
    forward_horizons: Sequence[int],
) -> pd.DataFrame:
    if event_df.empty:
        return _empty_df(_PAIRED_DELTA_COLUMNS)
    rows: list[dict[str, object]] = []
    group_columns = ["market_code", "market_name", "risk_adjusted_bucket"]
    for horizon in _normalize_forward_horizons(forward_horizons):
        frame = event_df.copy()
        frame["return_delta"] = frame[f"wait_return_{horizon}d"] - frame[f"catch_return_{horizon}d"]
        valid = frame[pd.to_numeric(frame["return_delta"], errors="coerce").notna()]
        for keys, group in valid.groupby(group_columns, dropna=False, sort=False):
            deltas = pd.to_numeric(group["return_delta"], errors="coerce").dropna()
            if deltas.empty:
                continue
            row = dict(zip(group_columns, keys, strict=True))
            row.update(
                {
                    "horizon_days": horizon,
                    "paired_count": int(len(deltas)),
                    "mean_wait_minus_catch_pct": float(deltas.mean() * 100.0),
                    "median_wait_minus_catch_pct": float(deltas.median() * 100.0),
                    "wait_better_rate_pct": float((deltas > 0.0).mean() * 100.0),
                }
            )
            rows.append(row)
    if not rows:
        return _empty_df(_PAIRED_DELTA_COLUMNS)
    return pd.DataFrame(rows, columns=list(_PAIRED_DELTA_COLUMNS)).sort_values(
        ["horizon_days", "market_code", "risk_adjusted_bucket"],
        kind="stable",
    ).reset_index(drop=True)


def _build_condition_profile_df(
    event_df: pd.DataFrame,
    *,
    severe_loss_threshold: float,
) -> pd.DataFrame:
    if event_df.empty:
        return _empty_df(_CONDITION_PROFILE_COLUMNS)
    return_column = next(
        (column for column in event_df.columns if column.startswith("catch_return_")),
        None,
    )
    rows: list[dict[str, object]] = []
    event_count = len(event_df)
    for column in (
        "sharp_5d_drop",
        "deep_20d_drop",
        "deep_60d_drawdown",
        "downtrend_sma",
        "poor_risk_adjusted_return",
    ):
        subset = event_df[event_df[column].astype(bool)]
        returns = (
            pd.to_numeric(subset[return_column], errors="coerce").dropna()
            if return_column is not None
            else pd.Series(dtype="float64")
        )
        rows.append(
            {
                "condition_name": column,
                "event_count": int(len(subset)),
                "event_rate_pct": float(len(subset) / event_count * 100.0)
                if event_count
                else math.nan,
                "catch_return_column": return_column,
                "mean_catch_return_pct": float(returns.mean() * 100.0)
                if not returns.empty
                else math.nan,
                "median_catch_return_pct": float(returns.median() * 100.0)
                if not returns.empty
                else math.nan,
                "severe_loss_rate_pct": float(
                    (returns <= severe_loss_threshold).mean() * 100.0
                )
                if not returns.empty
                else math.nan,
            }
        )
    return pd.DataFrame(rows, columns=list(_CONDITION_PROFILE_COLUMNS))


def run_falling_knife_reversal_study(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    market_codes: Sequence[str] = DEFAULT_MARKET_CODES,
    forward_horizons: Sequence[int] = DEFAULT_FORWARD_HORIZONS,
    risk_adjusted_lookback: int = DEFAULT_RISK_ADJUSTED_LOOKBACK,
    condition_ratio_type: RatioType = "sortino",
    five_day_drop_threshold: float = DEFAULT_FIVE_DAY_DROP_THRESHOLD,
    twenty_day_drop_threshold: float = DEFAULT_TWENTY_DAY_DROP_THRESHOLD,
    sixty_day_drawdown_threshold: float = DEFAULT_SIXTY_DAY_DRAWDOWN_THRESHOLD,
    risk_adjusted_threshold: float = DEFAULT_RISK_ADJUSTED_THRESHOLD,
    min_condition_count: int = DEFAULT_MIN_CONDITION_COUNT,
    max_wait_days: int = DEFAULT_MAX_WAIT_DAYS,
    signal_cooldown_days: int = DEFAULT_SIGNAL_COOLDOWN_DAYS,
    severe_loss_threshold: float = DEFAULT_SEVERE_LOSS_THRESHOLD,
) -> FallingKnifeReversalStudyResult:
    normalized_horizons = _normalize_forward_horizons(forward_horizons)
    normalized_markets = _normalize_market_codes(market_codes)
    with open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="falling-knife-reversal-study-",
    ) as ctx:
        available_start_date, available_end_date = fetch_date_range(
            ctx.connection,
            table_name="stock_data",
        )
        price_df = _query_daily_price_df(
            ctx.connection,
            market_codes=normalized_markets,
            start_date=start_date,
            end_date=end_date,
        )

    feature_df = _compute_feature_frame(
        price_df,
        forward_horizons=normalized_horizons,
        risk_adjusted_lookback=risk_adjusted_lookback,
    )
    event_df = _build_event_df(
        feature_df,
        forward_horizons=normalized_horizons,
        risk_adjusted_lookback=risk_adjusted_lookback,
        condition_ratio_type=condition_ratio_type,
        five_day_drop_threshold=five_day_drop_threshold,
        twenty_day_drop_threshold=twenty_day_drop_threshold,
        sixty_day_drawdown_threshold=sixty_day_drawdown_threshold,
        risk_adjusted_threshold=risk_adjusted_threshold,
        min_condition_count=min_condition_count,
        max_wait_days=max_wait_days,
        signal_cooldown_days=signal_cooldown_days,
    )
    trade_summary_df = _build_trade_summary_df(
        event_df,
        forward_horizons=normalized_horizons,
        severe_loss_threshold=severe_loss_threshold,
    )
    paired_delta_df = _build_paired_delta_df(
        event_df,
        forward_horizons=normalized_horizons,
    )
    condition_profile_df = _build_condition_profile_df(
        event_df,
        severe_loss_threshold=severe_loss_threshold,
    )
    analysis_start_date = (
        str(event_df["signal_date"].min()) if not event_df.empty else None
    )
    analysis_end_date = str(event_df["signal_date"].max()) if not event_df.empty else None
    wait_candidate_count = (
        int(event_df["wait_entry_date"].notna().sum()) if not event_df.empty else 0
    )
    research_note = (
        "Falling-knife events are selected by overlap across recent drawdown, trend, "
        "and Daily Risk Adjusted Return deterioration. Catch trades enter at the "
        "next session open after the stress signal. Wait trades enter only after "
        "a later close clears SMA5 and avoids a fresh 3-session low, then use the "
        "next session open. All signal features are measured at or before each "
        "signal date close."
    )
    return FallingKnifeReversalStudyResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        market_codes=normalized_markets,
        forward_horizons=normalized_horizons,
        risk_adjusted_lookback=int(risk_adjusted_lookback),
        condition_ratio_type=condition_ratio_type,
        five_day_drop_threshold=float(five_day_drop_threshold),
        twenty_day_drop_threshold=float(twenty_day_drop_threshold),
        sixty_day_drawdown_threshold=float(sixty_day_drawdown_threshold),
        risk_adjusted_threshold=float(risk_adjusted_threshold),
        min_condition_count=int(min_condition_count),
        max_wait_days=int(max_wait_days),
        signal_cooldown_days=int(signal_cooldown_days),
        severe_loss_threshold=float(severe_loss_threshold),
        source_row_count=int(len(price_df)),
        event_count=int(len(event_df)),
        wait_candidate_count=wait_candidate_count,
        research_note=research_note,
        event_df=event_df,
        trade_summary_df=trade_summary_df,
        paired_delta_df=paired_delta_df,
        condition_profile_df=condition_profile_df,
    )


def write_falling_knife_reversal_study_bundle(
    result: FallingKnifeReversalStudyResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=FALLING_KNIFE_REVERSAL_STUDY_EXPERIMENT_ID,
        module=__name__,
        function="run_falling_knife_reversal_study",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "market_codes": result.market_codes,
            "forward_horizons": result.forward_horizons,
            "risk_adjusted_lookback": result.risk_adjusted_lookback,
            "condition_ratio_type": result.condition_ratio_type,
            "min_condition_count": result.min_condition_count,
            "max_wait_days": result.max_wait_days,
            "signal_cooldown_days": result.signal_cooldown_days,
        },
        result=result,
        table_field_names=(
            "event_df",
            "trade_summary_df",
            "paired_delta_df",
            "condition_profile_df",
        ),
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_falling_knife_reversal_study_bundle(
    bundle_path: str | Path,
) -> FallingKnifeReversalStudyResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=FallingKnifeReversalStudyResult,
        table_field_names=(
            "event_df",
            "trade_summary_df",
            "paired_delta_df",
            "condition_profile_df",
        ),
    )


def get_falling_knife_reversal_study_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        FALLING_KNIFE_REVERSAL_STUDY_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_falling_knife_reversal_study_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        FALLING_KNIFE_REVERSAL_STUDY_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_research_bundle_summary_markdown(
    result: FallingKnifeReversalStudyResult,
) -> str:
    top_trade_rows = _top_rows(result.trade_summary_df, limit=12)
    top_delta_rows = _top_rows(result.paired_delta_df, limit=8)
    return "\n".join(
        [
            "# Falling Knife Reversal Study",
            "",
            "## Snapshot",
            "",
            f"- Data source: `{result.source_detail}`",
            f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
            f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
            f"- Market codes: `{', '.join(result.market_codes)}`",
            f"- Source rows: `{result.source_row_count}`",
            f"- Falling-knife events: `{result.event_count}`",
            f"- Events with stabilization entry: `{result.wait_candidate_count}`",
            f"- Same-code signal cooldown: `{result.signal_cooldown_days}` sessions",
            f"- Daily Risk Adjusted Return: `{result.risk_adjusted_lookback}d {result.condition_ratio_type}` threshold `{_fmt_ratio(result.risk_adjusted_threshold)}`",
            "",
            "## Definition",
            "",
            f"- Minimum overlap count: `{result.min_condition_count}` across `5d <= {_fmt_pct(result.five_day_drop_threshold)}`, `20d <= {_fmt_pct(result.twenty_day_drop_threshold)}`, `60d drawdown <= {_fmt_pct(result.sixty_day_drawdown_threshold)}`, SMA downtrend, and poor Daily Risk Adjusted Return.",
            f"- Wait rule: first later signal within `{result.max_wait_days}` sessions where close > SMA5 and the session low does not break the prior 3-session low; entry is the next session open.",
            "",
            "## Trade Summary",
            "",
            *top_trade_rows,
            "",
            "## Wait Minus Catch",
            "",
            *top_delta_rows,
            "",
            "## Tables",
            "",
            "- `event_df`",
            "- `trade_summary_df`",
            "- `paired_delta_df`",
            "- `condition_profile_df`",
        ]
    )


def _top_rows(frame: pd.DataFrame, *, limit: int) -> list[str]:
    if frame.empty:
        return ["- No rows."]
    rows: list[str] = []
    for row in frame.head(limit).to_dict(orient="records"):
        parts = [f"`{key}`={value}" for key, value in row.items()]
        rows.append("- " + ", ".join(parts))
    return rows


def _build_published_summary_payload(
    result: FallingKnifeReversalStudyResult,
) -> dict[str, Any]:
    trade_summary = (
        result.trade_summary_df.head(20).to_dict(orient="records")
        if not result.trade_summary_df.empty
        else []
    )
    paired_delta = (
        result.paired_delta_df.head(20).to_dict(orient="records")
        if not result.paired_delta_df.empty
        else []
    )
    return {
        "experimentId": FALLING_KNIFE_REVERSAL_STUDY_EXPERIMENT_ID,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "eventCount": result.event_count,
        "waitCandidateCount": result.wait_candidate_count,
        "riskAdjustedLookback": result.risk_adjusted_lookback,
        "conditionRatioType": result.condition_ratio_type,
        "tradeSummary": trade_summary,
        "pairedDelta": paired_delta,
        "note": result.research_note,
    }
