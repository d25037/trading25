"""Turtle-like Donchian channel momentum research."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import duckdb
import numpy as np
import pandas as pd

from src.domains.analytics.annual_value_composite_selection import _daily_stats, _series_mean
from src.domains.analytics.research_core import (
    UNIVERSE_LABELS,
    build_market_universe_case_sql,
    research_universe_market_codes,
    sort_research_table,
    sql_string_list,
    warmup_start_date,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
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
from src.domains.analytics.topix_rank_future_close_core import _default_start_date

TURTLE_LIKE_MOMENTUM_RESEARCH_EXPERIMENT_ID = "market-behavior/turtle-like-momentum-research"

DEFAULT_LOOKBACK_YEARS = 10
DEFAULT_CHANNEL_SPECS: tuple[tuple[int, int], ...] = ((20, 10), (55, 20))
DEFAULT_ENTRY_MODES: tuple[str, ...] = ("close_confirmed", "high_touch_next_open")
DEFAULT_SIZING_METHODS: tuple[str, ...] = ("equal_weight", "inverse_atr")
DEFAULT_MIN_AVG_TRADING_VALUE_MIL_JPY = 10.0
DEFAULT_ATR_SESSIONS = 20
_WARMUP_MULTIPLIER = 2.4
_GROUP_COLUMNS: tuple[str, ...] = (
    "universe_key",
    "entry_window_sessions",
    "exit_window_sessions",
    "turtle_label",
    "entry_mode",
    "sizing_method",
)
TABLE_FIELD_NAMES: tuple[str, ...] = (
    "universe_summary_df",
    "trade_ledger_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
)
EntryMode = Literal["close_confirmed", "high_touch_next_open"]
SizingMethod = Literal["equal_weight", "inverse_atr"]


@dataclass(frozen=True)
class TurtleLikeMomentumResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    default_start_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    lookback_years: int
    channel_specs: tuple[tuple[int, int], ...]
    entry_modes: tuple[str, ...]
    sizing_methods: tuple[str, ...]
    atr_sessions: int
    min_avg_trading_value_mil_jpy: float
    execution_policy: str
    universe_summary_df: pd.DataFrame
    trade_ledger_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame


def _sort_table(df: pd.DataFrame) -> pd.DataFrame:
    return sort_research_table(
        df,
        sort_columns=(
            "entry_window_sessions",
            "exit_window_sessions",
            "entry_mode",
            "sizing_method",
            "entry_signal_date",
            "date",
            "code",
        ),
    )


def _normalize_channel_specs(
    values: tuple[tuple[int, int], ...] | list[tuple[int, int]] | None,
) -> tuple[tuple[int, int], ...]:
    raw_values = DEFAULT_CHANNEL_SPECS if values is None else tuple(values)
    normalized: list[tuple[int, int]] = []
    for raw_entry, raw_exit in raw_values:
        entry_window = int(raw_entry)
        exit_window = int(raw_exit)
        if entry_window <= 1:
            raise ValueError("entry channel window must be greater than 1")
        if exit_window <= 1:
            raise ValueError("exit channel window must be greater than 1")
        if exit_window >= entry_window:
            raise ValueError("exit channel window must be smaller than entry channel window")
        spec = (entry_window, exit_window)
        if spec not in normalized:
            normalized.append(spec)
    if not normalized:
        raise ValueError("at least one channel spec is required")
    return tuple(sorted(normalized))


def _normalize_entry_modes(values: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    raw_values = DEFAULT_ENTRY_MODES if values is None else tuple(values)
    allowed = set(DEFAULT_ENTRY_MODES)
    normalized: list[str] = []
    for value in raw_values:
        mode = str(value)
        if mode not in allowed:
            raise ValueError(f"unsupported entry mode: {mode}")
        if mode not in normalized:
            normalized.append(mode)
    if not normalized:
        raise ValueError("at least one entry mode is required")
    return tuple(normalized)


def _normalize_sizing_methods(values: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    raw_values = DEFAULT_SIZING_METHODS if values is None else tuple(values)
    allowed = set(DEFAULT_SIZING_METHODS)
    normalized: list[str] = []
    for value in raw_values:
        method = str(value)
        if method not in allowed:
            raise ValueError(f"unsupported sizing method: {method}")
        if method not in normalized:
            normalized.append(method)
    if not normalized:
        raise ValueError("at least one sizing method is required")
    return tuple(normalized)


def _warmup_start_date(
    analysis_start_date: str | None,
    available_start_date: str | None,
    *,
    channel_specs: tuple[tuple[int, int], ...],
    atr_sessions: int,
) -> str | None:
    max_window = max(max(entry, exit) for entry, exit in channel_specs)
    return warmup_start_date(
        analysis_start_date,
        available_start_date,
        warmup_sessions=max(max_window, atr_sessions, 60),
        session_to_calendar_multiplier=_WARMUP_MULTIPLIER,
    )


def _query_analysis_panel(
    conn: Any,
    *,
    raw_start_date: str | None,
    analysis_end_date: str | None,
    channel_specs: tuple[tuple[int, int], ...],
    atr_sessions: int,
) -> pd.DataFrame:
    price_code = normalize_code_sql("sd.code")
    master_code = normalize_code_sql("smd.code")
    all_market_codes = research_universe_market_codes()
    raw_conditions: list[str] = []
    raw_params: list[str] = []
    if raw_start_date is not None:
        raw_conditions.append("sd.date >= ?")
        raw_params.append(raw_start_date)
    if analysis_end_date is not None:
        raw_conditions.append("sd.date <= ?")
        raw_params.append(analysis_end_date)
    raw_where = "" if not raw_conditions else "WHERE " + " AND ".join(raw_conditions)
    entry_high_exprs = ",\n                ".join(
        f"max(high) over (partition by code order by date rows between {entry} preceding and 1 preceding) "
        f"as prior_high_{entry}d"
        for entry, _ in channel_specs
    )
    exit_low_exprs = ",\n                ".join(
        f"min(low) over (partition by code order by date rows between {exit} preceding and 1 preceding) "
        f"as prior_low_{exit}d"
        for _, exit in channel_specs
    )
    frame = conn.execute(
        f"""
        WITH raw_prices AS (
            SELECT
                {price_code} AS code,
                sd.date,
                sd.open,
                sd.high,
                sd.low,
                sd.close,
                sd.volume,
                row_number() OVER (
                    PARTITION BY {price_code}, sd.date
                    ORDER BY CASE WHEN length(sd.code) = 4 THEN 0 ELSE 1 END, sd.code
                ) AS row_rank
            FROM stock_data sd
            {raw_where}
        ),
        prices AS (
            SELECT code, date, open, high, low, close, volume
            FROM raw_prices
            WHERE row_rank = 1
              AND open > 0 AND high > 0 AND low > 0 AND close > 0
        ),
        master AS (
            SELECT
                {master_code} AS code,
                smd.date,
                smd.company_name,
                smd.market_code,
                smd.scale_category,
                {build_market_universe_case_sql(market_code_column="smd.market_code", scale_category_column="smd.scale_category")}
                    AS universe_key,
                row_number() OVER (
                    PARTITION BY {master_code}, smd.date
                    ORDER BY CASE WHEN length(smd.code) = 4 THEN 0 ELSE 1 END, smd.code
                ) AS row_rank
            FROM stock_master_daily smd
            WHERE smd.market_code IN ({sql_string_list(all_market_codes)})
        ),
        scoped AS (
            SELECT p.*, m.company_name, m.market_code, m.scale_category, m.universe_key
            FROM prices p
            JOIN master m ON m.code = p.code AND m.date = p.date AND m.row_rank = 1
            WHERE m.universe_key IS NOT NULL
        ),
        tr_base AS (
            SELECT
                *,
                greatest(
                    high - low,
                    abs(high - lag(close) over (partition by code order by date)),
                    abs(low - lag(close) over (partition by code order by date))
                ) AS true_range
            FROM scoped
        )
        SELECT
            *,
            lead(date, 1) over (partition by code order by date) AS next_date,
            lead(open, 1) over (partition by code order by date) AS next_open,
            lag(close, 1) over (partition by code order by date) AS prev_close,
            avg(volume * close) over (
                partition by code order by date
                rows between 60 preceding and 1 preceding
            ) / 1000000.0 AS avg_trading_value_60d_mil_jpy,
            avg(true_range) over (
                partition by code order by date
                rows between {atr_sessions} preceding and 1 preceding
            ) AS atr_{atr_sessions}d,
            {entry_high_exprs},
            {exit_low_exprs}
        FROM tr_base
        ORDER BY code, date
        """,
        raw_params,
    ).fetchdf()
    if frame.empty:
        return frame
    frame["date"] = frame["date"].astype(str)
    frame["universe_label"] = frame["universe_key"].map(UNIVERSE_LABELS)
    return frame


def _query_calendar_df(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    conditions: list[str] = []
    params: list[str] = []
    if start_date is not None:
        conditions.append("date >= ?")
        params.append(start_date)
    if end_date is not None:
        conditions.append("date <= ?")
        params.append(end_date)
    where = "" if not conditions else "WHERE " + " AND ".join(conditions)
    return conn.execute(
        f"""
        SELECT date
        FROM topix_data
        {where}
        ORDER BY date
        """,
        params,
    ).fetchdf()


def _build_universe_summary(panel_df: pd.DataFrame) -> pd.DataFrame:
    if panel_df.empty:
        return pd.DataFrame(
            columns=["universe_key", "stock_day_count", "unique_code_count", "analysis_date_count"]
        )
    frame = (
        panel_df.groupby("universe_key", observed=True)
        .agg(
            stock_day_count=("code", "size"),
            unique_code_count=("code", "nunique"),
            analysis_date_count=("date", "nunique"),
        )
        .reset_index()
    )
    frame["universe_label"] = frame["universe_key"].map(UNIVERSE_LABELS)
    return _sort_table(frame)


def _build_trade_ledger_df(
    panel_df: pd.DataFrame,
    *,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    channel_specs: tuple[tuple[int, int], ...],
    entry_modes: tuple[str, ...],
    min_avg_trading_value_mil_jpy: float,
    atr_sessions: int,
) -> pd.DataFrame:
    columns = [
        "universe_key",
        "universe_label",
        "entry_window_sessions",
        "exit_window_sessions",
        "turtle_label",
        "entry_mode",
        "entry_signal_date",
        "entry_date",
        "exit_signal_date",
        "exit_date",
        "exit_reason",
        "code",
        "company_name",
        "market_code",
        "scale_category",
        "entry_open",
        "exit_price",
        "event_return",
        "event_return_pct",
        "holding_sessions",
        "breakout_distance_pct",
        "avg_trading_value_60d_mil_jpy",
        "atr",
        "atr_pct",
    ]
    if panel_df.empty:
        return pd.DataFrame(columns=columns)
    start_ts = pd.Timestamp(analysis_start_date) if analysis_start_date is not None else None
    end_ts = pd.Timestamp(analysis_end_date) if analysis_end_date is not None else None
    rows: list[dict[str, Any]] = []
    for _, code_group in panel_df.groupby("code", sort=False):
        code_frame = code_group.sort_values("date", kind="stable").reset_index(drop=True)
        code_frame["date_ts"] = pd.to_datetime(code_frame["date"], errors="coerce")
        date_ts = pd.to_datetime(code_frame["date_ts"], errors="coerce")
        date_values = code_frame["date"].astype(str).to_numpy()
        next_dates = code_frame["next_date"].astype(str).to_numpy()
        close_values = pd.to_numeric(code_frame["close"], errors="coerce").to_numpy(dtype=float)
        high_values = pd.to_numeric(code_frame["high"], errors="coerce").to_numpy(dtype=float)
        low_values = pd.to_numeric(code_frame["low"], errors="coerce").to_numpy(dtype=float)
        next_open_values = pd.to_numeric(
            code_frame["next_open"],
            errors="coerce",
        ).to_numpy(dtype=float)
        adv_values = pd.to_numeric(
            code_frame["avg_trading_value_60d_mil_jpy"],
            errors="coerce",
        ).to_numpy(dtype=float)
        date_mask = pd.Series(True, index=code_frame.index)
        if start_ts is not None:
            date_mask &= date_ts >= start_ts
        if end_ts is not None:
            date_mask &= date_ts <= end_ts
        date_mask_array = date_mask.fillna(False).to_numpy(dtype=bool)
        valid_next_open = np.isfinite(next_open_values)
        valid_next_date = ~np.isin(next_dates, ["", "None", "nan", "NaT"])
        valid_trade_entry = date_mask_array & valid_next_open & valid_next_date
        if not valid_trade_entry.any():
            continue
        valid_until_end_indices = np.flatnonzero(date_mask_array)
        if len(valid_until_end_indices) == 0:
            continue
        last_valid_index = int(valid_until_end_indices[-1])
        for entry_window, exit_window in channel_specs:
            prior_high_col = f"prior_high_{entry_window}d"
            prior_low_col = f"prior_low_{exit_window}d"
            prior_high_values = pd.to_numeric(
                code_frame[prior_high_col],
                errors="coerce",
            ).to_numpy(dtype=float)
            prior_low_values = pd.to_numeric(
                code_frame[prior_low_col],
                errors="coerce",
            ).to_numpy(dtype=float)
            for entry_mode in entry_modes:
                breakout_values = close_values if entry_mode == "close_confirmed" else high_values
                entry_mask = (
                    valid_trade_entry
                    & np.isfinite(prior_high_values)
                    & (prior_high_values > 0)
                    & np.isfinite(adv_values)
                    & (adv_values >= min_avg_trading_value_mil_jpy)
                    & (breakout_values > prior_high_values)
                )
                exit_probe_values = close_values if entry_mode == "close_confirmed" else low_values
                exit_mask = (
                    date_mask_array
                    & valid_next_open
                    & valid_next_date
                    & np.isfinite(prior_low_values)
                    & (prior_low_values > 0)
                    & (exit_probe_values < prior_low_values)
                )
                entry_indices = np.flatnonzero(entry_mask)
                exit_indices = np.flatnonzero(exit_mask)
                cursor = 0
                for index in entry_indices:
                    if index < cursor:
                        continue
                    entry_date = str(next_dates[index])
                    entry_open = float(next_open_values[index])
                    if end_ts is not None and pd.Timestamp(entry_date) > end_ts:
                        break
                    exit_position = int(np.searchsorted(exit_indices, index + 1))
                    if exit_position < len(exit_indices):
                        exit_index = int(exit_indices[exit_position])
                        exit_date = str(next_dates[exit_index])
                        exit_price = float(next_open_values[exit_index])
                        exit_signal_date: str | None = str(date_values[exit_index])
                        exit_reason = "channel_exit_next_open"
                        if end_ts is not None and pd.Timestamp(exit_date) > end_ts:
                            exit_index = last_valid_index
                            exit_date = str(date_values[exit_index])
                            exit_price = float(close_values[exit_index])
                            exit_signal_date = None
                            exit_reason = "end_of_sample_close"
                    else:
                        exit_index = last_valid_index
                        if exit_index <= index or not math.isfinite(close_values[exit_index]):
                            continue
                        exit_date = str(date_values[exit_index])
                        exit_price = float(close_values[exit_index])
                        exit_signal_date = None
                        exit_reason = "end_of_sample_close"
                    if pd.Timestamp(exit_date) < pd.Timestamp(entry_date):
                        continue
                    holding_sessions = max(1, exit_index - index)
                    event_return = exit_price / entry_open - 1.0
                    row = code_frame.iloc[int(index)]
                    prior_high = float(prior_high_values[index])
                    breakout_price = float(breakout_values[index])
                    atr = _float_or_nan(row[f"atr_{atr_sessions}d"])
                    close = float(close_values[index])
                    rows.append(
                        {
                            "universe_key": str(row["universe_key"]),
                            "universe_label": str(row["universe_label"]),
                            "entry_window_sessions": int(entry_window),
                            "exit_window_sessions": int(exit_window),
                            "turtle_label": f"{entry_window}d_entry_{exit_window}d_exit",
                            "entry_mode": str(entry_mode),
                            "entry_signal_date": str(row["date"]),
                            "entry_date": entry_date,
                            "exit_signal_date": exit_signal_date,
                            "exit_date": exit_date,
                            "exit_reason": exit_reason,
                            "code": str(row["code"]),
                            "company_name": str(row["company_name"]),
                            "market_code": str(row["market_code"]),
                            "scale_category": str(row["scale_category"]),
                            "entry_open": entry_open,
                            "exit_price": exit_price,
                            "event_return": event_return,
                            "event_return_pct": event_return * 100.0,
                            "holding_sessions": int(holding_sessions),
                            "breakout_distance_pct": (
                                (breakout_price / prior_high - 1.0) * 100.0
                                if math.isfinite(breakout_price)
                                and math.isfinite(prior_high)
                                and prior_high > 0
                                else np.nan
                            ),
                            "avg_trading_value_60d_mil_jpy": _float_or_nan(
                                row["avg_trading_value_60d_mil_jpy"]
                            ),
                            "atr": atr,
                            "atr_pct": (atr / close * 100.0)
                            if math.isfinite(atr) and math.isfinite(close) and close > 0
                            else np.nan,
                        }
                    )
                    cursor = max(exit_index + 1, index + 1)
    if not rows:
        return pd.DataFrame(columns=columns)
    return _sort_table(pd.DataFrame(rows)[columns])


def _build_portfolio_event_df(
    trade_ledger_df: pd.DataFrame,
    *,
    sizing_methods: tuple[str, ...],
) -> pd.DataFrame:
    if trade_ledger_df.empty:
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for sizing_method in sizing_methods:
        frame = trade_ledger_df.copy()
        frame["sizing_method"] = sizing_method
        if sizing_method == "inverse_atr":
            atr_pct = pd.to_numeric(frame["atr_pct"], errors="coerce")
            frame["position_weight"] = np.where(atr_pct > 0, 1.0 / atr_pct, np.nan)
            frame = frame[pd.to_numeric(frame["position_weight"], errors="coerce") > 0].copy()
        else:
            frame["position_weight"] = 1.0
        frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _build_portfolio_daily_df(
    panel_df: pd.DataFrame,
    trade_ledger_df: pd.DataFrame,
    calendar_df: pd.DataFrame,
    *,
    sizing_methods: tuple[str, ...],
    analysis_start_date: str | None,
    analysis_end_date: str | None,
) -> pd.DataFrame:
    columns = [
        *_GROUP_COLUMNS,
        "date",
        "active_positions",
        "active_weight",
        "mean_daily_return",
        "mean_daily_return_pct",
        "portfolio_value",
        "drawdown_pct",
    ]
    if trade_ledger_df.empty or panel_df.empty or calendar_df.empty:
        return pd.DataFrame(columns=columns)
    event_df = _build_portfolio_event_df(trade_ledger_df, sizing_methods=sizing_methods)
    if event_df.empty:
        return pd.DataFrame(columns=columns)
    price_df = panel_df[["code", "date", "close", "prev_close"]].copy()
    conn = duckdb.connect()
    try:
        conn.register("turtle_events", event_df)
        conn.register("turtle_prices", price_df)
        daily = conn.execute(
            """
            WITH active AS (
                SELECT
                    e.universe_key,
                    e.entry_window_sessions,
                    e.exit_window_sessions,
                    e.turtle_label,
                    e.entry_mode,
                    e.sizing_method,
                    p.date,
                    e.position_weight,
                    CASE
                        WHEN p.date = e.entry_date AND p.date = e.exit_date
                            THEN e.exit_price / nullif(e.entry_open, 0) - 1
                        WHEN p.date = e.entry_date
                            THEN p.close / nullif(e.entry_open, 0) - 1
                        WHEN p.date = e.exit_date
                            THEN e.exit_price / nullif(p.prev_close, 0) - 1
                        ELSE p.close / nullif(p.prev_close, 0) - 1
                    END AS daily_return
                FROM turtle_events e
                JOIN turtle_prices p
                  ON p.code = e.code
                 AND p.date >= e.entry_date
                 AND p.date <= e.exit_date
                WHERE e.position_weight > 0
            )
            SELECT
                universe_key,
                entry_window_sessions,
                exit_window_sessions,
                turtle_label,
                entry_mode,
                sizing_method,
                date,
                count(*) AS active_positions,
                sum(position_weight) AS active_weight,
                sum(daily_return * position_weight) / nullif(sum(position_weight), 0) AS mean_daily_return
            FROM active
            WHERE daily_return IS NOT NULL
            GROUP BY
                universe_key,
                entry_window_sessions,
                exit_window_sessions,
                turtle_label,
                entry_mode,
                sizing_method,
                date
            """
        ).fetchdf()
    finally:
        conn.close()
    if daily.empty:
        return pd.DataFrame(columns=columns)
    daily["mean_daily_return_pct"] = pd.to_numeric(
        daily["mean_daily_return"],
        errors="coerce",
    ) * 100.0
    daily = _densify_daily(daily, calendar_df, analysis_start_date, analysis_end_date)
    daily["portfolio_value"] = np.nan
    daily["drawdown_pct"] = np.nan
    for _, group in daily.groupby(list(_GROUP_COLUMNS), observed=True, sort=False):
        idx = list(group.index)
        values = (1.0 + pd.to_numeric(daily.loc[idx, "mean_daily_return"])).cumprod()
        peaks = values.cummax()
        daily.loc[idx, "portfolio_value"] = values.to_numpy()
        daily.loc[idx, "drawdown_pct"] = ((values / peaks - 1.0) * 100.0).to_numpy()
    return _sort_table(daily[columns])


def _densify_daily(
    active_daily_df: pd.DataFrame,
    calendar_df: pd.DataFrame,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
) -> pd.DataFrame:
    configs = active_daily_df[list(_GROUP_COLUMNS)].drop_duplicates()
    calendar = calendar_df.copy()
    calendar["date"] = calendar["date"].astype(str)
    if analysis_start_date is not None:
        calendar = calendar[calendar["date"] >= analysis_start_date]
    if analysis_end_date is not None:
        calendar = calendar[calendar["date"] <= analysis_end_date]
    dense = configs.merge(calendar, how="cross")
    merged = dense.merge(active_daily_df, on=[*_GROUP_COLUMNS, "date"], how="left")
    merged["active_positions"] = merged["active_positions"].fillna(0).astype(int)
    merged["active_weight"] = merged["active_weight"].fillna(0.0)
    merged["mean_daily_return"] = merged["mean_daily_return"].fillna(0.0)
    merged["mean_daily_return_pct"] = merged["mean_daily_return_pct"].fillna(0.0)
    return merged


def _build_portfolio_summary_df(
    portfolio_daily_df: pd.DataFrame,
    trade_ledger_df: pd.DataFrame,
    *,
    sizing_methods: tuple[str, ...],
) -> pd.DataFrame:
    columns = [
        *_GROUP_COLUMNS,
        "universe_label",
        "trade_count",
        "unique_code_count",
        "win_rate_pct",
        "mean_trade_return_pct",
        "median_trade_return_pct",
        "p10_trade_return_pct",
        "p90_trade_return_pct",
        "skew_trade_return",
        "avg_holding_sessions",
        "active_days",
        "avg_active_positions",
        "max_active_positions",
        "avg_active_weight",
        "total_return_pct",
        "cagr_pct",
        "max_drawdown_pct",
        "annualized_volatility_pct",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
    ]
    if portfolio_daily_df.empty:
        return pd.DataFrame(columns=columns)
    event_df = _build_portfolio_event_df(trade_ledger_df, sizing_methods=sizing_methods)
    event_stats = {tuple(keys): group for keys, group in event_df.groupby(list(_GROUP_COLUMNS), sort=False)}
    records: list[dict[str, Any]] = []
    for keys, group in portfolio_daily_df.groupby(list(_GROUP_COLUMNS), sort=False):
        event_group = event_stats.get(tuple(keys), pd.DataFrame())
        trade_return_source = (
            event_group["event_return_pct"]
            if "event_return_pct" in event_group.columns
            else pd.Series(dtype="float64")
        )
        trade_returns = pd.to_numeric(trade_return_source, errors="coerce").dropna()
        start_date = str(group["date"].iloc[0])
        end_date = str(group["date"].iloc[-1])
        total_return = float(group["portfolio_value"].iloc[-1] - 1.0)
        period_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days
        cagr = None
        if period_days > 0 and total_return > -1.0:
            cagr_value = (1.0 + total_return) ** (365.25 / period_days) - 1.0
            cagr = float(cagr_value) if math.isfinite(cagr_value) else None
        drawdown = pd.to_numeric(group["drawdown_pct"], errors="coerce").min()
        max_drawdown_pct = float(drawdown) if pd.notna(drawdown) else None
        records.append(
            {
                **dict(zip(_GROUP_COLUMNS, keys, strict=True)),
                "universe_label": UNIVERSE_LABELS.get(str(keys[0]), str(keys[0])),
                "trade_count": int(len(event_group)),
                "unique_code_count": int(event_group["code"].nunique()) if not event_group.empty else 0,
                "win_rate_pct": float((trade_returns > 0).mean() * 100.0)
                if not trade_returns.empty
                else None,
                "mean_trade_return_pct": float(trade_returns.mean())
                if not trade_returns.empty
                else None,
                "median_trade_return_pct": float(trade_returns.median())
                if not trade_returns.empty
                else None,
                "p10_trade_return_pct": float(trade_returns.quantile(0.10))
                if not trade_returns.empty
                else None,
                "p90_trade_return_pct": float(trade_returns.quantile(0.90))
                if not trade_returns.empty
                else None,
                "skew_trade_return": _float_or_nan(trade_returns.skew())
                if len(trade_returns) >= 3
                else None,
                "avg_holding_sessions": _series_mean(event_group["holding_sessions"])
                if not event_group.empty
                else None,
                "active_days": int((pd.to_numeric(group["active_positions"]) > 0).sum()),
                "avg_active_positions": _series_mean(group["active_positions"]),
                "max_active_positions": int(pd.to_numeric(group["active_positions"]).max()),
                "avg_active_weight": _series_mean(group["active_weight"]),
                "total_return_pct": total_return * 100.0,
                "cagr_pct": cagr * 100.0 if cagr is not None else None,
                "max_drawdown_pct": max_drawdown_pct,
                **_daily_stats(group["mean_daily_return"]),
                "calmar_ratio": (
                    cagr / abs(max_drawdown_pct / 100.0)
                    if cagr is not None and max_drawdown_pct is not None and max_drawdown_pct < -1e-12
                    else None
                ),
            }
        )
    return _sort_table(pd.DataFrame(records)[columns])


def run_turtle_like_momentum_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    channel_specs: tuple[tuple[int, int], ...] | list[tuple[int, int]] | None = None,
    entry_modes: tuple[str, ...] | list[str] | None = None,
    sizing_methods: tuple[str, ...] | list[str] | None = None,
    atr_sessions: int = DEFAULT_ATR_SESSIONS,
    min_avg_trading_value_mil_jpy: float = DEFAULT_MIN_AVG_TRADING_VALUE_MIL_JPY,
) -> TurtleLikeMomentumResearchResult:
    normalized_specs = _normalize_channel_specs(channel_specs)
    normalized_entry_modes = _normalize_entry_modes(entry_modes)
    normalized_sizing_methods = _normalize_sizing_methods(sizing_methods)
    if atr_sessions <= 1:
        raise ValueError("atr_sessions must be greater than 1")
    if min_avg_trading_value_mil_jpy < 0:
        raise ValueError("min_avg_trading_value_mil_jpy must be non-negative")
    with open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="turtle-like-momentum-",
    ) as ctx:
        conn = ctx.connection
        available_start_date, available_end_date = fetch_date_range(
            conn,
            table_name="stock_data",
        )
        default_start_date = _default_start_date(
            available_start_date=available_start_date,
            available_end_date=available_end_date,
            lookback_years=lookback_years,
        )
        analysis_start_date = start_date or default_start_date
        analysis_end_date = end_date or available_end_date
        raw_start_date = _warmup_start_date(
            analysis_start_date,
            available_start_date,
            channel_specs=normalized_specs,
            atr_sessions=atr_sessions,
        )
        panel_df = _query_analysis_panel(
            conn,
            raw_start_date=raw_start_date,
            analysis_end_date=analysis_end_date,
            channel_specs=normalized_specs,
            atr_sessions=atr_sessions,
        )
        calendar_df = _query_calendar_df(
            conn,
            start_date=analysis_start_date,
            end_date=analysis_end_date,
        )
        universe_summary_df = _build_universe_summary(panel_df)
        trade_ledger_df = _build_trade_ledger_df(
            panel_df,
            analysis_start_date=analysis_start_date,
            analysis_end_date=analysis_end_date,
            channel_specs=normalized_specs,
            entry_modes=normalized_entry_modes,
            min_avg_trading_value_mil_jpy=float(min_avg_trading_value_mil_jpy),
            atr_sessions=atr_sessions,
        )
        portfolio_daily_df = _build_portfolio_daily_df(
            panel_df,
            trade_ledger_df,
            calendar_df,
            sizing_methods=normalized_sizing_methods,
            analysis_start_date=analysis_start_date,
            analysis_end_date=analysis_end_date,
        )
        portfolio_summary_df = _build_portfolio_summary_df(
            portfolio_daily_df,
            trade_ledger_df,
            sizing_methods=normalized_sizing_methods,
        )
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail
    return TurtleLikeMomentumResearchResult(
        db_path=str(db_path),
        source_mode=source_mode,
        source_detail=source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        default_start_date=default_start_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        lookback_years=lookback_years,
        channel_specs=normalized_specs,
        entry_modes=normalized_entry_modes,
        sizing_methods=normalized_sizing_methods,
        atr_sessions=int(atr_sessions),
        min_avg_trading_value_mil_jpy=float(min_avg_trading_value_mil_jpy),
        execution_policy=(
            "daily approximation: close_confirmed uses close > prior entry-channel high, "
            "high_touch_next_open uses high > prior entry-channel high, both enter next open; "
            "exit uses close < prior exit-channel low for close_confirmed and low < prior "
            "exit-channel low for high_touch_next_open, then exits next open; no pyramiding"
        ),
        universe_summary_df=universe_summary_df,
        trade_ledger_df=trade_ledger_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_summary_df=portfolio_summary_df,
    )


def _float_or_nan(value: object) -> float:
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return float("nan")
    return number if math.isfinite(number) else float("nan")


def _format_int(value: object) -> str:
    try:
        number = int(float(cast(float, value)))
    except (TypeError, ValueError):
        return "-"
    return f"{number:,}"


def _format_number(value: object, *, digits: int = 2, suffix: str = "") -> str:
    try:
        number = float(cast(float, value))
    except (TypeError, ValueError):
        return "-"
    if pd.isna(number):
        return "-"
    return f"{number:.{digits}f}{suffix}"


def _build_research_bundle_summary_markdown(result: TurtleLikeMomentumResearchResult) -> str:
    lines = [
        "# Turtle-like Momentum Research",
        "",
        "## Parameters",
        "",
        f"- Analysis period: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Channel specs: `{', '.join(f'{entry}:{exit}' for entry, exit in result.channel_specs)}`",
        f"- Entry modes: `{', '.join(result.entry_modes)}`",
        f"- Sizing methods: `{', '.join(result.sizing_methods)}`",
        f"- ATR sessions: `{result.atr_sessions}`",
        f"- Minimum ADV60: `{result.min_avg_trading_value_mil_jpy:.1f}mn JPY`",
        f"- Execution policy: {result.execution_policy}.",
        f"- Source: `{result.source_detail}`",
        "",
        "## Top Portfolio Rows",
        "",
    ]
    if result.portfolio_summary_df.empty:
        lines.append("_No portfolio rows._")
    else:
        rows = result.portfolio_summary_df.sort_values(
            ["sharpe_ratio", "cagr_pct", "trade_count"],
            ascending=[False, False, False],
            na_position="last",
            kind="stable",
        ).head(24)
        lines.extend(
            [
                "| Universe | Turtle | Entry | Sizing | Trades | CAGR | Sharpe | MaxDD | Win | P90 | Skew |",
                "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in rows.itertuples(index=False):
            lines.append(
                "| "
                + " | ".join(
                    [
                        f"`{row.universe_key}`",
                        f"`{row.turtle_label}`",
                        f"`{row.entry_mode}`",
                        f"`{row.sizing_method}`",
                        _format_int(row.trade_count),
                        _format_number(row.cagr_pct, suffix="%"),
                        _format_number(row.sharpe_ratio),
                        _format_number(row.max_drawdown_pct, suffix="%"),
                        _format_number(row.win_rate_pct, suffix="%"),
                        _format_number(row.p90_trade_return_pct, suffix="%"),
                        _format_number(row.skew_trade_return),
                    ]
                )
                + " |"
            )
    return "\n".join(lines) + "\n"


def write_turtle_like_momentum_research_bundle(
    result: TurtleLikeMomentumResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TURTLE_LIKE_MOMENTUM_RESEARCH_EXPERIMENT_ID,
        module="src.domains.analytics.turtle_like_momentum_research",
        function="run_turtle_like_momentum_research",
        params={
            "lookback_years": result.lookback_years,
            "channel_specs": [list(spec) for spec in result.channel_specs],
            "entry_modes": list(result.entry_modes),
            "sizing_methods": list(result.sizing_methods),
            "atr_sessions": result.atr_sessions,
            "min_avg_trading_value_mil_jpy": result.min_avg_trading_value_mil_jpy,
        },
        result=result,
        table_field_names=TABLE_FIELD_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_turtle_like_momentum_research_bundle(
    bundle_path: str | Path,
) -> TurtleLikeMomentumResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=TurtleLikeMomentumResearchResult,
        table_field_names=TABLE_FIELD_NAMES,
    )


def get_turtle_like_momentum_research_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TURTLE_LIKE_MOMENTUM_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_turtle_like_momentum_research_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TURTLE_LIKE_MOMENTUM_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


__all__: Sequence[str] = (
    "DEFAULT_CHANNEL_SPECS",
    "DEFAULT_ENTRY_MODES",
    "DEFAULT_SIZING_METHODS",
    "TURTLE_LIKE_MOMENTUM_RESEARCH_EXPERIMENT_ID",
    "TurtleLikeMomentumResearchResult",
    "get_turtle_like_momentum_research_bundle_path_for_run_id",
    "get_turtle_like_momentum_research_latest_bundle_path",
    "load_turtle_like_momentum_research_bundle",
    "run_turtle_like_momentum_research",
    "write_turtle_like_momentum_research_bundle",
)
