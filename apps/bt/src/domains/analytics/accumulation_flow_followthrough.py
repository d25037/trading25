"""Accumulation-flow follow-through research.

This study evaluates whether CMF / Chaikin / OBV based accumulation pressure is
followed by positive next-open-to-future-close returns, with extra branches for
"not yet extended" price state and lower-wick absorption.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pandas as pd

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
from src.domains.strategy.indicators import (
    compute_chaikin_oscillator,
    compute_chaikin_money_flow,
    compute_on_balance_volume_score,
)

UniverseKey = Literal["topix500", "prime_ex_topix500", "standard", "growth"]
FilterKey = Literal[
    "accumulation",
    "not_extended",
    "not_extended_lower_wick",
]

ACCUMULATION_FLOW_FOLLOWTHROUGH_EXPERIMENT_ID = (
    "market-behavior/accumulation-flow-followthrough"
)
UNIVERSE_ORDER: tuple[UniverseKey, ...] = (
    "topix500",
    "prime_ex_topix500",
    "standard",
    "growth",
)
UNIVERSE_LABEL_MAP: dict[UniverseKey, str] = {
    "topix500": "TOPIX500",
    "prime_ex_topix500": "PRIME ex TOPIX500",
    "standard": "Standard",
    "growth": "Growth",
}
FILTER_ORDER: tuple[FilterKey, ...] = (
    "accumulation",
    "not_extended",
    "not_extended_lower_wick",
)
FILTER_LABEL_MAP: dict[FilterKey, str] = {
    "accumulation": "Accumulation pressure",
    "not_extended": "Accumulation + not extended",
    "not_extended_lower_wick": "Accumulation + not extended + lower wick",
}
DEFAULT_LOOKBACK_YEARS = 10
DEFAULT_HORIZONS: tuple[int, ...] = (5, 10, 20, 60)
DEFAULT_CMF_PERIOD = 20
DEFAULT_CHAIKIN_FAST_PERIOD = 3
DEFAULT_CHAIKIN_SLOW_PERIOD = 10
DEFAULT_OBV_LOOKBACK_PERIOD = 20
DEFAULT_CMF_THRESHOLD = 0.05
DEFAULT_CHAIKIN_OSCILLATOR_THRESHOLD = 0.0
DEFAULT_OBV_SCORE_THRESHOLD = 0.05
DEFAULT_MIN_VOTES = 2
DEFAULT_PRICE_SMA_PERIOD = 50
DEFAULT_PRICE_HIGH_LOOKBACK_PERIOD = 60
DEFAULT_MAX_CLOSE_TO_SMA = 0.05
DEFAULT_MAX_CLOSE_TO_HIGH = -0.03
DEFAULT_LOWER_WICK_THRESHOLD = 0.40
DEFAULT_CONCENTRATION_CAPS: tuple[int, ...] = (10, 25, 50)
TOPIX500_SCALE_CATEGORIES: tuple[str, ...] = (
    "TOPIX Core30",
    "TOPIX Large70",
    "TOPIX Mid400",
)
PRIME_MARKET_CODES: tuple[str, ...] = ("0111", "prime")
STANDARD_MARKET_CODES: tuple[str, ...] = ("0112", "standard")
GROWTH_MARKET_CODES: tuple[str, ...] = ("0113", "growth")
MEMBERSHIP_MODE = "latest_market_code_scale_category_proxy"
TABLE_FIELD_NAMES: tuple[str, ...] = (
    "universe_summary_df",
    "event_df",
    "event_summary_df",
    "yearly_summary_df",
    "entry_cohort_df",
    "cohort_portfolio_summary_df",
    "yearly_cohort_summary_df",
    "capped_entry_cohort_df",
    "capped_cohort_portfolio_summary_df",
    "oos_portfolio_summary_df",
)

OOS_PERIODS: tuple[tuple[str, str, int | None, int | None], ...] = (
    ("discovery_2016_2020", "Discovery 2016-2020", None, 2020),
    ("validation_2021_2023", "Validation 2021-2023", 2021, 2023),
    ("oos_2024_forward", "OOS 2024-forward", 2024, None),
)


@dataclass(frozen=True)
class AccumulationFlowFollowthroughResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    default_start_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    lookback_years: int
    horizons: tuple[int, ...]
    cmf_period: int
    chaikin_fast_period: int
    chaikin_slow_period: int
    obv_lookback_period: int
    cmf_threshold: float
    chaikin_oscillator_threshold: float
    obv_score_threshold: float
    min_votes: int
    price_sma_period: int
    price_high_lookback_period: int
    max_close_to_sma: float
    max_close_to_high: float
    lower_wick_threshold: float
    concentration_caps: tuple[int, ...]
    membership_mode: str
    execution_note: str
    feature_note: str
    universe_summary_df: pd.DataFrame
    event_df: pd.DataFrame
    event_summary_df: pd.DataFrame
    yearly_summary_df: pd.DataFrame
    entry_cohort_df: pd.DataFrame
    cohort_portfolio_summary_df: pd.DataFrame
    yearly_cohort_summary_df: pd.DataFrame
    capped_entry_cohort_df: pd.DataFrame
    capped_cohort_portfolio_summary_df: pd.DataFrame
    oos_portfolio_summary_df: pd.DataFrame


def _empty_universe_summary_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "universe_key",
            "universe_label",
            "membership_mode",
            "stock_count",
            "analysis_stock_count",
            "stock_day_count",
            "analysis_date_count",
            "available_start_date",
            "available_end_date",
            "analysis_start_date",
            "analysis_end_date",
            "accumulation_event_count",
            "not_extended_event_count",
            "not_extended_lower_wick_event_count",
        ]
    )


def _empty_event_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "date",
            "entry_date",
            "calendar_year",
            "universe_key",
            "universe_label",
            "filter_key",
            "filter_label",
            "code",
            "company_name",
            "close",
            "next_open",
            "cmf",
            "chaikin_oscillator",
            "obv_flow_score",
            "accumulation_vote_count",
            "close_to_sma",
            "close_to_high",
            "lower_wick_ratio",
            "price_not_extended",
            "lower_wick_absorption",
        ]
    )


def _empty_event_summary_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "universe_key",
            "universe_label",
            "filter_key",
            "filter_label",
            "horizon_days",
            "event_count",
            "unique_code_count",
            "signal_date_count",
            "mean_return",
            "median_return",
            "win_rate",
            "mean_topix_return",
            "median_topix_return",
            "mean_excess_return",
            "median_excess_return",
            "excess_win_rate",
            "first_event_date",
            "last_event_date",
        ]
    )


def _empty_yearly_summary_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "calendar_year",
            "universe_key",
            "universe_label",
            "filter_key",
            "filter_label",
            "horizon_days",
            "event_count",
            "unique_code_count",
            "signal_date_count",
            "mean_return",
            "median_return",
            "win_rate",
            "mean_topix_return",
            "median_topix_return",
            "mean_excess_return",
            "median_excess_return",
            "excess_win_rate",
        ]
    )


def _empty_entry_cohort_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "entry_date",
            "calendar_year",
            "universe_key",
            "universe_label",
            "filter_key",
            "filter_label",
            "horizon_days",
            "cohort_event_count",
            "cohort_unique_code_count",
            "equal_weight_return",
            "topix_return",
            "excess_equal_weight_return",
        ]
    )


def _empty_cohort_portfolio_summary_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "universe_key",
            "universe_label",
            "filter_key",
            "filter_label",
            "horizon_days",
            "date_count",
            "total_signal_count",
            "avg_names_per_date",
            "median_names_per_date",
            "mean_cohort_return",
            "median_cohort_return",
            "win_rate",
            "loss_rate",
            "mean_topix_return",
            "median_topix_return",
            "mean_excess_return",
            "median_excess_return",
            "excess_win_rate",
            "excess_loss_rate",
            "best_cohort_return",
            "worst_cohort_return",
            "best_excess_return",
            "worst_excess_return",
            "first_entry_date",
            "last_entry_date",
        ]
    )


def _empty_yearly_cohort_summary_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "calendar_year",
            "universe_key",
            "universe_label",
            "filter_key",
            "filter_label",
            "horizon_days",
            "date_count",
            "total_signal_count",
            "avg_names_per_date",
            "mean_cohort_return",
            "median_cohort_return",
            "win_rate",
            "mean_topix_return",
            "median_topix_return",
            "mean_excess_return",
            "median_excess_return",
            "excess_win_rate",
        ]
    )


def _empty_capped_entry_cohort_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "entry_date",
            "calendar_year",
            "universe_key",
            "universe_label",
            "filter_key",
            "filter_label",
            "horizon_days",
            "max_names_per_date",
            "cohort_event_count",
            "cohort_unique_code_count",
            "equal_weight_return",
            "topix_return",
            "excess_equal_weight_return",
        ]
    )


def _empty_capped_cohort_portfolio_summary_df() -> pd.DataFrame:
    df = _empty_cohort_portfolio_summary_df()
    df.insert(5, "max_names_per_date", pd.Series(dtype=int))
    return df


def _empty_oos_portfolio_summary_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "sample_key",
            "sample_label",
            "portfolio_variant",
            "max_names_per_date",
            "universe_key",
            "universe_label",
            "filter_key",
            "filter_label",
            "horizon_days",
            "date_count",
            "total_signal_count",
            "avg_names_per_date",
            "mean_cohort_return",
            "median_cohort_return",
            "win_rate",
            "mean_topix_return",
            "median_topix_return",
            "mean_excess_return",
            "median_excess_return",
            "excess_win_rate",
        ]
    )


def _normalize_int_sequence(
    values: tuple[int, ...] | list[int] | None,
    *,
    fallback: tuple[int, ...],
    name: str,
) -> tuple[int, ...]:
    if values is None:
        return fallback
    normalized = tuple(sorted(dict.fromkeys(int(value) for value in values if int(value) > 0)))
    if not normalized:
        raise ValueError(f"{name} must contain at least one positive integer")
    return normalized


def _normalize_optional_int_sequence(
    values: tuple[int, ...] | list[int] | None,
    *,
    fallback: tuple[int, ...],
    name: str,
) -> tuple[int, ...]:
    normalized = _normalize_int_sequence(values, fallback=fallback, name=name)
    return tuple(value for value in normalized if value > 0)


def _universe_case_sql() -> str:
    return """
        CASE
            WHEN coalesce(scale_category, '') IN (?, ?, ?) THEN 'topix500'
            WHEN lower(coalesce(market_code, '')) IN (?, ?) THEN 'prime_ex_topix500'
            WHEN lower(coalesce(market_code, '')) IN (?, ?) THEN 'standard'
            WHEN lower(coalesce(market_code, '')) IN (?, ?) THEN 'growth'
            ELSE NULL
        END
    """


def _universe_case_params() -> list[str]:
    return [
        *TOPIX500_SCALE_CATEGORIES,
        *PRIME_MARKET_CODES,
        *STANDARD_MARKET_CODES,
        *GROWTH_MARKET_CODES,
    ]


def _query_universe_codes(
    conn: Any,
    *,
    universe_key: UniverseKey,
) -> pd.DataFrame:
    normalized_code_sql = normalize_code_sql("code")
    sql = f"""
        WITH latest_universe_raw AS (
            SELECT
                {normalized_code_sql} AS normalized_code,
                coalesce(company_name, code) AS company_name,
                lower(coalesce(market_code, '')) AS market_code,
                coalesce(scale_category, '') AS scale_category,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stocks
        ),
        classified_universe AS (
            SELECT
                normalized_code AS code,
                company_name,
                {_universe_case_sql()} AS universe_key
            FROM latest_universe_raw
            WHERE row_priority = 1
        )
        SELECT code, company_name, universe_key
        FROM classified_universe
        WHERE universe_key = ?
        ORDER BY code
    """
    return cast(
        pd.DataFrame,
        conn.execute(sql, [*_universe_case_params(), universe_key]).fetchdf(),
    )


def _query_universe_stock_history(
    conn: Any,
    *,
    universe_key: UniverseKey,
    end_date: str | None,
) -> pd.DataFrame:
    normalized_code_sql = normalize_code_sql("code")
    params: list[Any] = [*_universe_case_params()]
    date_filter_sql = ""
    if end_date is not None:
        date_filter_sql = " AND date <= ?"
        params.append(end_date)
    params.append(universe_key)

    sql = f"""
        WITH latest_universe_raw AS (
            SELECT
                {normalized_code_sql} AS normalized_code,
                coalesce(company_name, code) AS company_name,
                lower(coalesce(market_code, '')) AS market_code,
                coalesce(scale_category, '') AS scale_category,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stocks
        ),
        classified_universe AS (
            SELECT
                normalized_code,
                company_name,
                {_universe_case_sql()} AS universe_key
            FROM latest_universe_raw
            WHERE row_priority = 1
        ),
        stock_rows_raw AS (
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
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stock_data
            WHERE open IS NOT NULL
              AND open > 0
              AND high IS NOT NULL
              AND low IS NOT NULL
              AND close IS NOT NULL
              AND close > 0
              AND volume IS NOT NULL
              AND volume > 0
              {date_filter_sql}
        ),
        stock_rows AS (
            SELECT date, normalized_code, open, high, low, close, volume
            FROM stock_rows_raw
            WHERE row_priority = 1
        )
        SELECT
            u.universe_key,
            u.normalized_code AS code,
            u.company_name,
            s.date,
            s.open,
            s.high,
            s.low,
            s.close,
            s.volume
        FROM stock_rows s
        JOIN classified_universe u
          ON u.normalized_code = s.normalized_code
        WHERE u.universe_key = ?
        ORDER BY u.normalized_code, s.date
    """
    return cast(pd.DataFrame, conn.execute(sql, params).fetchdf())


def _query_topix_history(
    conn: Any,
    *,
    end_date: str | None,
) -> pd.DataFrame:
    params: list[Any] = []
    date_filter_sql = ""
    if end_date is not None:
        date_filter_sql = "WHERE date <= ?"
        params.append(end_date)
    sql = f"""
        SELECT
            date,
            CAST(open AS DOUBLE) AS open,
            CAST(close AS DOUBLE) AS close
        FROM topix_data
        {date_filter_sql}
        ORDER BY date
    """
    return cast(pd.DataFrame, conn.execute(sql, params).fetchdf())


def _prepare_topix_return_panel(
    topix_df: pd.DataFrame,
    *,
    horizons: tuple[int, ...],
) -> pd.DataFrame:
    columns = ["date", "entry_date"]
    for horizon_days in horizons:
        columns.append(f"topix_next_open_to_close_{horizon_days}d_return")
    if topix_df.empty:
        return pd.DataFrame(columns=columns)

    benchmark_df = topix_df.copy()
    benchmark_df["date"] = pd.to_datetime(benchmark_df["date"])
    for column in ["open", "close"]:
        benchmark_df[column] = pd.to_numeric(benchmark_df[column], errors="coerce")
    benchmark_df = benchmark_df.dropna(subset=["open", "close"]).copy()
    benchmark_df = benchmark_df.loc[
        (benchmark_df["open"] > 0) & (benchmark_df["close"] > 0)
    ].copy()
    benchmark_df = benchmark_df.sort_values("date", kind="stable").reset_index(drop=True)
    if benchmark_df.empty:
        return pd.DataFrame(columns=columns)

    benchmark_df["entry_date"] = benchmark_df["date"].shift(-1)
    next_open = benchmark_df["open"].shift(-1)
    for horizon_days in horizons:
        return_column = f"topix_next_open_to_close_{horizon_days}d_return"
        future_close = benchmark_df["close"].shift(-horizon_days)
        benchmark_df[return_column] = future_close / next_open - 1.0
        benchmark_df[return_column] = benchmark_df[return_column].where(
            np.isfinite(benchmark_df[return_column])
        )

    benchmark_df["date"] = benchmark_df["date"].dt.strftime("%Y-%m-%d")
    benchmark_df["entry_date"] = pd.to_datetime(benchmark_df["entry_date"]).dt.strftime(
        "%Y-%m-%d"
    )
    return benchmark_df[columns]


def _attach_benchmark_returns(
    event_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
    *,
    horizons: tuple[int, ...],
) -> pd.DataFrame:
    if event_df.empty:
        enriched_df = event_df.copy()
        for horizon_days in horizons:
            enriched_df[f"topix_next_open_to_close_{horizon_days}d_return"] = pd.Series(
                dtype=float
            )
            enriched_df[f"excess_next_open_to_close_{horizon_days}d_return"] = pd.Series(
                dtype=float
            )
        return enriched_df

    enriched_df = event_df.merge(
        benchmark_df,
        on=["date", "entry_date"],
        how="left",
        validate="many_to_one",
    )
    for horizon_days in horizons:
        return_column = f"next_open_to_close_{horizon_days}d_return"
        topix_column = f"topix_next_open_to_close_{horizon_days}d_return"
        excess_column = f"excess_next_open_to_close_{horizon_days}d_return"
        enriched_df[excess_column] = (
            pd.to_numeric(enriched_df[return_column], errors="coerce")
            - pd.to_numeric(enriched_df[topix_column], errors="coerce")
        )
    return enriched_df


def _safe_optional_date(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def _prepare_accumulation_panel(
    raw_df: pd.DataFrame,
    *,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    horizons: tuple[int, ...],
    cmf_period: int,
    chaikin_fast_period: int,
    chaikin_slow_period: int,
    obv_lookback_period: int,
    cmf_threshold: float,
    chaikin_oscillator_threshold: float,
    obv_score_threshold: float,
    min_votes: int,
    price_sma_period: int,
    price_high_lookback_period: int,
    max_close_to_sma: float,
    max_close_to_high: float,
    lower_wick_threshold: float,
) -> pd.DataFrame:
    if raw_df.empty:
        return raw_df.copy()
    if not 1 <= min_votes <= 3:
        raise ValueError("min_votes must be between 1 and 3")

    panel_df = raw_df.copy()
    panel_df["date"] = pd.to_datetime(panel_df["date"])
    for column in ["open", "high", "low", "close", "volume"]:
        panel_df[column] = pd.to_numeric(panel_df[column], errors="coerce")
    panel_df = panel_df.dropna(
        subset=["open", "high", "low", "close", "volume"]
    ).copy()
    panel_df = panel_df.loc[
        (panel_df["open"] > 0)
        & (panel_df["close"] > 0)
        & (panel_df["volume"] > 0)
        & (panel_df["high"] >= panel_df["low"])
    ].copy()
    panel_df = panel_df.sort_values(["code", "date"], kind="stable").reset_index(drop=True)
    if panel_df.empty:
        return panel_df

    prepared_frames: list[pd.DataFrame] = []
    for _, code_df in panel_df.groupby("code", sort=False):
        scoped_df = code_df.copy()
        high = scoped_df["high"]
        low = scoped_df["low"]
        close = scoped_df["close"]
        volume = scoped_df["volume"]
        scoped_df["cmf"] = compute_chaikin_money_flow(
            high,
            low,
            close,
            volume,
            period=cmf_period,
        )
        scoped_df["chaikin_oscillator"] = compute_chaikin_oscillator(
            high,
            low,
            close,
            volume,
            fast_period=chaikin_fast_period,
            slow_period=chaikin_slow_period,
        )
        scoped_df["obv_flow_score"] = compute_on_balance_volume_score(
            close,
            volume,
            lookback_period=obv_lookback_period,
        )
        scoped_df["price_sma"] = close.rolling(
            window=price_sma_period,
            min_periods=price_sma_period,
        ).mean()
        scoped_df["rolling_high"] = close.rolling(
            window=price_high_lookback_period,
            min_periods=price_high_lookback_period,
        ).max()
        scoped_df["next_open"] = scoped_df["open"].shift(-1)
        scoped_df["entry_date"] = scoped_df["date"].shift(-1)
        for horizon_days in horizons:
            future_close = scoped_df["close"].shift(-horizon_days)
            return_col = f"next_open_to_close_{horizon_days}d_return"
            scoped_df[return_col] = future_close / scoped_df["next_open"] - 1.0
            scoped_df[return_col] = scoped_df[return_col].where(
                np.isfinite(scoped_df[return_col])
            )
        prepared_frames.append(scoped_df)

    panel_df = pd.concat(prepared_frames, ignore_index=True)
    panel_df["close_to_sma"] = (
        panel_df["close"] / panel_df["price_sma"].replace(0, np.nan) - 1.0
    ).where(np.isfinite(panel_df["close"] / panel_df["price_sma"].replace(0, np.nan) - 1.0))
    panel_df["close_to_high"] = (
        panel_df["close"] / panel_df["rolling_high"].replace(0, np.nan) - 1.0
    ).where(
        np.isfinite(panel_df["close"] / panel_df["rolling_high"].replace(0, np.nan) - 1.0)
    )
    bar_range = panel_df["high"] - panel_df["low"]
    lower_wick = np.minimum(panel_df["open"], panel_df["close"]) - panel_df["low"]
    panel_df["lower_wick_ratio"] = (lower_wick / bar_range.replace(0, np.nan)).clip(0, 1)
    panel_df["price_not_extended"] = (
        panel_df["close_to_sma"].le(max_close_to_sma)
        & panel_df["close_to_high"].le(max_close_to_high)
    )
    panel_df["lower_wick_absorption"] = panel_df["lower_wick_ratio"].ge(
        lower_wick_threshold
    )
    panel_df["accumulation_vote_count"] = (
        panel_df["cmf"].ge(cmf_threshold).fillna(False).astype(int)
        + panel_df["chaikin_oscillator"].ge(chaikin_oscillator_threshold).fillna(False).astype(int)
        + panel_df["obv_flow_score"].ge(obv_score_threshold).fillna(False).astype(int)
    )
    panel_df["accumulation_pressure"] = panel_df["accumulation_vote_count"].ge(min_votes)

    if analysis_start_date is not None:
        panel_df = panel_df.loc[
            panel_df["date"] >= pd.Timestamp(analysis_start_date)
        ].copy()
    if analysis_end_date is not None:
        panel_df = panel_df.loc[
            panel_df["date"] <= pd.Timestamp(analysis_end_date)
        ].copy()
    return panel_df.sort_values(["universe_key", "code", "date"], kind="stable").reset_index(
        drop=True
    )


def _build_event_df(
    panel_df: pd.DataFrame,
    *,
    horizons: tuple[int, ...],
) -> pd.DataFrame:
    if panel_df.empty:
        event_df = _empty_event_df()
        for horizon_days in horizons:
            event_df[f"next_open_to_close_{horizon_days}d_return"] = pd.Series(dtype=float)
        return event_df

    event_frames: list[pd.DataFrame] = []
    filter_masks: dict[FilterKey, pd.Series] = {
        "accumulation": panel_df["accumulation_pressure"],
        "not_extended": panel_df["accumulation_pressure"] & panel_df["price_not_extended"],
        "not_extended_lower_wick": (
            panel_df["accumulation_pressure"]
            & panel_df["price_not_extended"]
            & panel_df["lower_wick_absorption"]
        ),
    }
    base_columns = [
        "date",
        "entry_date",
        "universe_key",
        "code",
        "company_name",
        "close",
        "next_open",
        "cmf",
        "chaikin_oscillator",
        "obv_flow_score",
        "accumulation_vote_count",
        "close_to_sma",
        "close_to_high",
        "lower_wick_ratio",
        "price_not_extended",
        "lower_wick_absorption",
    ]
    return_columns = [
        f"next_open_to_close_{horizon_days}d_return" for horizon_days in horizons
    ]

    for filter_key in FILTER_ORDER:
        mask = filter_masks[filter_key].fillna(False)
        scoped_df = panel_df.loc[mask, [*base_columns, *return_columns]].copy()
        if scoped_df.empty:
            continue
        scoped_df["filter_key"] = filter_key
        scoped_df["filter_label"] = FILTER_LABEL_MAP[filter_key]
        event_frames.append(scoped_df)

    if not event_frames:
        event_df = _empty_event_df()
        for return_column in return_columns:
            event_df[return_column] = pd.Series(dtype=float)
        return event_df

    event_df = pd.concat(event_frames, ignore_index=True)
    event_df["date"] = pd.to_datetime(event_df["date"])
    event_df["entry_date"] = pd.to_datetime(event_df["entry_date"])
    event_df["calendar_year"] = event_df["entry_date"].dt.year.astype("Int64")
    event_df["universe_label"] = event_df["universe_key"].map(UNIVERSE_LABEL_MAP)
    event_df["date"] = event_df["date"].dt.strftime("%Y-%m-%d")
    event_df["entry_date"] = event_df["entry_date"].dt.strftime("%Y-%m-%d")
    keep_columns = [
        "date",
        "entry_date",
        "calendar_year",
        "universe_key",
        "universe_label",
        "filter_key",
        "filter_label",
        "code",
        "company_name",
        "close",
        "next_open",
        "cmf",
        "chaikin_oscillator",
        "obv_flow_score",
        "accumulation_vote_count",
        "close_to_sma",
        "close_to_high",
        "lower_wick_ratio",
        "price_not_extended",
        "lower_wick_absorption",
        *return_columns,
    ]
    return _sort_table(event_df[keep_columns]).reset_index(drop=True)


def _build_universe_summary_row(
    *,
    universe_key: UniverseKey,
    codes_df: pd.DataFrame,
    panel_df: pd.DataFrame,
    event_df: pd.DataFrame,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
) -> dict[str, Any]:
    scoped_event_df = event_df.loc[event_df["universe_key"] == universe_key].copy()
    return {
        "universe_key": universe_key,
        "universe_label": UNIVERSE_LABEL_MAP[universe_key],
        "membership_mode": MEMBERSHIP_MODE,
        "stock_count": int(codes_df["code"].nunique()) if not codes_df.empty else 0,
        "analysis_stock_count": int(panel_df["code"].nunique()) if not panel_df.empty else 0,
        "stock_day_count": int(len(panel_df)),
        "analysis_date_count": int(panel_df["date"].nunique()) if not panel_df.empty else 0,
        "available_start_date": _safe_optional_date(panel_df["date"].min()) if not panel_df.empty else None,
        "available_end_date": _safe_optional_date(panel_df["date"].max()) if not panel_df.empty else None,
        "analysis_start_date": analysis_start_date,
        "analysis_end_date": analysis_end_date,
        "accumulation_event_count": int(
            (scoped_event_df["filter_key"] == "accumulation").sum()
        ) if not scoped_event_df.empty else 0,
        "not_extended_event_count": int(
            (scoped_event_df["filter_key"] == "not_extended").sum()
        ) if not scoped_event_df.empty else 0,
        "not_extended_lower_wick_event_count": int(
            (scoped_event_df["filter_key"] == "not_extended_lower_wick").sum()
        ) if not scoped_event_df.empty else 0,
    }


def _summarize_event_group(
    group_df: pd.DataFrame,
    *,
    return_column: str,
    topix_return_column: str | None = None,
    excess_return_column: str | None = None,
) -> dict[str, Any]:
    values = pd.to_numeric(group_df[return_column], errors="coerce").dropna()
    if values.empty:
        return {
            "event_count": 0,
            "unique_code_count": 0,
            "signal_date_count": 0,
            "mean_return": None,
            "median_return": None,
            "win_rate": None,
            "mean_topix_return": None,
            "median_topix_return": None,
            "mean_excess_return": None,
            "median_excess_return": None,
            "excess_win_rate": None,
            "first_event_date": None,
            "last_event_date": None,
        }
    valid_df = group_df.loc[values.index]
    topix_values = (
        pd.to_numeric(valid_df[topix_return_column], errors="coerce").dropna()
        if topix_return_column and topix_return_column in valid_df.columns
        else pd.Series(dtype=float)
    )
    excess_values = (
        pd.to_numeric(valid_df[excess_return_column], errors="coerce").dropna()
        if excess_return_column and excess_return_column in valid_df.columns
        else pd.Series(dtype=float)
    )
    return {
        "event_count": int(len(valid_df)),
        "unique_code_count": int(valid_df["code"].nunique()),
        "signal_date_count": int(valid_df["date"].nunique()),
        "mean_return": float(values.mean()),
        "median_return": float(values.median()),
        "win_rate": float((values > 0).mean()),
        "mean_topix_return": float(topix_values.mean()) if not topix_values.empty else None,
        "median_topix_return": float(topix_values.median()) if not topix_values.empty else None,
        "mean_excess_return": float(excess_values.mean()) if not excess_values.empty else None,
        "median_excess_return": (
            float(excess_values.median()) if not excess_values.empty else None
        ),
        "excess_win_rate": float((excess_values > 0).mean()) if not excess_values.empty else None,
        "first_event_date": str(valid_df["date"].min()),
        "last_event_date": str(valid_df["date"].max()),
    }


def _build_event_summary_df(
    event_df: pd.DataFrame,
    *,
    horizons: tuple[int, ...],
) -> pd.DataFrame:
    if event_df.empty:
        return _empty_event_summary_df()

    rows: list[dict[str, Any]] = []
    grouped = event_df.groupby(
        ["universe_key", "universe_label", "filter_key", "filter_label"],
        dropna=False,
        sort=False,
    )
    for group_key, group_df in grouped:
        universe_key, universe_label, filter_key, filter_label = group_key
        for horizon_days in horizons:
            return_column = f"next_open_to_close_{horizon_days}d_return"
            topix_return_column = f"topix_next_open_to_close_{horizon_days}d_return"
            excess_return_column = f"excess_next_open_to_close_{horizon_days}d_return"
            rows.append(
                {
                    "universe_key": str(universe_key),
                    "universe_label": str(universe_label),
                    "filter_key": str(filter_key),
                    "filter_label": str(filter_label),
                    "horizon_days": horizon_days,
                    **_summarize_event_group(
                        group_df,
                        return_column=return_column,
                        topix_return_column=topix_return_column,
                        excess_return_column=excess_return_column,
                    ),
                }
            )
    return _sort_table(pd.DataFrame(rows)).reset_index(drop=True)


def _build_yearly_summary_df(
    event_df: pd.DataFrame,
    *,
    horizons: tuple[int, ...],
) -> pd.DataFrame:
    if event_df.empty:
        return _empty_yearly_summary_df()

    rows: list[dict[str, Any]] = []
    grouped = event_df.dropna(subset=["calendar_year"]).groupby(
        [
            "calendar_year",
            "universe_key",
            "universe_label",
            "filter_key",
            "filter_label",
        ],
        dropna=False,
        sort=False,
    )
    for group_key, group_df in grouped:
        calendar_year, universe_key, universe_label, filter_key, filter_label = group_key
        for horizon_days in horizons:
            return_column = f"next_open_to_close_{horizon_days}d_return"
            topix_return_column = f"topix_next_open_to_close_{horizon_days}d_return"
            excess_return_column = f"excess_next_open_to_close_{horizon_days}d_return"
            summary = _summarize_event_group(
                group_df,
                return_column=return_column,
                topix_return_column=topix_return_column,
                excess_return_column=excess_return_column,
            )
            rows.append(
                {
                    "calendar_year": int(calendar_year),
                    "universe_key": str(universe_key),
                    "universe_label": str(universe_label),
                    "filter_key": str(filter_key),
                    "filter_label": str(filter_label),
                    "horizon_days": horizon_days,
                    **{
                        key: value
                        for key, value in summary.items()
                        if key not in {"first_event_date", "last_event_date"}
                    },
                }
            )
    return _sort_table(pd.DataFrame(rows)).reset_index(drop=True)


def _build_entry_cohort_df(
    event_df: pd.DataFrame,
    *,
    horizons: tuple[int, ...],
) -> pd.DataFrame:
    if event_df.empty:
        return _empty_entry_cohort_df()

    rows: list[dict[str, Any]] = []
    group_columns = [
        "entry_date",
        "calendar_year",
        "universe_key",
        "universe_label",
        "filter_key",
        "filter_label",
    ]
    for group_key, group_df in event_df.groupby(group_columns, dropna=False, sort=False):
        entry_date, calendar_year, universe_key, universe_label, filter_key, filter_label = (
            group_key
        )
        for horizon_days in horizons:
            return_column = f"next_open_to_close_{horizon_days}d_return"
            topix_return_column = f"topix_next_open_to_close_{horizon_days}d_return"
            excess_return_column = f"excess_next_open_to_close_{horizon_days}d_return"
            values = pd.to_numeric(group_df[return_column], errors="coerce").dropna()
            if values.empty:
                continue
            valid_df = group_df.loc[values.index]
            topix_values = (
                pd.to_numeric(valid_df[topix_return_column], errors="coerce").dropna()
                if topix_return_column in valid_df.columns
                else pd.Series(dtype=float)
            )
            excess_values = (
                pd.to_numeric(valid_df[excess_return_column], errors="coerce").dropna()
                if excess_return_column in valid_df.columns
                else pd.Series(dtype=float)
            )
            rows.append(
                {
                    "entry_date": str(entry_date),
                    "calendar_year": int(calendar_year),
                    "universe_key": str(universe_key),
                    "universe_label": str(universe_label),
                    "filter_key": str(filter_key),
                    "filter_label": str(filter_label),
                    "horizon_days": horizon_days,
                    "cohort_event_count": int(len(valid_df)),
                    "cohort_unique_code_count": int(valid_df["code"].nunique()),
                    "equal_weight_return": float(values.mean()),
                    "topix_return": float(topix_values.mean()) if not topix_values.empty else None,
                    "excess_equal_weight_return": (
                        float(excess_values.mean()) if not excess_values.empty else None
                    ),
                }
            )
    if not rows:
        return _empty_entry_cohort_df()
    return _sort_table(pd.DataFrame(rows)).reset_index(drop=True)


def _build_cohort_portfolio_summary_df(
    entry_cohort_df: pd.DataFrame,
) -> pd.DataFrame:
    if entry_cohort_df.empty:
        return _empty_cohort_portfolio_summary_df()

    rows: list[dict[str, Any]] = []
    grouped = entry_cohort_df.groupby(
        ["universe_key", "universe_label", "filter_key", "filter_label", "horizon_days"],
        dropna=False,
        sort=False,
    )
    for group_key, group_df in grouped:
        universe_key, universe_label, filter_key, filter_label, horizon_days = group_key
        values = pd.to_numeric(group_df["equal_weight_return"], errors="coerce").dropna()
        if values.empty:
            continue
        topix_values = (
            pd.to_numeric(group_df["topix_return"], errors="coerce").dropna()
            if "topix_return" in group_df.columns
            else pd.Series(dtype=float)
        )
        excess_values = (
            pd.to_numeric(group_df["excess_equal_weight_return"], errors="coerce").dropna()
            if "excess_equal_weight_return" in group_df.columns
            else pd.Series(dtype=float)
        )
        total_signal_count = int(group_df["cohort_event_count"].sum())
        rows.append(
            {
                "universe_key": str(universe_key),
                "universe_label": str(universe_label),
                "filter_key": str(filter_key),
                "filter_label": str(filter_label),
                "horizon_days": int(horizon_days),
                "date_count": int(len(group_df)),
                "total_signal_count": total_signal_count,
                "avg_names_per_date": float(group_df["cohort_event_count"].mean()),
                "median_names_per_date": float(group_df["cohort_event_count"].median()),
                "mean_cohort_return": float(values.mean()),
                "median_cohort_return": float(values.median()),
                "win_rate": float((values > 0).mean()),
                "loss_rate": float((values < 0).mean()),
                "mean_topix_return": float(topix_values.mean()) if not topix_values.empty else None,
                "median_topix_return": (
                    float(topix_values.median()) if not topix_values.empty else None
                ),
                "mean_excess_return": (
                    float(excess_values.mean()) if not excess_values.empty else None
                ),
                "median_excess_return": (
                    float(excess_values.median()) if not excess_values.empty else None
                ),
                "excess_win_rate": (
                    float((excess_values > 0).mean()) if not excess_values.empty else None
                ),
                "excess_loss_rate": (
                    float((excess_values < 0).mean()) if not excess_values.empty else None
                ),
                "best_cohort_return": float(values.max()),
                "worst_cohort_return": float(values.min()),
                "best_excess_return": float(excess_values.max()) if not excess_values.empty else None,
                "worst_excess_return": float(excess_values.min()) if not excess_values.empty else None,
                "first_entry_date": str(group_df["entry_date"].min()),
                "last_entry_date": str(group_df["entry_date"].max()),
            }
        )
    if not rows:
        return _empty_cohort_portfolio_summary_df()
    return _sort_table(pd.DataFrame(rows)).reset_index(drop=True)


def _build_yearly_cohort_summary_df(entry_cohort_df: pd.DataFrame) -> pd.DataFrame:
    if entry_cohort_df.empty:
        return _empty_yearly_cohort_summary_df()

    rows: list[dict[str, Any]] = []
    grouped = entry_cohort_df.dropna(subset=["calendar_year"]).groupby(
        [
            "calendar_year",
            "universe_key",
            "universe_label",
            "filter_key",
            "filter_label",
            "horizon_days",
        ],
        dropna=False,
        sort=False,
    )
    for group_key, group_df in grouped:
        calendar_year, universe_key, universe_label, filter_key, filter_label, horizon_days = (
            group_key
        )
        values = pd.to_numeric(group_df["equal_weight_return"], errors="coerce").dropna()
        if values.empty:
            continue
        topix_values = pd.to_numeric(group_df["topix_return"], errors="coerce").dropna()
        excess_values = pd.to_numeric(
            group_df["excess_equal_weight_return"],
            errors="coerce",
        ).dropna()
        rows.append(
            {
                "calendar_year": int(calendar_year),
                "universe_key": str(universe_key),
                "universe_label": str(universe_label),
                "filter_key": str(filter_key),
                "filter_label": str(filter_label),
                "horizon_days": int(horizon_days),
                "date_count": int(len(group_df)),
                "total_signal_count": int(group_df["cohort_event_count"].sum()),
                "avg_names_per_date": float(group_df["cohort_event_count"].mean()),
                "mean_cohort_return": float(values.mean()),
                "median_cohort_return": float(values.median()),
                "win_rate": float((values > 0).mean()),
                "mean_topix_return": float(topix_values.mean())
                if not topix_values.empty
                else None,
                "median_topix_return": float(topix_values.median())
                if not topix_values.empty
                else None,
                "mean_excess_return": float(excess_values.mean())
                if not excess_values.empty
                else None,
                "median_excess_return": float(excess_values.median())
                if not excess_values.empty
                else None,
                "excess_win_rate": float((excess_values > 0).mean())
                if not excess_values.empty
                else None,
            }
        )
    if not rows:
        return _empty_yearly_cohort_summary_df()
    return _sort_table(pd.DataFrame(rows)).reset_index(drop=True)


def _build_capped_entry_cohort_df(
    event_df: pd.DataFrame,
    *,
    horizons: tuple[int, ...],
    concentration_caps: tuple[int, ...],
) -> pd.DataFrame:
    if event_df.empty or not concentration_caps:
        return _empty_capped_entry_cohort_df()

    rows: list[dict[str, Any]] = []
    group_columns = [
        "entry_date",
        "calendar_year",
        "universe_key",
        "universe_label",
        "filter_key",
        "filter_label",
    ]
    rank_columns = [
        "accumulation_vote_count",
        "cmf",
        "obv_flow_score",
        "chaikin_oscillator",
        "lower_wick_ratio",
        "code",
    ]
    ascending = [False, False, False, False, False, True]
    for group_key, group_df in event_df.groupby(group_columns, dropna=False, sort=False):
        entry_date, calendar_year, universe_key, universe_label, filter_key, filter_label = (
            group_key
        )
        ranked_df = group_df.sort_values(
            rank_columns,
            ascending=ascending,
            kind="stable",
        )
        for max_names_per_date in concentration_caps:
            capped_df = ranked_df.head(max_names_per_date)
            for horizon_days in horizons:
                return_column = f"next_open_to_close_{horizon_days}d_return"
                topix_return_column = f"topix_next_open_to_close_{horizon_days}d_return"
                excess_return_column = f"excess_next_open_to_close_{horizon_days}d_return"
                values = pd.to_numeric(capped_df[return_column], errors="coerce").dropna()
                if values.empty:
                    continue
                valid_df = capped_df.loc[values.index]
                topix_values = pd.to_numeric(
                    valid_df[topix_return_column],
                    errors="coerce",
                ).dropna()
                excess_values = pd.to_numeric(
                    valid_df[excess_return_column],
                    errors="coerce",
                ).dropna()
                rows.append(
                    {
                        "entry_date": str(entry_date),
                        "calendar_year": int(calendar_year),
                        "universe_key": str(universe_key),
                        "universe_label": str(universe_label),
                        "filter_key": str(filter_key),
                        "filter_label": str(filter_label),
                        "horizon_days": horizon_days,
                        "max_names_per_date": max_names_per_date,
                        "cohort_event_count": int(len(valid_df)),
                        "cohort_unique_code_count": int(valid_df["code"].nunique()),
                        "equal_weight_return": float(values.mean()),
                        "topix_return": float(topix_values.mean())
                        if not topix_values.empty
                        else None,
                        "excess_equal_weight_return": float(excess_values.mean())
                        if not excess_values.empty
                        else None,
                    }
                )
    if not rows:
        return _empty_capped_entry_cohort_df()
    return _sort_table(pd.DataFrame(rows)).reset_index(drop=True)


def _build_capped_cohort_portfolio_summary_df(
    capped_entry_cohort_df: pd.DataFrame,
) -> pd.DataFrame:
    if capped_entry_cohort_df.empty:
        return _empty_capped_cohort_portfolio_summary_df()

    summary_frames: list[pd.DataFrame] = []
    for max_names_per_date, scoped_df in capped_entry_cohort_df.groupby(
        "max_names_per_date",
        dropna=False,
        sort=False,
    ):
        summary_df = _build_cohort_portfolio_summary_df(scoped_df)
        if summary_df.empty:
            continue
        cap_value = int(cast(Any, max_names_per_date))
        summary_df.insert(5, "max_names_per_date", cap_value)
        summary_frames.append(summary_df)
    if not summary_frames:
        return _empty_capped_cohort_portfolio_summary_df()
    return _sort_table(pd.concat(summary_frames, ignore_index=True)).reset_index(drop=True)


def _sample_period_for_year(year: int) -> tuple[str, str] | None:
    for sample_key, sample_label, start_year, end_year in OOS_PERIODS:
        if start_year is not None and year < start_year:
            continue
        if end_year is not None and year > end_year:
            continue
        return sample_key, sample_label
    return None


def _build_oos_portfolio_summary_df(
    entry_cohort_df: pd.DataFrame,
    capped_entry_cohort_df: pd.DataFrame,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    if not entry_cohort_df.empty:
        all_df = entry_cohort_df.copy()
        all_df["portfolio_variant"] = "all_signals"
        all_df["max_names_per_date"] = pd.NA
        frames.append(all_df)
    if not capped_entry_cohort_df.empty:
        capped_df = capped_entry_cohort_df.copy()
        capped_df["portfolio_variant"] = (
            "cap_" + capped_df["max_names_per_date"].astype(str)
        )
        frames.append(capped_df)
    if not frames:
        return _empty_oos_portfolio_summary_df()

    combined_df = pd.concat(frames, ignore_index=True)
    sample_pairs = combined_df["calendar_year"].map(
        lambda value: _sample_period_for_year(int(value)) if not pd.isna(value) else None
    )
    combined_df["sample_key"] = sample_pairs.map(
        lambda value: value[0] if value is not None else None
    )
    combined_df["sample_label"] = sample_pairs.map(
        lambda value: value[1] if value is not None else None
    )
    combined_df = combined_df.dropna(subset=["sample_key", "sample_label"])
    if combined_df.empty:
        return _empty_oos_portfolio_summary_df()

    rows: list[dict[str, Any]] = []
    grouped = combined_df.groupby(
        [
            "sample_key",
            "sample_label",
            "portfolio_variant",
            "max_names_per_date",
            "universe_key",
            "universe_label",
            "filter_key",
            "filter_label",
            "horizon_days",
        ],
        dropna=False,
        sort=False,
    )
    for group_key, group_df in grouped:
        (
            sample_key,
            sample_label,
            portfolio_variant,
            max_names_per_date,
            universe_key,
            universe_label,
            filter_key,
            filter_label,
            horizon_days,
        ) = group_key
        values = pd.to_numeric(group_df["equal_weight_return"], errors="coerce").dropna()
        if values.empty:
            continue
        topix_values = pd.to_numeric(group_df["topix_return"], errors="coerce").dropna()
        excess_values = pd.to_numeric(
            group_df["excess_equal_weight_return"],
            errors="coerce",
        ).dropna()
        rows.append(
            {
                "sample_key": str(sample_key),
                "sample_label": str(sample_label),
                "portfolio_variant": str(portfolio_variant),
                "max_names_per_date": None
                if pd.isna(max_names_per_date)
                else int(max_names_per_date),
                "universe_key": str(universe_key),
                "universe_label": str(universe_label),
                "filter_key": str(filter_key),
                "filter_label": str(filter_label),
                "horizon_days": int(horizon_days),
                "date_count": int(len(group_df)),
                "total_signal_count": int(group_df["cohort_event_count"].sum()),
                "avg_names_per_date": float(group_df["cohort_event_count"].mean()),
                "mean_cohort_return": float(values.mean()),
                "median_cohort_return": float(values.median()),
                "win_rate": float((values > 0).mean()),
                "mean_topix_return": float(topix_values.mean())
                if not topix_values.empty
                else None,
                "median_topix_return": float(topix_values.median())
                if not topix_values.empty
                else None,
                "mean_excess_return": float(excess_values.mean())
                if not excess_values.empty
                else None,
                "median_excess_return": float(excess_values.median())
                if not excess_values.empty
                else None,
                "excess_win_rate": float((excess_values > 0).mean())
                if not excess_values.empty
                else None,
            }
        )
    if not rows:
        return _empty_oos_portfolio_summary_df()
    return _sort_table(pd.DataFrame(rows)).reset_index(drop=True)


def _sort_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    sorted_df = df.copy()
    if "universe_key" in sorted_df.columns:
        sorted_df["_universe_order"] = sorted_df["universe_key"].map(
            {key: index for index, key in enumerate(UNIVERSE_ORDER, start=1)}
        )
    if "filter_key" in sorted_df.columns:
        sorted_df["_filter_order"] = sorted_df["filter_key"].map(
            {key: index for index, key in enumerate(FILTER_ORDER, start=1)}
        )
    if "sample_key" in sorted_df.columns:
        sorted_df["_sample_order"] = sorted_df["sample_key"].map(
            {key: index for index, (key, _, _, _) in enumerate(OOS_PERIODS, start=1)}
        )
    sort_columns = [
        column
        for column in [
            "_sample_order",
            "_universe_order",
            "_filter_order",
            "horizon_days",
            "max_names_per_date",
            "portfolio_variant",
            "calendar_year",
            "entry_date",
            "date",
            "code",
        ]
        if column in sorted_df.columns
    ]
    if sort_columns:
        sorted_df = sorted_df.sort_values(sort_columns, kind="stable").reset_index(drop=True)
    return sorted_df.drop(
        columns=[
            column
            for column in ["_sample_order", "_universe_order", "_filter_order"]
            if column in sorted_df.columns
        ]
    )


def run_accumulation_flow_followthrough_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    horizons: tuple[int, ...] | list[int] | None = None,
    cmf_period: int = DEFAULT_CMF_PERIOD,
    chaikin_fast_period: int = DEFAULT_CHAIKIN_FAST_PERIOD,
    chaikin_slow_period: int = DEFAULT_CHAIKIN_SLOW_PERIOD,
    obv_lookback_period: int = DEFAULT_OBV_LOOKBACK_PERIOD,
    cmf_threshold: float = DEFAULT_CMF_THRESHOLD,
    chaikin_oscillator_threshold: float = DEFAULT_CHAIKIN_OSCILLATOR_THRESHOLD,
    obv_score_threshold: float = DEFAULT_OBV_SCORE_THRESHOLD,
    min_votes: int = DEFAULT_MIN_VOTES,
    price_sma_period: int = DEFAULT_PRICE_SMA_PERIOD,
    price_high_lookback_period: int = DEFAULT_PRICE_HIGH_LOOKBACK_PERIOD,
    max_close_to_sma: float = DEFAULT_MAX_CLOSE_TO_SMA,
    max_close_to_high: float = DEFAULT_MAX_CLOSE_TO_HIGH,
    lower_wick_threshold: float = DEFAULT_LOWER_WICK_THRESHOLD,
    concentration_caps: tuple[int, ...] | list[int] | None = None,
) -> AccumulationFlowFollowthroughResult:
    normalized_horizons = _normalize_int_sequence(
        horizons,
        fallback=DEFAULT_HORIZONS,
        name="horizons",
    )
    normalized_concentration_caps = _normalize_optional_int_sequence(
        concentration_caps,
        fallback=DEFAULT_CONCENTRATION_CAPS,
        name="concentration_caps",
    )
    with open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="trading25-accumulation-flow-",
    ) as context:
        conn = context.connection
        available_start_date, available_end_date = fetch_date_range(
            conn,
            table_name="stock_data",
        )
        bounded_available_end_date = end_date or available_end_date
        default_start_date = _default_start_date(
            available_start_date=available_start_date,
            available_end_date=bounded_available_end_date,
            lookback_years=lookback_years,
        )
        analysis_start_date = start_date or default_start_date
        analysis_end_date = end_date or available_end_date
        topix_df = _query_topix_history(conn, end_date=analysis_end_date)
        topix_return_df = _prepare_topix_return_panel(
            topix_df,
            horizons=normalized_horizons,
        )

        universe_summary_rows: list[dict[str, Any]] = []
        event_frames: list[pd.DataFrame] = []
        for universe_key in UNIVERSE_ORDER:
            codes_df = _query_universe_codes(conn, universe_key=universe_key)
            raw_df = _query_universe_stock_history(
                conn,
                universe_key=universe_key,
                end_date=analysis_end_date,
            )
            panel_df = _prepare_accumulation_panel(
                raw_df,
                analysis_start_date=analysis_start_date,
                analysis_end_date=analysis_end_date,
                horizons=normalized_horizons,
                cmf_period=cmf_period,
                chaikin_fast_period=chaikin_fast_period,
                chaikin_slow_period=chaikin_slow_period,
                obv_lookback_period=obv_lookback_period,
                cmf_threshold=cmf_threshold,
                chaikin_oscillator_threshold=chaikin_oscillator_threshold,
                obv_score_threshold=obv_score_threshold,
                min_votes=min_votes,
                price_sma_period=price_sma_period,
                price_high_lookback_period=price_high_lookback_period,
                max_close_to_sma=max_close_to_sma,
                max_close_to_high=max_close_to_high,
                lower_wick_threshold=lower_wick_threshold,
            )
            event_df = _build_event_df(panel_df, horizons=normalized_horizons)
            event_frames.append(event_df)
            universe_summary_rows.append(
                _build_universe_summary_row(
                    universe_key=universe_key,
                    codes_df=codes_df,
                    panel_df=panel_df,
                    event_df=event_df,
                    analysis_start_date=analysis_start_date,
                    analysis_end_date=analysis_end_date,
                )
            )

        event_df = (
            _sort_table(pd.concat(event_frames, ignore_index=True))
            if event_frames
            else _empty_event_df()
        )
        event_df = _attach_benchmark_returns(
            event_df,
            topix_return_df,
            horizons=normalized_horizons,
        )
        universe_summary_df = (
            _sort_table(pd.DataFrame(universe_summary_rows))
            if universe_summary_rows
            else _empty_universe_summary_df()
        )
        event_summary_df = _build_event_summary_df(
            event_df,
            horizons=normalized_horizons,
        )
        yearly_summary_df = _build_yearly_summary_df(
            event_df,
            horizons=normalized_horizons,
        )
        entry_cohort_df = _build_entry_cohort_df(
            event_df,
            horizons=normalized_horizons,
        )
        cohort_portfolio_summary_df = _build_cohort_portfolio_summary_df(entry_cohort_df)
        yearly_cohort_summary_df = _build_yearly_cohort_summary_df(entry_cohort_df)
        capped_entry_cohort_df = _build_capped_entry_cohort_df(
            event_df,
            horizons=normalized_horizons,
            concentration_caps=normalized_concentration_caps,
        )
        capped_cohort_portfolio_summary_df = _build_capped_cohort_portfolio_summary_df(
            capped_entry_cohort_df
        )
        oos_portfolio_summary_df = _build_oos_portfolio_summary_df(
            entry_cohort_df,
            capped_entry_cohort_df,
        )

        execution_note = (
            "Forward returns assume an entry at the next session open after the "
            "signal date and exits at future daily closes; benchmark-neutral "
            "figures subtract the TOPIX return over the same entry/exit window."
        )
        feature_note = (
            "CMF, Chaikin oscillator, and OBV flow score are computed per code using "
            "only same-day-or-earlier OHLCV. `not_extended` additionally requires "
            "close/SMA and close/recent-high constraints; the lower-wick branch "
            "adds an intraday absorption proxy."
        )
        return AccumulationFlowFollowthroughResult(
            db_path=db_path,
            source_mode=context.source_mode,
            source_detail=context.source_detail,
            available_start_date=available_start_date,
            available_end_date=available_end_date,
            default_start_date=default_start_date,
            analysis_start_date=analysis_start_date,
            analysis_end_date=analysis_end_date,
            lookback_years=lookback_years,
            horizons=normalized_horizons,
            cmf_period=cmf_period,
            chaikin_fast_period=chaikin_fast_period,
            chaikin_slow_period=chaikin_slow_period,
            obv_lookback_period=obv_lookback_period,
            cmf_threshold=cmf_threshold,
            chaikin_oscillator_threshold=chaikin_oscillator_threshold,
            obv_score_threshold=obv_score_threshold,
            min_votes=min_votes,
            price_sma_period=price_sma_period,
            price_high_lookback_period=price_high_lookback_period,
            max_close_to_sma=max_close_to_sma,
            max_close_to_high=max_close_to_high,
            lower_wick_threshold=lower_wick_threshold,
            concentration_caps=normalized_concentration_caps,
            membership_mode=MEMBERSHIP_MODE,
            execution_note=execution_note,
            feature_note=feature_note,
            universe_summary_df=universe_summary_df,
            event_df=event_df,
            event_summary_df=event_summary_df,
            yearly_summary_df=yearly_summary_df,
            entry_cohort_df=entry_cohort_df,
            cohort_portfolio_summary_df=cohort_portfolio_summary_df,
            yearly_cohort_summary_df=yearly_cohort_summary_df,
            capped_entry_cohort_df=capped_entry_cohort_df,
            capped_cohort_portfolio_summary_df=capped_cohort_portfolio_summary_df,
            oos_portfolio_summary_df=oos_portfolio_summary_df,
        )


def write_accumulation_flow_followthrough_research_bundle(
    result: AccumulationFlowFollowthroughResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=ACCUMULATION_FLOW_FOLLOWTHROUGH_EXPERIMENT_ID,
        module=__name__,
        function="run_accumulation_flow_followthrough_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "lookback_years": result.lookback_years,
            "horizons": list(result.horizons),
            "cmf_period": result.cmf_period,
            "chaikin_fast_period": result.chaikin_fast_period,
            "chaikin_slow_period": result.chaikin_slow_period,
            "obv_lookback_period": result.obv_lookback_period,
            "cmf_threshold": result.cmf_threshold,
            "chaikin_oscillator_threshold": result.chaikin_oscillator_threshold,
            "obv_score_threshold": result.obv_score_threshold,
            "min_votes": result.min_votes,
            "price_sma_period": result.price_sma_period,
            "price_high_lookback_period": result.price_high_lookback_period,
            "max_close_to_sma": result.max_close_to_sma,
            "max_close_to_high": result.max_close_to_high,
            "lower_wick_threshold": result.lower_wick_threshold,
            "concentration_caps": list(result.concentration_caps),
        },
        result=result,
        table_field_names=TABLE_FIELD_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_accumulation_flow_followthrough_research_bundle(
    bundle_path: str | Path,
) -> AccumulationFlowFollowthroughResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=AccumulationFlowFollowthroughResult,
        table_field_names=TABLE_FIELD_NAMES,
    )


def get_accumulation_flow_followthrough_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ACCUMULATION_FLOW_FOLLOWTHROUGH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_accumulation_flow_followthrough_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        ACCUMULATION_FLOW_FOLLOWTHROUGH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_research_bundle_summary_markdown(
    result: AccumulationFlowFollowthroughResult,
) -> str:
    focus_horizon = 20 if 20 in result.horizons else result.horizons[0]
    best_event_rows = _top_event_summary_rows(
        result.event_summary_df,
        horizon_days=focus_horizon,
        limit=8,
    )
    best_portfolio_rows = _top_cohort_portfolio_rows(
        result.cohort_portfolio_summary_df,
        horizon_days=focus_horizon,
        limit=8,
    )
    best_capped_rows = _top_capped_portfolio_rows(
        result.capped_cohort_portfolio_summary_df,
        horizon_days=focus_horizon,
        limit=8,
    )
    best_oos_rows = _top_oos_portfolio_rows(
        result.oos_portfolio_summary_df,
        horizon_days=focus_horizon,
        sample_key="oos_2024_forward",
        limit=8,
    )
    lines = [
        "# Accumulation Flow Followthrough",
        "",
        "## Scope",
        "",
        "- Universes: `TOPIX500 / PRIME ex TOPIX500 / Standard / Growth`.",
        "- Accumulation proxy: `CMF`, `Chaikin oscillator`, `OBV flow score` vote.",
        "- Entry assumption: next session open after the signal date.",
        "- Evaluation: next-open to future-close returns and TOPIX-neutral excess returns.",
        "",
        "## Notes",
        "",
        f"- {result.execution_note}",
        f"- {result.feature_note}",
        f"- Available stock-data range: `{result.available_start_date}` -> `{result.available_end_date}`.",
        f"- Analysis range: `{result.analysis_start_date}` -> `{result.analysis_end_date}`.",
        f"- Horizons: `{', '.join(str(value) for value in result.horizons)}` sessions.",
        f"- Vote rule: `min_votes={result.min_votes}`, thresholds CMF=`{result.cmf_threshold}`, Chaikin=`{result.chaikin_oscillator_threshold}`, OBV=`{result.obv_score_threshold}`.",
        f"- Concentration caps: `{', '.join(str(value) for value in result.concentration_caps)}` names per entry date.",
        "",
        "## Universe Coverage",
        "",
        _markdown_table(
            cast(list[dict[str, Any]], result.universe_summary_df.to_dict("records")),
            columns=(
                ("Universe", "universe_label"),
                ("Stocks", "stock_count"),
                ("Analysis Stocks", "analysis_stock_count"),
                ("Stock Days", "stock_day_count"),
                ("Accum Events", "accumulation_event_count"),
                ("Not Ext", "not_extended_event_count"),
                ("Wick", "not_extended_lower_wick_event_count"),
            ),
        ),
        "",
        f"## Best Event Buckets ({focus_horizon}d)",
        "",
        _markdown_table(
            best_event_rows,
            columns=(
                ("Universe", "universe_label"),
                ("Filter", "filter_label"),
                ("Events", "event_count"),
                ("Mean", "mean_return"),
                ("TOPIX", "mean_topix_return"),
                ("Excess", "mean_excess_return"),
                ("Median", "median_return"),
                ("Win", "win_rate"),
                ("Excess Win", "excess_win_rate"),
            ),
        ),
        "",
        f"## Entry-Cohort Portfolio Lens ({focus_horizon}d)",
        "",
        _markdown_table(
            best_portfolio_rows,
            columns=(
                ("Universe", "universe_label"),
                ("Filter", "filter_label"),
                ("Dates", "date_count"),
                ("Signals", "total_signal_count"),
                ("Avg Names", "avg_names_per_date"),
                ("Mean Cohort", "mean_cohort_return"),
                ("TOPIX", "mean_topix_return"),
                ("Excess", "mean_excess_return"),
                ("Win", "win_rate"),
                ("Excess Win", "excess_win_rate"),
            ),
        ),
        "",
        f"## Capped Portfolio Lens ({focus_horizon}d)",
        "",
        _markdown_table(
            best_capped_rows,
            columns=(
                ("Universe", "universe_label"),
                ("Filter", "filter_label"),
                ("Cap", "max_names_per_date"),
                ("Dates", "date_count"),
                ("Avg Names", "avg_names_per_date"),
                ("Mean Cohort", "mean_cohort_return"),
                ("TOPIX", "mean_topix_return"),
                ("Excess", "mean_excess_return"),
                ("Excess Win", "excess_win_rate"),
            ),
        ),
        "",
        f"## OOS 2024-Forward Portfolio Lens ({focus_horizon}d)",
        "",
        _markdown_table(
            best_oos_rows,
            columns=(
                ("Variant", "portfolio_variant"),
                ("Universe", "universe_label"),
                ("Filter", "filter_label"),
                ("Cap", "max_names_per_date"),
                ("Dates", "date_count"),
                ("Mean Cohort", "mean_cohort_return"),
                ("TOPIX", "mean_topix_return"),
                ("Excess", "mean_excess_return"),
                ("Excess Win", "excess_win_rate"),
            ),
        ),
    ]
    return "\n".join(lines)


def _build_published_summary_payload(
    result: AccumulationFlowFollowthroughResult,
) -> dict[str, Any]:
    focus_horizon = 20 if 20 in result.horizons else result.horizons[0]
    return {
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "horizons": list(result.horizons),
        "membershipMode": result.membership_mode,
        "concentrationCaps": list(result.concentration_caps),
        "executionNote": result.execution_note,
        "featureNote": result.feature_note,
        "universeCoverage": result.universe_summary_df.to_dict("records"),
        "bestEventBuckets": _top_event_summary_rows(
            result.event_summary_df,
            horizon_days=focus_horizon,
            limit=8,
        ),
        "cohortPortfolioLens": _top_cohort_portfolio_rows(
            result.cohort_portfolio_summary_df,
            horizon_days=focus_horizon,
            limit=8,
        ),
        "cappedPortfolioLens": _top_capped_portfolio_rows(
            result.capped_cohort_portfolio_summary_df,
            horizon_days=focus_horizon,
            limit=8,
        ),
        "oosPortfolioLens": _top_oos_portfolio_rows(
            result.oos_portfolio_summary_df,
            horizon_days=focus_horizon,
            sample_key="oos_2024_forward",
            limit=8,
        ),
    }


def _top_event_summary_rows(
    event_summary_df: pd.DataFrame,
    *,
    horizon_days: int,
    limit: int,
) -> list[dict[str, Any]]:
    if event_summary_df.empty:
        return []
    scoped_df = event_summary_df.loc[
        event_summary_df["horizon_days"].astype(int) == horizon_days
    ].copy()
    scoped_df = scoped_df.loc[scoped_df["event_count"].astype(int) > 0].copy()
    if scoped_df.empty:
        return []
    ranked_df = scoped_df.sort_values(
        by=["mean_return", "event_count", "universe_key"],
        ascending=[False, False, True],
        kind="stable",
    ).head(limit)
    return cast(list[dict[str, Any]], ranked_df.to_dict("records"))


def _top_cohort_portfolio_rows(
    cohort_portfolio_summary_df: pd.DataFrame,
    *,
    horizon_days: int,
    limit: int,
) -> list[dict[str, Any]]:
    if cohort_portfolio_summary_df.empty:
        return []
    scoped_df = cohort_portfolio_summary_df.loc[
        cohort_portfolio_summary_df["horizon_days"].astype(int) == horizon_days
    ].copy()
    if scoped_df.empty:
        return []
    ranked_df = scoped_df.sort_values(
        by=["mean_cohort_return", "date_count", "universe_key"],
        ascending=[False, False, True],
        kind="stable",
    ).head(limit)
    return cast(list[dict[str, Any]], ranked_df.to_dict("records"))


def _top_capped_portfolio_rows(
    capped_cohort_portfolio_summary_df: pd.DataFrame,
    *,
    horizon_days: int,
    limit: int,
) -> list[dict[str, Any]]:
    if capped_cohort_portfolio_summary_df.empty:
        return []
    scoped_df = capped_cohort_portfolio_summary_df.loc[
        capped_cohort_portfolio_summary_df["horizon_days"].astype(int) == horizon_days
    ].copy()
    if scoped_df.empty:
        return []
    ranked_df = scoped_df.sort_values(
        by=["mean_excess_return", "mean_cohort_return", "date_count", "universe_key"],
        ascending=[False, False, False, True],
        kind="stable",
    ).head(limit)
    return cast(list[dict[str, Any]], ranked_df.to_dict("records"))


def _top_oos_portfolio_rows(
    oos_portfolio_summary_df: pd.DataFrame,
    *,
    horizon_days: int,
    sample_key: str,
    limit: int,
) -> list[dict[str, Any]]:
    if oos_portfolio_summary_df.empty:
        return []
    scoped_df = oos_portfolio_summary_df.loc[
        (oos_portfolio_summary_df["horizon_days"].astype(int) == horizon_days)
        & (oos_portfolio_summary_df["sample_key"] == sample_key)
    ].copy()
    if scoped_df.empty:
        return []
    ranked_df = scoped_df.sort_values(
        by=["mean_excess_return", "mean_cohort_return", "date_count", "universe_key"],
        ascending=[False, False, False, True],
        kind="stable",
    ).head(limit)
    return cast(list[dict[str, Any]], ranked_df.to_dict("records"))


def _markdown_table(
    rows: list[dict[str, Any]],
    *,
    columns: tuple[tuple[str, str], ...],
) -> str:
    header = "| " + " | ".join(label for label, _ in columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    if not rows:
        return "\n".join([header, separator, "| (none) |" + " |" * (len(columns) - 1)])
    body: list[str] = []
    for row in rows:
        rendered = " | ".join(_format_markdown_cell(row.get(key)) for _, key in columns)
        body.append(f"| {rendered} |")
    return "\n".join([header, separator, *body])


def _format_markdown_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and np.isnan(value):
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value).replace("|", "\\|")
