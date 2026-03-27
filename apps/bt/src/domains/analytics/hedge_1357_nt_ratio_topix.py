"""
1357 x NT ratio / TOPIX hedge research analytics.

This module is read-only. It aligns local market snapshot data for:
- 1357 (Nikkei double inverse ETF)
- TOPIX
- N225_UNDERPX

It then evaluates rule-based hedge overlays against proxy long baskets derived
from the local stock snapshot.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast

import numpy as np
import pandas as pd

from src.domains.analytics.nt_ratio_change_stock_overnight_distribution import (
    NT_RATIO_BUCKET_ORDER,
    NtRatioBucketKey,
    NtRatioReturnStats,
    format_nt_ratio_bucket_label,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    CLOSE_BUCKET_ORDER,
    STOCK_GROUP_ORDER as _STOCK_GROUP_ORDER,
    CloseBucketKey,
    SourceMode,
    StockGroup,
    TopixCloseReturnStats,
    _date_where_clause,
    _normalize_code_sql,
    _open_analysis_connection,
    _validate_selected_groups,
    format_close_bucket_label,
)
from src.domains.strategy.indicators import compute_moving_average

TargetName = Literal[
    "next_overnight",
    "next_intraday",
    "next_close_to_close",
    "forward_3d_close_to_close",
    "forward_5d_close_to_close",
]
RuleName = Literal[
    "shock_topix_le_negative_threshold_2",
    "shock_joint_adverse",
    "trend_ma_bearish",
    "trend_macd_negative",
    "hybrid_bearish_joint",
]
SplitName = Literal["overall", "discovery", "validation"]

TARGET_ORDER: tuple[TargetName, ...] = (
    "next_overnight",
    "next_intraday",
    "next_close_to_close",
    "forward_3d_close_to_close",
    "forward_5d_close_to_close",
)
RULE_ORDER: tuple[RuleName, ...] = (
    "shock_topix_le_negative_threshold_2",
    "shock_joint_adverse",
    "trend_ma_bearish",
    "trend_macd_negative",
    "hybrid_bearish_joint",
)
SPLIT_ORDER: tuple[SplitName, ...] = ("overall", "discovery", "validation")
STOCK_GROUP_ORDER: tuple[StockGroup, ...] = _STOCK_GROUP_ORDER

_NIKKEI_SYNTHETIC_INDEX_CODE = "N225_UNDERPX"
_ETF_CODE = "1357"
_DISCOVERY_END_DATE = "2021-12-31"
_VALIDATION_START_DATE = "2022-01-01"
_BETA_LOOKBACK_DAYS = 60
_BETA_MIN_PERIODS = 20
_MACD_FAST_PERIOD = 12
_MACD_SLOW_PERIOD = 26
_MACD_SIGNAL_PERIOD = 9
_MACD_BASIS = "ema_adjust_false"

_TARGET_COLUMN_MAP: dict[TargetName, tuple[str, str]] = {
    "next_overnight": (
        "long_next_overnight_return",
        "etf_next_overnight_return",
    ),
    "next_intraday": (
        "long_next_intraday_return",
        "etf_next_intraday_return",
    ),
    "next_close_to_close": (
        "long_next_close_to_close_return",
        "etf_next_close_to_close_return",
    ),
    "forward_3d_close_to_close": (
        "long_forward_3d_close_to_close_return",
        "etf_forward_3d_close_to_close_return",
    ),
    "forward_5d_close_to_close": (
        "long_forward_5d_close_to_close_return",
        "etf_forward_5d_close_to_close_return",
    ),
}


@dataclass(frozen=True)
class Hedge1357NtRatioTopixResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    discovery_end_date: str
    validation_start_date: str
    sigma_threshold_1: float
    sigma_threshold_2: float
    selected_groups: tuple[StockGroup, ...]
    fixed_weights: tuple[float, ...]
    macd_basis: str
    macd_fast_period: int
    macd_slow_period: int
    macd_signal_period: int
    topix_close_stats: TopixCloseReturnStats | None
    nt_ratio_stats: NtRatioReturnStats | None
    daily_market_df: pd.DataFrame
    daily_proxy_returns_df: pd.DataFrame
    joint_forward_summary_df: pd.DataFrame
    rule_signal_summary_df: pd.DataFrame
    hedge_metrics_df: pd.DataFrame
    etf_strategy_metrics_df: pd.DataFrame
    etf_strategy_split_comparison_df: pd.DataFrame
    split_comparison_df: pd.DataFrame
    annual_rule_summary_df: pd.DataFrame
    shortlist_df: pd.DataFrame


def _filter_date_range_df(
    df: pd.DataFrame,
    *,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    filtered = df.copy()
    if start_date:
        filtered = filtered[filtered["date"] >= start_date]
    if end_date:
        filtered = filtered[filtered["date"] <= end_date]
    return filtered.reset_index(drop=True)


def _validate_fixed_weights(fixed_weights: Sequence[float]) -> tuple[float, ...]:
    validated: list[float] = []
    for raw_value in fixed_weights:
        value = float(raw_value)
        if value <= 0:
            raise ValueError("fixed_weights must contain positive values")
        validated.append(value)
    if not validated:
        raise ValueError("fixed_weights must contain at least one positive value")
    return tuple(validated)


def _compute_ema_macd_histogram(
    close: pd.Series,
    *,
    fast_period: int = _MACD_FAST_PERIOD,
    slow_period: int = _MACD_SLOW_PERIOD,
    signal_period: int = _MACD_SIGNAL_PERIOD,
) -> pd.Series:
    fast_ema = compute_moving_average(close, fast_period, ma_type="ema")
    slow_ema = compute_moving_average(close, slow_period, ma_type="ema")
    macd_line = fast_ema - slow_ema
    signal_line = compute_moving_average(macd_line, signal_period, ma_type="ema")
    return macd_line - signal_line


def _query_market_daily_frame(conn: Any) -> pd.DataFrame:
    normalized_code_sql = _normalize_code_sql("code")
    rows = conn.execute(
        f"""
        WITH etf_raw AS (
            SELECT
                date,
                {normalized_code_sql} AS normalized_code,
                CAST(open AS DOUBLE) AS etf_open,
                CAST(close AS DOUBLE) AS etf_close,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}, date
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stock_data
            WHERE close IS NOT NULL
        ),
        etf AS (
            SELECT
                date,
                etf_open,
                etf_close
            FROM etf_raw
            WHERE row_priority = 1
              AND normalized_code = ?
        )
        SELECT
            t.date,
            CAST(t.open AS DOUBLE) AS topix_open,
            CAST(t.close AS DOUBLE) AS topix_close,
            CAST(n.close AS DOUBLE) AS n225_close,
            etf.etf_open,
            etf.etf_close
        FROM topix_data t
        JOIN indices_data n
          ON n.date = t.date
         AND n.code = ?
        JOIN etf
          ON etf.date = t.date
        WHERE t.close IS NOT NULL
          AND t.close > 0
          AND n.close IS NOT NULL
          AND n.close > 0
          AND etf.etf_close IS NOT NULL
          AND etf.etf_close > 0
        ORDER BY t.date
        """,
        (_ETF_CODE, _NIKKEI_SYNTHETIC_INDEX_CODE),
    ).fetchdf()
    if rows.empty:
        raise ValueError("No aligned 1357 / TOPIX / N225_UNDERPX rows were found")
    return rows


def _query_group_daily_returns(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    selected_groups: Sequence[StockGroup],
) -> pd.DataFrame:
    date_where_sql, date_params = _date_where_clause("date", start_date, end_date)
    normalized_code_sql = _normalize_code_sql("code")
    group_selects = {
        "PRIME": """
            SELECT
                date,
                next_date,
                normalized_code,
                event_close,
                next_open,
                next_close,
                close_3d,
                close_5d,
                'PRIME' AS stock_group
            FROM stock_with_membership
            WHERE is_prime
        """,
        "TOPIX100": """
            SELECT
                date,
                next_date,
                normalized_code,
                event_close,
                next_open,
                next_close,
                close_3d,
                close_5d,
                'TOPIX100' AS stock_group
            FROM stock_with_membership
            WHERE is_topix100
        """,
        "TOPIX500": """
            SELECT
                date,
                next_date,
                normalized_code,
                event_close,
                next_open,
                next_close,
                close_3d,
                close_5d,
                'TOPIX500' AS stock_group
            FROM stock_with_membership
            WHERE is_topix500
        """,
        "PRIME ex TOPIX500": """
            SELECT
                date,
                next_date,
                normalized_code,
                event_close,
                next_open,
                next_close,
                close_3d,
                close_5d,
                'PRIME ex TOPIX500' AS stock_group
            FROM stock_with_membership
            WHERE is_prime_ex_topix500
        """,
    }
    union_sql = "\nUNION ALL\n".join(group_selects[group] for group in selected_groups)

    query = f"""
        WITH stocks_snapshot_raw AS (
            SELECT
                {normalized_code_sql} AS normalized_code,
                lower(trim(market_code)) AS market_code_norm,
                coalesce(scale_category, '') AS scale_category,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stocks
        ),
        stocks_snapshot AS (
            SELECT
                normalized_code,
                market_code_norm IN ('prime', '0111') AS is_prime,
                scale_category IN ('TOPIX Core30', 'TOPIX Large70') AS is_topix100,
                scale_category IN ('TOPIX Core30', 'TOPIX Large70', 'TOPIX Mid400') AS is_topix500,
                market_code_norm IN ('prime', '0111')
                    AND scale_category NOT IN ('TOPIX Core30', 'TOPIX Large70', 'TOPIX Mid400') AS is_prime_ex_topix500
            FROM stocks_snapshot_raw
            WHERE row_priority = 1
        ),
        stock_prices_raw AS (
            SELECT
                date,
                {normalized_code_sql} AS normalized_code,
                CAST(open AS DOUBLE) AS open,
                CAST(close AS DOUBLE) AS close,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}, date
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stock_data
            WHERE close IS NOT NULL
        ),
        stock_prices AS (
            SELECT
                date,
                normalized_code,
                open,
                close
            FROM stock_prices_raw
            WHERE row_priority = 1
        ),
        stock_forward AS (
            SELECT
                normalized_code,
                date,
                LEAD(date) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                ) AS next_date,
                close AS event_close,
                LEAD(open) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                ) AS next_open,
                LEAD(close) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                ) AS next_close,
                LEAD(close, 3) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                ) AS close_3d,
                LEAD(close, 5) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                ) AS close_5d
            FROM stock_prices
        ),
        stock_forward_filtered AS (
            SELECT
                normalized_code,
                date,
                next_date,
                event_close,
                next_open,
                next_close,
                close_3d,
                close_5d
            FROM stock_forward
            {date_where_sql}
        ),
        stock_with_membership AS (
            SELECT
                s.date,
                s.next_date,
                s.normalized_code,
                s.event_close,
                s.next_open,
                s.next_close,
                s.close_3d,
                s.close_5d,
                m.is_prime,
                m.is_topix100,
                m.is_topix500,
                m.is_prime_ex_topix500
            FROM stock_forward_filtered s
            JOIN stocks_snapshot m
              ON m.normalized_code = s.normalized_code
        ),
        grouped_stock_days AS (
            {union_sql}
        )
        SELECT
            date,
            next_date,
            stock_group,
            COUNT(*) AS constituent_count,
            AVG(
                CASE
                    WHEN event_close = 0 OR next_open IS NULL THEN NULL
                    ELSE (next_open - event_close) / event_close
                END
            ) AS long_next_overnight_return,
            AVG(
                CASE
                    WHEN next_open IS NULL OR next_open = 0 OR next_close IS NULL THEN NULL
                    ELSE (next_close - next_open) / next_open
                END
            ) AS long_next_intraday_return,
            AVG(
                CASE
                    WHEN event_close = 0 OR next_close IS NULL THEN NULL
                    ELSE (next_close - event_close) / event_close
                END
            ) AS long_next_close_to_close_return,
            AVG(
                CASE
                    WHEN event_close = 0 OR close_3d IS NULL THEN NULL
                    ELSE (close_3d - event_close) / event_close
                END
            ) AS long_forward_3d_close_to_close_return,
            AVG(
                CASE
                    WHEN event_close = 0 OR close_5d IS NULL THEN NULL
                    ELSE (close_5d - event_close) / event_close
                END
            ) AS long_forward_5d_close_to_close_return
        FROM grouped_stock_days
        GROUP BY date, next_date, stock_group
        ORDER BY date, stock_group
    """
    return conn.execute(query, date_params).fetchdf()


def _build_topix_close_stats(
    market_df: pd.DataFrame,
    *,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> TopixCloseReturnStats | None:
    valid = market_df["topix_close_return"].dropna()
    if valid.empty:
        return None
    std_return = float(valid.std(ddof=1))
    if std_return <= 0:
        raise ValueError("topix_close_return standard deviation must be positive")
    return TopixCloseReturnStats(
        sample_count=int(valid.shape[0]),
        mean_return=float(valid.mean()),
        std_return=std_return,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        threshold_1=sigma_threshold_1 * std_return,
        threshold_2=sigma_threshold_2 * std_return,
        min_return=float(valid.min()),
        q25_return=float(valid.quantile(0.25)),
        median_return=float(valid.median()),
        q75_return=float(valid.quantile(0.75)),
        max_return=float(valid.max()),
    )


def _build_nt_ratio_stats(
    market_df: pd.DataFrame,
    *,
    sigma_threshold_1: float,
    sigma_threshold_2: float,
) -> NtRatioReturnStats | None:
    valid = market_df["nt_ratio_return"].dropna()
    if valid.empty:
        return None
    std_return = float(valid.std(ddof=1))
    if std_return <= 0:
        raise ValueError("nt_ratio_return standard deviation must be positive")
    mean_return = float(valid.mean())
    return NtRatioReturnStats(
        sample_count=int(valid.shape[0]),
        mean_return=mean_return,
        std_return=std_return,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        lower_threshold_2=mean_return - sigma_threshold_2 * std_return,
        lower_threshold_1=mean_return - sigma_threshold_1 * std_return,
        upper_threshold_1=mean_return + sigma_threshold_1 * std_return,
        upper_threshold_2=mean_return + sigma_threshold_2 * std_return,
        min_return=float(valid.min()),
        q25_return=float(valid.quantile(0.25)),
        median_return=float(valid.median()),
        q75_return=float(valid.quantile(0.75)),
        max_return=float(valid.max()),
    )


def _bucket_topix_close_return(
    value: float | None,
    *,
    stats: TopixCloseReturnStats | None,
) -> CloseBucketKey | None:
    if value is None or stats is None or pd.isna(value):
        return None
    if value <= -stats.threshold_2:
        return "close_le_negative_threshold_2"
    if value <= -stats.threshold_1:
        return "close_negative_threshold_2_to_1"
    if value < stats.threshold_1:
        return "close_negative_threshold_1_to_threshold_1"
    if value < stats.threshold_2:
        return "close_threshold_1_to_2"
    return "close_ge_threshold_2"


def _bucket_nt_ratio_return(
    value: float | None,
    *,
    stats: NtRatioReturnStats | None,
) -> NtRatioBucketKey | None:
    if value is None or stats is None or pd.isna(value):
        return None
    if value <= stats.lower_threshold_2:
        return "return_le_mean_minus_2sd"
    if value <= stats.lower_threshold_1:
        return "return_mean_minus_2sd_to_minus_1sd"
    if value < stats.upper_threshold_1:
        return "return_mean_minus_1sd_to_plus_1sd"
    if value < stats.upper_threshold_2:
        return "return_mean_plus_1sd_to_plus_2sd"
    return "return_ge_mean_plus_2sd"


def _calculate_market_features(
    market_df: pd.DataFrame,
    *,
    topix_close_stats: TopixCloseReturnStats | None,
    nt_ratio_stats: NtRatioReturnStats | None,
) -> pd.DataFrame:
    enriched = market_df.copy()
    enriched["date"] = enriched["date"].astype(str)
    enriched["topix_close_return"] = enriched["topix_close"].pct_change()
    enriched["nt_ratio"] = enriched["n225_close"] / enriched["topix_close"]
    enriched["nt_ratio_return"] = enriched["nt_ratio"].pct_change()
    enriched["next_date"] = enriched["date"].shift(-1)
    enriched["next_etf_open"] = enriched["etf_open"].shift(-1)
    enriched["next_etf_close"] = enriched["etf_close"].shift(-1)
    enriched["etf_next_overnight_return"] = (
        enriched["next_etf_open"] - enriched["etf_close"]
    ) / enriched["etf_close"]
    enriched["etf_next_intraday_return"] = (
        enriched["next_etf_close"] - enriched["next_etf_open"]
    ) / enriched["next_etf_open"]
    enriched["etf_next_close_to_close_return"] = (
        enriched["next_etf_close"] - enriched["etf_close"]
    ) / enriched["etf_close"]
    enriched["etf_forward_3d_close_to_close_return"] = (
        enriched["etf_close"].shift(-3) - enriched["etf_close"]
    ) / enriched["etf_close"]
    enriched["etf_forward_5d_close_to_close_return"] = (
        enriched["etf_close"].shift(-5) - enriched["etf_close"]
    ) / enriched["etf_close"]
    enriched["topix_ma20"] = enriched["topix_close"].rolling(20, min_periods=20).mean()
    enriched["topix_ma60"] = enriched["topix_close"].rolling(60, min_periods=60).mean()
    enriched["topix_ma20_slope_5d"] = enriched["topix_ma20"] - enriched["topix_ma20"].shift(5)
    enriched["topix_close_bucket_key"] = enriched["topix_close_return"].map(
        lambda value: _bucket_topix_close_return(value, stats=topix_close_stats)
    )
    enriched["nt_ratio_bucket_key"] = enriched["nt_ratio_return"].map(
        lambda value: _bucket_nt_ratio_return(value, stats=nt_ratio_stats)
    )
    enriched["topix_close_bucket_label"] = enriched["topix_close_bucket_key"].map(
        lambda value: (
            format_close_bucket_label(
                cast(CloseBucketKey, value),
                close_threshold_1=topix_close_stats.threshold_1,
                close_threshold_2=topix_close_stats.threshold_2,
            )
            if value is not None and topix_close_stats is not None
            else None
        )
    )
    enriched["nt_ratio_bucket_label"] = enriched["nt_ratio_bucket_key"].map(
        lambda value: (
            format_nt_ratio_bucket_label(
                cast(NtRatioBucketKey, value),
                sigma_threshold_1=nt_ratio_stats.sigma_threshold_1,
                sigma_threshold_2=nt_ratio_stats.sigma_threshold_2,
            )
            if value is not None and nt_ratio_stats is not None
            else None
        )
    )
    enriched["topix_macd_histogram"] = _compute_ema_macd_histogram(
        enriched["topix_close"]
    )
    enriched["trend_ma_bearish"] = (
        enriched["topix_close"].lt(enriched["topix_ma20"])
        & enriched["topix_ma20"].lt(enriched["topix_ma60"])
        & enriched["topix_ma20_slope_5d"].lt(0)
    )
    enriched["trend_ma_bearish"] = enriched["trend_ma_bearish"].fillna(False)
    enriched["trend_macd_negative"] = enriched["topix_macd_histogram"].lt(0).fillna(False)
    shock_topix = enriched["topix_close_bucket_key"].eq("close_le_negative_threshold_2")
    shock_joint = (
        enriched["topix_close_return"].le(
            -(topix_close_stats.threshold_1 if topix_close_stats is not None else np.inf)
        )
        & enriched["nt_ratio_return"].ge(
            nt_ratio_stats.upper_threshold_1 if nt_ratio_stats is not None else np.inf
        )
    )
    enriched["shock_topix_le_negative_threshold_2"] = shock_topix.fillna(False)
    enriched["shock_joint_adverse"] = shock_joint.fillna(False)
    enriched["hybrid_bearish_joint"] = (
        enriched["trend_ma_bearish"] & enriched["shock_joint_adverse"]
    )
    enriched["split"] = np.where(
        enriched["date"] <= _DISCOVERY_END_DATE,
        "discovery",
        "validation",
    )
    enriched["calendar_year"] = pd.to_datetime(enriched["date"]).dt.year.astype(int)
    return enriched


def _build_joint_forward_summary(
    daily_market_df: pd.DataFrame,
    *,
    topix_close_stats: TopixCloseReturnStats | None,
    nt_ratio_stats: NtRatioReturnStats | None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for split_name in SPLIT_ORDER:
        if split_name == "overall":
            split_df = daily_market_df.copy()
        else:
            split_df = daily_market_df[daily_market_df["split"] == split_name].copy()
        for target_name in TARGET_ORDER:
            etf_column = _TARGET_COLUMN_MAP[target_name][1]
            valid = split_df.dropna(
                subset=[
                    "topix_close_bucket_key",
                    "nt_ratio_bucket_key",
                    etf_column,
                ]
            )
            grouped = (
                valid.groupby(
                    ["nt_ratio_bucket_key", "topix_close_bucket_key"],
                    as_index=False,
                )[etf_column]
                .agg(["count", "mean", "median"])
                .reset_index()
            )
            grouped = grouped.rename(
                columns={
                    "count": "event_count",
                    "mean": "mean_etf_return",
                    "median": "median_etf_return",
                }
            )
            positive_rate = (
                valid.groupby(
                    ["nt_ratio_bucket_key", "topix_close_bucket_key"]
                )[etf_column]
                .apply(lambda values: float((values > 0).mean()))
                .reset_index(name="positive_rate")
            )
            merged = grouped.merge(
                positive_rate,
                on=["nt_ratio_bucket_key", "topix_close_bucket_key"],
                how="left",
            )
            for nt_bucket in NT_RATIO_BUCKET_ORDER:
                for topix_bucket in CLOSE_BUCKET_ORDER:
                    row = merged[
                        (merged["nt_ratio_bucket_key"] == nt_bucket)
                        & (merged["topix_close_bucket_key"] == topix_bucket)
                    ]
                    if row.empty:
                        rows.append(
                            {
                                "split": split_name,
                                "target_name": target_name,
                                "nt_ratio_bucket_key": nt_bucket,
                                "nt_ratio_bucket_label": (
                                    format_nt_ratio_bucket_label(
                                        nt_bucket,
                                        sigma_threshold_1=nt_ratio_stats.sigma_threshold_1,
                                        sigma_threshold_2=nt_ratio_stats.sigma_threshold_2,
                                    )
                                    if nt_ratio_stats is not None
                                    else None
                                ),
                                "topix_close_bucket_key": topix_bucket,
                                "topix_close_bucket_label": (
                                    format_close_bucket_label(
                                        topix_bucket,
                                        close_threshold_1=topix_close_stats.threshold_1,
                                        close_threshold_2=topix_close_stats.threshold_2,
                                    )
                                    if topix_close_stats is not None
                                    else None
                                ),
                                "event_count": 0,
                                "mean_etf_return": np.nan,
                                "median_etf_return": np.nan,
                                "positive_rate": np.nan,
                            }
                        )
                        continue
                    first = row.iloc[0]
                    rows.append(
                        {
                            "split": split_name,
                            "target_name": target_name,
                            "nt_ratio_bucket_key": nt_bucket,
                            "nt_ratio_bucket_label": first.get("nt_ratio_bucket_label"),
                            "topix_close_bucket_key": topix_bucket,
                            "topix_close_bucket_label": first.get("topix_close_bucket_label"),
                            "event_count": int(first["event_count"]),
                            "mean_etf_return": float(first["mean_etf_return"]),
                            "median_etf_return": float(first["median_etf_return"]),
                            "positive_rate": float(first["positive_rate"]),
                        }
                    )
    summary_df = pd.DataFrame(rows)
    label_lookup = (
        daily_market_df.dropna(
            subset=["nt_ratio_bucket_key", "nt_ratio_bucket_label"]
        )
        .drop_duplicates(subset=["nt_ratio_bucket_key"])[
            ["nt_ratio_bucket_key", "nt_ratio_bucket_label"]
        ]
    )
    topix_label_lookup = (
        daily_market_df.dropna(
            subset=["topix_close_bucket_key", "topix_close_bucket_label"]
        )
        .drop_duplicates(subset=["topix_close_bucket_key"])[
            ["topix_close_bucket_key", "topix_close_bucket_label"]
        ]
    )
    summary_df = summary_df.drop(columns=["nt_ratio_bucket_label", "topix_close_bucket_label"])
    summary_df = summary_df.merge(label_lookup, on="nt_ratio_bucket_key", how="left")
    summary_df = summary_df.merge(
        topix_label_lookup,
        on="topix_close_bucket_key",
        how="left",
    )
    return summary_df


def _run_lengths(signal: pd.Series) -> list[int]:
    run_lengths: list[int] = []
    current_run = 0
    for value in signal.fillna(False).astype(bool):
        if value:
            current_run += 1
            continue
        if current_run:
            run_lengths.append(current_run)
            current_run = 0
    if current_run:
        run_lengths.append(current_run)
    return run_lengths


def _build_rule_signal_summary(daily_market_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for split_name in SPLIT_ORDER:
        if split_name == "overall":
            split_df = daily_market_df.copy()
        else:
            split_df = daily_market_df[daily_market_df["split"] == split_name].copy()
        total_days = int(split_df.shape[0])
        for rule_name in RULE_ORDER:
            signal = split_df[rule_name].fillna(False).astype(bool)
            run_lengths = _run_lengths(signal)
            transitions = int(signal.astype(int).diff().abs().fillna(signal.astype(int)).sum())
            rows.append(
                {
                    "split": split_name,
                    "rule_name": rule_name,
                    "total_days": total_days,
                    "active_day_count": int(signal.sum()),
                    "active_ratio": float(signal.mean()) if total_days else np.nan,
                    "transitions": transitions,
                    "average_run_length": (
                        float(np.mean(run_lengths)) if run_lengths else np.nan
                    ),
                    "max_run_length": int(max(run_lengths)) if run_lengths else 0,
                }
            )
    return pd.DataFrame(rows)


def _expected_shortfall(series: pd.Series, tail_probability: float = 0.05) -> float | None:
    valid = series.dropna().sort_values()
    if valid.empty:
        return None
    tail_count = max(1, int(np.ceil(len(valid) * tail_probability)))
    return float(valid.iloc[:tail_count].mean())


def _max_drawdown(series: pd.Series) -> float | None:
    valid = series.dropna()
    if valid.empty:
        return None
    cumulative = (1.0 + valid).cumprod()
    peaks = cumulative.cummax()
    drawdown = 1.0 - (cumulative / peaks)
    return float(drawdown.max())


def _split_filter(df: pd.DataFrame, split_name: SplitName) -> pd.DataFrame:
    if split_name == "overall":
        return df.copy()
    return df[df["split"] == split_name].copy()


def _build_beta_neutral_weights(daily_proxy_returns_df: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for stock_group, group_df in daily_proxy_returns_df.groupby("stock_group", sort=False):
        ordered = group_df.sort_values("date").copy()
        long_returns = ordered["long_next_close_to_close_return"].shift(1)
        etf_returns = ordered["etf_next_close_to_close_return"].shift(1)
        covariance = long_returns.rolling(
            _BETA_LOOKBACK_DAYS,
            min_periods=_BETA_MIN_PERIODS,
        ).cov(etf_returns)
        variance = etf_returns.rolling(
            _BETA_LOOKBACK_DAYS,
            min_periods=_BETA_MIN_PERIODS,
        ).var()
        weights = (-covariance / variance).replace([np.inf, -np.inf], np.nan)
        ordered["beta_neutral_weight"] = weights.clip(lower=0.0, upper=1.0)
        ordered["beta_neutral_weight_effective"] = ordered["beta_neutral_weight"].fillna(0.0)
        frames.append(ordered)
    if not frames:
        return daily_proxy_returns_df
    return pd.concat(frames, ignore_index=True)


def _evaluate_hedge_config(
    df: pd.DataFrame,
    *,
    target_name: TargetName,
    rule_name: RuleName,
    weight_label: str,
    fixed_weight: float | None,
) -> dict[str, Any]:
    long_column, etf_column = _TARGET_COLUMN_MAP[target_name]
    required_columns = [long_column, etf_column, rule_name]
    if fixed_weight is None:
        required_columns.extend(
            ["beta_neutral_weight", "beta_neutral_weight_effective"]
        )
    valid = df[required_columns].dropna(subset=[long_column, etf_column, rule_name]).copy()
    signal = valid[rule_name].astype(bool)
    if fixed_weight is None:
        raw_weight = valid["beta_neutral_weight_effective"].astype(float)
        mean_weight = (
            float(valid.loc[signal, "beta_neutral_weight"].dropna().mean())
            if signal.any()
            else np.nan
        )
    else:
        raw_weight = pd.Series(float(fixed_weight), index=valid.index)
        mean_weight = float(fixed_weight) if signal.any() else np.nan
    applied_weight = raw_weight.where(signal, 0.0)
    unhedged = valid[long_column]
    hedged = unhedged + applied_weight * valid[etf_column]
    hedge_delta = hedged - unhedged
    stress_mask = unhedged < 0
    non_stress_mask = ~stress_mask
    es_unhedged = _expected_shortfall(unhedged)
    es_hedged = _expected_shortfall(hedged)
    mdd_unhedged = _max_drawdown(unhedged)
    mdd_hedged = _max_drawdown(hedged)
    stress_unhedged = unhedged[stress_mask]
    stress_hedged = hedged[stress_mask]
    non_stress_delta = hedge_delta[non_stress_mask]
    down_day_hit_rate = (
        float((stress_hedged > stress_unhedged).mean())
        if not stress_unhedged.empty
        else np.nan
    )
    return {
        "target_name": target_name,
        "rule_name": rule_name,
        "weight_label": weight_label,
        "sample_count": int(valid.shape[0]),
        "active_day_count": int(signal.sum()),
        "active_ratio": float(signal.mean()) if not valid.empty else np.nan,
        "mean_weight_when_active": mean_weight,
        "unhedged_mean_return": float(unhedged.mean()) if not unhedged.empty else np.nan,
        "hedged_mean_return": float(hedged.mean()) if not hedged.empty else np.nan,
        "hedge_pnl_mean": float(hedge_delta.mean()) if not hedge_delta.empty else np.nan,
        "stress_day_count": int(stress_mask.sum()),
        "unhedged_stress_mean_return": (
            float(stress_unhedged.mean()) if not stress_unhedged.empty else np.nan
        ),
        "hedged_stress_mean_return": (
            float(stress_hedged.mean()) if not stress_hedged.empty else np.nan
        ),
        "stress_mean_loss_improvement": (
            float(stress_hedged.mean() - stress_unhedged.mean())
            if not stress_unhedged.empty
            else np.nan
        ),
        "expected_shortfall_5_unhedged": (
            float(es_unhedged) if es_unhedged is not None else np.nan
        ),
        "expected_shortfall_5_hedged": (
            float(es_hedged) if es_hedged is not None else np.nan
        ),
        "expected_shortfall_improvement": (
            float(es_hedged - es_unhedged)
            if es_unhedged is not None and es_hedged is not None
            else np.nan
        ),
        "max_drawdown_unhedged": (
            float(mdd_unhedged) if mdd_unhedged is not None else np.nan
        ),
        "max_drawdown_hedged": float(mdd_hedged) if mdd_hedged is not None else np.nan,
        "max_drawdown_improvement": (
            float(mdd_unhedged - mdd_hedged)
            if mdd_unhedged is not None and mdd_hedged is not None
            else np.nan
        ),
        "down_day_hit_rate": down_day_hit_rate,
        "carry_cost_non_stress": (
            float(non_stress_delta.mean()) if not non_stress_delta.empty else np.nan
        ),
    }


def _build_hedge_metrics(
    daily_proxy_returns_df: pd.DataFrame,
    *,
    fixed_weights: tuple[float, ...],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    weight_specs: list[tuple[str, float | None]] = [
        (f"fixed_{weight:.2f}", weight) for weight in fixed_weights
    ]
    weight_specs.append(("beta_neutral_60d", None))
    for stock_group, group_df in daily_proxy_returns_df.groupby("stock_group", sort=False):
        for split_name in SPLIT_ORDER:
            split_df = _split_filter(group_df, split_name)
            for target_name in TARGET_ORDER:
                for rule_name in RULE_ORDER:
                    for weight_label, fixed_weight in weight_specs:
                        metrics = _evaluate_hedge_config(
                            split_df,
                            target_name=target_name,
                            rule_name=rule_name,
                            weight_label=weight_label,
                            fixed_weight=fixed_weight,
                        )
                        metrics.update(
                            {
                                "split": split_name,
                                "stock_group": stock_group,
                            }
                        )
                        rows.append(metrics)
    return pd.DataFrame(rows)


def _evaluate_etf_strategy(
    df: pd.DataFrame,
    *,
    target_name: TargetName,
    rule_name: RuleName,
) -> dict[str, Any]:
    etf_column = _TARGET_COLUMN_MAP[target_name][1]
    valid = df[[etf_column, rule_name]].dropna(subset=[etf_column, rule_name]).copy()
    signal = valid[rule_name].astype(bool)
    etf_returns = valid[etf_column].astype(float)
    strategy_returns = etf_returns.where(signal, 0.0)
    active_returns = etf_returns[signal]
    expected_shortfall = _expected_shortfall(strategy_returns)
    max_drawdown = _max_drawdown(strategy_returns)
    strategy_total_return = (
        float((1.0 + strategy_returns).prod() - 1.0) if not strategy_returns.empty else np.nan
    )
    return {
        "target_name": target_name,
        "rule_name": rule_name,
        "sample_count": int(valid.shape[0]),
        "active_day_count": int(signal.sum()),
        "active_ratio": float(signal.mean()) if not valid.empty else np.nan,
        "mean_return_when_active": (
            float(active_returns.mean()) if not active_returns.empty else np.nan
        ),
        "strategy_mean_return": (
            float(strategy_returns.mean()) if not strategy_returns.empty else np.nan
        ),
        "strategy_total_return": strategy_total_return,
        "expected_shortfall_5": (
            float(expected_shortfall) if expected_shortfall is not None else np.nan
        ),
        "max_drawdown": float(max_drawdown) if max_drawdown is not None else np.nan,
        "positive_rate_when_active": (
            float((active_returns > 0).mean()) if not active_returns.empty else np.nan
        ),
    }


def _build_etf_strategy_metrics(daily_market_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for split_name in SPLIT_ORDER:
        split_df = _split_filter(daily_market_df, split_name)
        for target_name in TARGET_ORDER:
            for rule_name in RULE_ORDER:
                metrics = _evaluate_etf_strategy(
                    split_df,
                    target_name=target_name,
                    rule_name=rule_name,
                )
                metrics["split"] = split_name
                rows.append(metrics)
    return pd.DataFrame(rows)


def _build_etf_strategy_split_comparison(
    etf_strategy_metrics_df: pd.DataFrame,
) -> pd.DataFrame:
    split_df = etf_strategy_metrics_df[
        etf_strategy_metrics_df["split"].isin(["discovery", "validation"])
    ].copy()
    index_columns = ["target_name", "rule_name"]
    value_columns = [
        "sample_count",
        "active_day_count",
        "active_ratio",
        "mean_return_when_active",
        "strategy_mean_return",
        "strategy_total_return",
        "expected_shortfall_5",
        "max_drawdown",
        "positive_rate_when_active",
    ]
    pivot = split_df.pivot_table(
        index=index_columns,
        columns="split",
        values=value_columns,
        aggfunc="first",
    )
    if pivot.empty:
        return pd.DataFrame(columns=index_columns)
    pivot.columns = [f"{metric}_{split_name}" for metric, split_name in pivot.columns]
    pivot = pivot.reset_index()
    for metric in value_columns:
        for split_name in ("discovery", "validation"):
            column_name = f"{metric}_{split_name}"
            if column_name not in pivot.columns:
                pivot[column_name] = np.nan
    return pivot


def _build_split_comparison(hedge_metrics_df: pd.DataFrame) -> pd.DataFrame:
    split_df = hedge_metrics_df[hedge_metrics_df["split"].isin(["discovery", "validation"])].copy()
    index_columns = ["stock_group", "target_name", "rule_name", "weight_label"]
    value_columns = [
        "sample_count",
        "active_day_count",
        "active_ratio",
        "mean_weight_when_active",
        "stress_day_count",
        "stress_mean_loss_improvement",
        "expected_shortfall_improvement",
        "max_drawdown_improvement",
        "down_day_hit_rate",
        "carry_cost_non_stress",
    ]
    pivot = split_df.pivot_table(
        index=index_columns,
        columns="split",
        values=value_columns,
        aggfunc="first",
    )
    if pivot.empty:
        return pd.DataFrame(columns=index_columns)
    flattened_columns: list[str] = []
    for left, right in pivot.columns:
        if left in {"discovery", "validation"}:
            flattened_columns.append(f"{right}_{left}")
        else:
            flattened_columns.append(f"{left}_{right}")
    pivot.columns = flattened_columns
    pivot = pivot.reset_index()
    expected_metric_columns = [
        "sample_count",
        "active_day_count",
        "active_ratio",
        "mean_weight_when_active",
        "stress_day_count",
        "stress_mean_loss_improvement",
        "expected_shortfall_improvement",
        "max_drawdown_improvement",
        "down_day_hit_rate",
        "carry_cost_non_stress",
    ]
    for metric in expected_metric_columns:
        for split_name in ("discovery", "validation"):
            column_name = f"{metric}_{split_name}"
            if column_name not in pivot.columns:
                pivot[column_name] = np.nan
    return pivot


def _build_shortlist(split_comparison_df: pd.DataFrame) -> pd.DataFrame:
    if split_comparison_df.empty:
        return pd.DataFrame(
            columns=[
                "stock_group",
                "target_name",
                "rule_name",
                "weight_label",
                "score",
                "selection_status",
            ]
        )
    shortlist = split_comparison_df.copy()
    shortlist["qualified"] = (
        shortlist["active_day_count_discovery"].fillna(0).ge(20)
        & shortlist["active_day_count_validation"].fillna(0).ge(20)
        & shortlist["stress_mean_loss_improvement_discovery"].fillna(-np.inf).gt(0)
        & shortlist["stress_mean_loss_improvement_validation"].fillna(-np.inf).gt(0)
        & shortlist["expected_shortfall_improvement_discovery"].fillna(-np.inf).gt(0)
        & shortlist["expected_shortfall_improvement_validation"].fillna(-np.inf).gt(0)
        & shortlist["carry_cost_non_stress_discovery"].fillna(-np.inf).ge(
            -0.5 * shortlist["stress_mean_loss_improvement_discovery"].fillna(np.inf)
        )
        & shortlist["carry_cost_non_stress_validation"].fillna(-np.inf).ge(
            -0.5 * shortlist["stress_mean_loss_improvement_validation"].fillna(np.inf)
        )
    )
    qualified = shortlist[shortlist["qualified"]].copy()
    if qualified.empty:
        qualified = shortlist[
            shortlist["active_day_count_discovery"].fillna(0).ge(10)
            & shortlist["active_day_count_validation"].fillna(0).ge(10)
        ].copy()
        selection_status = "fallback"
    else:
        selection_status = "qualified"
    if qualified.empty:
        return pd.DataFrame(
            columns=[
                "stock_group",
                "target_name",
                "rule_name",
                "weight_label",
                "score",
                "selection_status",
            ]
        )
    qualified["score"] = (
        qualified["expected_shortfall_improvement_discovery"].fillna(0)
        + qualified["expected_shortfall_improvement_validation"].fillna(0)
        + qualified["stress_mean_loss_improvement_discovery"].fillna(0)
        + qualified["stress_mean_loss_improvement_validation"].fillna(0)
        + 0.5 * qualified["max_drawdown_improvement_discovery"].fillna(0)
        + 0.5 * qualified["max_drawdown_improvement_validation"].fillna(0)
    )
    qualified["selection_status"] = selection_status
    columns = [
        "stock_group",
        "target_name",
        "rule_name",
        "weight_label",
        "score",
        "selection_status",
        "active_day_count_discovery",
        "active_day_count_validation",
        "stress_mean_loss_improvement_discovery",
        "stress_mean_loss_improvement_validation",
        "expected_shortfall_improvement_discovery",
        "expected_shortfall_improvement_validation",
        "carry_cost_non_stress_discovery",
        "carry_cost_non_stress_validation",
    ]
    return qualified.sort_values("score", ascending=False)[columns].head(3).reset_index(drop=True)


def _build_annual_rule_summary(
    daily_proxy_returns_df: pd.DataFrame,
    *,
    fixed_weights: tuple[float, ...],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    weight_specs: list[tuple[str, float | None]] = [
        (f"fixed_{weight:.2f}", weight) for weight in fixed_weights
    ]
    weight_specs.append(("beta_neutral_60d", None))
    for (stock_group, year), group_df in daily_proxy_returns_df.groupby(
        ["stock_group", "calendar_year"],
        sort=False,
    ):
        for target_name in TARGET_ORDER:
            for rule_name in RULE_ORDER:
                for weight_label, fixed_weight in weight_specs:
                    metrics = _evaluate_hedge_config(
                        group_df,
                        target_name=target_name,
                        rule_name=rule_name,
                        weight_label=weight_label,
                        fixed_weight=fixed_weight,
                    )
                    metrics.update(
                        {
                            "stock_group": stock_group,
                            "calendar_year": int(year),
                        }
                    )
                    rows.append(metrics)
    return pd.DataFrame(rows)


def get_1357_nt_ratio_topix_available_date_range(
    db_path: str,
) -> tuple[str | None, str | None]:
    with _open_analysis_connection(db_path) as ctx:
        market_df = _query_market_daily_frame(ctx.connection)
    return str(market_df["date"].min()), str(market_df["date"].max())


def run_1357_nt_ratio_topix_hedge_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    sigma_threshold_1: float = 1.0,
    sigma_threshold_2: float = 2.0,
    selected_groups: Sequence[str] | None = None,
    fixed_weights: Sequence[float] = (0.1, 0.2, 0.3, 0.4, 0.5),
) -> Hedge1357NtRatioTopixResearchResult:
    validated_groups = _validate_selected_groups(selected_groups)
    validated_weights = _validate_fixed_weights(fixed_weights)
    with _open_analysis_connection(db_path) as ctx:
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail
        market_df_raw = _query_market_daily_frame(ctx.connection)
        available_start_date = str(market_df_raw["date"].min()) if not market_df_raw.empty else None
        available_end_date = str(market_df_raw["date"].max()) if not market_df_raw.empty else None
        market_df_filtered = _filter_date_range_df(
            market_df_raw,
            start_date=start_date,
            end_date=end_date,
        )
        if market_df_filtered.empty:
            raise ValueError("No aligned market rows were found in the selected date range")
        stats_input_df = market_df_filtered.copy()
        stats_input_df["topix_close_return"] = stats_input_df["topix_close"].pct_change()
        stats_input_df["nt_ratio"] = stats_input_df["n225_close"] / stats_input_df["topix_close"]
        stats_input_df["nt_ratio_return"] = stats_input_df["nt_ratio"].pct_change()
        topix_close_stats = _build_topix_close_stats(
            stats_input_df,
            sigma_threshold_1=sigma_threshold_1,
            sigma_threshold_2=sigma_threshold_2,
        )
        nt_ratio_stats = _build_nt_ratio_stats(
            stats_input_df,
            sigma_threshold_1=sigma_threshold_1,
            sigma_threshold_2=sigma_threshold_2,
        )
        daily_market_df = _calculate_market_features(
            market_df_filtered,
            topix_close_stats=topix_close_stats,
            nt_ratio_stats=nt_ratio_stats,
        )
        group_returns_df = _query_group_daily_returns(
            ctx.connection,
            start_date=start_date,
            end_date=end_date,
            selected_groups=validated_groups,
        )
    daily_proxy_returns_df = group_returns_df.merge(
        daily_market_df[
            [
                "date",
                "next_date",
                "split",
                "calendar_year",
                "topix_close",
                "topix_close_return",
                "topix_close_bucket_key",
                "topix_close_bucket_label",
                "nt_ratio",
                "nt_ratio_return",
                "nt_ratio_bucket_key",
                "nt_ratio_bucket_label",
                "topix_ma20",
                "topix_ma60",
                "topix_ma20_slope_5d",
                "topix_macd_histogram",
                "etf_next_overnight_return",
                "etf_next_intraday_return",
                "etf_next_close_to_close_return",
                "etf_forward_3d_close_to_close_return",
                "etf_forward_5d_close_to_close_return",
                "shock_topix_le_negative_threshold_2",
                "shock_joint_adverse",
                "trend_ma_bearish",
                "trend_macd_negative",
                "hybrid_bearish_joint",
            ]
        ],
        on=["date", "next_date"],
        how="inner",
    )
    daily_proxy_returns_df = _build_beta_neutral_weights(daily_proxy_returns_df)
    analysis_valid = daily_proxy_returns_df.dropna(
        subset=[
            "topix_close_return",
            "nt_ratio_return",
            "etf_next_overnight_return",
            "etf_next_intraday_return",
            "etf_next_close_to_close_return",
            "etf_forward_3d_close_to_close_return",
            "etf_forward_5d_close_to_close_return",
            "long_next_overnight_return",
            "long_next_intraday_return",
            "long_next_close_to_close_return",
            "long_forward_3d_close_to_close_return",
            "long_forward_5d_close_to_close_return",
        ]
    )
    analysis_start_date = (
        str(analysis_valid["date"].min()) if not analysis_valid.empty else None
    )
    analysis_end_date = str(analysis_valid["date"].max()) if not analysis_valid.empty else None
    joint_forward_summary_df = _build_joint_forward_summary(
        daily_market_df,
        topix_close_stats=topix_close_stats,
        nt_ratio_stats=nt_ratio_stats,
    )
    rule_signal_summary_df = _build_rule_signal_summary(daily_market_df)
    hedge_metrics_df = _build_hedge_metrics(
        daily_proxy_returns_df,
        fixed_weights=validated_weights,
    )
    etf_strategy_metrics_df = _build_etf_strategy_metrics(daily_market_df)
    etf_strategy_split_comparison_df = _build_etf_strategy_split_comparison(
        etf_strategy_metrics_df
    )
    split_comparison_df = _build_split_comparison(hedge_metrics_df)
    annual_rule_summary_df = _build_annual_rule_summary(
        daily_proxy_returns_df,
        fixed_weights=validated_weights,
    )
    shortlist_df = _build_shortlist(split_comparison_df)
    return Hedge1357NtRatioTopixResearchResult(
        db_path=db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        discovery_end_date=_DISCOVERY_END_DATE,
        validation_start_date=_VALIDATION_START_DATE,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        selected_groups=validated_groups,
        fixed_weights=validated_weights,
        macd_basis=_MACD_BASIS,
        macd_fast_period=_MACD_FAST_PERIOD,
        macd_slow_period=_MACD_SLOW_PERIOD,
        macd_signal_period=_MACD_SIGNAL_PERIOD,
        topix_close_stats=topix_close_stats,
        nt_ratio_stats=nt_ratio_stats,
        daily_market_df=daily_market_df,
        daily_proxy_returns_df=daily_proxy_returns_df,
        joint_forward_summary_df=joint_forward_summary_df,
        rule_signal_summary_df=rule_signal_summary_df,
        hedge_metrics_df=hedge_metrics_df,
        etf_strategy_metrics_df=etf_strategy_metrics_df,
        etf_strategy_split_comparison_df=etf_strategy_split_comparison_df,
        split_comparison_df=split_comparison_df,
        annual_rule_summary_df=annual_rule_summary_df,
        shortlist_df=shortlist_df,
    )
