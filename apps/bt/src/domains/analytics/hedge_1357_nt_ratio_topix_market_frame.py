from __future__ import annotations

from typing import Any, cast

import numpy as np
import pandas as pd

from src.domains.analytics.hedge_1357_nt_ratio_topix_support import (
    CloseBucketKey,
    NtRatioBucketKey,
    NtRatioReturnStats,
    StockGroup,
    TopixCloseReturnStats,
    _DISCOVERY_END_DATE,
    _ETF_CODE,
    _MACD_FAST_PERIOD,
    _MACD_SIGNAL_PERIOD,
    _MACD_SLOW_PERIOD,
    _NIKKEI_SYNTHETIC_INDEX_CODE,
    _date_where_clause,
    _normalize_code_sql,
    format_close_bucket_label,
    format_nt_ratio_bucket_label,
)
from src.domains.strategy.indicators import compute_moving_average


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
    selected_groups: tuple[StockGroup, ...],
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


__all__ = [
    "_build_nt_ratio_stats",
    "_build_topix_close_stats",
    "_bucket_nt_ratio_return",
    "_bucket_topix_close_return",
    "_calculate_market_features",
    "_query_group_daily_returns",
    "_query_market_daily_frame",
]
