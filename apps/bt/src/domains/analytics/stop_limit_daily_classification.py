"""Classify stop-limit days from the daily OHLC market database.

This study uses the main daily ``stock_data`` table only. It classifies days
that touched the standard TSE daily price limits (stop high / stop low),
groups them by the latest ``stocks`` market snapshot, and separates one-price
days from days that showed intraday range.

Rows that move outside the standard daily price-limit band are not folded into
the primary event set because they likely require broadened-limit or special
quote handling. They are emitted separately for follow-up.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd

from src.domains.analytics.jpx_daily_price_limits import (
    JPX_DAILY_PRICE_LIMITS_REFERENCE_LABEL,
    build_standard_daily_limit_table_df,
    build_standard_daily_limit_width_case_sql,
)
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
    load_research_bundle_info,
    load_research_bundle_tables,
    write_dataclass_research_bundle,
)

StopLimitSide = Literal["stop_high", "stop_low", "both"]
OutsideStandardSide = Literal["above_standard_upper", "below_standard_lower", "both"]
ForwardReturnHorizon = Literal["next_open", "next_close", "close_3d", "close_5d"]
CandidateStrategyFamily = Literal[
    "continuation_single_price",
    "reversal_intraday_off_limit_close",
]

STOP_LIMIT_DAILY_CLASSIFICATION_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/stop-limit-daily-classification"
)
UNMAPPED_LATEST_MARKET_CODE = "unmapped_latest_stocks"
UNMAPPED_LATEST_MARKET_NAME = "UNMAPPED_LATEST_STOCKS"
STOP_LIMIT_SIDE_ORDER: tuple[StopLimitSide, ...] = ("stop_high", "stop_low", "both")
OUTSIDE_STANDARD_SIDE_ORDER: tuple[OutsideStandardSide, ...] = (
    "above_standard_upper",
    "below_standard_lower",
    "both",
)
FORWARD_RETURN_ORDER: tuple[ForwardReturnHorizon, ...] = (
    "next_open",
    "next_close",
    "close_3d",
    "close_5d",
)
FORWARD_RETURN_SPECS: tuple[tuple[ForwardReturnHorizon, str, str, str], ...] = (
    (
        "next_open",
        "next_open_return",
        "next_open_directional_return",
        "event close -> next session open",
    ),
    (
        "next_close",
        "next_close_return",
        "next_close_directional_return",
        "event close -> next session close",
    ),
    (
        "close_3d",
        "close_3d_return",
        "close_3d_directional_return",
        "event close -> close +3 sessions",
    ),
    (
        "close_5d",
        "close_5d_return",
        "close_5d_directional_return",
        "event close -> close +5 sessions",
    ),
)
TRADE_RETURN_SPECS: tuple[tuple[str, str, str], ...] = (
    (
        "next_open_to_next_close",
        "next_open_to_next_close_return",
        "next session open -> next session close",
    ),
    (
        "next_open_to_close_3d",
        "next_open_to_close_3d_return",
        "next session open -> close +3 sessions",
    ),
    (
        "next_open_to_close_5d",
        "next_open_to_close_5d_return",
        "next session open -> close +5 sessions",
    ),
)
CONTINUATION_GROUP_COLUMNS: tuple[str, ...] = (
    "market_code",
    "market_name",
    "limit_side",
    "intraday_state",
    "close_limit_state",
)
TRADE_CANDIDATE_GROUP_COLUMNS: tuple[str, ...] = (
    "market_code",
    "market_name",
    "candidate_strategy_family",
    "candidate_strategy_label",
    "trade_direction_label",
    "limit_side",
)
_EPSILON = 1e-6
_PREFER_4DIGIT_ORDER_SQL = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"


@dataclass(frozen=True)
class StopLimitDailyClassificationResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    total_event_count: int
    total_directional_event_count: int
    total_outside_standard_band_count: int
    unmapped_latest_market_event_count: int
    jpx_reference_label: str
    classification_note: str
    continuation_note: str
    candidate_strategy_note: str
    limit_table_df: pd.DataFrame
    event_df: pd.DataFrame
    summary_df: pd.DataFrame
    continuation_summary_df: pd.DataFrame
    candidate_trade_summary_df: pd.DataFrame
    outside_standard_band_df: pd.DataFrame
    outside_standard_band_summary_df: pd.DataFrame

def _classified_stock_days_cte(
    *,
    start_date: str | None,
    end_date: str | None,
) -> tuple[str, list[str]]:
    normalized_code_sql = normalize_code_sql("code")
    limit_width_sql = build_standard_daily_limit_width_case_sql("prev_close")
    single_price_day_sql = (
        f"ABS(open - high) <= {_EPSILON}"
        f" AND ABS(high - low) <= {_EPSILON}"
        f" AND ABS(low - close) <= {_EPSILON}"
    )
    stock_where_sql, stock_params = date_where_clause("date", start_date, end_date)
    cte_sql = f"""
        WITH stocks_snapshot AS (
            SELECT
                normalized_code,
                market_code,
                market_name
            FROM (
                SELECT
                    {normalized_code_sql} AS normalized_code,
                    NULLIF(trim(market_code), '') AS market_code,
                    NULLIF(trim(market_name), '') AS market_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized_code_sql}
                        ORDER BY {_PREFER_4DIGIT_ORDER_SQL}, code
                    ) AS row_priority
                FROM stocks
            )
            WHERE row_priority = 1
        ),
        stock_daily AS (
            SELECT
                date,
                normalized_code,
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
                {stock_where_sql}
            )
            WHERE row_priority = 1
                AND open IS NOT NULL
                AND high IS NOT NULL
                AND low IS NOT NULL
                AND close IS NOT NULL
        ),
        stock_with_prev AS (
            SELECT
                date,
                normalized_code AS code,
                open,
                high,
                low,
                close,
                volume,
                LEAD(date) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                ) AS next_date,
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
                ) AS close_5d,
                LAG(close) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                ) AS prev_close
            FROM stock_daily
        ),
        classified_raw AS (
            SELECT
                stock_with_prev.date,
                stock_with_prev.code,
                COALESCE(stocks_snapshot.market_code, '{UNMAPPED_LATEST_MARKET_CODE}') AS market_code,
                COALESCE(stocks_snapshot.market_name, '{UNMAPPED_LATEST_MARKET_NAME}') AS market_name,
                stocks_snapshot.normalized_code IS NOT NULL AS latest_market_mapped,
                stock_with_prev.open,
                stock_with_prev.high,
                stock_with_prev.low,
                stock_with_prev.close,
                stock_with_prev.volume,
                stock_with_prev.next_date,
                stock_with_prev.next_open,
                stock_with_prev.next_close,
                stock_with_prev.close_3d,
                stock_with_prev.close_5d,
                stock_with_prev.prev_close,
                {limit_width_sql} AS limit_width,
                {single_price_day_sql} AS single_price_day
            FROM stock_with_prev
            LEFT JOIN stocks_snapshot
                ON stocks_snapshot.normalized_code = stock_with_prev.code
            WHERE stock_with_prev.prev_close IS NOT NULL
                AND stock_with_prev.prev_close > 0
        ),
        classified AS (
            SELECT
                *,
                prev_close + limit_width AS upper_limit,
                prev_close - limit_width AS lower_limit,
                ABS(high - (prev_close + limit_width)) <= {_EPSILON} AS hit_stop_high,
                ABS(low - (prev_close - limit_width)) <= {_EPSILON} AS hit_stop_low,
                ABS(close - (prev_close + limit_width)) <= {_EPSILON} AS closed_at_upper_limit,
                ABS(close - (prev_close - limit_width)) <= {_EPSILON} AS closed_at_lower_limit,
                NOT single_price_day AS intraday_range,
                high > prev_close + limit_width + {_EPSILON} AS above_standard_band,
                low < prev_close - limit_width - {_EPSILON} AS below_standard_band
            FROM classified_raw
            WHERE limit_width IS NOT NULL
        )
    """
    return cte_sql, stock_params


def _query_event_df(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    cte_sql, params = _classified_stock_days_cte(start_date=start_date, end_date=end_date)
    sql = f"""
        {cte_sql}
        , event_rows AS (
            SELECT
                date,
                code,
                market_code,
                market_name,
                latest_market_mapped,
                prev_close,
                limit_width,
                upper_limit,
                lower_limit,
                open,
                high,
                low,
                close,
                volume,
                next_date,
                next_open,
                next_close,
                close_3d,
                close_5d,
                hit_stop_high,
                hit_stop_low,
                CASE
                    WHEN hit_stop_high AND hit_stop_low THEN 'both'
                    WHEN hit_stop_high THEN 'stop_high'
                    ELSE 'stop_low'
                END AS limit_side,
                CASE
                    WHEN hit_stop_high AND hit_stop_low THEN NULL
                    WHEN hit_stop_high THEN 1.0
                    ELSE -1.0
                END AS event_direction_sign,
                single_price_day,
                intraday_range,
                CASE
                    WHEN intraday_range THEN 'intraday_range'
                    ELSE 'single_price'
                END AS intraday_state,
                closed_at_upper_limit,
                closed_at_lower_limit,
                CASE
                    WHEN closed_at_upper_limit AND closed_at_lower_limit THEN 'both'
                    WHEN closed_at_upper_limit THEN 'stop_high'
                    WHEN closed_at_lower_limit THEN 'stop_low'
                    ELSE 'off_limit_close'
                END AS close_limit_state
            FROM classified
            WHERE NOT above_standard_band
                AND NOT below_standard_band
                AND (hit_stop_high OR hit_stop_low)
        )
        SELECT
            date,
            code,
            market_code,
            market_name,
            latest_market_mapped,
            prev_close,
            limit_width,
            upper_limit,
            lower_limit,
            open,
            high,
            low,
            close,
            volume,
            next_date,
            next_open,
            next_close,
            close_3d,
            close_5d,
            hit_stop_high,
            hit_stop_low,
            limit_side,
            event_direction_sign,
            single_price_day,
            intraday_range,
            intraday_state,
            closed_at_upper_limit,
            closed_at_lower_limit,
            close_limit_state,
            CASE
                WHEN next_open IS NULL OR ABS(close) <= {_EPSILON} THEN NULL
                ELSE next_open / close - 1.0
            END AS next_open_return,
            CASE
                WHEN next_close IS NULL OR ABS(close) <= {_EPSILON} THEN NULL
                ELSE next_close / close - 1.0
            END AS next_close_return,
            CASE
                WHEN close_3d IS NULL OR ABS(close) <= {_EPSILON} THEN NULL
                ELSE close_3d / close - 1.0
            END AS close_3d_return,
            CASE
                WHEN close_5d IS NULL OR ABS(close) <= {_EPSILON} THEN NULL
                ELSE close_5d / close - 1.0
            END AS close_5d_return,
            CASE
                WHEN event_direction_sign IS NULL OR next_open IS NULL OR ABS(close) <= {_EPSILON}
                    THEN NULL
                ELSE event_direction_sign * (next_open / close - 1.0)
            END AS next_open_directional_return,
            CASE
                WHEN event_direction_sign IS NULL OR next_close IS NULL OR ABS(close) <= {_EPSILON}
                    THEN NULL
                ELSE event_direction_sign * (next_close / close - 1.0)
            END AS next_close_directional_return,
            CASE
                WHEN event_direction_sign IS NULL OR close_3d IS NULL OR ABS(close) <= {_EPSILON}
                    THEN NULL
                ELSE event_direction_sign * (close_3d / close - 1.0)
            END AS close_3d_directional_return,
            CASE
                WHEN event_direction_sign IS NULL OR close_5d IS NULL OR ABS(close) <= {_EPSILON}
                    THEN NULL
                ELSE event_direction_sign * (close_5d / close - 1.0)
            END AS close_5d_directional_return,
            CASE
                WHEN next_open IS NULL OR next_close IS NULL OR ABS(next_open) <= {_EPSILON}
                    THEN NULL
                ELSE next_close / next_open - 1.0
            END AS next_open_to_next_close_return,
            CASE
                WHEN next_open IS NULL OR close_3d IS NULL OR ABS(next_open) <= {_EPSILON}
                    THEN NULL
                ELSE close_3d / next_open - 1.0
            END AS next_open_to_close_3d_return,
            CASE
                WHEN next_open IS NULL OR close_5d IS NULL OR ABS(next_open) <= {_EPSILON}
                    THEN NULL
                ELSE close_5d / next_open - 1.0
            END AS next_open_to_close_5d_return
        FROM event_rows
        ORDER BY date, market_code, code
    """
    return cast(pd.DataFrame, conn.execute(sql, params).fetchdf())


def _query_summary_df(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    cte_sql, params = _classified_stock_days_cte(start_date=start_date, end_date=end_date)
    sql = f"""
        {cte_sql}
        SELECT
            market_code,
            market_name,
            CASE
                WHEN hit_stop_high AND hit_stop_low THEN 'both'
                WHEN hit_stop_high THEN 'stop_high'
                ELSE 'stop_low'
            END AS limit_side,
            intraday_range,
            CASE
                WHEN intraday_range THEN 'intraday_range'
                ELSE 'single_price'
            END AS intraday_state,
            COUNT(*) AS event_count,
            COUNT(DISTINCT code) AS unique_code_count,
            SUM(CASE WHEN latest_market_mapped THEN 1 ELSE 0 END) AS latest_market_mapped_event_count,
            SUM(CASE WHEN single_price_day THEN 1 ELSE 0 END) AS single_price_day_count,
            SUM(
                CASE
                    WHEN closed_at_upper_limit OR closed_at_lower_limit THEN 1
                    ELSE 0
                END
            ) AS close_at_limit_count,
            MIN(date) AS first_date,
            MAX(date) AS last_date
        FROM classified
        WHERE NOT above_standard_band
            AND NOT below_standard_band
            AND (hit_stop_high OR hit_stop_low)
        GROUP BY market_code, market_name, limit_side, intraday_range, intraday_state
        ORDER BY market_code, market_name, limit_side, intraday_range
    """
    return cast(pd.DataFrame, conn.execute(sql, params).fetchdf())


def _query_outside_standard_band_df(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    cte_sql, params = _classified_stock_days_cte(start_date=start_date, end_date=end_date)
    sql = f"""
        {cte_sql}
        SELECT
            date,
            code,
            market_code,
            market_name,
            latest_market_mapped,
            prev_close,
            limit_width,
            upper_limit,
            lower_limit,
            open,
            high,
            low,
            close,
            volume,
            hit_stop_high,
            hit_stop_low,
            CASE
                WHEN above_standard_band AND below_standard_band THEN 'both'
                WHEN above_standard_band THEN 'above_standard_upper'
                ELSE 'below_standard_lower'
            END AS outside_standard_side,
            single_price_day,
            intraday_range
        FROM classified
        WHERE above_standard_band OR below_standard_band
        ORDER BY date, market_code, code
    """
    return cast(pd.DataFrame, conn.execute(sql, params).fetchdf())


def _query_outside_standard_band_summary_df(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    cte_sql, params = _classified_stock_days_cte(start_date=start_date, end_date=end_date)
    sql = f"""
        {cte_sql}
        SELECT
            market_code,
            market_name,
            CASE
                WHEN above_standard_band AND below_standard_band THEN 'both'
                WHEN above_standard_band THEN 'above_standard_upper'
                ELSE 'below_standard_lower'
            END AS outside_standard_side,
            COUNT(*) AS event_count,
            COUNT(DISTINCT code) AS unique_code_count,
            MIN(date) AS first_date,
            MAX(date) AS last_date
        FROM classified
        WHERE above_standard_band OR below_standard_band
        GROUP BY market_code, market_name, outside_standard_side
        ORDER BY market_code, market_name, outside_standard_side
    """
    return cast(pd.DataFrame, conn.execute(sql, params).fetchdf())


def _build_continuation_summary_df(event_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        *CONTINUATION_GROUP_COLUMNS,
        "horizon_key",
        "horizon_label",
        "event_count",
        "unique_code_count",
        "available_sample_count",
        "directional_sample_count",
        "mean_raw_return",
        "median_raw_return",
        "mean_directional_return",
        "median_directional_return",
        "mean_abs_raw_return",
        "continuation_count",
        "reversal_count",
        "flat_count",
        "continuation_ratio",
        "reversal_ratio",
        "flat_ratio",
        "first_event_date",
        "last_event_date",
    ]
    if event_df.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, Any]] = []
    grouped = event_df.groupby(list(CONTINUATION_GROUP_COLUMNS), dropna=False, sort=False)
    for group_key, group_df in grouped:
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        group_payload = dict(zip(CONTINUATION_GROUP_COLUMNS, group_key, strict=False))
        event_count = int(len(group_df))
        unique_code_count = int(group_df["code"].nunique())
        first_event_date = str(group_df["date"].min()) if event_count > 0 else None
        last_event_date = str(group_df["date"].max()) if event_count > 0 else None
        for horizon_key, raw_return_col, directional_return_col, horizon_label in FORWARD_RETURN_SPECS:
            raw_returns = pd.to_numeric(group_df[raw_return_col], errors="coerce").dropna()
            directional_returns = pd.to_numeric(
                group_df[directional_return_col],
                errors="coerce",
            ).dropna()
            directional_sample_count = int(len(directional_returns))
            if directional_sample_count > 0:
                continuation_count = int((directional_returns > _EPSILON).sum())
                reversal_count = int((directional_returns < -_EPSILON).sum())
                flat_count = directional_sample_count - continuation_count - reversal_count
                continuation_ratio = continuation_count / directional_sample_count
                reversal_ratio = reversal_count / directional_sample_count
                flat_ratio = flat_count / directional_sample_count
                mean_directional_return = float(directional_returns.mean())
                median_directional_return = float(directional_returns.median())
            else:
                continuation_count = 0
                reversal_count = 0
                flat_count = 0
                continuation_ratio = None
                reversal_ratio = None
                flat_ratio = None
                mean_directional_return = None
                median_directional_return = None

            rows.append(
                {
                    **group_payload,
                    "horizon_key": horizon_key,
                    "horizon_label": horizon_label,
                    "event_count": event_count,
                    "unique_code_count": unique_code_count,
                    "available_sample_count": int(len(raw_returns)),
                    "directional_sample_count": directional_sample_count,
                    "mean_raw_return": float(raw_returns.mean()) if not raw_returns.empty else None,
                    "median_raw_return": (
                        float(raw_returns.median()) if not raw_returns.empty else None
                    ),
                    "mean_directional_return": mean_directional_return,
                    "median_directional_return": median_directional_return,
                    "mean_abs_raw_return": (
                        float(raw_returns.abs().mean()) if not raw_returns.empty else None
                    ),
                    "continuation_count": continuation_count,
                    "reversal_count": reversal_count,
                    "flat_count": flat_count,
                    "continuation_ratio": continuation_ratio,
                    "reversal_ratio": reversal_ratio,
                    "flat_ratio": flat_ratio,
                    "first_event_date": first_event_date,
                    "last_event_date": last_event_date,
                }
            )

    continuation_summary_df = pd.DataFrame(rows, columns=columns)
    horizon_rank_map = {
        horizon_key: index for index, horizon_key in enumerate(FORWARD_RETURN_ORDER)
    }
    return (
        continuation_summary_df.sort_values(
            by=[
                "horizon_key",
                "event_count",
                "unique_code_count",
                "market_code",
                "limit_side",
                "intraday_state",
                "close_limit_state",
            ],
            ascending=[True, False, False, True, True, True, True],
            key=lambda series: (
                series.map(horizon_rank_map)
                if series.name == "horizon_key"
                else series
            ),
            kind="stable",
        )
        .reset_index(drop=True)
    )


def _build_candidate_trade_frame(event_df: pd.DataFrame) -> pd.DataFrame:
    if event_df.empty:
        columns = [
            *event_df.columns.tolist(),
            "candidate_strategy_family",
            "candidate_strategy_label",
            "trade_direction_sign",
            "trade_direction_label",
            "trade_basis_note",
            "next_open_to_next_close_directional_return",
            "next_open_to_close_3d_directional_return",
            "next_open_to_close_5d_directional_return",
        ]
        return pd.DataFrame(columns=columns)

    result = event_df.copy()
    result["candidate_strategy_family"] = pd.Series(pd.NA, index=result.index, dtype="object")
    result["candidate_strategy_label"] = pd.Series(pd.NA, index=result.index, dtype="object")
    result["trade_direction_sign"] = pd.Series(float("nan"), index=result.index, dtype="float64")
    result["trade_direction_label"] = pd.Series(pd.NA, index=result.index, dtype="object")
    result["trade_basis_note"] = pd.Series(pd.NA, index=result.index, dtype="object")

    continuation_mask = (
        result["event_direction_sign"].notna()
        & (result["intraday_state"] == "single_price")
    )
    result.loc[continuation_mask, "candidate_strategy_family"] = "continuation_single_price"
    result.loc[continuation_mask, "candidate_strategy_label"] = "Single-price continuation"
    result.loc[continuation_mask, "trade_direction_sign"] = result.loc[
        continuation_mask, "event_direction_sign"
    ]
    result.loc[continuation_mask, "trade_basis_note"] = (
        "single_price stop event; continue in event direction from next open"
    )

    reversal_mask = (
        result["event_direction_sign"].notna()
        & (result["intraday_state"] == "intraday_range")
        & (result["close_limit_state"] == "off_limit_close")
    )
    result.loc[reversal_mask, "candidate_strategy_family"] = "reversal_intraday_off_limit_close"
    result.loc[reversal_mask, "candidate_strategy_label"] = "Intraday off-limit-close reversal"
    result.loc[reversal_mask, "trade_direction_sign"] = -result.loc[
        reversal_mask, "event_direction_sign"
    ]
    result.loc[reversal_mask, "trade_basis_note"] = (
        "intraday range with off-limit close; fade event direction from next open"
    )

    result.loc[result["trade_direction_sign"] == 1.0, "trade_direction_label"] = "long"
    result.loc[result["trade_direction_sign"] == -1.0, "trade_direction_label"] = "short"

    for trade_key, raw_return_col, _ in TRADE_RETURN_SPECS:
        directional_col = f"{trade_key}_directional_return"
        result[directional_col] = pd.NA
        eligible_mask = result["trade_direction_sign"].notna() & result[raw_return_col].notna()
        result.loc[eligible_mask, directional_col] = (
            result.loc[eligible_mask, "trade_direction_sign"].astype(float)
            * result.loc[eligible_mask, raw_return_col].astype(float)
        )

    scoped_df = result[result["candidate_strategy_family"].notna()].copy()
    scoped_df = scoped_df.reset_index(drop=True)
    return scoped_df


def _build_candidate_trade_summary_df(event_df: pd.DataFrame) -> pd.DataFrame:
    candidate_event_df = _build_candidate_trade_frame(event_df)
    columns = [
        *TRADE_CANDIDATE_GROUP_COLUMNS,
        "trade_horizon_key",
        "trade_horizon_label",
        "event_count",
        "unique_code_count",
        "sample_count",
        "mean_trade_return",
        "median_trade_return",
        "mean_abs_trade_return",
        "profit_count",
        "loss_count",
        "flat_count",
        "profit_ratio",
        "loss_ratio",
        "flat_ratio",
        "first_event_date",
        "last_event_date",
    ]
    if candidate_event_df.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, Any]] = []
    grouped = candidate_event_df.groupby(
        list(TRADE_CANDIDATE_GROUP_COLUMNS),
        dropna=False,
        sort=False,
    )
    for group_key, group_df in grouped:
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        group_payload = dict(zip(TRADE_CANDIDATE_GROUP_COLUMNS, group_key, strict=False))
        event_count = int(len(group_df))
        unique_code_count = int(group_df["code"].nunique())
        first_event_date = str(group_df["date"].min()) if event_count > 0 else None
        last_event_date = str(group_df["date"].max()) if event_count > 0 else None
        for trade_key, _, trade_horizon_label in TRADE_RETURN_SPECS:
            trade_returns = pd.to_numeric(
                group_df[f"{trade_key}_directional_return"],
                errors="coerce",
            ).dropna()
            sample_count = int(len(trade_returns))
            if sample_count > 0:
                profit_count = int((trade_returns > _EPSILON).sum())
                loss_count = int((trade_returns < -_EPSILON).sum())
                flat_count = sample_count - profit_count - loss_count
                profit_ratio = profit_count / sample_count
                loss_ratio = loss_count / sample_count
                flat_ratio = flat_count / sample_count
                mean_trade_return = float(trade_returns.mean())
                median_trade_return = float(trade_returns.median())
                mean_abs_trade_return = float(trade_returns.abs().mean())
            else:
                profit_count = 0
                loss_count = 0
                flat_count = 0
                profit_ratio = None
                loss_ratio = None
                flat_ratio = None
                mean_trade_return = None
                median_trade_return = None
                mean_abs_trade_return = None
            rows.append(
                {
                    **group_payload,
                    "trade_horizon_key": trade_key,
                    "trade_horizon_label": trade_horizon_label,
                    "event_count": event_count,
                    "unique_code_count": unique_code_count,
                    "sample_count": sample_count,
                    "mean_trade_return": mean_trade_return,
                    "median_trade_return": median_trade_return,
                    "mean_abs_trade_return": mean_abs_trade_return,
                    "profit_count": profit_count,
                    "loss_count": loss_count,
                    "flat_count": flat_count,
                    "profit_ratio": profit_ratio,
                    "loss_ratio": loss_ratio,
                    "flat_ratio": flat_ratio,
                    "first_event_date": first_event_date,
                    "last_event_date": last_event_date,
                }
            )

    summary_df = pd.DataFrame(rows, columns=columns)
    trade_rank_map = {
        trade_key: index for index, (trade_key, _, _) in enumerate(TRADE_RETURN_SPECS)
    }
    family_rank_map = {
        "continuation_single_price": 1,
        "reversal_intraday_off_limit_close": 2,
    }
    return (
        summary_df.sort_values(
            by=[
                "trade_horizon_key",
                "candidate_strategy_family",
                "event_count",
                "market_code",
                "limit_side",
            ],
            ascending=[True, True, False, True, True],
            key=lambda series: (
                series.map(trade_rank_map)
                if series.name == "trade_horizon_key"
                else series.map(family_rank_map)
                if series.name == "candidate_strategy_family"
                else series
            ),
            kind="stable",
        )
        .reset_index(drop=True)
    )


def run_stop_limit_daily_classification_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> StopLimitDailyClassificationResult:
    with open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="stop-limit-daily-classification-",
    ) as ctx:
        available_start_date, available_end_date = fetch_date_range(
            ctx.connection,
            table_name="stock_data",
        )
        limit_table_df = build_standard_daily_limit_table_df()
        event_df = _query_event_df(ctx.connection, start_date=start_date, end_date=end_date)
        summary_df = _query_summary_df(ctx.connection, start_date=start_date, end_date=end_date)
        continuation_summary_df = _build_continuation_summary_df(event_df)
        candidate_trade_summary_df = _build_candidate_trade_summary_df(event_df)
        outside_standard_band_df = _query_outside_standard_band_df(
            ctx.connection,
            start_date=start_date,
            end_date=end_date,
        )
        outside_standard_band_summary_df = _query_outside_standard_band_summary_df(
            ctx.connection,
            start_date=start_date,
            end_date=end_date,
        )

    analysis_start_date = (
        str(event_df["date"].min()) if not event_df.empty else None
    )
    analysis_end_date = str(event_df["date"].max()) if not event_df.empty else None
    unmapped_latest_market_event_count = (
        int((~event_df["latest_market_mapped"]).sum()) if not event_df.empty else 0
    )
    total_directional_event_count = (
        int(event_df["event_direction_sign"].notna().sum()) if not event_df.empty else 0
    )
    classification_note = (
        "Primary counts use the standard TSE daily price-limit table against the "
        "previous close. Rows that move outside the standard band are emitted "
        "separately because broadened-limit or special-quote handling is needed "
        "to classify them exactly from daily OHLC alone."
    )
    continuation_note = (
        "Forward returns are measured from the event-day close. Continuation/reversal "
        "is judged by sign-aligned return: positive means follow-through in the same "
        "direction as the stop-limit event, negative means reversal. `both` events "
        "keep raw returns but are excluded from directional continuation scoring."
    )
    candidate_strategy_note = (
        "Candidate strategies use next-session open as the first tradable entry. "
        "`Single-price continuation` trades in the event direction after one-price "
        "stop events. `Intraday off-limit-close reversal` fades intraday-range stop "
        "events that failed to close at the limit."
    )
    return StopLimitDailyClassificationResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        total_event_count=int(len(event_df)),
        total_directional_event_count=total_directional_event_count,
        total_outside_standard_band_count=int(len(outside_standard_band_df)),
        unmapped_latest_market_event_count=unmapped_latest_market_event_count,
        jpx_reference_label=JPX_DAILY_PRICE_LIMITS_REFERENCE_LABEL,
        classification_note=classification_note,
        continuation_note=continuation_note,
        candidate_strategy_note=candidate_strategy_note,
        limit_table_df=limit_table_df,
        event_df=event_df,
        summary_df=summary_df,
        continuation_summary_df=continuation_summary_df,
        candidate_trade_summary_df=candidate_trade_summary_df,
        outside_standard_band_df=outside_standard_band_df,
        outside_standard_band_summary_df=outside_standard_band_summary_df,
    )


def write_stop_limit_daily_classification_research_bundle(
    result: StopLimitDailyClassificationResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=STOP_LIMIT_DAILY_CLASSIFICATION_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_stop_limit_daily_classification_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
        },
        result=result,
        table_field_names=(
            "limit_table_df",
            "event_df",
            "summary_df",
            "continuation_summary_df",
            "candidate_trade_summary_df",
            "outside_standard_band_df",
            "outside_standard_band_summary_df",
        ),
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_stop_limit_daily_classification_research_bundle(
    bundle_path: str | Path,
) -> StopLimitDailyClassificationResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return StopLimitDailyClassificationResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=metadata.get("available_start_date"),
        available_end_date=metadata.get("available_end_date"),
        analysis_start_date=metadata.get("analysis_start_date"),
        analysis_end_date=metadata.get("analysis_end_date"),
        total_event_count=int(metadata["total_event_count"]),
        total_directional_event_count=int(metadata["total_directional_event_count"]),
        total_outside_standard_band_count=int(metadata["total_outside_standard_band_count"]),
        unmapped_latest_market_event_count=int(metadata["unmapped_latest_market_event_count"]),
        jpx_reference_label=str(metadata["jpx_reference_label"]),
        classification_note=str(metadata["classification_note"]),
        continuation_note=str(metadata["continuation_note"]),
        candidate_strategy_note=str(metadata["candidate_strategy_note"]),
        limit_table_df=tables["limit_table_df"],
        event_df=tables["event_df"],
        summary_df=tables["summary_df"],
        continuation_summary_df=tables["continuation_summary_df"],
        candidate_trade_summary_df=tables["candidate_trade_summary_df"],
        outside_standard_band_df=tables["outside_standard_band_df"],
        outside_standard_band_summary_df=tables["outside_standard_band_summary_df"],
    )


def get_stop_limit_daily_classification_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        STOP_LIMIT_DAILY_CLASSIFICATION_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_stop_limit_daily_classification_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        STOP_LIMIT_DAILY_CLASSIFICATION_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_research_bundle_summary_markdown(
    result: StopLimitDailyClassificationResult,
) -> str:
    summary_top = _top_summary_rows(result.summary_df, limit=12)
    next_close_best = _top_continuation_rows(
        result.continuation_summary_df,
        horizon_key="next_close",
        ascending=False,
        limit=8,
        min_samples=50,
    )
    close_5d_worst = _top_continuation_rows(
        result.continuation_summary_df,
        horizon_key="close_5d",
        ascending=True,
        limit=8,
        min_samples=50,
    )
    best_trade_candidates = _top_candidate_trade_rows(
        result.candidate_trade_summary_df,
        trade_horizon_key="next_open_to_close_5d",
        ascending=False,
        limit=10,
        min_samples=50,
    )
    worst_trade_candidates = _top_candidate_trade_rows(
        result.candidate_trade_summary_df,
        trade_horizon_key="next_open_to_close_5d",
        ascending=True,
        limit=10,
        min_samples=50,
    )
    outside_top = _top_outside_summary_rows(result.outside_standard_band_summary_df, limit=8)
    lines = [
        "# Stop-Limit Daily Classification",
        "",
        "## Scope",
        "",
        "- Data source: `stock_data` daily OHLC only.",
        "- Market grouping: latest `stocks` snapshot only.",
        f"- JPX reference: {result.jpx_reference_label}.",
        f"- Source mode: `{result.source_mode}` ({result.source_detail}).",
        "",
        "## Notes",
        "",
        f"- {result.classification_note}",
        f"- Available stock-data range: `{result.available_start_date}` -> `{result.available_end_date}`.",
        f"- Classified event range: `{result.analysis_start_date}` -> `{result.analysis_end_date}`.",
        f"- Primary classified events: `{result.total_event_count}`.",
        f"- Directional events (`stop_high` / `stop_low` only): `{result.total_directional_event_count}`.",
        f"- Outside-standard-band rows: `{result.total_outside_standard_band_count}`.",
        f"- Primary events with missing latest-market mapping: `{result.unmapped_latest_market_event_count}`.",
        f"- {result.continuation_note}",
        f"- {result.candidate_strategy_note}",
        "",
        "## Top Segments",
        "",
        _markdown_table(
            summary_top,
            columns=(
                ("Market", "market_name"),
                ("Side", "limit_side"),
                ("Intraday", "intraday_state"),
                ("Events", "event_count"),
                ("Unique Codes", "unique_code_count"),
                ("Close@Limit", "close_at_limit_count"),
                ("First Date", "first_date"),
                ("Last Date", "last_date"),
            ),
        ),
        "",
        "## Continuation Snapshot (Best next close)",
        "",
        _markdown_table(
            next_close_best,
            columns=(
                ("Market", "market_name"),
                ("Side", "limit_side"),
                ("Intraday", "intraday_state"),
                ("Close State", "close_limit_state"),
                ("Samples", "directional_sample_count"),
                ("Cont%", "continuation_ratio"),
                ("Rev%", "reversal_ratio"),
                ("Mean Raw", "mean_raw_return"),
                ("Mean Dir", "mean_directional_return"),
            ),
        ),
        "",
        "## Reversal Snapshot (Worst close +5 sessions)",
        "",
        _markdown_table(
            close_5d_worst,
            columns=(
                ("Market", "market_name"),
                ("Side", "limit_side"),
                ("Intraday", "intraday_state"),
                ("Close State", "close_limit_state"),
                ("Samples", "directional_sample_count"),
                ("Cont%", "continuation_ratio"),
                ("Rev%", "reversal_ratio"),
                ("Mean Raw", "mean_raw_return"),
                ("Mean Dir", "mean_directional_return"),
            ),
        ),
        "",
        "## Candidate Trades (Best next open -> close +5 sessions)",
        "",
        _markdown_table(
            best_trade_candidates,
            columns=(
                ("Market", "market_name"),
                ("Family", "candidate_strategy_label"),
                ("Direction", "trade_direction_label"),
                ("Side", "limit_side"),
                ("Samples", "sample_count"),
                ("Profit%", "profit_ratio"),
                ("Loss%", "loss_ratio"),
                ("Mean Trade", "mean_trade_return"),
            ),
        ),
        "",
        "## Candidate Trades (Worst next open -> close +5 sessions)",
        "",
        _markdown_table(
            worst_trade_candidates,
            columns=(
                ("Market", "market_name"),
                ("Family", "candidate_strategy_label"),
                ("Direction", "trade_direction_label"),
                ("Side", "limit_side"),
                ("Samples", "sample_count"),
                ("Profit%", "profit_ratio"),
                ("Loss%", "loss_ratio"),
                ("Mean Trade", "mean_trade_return"),
            ),
        ),
        "",
        "## Outside Standard Band",
        "",
        _markdown_table(
            outside_top,
            columns=(
                ("Market", "market_name"),
                ("Side", "outside_standard_side"),
                ("Events", "event_count"),
                ("Unique Codes", "unique_code_count"),
                ("First Date", "first_date"),
                ("Last Date", "last_date"),
            ),
        ),
        "",
        "## Standard Daily Price-Limit Table",
        "",
        _markdown_table(
            cast(list[dict[str, Any]], result.limit_table_df.to_dict("records")),
            columns=(
                ("Base Price Rule", "base_price_rule"),
                ("Limit Width", "daily_limit_width"),
            ),
        ),
    ]
    return "\n".join(lines)


def _build_published_summary_payload(
    result: StopLimitDailyClassificationResult,
) -> dict[str, Any]:
    return {
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "availableStartDate": result.available_start_date,
        "availableEndDate": result.available_end_date,
        "sourceMode": result.source_mode,
        "sourceDetail": result.source_detail,
        "eventCount": result.total_event_count,
        "directionalEventCount": result.total_directional_event_count,
        "outsideStandardBandCount": result.total_outside_standard_band_count,
        "unmappedLatestMarketEventCount": result.unmapped_latest_market_event_count,
        "jpxReferenceLabel": result.jpx_reference_label,
        "classificationNote": result.classification_note,
        "continuationNote": result.continuation_note,
        "candidateStrategyNote": result.candidate_strategy_note,
        "topSegments": _top_summary_rows(result.summary_df, limit=12),
        "bestNextCloseContinuation": _top_continuation_rows(
            result.continuation_summary_df,
            horizon_key="next_close",
            ascending=False,
            limit=8,
            min_samples=50,
        ),
        "worstClose5dDirectional": _top_continuation_rows(
            result.continuation_summary_df,
            horizon_key="close_5d",
            ascending=True,
            limit=8,
            min_samples=50,
        ),
        "bestCandidateTradesClose5d": _top_candidate_trade_rows(
            result.candidate_trade_summary_df,
            trade_horizon_key="next_open_to_close_5d",
            ascending=False,
            limit=10,
            min_samples=50,
        ),
        "worstCandidateTradesClose5d": _top_candidate_trade_rows(
            result.candidate_trade_summary_df,
            trade_horizon_key="next_open_to_close_5d",
            ascending=True,
            limit=10,
            min_samples=50,
        ),
        "outsideStandardBandTopSegments": _top_outside_summary_rows(
            result.outside_standard_band_summary_df,
            limit=8,
        ),
    }


def _top_summary_rows(summary_df: pd.DataFrame, *, limit: int) -> list[dict[str, Any]]:
    if summary_df.empty:
        return []
    ranked = (
        summary_df.sort_values(
            by=["event_count", "unique_code_count", "market_name", "limit_side", "intraday_state"],
            ascending=[False, False, True, True, True],
            kind="stable",
        )
        .head(limit)
        .reset_index(drop=True)
    )
    return cast(list[dict[str, Any]], ranked.to_dict("records"))


def _top_outside_summary_rows(
    summary_df: pd.DataFrame,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    if summary_df.empty:
        return []
    ranked = (
        summary_df.sort_values(
            by=["event_count", "unique_code_count", "market_name", "outside_standard_side"],
            ascending=[False, False, True, True],
            kind="stable",
        )
        .head(limit)
        .reset_index(drop=True)
    )
    return cast(list[dict[str, Any]], ranked.to_dict("records"))


def _top_continuation_rows(
    continuation_summary_df: pd.DataFrame,
    *,
    horizon_key: ForwardReturnHorizon,
    ascending: bool,
    limit: int,
    min_samples: int,
) -> list[dict[str, Any]]:
    if continuation_summary_df.empty:
        return []
    scoped_df = continuation_summary_df[
        (continuation_summary_df["horizon_key"] == horizon_key)
        & (continuation_summary_df["directional_sample_count"] >= min_samples)
        & (continuation_summary_df["mean_directional_return"].notna())
    ].copy()
    if scoped_df.empty:
        return []
    ranked = (
        scoped_df.sort_values(
            by=["mean_directional_return", "directional_sample_count", "market_name"],
            ascending=[ascending, False, True],
            kind="stable",
        )
        .head(limit)
        .reset_index(drop=True)
    )
    return cast(list[dict[str, Any]], ranked.to_dict("records"))


def _top_candidate_trade_rows(
    candidate_trade_summary_df: pd.DataFrame,
    *,
    trade_horizon_key: str,
    ascending: bool,
    limit: int,
    min_samples: int,
) -> list[dict[str, Any]]:
    if candidate_trade_summary_df.empty:
        return []
    scoped_df = candidate_trade_summary_df[
        (candidate_trade_summary_df["trade_horizon_key"] == trade_horizon_key)
        & (candidate_trade_summary_df["sample_count"] >= min_samples)
        & (candidate_trade_summary_df["mean_trade_return"].notna())
    ].copy()
    if scoped_df.empty:
        return []
    ranked = (
        scoped_df.sort_values(
            by=["mean_trade_return", "sample_count", "market_name"],
            ascending=[ascending, False, True],
            kind="stable",
        )
        .head(limit)
        .reset_index(drop=True)
    )
    return cast(list[dict[str, Any]], ranked.to_dict("records"))


def _markdown_table(
    rows: Sequence[Mapping[str, Any]],
    *,
    columns: tuple[tuple[str, str], ...],
) -> str:
    if not rows:
        header = "| " + " | ".join(label for label, _ in columns) + " |"
        separator = "| " + " | ".join("---" for _ in columns) + " |"
        return "\n".join([header, separator, "| (none) |" + " |" * (len(columns) - 1)])
    header = "| " + " | ".join(label for label, _ in columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body = []
    for row in rows:
        rendered = " | ".join(
            _format_markdown_cell(row.get(key)) for _, key in columns
        )
        body.append(f"| {rendered} |")
    return "\n".join([header, separator, *body])


def _format_markdown_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value).replace("|", "\\|")
