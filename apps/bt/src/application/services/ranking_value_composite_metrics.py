"""Value-composite ranking metric query helpers."""

from __future__ import annotations

from typing import Any

from src.application.services.ranking_daily_queries import get_trading_date_before
from src.application.services.ranking_query_helpers import (
    normalize_equity_code,
    normalized_code_sql,
    prefer_4digit_order_sql,
)
from src.application.services.ranking_response_items import finite_or_none, int_or_none
from src.application.services.ranking_value_composite_config import ValueCompositeProfileSpec
from src.infrastructure.db.market.market_reader import MarketDbReader


def load_value_composite_profile_metrics(
    reader: MarketDbReader,
    *,
    target_date: str,
    codes: list[str],
    profile: ValueCompositeProfileSpec,
) -> dict[str, dict[str, Any]]:
    normalized_codes = sorted({normalize_equity_code(code) for code in codes})
    if not normalized_codes:
        return {}

    placeholders = ",".join("?" for _ in normalized_codes)
    normalized = normalized_code_sql("code")
    order = prefer_4digit_order_sql("code")
    required_session_offset = 0
    if profile.min_adv60_mil_jpy is not None:
        required_session_offset = max(required_session_offset, 59)
    if profile.breakout_window is not None:
        required_session_offset = max(
            required_session_offset,
            int(profile.breakout_window) + int(profile.breakout_lookback_sessions or 0),
        )
    start_date = get_trading_date_before(reader, target_date, required_session_offset)
    lower_bound_clause = " AND date >= ?" if start_date is not None else ""
    params: tuple[Any, ...] = (
        (target_date, start_date, *normalized_codes)
        if start_date is not None
        else (target_date, *normalized_codes)
    )

    if profile.breakout_window is None:
        sql = f"""
            WITH signal_history AS (
                SELECT
                    normalized_code,
                    date,
                    close,
                    volume,
                    close * volume AS trading_value
                FROM (
                    SELECT
                        {normalized} AS normalized_code,
                        date,
                        close,
                        volume,
                        ROW_NUMBER() OVER (
                            PARTITION BY {normalized}, date
                            ORDER BY {order}
                        ) AS rn
                    FROM stock_data
                    WHERE date < ?{lower_bound_clause}
                      AND {normalized} IN ({placeholders})
                )
                WHERE rn = 1
            ),
            signal_metrics AS (
                SELECT
                    normalized_code,
                    date,
                    AVG(trading_value) OVER (
                        PARTITION BY normalized_code
                        ORDER BY date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) AS avg_trading_value_60d,
                    COUNT(trading_value) OVER (
                        PARTITION BY normalized_code
                        ORDER BY date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) AS avg_trading_value_60d_source_sessions,
                    ROW_NUMBER() OVER (
                        PARTITION BY normalized_code ORDER BY date DESC
                    ) AS latest_signal_rank
                FROM signal_history
            )
            SELECT
                normalized_code,
                CASE
                    WHEN avg_trading_value_60d_source_sessions >= 60
                    THEN avg_trading_value_60d / 1000000.0
                    ELSE NULL
                END AS avg_trading_value_60d_mil_jpy,
                avg_trading_value_60d_source_sessions
            FROM signal_metrics
            WHERE latest_signal_rank = 1
        """
        rows = reader.query(sql, params)
        return {
            str(row["normalized_code"]): {
                "avg_trading_value_60d_mil_jpy": finite_or_none(
                    row["avg_trading_value_60d_mil_jpy"]
                ),
                "avg_trading_value_60d_source_sessions": int_or_none(
                    row["avg_trading_value_60d_source_sessions"]
                ),
            }
            for row in rows
        }

    breakout_window = int(profile.breakout_window)
    sql = f"""
        WITH signal_history AS (
            SELECT
                normalized_code,
                date,
                high,
                close,
                volume,
                close * volume AS trading_value,
                ROW_NUMBER() OVER (
                    PARTITION BY normalized_code ORDER BY date
                ) AS signal_row_number
            FROM (
                SELECT
                    {normalized} AS normalized_code,
                    date,
                    high,
                    close,
                    volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized}, date
                        ORDER BY {order}
                    ) AS rn
                FROM stock_data
                WHERE date < ?{lower_bound_clause}
                  AND {normalized} IN ({placeholders})
            )
            WHERE rn = 1
        ),
        signal_metrics AS (
            SELECT
                normalized_code,
                date,
                high,
                close,
                signal_row_number,
                MAX(high) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN {breakout_window} PRECEDING AND 1 PRECEDING
                ) AS prior_high,
                AVG(trading_value) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS avg_trading_value_60d,
                COUNT(trading_value) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS avg_trading_value_60d_source_sessions,
                ROW_NUMBER() OVER (
                    PARTITION BY normalized_code ORDER BY date DESC
                ) AS latest_signal_rank
            FROM signal_history
        ),
        signal_flags AS (
            SELECT
                *,
                prior_high IS NOT NULL AND high > prior_high AS new_high
            FROM signal_metrics
        ),
        latest_signal AS (
            SELECT *
            FROM signal_flags
            WHERE latest_signal_rank = 1
        ),
        latest_breakout AS (
            SELECT
                normalized_code,
                MAX(CASE WHEN new_high THEN signal_row_number ELSE NULL END)
                    AS latest_new_high_row_number
            FROM signal_flags
            GROUP BY normalized_code
        )
        SELECT
            latest_signal.normalized_code,
            CASE
                WHEN latest_signal.avg_trading_value_60d_source_sessions >= 60
                THEN latest_signal.avg_trading_value_60d / 1000000.0
                ELSE NULL
            END AS avg_trading_value_60d_mil_jpy,
            latest_signal.avg_trading_value_60d_source_sessions,
            latest_signal.new_high,
            latest_signal.signal_row_number - latest_breakout.latest_new_high_row_number
                AS days_since_new_high,
            CASE
                WHEN latest_signal.prior_high IS NULL OR latest_signal.prior_high = 0
                THEN NULL
                ELSE (latest_signal.close / latest_signal.prior_high - 1.0) * 100.0
            END AS close_to_prior_high_pct
        FROM latest_signal
        LEFT JOIN latest_breakout USING (normalized_code)
    """
    rows = reader.query(sql, params)
    return {
        str(row["normalized_code"]): {
            "avg_trading_value_60d_mil_jpy": finite_or_none(
                row["avg_trading_value_60d_mil_jpy"]
            ),
            "avg_trading_value_60d_source_sessions": int_or_none(
                row["avg_trading_value_60d_source_sessions"]
            ),
            f"new_high_{breakout_window}d": (
                bool(row["new_high"]) if row["new_high"] is not None else None
            ),
            f"days_since_new_high_{breakout_window}d": int_or_none(
                row["days_since_new_high"]
            ),
            f"close_to_prior_high_{breakout_window}d_pct": finite_or_none(
                row["close_to_prior_high_pct"]
            ),
        }
        for row in rows
    }
