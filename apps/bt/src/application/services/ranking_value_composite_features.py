"""Value-composite technical and profile feature helpers."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.application.services.ranking_fundamental_queries import (
    resolve_ready_adjustment_bases,
    resolved_basis_price_ctes,
)
from src.application.services.ranking_query_helpers import (
    normalize_equity_code,
)
from src.application.services.ranking_response_items import (
    finite_or_none,
    int_or_none,
    str_or_none,
)
from src.application.services.ranking_value_composite_config import (
    ValueCompositeProfileSpec,
)
from src.infrastructure.db.market.market_reader import MarketDbReader


def append_value_composite_profile_metrics(
    frame: pd.DataFrame,
    reader: MarketDbReader,
    *,
    target_date: str,
    profile: ValueCompositeProfileSpec,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    profile_metrics = load_value_composite_profile_metrics(
        reader,
        target_date=target_date,
        codes=[str(code) for code in result["code"].tolist()],
        profile=profile,
    )
    technical_columns = [
        "avg_trading_value_60d_mil_jpy",
        "avg_trading_value_60d_source_sessions",
    ]
    if profile.breakout_window is not None:
        technical_columns.extend(
            [
                f"new_high_{profile.breakout_window}d",
                f"days_since_new_high_{profile.breakout_window}d",
                f"close_to_prior_high_{profile.breakout_window}d_pct",
            ]
        )
    normalized_codes = result["code"].map(normalize_equity_code)
    for column in technical_columns:
        result[column] = normalized_codes.map(
            lambda code, column=column: profile_metrics.get(str(code), {}).get(column)
        )
    return result


def append_value_composite_technical_metrics(
    frame: pd.DataFrame,
    reader: MarketDbReader,
    *,
    target_date: str,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    technical_metrics = load_value_composite_technical_metrics(
        reader,
        target_date=target_date,
        codes=[str(code) for code in result["code"].tolist()],
    )
    technical_columns = (
        "technical_feature_date",
        "breakout_feature_date",
        "rebound_from_252d_low_pct",
        "return_252d_pct",
        "volatility_20d_pct",
        "volatility_60d_pct",
        "downside_volatility_60d_pct",
        "avg_trading_value_60d_mil_jpy",
        "avg_trading_value_60d_source_sessions",
        "new_high_20d",
        "days_since_new_high_20d",
        "close_to_prior_high_20d_pct",
        "new_high_120d",
        "days_since_new_high_120d",
        "close_to_prior_high_120d_pct",
    )
    normalized_codes = result["code"].map(normalize_equity_code)
    for column in technical_columns:
        result[column] = normalized_codes.map(
            lambda code, column=column: technical_metrics.get(str(code), {}).get(column)
        )
    return result


def load_value_composite_technical_metrics(
    reader: MarketDbReader,
    *,
    target_date: str,
    codes: list[str],
) -> dict[str, dict[str, Any]]:
    normalized_codes = sorted({normalize_equity_code(code) for code in codes})
    if not normalized_codes:
        return {}
    placeholders = ",".join("?" for _ in normalized_codes)
    bases = resolve_ready_adjustment_bases(reader, normalized_codes, target_date)
    price_ctes, basis_params = resolved_basis_price_ctes(bases)
    sql = f"""
        WITH {price_ctes},
        stock_history AS (
            SELECT
                normalized_code,
                date,
                close
            FROM basis_price
            WHERE date <= ?
              AND normalized_code IN ({placeholders})
        ),
        returns AS (
            SELECT
                normalized_code,
                date,
                close,
                close / NULLIF(LAG(close, 252) OVER (
                    PARTITION BY normalized_code ORDER BY date
                ), 0) - 1 AS return_252d,
                close / NULLIF(MIN(close) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                ), 0) - 1 AS rebound_from_252d_low,
                close / NULLIF(LAG(close) OVER (
                    PARTITION BY normalized_code ORDER BY date
                ), 0) - 1 AS daily_return
            FROM stock_history
        ),
        metrics AS (
            SELECT
                normalized_code,
                date,
                rebound_from_252d_low * 100.0 AS rebound_from_252d_low_pct,
                return_252d * 100.0 AS return_252d_pct,
                STDDEV_SAMP(daily_return) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) * SQRT(252.0) * 100.0 AS volatility_20d_pct,
                COUNT(daily_return) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS volatility_20d_count,
                STDDEV_SAMP(daily_return) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) * SQRT(252.0) * 100.0 AS volatility_60d_pct,
                COUNT(daily_return) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS volatility_60d_count,
                STDDEV_SAMP(CASE WHEN daily_return < 0 THEN daily_return ELSE NULL END) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) * SQRT(252.0) * 100.0 AS downside_volatility_60d_pct,
                COUNT(CASE WHEN daily_return < 0 THEN daily_return ELSE NULL END) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS downside_volatility_60d_count,
                ROW_NUMBER() OVER (
                    PARTITION BY normalized_code ORDER BY date DESC
                ) AS latest_rank
            FROM returns
        ),
        latest_metrics AS (
            SELECT
                normalized_code,
                date AS technical_feature_date,
                rebound_from_252d_low_pct,
                return_252d_pct,
                CASE WHEN volatility_20d_count >= 20 THEN volatility_20d_pct ELSE NULL END AS volatility_20d_pct,
                CASE WHEN volatility_60d_count >= 60 THEN volatility_60d_pct ELSE NULL END AS volatility_60d_pct,
                CASE
                    WHEN downside_volatility_60d_count >= 2 THEN downside_volatility_60d_pct
                    ELSE NULL
                END AS downside_volatility_60d_pct
            FROM metrics
            WHERE latest_rank = 1
        ),
        signal_history AS (
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
            FROM basis_price
            WHERE date < ?
              AND normalized_code IN ({placeholders})
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
                    ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                ) AS prior_high_20d,
                MAX(high) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 120 PRECEDING AND 1 PRECEDING
                ) AS prior_high_120d,
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
                prior_high_20d IS NOT NULL AND high > prior_high_20d AS new_high_20d,
                prior_high_120d IS NOT NULL AND high > prior_high_120d AS new_high_120d
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
                MAX(CASE WHEN new_high_20d THEN signal_row_number ELSE NULL END)
                    AS latest_new_high_20d_row_number,
                MAX(CASE WHEN new_high_120d THEN signal_row_number ELSE NULL END)
                    AS latest_new_high_120d_row_number
            FROM signal_flags
            GROUP BY normalized_code
        )
        SELECT
            latest_metrics.*,
            latest_signal.date AS breakout_feature_date,
            CASE
                WHEN latest_signal.avg_trading_value_60d_source_sessions >= 60
                THEN latest_signal.avg_trading_value_60d / 1000000.0
                ELSE NULL
            END AS avg_trading_value_60d_mil_jpy,
            latest_signal.avg_trading_value_60d_source_sessions,
            latest_signal.new_high_20d,
            latest_signal.signal_row_number - latest_breakout.latest_new_high_20d_row_number
                AS days_since_new_high_20d,
            CASE
                WHEN latest_signal.prior_high_20d IS NULL OR latest_signal.prior_high_20d = 0
                THEN NULL
                ELSE (latest_signal.close / latest_signal.prior_high_20d - 1.0) * 100.0
            END AS close_to_prior_high_20d_pct,
            latest_signal.new_high_120d,
            latest_signal.signal_row_number - latest_breakout.latest_new_high_120d_row_number
                AS days_since_new_high_120d,
            CASE
                WHEN latest_signal.prior_high_120d IS NULL OR latest_signal.prior_high_120d = 0
                THEN NULL
                ELSE (latest_signal.close / latest_signal.prior_high_120d - 1.0) * 100.0
            END AS close_to_prior_high_120d_pct
        FROM latest_metrics
        LEFT JOIN latest_signal USING (normalized_code)
        LEFT JOIN latest_breakout USING (normalized_code)
    """
    rows = reader.query(
        sql,
        (
            *basis_params,
            target_date,
            *normalized_codes,
            target_date,
            *normalized_codes,
        ),
    )
    return {
        str(row["normalized_code"]): {
            "technical_feature_date": str_or_none(row["technical_feature_date"]),
            "breakout_feature_date": str_or_none(row["breakout_feature_date"]),
            "rebound_from_252d_low_pct": finite_or_none(
                row["rebound_from_252d_low_pct"]
            ),
            "return_252d_pct": finite_or_none(row["return_252d_pct"]),
            "volatility_20d_pct": finite_or_none(row["volatility_20d_pct"]),
            "volatility_60d_pct": finite_or_none(row["volatility_60d_pct"]),
            "downside_volatility_60d_pct": finite_or_none(
                row["downside_volatility_60d_pct"]
            ),
            "avg_trading_value_60d_mil_jpy": finite_or_none(
                row["avg_trading_value_60d_mil_jpy"]
            ),
            "avg_trading_value_60d_source_sessions": int_or_none(
                row["avg_trading_value_60d_source_sessions"]
            ),
            "new_high_20d": (
                bool(row["new_high_20d"]) if row["new_high_20d"] is not None else None
            ),
            "days_since_new_high_20d": int_or_none(row["days_since_new_high_20d"]),
            "close_to_prior_high_20d_pct": finite_or_none(
                row["close_to_prior_high_20d_pct"]
            ),
            "new_high_120d": (
                bool(row["new_high_120d"])
                if row["new_high_120d"] is not None
                else None
            ),
            "days_since_new_high_120d": int_or_none(row["days_since_new_high_120d"]),
            "close_to_prior_high_120d_pct": finite_or_none(
                row["close_to_prior_high_120d_pct"]
            ),
        }
        for row in rows
    }


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
    bases = resolve_ready_adjustment_bases(reader, normalized_codes, target_date)
    price_ctes, basis_params = resolved_basis_price_ctes(bases)
    required_session_offset = 0
    if profile.min_adv60_mil_jpy is not None:
        required_session_offset = max(required_session_offset, 59)
    if profile.breakout_window is not None:
        required_session_offset = max(
            required_session_offset,
            int(profile.breakout_window) + int(profile.breakout_lookback_sessions or 0),
        )
    start_row = reader.query_one(
        """
        SELECT date
        FROM (SELECT DISTINCT date FROM stock_data_raw WHERE date < ? ORDER BY date DESC)
        LIMIT 1 OFFSET ?
        """,
        (target_date, required_session_offset),
    )
    start_date = str(start_row["date"]) if start_row is not None else None
    lower_bound_clause = " AND date >= ?" if start_date is not None else ""
    params: tuple[Any, ...] = (
        (*basis_params, target_date, start_date, *normalized_codes)
        if start_date is not None
        else (*basis_params, target_date, *normalized_codes)
    )

    if profile.breakout_window is None:
        sql = f"""
            WITH {price_ctes},
            signal_history AS (
                SELECT
                    normalized_code,
                    date,
                    close,
                    volume,
                    close * volume AS trading_value
                FROM basis_price
                WHERE date < ?{lower_bound_clause}
                  AND normalized_code IN ({placeholders})
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
        WITH {price_ctes},
        signal_history AS (
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
            FROM basis_price
            WHERE date < ?{lower_bound_clause}
              AND normalized_code IN ({placeholders})
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
