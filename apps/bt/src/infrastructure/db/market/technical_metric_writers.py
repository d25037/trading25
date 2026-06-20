"""Daily technical metric materialization helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from src.infrastructure.db.market.market_schema import (
    DAILY_TECHNICAL_METRICS_COLUMNS as _DAILY_TECHNICAL_METRICS_COLUMNS,
)


def rebuild_daily_technical_metrics_from_stock_data(
    conn: Any,
    lock: Any,
    table_exists: Any,
) -> int:
    """Rebuild daily technical metrics from canonical adjusted stock_data."""
    if not table_exists("stock_data"):
        return 0

    created_at = datetime.now().isoformat()
    columns_sql = ", ".join(_DAILY_TECHNICAL_METRICS_COLUMNS)
    with lock:
        conn.execute("DELETE FROM daily_technical_metrics")
        conn.execute(
            f"""
            INSERT INTO daily_technical_metrics ({columns_sql})
            WITH raw_prices AS (
                SELECT
                    CASE
                        WHEN length(code) = 5 AND right(code, 1) = '0' THEN left(code, 4)
                        ELSE code
                    END AS code,
                    date,
                    close,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            CASE
                                WHEN length(code) = 5 AND right(code, 1) = '0' THEN left(code, 4)
                                ELSE code
                            END,
                            date
                        ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                    ) AS rn
                FROM stock_data
                WHERE close > 0
            ),
            prices AS (
                SELECT code, date, close
                FROM raw_prices
                WHERE rn = 1
            ),
            sma_features AS (
                SELECT
                    code,
                    date,
                    close,
                    AVG(close) OVER (
                        PARTITION BY code
                        ORDER BY date
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) AS sma5,
                    COUNT(close) OVER (
                        PARTITION BY code
                        ORDER BY date
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) AS sma5_sessions
                FROM prices
            ),
            flags AS (
                SELECT
                    code,
                    date,
                    close,
                    sma5,
                    sma5_sessions,
                    CASE
                        WHEN sma5_sessions = 5 AND close > sma5 THEN 1
                        WHEN sma5_sessions = 5 THEN 0
                    END AS close_above_sma5_flag
                FROM sma_features
            ),
            counted AS (
                SELECT
                    code,
                    date,
                    close,
                    sma5,
                    sma5_sessions,
                    close_above_sma5_flag,
                    SUM(close_above_sma5_flag) OVER (
                        PARTITION BY code
                        ORDER BY date
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) AS sma5_above_count_5d,
                    COUNT(close_above_sma5_flag) OVER (
                        PARTITION BY code
                        ORDER BY date
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) AS sma5_above_count_sessions
                FROM flags
            )
            SELECT
                code,
                date,
                close,
                sma5,
                sma5_sessions,
                close_above_sma5_flag,
                CAST(sma5_above_count_5d AS INTEGER) AS sma5_above_count_5d,
                CAST(sma5_above_count_sessions AS INTEGER) AS sma5_above_count_sessions,
                CASE
                    WHEN sma5_above_count_5d <= 1 THEN 'weak'
                    WHEN sma5_above_count_5d >= 4 THEN 'strong'
                    ELSE 'neutral'
                END AS sma5_above_count_group,
                ? AS created_at
            FROM counted
            WHERE sma5_above_count_sessions = 5
            """,
            [created_at],
        )
        row = conn.execute("SELECT COUNT(*) FROM daily_technical_metrics").fetchone()
    return int(row[0] or 0) if row else 0
