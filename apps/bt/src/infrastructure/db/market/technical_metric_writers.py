"""Daily technical metric materialization helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from src.infrastructure.db.market.market_mutations import MarketMutationStats
from src.infrastructure.db.market.market_schema import (
    DAILY_TECHNICAL_METRICS_COLUMNS as _DAILY_TECHNICAL_METRICS_COLUMNS,
)


_DESIRED_RELATION = "desired_daily_technical_metrics"
_KEY_COLUMNS = ("code", "date")
_SEMANTIC_COLUMNS = (
    "close",
    "sma5",
    "sma5_sessions",
    "close_above_sma5_flag",
    "sma5_above_count_5d",
    "sma5_above_count_sessions",
    "sma5_above_count_group",
    "sma5_below_streak",
)


@dataclass(frozen=True, slots=True)
class TechnicalMetricRebuildResult:
    """Semantic mutations and final row count for a technical rebuild."""

    stats: MarketMutationStats
    final_count: int


def rebuild_daily_technical_metrics_from_stock_data(
    conn: Any,
    lock: Any,
    table_exists: Any,
) -> TechnicalMetricRebuildResult:
    """Reconcile daily technical metrics from canonical adjusted stock_data."""
    if not table_exists("stock_data"):
        return TechnicalMetricRebuildResult(MarketMutationStats.empty(), 0)

    with lock:
        try:
            _materialize_desired_relation(conn)
            stats = _classify_delta(conn)
            if stats.mutated_rows:
                _apply_delta(conn)
            return TechnicalMetricRebuildResult(stats, stats.input)
        finally:
            conn.execute(f"DROP TABLE IF EXISTS {_DESIRED_RELATION}")


def _materialize_desired_relation(conn: Any) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {_DESIRED_RELATION}")
    conn.execute(
        f"""
        CREATE TEMP TABLE {_DESIRED_RELATION} AS
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
                END AS close_above_sma5_flag,
                CASE
                    WHEN sma5_sessions = 5 AND close < sma5 THEN 1
                    WHEN sma5_sessions = 5 THEN 0
                END AS close_below_sma5_flag
            FROM sma_features
        ),
        flagged_groups AS (
            SELECT
                code,
                date,
                close,
                sma5,
                sma5_sessions,
                close_above_sma5_flag,
                close_below_sma5_flag,
                SUM(
                    CASE WHEN close_below_sma5_flag = 1 THEN 0 ELSE 1 END
                ) OVER (
                    PARTITION BY code
                    ORDER BY date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS sma5_below_reset_group
            FROM flags
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
                ) AS sma5_above_count_sessions,
                CASE
                    WHEN close_below_sma5_flag = 1 THEN SUM(close_below_sma5_flag) OVER (
                        PARTITION BY code, sma5_below_reset_group
                        ORDER BY date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    )
                    ELSE 0
                END AS sma5_below_streak
            FROM flagged_groups
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
            CAST(sma5_below_streak AS INTEGER) AS sma5_below_streak
        FROM counted
        WHERE sma5_above_count_sessions = 5
        """
    )


def _classify_delta(conn: Any) -> MarketMutationStats:
    distinct = " OR ".join(
        f"target.{column} IS DISTINCT FROM desired.{column}"
        for column in _SEMANTIC_COLUMNS
    )
    row = conn.execute(
        f"""
        SELECT
            COUNT(*) AS input,
            COUNT(*) FILTER (WHERE target.code IS NULL) AS inserted,
            COUNT(*) FILTER (
                WHERE target.code IS NOT NULL AND ({distinct})
            ) AS updated,
            COUNT(*) FILTER (
                WHERE target.code IS NOT NULL AND NOT ({distinct})
            ) AS unchanged,
            (
                SELECT COUNT(*)
                FROM daily_technical_metrics stale
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {_DESIRED_RELATION} desired_stale
                    WHERE desired_stale.code = stale.code
                      AND desired_stale.date = stale.date
                )
            ) AS deleted
        FROM {_DESIRED_RELATION} desired
        LEFT JOIN daily_technical_metrics target USING (code, date)
        """
    ).fetchone()
    if row is None:
        return MarketMutationStats.empty()
    return MarketMutationStats(*(int(value or 0) for value in row))


def _apply_delta(conn: Any) -> None:
    distinct = " OR ".join(
        f"target.{column} IS DISTINCT FROM desired.{column}"
        for column in _SEMANTIC_COLUMNS
    )
    assignments = ", ".join(
        [
            *(f"{column} = desired.{column}" for column in _SEMANTIC_COLUMNS),
            "created_at = ?",
        ]
    )
    insert_columns = ", ".join(_DAILY_TECHNICAL_METRICS_COLUMNS)
    select_columns = ", ".join(
        [*(f"desired.{column}" for column in (*_KEY_COLUMNS, *_SEMANTIC_COLUMNS)), "?"]
    )
    created_at = datetime.now(UTC).isoformat()
    conn.execute("BEGIN")
    try:
        conn.execute(
            f"""
            DELETE FROM daily_technical_metrics AS target
            WHERE NOT EXISTS (
                SELECT 1
                FROM {_DESIRED_RELATION} desired
                WHERE desired.code = target.code
                  AND desired.date = target.date
            )
            """
        )
        conn.execute(
            f"""
            UPDATE daily_technical_metrics AS target
            SET {assignments}
            FROM {_DESIRED_RELATION} desired
            WHERE target.code = desired.code
              AND target.date = desired.date
              AND ({distinct})
            """,
            [created_at],
        )
        conn.execute(
            f"""
            INSERT INTO daily_technical_metrics ({insert_columns})
            SELECT {select_columns}
            FROM {_DESIRED_RELATION} desired
            WHERE NOT EXISTS (
                SELECT 1
                FROM daily_technical_metrics target
                WHERE target.code = desired.code
                  AND target.date = desired.date
            )
            """,
            [created_at],
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
