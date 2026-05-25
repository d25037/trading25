"""Index performance loading for ranking responses."""

from __future__ import annotations

from collections.abc import Callable

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.entrypoints.http.schemas.ranking import IndexPerformanceItem


def load_index_performance(
    reader: MarketDbReader,
    *,
    table_exists: Callable[[str], bool],
    date: str,
    lookback_days: int,
) -> list[IndexPerformanceItem]:
    if lookback_days < 1:
        return []
    if not table_exists("index_master") or not table_exists("indices_data"):
        return []

    rows = reader.query(
        """
        WITH ranked_index_history AS (
            SELECT
                m.code,
                m.name,
                m.category,
                d.date,
                d.close,
                ROW_NUMBER() OVER (
                    PARTITION BY m.code
                    ORDER BY d.date DESC
                ) AS rn
            FROM index_master m
            JOIN indices_data d
                ON d.code = m.code
            WHERE d.date <= ?
                AND d.close IS NOT NULL
                AND d.close > 0
        ),
        current_rows AS (
            SELECT
                code,
                name,
                category,
                date AS current_date,
                close AS current_close
            FROM ranked_index_history
            WHERE rn = 1
        ),
        base_rows AS (
            SELECT
                code,
                date AS base_date,
                close AS base_close
            FROM ranked_index_history
            WHERE rn = ?
        )
        SELECT
            c.code,
            c.name,
            c.category,
            c.current_date,
            b.base_date,
            c.current_close,
            b.base_close,
            (c.current_close - b.base_close) AS change_amount,
            ((c.current_close - b.base_close) / b.base_close * 100) AS change_percentage
        FROM current_rows c
        JOIN base_rows b
            ON b.code = c.code
        WHERE b.base_close > 0
        ORDER BY
            CASE c.category
                WHEN 'synthetic' THEN 0
                WHEN 'topix' THEN 1
                WHEN 'sector17' THEN 2
                WHEN 'sector33' THEN 3
                WHEN 'market' THEN 4
                WHEN 'style' THEN 5
                WHEN 'growth' THEN 6
                WHEN 'reit' THEN 7
                ELSE 99
            END,
            c.code
        """,
        (date, lookback_days + 1),
    )
    return [
        IndexPerformanceItem(
            code=row["code"],
            name=row["name"],
            category=row["category"],
            currentDate=row["current_date"],
            baseDate=row["base_date"],
            currentClose=row["current_close"],
            baseClose=row["base_close"],
            changeAmount=row["change_amount"],
            changePercentage=row["change_percentage"],
            lookbackDays=lookback_days,
        )
        for row in rows
    ]
