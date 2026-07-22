"""Materialized daily technical metric enrichment for market rankings."""

from __future__ import annotations

from src.application.contracts import ranking as ranking_contracts
from src.application.services.ranking_collection_filters import (
    group_ranking_items_by_normalized_code,
)
from src.application.services.ranking_response_items import int_or_none
from src.infrastructure.db.market.market_reader import MarketDbReader


def enrich_ranking_collections_with_daily_technical_metrics(
    reader: MarketDbReader,
    collections: tuple[list[ranking_contracts.RankingItem], ...],
    *,
    target_date: str,
) -> None:
    """Read sync-materialized metrics for the requested date and selected codes."""
    items_by_code = group_ranking_items_by_normalized_code(collections)
    if not target_date or not items_by_code:
        return

    table_row = reader.query_one(
        """
        SELECT count(*) AS table_count
        FROM information_schema.tables
        WHERE table_name = 'daily_technical_metrics'
        """
    )
    if table_row is None or int(table_row["table_count"]) != 1:
        return

    codes = tuple(items_by_code)
    placeholders = ",".join("?" for _ in codes)
    rows = reader.query(
        f"""
        SELECT
            CASE
                WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                THEN left(code, length(code) - 1)
                ELSE code
            END AS code,
            sma5_above_count_5d,
            sma5_below_streak
        FROM daily_technical_metrics
        WHERE date = ?
          AND CASE
                WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                THEN left(code, length(code) - 1)
                ELSE code
              END IN ({placeholders})
        """,
        (target_date, *codes),
    )
    for row in rows:
        code = str(row["code"])
        count = int_or_none(row["sma5_above_count_5d"])
        below_streak = int_or_none(row["sma5_below_streak"])
        for item in items_by_code.get(code, []):
            if count is not None:
                item.sma5AboveCount5d = count
            if below_streak is not None:
                item.sma5BelowStreak = below_streak
