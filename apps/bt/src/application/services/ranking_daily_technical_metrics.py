"""Materialized daily technical metric enrichment for market rankings."""

from __future__ import annotations

from src.application.contracts import ranking as ranking_contracts
from src.application.services.ranking_collection_filters import (
    group_ranking_items_by_normalized_code,
)
from src.application.services.ranking_query_helpers import event_time_signal_sql
from src.application.services.ranking_response_items import int_or_none
from src.application.services.ranking_technical_flags import (
    technical_feature_lower_bound_date,
)
from src.domains.analytics.daily_ranking_event_time_prices import EventTimeSignalSql
from src.infrastructure.db.market.market_reader import MarketDbReader


def enrich_ranking_collections_with_daily_technical_metrics(
    reader: MarketDbReader,
    collections: tuple[list[ranking_contracts.RankingItem], ...],
    *,
    target_date: str,
    market_codes: list[str] | None = None,
    signal_sql: EventTimeSignalSql | None = None,
) -> None:
    items_by_code = group_ranking_items_by_normalized_code(collections)
    if not target_date or not items_by_code:
        return

    lower_bound_date = technical_feature_lower_bound_date(target_date)
    if signal_sql is None:
        with event_time_signal_sql(
            reader,
            signal_date=target_date,
            start_date=lower_bound_date,
            market_codes=market_codes or [],
        ) as materialized_signal:
            return enrich_ranking_collections_with_daily_technical_metrics(
                reader,
                collections,
                target_date=target_date,
                market_codes=market_codes,
                signal_sql=materialized_signal,
            )

    materialized_codes: set[str] = set()
    table_row = reader.query_one(
        """
        SELECT count(*) AS table_count
        FROM information_schema.tables
        WHERE table_name = 'daily_technical_metrics'
        """
    )
    if table_row is not None and int(table_row["table_count"]) == 1:
        codes = tuple(items_by_code.keys())
        placeholders = ",".join("?" for _ in codes)
        materialized_rows = reader.query(
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
        for row in materialized_rows:
            code = str(row["code"])
            materialized_codes.add(code)
            count = int_or_none(row["sma5_above_count_5d"])
            below_streak = int_or_none(row["sma5_below_streak"])
            for item in items_by_code.get(code, []):
                if count is not None:
                    item.sma5AboveCount5d = count
                if below_streak is not None:
                    item.sma5BelowStreak = below_streak

    if materialized_codes == set(items_by_code):
        return

    codes = tuple(items_by_code.keys())
    placeholders = ",".join("?" for _ in codes)
    rows = reader.query(
        f"""
        WITH
        {signal_sql.cte_sql},
        sma_features AS (
            SELECT
                normalized_code AS code,
                date,
                close,
                AVG(close) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS sma5,
                COUNT(close) OVER (
                    PARTITION BY normalized_code
                    ORDER BY date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS sma5_sessions
            FROM {signal_sql.relation_name}
            WHERE date >= ? AND close > 0
        ),
        flags AS (
            SELECT
                *,
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
                *,
                SUM(CASE WHEN close_below_sma5_flag = 1 THEN 0 ELSE 1 END) OVER (
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
                    WHEN close_below_sma5_flag = 1 THEN
                        SUM(close_below_sma5_flag) OVER (
                            PARTITION BY code, sma5_below_reset_group
                            ORDER BY date
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )
                    ELSE 0
                END AS sma5_below_streak
            FROM flagged_groups
        )
        SELECT code, sma5_above_count_5d, sma5_below_streak
        FROM counted
        WHERE date = ?
          AND sma5_above_count_sessions = 5
          AND code IN ({placeholders})
        """,
        (*signal_sql.params, lower_bound_date, target_date, *codes),
    )
    for row in rows:
        code = str(row["code"])
        if code in materialized_codes:
            continue
        count = int_or_none(row["sma5_above_count_5d"])
        below_streak = int_or_none(row["sma5_below_streak"])
        for item in items_by_code.get(code, []):
            if count is not None:
                item.sma5AboveCount5d = count
            if below_streak is not None:
                item.sma5BelowStreak = below_streak
