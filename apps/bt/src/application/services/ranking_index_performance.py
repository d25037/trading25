"""Index performance loading for ranking responses."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from src.entrypoints.http.schemas.ranking import IndexPerformanceItem
from src.infrastructure.db.market.market_reader import MarketDbReader

from .ranking_query_helpers import (
    normalize_sector_filter_name,
    normalized_code_sql,
    prefer_4digit_order_sql,
)

_SECTOR_STRENGTH_HISTORY_CALENDAR_DAYS = 160


def _sector_strength_bucket(score: float | None) -> str | None:
    if score is None:
        return None
    if score >= 0.8:
        return "sector_strong"
    if score <= 0.2:
        return "sector_weak"
    return "sector_neutral"


def _history_start_date(target_date: str) -> str:
    parsed = datetime.strptime(target_date, "%Y-%m-%d").date()
    return (parsed - timedelta(days=_SECTOR_STRENGTH_HISTORY_CALENDAR_DAYS)).isoformat()


def load_sector_strength_by_name(
    reader: MarketDbReader,
    *,
    table_exists: Callable[[str], bool],
    date: str,
    market_codes: list[str],
) -> dict[str, dict[str, Any]]:
    required_tables = ("stock_master_daily", "stock_data", "topix_data", "indices_data")
    if not all(table_exists(table_name) for table_name in required_tables):
        return {}

    market_clause = ""
    market_params: list[str] = []
    if market_codes:
        market_clause = f"AND m.market_code IN ({','.join('?' for _ in market_codes)})"
        market_params = list(market_codes)

    price_code = normalized_code_sql("code")
    master_code = normalized_code_sql("m.code")
    prefer_price_code = prefer_4digit_order_sql("code")
    prefer_master_code = prefer_4digit_order_sql("m.code")
    history_start_date = _history_start_date(date)

    rows = reader.query(
        f"""
        WITH topix_ranked AS (
            SELECT
                date,
                close,
                ROW_NUMBER() OVER (ORDER BY date DESC) AS rn
            FROM topix_data
            WHERE date <= ?
                AND date >= ?
                AND close IS NOT NULL
                AND close > 0
        ),
        topix_points AS (
            SELECT
                MAX(CASE WHEN rn = 1 THEN close END) AS current_close,
                MAX(CASE WHEN rn = 21 THEN close END) AS close_20d,
                MAX(CASE WHEN rn = 61 THEN close END) AS close_60d
            FROM topix_ranked
        ),
        sector_index_map(sector_33_name, sector_index_code) AS (
            VALUES
                ('水産･農林業', '0040'),
                ('鉱業', '0041'),
                ('建設業', '0042'),
                ('食料品', '0043'),
                ('繊維製品', '0044'),
                ('パルプ･紙', '0045'),
                ('化学', '0046'),
                ('医薬品', '0047'),
                ('石油･石炭製品', '0048'),
                ('ゴム製品', '0049'),
                ('ガラス･土石製品', '004A'),
                ('鉄鋼', '004B'),
                ('非鉄金属', '004C'),
                ('金属製品', '004D'),
                ('機械', '004E'),
                ('電気機器', '004F'),
                ('輸送用機器', '0050'),
                ('精密機器', '0051'),
                ('その他製品', '0052'),
                ('電気･ガス業', '0053'),
                ('陸運業', '0054'),
                ('海運業', '0055'),
                ('空運業', '0056'),
                ('倉庫･運輸関連業', '0057'),
                ('情報･通信業', '0058'),
                ('卸売業', '0059'),
                ('小売業', '005A'),
                ('銀行業', '005B'),
                ('証券･商品先物取引業', '005C'),
                ('保険業', '005D'),
                ('その他金融業', '005E'),
                ('不動産業', '005F'),
                ('サービス業', '0060')
        ),
        sector_index_ranked AS (
            SELECT
                sim.sector_33_name,
                sim.sector_index_code,
                i.date,
                i.close,
                ROW_NUMBER() OVER (
                    PARTITION BY sim.sector_33_name
                    ORDER BY i.date DESC
                ) AS rn
            FROM sector_index_map sim
            JOIN indices_data i
              ON i.code = sim.sector_index_code
            WHERE i.date <= ?
                AND i.date >= ?
                AND i.close IS NOT NULL
                AND i.close > 0
        ),
        sector_index_points AS (
            SELECT
                sector_33_name,
                sector_index_code,
                MAX(CASE WHEN rn = 1 THEN close END) AS current_close,
                MAX(CASE WHEN rn = 6 THEN close END) AS close_5d,
                MAX(CASE WHEN rn = 21 THEN close END) AS close_20d,
                MAX(CASE WHEN rn = 61 THEN close END) AS close_60d
            FROM sector_index_ranked
            GROUP BY sector_33_name, sector_index_code
        ),
        sector_index_metrics AS (
            SELECT
                sector_33_name,
                sector_index_code,
                ((sip.current_close / sip.close_5d) - 1.0) * 100.0
                    - ((t.current_close / t.close_5d) - 1.0) * 100.0
                    AS sector_index_5d_topix_excess_pct,
                ((sip.current_close / sip.close_20d) - 1.0) * 100.0
                    - ((t.current_close / t.close_20d) - 1.0) * 100.0
                    AS sector_index_20d_topix_excess_pct,
                ((sip.current_close / sip.close_60d) - 1.0) * 100.0
                    - ((t.current_close / t.close_60d) - 1.0) * 100.0
                    AS sector_index_60d_topix_excess_pct
            FROM sector_index_points sip
            CROSS JOIN (
                SELECT
                    tp.current_close,
                    MAX(CASE WHEN tr.rn = 6 THEN tr.close END) AS close_5d,
                    tp.close_20d,
                    tp.close_60d
                FROM topix_ranked tr
                CROSS JOIN topix_points tp
                GROUP BY tp.current_close, tp.close_20d, tp.close_60d
            ) t
            WHERE sip.current_close IS NOT NULL
                AND sip.close_5d IS NOT NULL
                AND sip.close_20d IS NOT NULL
                AND sip.close_60d IS NOT NULL
                AND sip.close_5d > 0
                AND sip.close_20d > 0
                AND sip.close_60d > 0
                AND t.current_close IS NOT NULL
                AND t.close_5d IS NOT NULL
                AND t.close_20d IS NOT NULL
                AND t.close_60d IS NOT NULL
                AND t.close_5d > 0
                AND t.close_20d > 0
                AND t.close_60d > 0
        ),
        price_dedup AS (
            SELECT
                normalized_code,
                date,
                close
            FROM (
                SELECT
                    {price_code} AS normalized_code,
                    date,
                    close,
                    ROW_NUMBER() OVER (
                        PARTITION BY {price_code}, date
                        ORDER BY {prefer_price_code}
                    ) AS rn
                FROM stock_data
                WHERE date <= ?
                    AND date >= ?
                    AND close IS NOT NULL
                    AND close > 0
            )
            WHERE rn = 1
        ),
        price_ranked AS (
            SELECT
                normalized_code,
                date,
                close,
                ROW_NUMBER() OVER (
                    PARTITION BY normalized_code
                    ORDER BY date DESC
                ) AS rn
            FROM price_dedup
        ),
        price_points AS (
            SELECT
                normalized_code,
                MAX(CASE WHEN rn = 1 THEN close END) AS current_close,
                MAX(CASE WHEN rn = 21 THEN close END) AS close_20d,
                MAX(CASE WHEN rn = 61 THEN close END) AS close_60d
            FROM price_ranked
            GROUP BY normalized_code
        ),
        stock_master AS (
            SELECT
                normalized_code,
                sector_33_name
            FROM (
                SELECT
                    {master_code} AS normalized_code,
                    replace(trim(m.sector_33_name), '・', '･') AS sector_33_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY {master_code}
                        ORDER BY {prefer_master_code}
                    ) AS rn
                FROM stock_master_daily m
                WHERE m.date = ?
                    AND m.sector_33_name IS NOT NULL
                    AND trim(m.sector_33_name) != ''
                    {market_clause}
            )
            WHERE rn = 1
        ),
        stock_returns AS (
            SELECT
                m.sector_33_name,
                p.normalized_code,
                ((p.current_close / p.close_20d) - 1.0) * 100.0
                    - ((t.current_close / t.close_20d) - 1.0) * 100.0 AS excess_20d,
                ((p.current_close / p.close_60d) - 1.0) * 100.0
                    - ((t.current_close / t.close_60d) - 1.0) * 100.0 AS excess_60d
            FROM price_points p
            JOIN stock_master m
                ON m.normalized_code = p.normalized_code
            CROSS JOIN topix_points t
            WHERE p.current_close IS NOT NULL
                AND p.close_20d IS NOT NULL
                AND p.close_60d IS NOT NULL
                AND p.close_20d > 0
                AND p.close_60d > 0
                AND t.current_close IS NOT NULL
                AND t.close_20d IS NOT NULL
                AND t.close_60d IS NOT NULL
                AND t.close_20d > 0
                AND t.close_60d > 0
        ),
        sector_metrics AS (
            SELECT
                sector_33_name,
                COUNT(*) AS stock_count,
                AVG(excess_20d) AS sector_20d_topix_excess_pct,
                AVG(excess_60d) AS sector_60d_topix_excess_pct,
                AVG(CASE WHEN excess_20d > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                    AS sector_breadth_20d_pct
            FROM stock_returns
            GROUP BY sector_33_name
        ),
        ranked_metrics AS (
            SELECT
                sm.*,
                sim.sector_index_code,
                sim.sector_index_5d_topix_excess_pct,
                sim.sector_index_20d_topix_excess_pct,
                sim.sector_index_60d_topix_excess_pct,
                CASE
                    WHEN COUNT(*) OVER () = 1 THEN 0.5
                    ELSE PERCENT_RANK() OVER (ORDER BY sim.sector_index_5d_topix_excess_pct)
                END AS rank_index_5d,
                CASE
                    WHEN COUNT(*) OVER () = 1 THEN 0.5
                    ELSE PERCENT_RANK() OVER (ORDER BY sim.sector_index_20d_topix_excess_pct)
                END AS rank_index_20d,
                CASE
                    WHEN COUNT(*) OVER () = 1 THEN 0.5
                    ELSE PERCENT_RANK() OVER (ORDER BY sim.sector_index_60d_topix_excess_pct)
                END AS rank_index_60d,
                CASE
                    WHEN COUNT(*) OVER () = 1 THEN 0.5
                    ELSE PERCENT_RANK() OVER (ORDER BY sm.sector_20d_topix_excess_pct)
                END AS rank_constituent_20d,
                CASE
                    WHEN COUNT(*) OVER () = 1 THEN 0.5
                    ELSE PERCENT_RANK() OVER (ORDER BY sm.sector_60d_topix_excess_pct)
                END AS rank_constituent_60d,
                CASE
                    WHEN COUNT(*) OVER () = 1 THEN 0.5
                    ELSE PERCENT_RANK() OVER (ORDER BY sm.sector_breadth_20d_pct)
                END AS rank_breadth
            FROM sector_metrics sm
            JOIN sector_index_metrics sim
              ON sim.sector_33_name = sm.sector_33_name
        ),
        scored_metrics AS (
            SELECT
                *,
                (
                    rank_index_5d * 0.20
                    + rank_index_20d * 0.45
                    + rank_index_60d * 0.25
                    + rank_breadth * 0.10
                ) AS sector_index_strength_score,
                (
                    rank_constituent_20d
                    + rank_constituent_60d
                    + rank_breadth
                ) / 3.0 AS sector_constituent_strength_score
            FROM ranked_metrics
        )
        SELECT
            sector_33_name,
            stock_count,
            sector_20d_topix_excess_pct,
            sector_60d_topix_excess_pct,
            sector_breadth_20d_pct,
            (
                sector_index_strength_score
                + sector_constituent_strength_score
            ) / 2.0 AS sector_strength_score
        FROM scored_metrics
        """,
        (
            date,
            history_start_date,
            date,
            history_start_date,
            date,
            history_start_date,
            date,
            *market_params,
        ),
    )

    strength_by_name: dict[str, dict[str, Any]] = {}
    for row in rows:
        score = float(row["sector_strength_score"])
        sector_name = str(row["sector_33_name"])
        strength_by_name[sector_name] = {
            "sectorStrengthScore": score,
            "sectorStrengthBucket": _sector_strength_bucket(score),
            "sector20dTopixExcessPct": float(row["sector_20d_topix_excess_pct"]),
            "sector60dTopixExcessPct": float(row["sector_60d_topix_excess_pct"]),
            "sectorBreadth20dPct": float(row["sector_breadth_20d_pct"]),
            "sectorStockCount": int(row["stock_count"]),
        }
    return strength_by_name


def load_index_performance(
    reader: MarketDbReader,
    *,
    table_exists: Callable[[str], bool],
    date: str,
    lookback_days: int,
    market_codes: list[str] | None = None,
    include_sector_strength: bool = False,
    sector_strength_by_name: dict[str, dict[str, Any]] | None = None,
) -> list[IndexPerformanceItem]:
    if lookback_days < 1:
        return []
    if not table_exists("index_master") or not table_exists("indices_data"):
        return []

    if include_sector_strength:
        if sector_strength_by_name is None:
            sector_strength_by_name = load_sector_strength_by_name(
                reader,
                table_exists=table_exists,
                date=date,
                market_codes=market_codes or [],
            )
    else:
        sector_strength_by_name = {}

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
    items: list[IndexPerformanceItem] = []
    for row in rows:
        strength = None
        if row["category"] == "sector33":
            sector_name = normalize_sector_filter_name(str(row["name"]))
            strength = sector_strength_by_name.get(sector_name)
        items.append(
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
                **(strength or {}),
            )
        )
    return items
