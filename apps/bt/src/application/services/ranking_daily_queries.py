"""Daily market ranking query helpers."""

from __future__ import annotations

from typing import Literal

from src.application.contracts import ranking as ranking_contracts
from src.application.services.ranking_query_helpers import (
    build_stock_scope_filter,
    limit_clause,
    stock_data_dedup_cte,
    stocks_canonical_cte,
)
from src.application.services.ranking_response_items import build_ranking_item
from src.infrastructure.db.market.market_reader import MarketDbReader


RANKING_BASE_COLUMNS = "s.code, s.company_name, s.market_code, s.sector_33_name"


def get_trading_date_before(reader: MarketDbReader, date: str, offset: int) -> str | None:
    """N営業日前の取引日を取得"""
    row = reader.query_one(
        "SELECT DISTINCT date FROM stock_data WHERE date < ? ORDER BY date DESC LIMIT 1 OFFSET ?",
        (date, offset),
    )
    return row["date"] if row else None


def ranking_by_trading_value(
    reader: MarketDbReader,
    date: str,
    limit: int,
    market_codes: list[str],
    *,
    sector33_name: str | None = None,
    sector17_name: str | None = None,
) -> list[ranking_contracts.RankingItem]:
    """売買代金ランキング（単日）"""
    market_clause, market_params = build_stock_scope_filter(
        market_codes,
        sector33_name=sector33_name,
        sector17_name=sector17_name,
    )
    stocks_cte = stocks_canonical_cte()
    stock_daily_cte = stock_data_dedup_cte("stock_daily", where_clause="date = ?")
    prev_cte = stock_data_dedup_cte("prev_daily", where_clause="date = ?")
    limit_sql, limit_params = limit_clause(limit)
    sql = f"""
        WITH
        {stocks_cte},
        {stock_daily_cte},
        {prev_cte}
        SELECT {RANKING_BASE_COLUMNS},
            sd.close as current_price,
            sd.volume,
            sd.close * sd.volume as trading_value,
            prev.close as previous_price,
            (sd.close - prev.close) as change_amount,
            CASE
                WHEN prev.close > 0 AND sd.close > 0
                THEN ((sd.close - prev.close) / prev.close * 100)
                ELSE NULL
            END as change_percentage
        FROM stock_daily sd
        LEFT JOIN prev_daily prev
            ON sd.normalized_code = prev.normalized_code
        JOIN stocks_canonical s
            ON s.normalized_code = sd.normalized_code
        WHERE 1 = 1{market_clause}
        ORDER BY trading_value DESC{limit_sql}
    """
    prev_date = get_trading_date_before(reader, date, 0)
    rows = reader.query(sql, (date, date, prev_date or "", *market_params, *limit_params))
    return [
        build_ranking_item(
            row,
            i + 1,
            tradingValue=row["trading_value"],
            previousPrice=row["previous_price"],
            changeAmount=row["change_amount"],
            changePercentage=row["change_percentage"],
        )
        for i, row in enumerate(rows)
    ]


def ranking_by_trading_value_average(
    reader: MarketDbReader,
    date: str,
    lookback_days: int,
    limit: int,
    market_codes: list[str],
    *,
    sector33_name: str | None = None,
    sector17_name: str | None = None,
) -> list[ranking_contracts.RankingItem]:
    """売買代金平均ランキング（N日平均）"""
    start_date = get_trading_date_before(reader, date, lookback_days - 1)
    if not start_date:
        return []
    base_date = get_trading_date_before(reader, date, lookback_days)
    if not base_date:
        return []

    market_clause, market_params = build_stock_scope_filter(
        market_codes,
        sector33_name=sector33_name,
        sector17_name=sector17_name,
    )
    stocks_cte = stocks_canonical_cte()
    stock_window_cte = stock_data_dedup_cte(
        "stock_window",
        where_clause="date >= ? AND date <= ?",
    )
    curr_cte = stock_data_dedup_cte("curr_daily", where_clause="date = ?")
    base_cte = stock_data_dedup_cte("base_daily", where_clause="date = ?")
    limit_sql, limit_params = limit_clause(limit)
    sql = f"""
        WITH
        {stocks_cte},
        {stock_window_cte},
        {curr_cte},
        {base_cte}
        SELECT {RANKING_BASE_COLUMNS},
            curr.close as current_price,
            SUM(sd.volume) as volume,
            AVG(sd.close * sd.volume) as avg_trading_value,
            base.close as base_price,
            (curr.close - base.close) as change_amount,
            CASE
                WHEN base.close > 0 AND curr.close > 0
                THEN ((curr.close - base.close) / base.close * 100)
                ELSE NULL
            END as change_percentage
        FROM stock_window sd
        JOIN curr_daily curr
            ON curr.normalized_code = sd.normalized_code
        JOIN base_daily base
            ON base.normalized_code = sd.normalized_code
        JOIN stocks_canonical s
            ON s.normalized_code = sd.normalized_code
        WHERE 1 = 1{market_clause}
        GROUP BY
            s.code,
            s.company_name,
            s.market_code,
            s.sector_33_name,
            curr.close,
            base.close
        ORDER BY avg_trading_value DESC{limit_sql}
    """
    rows = reader.query(
        sql,
        (date, start_date, date, date, base_date, *market_params, *limit_params),
    )
    return [
        build_ranking_item(
            row,
            i + 1,
            tradingValueAverage=row["avg_trading_value"],
            basePrice=row["base_price"],
            changeAmount=row["change_amount"],
            changePercentage=row["change_percentage"],
            lookbackDays=lookback_days,
        )
        for i, row in enumerate(rows)
    ]


def _ranking_by_price_change_against_base(
    reader: MarketDbReader,
    date: str,
    base_date: str,
    limit: int,
    market_codes: list[str],
    order_dir: Literal["ASC", "DESC"],
    *,
    lookback_days: int | None = None,
    sector33_name: str | None = None,
    sector17_name: str | None = None,
) -> list[ranking_contracts.RankingItem]:
    market_clause, market_params = build_stock_scope_filter(
        market_codes,
        sector33_name=sector33_name,
        sector17_name=sector17_name,
    )
    stocks_cte = stocks_canonical_cte()
    curr_cte = stock_data_dedup_cte("curr_daily", where_clause="date = ?")
    base_cte = stock_data_dedup_cte("base_daily", where_clause="date = ?")
    limit_sql, limit_params = limit_clause(limit)
    sql = f"""
        WITH
        {stocks_cte},
        {curr_cte},
        {base_cte}
        SELECT {RANKING_BASE_COLUMNS},
            curr.close as current_price,
            curr.volume,
            base.close as base_price,
            (curr.close - base.close) as change_amount,
            ((curr.close - base.close) / base.close * 100) as change_percentage
        FROM curr_daily curr
        JOIN base_daily base
            ON curr.normalized_code = base.normalized_code
        JOIN stocks_canonical s
            ON s.normalized_code = curr.normalized_code
        WHERE 1 = 1
            AND base.close > 0
            AND curr.close > 0
            AND curr.close != base.close{market_clause}
        ORDER BY change_percentage {order_dir}{limit_sql}
    """
    rows = reader.query(sql, (date, date, base_date, *market_params, *limit_params))
    return [
        build_ranking_item(
            row,
            i + 1,
            previousPrice=row["base_price"] if lookback_days is None else None,
            basePrice=row["base_price"] if lookback_days is not None else None,
            changeAmount=row["change_amount"],
            changePercentage=row["change_percentage"],
            lookbackDays=lookback_days,
        )
        for i, row in enumerate(rows)
    ]


def ranking_by_price_change(
    reader: MarketDbReader,
    date: str,
    limit: int,
    market_codes: list[str],
    order_dir: Literal["ASC", "DESC"],
    *,
    sector33_name: str | None = None,
    sector17_name: str | None = None,
) -> list[ranking_contracts.RankingItem]:
    """騰落率ランキング（単日）"""
    prev_date = get_trading_date_before(reader, date, 0)
    if not prev_date:
        return []

    return _ranking_by_price_change_against_base(
        reader,
        date,
        prev_date,
        limit,
        market_codes,
        order_dir,
        sector33_name=sector33_name,
        sector17_name=sector17_name,
    )


def ranking_by_price_change_from_days(
    reader: MarketDbReader,
    date: str,
    lookback_days: int,
    limit: int,
    market_codes: list[str],
    order_dir: Literal["ASC", "DESC"],
    *,
    sector33_name: str | None = None,
    sector17_name: str | None = None,
) -> list[ranking_contracts.RankingItem]:
    """騰落率ランキング（N日前比較）"""
    base_date = get_trading_date_before(reader, date, lookback_days)
    if not base_date:
        return []

    return _ranking_by_price_change_against_base(
        reader,
        date,
        base_date,
        limit,
        market_codes,
        order_dir,
        lookback_days=lookback_days,
        sector33_name=sector33_name,
        sector17_name=sector17_name,
    )


def _ranking_by_period_extreme(
    reader: MarketDbReader,
    date: str,
    period_days: int,
    limit: int,
    market_codes: list[str],
    *,
    aggregate_expr: Literal["MAX(high)", "MIN(low)"],
    comparison_operator: Literal[">=", "<="],
    order_dir: Literal["ASC", "DESC"],
    sector33_name: str | None = None,
    sector17_name: str | None = None,
) -> list[ranking_contracts.RankingItem]:
    start_date = get_trading_date_before(reader, date, period_days)
    if not start_date:
        return []

    market_clause, market_params = build_stock_scope_filter(
        market_codes,
        sector33_name=sector33_name,
        sector17_name=sector17_name,
    )
    stocks_cte = stocks_canonical_cte()
    stock_window_cte = stock_data_dedup_cte(
        "stock_window",
        where_clause="date > ? AND date < ?",
    )
    curr_cte = stock_data_dedup_cte("curr_daily", where_clause="date = ?")
    limit_sql, limit_params = limit_clause(limit)
    sql = f"""
        WITH
        {stocks_cte},
        {stock_window_cte},
        {curr_cte},
        period_extreme AS (
            SELECT normalized_code, {aggregate_expr} as period_extreme_price
            FROM stock_window
            GROUP BY normalized_code
        )
        SELECT {RANKING_BASE_COLUMNS},
            curr.close as current_price,
            curr.volume,
            curr.close * curr.volume as trading_value,
            pe.period_extreme_price as base_price,
            (curr.close - pe.period_extreme_price) as change_amount,
            ((curr.close - pe.period_extreme_price) / pe.period_extreme_price * 100) as change_percentage
        FROM curr_daily curr
        JOIN stocks_canonical s
            ON s.normalized_code = curr.normalized_code
        JOIN period_extreme pe
            ON pe.normalized_code = curr.normalized_code
        WHERE curr.close {comparison_operator} pe.period_extreme_price
            AND pe.period_extreme_price > 0{market_clause}
        ORDER BY change_percentage {order_dir}{limit_sql}
    """
    rows = reader.query(sql, (date, start_date, date, date, *market_params, *limit_params))
    return [
        build_ranking_item(
            row,
            i + 1,
            tradingValue=row["trading_value"],
            basePrice=row["base_price"],
            changeAmount=row["change_amount"],
            changePercentage=row["change_percentage"],
            lookbackDays=period_days,
        )
        for i, row in enumerate(rows)
    ]


def ranking_by_period_high(
    reader: MarketDbReader,
    date: str,
    period_days: int,
    limit: int,
    market_codes: list[str],
    *,
    sector33_name: str | None = None,
    sector17_name: str | None = None,
) -> list[ranking_contracts.RankingItem]:
    """期間高値ランキング"""
    return _ranking_by_period_extreme(
        reader,
        date,
        period_days,
        limit,
        market_codes,
        aggregate_expr="MAX(high)",
        comparison_operator=">=",
        order_dir="DESC",
        sector33_name=sector33_name,
        sector17_name=sector17_name,
    )


def ranking_by_period_low(
    reader: MarketDbReader,
    date: str,
    period_days: int,
    limit: int,
    market_codes: list[str],
    *,
    sector33_name: str | None = None,
    sector17_name: str | None = None,
) -> list[ranking_contracts.RankingItem]:
    """期間安値ランキング"""
    return _ranking_by_period_extreme(
        reader,
        date,
        period_days,
        limit,
        market_codes,
        aggregate_expr="MIN(low)",
        comparison_operator="<=",
        order_dir="ASC",
        sector33_name=sector33_name,
        sector17_name=sector17_name,
    )
