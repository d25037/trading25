"""Fundamental ranking query helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.application.services.ranking_query_helpers import (
    build_market_filter,
    stock_data_dedup_cte,
    stocks_canonical_cte,
)
from src.infrastructure.db.market.market_reader import MarketDbReader

FUNDAMENTAL_BASE_COLUMNS = (
    "s.code, s.company_name, s.market_code, s.sector_33_name, "
    "sd.close as current_price, sd.volume"
)


def load_fundamental_stock_rows(
    reader: MarketDbReader,
    date: str,
    market_codes: list[str],
) -> list[Mapping[str, Any]]:
    market_clause, market_params = build_market_filter(market_codes)
    stocks_cte = stocks_canonical_cte()
    stock_daily_cte = stock_data_dedup_cte("stock_daily", where_clause="date = ?")
    sql = f"""
        WITH
        {stocks_cte},
        {stock_daily_cte}
        SELECT {FUNDAMENTAL_BASE_COLUMNS}
        FROM stocks_canonical s
        JOIN stock_daily sd
            ON sd.normalized_code = s.normalized_code
        WHERE 1 = 1{market_clause}
    """
    return reader.query(sql, (date, *market_params))
