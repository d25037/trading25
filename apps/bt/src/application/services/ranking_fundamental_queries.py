"""Fundamental ranking query helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd

from src.application.services.ranking_query_helpers import (
    build_market_filter,
    normalized_code_sql,
    prefer_4digit_order_sql,
    stock_data_dedup_cte,
    stocks_canonical_cte,
)
from src.infrastructure.db.market.market_reader import MarketDbReader

FUNDAMENTAL_BASE_COLUMNS = (
    "s.code, s.company_name, s.market_code, s.sector_33_name, "
    "sd.close as current_price, sd.volume"
)


def table_exists(reader: MarketDbReader, table_name: str) -> bool:
    row = reader.query_one(
        """
        SELECT 1 AS exists
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        LIMIT 1
        """,
        (table_name,),
    )
    return row is not None


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


def load_adjusted_daily_valuation_frame(
    reader: MarketDbReader,
    date: str,
    market_codes: list[str],
) -> pd.DataFrame:
    if not table_exists(reader, "daily_valuation"):
        return pd.DataFrame()
    market_clause, market_params = build_market_filter(market_codes)
    stocks_cte = stocks_canonical_cte()
    stock_daily_cte = stock_data_dedup_cte("stock_daily", where_clause="date = ?")
    valuation_norm = normalized_code_sql("code")
    valuation_order = prefer_4digit_order_sql("code")
    sql = f"""
        WITH
        {stocks_cte},
        {stock_daily_cte},
        valuation_canonical AS (
            SELECT
                normalized_code,
                date,
                price_basis_date,
                close,
                eps,
                bps,
                forward_eps,
                per,
                forward_per,
                p_op,
                forward_p_op,
                pbr,
                market_cap,
                free_float_market_cap,
                statement_disclosed_date,
                forward_eps_disclosed_date,
                forward_eps_source,
                basis_version
            FROM (
                SELECT
                    {valuation_norm} AS normalized_code,
                    date,
                    price_basis_date,
                    close,
                    eps,
                    bps,
                    forward_eps,
                    per,
                    forward_per,
                    p_op,
                    forward_p_op,
                    pbr,
                    market_cap,
                    free_float_market_cap,
                    statement_disclosed_date,
                    forward_eps_disclosed_date,
                    forward_eps_source,
                    basis_version,
                    ROW_NUMBER() OVER (
                        PARTITION BY {valuation_norm}, date
                        ORDER BY
                            price_basis_date DESC NULLS LAST,
                            basis_version DESC,
                            {valuation_order}
                    ) AS rn
                FROM daily_valuation
                WHERE date = ?
            )
            WHERE rn = 1
        )
        SELECT
            s.code,
            s.company_name,
            s.market_code,
            s.sector_33_name,
            COALESCE(v.close, sd.close) AS current_price,
            sd.volume,
            v.eps,
            v.bps,
            v.forward_eps,
            v.per,
            v.forward_per,
            v.p_op,
            v.forward_p_op,
            v.pbr,
            v.market_cap,
            v.free_float_market_cap,
            v.statement_disclosed_date,
            v.forward_eps_disclosed_date,
            v.forward_eps_source,
            v.price_basis_date,
            v.basis_version
        FROM valuation_canonical v
        JOIN stocks_canonical s
            ON s.normalized_code = v.normalized_code
        JOIN stock_daily sd
            ON sd.normalized_code = v.normalized_code
        WHERE 1 = 1{market_clause}
    """
    rows = reader.query(sql, (date, date, *market_params))
    return pd.DataFrame([dict(row.items()) for row in rows])
