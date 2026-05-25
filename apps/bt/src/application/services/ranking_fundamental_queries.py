"""Fundamental ranking query helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd

from src.application.services.ranking_query_helpers import (
    build_market_filter,
    normalize_equity_code,
    normalized_code_sql,
    prefer_4digit_order_sql,
    stock_data_dedup_cte,
    stocks_canonical_cte,
)
from src.shared.utils.share_adjustment import ShareAdjustmentEvent
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


def resolve_latest_stock_data_date(reader: MarketDbReader) -> str:
    row = reader.query_one("SELECT MAX(date) as max_date FROM stock_data")
    if row is None or row["max_date"] is None:
        raise ValueError("No trading data available in database")
    return str(row["max_date"])


def load_adjustment_events_by_code(
    reader: MarketDbReader,
    *,
    through_date: str,
    market_codes: list[str],
) -> dict[str, list[ShareAdjustmentEvent]]:
    if not table_exists(reader, "stock_data_raw"):
        return {}

    market_clause, market_params = build_market_filter(market_codes)
    raw_normalized = normalized_code_sql("raw.code")
    stocks_normalized = normalized_code_sql("s.code")
    raw_prefer_4digit = prefer_4digit_order_sql("raw.code")
    stocks_prefer_4digit = prefer_4digit_order_sql("s.code")
    sql = f"""
        WITH stocks_canonical AS (
            SELECT code, normalized_code, market_code
            FROM (
                SELECT
                    code,
                    market_code,
                    {stocks_normalized} AS normalized_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY {stocks_normalized}
                        ORDER BY {stocks_prefer_4digit}
                    ) AS rn
                FROM stocks s
            )
            WHERE rn = 1
        ),
        adjustment_canonical AS (
            SELECT
                s.code,
                raw.date,
                raw.adjustment_factor,
                ROW_NUMBER() OVER (
                    PARTITION BY s.code, raw.date
                    ORDER BY {raw_prefer_4digit}
                ) AS rn
            FROM stock_data_raw raw
            JOIN stocks_canonical s
                ON s.normalized_code = {raw_normalized}
            WHERE raw.date <= ?
              AND raw.adjustment_factor IS NOT NULL
              AND raw.adjustment_factor != 1.0
              {market_clause}
        )
        SELECT code, date, adjustment_factor
        FROM adjustment_canonical
        WHERE rn = 1
        ORDER BY code, date
    """
    grouped: dict[str, list[ShareAdjustmentEvent]] = {}
    for row in reader.query(sql, (through_date, *market_params)):
        code = normalize_equity_code(row["code"])
        grouped.setdefault(code, []).append(
            ShareAdjustmentEvent(
                date=str(row["date"]),
                adjustment_factor=float(row["adjustment_factor"]),
            )
        )
    return grouped


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


def load_adjusted_statement_metric_rows(
    reader: MarketDbReader,
    date: str,
    market_codes: list[str],
) -> list[Mapping[str, Any]]:
    if not table_exists(reader, "statement_metrics_adjusted"):
        return []
    market_clause, market_params = build_market_filter(market_codes)
    stocks_cte = stocks_canonical_cte()
    stock_daily_cte = stock_data_dedup_cte("stock_daily", where_clause="date = ?")
    metrics_norm = normalized_code_sql("code")
    metrics_order = prefer_4digit_order_sql("code")
    sql = f"""
        WITH
        {stocks_cte},
        {stock_daily_cte},
        metrics_canonical AS (
            SELECT
                normalized_code,
                disclosed_date,
                period_type,
                adjusted_eps,
                adjusted_bps,
                adjusted_forecast_eps,
                basis_version
            FROM (
                SELECT
                    {metrics_norm} AS normalized_code,
                    disclosed_date,
                    period_type,
                    adjusted_eps,
                    adjusted_bps,
                    adjusted_forecast_eps,
                    basis_version,
                    ROW_NUMBER() OVER (
                        PARTITION BY {metrics_norm}, disclosed_date
                        ORDER BY
                            price_basis_date DESC NULLS LAST,
                            basis_version DESC,
                            {metrics_order}
                    ) AS rn
                FROM statement_metrics_adjusted
                WHERE disclosed_date <= ?
            )
            WHERE rn = 1
        )
        SELECT
            s.code,
            m.disclosed_date,
            m.period_type,
            m.adjusted_eps,
            m.adjusted_bps,
            m.adjusted_forecast_eps,
            m.basis_version
        FROM metrics_canonical m
        JOIN stocks_canonical s
            ON s.normalized_code = m.normalized_code
        JOIN stock_daily sd
            ON sd.normalized_code = m.normalized_code
        WHERE 1 = 1{market_clause}
        ORDER BY s.code, m.disclosed_date DESC
    """
    return reader.query(sql, (date, date, *market_params))


def load_fundamental_statement_rows(
    reader: MarketDbReader,
    date: str,
    market_codes: list[str],
) -> list[Mapping[str, Any]]:
    market_clause, market_params = build_market_filter(market_codes)
    stocks_cte = stocks_canonical_cte()
    stock_daily_cte = stock_data_dedup_cte("stock_daily", where_clause="date = ?")
    statements_norm = normalized_code_sql("code")
    statements_order = prefer_4digit_order_sql("code")
    statement_columns = statement_table_columns(reader)
    forecast_operating_profit_expr = optional_statement_double_expr(
        "forecast_operating_profit",
        statement_columns,
    )
    next_year_forecast_operating_profit_expr = optional_statement_double_expr(
        "next_year_forecast_operating_profit",
        statement_columns,
    )
    sql = f"""
        WITH
        {stocks_cte},
        {stock_daily_cte},
        statements_canonical AS (
            SELECT
                normalized_code,
                disclosed_date,
                type_of_current_period,
                type_of_document,
                earnings_per_share,
                bps,
                forecast_eps,
                next_year_forecast_earnings_per_share,
                operating_profit,
                forecast_operating_profit,
                next_year_forecast_operating_profit,
                shares_outstanding,
                treasury_shares
            FROM (
                SELECT
                    {statements_norm} AS normalized_code,
                    disclosed_date,
                    type_of_current_period,
                    type_of_document,
                    earnings_per_share,
                    bps,
                    forecast_eps,
                    next_year_forecast_earnings_per_share,
                    operating_profit,
                    {forecast_operating_profit_expr},
                    {next_year_forecast_operating_profit_expr},
                    shares_outstanding,
                    treasury_shares,
                    ROW_NUMBER() OVER (
                        PARTITION BY {statements_norm}, disclosed_date
                        ORDER BY {statements_order}
                    ) AS rn
                FROM statements
            )
            WHERE rn = 1
        )
        SELECT
            s.code,
            st.disclosed_date,
            st.type_of_current_period,
            st.type_of_document,
            st.earnings_per_share,
            st.bps,
            st.forecast_eps,
            st.next_year_forecast_earnings_per_share,
            st.operating_profit,
            st.forecast_operating_profit,
            st.next_year_forecast_operating_profit,
            st.shares_outstanding,
            st.treasury_shares
        FROM statements_canonical st
        JOIN stocks_canonical s
            ON s.normalized_code = st.normalized_code
        JOIN stock_daily sd
            ON sd.normalized_code = st.normalized_code
        WHERE st.disclosed_date <= ?{market_clause}
        ORDER BY s.code, st.disclosed_date DESC
    """
    return reader.query(sql, (date, date, *market_params))


def statement_table_columns(reader: MarketDbReader) -> set[str]:
    try:
        rows = reader.query("SELECT name FROM pragma_table_info('statements')")
    except Exception:  # noqa: BLE001 - main statement query will surface the real failure
        return set()
    return {str(row["name"]) for row in rows}


def optional_statement_double_expr(column: str, columns: set[str]) -> str:
    if column in columns:
        return column
    return f"CAST(NULL AS DOUBLE) AS {column}"
