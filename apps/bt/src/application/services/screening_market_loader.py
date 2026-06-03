"""
Screening Market Loader

screening の market データソース向けローダー。
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from loguru import logger

from src.infrastructure.external_api.dataset.statements_mixin import APIPeriodType
from src.infrastructure.db.market.market_reader import MarketDbQueryable
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.application.services import (
    screening_margin_loader,
    screening_price_loader,
    screening_statement_loader,
)

__all__ = [
    "load_market_multi_data",
    "load_market_sector_indices",
    "load_market_stock_sector_mapping",
    "load_market_topix_data",
]


def load_market_multi_data(
    reader: MarketDbQueryable,
    stock_codes: list[str],
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    include_margin_data: bool = False,
    include_statements_data: bool = False,
    period_type: APIPeriodType = "FY",
    include_forecast_revision: bool = False,
) -> tuple[dict[str, dict[str, pd.DataFrame]], list[str]]:
    """DuckDB から複数銘柄の screening 用データを取得"""
    warnings: list[str] = []
    normalized_codes = screening_price_loader.normalize_codes(stock_codes)
    if not normalized_codes:
        return {}, warnings

    try:
        daily_by_code = screening_price_loader.load_daily_by_code(
            reader,
            normalized_codes,
            start_date=start_date,
            end_date=end_date,
        )
    except Exception as e:  # noqa: BLE001 - loader should degrade gracefully
        logger.warning("market stock_data load failed: {}", e)
        return {}, [f"market daily load failed ({e})"]

    result: dict[str, dict[str, pd.DataFrame]] = {}
    daily_index_by_code: dict[str, pd.DatetimeIndex] = {}

    for code in normalized_codes:
        daily = daily_by_code.get(code)
        if daily is None or daily.empty:
            continue
        result[code] = {"daily": daily}
        daily_index_by_code[code] = pd.DatetimeIndex(daily.index)

    if include_margin_data and daily_index_by_code:
        try:
            margin_warnings = screening_margin_loader.attach_margin(
                reader,
                result,
                daily_index_by_code,
                start_date=start_date,
                end_date=end_date,
            )
            warnings.extend(margin_warnings)
        except Exception as e:  # noqa: BLE001 - degrade to daily/statements without margin
            logger.warning("market margin_data load failed: {}", e)
            warnings.append(f"market margin load failed ({e})")

    if include_statements_data and daily_index_by_code:
        statements_warnings = screening_statement_loader.attach_statements(
            reader,
            result,
            daily_index_by_code,
            start_date=start_date,
            end_date=end_date,
            period_type=period_type,
            include_forecast_revision=include_forecast_revision,
        )
        warnings.extend(statements_warnings)

    return result, warnings


def load_market_topix_data(
    reader: MarketDbQueryable,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """DuckDB topix_data から benchmark を取得"""
    sql = "SELECT date, open, high, low, close FROM topix_data"
    params: list[str] = []
    conds: list[str] = []

    if start_date:
        conds.append("date >= ?")
        params.append(start_date)
    if end_date:
        conds.append("date <= ?")
        params.append(end_date)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY date"

    rows = reader.query(sql, tuple(params))
    return screening_price_loader.rows_to_ohlc_df(rows)


def load_market_sector_indices(
    reader: MarketDbQueryable,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, pd.DataFrame]:
    """DuckDB indices_data から sector 指数を取得"""
    sql = """
        SELECT
            d.code,
            d.date,
            d.open,
            d.high,
            d.low,
            d.close,
            COALESCE(NULLIF(d.sector_name, ''), m.name) AS sector_name,
            m.category
        FROM indices_data d
        LEFT JOIN index_master m ON d.code = m.code
    """
    params: list[str] = []
    conds: list[str] = []

    if start_date:
        conds.append("d.date >= ?")
        params.append(start_date)
    if end_date:
        conds.append("d.date <= ?")
        params.append(end_date)

    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY d.code, d.date"

    try:
        rows = reader.query(sql, tuple(params))
    except Exception:  # noqa: BLE001 - optional data source
        return {}

    grouped: dict[str, list[Any]] = {}
    for row in rows:
        sector_name = str(row["sector_name"] or "").strip()
        category = str(row["category"] or "").strip().lower()

        if not sector_name:
            continue
        if category and not category.startswith("sector"):
            continue

        grouped.setdefault(sector_name, []).append(row)

    return {
        sector_name: screening_price_loader.rows_to_ohlc_df(sector_rows)
        for sector_name, sector_rows in grouped.items()
        if sector_rows
    }


def load_market_stock_sector_mapping(reader: MarketDbQueryable) -> dict[str, str]:
    """latest master から銘柄 -> sector_33_name を取得"""
    rows = reader.query(
        """
        SELECT code, sector_33_name
        FROM stocks_latest
        WHERE sector_33_name IS NOT NULL
        """
    )
    mapping: dict[str, str] = {}
    for row in rows:
        code = normalize_stock_code(str(row["code"]))
        sector = str(row["sector_33_name"] or "").strip()
        if code and sector:
            mapping[code] = sector
    return mapping
