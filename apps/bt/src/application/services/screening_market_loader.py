"""
Screening Market Loader

screening の market データソース向けローダー。
"""

from __future__ import annotations

import sqlite3
from typing import Any

import pandas as pd
from loguru import logger

from src.infrastructure.external_api.dataset.statements_mixin import APIPeriodType
from src.infrastructure.data_access.loaders.statements_loaders import (
    merge_forward_forecast_revision,
    transform_statements_df,
)
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.shared.models.types import normalize_period_type

_LEGACY_PERIOD_TYPE_MAP = {
    "1Q": "Q1",
    "2Q": "Q2",
    "3Q": "Q3",
}

_STATEMENT_DB_TO_API_COLUMNS = {
    "disclosed_date": "disclosedDate",
    "earnings_per_share": "earningsPerShare",
    "profit": "profit",
    "equity": "equity",
    "type_of_current_period": "typeOfCurrentPeriod",
    "type_of_document": "typeOfDocument",
    "next_year_forecast_earnings_per_share": "nextYearForecastEarningsPerShare",
    "bps": "bps",
    "sales": "sales",
    "operating_profit": "operatingProfit",
    "ordinary_profit": "ordinaryProfit",
    "operating_cash_flow": "operatingCashFlow",
    "dividend_fy": "dividendFY",
    "forecast_dividend_fy": "forecastDividendFY",
    "next_year_forecast_dividend_fy": "nextYearForecastDividendFY",
    "payout_ratio": "payoutRatio",
    "forecast_payout_ratio": "forecastPayoutRatio",
    "next_year_forecast_payout_ratio": "nextYearForecastPayoutRatio",
    "forecast_eps": "forecastEps",
    "investing_cash_flow": "investingCashFlow",
    "financing_cash_flow": "financingCashFlow",
    "cash_and_equivalents": "cashAndEquivalents",
    "total_assets": "totalAssets",
    "shares_outstanding": "sharesOutstanding",
    "treasury_shares": "treasuryShares",
}


def load_market_multi_data(
    reader: MarketDbReader,
    stock_codes: list[str],
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    include_statements_data: bool = False,
    period_type: APIPeriodType = "FY",
    include_forecast_revision: bool = False,
) -> tuple[dict[str, dict[str, pd.DataFrame]], list[str]]:
    """market.db から複数銘柄の screening 用データを取得"""
    warnings: list[str] = []
    normalized_codes = _normalize_codes(stock_codes)
    if not normalized_codes:
        return {}, warnings

    try:
        daily_by_code = _load_daily_by_code(
            reader,
            normalized_codes,
            start_date=start_date,
            end_date=end_date,
        )
    except sqlite3.OperationalError as e:
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

    if include_statements_data and daily_index_by_code:
        statements_warnings = _attach_statements(
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
    reader: MarketDbReader,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """market.db topix_data から benchmark を取得"""
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
    return _rows_to_ohlc_df(rows)


def load_market_sector_indices(
    reader: MarketDbReader,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, pd.DataFrame]:
    """market.db indices_data から sector 指数を取得"""
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
    except sqlite3.OperationalError:
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
        sector_name: _rows_to_ohlc_df(sector_rows)
        for sector_name, sector_rows in grouped.items()
        if sector_rows
    }


def load_market_stock_sector_mapping(reader: MarketDbReader) -> dict[str, str]:
    """stocks から銘柄 -> sector_33_name を取得"""
    rows = reader.query(
        """
        SELECT code, sector_33_name
        FROM stocks
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


def _normalize_codes(stock_codes: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for stock_code in stock_codes:
        code = normalize_stock_code(stock_code)
        if not code or code in seen:
            continue
        seen.add(code)
        deduped.append(code)
    return deduped


def _load_daily_by_code(
    reader: MarketDbReader,
    stock_codes: list[str],
    *,
    start_date: str | None,
    end_date: str | None,
) -> dict[str, pd.DataFrame]:
    placeholders = ",".join("?" for _ in stock_codes)
    sql = f"""
        SELECT code, date, open, high, low, close, volume
        FROM stock_data
        WHERE code IN ({placeholders})
    """
    params: list[str] = list(stock_codes)
    conds: list[str] = []

    if start_date:
        conds.append("date >= ?")
        params.append(start_date)
    if end_date:
        conds.append("date <= ?")
        params.append(end_date)
    if conds:
        sql += " AND " + " AND ".join(conds)
    sql += " ORDER BY code, date"

    rows = reader.query(sql, tuple(params))

    grouped: dict[str, list[Any]] = {}
    for row in rows:
        grouped.setdefault(str(row["code"]), []).append(row)

    return {
        code: _rows_to_ohlcv_df(grouped.get(code, []))
        for code in stock_codes
    }


def _attach_statements(
    reader: MarketDbReader,
    result: dict[str, dict[str, pd.DataFrame]],
    daily_index_by_code: dict[str, pd.DatetimeIndex],
    *,
    start_date: str | None,
    end_date: str | None,
    period_type: APIPeriodType,
    include_forecast_revision: bool,
) -> list[str]:
    warnings: list[str] = []
    codes = list(daily_index_by_code.keys())
    if not codes:
        return warnings

    should_merge_forecast_revision = (
        include_forecast_revision and normalize_period_type(period_type) == "FY"
    )

    try:
        base_rows = _query_statements_rows(
            reader,
            codes,
            start_date=start_date,
            end_date=end_date,
            period_type=period_type,
            actual_only=True,
        )
    except sqlite3.OperationalError as e:
        if "no such table" in str(e).lower():
            warnings.append("market statements table is missing; statements signals may be skipped")
            return warnings
        raise

    revision_rows: list[Any] = []
    if should_merge_forecast_revision:
        try:
            revision_rows = _query_statements_rows(
                reader,
                codes,
                start_date=start_date,
                end_date=end_date,
                period_type="all",
                actual_only=False,
            )
        except sqlite3.OperationalError as e:
            warnings.append(f"market statements revision load failed ({e})")

    base_map = _group_statement_rows(base_rows)
    revision_map = _group_statement_rows(revision_rows) if revision_rows else {}

    for code, daily_index in daily_index_by_code.items():
        base_df = base_map.get(code)
        if base_df is None or base_df.empty:
            continue

        try:
            base_daily = transform_statements_df(base_df).reindex(daily_index).ffill()
            if should_merge_forecast_revision:
                revision_df = revision_map.get(code)
                if revision_df is not None and not revision_df.empty:
                    revision_daily = transform_statements_df(revision_df).reindex(daily_index).ffill()
                    base_daily = merge_forward_forecast_revision(base_daily, revision_daily)
            result.setdefault(code, {})["statements_daily"] = base_daily
        except Exception as e:  # noqa: BLE001 - screening should continue
            warnings.append(f"{code} statements transform failed ({e})")

    return warnings


def _query_statements_rows(
    reader: MarketDbReader,
    stock_codes: list[str],
    *,
    start_date: str | None,
    end_date: str | None,
    period_type: APIPeriodType | str,
    actual_only: bool,
) -> list[Any]:
    placeholders = ",".join("?" for _ in stock_codes)
    sql = f"""
        SELECT
            code,
            disclosed_date,
            earnings_per_share,
            profit,
            equity,
            type_of_current_period,
            type_of_document,
            next_year_forecast_earnings_per_share,
            bps,
            sales,
            operating_profit,
            ordinary_profit,
            operating_cash_flow,
            dividend_fy,
            forecast_dividend_fy,
            next_year_forecast_dividend_fy,
            payout_ratio,
            forecast_payout_ratio,
            next_year_forecast_payout_ratio,
            forecast_eps,
            investing_cash_flow,
            financing_cash_flow,
            cash_and_equivalents,
            total_assets,
            shares_outstanding,
            treasury_shares
        FROM statements
        WHERE code IN ({placeholders})
    """
    params: list[Any] = list(stock_codes)

    if start_date:
        sql += " AND disclosed_date >= ?"
        params.append(start_date)
    if end_date:
        sql += " AND disclosed_date <= ?"
        params.append(end_date)

    period_values = _resolve_period_filter_values(str(period_type))
    if period_values:
        placeholders_period = ",".join("?" for _ in period_values)
        sql += f" AND type_of_current_period IN ({placeholders_period})"
        params.extend(period_values)

    if actual_only:
        sql += """
            AND (
                earnings_per_share IS NOT NULL
                OR profit IS NOT NULL
                OR equity IS NOT NULL
            )
        """

    sql += " ORDER BY code, disclosed_date"
    return reader.query(sql, tuple(params))


def _resolve_period_filter_values(period_type: str) -> list[str] | None:
    normalized = normalize_period_type(period_type)
    if normalized is None or normalized == "all":
        return None
    values = [normalized]
    legacy = _LEGACY_PERIOD_TYPE_MAP.get(normalized)
    if legacy:
        values.append(legacy)
    return values


def _group_statement_rows(rows: list[Any]) -> dict[str, pd.DataFrame]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        code = normalize_stock_code(str(row["code"]))
        grouped.setdefault(code, []).append({
            "disclosedDate": row["disclosed_date"],
            **{
                api_col: row[db_col]
                for db_col, api_col in _STATEMENT_DB_TO_API_COLUMNS.items()
                if db_col != "disclosed_date"
            },
        })

    result: dict[str, pd.DataFrame] = {}
    for code, records in grouped.items():
        if not records:
            continue
        df = pd.DataFrame(records)
        df["disclosedDate"] = pd.to_datetime(df["disclosedDate"])
        df = df.set_index("disclosedDate").sort_index()
        result[code] = df
    return result


def _rows_to_ohlcv_df(rows: list[Any]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        [
            {
                "date": row["date"],
                "Open": row["open"],
                "High": row["high"],
                "Low": row["low"],
                "Close": row["close"],
                "Volume": row["volume"],
            }
            for row in rows
        ]
    )
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date").sort_index()


def _rows_to_ohlc_df(rows: list[Any]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        [
            {
                "date": row["date"],
                "Open": row["open"],
                "High": row["high"],
                "Low": row["low"],
                "Close": row["close"],
            }
            for row in rows
        ]
    )
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date").sort_index()
