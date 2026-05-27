"""Statement data query/grouping helpers for market screening."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

from src.infrastructure.external_api.dataset.statements_mixin import APIPeriodType
from src.infrastructure.data_access.loaders.statements_loaders import (
    merge_forward_forecast_revision,
    resolve_adjusted_shared_baseline_shares,
    transform_statements_df,
)
from src.infrastructure.db.market.market_reader import MarketDbQueryable
from src.infrastructure.db.market.query_helpers import normalize_stock_code, stock_code_query_candidates
from src.shared.models.types import normalize_period_type
from src.shared.utils.share_adjustment import ShareAdjustmentEvent

OPTIONAL_STATEMENT_COLUMNS = {
    "forecast_operating_profit",
    "next_year_forecast_operating_profit",
}

LEGACY_PERIOD_TYPE_MAP = {
    "1Q": "Q1",
    "2Q": "Q2",
    "3Q": "Q3",
}

STATEMENT_DB_TO_API_COLUMNS = {
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
    "forecast_operating_profit": "forecastOperatingProfit",
    "next_year_forecast_operating_profit": "nextYearForecastOperatingProfit",
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


def attach_statements(
    reader: MarketDbQueryable,
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
        base_rows = query_statements_rows(
            reader,
            codes,
            start_date=start_date,
            end_date=end_date,
            period_type=period_type,
            actual_only=True,
        )
    except Exception as e:  # noqa: BLE001 - backend error path
        if _is_missing_table_error(e):
            warnings.append("market statements table is missing; statements signals may be skipped")
            return warnings
        raise

    revision_rows: list[Any] = []
    if should_merge_forecast_revision:
        try:
            revision_rows = query_statements_rows(
                reader,
                codes,
                start_date=start_date,
                end_date=end_date,
                period_type="all",
                actual_only=False,
            )
        except Exception as e:  # noqa: BLE001 - revision is best-effort
            warnings.append(f"market statements revision load failed ({e})")

    base_map = group_statement_rows(base_rows)
    revision_map = group_statement_rows(revision_rows) if revision_rows else {}
    adjusted_metrics_map = group_adjusted_statement_metric_rows(
        query_adjusted_statement_metric_rows(
            reader,
            codes,
            start_date=start_date,
            end_date=end_date,
        )
    )
    adjustment_events_by_code = load_adjustment_events_by_code(
        reader,
        codes,
        end_date=end_date,
    )

    for code, daily_index in daily_index_by_code.items():
        base_df = base_map.get(code)
        if base_df is None or base_df.empty:
            continue

        try:
            revision_df = revision_map.get(code) if should_merge_forecast_revision else None
            shared_baseline_shares = resolve_adjusted_shared_baseline_shares(
                base_df,
                revision_df,
                adjustment_events_by_code.get(code, []),
                through_date=latest_index_date(daily_index),
            )
            adjusted_metrics_df = adjusted_metrics_map.get(code)
            base_daily = transform_statements_df(
                base_df,
                baseline_shares=shared_baseline_shares,
                adjusted_metrics_df=adjusted_metrics_df,
            ).reindex(daily_index).ffill()
            if should_merge_forecast_revision and revision_df is not None and not revision_df.empty:
                revision_daily = transform_statements_df(
                    revision_df,
                    baseline_shares=shared_baseline_shares,
                    adjusted_metrics_df=adjusted_metrics_df,
                ).reindex(daily_index).ffill()
                base_daily = merge_forward_forecast_revision(base_daily, revision_daily)
            result.setdefault(code, {})["statements_daily"] = base_daily
        except Exception as e:  # noqa: BLE001 - screening should continue
            warnings.append(f"{code} statements transform failed ({e})")

    return warnings


def latest_index_date(index: pd.DatetimeIndex) -> str | None:
    if index.empty:
        return None
    return index.max().strftime("%Y-%m-%d")


def load_adjustment_events_by_code(
    reader: MarketDbQueryable,
    stock_codes: list[str],
    *,
    end_date: str | None,
) -> dict[str, list[ShareAdjustmentEvent]]:
    query_codes = stock_code_query_candidates(stock_codes)
    if not query_codes:
        return {}
    placeholders = ",".join("?" for _ in query_codes)
    sql = f"""
        SELECT code, date, adjustment_factor
        FROM stock_data_raw
        WHERE code IN ({placeholders})
          AND adjustment_factor IS NOT NULL
          AND adjustment_factor != 1.0
    """
    params: list[Any] = list(query_codes)
    if end_date:
        sql += " AND date <= ?"
        params.append(end_date)
    sql += " ORDER BY code, date"
    try:
        rows = reader.query(sql, tuple(params))
    except Exception:  # noqa: BLE001 - stock_data_raw is unavailable in some old test DBs
        return {}
    grouped: dict[str, list[ShareAdjustmentEvent]] = {}
    for row in rows:
        code = normalize_stock_code(str(row["code"]))
        grouped.setdefault(code, []).append(
            ShareAdjustmentEvent(
                date=str(row["date"]),
                adjustment_factor=float(row["adjustment_factor"]),
            )
        )
    return grouped


def query_statements_rows(
    reader: MarketDbQueryable,
    stock_codes: list[str],
    *,
    start_date: str | None,
    end_date: str | None,
    period_type: str,
    actual_only: bool,
) -> list[Any]:
    query_codes = stock_code_query_candidates(stock_codes)
    placeholders = ",".join("?" for _ in query_codes)
    missing_optional_columns: set[str] = set()

    for _attempt in range(len(OPTIONAL_STATEMENT_COLUMNS) + 1):
        sql = build_statements_rows_sql(
            placeholders=placeholders,
            start_date=start_date,
            end_date=end_date,
            period_type=str(period_type),
            actual_only=actual_only,
            missing_optional_columns=missing_optional_columns,
        )
        params = build_statements_rows_params(
            query_codes=query_codes,
            start_date=start_date,
            end_date=end_date,
            period_type=str(period_type),
        )
        try:
            return reader.query(sql, tuple(params))
        except Exception as e:  # noqa: BLE001 - retry old schemas without optional columns
            missing_column = missing_optional_statement_column_from_error(e)
            if missing_column is None or missing_column in missing_optional_columns:
                raise
            missing_optional_columns.add(missing_column)

    raise RuntimeError("failed to query statements rows")


def build_statements_rows_sql(
    *,
    placeholders: str,
    start_date: str | None,
    end_date: str | None,
    period_type: str,
    actual_only: bool,
    missing_optional_columns: set[str],
) -> str:
    forecast_operating_profit_expr = optional_statement_select_expr(
        "forecast_operating_profit",
        missing_optional_columns,
    )
    next_year_forecast_operating_profit_expr = optional_statement_select_expr(
        "next_year_forecast_operating_profit",
        missing_optional_columns,
    )
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
            {forecast_operating_profit_expr},
            {next_year_forecast_operating_profit_expr},
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

    if start_date:
        sql += " AND disclosed_date >= ?"
    if end_date:
        sql += " AND disclosed_date <= ?"

    period_values = resolve_period_filter_values(period_type)
    if period_values:
        placeholders_period = ",".join("?" for _ in period_values)
        sql += f" AND type_of_current_period IN ({placeholders_period})"

    if actual_only:
        sql += """
            AND (
                earnings_per_share IS NOT NULL
                OR profit IS NOT NULL
                OR equity IS NOT NULL
            )
        """

    sql += " ORDER BY code, disclosed_date"
    return sql


def build_statements_rows_params(
    *,
    query_codes: Sequence[str],
    start_date: str | None,
    end_date: str | None,
    period_type: str,
) -> list[Any]:
    params: list[Any] = list(query_codes)
    if start_date:
        params.append(start_date)
    if end_date:
        params.append(end_date)
    period_values = resolve_period_filter_values(period_type)
    if period_values:
        params.extend(period_values)
    return params


def optional_statement_select_expr(column: str, missing_optional_columns: set[str]) -> str:
    if column in missing_optional_columns:
        return f"CAST(NULL AS DOUBLE) AS {column}"
    return column


def missing_optional_statement_column_from_error(exc: Exception) -> str | None:
    message = str(exc).lower()
    for column in OPTIONAL_STATEMENT_COLUMNS:
        if column in message:
            return column
    return None


def query_adjusted_statement_metric_rows(
    reader: MarketDbQueryable,
    stock_codes: list[str],
    *,
    start_date: str | None,
    end_date: str | None,
) -> list[Any]:
    query_codes = stock_code_query_candidates(stock_codes)
    if not query_codes:
        return []
    placeholders = ",".join("?" for _ in query_codes)
    sql = f"""
        SELECT
            code,
            disclosed_date,
            adjusted_eps,
            adjusted_bps,
            adjusted_forecast_eps,
            adjusted_dividend_fy
        FROM statement_metrics_adjusted
        WHERE code IN ({placeholders})
    """
    params: list[Any] = list(query_codes)
    if start_date:
        sql += " AND disclosed_date >= ?"
        params.append(start_date)
    if end_date:
        sql += " AND disclosed_date <= ?"
        params.append(end_date)
    sql += " ORDER BY code, disclosed_date"
    try:
        return reader.query(sql, tuple(params))
    except Exception as e:  # noqa: BLE001 - older DBs do not have adjusted metrics
        if _is_missing_table_error(e):
            return []
        raise


def _is_missing_table_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "no such table" in message or ("does not exist" in message and "table" in message)


def resolve_period_filter_values(period_type: str) -> list[str] | None:
    normalized = normalize_period_type(period_type)
    if normalized is None or normalized == "all":
        return None
    values = [normalized]
    legacy = LEGACY_PERIOD_TYPE_MAP.get(normalized)
    if legacy:
        values.append(legacy)
    return values


def group_statement_rows(rows: list[Any]) -> dict[str, pd.DataFrame]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        code = normalize_stock_code(str(row["code"]))
        grouped.setdefault(code, []).append({
            "disclosedDate": row["disclosed_date"],
            **{
                api_col: row.get(db_col) if hasattr(row, "get") else row[db_col]
                for db_col, api_col in STATEMENT_DB_TO_API_COLUMNS.items()
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


def group_adjusted_statement_metric_rows(rows: list[Any]) -> dict[str, pd.DataFrame]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        code = normalize_stock_code(str(row["code"]))
        grouped.setdefault(code, []).append(
            {
                "disclosedDate": row["disclosed_date"],
                "adjustedEps": row["adjusted_eps"],
                "adjustedBps": row["adjusted_bps"],
                "adjustedForecastEps": row["adjusted_forecast_eps"],
                "adjustedDividendFy": row["adjusted_dividend_fy"],
            }
        )

    result: dict[str, pd.DataFrame] = {}
    for code, records in grouped.items():
        if not records:
            continue
        df = pd.DataFrame(records)
        df["disclosedDate"] = pd.to_datetime(df["disclosedDate"])
        df = df.set_index("disclosedDate").sort_index()
        result[code] = df
    return result
