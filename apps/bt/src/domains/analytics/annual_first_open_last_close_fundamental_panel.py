"""Annual first-open to last-close fundamental panel research."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pandas as pd

from src.domains.analytics.fundamental_ranking import (
    adjust_per_share_value,
    normalize_period_label,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    _connect_duckdb as _shared_connect_duckdb,
    fetch_date_range as _fetch_date_range,
    normalize_code_sql as _normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)
from src.shared.utils.market_code_alias import expand_market_codes, normalize_market_scope
from src.shared.utils.share_adjustment import is_valid_share_count


AnnualEventStatus = Literal[
    "realized",
    "not_listed_on_entry",
    "missing_price_history",
    "missing_entry_session",
    "missing_exit_session",
    "invalid_entry_open",
    "invalid_exit_close",
    "invalid_price_path",
    "empty_holding_window",
]

ANNUAL_FIRST_OPEN_LAST_CLOSE_FUNDAMENTAL_PANEL_EXPERIMENT_ID = (
    "market-behavior/annual-first-open-last-close-fundamental-panel"
)
DEFAULT_MARKETS: tuple[str, ...] = ("prime", "standard", "growth")
DEFAULT_BUCKET_COUNT = 5
DEFAULT_ADV_WINDOW = 60
_QUARTER_PERIODS = {"1Q", "2Q", "3Q"}
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "calendar_df",
    "event_ledger_df",
    "feature_coverage_df",
    "feature_bucket_summary_df",
    "factor_spread_summary_df",
    "annual_portfolio_daily_df",
    "annual_portfolio_summary_df",
)
_MARKET_SCOPE_ORDER: tuple[str, ...] = ("all", "prime", "standard", "growth")


@dataclass(frozen=True)
class FeatureDefinition:
    name: str
    label: str
    higher_is_better: bool | None


@dataclass(frozen=True)
class ShareBaselineSnapshot:
    shares: float | None
    shares_source_date: str | None
    shares_source_period_type: str | None
    treasury_shares: float | None
    treasury_source_date: str | None
    treasury_source_period_type: str | None


@dataclass(frozen=True)
class AnnualFirstOpenLastCloseFundamentalPanelResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    selected_markets: tuple[str, ...]
    bucket_count: int
    adv_window: int
    current_market_snapshot_only: bool
    entry_timing: str
    exit_timing: str
    share_adjustment_policy: str
    calendar_df: pd.DataFrame
    event_ledger_df: pd.DataFrame
    feature_coverage_df: pd.DataFrame
    feature_bucket_summary_df: pd.DataFrame
    factor_spread_summary_df: pd.DataFrame
    annual_portfolio_daily_df: pd.DataFrame
    annual_portfolio_summary_df: pd.DataFrame


_FACTOR_DEFINITIONS: tuple[FeatureDefinition, ...] = (
    FeatureDefinition("eps", "EPS", True),
    FeatureDefinition("forward_eps", "Forward EPS", True),
    FeatureDefinition("forward_eps_to_actual_eps", "Forward EPS / actual EPS", True),
    FeatureDefinition("bps", "BPS", True),
    FeatureDefinition("per", "PER", False),
    FeatureDefinition("forward_per", "Forward PER", False),
    FeatureDefinition("pbr", "PBR", False),
    FeatureDefinition("market_cap_bil_jpy", "Market cap, bn JPY", None),
    FeatureDefinition("free_float_market_cap_bil_jpy", "Free-float market cap, bn JPY", None),
    FeatureDefinition("free_float_ratio_pct", "Free-float ratio, pct", True),
    FeatureDefinition("avg_trading_value_60d_mil_jpy", "ADV60, mn JPY", True),
    FeatureDefinition("market_cap_to_adv60", "Market cap / ADV60", False),
    FeatureDefinition("adv60_to_market_cap_pct", "ADV60 / market cap, pct", True),
    FeatureDefinition("equity_mil_jpy", "Equity, mn JPY", True),
    FeatureDefinition("total_assets_mil_jpy", "Total assets, mn JPY", None),
    FeatureDefinition("net_sales_mil_jpy", "Sales, mn JPY", True),
    FeatureDefinition("net_profit_mil_jpy", "Net profit, mn JPY", True),
    FeatureDefinition("roe_pct", "ROE, pct", True),
    FeatureDefinition("roa_pct", "ROA, pct", True),
    FeatureDefinition("operating_margin_pct", "Operating margin, pct", True),
    FeatureDefinition("net_margin_pct", "Net margin, pct", True),
    FeatureDefinition("equity_ratio_pct", "Equity ratio, pct", True),
    FeatureDefinition("cfo_margin_pct", "CFO margin, pct", True),
    FeatureDefinition("fcf_margin_pct", "FCF margin, pct", True),
    FeatureDefinition("cfo_to_net_profit_ratio", "CFO / net profit", True),
    FeatureDefinition("cfo_yield_pct", "CFO yield, pct", True),
    FeatureDefinition("fcf_yield_pct", "FCF yield, pct", True),
    FeatureDefinition("dividend_yield_pct", "Dividend yield, pct", True),
    FeatureDefinition("forecast_dividend_yield_pct", "Forecast dividend yield, pct", True),
    FeatureDefinition("payout_ratio_pct", "Payout ratio, pct", None),
    FeatureDefinition("forecast_payout_ratio_pct", "Forecast payout ratio, pct", None),
    FeatureDefinition("prior_20d_return_pct", "Prior 20D return, pct", None),
    FeatureDefinition("prior_63d_return_pct", "Prior 63D return, pct", None),
    FeatureDefinition("prior_252d_return_pct", "Prior 252D return, pct", None),
    FeatureDefinition("pre_entry_volatility_20d_pct", "Pre-entry volatility 20D, pct", False),
)
_FACTOR_BY_NAME = {definition.name: definition for definition in _FACTOR_DEFINITIONS}
_FEATURE_COLUMNS: tuple[str, ...] = tuple(definition.name for definition in _FACTOR_DEFINITIONS)


def _connect_duckdb(db_path: str, *, read_only: bool = True) -> Any:
    return _shared_connect_duckdb(db_path, read_only=read_only)


def _open_analysis_connection(db_path: str):
    return open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="annual-first-open-last-close-",
        connect_fn=_connect_duckdb,
    )


def _placeholder_sql(size: int) -> str:
    if size <= 0:
        raise ValueError("placeholder size must be positive")
    return ",".join("?" for _ in range(size))


def _values_sql(row_count: int, column_count: int) -> str:
    if row_count <= 0 or column_count <= 0:
        raise ValueError("row_count and column_count must be positive")
    row_sql = f"({', '.join('?' for _ in range(column_count))})"
    return ", ".join(row_sql for _ in range(row_count))


def _empty_result_df(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _normalize_selected_markets(markets: Sequence[str]) -> tuple[str, ...]:
    if not markets:
        raise ValueError("markets must not be empty")
    normalized: list[str] = []
    seen: set[str] = set()
    for market in markets:
        canonical = normalize_market_scope(market)
        if canonical is None:
            raise ValueError(f"Unsupported market: {market}")
        if canonical in seen:
            continue
        normalized.append(canonical)
        seen.add(canonical)
    return tuple(normalized)


def _market_query_codes(markets: Sequence[str]) -> tuple[str, ...]:
    return tuple(expand_market_codes([str(market) for market in markets]))


def _validate_bucket_count(bucket_count: int) -> int:
    value = int(bucket_count)
    if value < 2:
        raise ValueError("bucket_count must be >= 2")
    return value


def _validate_adv_window(adv_window: int) -> int:
    value = int(adv_window)
    if value < 1:
        raise ValueError("adv_window must be >= 1")
    return value


def _to_nullable_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(coerced):
        return None
    return coerced


def _is_valid_non_negative(value: float | int | None) -> bool:
    if value is None:
        return False
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and number >= 0.0


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None:
        return None
    if not (math.isfinite(numerator) and math.isfinite(denominator)):
        return None
    if math.isclose(denominator, 0.0, abs_tol=1e-12):
        return None
    value = numerator / denominator
    return value if math.isfinite(value) else None


def _ratio_pct(numerator: float | None, denominator: float | None) -> float | None:
    value = _ratio(numerator, denominator)
    return value * 100.0 if value is not None else None


def _normalize_payout_ratio(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return value * 100.0 if abs(value) <= 1.0 else value


def _market_label(value: Any) -> str:
    return str(normalize_market_scope(value, default=str(value).lower()))


def _table_exists(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _query_trading_calendar(
    conn: Any,
    *,
    start_year: int | None,
    end_year: int | None,
    include_incomplete_last_year: bool,
) -> pd.DataFrame:
    conditions: list[str] = []
    params: list[Any] = []
    if start_year is not None:
        conditions.append("date >= ?")
        params.append(f"{int(start_year):04d}-01-01")
    if end_year is not None:
        conditions.append("date <= ?")
        params.append(f"{int(end_year):04d}-12-31")
    where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    df = conn.execute(
        f"""
        SELECT
            substr(date, 1, 4) AS year,
            MIN(date) AS entry_date,
            MAX(date) AS exit_date,
            COUNT(DISTINCT date) AS market_trading_days
        FROM stock_data
        {where_sql}
        GROUP BY year
        ORDER BY year
        """,
        params,
    ).fetchdf()
    if df.empty:
        return _empty_result_df(["year", "entry_date", "exit_date", "market_trading_days"])
    df["year"] = df["year"].astype(str)
    df["entry_date"] = df["entry_date"].astype(str)
    df["exit_date"] = df["exit_date"].astype(str)
    df["entry_mmdd"] = df["entry_date"].str[5:]
    df["exit_mmdd"] = df["exit_date"].str[5:]
    if not include_incomplete_last_year:
        df = df[(df["entry_mmdd"] <= "01-15") & (df["exit_mmdd"] >= "12-15")].copy()
    df = df[df["entry_date"] < df["exit_date"]].copy()
    return df[["year", "entry_date", "exit_date", "market_trading_days"]].reset_index(drop=True)


def _stock_master_columns() -> list[str]:
    return [
        "year",
        "entry_date",
        "exit_date",
        "market_trading_days",
        "code",
        "company_name",
        "market_code",
        "market_name",
        "sector_33_name",
        "scale_category",
        "listed_date",
        "normalized_code",
        "market",
    ]


def _query_canonical_stocks(conn: Any, *, market_codes: Sequence[str]) -> pd.DataFrame:
    placeholders = _placeholder_sql(len(market_codes))
    normalized_code = _normalize_code_sql("code")
    prefer_4digit = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"
    df = conn.execute(
        f"""
        SELECT
            code,
            company_name,
            market_code,
            market_name,
            sector_33_name,
            scale_category,
            listed_date,
            normalized_code
        FROM (
            SELECT
                code,
                company_name,
                market_code,
                market_name,
                sector_33_name,
                scale_category,
                listed_date,
                {normalized_code} AS normalized_code,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code}
                    ORDER BY {prefer_4digit}
                ) AS rn
            FROM stocks
            WHERE lower(market_code) IN ({placeholders})
        )
        WHERE rn = 1
        ORDER BY code
        """,
        [str(code).lower() for code in market_codes],
    ).fetchdf()
    if df.empty:
        return _empty_result_df(
            [
                "code",
                "company_name",
                "market_code",
                "market_name",
                "sector_33_name",
                "scale_category",
                "listed_date",
                "normalized_code",
                "market",
            ]
        )
    df["code"] = df["code"].astype(str)
    df["normalized_code"] = df["normalized_code"].astype(str)
    df["market_code"] = df["market_code"].astype(str)
    df["listed_date"] = df["listed_date"].fillna("").astype(str)
    df["market"] = df["market_code"].map(_market_label)
    return df


def _query_entry_stock_master(
    conn: Any,
    *,
    calendar_df: pd.DataFrame,
    market_codes: Sequence[str],
) -> tuple[pd.DataFrame, bool]:
    columns = _stock_master_columns()
    if calendar_df.empty:
        return _empty_result_df(columns), False
    if not _table_exists(conn, "stock_master_daily"):
        stock_df = _query_canonical_stocks(conn, market_codes=market_codes)
        if stock_df.empty:
            return _empty_result_df(columns), False
        records: list[dict[str, Any]] = []
        for calendar in calendar_df.to_dict(orient="records"):
            for stock in stock_df.to_dict(orient="records"):
                record: dict[str, Any] = {
                    "year": str(calendar["year"]),
                    "entry_date": str(calendar["entry_date"]),
                    "exit_date": str(calendar["exit_date"]),
                    "market_trading_days": int(calendar["market_trading_days"]),
                }
                record.update(cast(dict[str, Any], stock))
                records.append(record)
        return pd.DataFrame(records, columns=columns), False

    calendar_records = calendar_df.to_dict(orient="records")
    calendar_values_sql = _values_sql(len(calendar_records), 4)
    calendar_params: list[Any] = []
    for row in calendar_records:
        calendar_params.extend(
            [
                str(row["year"]),
                str(row["entry_date"]),
                str(row["exit_date"]),
                int(row["market_trading_days"]),
            ]
        )
    market_placeholders = _placeholder_sql(len(market_codes))
    normalized_code = _normalize_code_sql("code")
    prefer_4digit = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"
    df = conn.execute(
        f"""
        WITH calendar(year, entry_date, exit_date, market_trading_days) AS (
            VALUES {calendar_values_sql}
        ),
        entry_master AS (
            SELECT
                c.year,
                c.entry_date,
                c.exit_date,
                c.market_trading_days,
                m.code,
                m.company_name,
                m.market_code,
                m.market_name,
                m.sector_33_name,
                m.scale_category,
                m.listed_date,
                {normalized_code} AS normalized_code,
                ROW_NUMBER() OVER (
                    PARTITION BY c.entry_date, {normalized_code}
                    ORDER BY {prefer_4digit}
                ) AS rn
            FROM calendar c
            JOIN stock_master_daily m
              ON m.date = c.entry_date
            WHERE lower(m.market_code) IN ({market_placeholders})
        )
        SELECT
            year,
            entry_date,
            exit_date,
            market_trading_days,
            code,
            company_name,
            market_code,
            market_name,
            sector_33_name,
            scale_category,
            listed_date,
            normalized_code
        FROM entry_master
        WHERE rn = 1
        ORDER BY year, market_code, code
        """,
        [*calendar_params, *[str(code).lower() for code in market_codes]],
    ).fetchdf()
    if df.empty:
        return _empty_result_df(columns), True
    df["year"] = df["year"].astype(str)
    df["entry_date"] = df["entry_date"].astype(str)
    df["exit_date"] = df["exit_date"].astype(str)
    df["code"] = df["code"].astype(str)
    df["normalized_code"] = df["normalized_code"].astype(str)
    df["market_code"] = df["market_code"].astype(str)
    df["listed_date"] = df["listed_date"].fillna("").astype(str)
    df["market"] = df["market_code"].map(_market_label)
    return df[columns], True


def _query_statement_rows(conn: Any, *, codes: Sequence[str]) -> pd.DataFrame:
    if not codes:
        return _empty_result_df(
            [
                "code",
                "disclosed_date",
                "type_of_current_period",
                "period_type",
                "earnings_per_share",
                "bps",
                "forecast_eps",
                "next_year_forecast_earnings_per_share",
                "profit",
                "equity",
                "total_assets",
                "sales",
                "operating_profit",
                "operating_cash_flow",
                "investing_cash_flow",
                "dividend_fy",
                "forecast_dividend_fy",
                "next_year_forecast_dividend_fy",
                "payout_ratio",
                "forecast_payout_ratio",
                "next_year_forecast_payout_ratio",
                "shares_outstanding",
                "treasury_shares",
            ]
        )
    selected_values_sql = _values_sql(len(codes), 1)
    normalized_code = _normalize_code_sql("code")
    statement_normalized_code = _normalize_code_sql("st.code")
    prefer_4digit = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"
    statement_prefer_4digit = "CASE WHEN length(st.code) = 4 THEN 0 ELSE 1 END"
    df = conn.execute(
        f"""
        WITH selected_codes(code) AS (
            VALUES {selected_values_sql}
        ),
        stocks_canonical AS (
            SELECT code, normalized_code
            FROM (
                SELECT
                    code,
                    {normalized_code} AS normalized_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized_code}
                        ORDER BY {prefer_4digit}
                    ) AS rn
                FROM selected_codes
            )
            WHERE rn = 1
        ),
        selected_normalized_codes AS (
            SELECT DISTINCT normalized_code
            FROM stocks_canonical
        ),
        statements_canonical AS (
            SELECT
                normalized_code,
                disclosed_date,
                type_of_current_period,
                earnings_per_share,
                bps,
                forecast_eps,
                next_year_forecast_earnings_per_share,
                profit,
                equity,
                total_assets,
                sales,
                operating_profit,
                operating_cash_flow,
                investing_cash_flow,
                dividend_fy,
                forecast_dividend_fy,
                next_year_forecast_dividend_fy,
                payout_ratio,
                forecast_payout_ratio,
                next_year_forecast_payout_ratio,
                shares_outstanding,
                treasury_shares
            FROM (
                SELECT
                    {statement_normalized_code} AS normalized_code,
                    st.disclosed_date,
                    st.type_of_current_period,
                    st.earnings_per_share,
                    st.bps,
                    st.forecast_eps,
                    st.next_year_forecast_earnings_per_share,
                    st.profit,
                    st.equity,
                    st.total_assets,
                    st.sales,
                    st.operating_profit,
                    st.operating_cash_flow,
                    st.investing_cash_flow,
                    st.dividend_fy,
                    st.forecast_dividend_fy,
                    st.next_year_forecast_dividend_fy,
                    st.payout_ratio,
                    st.forecast_payout_ratio,
                    st.next_year_forecast_payout_ratio,
                    st.shares_outstanding,
                    st.treasury_shares,
                    ROW_NUMBER() OVER (
                        PARTITION BY {statement_normalized_code}, st.disclosed_date
                        ORDER BY {statement_prefer_4digit}
                    ) AS rn
                FROM statements st
                JOIN selected_normalized_codes selected
                  ON selected.normalized_code = {statement_normalized_code}
            )
            WHERE rn = 1
        )
        SELECT
            s.code,
            st.*
        FROM statements_canonical st
        JOIN stocks_canonical s
          ON s.normalized_code = st.normalized_code
        ORDER BY s.code, st.disclosed_date
        """,
        [str(code) for code in codes],
    ).fetchdf()
    if df.empty:
        return _empty_result_df(
            [
                "code",
                "disclosed_date",
                "type_of_current_period",
                "period_type",
                "earnings_per_share",
                "bps",
                "forecast_eps",
                "next_year_forecast_earnings_per_share",
                "profit",
                "equity",
                "total_assets",
                "sales",
                "operating_profit",
                "operating_cash_flow",
                "investing_cash_flow",
                "dividend_fy",
                "forecast_dividend_fy",
                "next_year_forecast_dividend_fy",
                "payout_ratio",
                "forecast_payout_ratio",
                "next_year_forecast_payout_ratio",
                "shares_outstanding",
                "treasury_shares",
            ]
        )
    df["code"] = df["code"].astype(str)
    df["disclosed_date"] = df["disclosed_date"].astype(str)
    df["period_type"] = df["type_of_current_period"].map(normalize_period_label)
    return df.sort_values(["code", "disclosed_date"], kind="stable").reset_index(drop=True)


def _query_price_rows(
    conn: Any,
    *,
    codes: Sequence[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    if not codes:
        return _empty_result_df(["code", "date", "open", "high", "low", "close", "volume"])
    selected_values_sql = _values_sql(len(codes), 1)
    normalized_code = _normalize_code_sql("code")
    price_normalized_code = _normalize_code_sql("sd.code")
    prefer_4digit = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"
    price_prefer_4digit = "CASE WHEN length(sd.code) = 4 THEN 0 ELSE 1 END"
    df = conn.execute(
        f"""
        WITH selected_codes(code) AS (
            VALUES {selected_values_sql}
        ),
        stocks_canonical AS (
            SELECT code, normalized_code
            FROM (
                SELECT
                    code,
                    {normalized_code} AS normalized_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized_code}
                        ORDER BY {prefer_4digit}
                    ) AS rn
                FROM selected_codes
            )
            WHERE rn = 1
        ),
        selected_normalized_codes AS (
            SELECT DISTINCT normalized_code
            FROM stocks_canonical
        ),
        stock_data_canonical AS (
            SELECT
                normalized_code,
                date,
                open,
                high,
                low,
                close,
                volume
            FROM (
                SELECT
                    {price_normalized_code} AS normalized_code,
                    sd.date,
                    sd.open,
                    sd.high,
                    sd.low,
                    sd.close,
                    sd.volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY {price_normalized_code}, sd.date
                        ORDER BY {price_prefer_4digit}
                    ) AS rn
                FROM stock_data sd
                JOIN selected_normalized_codes selected
                  ON selected.normalized_code = {price_normalized_code}
                WHERE sd.date >= ? AND sd.date <= ?
            )
            WHERE rn = 1
        )
        SELECT
            s.code,
            sd.date,
            sd.open,
            sd.high,
            sd.low,
            sd.close,
            sd.volume
        FROM stock_data_canonical sd
        JOIN stocks_canonical s
          ON s.normalized_code = sd.normalized_code
        ORDER BY s.code, sd.date
        """,
        [*[str(code) for code in codes], start_date, end_date],
    ).fetchdf()
    if df.empty:
        return _empty_result_df(["code", "date", "open", "high", "low", "close", "volume"])
    df["code"] = df["code"].astype(str)
    df["date"] = df["date"].astype(str)
    return df


def _resolve_baseline_share_snapshot(
    statement_frame: pd.DataFrame,
    *,
    as_of_date: str,
) -> ShareBaselineSnapshot:
    if statement_frame.empty:
        return ShareBaselineSnapshot(None, None, None, None, None, None)
    eligible = statement_frame[statement_frame["disclosed_date"].astype(str) <= str(as_of_date)].copy()
    if eligible.empty:
        return ShareBaselineSnapshot(None, None, None, None, None, None)
    eligible = eligible.sort_values("disclosed_date", kind="stable")

    share_row: pd.Series | None = None
    share_candidates = eligible[eligible["shares_outstanding"].map(_to_nullable_float).map(is_valid_share_count)]
    if not share_candidates.empty:
        quarterly = share_candidates[share_candidates["period_type"].isin(_QUARTER_PERIODS)]
        share_row = (quarterly if not quarterly.empty else share_candidates).iloc[-1]

    treasury_row: pd.Series | None = None
    treasury_candidates = eligible[
        eligible["treasury_shares"].map(_to_nullable_float).map(_is_valid_non_negative)
    ]
    if not treasury_candidates.empty:
        quarterly_treasury = treasury_candidates[treasury_candidates["period_type"].isin(_QUARTER_PERIODS)]
        treasury_row = (quarterly_treasury if not quarterly_treasury.empty else treasury_candidates).iloc[-1]

    return ShareBaselineSnapshot(
        shares=_to_nullable_float(share_row["shares_outstanding"]) if share_row is not None else None,
        shares_source_date=str(share_row["disclosed_date"]) if share_row is not None else None,
        shares_source_period_type=str(share_row["period_type"]) if share_row is not None else None,
        treasury_shares=(
            _to_nullable_float(treasury_row["treasury_shares"]) if treasury_row is not None else None
        ),
        treasury_source_date=str(treasury_row["disclosed_date"]) if treasury_row is not None else None,
        treasury_source_period_type=str(treasury_row["period_type"]) if treasury_row is not None else None,
    )


def _latest_fy_statement(statement_frame: pd.DataFrame, *, as_of_date: str) -> pd.Series | None:
    if statement_frame.empty:
        return None
    eligible = statement_frame[
        (statement_frame["period_type"] == "FY")
        & (statement_frame["disclosed_date"].astype(str) <= str(as_of_date))
    ].copy()
    if eligible.empty:
        return None
    return eligible.sort_values("disclosed_date", kind="stable").iloc[-1]


def _resolve_forward_eps(
    statement_frame: pd.DataFrame,
    *,
    latest_fy: pd.Series,
    baseline_shares: float | None,
    as_of_date: str,
) -> tuple[float | None, str | None, str | None, str | None]:
    fy_disclosed_date = str(latest_fy["disclosed_date"])
    eligible = statement_frame[
        (statement_frame["disclosed_date"].astype(str) <= str(as_of_date))
        & (statement_frame["disclosed_date"].astype(str) > fy_disclosed_date)
        & (statement_frame["period_type"].isin(_QUARTER_PERIODS))
    ].copy()
    if not eligible.empty:
        eligible = eligible.sort_values("disclosed_date", kind="stable")
        for row in reversed(list(eligible.to_dict(orient="records"))):
            raw_revised = _to_nullable_float(row.get("forecast_eps"))
            if raw_revised is None:
                raw_revised = _to_nullable_float(row.get("next_year_forecast_earnings_per_share"))
            adjusted = adjust_per_share_value(
                raw_revised,
                _to_nullable_float(row.get("shares_outstanding")),
                baseline_shares,
            )
            if adjusted is not None:
                return adjusted, "revised", str(row["disclosed_date"]), str(row["period_type"])

    raw_fy_forecast = _to_nullable_float(latest_fy["next_year_forecast_earnings_per_share"])
    if raw_fy_forecast is None:
        raw_fy_forecast = _to_nullable_float(latest_fy["forecast_eps"])
    adjusted_fy = adjust_per_share_value(
        raw_fy_forecast,
        _to_nullable_float(latest_fy["shares_outstanding"]),
        baseline_shares,
    )
    return adjusted_fy, "fy" if adjusted_fy is not None else None, fy_disclosed_date, "FY"


def _compute_prior_return_pct(
    price_frame: pd.DataFrame,
    *,
    entry_idx: int,
    sessions: int,
) -> float | None:
    start_idx = entry_idx - sessions
    if start_idx < 0:
        return None
    start_close = _to_nullable_float(price_frame.iloc[start_idx]["close"])
    prior_close = _to_nullable_float(price_frame.iloc[entry_idx - 1]["close"]) if entry_idx > 0 else None
    if start_close is None or prior_close is None or start_close <= 0:
        return None
    return (prior_close / start_close - 1.0) * 100.0


def _compute_pre_entry_volatility_pct(
    price_frame: pd.DataFrame,
    *,
    entry_idx: int,
    sessions: int = 20,
) -> float | None:
    start_idx = entry_idx - sessions
    if start_idx < 0:
        return None
    close = pd.to_numeric(price_frame.iloc[start_idx:entry_idx]["close"], errors="coerce")
    returns = close.pct_change().dropna()
    if len(returns) < 2:
        return None
    return float(returns.std(ddof=1) * math.sqrt(252.0) * 100.0)


def _compute_adv(
    price_frame: pd.DataFrame,
    *,
    entry_idx: int,
    adv_window: int,
) -> tuple[float | None, int]:
    start_idx = max(0, entry_idx - adv_window)
    prior = price_frame.iloc[start_idx:entry_idx].copy()
    sessions = int(len(prior))
    if sessions < adv_window:
        return None, sessions
    close = pd.to_numeric(prior["close"], errors="coerce")
    volume = pd.to_numeric(prior["volume"], errors="coerce")
    trading_value = (close * volume).dropna()
    if len(trading_value) < adv_window:
        return None, sessions
    value = float(trading_value.tail(adv_window).mean())
    return (value if math.isfinite(value) and value > 0 else None), sessions


def _calc_path_metrics(entry_open: float, path_df: pd.DataFrame) -> dict[str, float | None]:
    close_values = pd.to_numeric(path_df["close"], errors="coerce").astype(float).to_numpy()
    if len(close_values) == 0 or not np.isfinite(close_values).all():
        return {}
    previous_close = np.concatenate(([entry_open], close_values[:-1]))
    daily_returns = close_values / previous_close - 1.0
    equity_curve = close_values / entry_open
    peaks = np.maximum.accumulate(np.concatenate(([1.0], equity_curve)))
    curve_with_initial = np.concatenate(([1.0], equity_curve))
    drawdowns = curve_with_initial / peaks - 1.0
    max_drawdown = float(np.min(drawdowns))
    total_return = float(equity_curve[-1] - 1.0)
    start_date = str(path_df.iloc[0]["date"])
    end_date = str(path_df.iloc[-1]["date"])
    period_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days
    cagr = None
    if period_days > 0 and total_return > -1.0:
        cagr_value = (1.0 + total_return) ** (365.25 / period_days) - 1.0
        cagr = float(cagr_value) if math.isfinite(cagr_value) else None
    return {
        "event_return": total_return,
        "event_return_pct": total_return * 100.0,
        "max_drawdown_pct": max_drawdown * 100.0,
        "max_runup_pct": (float(np.max(curve_with_initial)) - 1.0) * 100.0,
        "annualized_volatility_pct": _annualized_volatility_pct(pd.Series(daily_returns)),
        "sharpe_ratio": _annualized_sharpe(pd.Series(daily_returns)),
        "sortino_ratio": _annualized_sortino(pd.Series(daily_returns)),
        "cagr_pct": cagr * 100.0 if cagr is not None else None,
        "calmar_ratio": (
            cagr / abs(max_drawdown)
            if cagr is not None and max_drawdown < -1e-12
            else None
        ),
    }


def _build_feature_values(
    *,
    statement_frame: pd.DataFrame,
    latest_fy: pd.Series | None,
    baseline: ShareBaselineSnapshot,
    as_of_date: str,
    entry_open: float | None,
    entry_previous_close: float | None,
    price_frame: pd.DataFrame,
    entry_idx: int | None,
    adv_window: int,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "has_fy_as_of_entry": latest_fy is not None,
        "latest_fy_disclosed_date": None,
        "latest_fy_period_end": None,
        "baseline_shares": baseline.shares,
        "baseline_shares_source_date": baseline.shares_source_date,
        "baseline_shares_source_period_type": baseline.shares_source_period_type,
        "baseline_treasury_shares": baseline.treasury_shares,
        "baseline_treasury_source_date": baseline.treasury_source_date,
        "baseline_treasury_source_period_type": baseline.treasury_source_period_type,
        "fy_shares_outstanding": None,
        "share_adjustment_ratio": None,
        "share_adjustment_applied": False,
        "forward_eps_source": None,
        "forward_eps_disclosed_date": None,
        "forward_eps_period_type": None,
        "avg_trading_value_60d_source_sessions": 0,
    }
    for column in _FEATURE_COLUMNS:
        record[column] = None
    if latest_fy is None:
        return record

    fy_shares = _to_nullable_float(latest_fy["shares_outstanding"])
    share_adjustment_ratio = (
        fy_shares / baseline.shares
        if fy_shares is not None
        and baseline.shares is not None
        and is_valid_share_count(fy_shares)
        and is_valid_share_count(baseline.shares)
        else None
    )
    eps = adjust_per_share_value(
        _to_nullable_float(latest_fy["earnings_per_share"]),
        fy_shares,
        baseline.shares,
    )
    bps = adjust_per_share_value(_to_nullable_float(latest_fy["bps"]), fy_shares, baseline.shares)
    forward_eps, forward_eps_source, forward_eps_date, forward_eps_period = _resolve_forward_eps(
        statement_frame,
        latest_fy=latest_fy,
        baseline_shares=baseline.shares,
        as_of_date=as_of_date,
    )

    entry_price = entry_open
    previous_price = entry_previous_close
    market_cap = (
        entry_price * baseline.shares
        if entry_price is not None
        and baseline.shares is not None
        and entry_price > 0
        and baseline.shares > 0
        else None
    )
    free_float_shares = None
    if baseline.shares is not None and baseline.shares > 0:
        treasury = baseline.treasury_shares or 0.0
        if baseline.shares - treasury > 0:
            free_float_shares = baseline.shares - treasury
    free_float_market_cap = (
        entry_price * free_float_shares
        if entry_price is not None and free_float_shares is not None and entry_price > 0
        else None
    )
    adv, adv_sessions = (
        _compute_adv(price_frame, entry_idx=entry_idx, adv_window=adv_window)
        if entry_idx is not None
        else (None, 0)
    )

    profit = _to_nullable_float(latest_fy["profit"])
    equity = _to_nullable_float(latest_fy["equity"])
    total_assets = _to_nullable_float(latest_fy["total_assets"])
    sales = _to_nullable_float(latest_fy["sales"])
    operating_profit = _to_nullable_float(latest_fy["operating_profit"])
    cfo = _to_nullable_float(latest_fy["operating_cash_flow"])
    cfi = _to_nullable_float(latest_fy["investing_cash_flow"])
    fcf = cfo + cfi if cfo is not None and cfi is not None else None
    dividend_fy = adjust_per_share_value(
        _to_nullable_float(latest_fy["dividend_fy"]),
        fy_shares,
        baseline.shares,
    )
    raw_forecast_dividend = _to_nullable_float(latest_fy["next_year_forecast_dividend_fy"])
    if raw_forecast_dividend is None:
        raw_forecast_dividend = _to_nullable_float(latest_fy["forecast_dividend_fy"])
    forecast_dividend_fy = adjust_per_share_value(raw_forecast_dividend, fy_shares, baseline.shares)
    raw_forecast_payout_ratio = _to_nullable_float(latest_fy["next_year_forecast_payout_ratio"])
    if raw_forecast_payout_ratio is None:
        raw_forecast_payout_ratio = _to_nullable_float(latest_fy["forecast_payout_ratio"])

    record.update(
        {
            "latest_fy_disclosed_date": str(latest_fy["disclosed_date"]),
            "latest_fy_period_end": None,
            "fy_shares_outstanding": fy_shares,
            "share_adjustment_ratio": share_adjustment_ratio,
            "share_adjustment_applied": (
                share_adjustment_ratio is not None
                and not math.isclose(share_adjustment_ratio, 1.0, rel_tol=1e-9, abs_tol=1e-12)
            ),
            "eps": eps,
            "forward_eps": forward_eps,
            "forward_eps_to_actual_eps": _ratio(forward_eps, eps),
            "forward_eps_source": forward_eps_source,
            "forward_eps_disclosed_date": forward_eps_date,
            "forward_eps_period_type": forward_eps_period,
            "bps": bps,
            "per": _ratio(entry_price, eps),
            "forward_per": _ratio(entry_price, forward_eps),
            "pbr": _ratio(entry_price, bps),
            "preopen_per_prev_close": _ratio(previous_price, eps),
            "preopen_forward_per_prev_close": _ratio(previous_price, forward_eps),
            "preopen_pbr_prev_close": _ratio(previous_price, bps),
            "market_cap_bil_jpy": market_cap / 1_000_000_000.0 if market_cap is not None else None,
            "free_float_market_cap_bil_jpy": (
                free_float_market_cap / 1_000_000_000.0
                if free_float_market_cap is not None
                else None
            ),
            "free_float_ratio_pct": (
                free_float_shares / baseline.shares * 100.0
                if free_float_shares is not None and baseline.shares is not None
                else None
            ),
            "avg_trading_value_60d_mil_jpy": adv / 1_000_000.0 if adv is not None else None,
            "avg_trading_value_60d_source_sessions": adv_sessions,
            "market_cap_to_adv60": _ratio(market_cap, adv),
            "adv60_to_market_cap_pct": _ratio_pct(adv, market_cap),
            "equity_mil_jpy": equity,
            "total_assets_mil_jpy": total_assets,
            "net_sales_mil_jpy": sales,
            "net_profit_mil_jpy": profit,
            "operating_profit_mil_jpy": operating_profit,
            "operating_cash_flow_mil_jpy": cfo,
            "simple_fcf_mil_jpy": fcf,
            "roe_pct": _ratio_pct(profit, equity),
            "roa_pct": _ratio_pct(profit, total_assets),
            "operating_margin_pct": _ratio_pct(operating_profit, sales),
            "net_margin_pct": _ratio_pct(profit, sales),
            "equity_ratio_pct": _ratio_pct(equity, total_assets),
            "cfo_margin_pct": _ratio_pct(cfo, sales),
            "fcf_margin_pct": _ratio_pct(fcf, sales),
            "cfo_to_net_profit_ratio": _ratio(cfo, profit),
            "cfo_yield_pct": _ratio_pct(cfo, market_cap),
            "fcf_yield_pct": _ratio_pct(fcf, market_cap),
            "dividend_yield_pct": _ratio_pct(dividend_fy, entry_price),
            "forecast_dividend_yield_pct": _ratio_pct(forecast_dividend_fy, entry_price),
            "payout_ratio_pct": _normalize_payout_ratio(_to_nullable_float(latest_fy["payout_ratio"])),
            "forecast_payout_ratio_pct": _normalize_payout_ratio(raw_forecast_payout_ratio),
            "prior_20d_return_pct": (
                _compute_prior_return_pct(price_frame, entry_idx=entry_idx, sessions=20)
                if entry_idx is not None
                else None
            ),
            "prior_63d_return_pct": (
                _compute_prior_return_pct(price_frame, entry_idx=entry_idx, sessions=63)
                if entry_idx is not None
                else None
            ),
            "prior_252d_return_pct": (
                _compute_prior_return_pct(price_frame, entry_idx=entry_idx, sessions=252)
                if entry_idx is not None
                else None
            ),
            "pre_entry_volatility_20d_pct": (
                _compute_pre_entry_volatility_pct(price_frame, entry_idx=entry_idx, sessions=20)
                if entry_idx is not None
                else None
            ),
        }
    )
    return record


def _build_event_ledger(
    *,
    calendar_df: pd.DataFrame,
    stock_df: pd.DataFrame,
    statement_df: pd.DataFrame,
    price_df: pd.DataFrame,
    adv_window: int,
) -> pd.DataFrame:
    columns = [
        "event_id",
        "year",
        "code",
        "company_name",
        "market",
        "market_code",
        "sector_33_name",
        "scale_category",
        "listed_date",
        "status",
        "entry_date",
        "exit_date",
        "entry_open",
        "entry_close",
        "entry_previous_close",
        "exit_close",
        "holding_trading_days",
        "holding_calendar_days",
        "has_fy_as_of_entry",
        "latest_fy_disclosed_date",
        "latest_fy_period_end",
        "baseline_shares",
        "baseline_shares_source_date",
        "baseline_shares_source_period_type",
        "baseline_treasury_shares",
        "baseline_treasury_source_date",
        "baseline_treasury_source_period_type",
        "fy_shares_outstanding",
        "share_adjustment_ratio",
        "share_adjustment_applied",
        "forward_eps_source",
        "forward_eps_disclosed_date",
        "forward_eps_period_type",
        "preopen_per_prev_close",
        "preopen_forward_per_prev_close",
        "preopen_pbr_prev_close",
        "avg_trading_value_60d_source_sessions",
        "operating_profit_mil_jpy",
        "operating_cash_flow_mil_jpy",
        "simple_fcf_mil_jpy",
        "event_return",
        "event_return_pct",
        "cagr_pct",
        "max_drawdown_pct",
        "max_runup_pct",
        "annualized_volatility_pct",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
        *_FEATURE_COLUMNS,
    ]
    if calendar_df.empty or stock_df.empty:
        return _empty_result_df(columns)

    price_by_code = {
        str(code): frame.sort_values("date", kind="stable").reset_index(drop=True)
        for code, frame in price_df.groupby("code", sort=False)
    }
    statements_by_code = {
        str(code): frame.sort_values("disclosed_date", kind="stable").reset_index(drop=True)
        for code, frame in statement_df.groupby("code", sort=False)
    }
    records: list[dict[str, Any]] = []

    for stock in stock_df.to_dict(orient="records"):
        code = str(stock["code"])
        price_frame = price_by_code.get(code)
        if price_frame is None or price_frame.empty:
            continue
        statement_frame = statements_by_code.get(code, _empty_result_df(list(statement_df.columns)))
        price_dates = price_frame["date"].astype(str).to_numpy()
        listed_date = str(stock.get("listed_date") or "")

        year = str(stock["year"])
        entry_date = str(stock["entry_date"])
        exit_date = str(stock["exit_date"])
        event_id = f"{code}:{year}"
        record: dict[str, Any] = {
            "event_id": event_id,
            "year": year,
            "code": code,
            "company_name": str(stock.get("company_name") or ""),
            "market": str(stock.get("market") or ""),
            "market_code": str(stock.get("market_code") or ""),
            "sector_33_name": str(stock.get("sector_33_name") or ""),
            "scale_category": str(stock.get("scale_category") or ""),
            "listed_date": listed_date,
            "status": None,
            "entry_date": entry_date,
            "exit_date": exit_date,
            "entry_open": None,
            "entry_close": None,
            "entry_previous_close": None,
            "exit_close": None,
            "holding_trading_days": None,
            "holding_calendar_days": None,
            "event_return": None,
            "event_return_pct": None,
            "cagr_pct": None,
            "max_drawdown_pct": None,
            "max_runup_pct": None,
            "annualized_volatility_pct": None,
            "sharpe_ratio": None,
            "sortino_ratio": None,
            "calmar_ratio": None,
        }
        entry_positions = np.where(price_dates == entry_date)[0]
        exit_positions = np.where(price_dates == exit_date)[0]
        if len(entry_positions) == 0:
            record["status"] = "missing_entry_session"
            records.append(record)
            continue
        if len(exit_positions) == 0:
            record["status"] = "missing_exit_session"
            records.append(record)
            continue

        entry_idx = int(entry_positions[0])
        exit_idx = int(exit_positions[0])
        if entry_idx > exit_idx:
            record["status"] = "empty_holding_window"
            records.append(record)
            continue
        path_df = price_frame.iloc[entry_idx : exit_idx + 1].copy().reset_index(drop=True)
        if path_df.empty:
            record["status"] = "empty_holding_window"
            records.append(record)
            continue
        entry_open = _to_nullable_float(path_df.iloc[0]["open"])
        entry_close = _to_nullable_float(path_df.iloc[0]["close"])
        entry_previous_close = (
            _to_nullable_float(price_frame.iloc[entry_idx - 1]["close"]) if entry_idx > 0 else None
        )
        exit_close = _to_nullable_float(path_df.iloc[-1]["close"])
        if entry_open is None or entry_open <= 0:
            record["status"] = "invalid_entry_open"
            records.append(record)
            continue
        if exit_close is None or exit_close <= 0:
            record["status"] = "invalid_exit_close"
            records.append(record)
            continue
        if pd.to_numeric(path_df["close"], errors="coerce").isna().any():
            record["status"] = "invalid_price_path"
            records.append(record)
            continue

        statement_frame_for_entry = statement_frame.copy()
        statement_frame_for_entry.attrs["as_of_date"] = entry_date
        baseline = _resolve_baseline_share_snapshot(
            statement_frame_for_entry,
            as_of_date=entry_date,
        )
        latest_fy = _latest_fy_statement(statement_frame_for_entry, as_of_date=entry_date)
        feature_values = _build_feature_values(
            statement_frame=statement_frame_for_entry,
            latest_fy=latest_fy,
            baseline=baseline,
            as_of_date=entry_date,
            entry_open=entry_open,
            entry_previous_close=entry_previous_close,
            price_frame=price_frame,
            entry_idx=entry_idx,
            adv_window=adv_window,
        )
        metrics = _calc_path_metrics(entry_open, path_df)
        holding_calendar_days = (pd.Timestamp(exit_date) - pd.Timestamp(entry_date)).days
        record.update(
            {
                **feature_values,
                **metrics,
                "status": "realized",
                "entry_open": entry_open,
                "entry_close": entry_close,
                "entry_previous_close": entry_previous_close,
                "exit_close": exit_close,
                "holding_trading_days": int(len(path_df)),
                "holding_calendar_days": int(holding_calendar_days),
            }
        )
        records.append(record)

    if not records:
        return _empty_result_df(columns)
    result = pd.DataFrame(records)
    for column in columns:
        if column not in result.columns:
            result[column] = None
    return result[columns].sort_values(["year", "market", "code"], kind="stable").reset_index(drop=True)


def _expand_market_scope(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    scoped = frame.copy()
    scoped["market_scope"] = scoped["market"].astype(str)
    all_scoped = frame.copy()
    all_scoped["market_scope"] = "all"
    return pd.concat([all_scoped, scoped], ignore_index=True)


def _series_stat(series: pd.Series, fn: str) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    if fn == "mean":
        return float(numeric.mean())
    if fn == "median":
        return float(numeric.median())
    if fn == "q25":
        return float(numeric.quantile(0.25))
    if fn == "q75":
        return float(numeric.quantile(0.75))
    raise ValueError(f"Unsupported stat: {fn}")


def _bool_ratio_pct(mask: pd.Series) -> float | None:
    if mask.empty:
        return None
    return float(mask.mean() * 100.0)


def _build_feature_coverage_df(event_ledger_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["market_scope", "feature_name", "feature_label", "event_count", "non_null_count", "coverage_pct"]
    realized_df = event_ledger_df[event_ledger_df["status"] == "realized"].copy()
    if realized_df.empty:
        return _empty_result_df(columns)
    scoped = _expand_market_scope(realized_df)
    records: list[dict[str, Any]] = []
    for market_scope, group_df in scoped.groupby("market_scope", observed=True, sort=False):
        event_count = int(len(group_df))
        for definition in _FACTOR_DEFINITIONS:
            non_null_count = int(pd.to_numeric(group_df[definition.name], errors="coerce").notna().sum())
            records.append(
                {
                    "market_scope": str(market_scope),
                    "feature_name": definition.name,
                    "feature_label": definition.label,
                    "event_count": event_count,
                    "non_null_count": non_null_count,
                    "coverage_pct": non_null_count / event_count * 100.0 if event_count else None,
                }
            )
    result = pd.DataFrame(records)
    result["market_scope"] = pd.Categorical(
        result["market_scope"],
        categories=[scope for scope in _MARKET_SCOPE_ORDER if scope in set(result["market_scope"])],
        ordered=True,
    )
    return result.sort_values(["market_scope", "feature_name"], kind="stable").reset_index(drop=True)


def _assign_factor_buckets(
    scoped_df: pd.DataFrame,
    *,
    factor_name: str,
    bucket_count: int,
) -> pd.DataFrame:
    columns = [*scoped_df.columns, "feature_name", "feature_label", "bucket", "bucket_label"]
    value_df = scoped_df.copy()
    value_df["feature_value"] = pd.to_numeric(value_df[factor_name], errors="coerce")
    value_df = value_df[value_df["feature_value"].notna()].copy()
    if value_df.empty:
        return _empty_result_df(columns)

    definition = _FACTOR_BY_NAME[factor_name]
    frames: list[pd.DataFrame] = []
    for (_, _), group_df in value_df.groupby(["market_scope", "year"], observed=True, sort=False):
        if len(group_df) < bucket_count:
            continue
        ordered = group_df.sort_values(["feature_value", "code"], kind="stable").copy()
        ranks = np.arange(len(ordered), dtype=float)
        ordered["bucket"] = np.floor(ranks * bucket_count / len(ordered)).astype(int) + 1
        ordered["feature_name"] = factor_name
        ordered["feature_label"] = definition.label
        ordered["bucket_label"] = ordered["bucket"].map(lambda value: f"Q{int(value)}")
        frames.append(ordered)
    if not frames:
        return _empty_result_df(columns)
    return pd.concat(frames, ignore_index=True)


def _build_feature_bucket_summary_df(
    event_ledger_df: pd.DataFrame,
    *,
    bucket_count: int,
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "feature_name",
        "feature_label",
        "bucket",
        "bucket_label",
        "year_count",
        "realized_event_count",
        "mean_feature_value",
        "median_feature_value",
        "mean_return_pct",
        "median_return_pct",
        "q25_return_pct",
        "q75_return_pct",
        "win_rate_pct",
        "mean_max_drawdown_pct",
        "mean_sharpe_ratio",
        "mean_sortino_ratio",
        "mean_calmar_ratio",
    ]
    realized_df = event_ledger_df[event_ledger_df["status"] == "realized"].copy()
    if realized_df.empty:
        return _empty_result_df(columns)
    scoped = _expand_market_scope(realized_df)
    records: list[dict[str, Any]] = []
    for definition in _FACTOR_DEFINITIONS:
        bucketed = _assign_factor_buckets(
            scoped,
            factor_name=definition.name,
            bucket_count=bucket_count,
        )
        if bucketed.empty:
            continue
        for (market_scope, bucket), group_df in bucketed.groupby(
            ["market_scope", "bucket"],
            observed=True,
            sort=False,
        ):
            records.append(
                {
                    "market_scope": str(market_scope),
                    "feature_name": definition.name,
                    "feature_label": definition.label,
                    "bucket": int(bucket),
                    "bucket_label": f"Q{int(bucket)}",
                    "year_count": int(group_df["year"].nunique()),
                    "realized_event_count": int(len(group_df)),
                    "mean_feature_value": _series_stat(group_df["feature_value"], "mean"),
                    "median_feature_value": _series_stat(group_df["feature_value"], "median"),
                    "mean_return_pct": _series_stat(group_df["event_return_pct"], "mean"),
                    "median_return_pct": _series_stat(group_df["event_return_pct"], "median"),
                    "q25_return_pct": _series_stat(group_df["event_return_pct"], "q25"),
                    "q75_return_pct": _series_stat(group_df["event_return_pct"], "q75"),
                    "win_rate_pct": _bool_ratio_pct(group_df["event_return"] > 0),
                    "mean_max_drawdown_pct": _series_stat(group_df["max_drawdown_pct"], "mean"),
                    "mean_sharpe_ratio": _series_stat(group_df["sharpe_ratio"], "mean"),
                    "mean_sortino_ratio": _series_stat(group_df["sortino_ratio"], "mean"),
                    "mean_calmar_ratio": _series_stat(group_df["calmar_ratio"], "mean"),
                }
            )
    if not records:
        return _empty_result_df(columns)
    result = pd.DataFrame(records)
    result["market_scope"] = pd.Categorical(
        result["market_scope"],
        categories=[scope for scope in _MARKET_SCOPE_ORDER if scope in set(result["market_scope"])],
        ordered=True,
    )
    return result.sort_values(
        ["market_scope", "feature_name", "bucket"],
        kind="stable",
    ).reset_index(drop=True)


def _build_factor_spread_summary_df(
    feature_bucket_summary_df: pd.DataFrame,
    *,
    bucket_count: int,
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "feature_name",
        "feature_label",
        "higher_is_better",
        "low_bucket_mean_return_pct",
        "high_bucket_mean_return_pct",
        "high_minus_low_mean_return_pct",
        "preferred_minus_opposite_mean_return_pct",
    ]
    if feature_bucket_summary_df.empty:
        return _empty_result_df(columns)
    records: list[dict[str, Any]] = []
    for (market_scope, feature_name), group_df in feature_bucket_summary_df.groupby(
        ["market_scope", "feature_name"],
        observed=True,
        sort=False,
    ):
        low = group_df[group_df["bucket"] == 1]
        high = group_df[group_df["bucket"] == bucket_count]
        if low.empty or high.empty:
            continue
        definition = _FACTOR_BY_NAME[str(feature_name)]
        low_return = _to_nullable_float(low.iloc[0]["mean_return_pct"])
        high_return = _to_nullable_float(high.iloc[0]["mean_return_pct"])
        high_minus_low = (
            high_return - low_return if high_return is not None and low_return is not None else None
        )
        preferred = None
        if high_minus_low is not None and definition.higher_is_better is not None:
            preferred = high_minus_low if definition.higher_is_better else -high_minus_low
        records.append(
            {
                "market_scope": str(market_scope),
                "feature_name": str(feature_name),
                "feature_label": definition.label,
                "higher_is_better": definition.higher_is_better,
                "low_bucket_mean_return_pct": low_return,
                "high_bucket_mean_return_pct": high_return,
                "high_minus_low_mean_return_pct": high_minus_low,
                "preferred_minus_opposite_mean_return_pct": preferred,
            }
        )
    if not records:
        return _empty_result_df(columns)
    result = pd.DataFrame(records)
    result["market_scope"] = pd.Categorical(
        result["market_scope"],
        categories=[scope for scope in _MARKET_SCOPE_ORDER if scope in set(result["market_scope"])],
        ordered=True,
    )
    return result.sort_values(["market_scope", "feature_name"], kind="stable").reset_index(drop=True)


def _annualized_volatility_pct(daily_returns: pd.Series) -> float | None:
    numeric = pd.to_numeric(daily_returns, errors="coerce").dropna()
    if len(numeric) < 2:
        return None
    value = float(numeric.std(ddof=1) * math.sqrt(252.0) * 100.0)
    return value if math.isfinite(value) else None


def _annualized_sharpe(daily_returns: pd.Series) -> float | None:
    numeric = pd.to_numeric(daily_returns, errors="coerce").dropna()
    if len(numeric) < 2:
        return None
    std = float(numeric.std(ddof=1))
    if not math.isfinite(std) or math.isclose(std, 0.0, abs_tol=1e-12):
        return None
    value = float(numeric.mean()) / std * math.sqrt(252.0)
    return value if math.isfinite(value) else None


def _annualized_sortino(daily_returns: pd.Series) -> float | None:
    numeric = pd.to_numeric(daily_returns, errors="coerce").dropna()
    if len(numeric) < 2:
        return None
    downside = numeric[numeric < 0.0]
    if len(downside) < 2:
        return None
    downside_std = float(downside.std(ddof=1))
    if not math.isfinite(downside_std) or math.isclose(downside_std, 0.0, abs_tol=1e-12):
        return None
    value = float(numeric.mean()) / downside_std * math.sqrt(252.0)
    return value if math.isfinite(value) else None


def _build_annual_portfolio_daily_df(
    *,
    event_ledger_df: pd.DataFrame,
    price_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "portfolio_scope",
        "date",
        "active_positions",
        "mean_daily_return",
        "mean_daily_return_pct",
        "portfolio_value",
        "drawdown_pct",
    ]
    realized_df = event_ledger_df[event_ledger_df["status"] == "realized"].copy()
    if realized_df.empty or price_df.empty:
        return _empty_result_df(columns)
    price_by_code = {
        str(code): frame.sort_values("date", kind="stable").reset_index(drop=True)
        for code, frame in price_df.groupby("code", sort=False)
    }
    aggregate: dict[tuple[str, str, str], list[float]] = defaultdict(lambda: [0.0, 0.0])
    for event in realized_df.to_dict(orient="records"):
        code = str(event["code"])
        price_frame = price_by_code.get(code)
        if price_frame is None:
            continue
        path_df = price_frame[
            (price_frame["date"] >= str(event["entry_date"]))
            & (price_frame["date"] <= str(event["exit_date"]))
        ].copy()
        if path_df.empty:
            continue
        entry_open = _to_nullable_float(event.get("entry_open"))
        if entry_open is None or entry_open <= 0:
            continue
        close_values = pd.to_numeric(path_df["close"], errors="coerce").astype(float).to_numpy()
        if not np.isfinite(close_values).all():
            continue
        previous_close = np.concatenate(([entry_open], close_values[:-1]))
        daily_returns = close_values / previous_close - 1.0
        scopes = ("all", str(event["market"]))
        portfolio_scopes = ("all_years", str(event["year"]))
        for market_scope in scopes:
            for portfolio_scope in portfolio_scopes:
                for date_value, daily_return in zip(
                    path_df["date"].astype(str),
                    daily_returns,
                    strict=True,
                ):
                    bucket = aggregate[(market_scope, portfolio_scope, str(date_value))]
                    bucket[0] += float(daily_return)
                    bucket[1] += 1.0
    if not aggregate:
        return _empty_result_df(columns)
    records = [
        {
            "market_scope": market_scope,
            "portfolio_scope": portfolio_scope,
            "date": date_value,
            "active_positions": int(values[1]),
            "mean_daily_return": float(values[0] / values[1]),
            "mean_daily_return_pct": float(values[0] / values[1] * 100.0),
        }
        for (market_scope, portfolio_scope, date_value), values in aggregate.items()
    ]
    daily_df = pd.DataFrame(records).sort_values(
        ["market_scope", "portfolio_scope", "date"],
        kind="stable",
    ).reset_index(drop=True)
    daily_df["portfolio_value"] = np.nan
    daily_df["drawdown_pct"] = np.nan
    for _, group_df in daily_df.groupby(
        ["market_scope", "portfolio_scope"],
        observed=True,
        sort=False,
    ):
        idx = list(group_df.index)
        values = (1.0 + daily_df.loc[idx, "mean_daily_return"]).cumprod()
        peaks = values.cummax()
        daily_df.loc[idx, "portfolio_value"] = values.to_numpy()
        daily_df.loc[idx, "drawdown_pct"] = ((values / peaks - 1.0) * 100.0).to_numpy()
    return daily_df


def _build_annual_portfolio_summary_df(
    *,
    annual_portfolio_daily_df: pd.DataFrame,
    event_ledger_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "portfolio_scope",
        "realized_event_count",
        "start_date",
        "end_date",
        "active_days",
        "avg_active_positions",
        "max_active_positions",
        "total_return_pct",
        "cagr_pct",
        "max_drawdown_pct",
        "annualized_volatility_pct",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
    ]
    if annual_portfolio_daily_df.empty:
        return _empty_result_df(columns)
    realized_df = event_ledger_df[event_ledger_df["status"] == "realized"].copy()
    count_map: dict[tuple[str, str], int] = defaultdict(int)
    for event in realized_df.to_dict(orient="records"):
        for market_scope in ("all", str(event["market"])):
            count_map[(market_scope, "all_years")] += 1
            count_map[(market_scope, str(event["year"]))] += 1
    records: list[dict[str, Any]] = []
    for (market_scope, portfolio_scope), group_df in annual_portfolio_daily_df.groupby(
        ["market_scope", "portfolio_scope"],
        observed=True,
        sort=False,
    ):
        start_date = str(group_df["date"].iloc[0])
        end_date = str(group_df["date"].iloc[-1])
        total_return = float(group_df["portfolio_value"].iloc[-1] - 1.0)
        period_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days
        cagr = None
        if period_days > 0 and total_return > -1.0:
            cagr_value = (1.0 + total_return) ** (365.25 / period_days) - 1.0
            cagr = float(cagr_value) if math.isfinite(cagr_value) else None
        max_drawdown_pct = _series_stat(group_df["drawdown_pct"], "mean")
        drawdown_min = pd.to_numeric(group_df["drawdown_pct"], errors="coerce").min()
        max_drawdown_pct = float(drawdown_min) if pd.notna(drawdown_min) else max_drawdown_pct
        records.append(
            {
                "market_scope": str(market_scope),
                "portfolio_scope": str(portfolio_scope),
                "realized_event_count": int(count_map[(str(market_scope), str(portfolio_scope))]),
                "start_date": start_date,
                "end_date": end_date,
                "active_days": int(len(group_df)),
                "avg_active_positions": _series_stat(group_df["active_positions"], "mean"),
                "max_active_positions": int(pd.to_numeric(group_df["active_positions"]).max()),
                "total_return_pct": total_return * 100.0,
                "cagr_pct": cagr * 100.0 if cagr is not None else None,
                "max_drawdown_pct": max_drawdown_pct,
                "annualized_volatility_pct": _annualized_volatility_pct(
                    group_df["mean_daily_return"]
                ),
                "sharpe_ratio": _annualized_sharpe(group_df["mean_daily_return"]),
                "sortino_ratio": _annualized_sortino(group_df["mean_daily_return"]),
                "calmar_ratio": (
                    cagr / abs(max_drawdown_pct / 100.0)
                    if cagr is not None
                    and max_drawdown_pct is not None
                    and max_drawdown_pct < -1e-12
                    else None
                ),
            }
        )
    result = pd.DataFrame(records)
    result["market_scope"] = pd.Categorical(
        result["market_scope"],
        categories=[scope for scope in _MARKET_SCOPE_ORDER if scope in set(result["market_scope"])],
        ordered=True,
    )
    return result.sort_values(["market_scope", "portfolio_scope"], kind="stable").reset_index(drop=True)


def run_annual_first_open_last_close_fundamental_panel(
    db_path: str,
    *,
    markets: Sequence[str] = DEFAULT_MARKETS,
    start_year: int | None = None,
    end_year: int | None = None,
    bucket_count: int = DEFAULT_BUCKET_COUNT,
    adv_window: int = DEFAULT_ADV_WINDOW,
    include_incomplete_last_year: bool = False,
) -> AnnualFirstOpenLastCloseFundamentalPanelResult:
    normalized_markets = _normalize_selected_markets(markets)
    normalized_bucket_count = _validate_bucket_count(bucket_count)
    normalized_adv_window = _validate_adv_window(adv_window)
    market_codes = _market_query_codes(normalized_markets)

    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail
        available_start_date, available_end_date = _fetch_date_range(conn, table_name="stock_data")
        calendar_df = _query_trading_calendar(
            conn,
            start_year=start_year,
            end_year=end_year,
            include_incomplete_last_year=include_incomplete_last_year,
        )
        stock_df, uses_pit_stock_master = _query_entry_stock_master(
            conn,
            calendar_df=calendar_df,
            market_codes=market_codes,
        )
        allowed_codes = set(stock_df["code"].astype(str))
        statement_df = _query_statement_rows(conn, codes=tuple(sorted(allowed_codes)))
        if calendar_df.empty or stock_df.empty:
            empty_df = _empty_result_df([])
            return AnnualFirstOpenLastCloseFundamentalPanelResult(
                db_path=db_path,
                source_mode=source_mode,
                source_detail=source_detail,
                available_start_date=available_start_date,
                available_end_date=available_end_date,
                analysis_start_date=None,
                analysis_end_date=None,
                selected_markets=normalized_markets,
                bucket_count=normalized_bucket_count,
                adv_window=normalized_adv_window,
                current_market_snapshot_only=not uses_pit_stock_master,
                entry_timing="first_trading_day_open",
                exit_timing="last_trading_day_close",
                share_adjustment_policy=(
                    "per-share metrics use the latest disclosed share baseline as of entry, "
                    "preferring quarterly shares, then latest any disclosure"
                ),
                calendar_df=empty_df.copy(),
                event_ledger_df=empty_df.copy(),
                feature_coverage_df=empty_df.copy(),
                feature_bucket_summary_df=empty_df.copy(),
                factor_spread_summary_df=empty_df.copy(),
                annual_portfolio_daily_df=empty_df.copy(),
                annual_portfolio_summary_df=empty_df.copy(),
            )

        first_entry_year = int(str(calendar_df["year"].min()))
        price_start_date = f"{first_entry_year - 1:04d}-01-01"
        price_end_date = str(calendar_df["exit_date"].max())
        price_df = _query_price_rows(
            conn,
            codes=tuple(sorted(allowed_codes)),
            start_date=price_start_date,
            end_date=price_end_date,
        )

    event_ledger_df = _build_event_ledger(
        calendar_df=calendar_df,
        stock_df=stock_df,
        statement_df=statement_df,
        price_df=price_df,
        adv_window=normalized_adv_window,
    )
    feature_coverage_df = _build_feature_coverage_df(event_ledger_df)
    feature_bucket_summary_df = _build_feature_bucket_summary_df(
        event_ledger_df,
        bucket_count=normalized_bucket_count,
    )
    factor_spread_summary_df = _build_factor_spread_summary_df(
        feature_bucket_summary_df,
        bucket_count=normalized_bucket_count,
    )
    annual_portfolio_daily_df = _build_annual_portfolio_daily_df(
        event_ledger_df=event_ledger_df,
        price_df=price_df,
    )
    annual_portfolio_summary_df = _build_annual_portfolio_summary_df(
        annual_portfolio_daily_df=annual_portfolio_daily_df,
        event_ledger_df=event_ledger_df,
    )

    realized_df = event_ledger_df[event_ledger_df["status"] == "realized"].copy()
    analysis_start_date = str(realized_df["entry_date"].min()) if not realized_df.empty else None
    analysis_end_date = str(realized_df["exit_date"].max()) if not realized_df.empty else None

    return AnnualFirstOpenLastCloseFundamentalPanelResult(
        db_path=db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        selected_markets=normalized_markets,
        bucket_count=normalized_bucket_count,
        adv_window=normalized_adv_window,
        current_market_snapshot_only=not uses_pit_stock_master,
        entry_timing="first_trading_day_open",
        exit_timing="last_trading_day_close",
        share_adjustment_policy=(
            "per-share metrics use the latest disclosed share baseline as of entry, "
            "preferring quarterly shares, then latest any disclosure"
        ),
        calendar_df=calendar_df,
        event_ledger_df=event_ledger_df,
        feature_coverage_df=feature_coverage_df,
        feature_bucket_summary_df=feature_bucket_summary_df,
        factor_spread_summary_df=factor_spread_summary_df,
        annual_portfolio_daily_df=annual_portfolio_daily_df,
        annual_portfolio_summary_df=annual_portfolio_summary_df,
    )


def _fmt_num(value: float | int | None, digits: int = 1) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "-"
    if isinstance(value, int):
        return f"{value}"
    return f"{value:.{digits}f}"


def _build_summary_markdown(result: AnnualFirstOpenLastCloseFundamentalPanelResult) -> str:
    lines = [
        "# Annual First-Open Last-Close Fundamental Panel",
        "",
        "## Setup",
        "",
        f"- Scope: `{', '.join(result.selected_markets)}`",
        "- Event: buy each stock at the first trading day's open and sell at the last trading day's close for each complete calendar year.",
        (
            "- Market classification uses `stock_master_daily` on each entry date; historical market membership is PIT-safe for the annual entry universe."
            if not result.current_market_snapshot_only
            else "- Market classification uses the current `stocks` snapshot fallback; historical market migrations are not reconstructed."
        ),
        "- Fundamental as-of: latest FY disclosure available on or before the entry date.",
        "- Per-share adjustment: EPS, BPS, forward EPS and dividend-per-share fields are adjusted to the latest share baseline available on or before entry, preferring quarterly shares. This is intended to avoid post-FY stock-split distortions.",
        f"- Factor buckets: `{result.bucket_count}` within each year and market scope.",
        "",
        "## Portfolio Summary",
        "",
    ]
    all_years = result.annual_portfolio_summary_df[
        result.annual_portfolio_summary_df["portfolio_scope"].astype(str) == "all_years"
    ] if not result.annual_portfolio_summary_df.empty else pd.DataFrame()
    if all_years.empty:
        lines.append("- No realized annual portfolio could be built.")
    else:
        for row in all_years.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['market_scope']}`: events `{int(cast(int, row['realized_event_count']))}`, "
                f"total `{_fmt_num(cast(float | int | None, row['total_return_pct']))}%`, "
                f"CAGR `{_fmt_num(cast(float | int | None, row['cagr_pct']))}%`, "
                f"Sharpe `{_fmt_num(cast(float | int | None, row['sharpe_ratio']), 2)}`, "
                f"Sortino `{_fmt_num(cast(float | int | None, row['sortino_ratio']), 2)}`, "
                f"Calmar `{_fmt_num(cast(float | int | None, row['calmar_ratio']), 2)}`, "
                f"maxDD `{_fmt_num(cast(float | int | None, row['max_drawdown_pct']))}%`"
            )

    lines.extend(["", "## Strongest Factor Spreads", ""])
    preferred = result.factor_spread_summary_df.copy()
    if preferred.empty:
        lines.append("- No factor spread summary was available.")
    else:
        preferred["abs_spread"] = pd.to_numeric(
            preferred["preferred_minus_opposite_mean_return_pct"],
            errors="coerce",
        ).abs()
        preferred = preferred.sort_values("abs_spread", ascending=False).head(12)
        for row in preferred.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['market_scope']}` / `{row['feature_name']}`: "
                f"preferred spread `{_fmt_num(cast(float | int | None, row['preferred_minus_opposite_mean_return_pct']))}%`, "
                f"high-low `{_fmt_num(cast(float | int | None, row['high_minus_low_mean_return_pct']))}%`"
            )

    lines.extend(["", "## Diagnostics", ""])
    if result.event_ledger_df.empty:
        lines.append("- Event ledger is empty.")
    else:
        realized = result.event_ledger_df[result.event_ledger_df["status"] == "realized"]
        adjusted = realized[realized["share_adjustment_applied"] == True]  # noqa: E712
        lines.append(f"- Realized events: `{len(realized)}`")
        lines.append(f"- Events with per-share split adjustment applied: `{len(adjusted)}`")
    return "\n".join(lines)


def _build_published_summary(result: AnnualFirstOpenLastCloseFundamentalPanelResult) -> dict[str, Any]:
    return {
        "selectedMarkets": list(result.selected_markets),
        "bucketCount": result.bucket_count,
        "advWindow": result.adv_window,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "entryTiming": result.entry_timing,
        "exitTiming": result.exit_timing,
        "shareAdjustmentPolicy": result.share_adjustment_policy,
        "annualPortfolioSummary": result.annual_portfolio_summary_df.to_dict(orient="records"),
        "factorSpreadSummary": result.factor_spread_summary_df.to_dict(orient="records"),
        "featureCoverage": result.feature_coverage_df.to_dict(orient="records"),
    }


def write_annual_first_open_last_close_fundamental_panel_bundle(
    result: AnnualFirstOpenLastCloseFundamentalPanelResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=ANNUAL_FIRST_OPEN_LAST_CLOSE_FUNDAMENTAL_PANEL_EXPERIMENT_ID,
        module=__name__,
        function="run_annual_first_open_last_close_fundamental_panel",
        params={
            "markets": list(result.selected_markets),
            "bucket_count": result.bucket_count,
            "adv_window": result.adv_window,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_annual_first_open_last_close_fundamental_panel_bundle(
    bundle_path: str | Path,
) -> AnnualFirstOpenLastCloseFundamentalPanelResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=AnnualFirstOpenLastCloseFundamentalPanelResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_annual_first_open_last_close_fundamental_panel_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        ANNUAL_FIRST_OPEN_LAST_CLOSE_FUNDAMENTAL_PANEL_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_annual_first_open_last_close_fundamental_panel_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        ANNUAL_FIRST_OPEN_LAST_CLOSE_FUNDAMENTAL_PANEL_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
