"""Decompose market-scoped negative-FY-EPS events by forecast/CFO/liquidity."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd

from src.shared.utils.market_code_alias import expand_market_codes, normalize_market_scope
from src.shared.utils.statement_document import is_actual_fy_financial_statement
from src.domains.analytics.fundamental_ranking import (
    FundamentalRankingCalculator,
    StatementRow,
    adjust_per_share_value,
    normalize_period_label,
    resolve_fy_cycle_key,
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

ForecastSign = Literal["forecast_positive", "forecast_non_positive", "forecast_missing"]
CfoSign = Literal["cfo_positive", "cfo_non_positive", "cfo_missing"]
LiquidityState = Literal["high_liquidity", "low_liquidity", "missing_liquidity"]
EventStatus = Literal[
    "realized",
    "no_next_fy",
    "missing_price_history",
    "missing_entry_session",
    "missing_exit_session",
    "invalid_entry_open",
    "invalid_exit_close",
    "invalid_price_path",
    "empty_holding_window",
]

STANDARD_NEGATIVE_EPS_RIGHT_TAIL_EXPERIMENT_ID = (
    "market-behavior/standard-negative-eps-right-tail-decomposition"
)
PRIME_NEGATIVE_EPS_RIGHT_TAIL_EXPERIMENT_ID = (
    "market-behavior/prime-negative-eps-right-tail-decomposition"
)
DEFAULT_MARKET = "standard"
DEFAULT_ADV_WINDOW = 20
DEFAULT_SCOPE_NAME = "standard / FY actual EPS < 0"
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "event_summary_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
    "tail_concentration_df",
    "liquidity_thresholds_df",
    "top_winner_events_df",
    "event_ledger_df",
)
_EXPERIMENT_ID_BY_MARKET: dict[str, str] = {
    "prime": PRIME_NEGATIVE_EPS_RIGHT_TAIL_EXPERIMENT_ID,
    "standard": STANDARD_NEGATIVE_EPS_RIGHT_TAIL_EXPERIMENT_ID,
}
_SCOPE_NAME_BY_MARKET: dict[str, str] = {
    "prime": "prime / FY actual EPS < 0",
    "standard": DEFAULT_SCOPE_NAME,
}
_FORECAST_SIGN_ORDER: tuple[str, ...] = (
    "all",
    "forecast_positive",
    "forecast_non_positive",
    "forecast_missing",
)
_CFO_SIGN_ORDER: tuple[str, ...] = (
    "all",
    "cfo_positive",
    "cfo_non_positive",
    "cfo_missing",
)
_LIQUIDITY_FILTER_ORDER: tuple[str, ...] = (
    "all_liquidity",
    "high_liquidity",
    "low_liquidity",
)
_STATUS_ORDER: tuple[str, ...] = (
    "realized",
    "no_next_fy",
    "missing_price_history",
    "missing_entry_session",
    "missing_exit_session",
    "invalid_entry_open",
    "invalid_exit_close",
    "invalid_price_path",
    "empty_holding_window",
)


def _group_key(forecast_sign: str, cfo_sign: str) -> str:
    return f"{forecast_sign}__{cfo_sign}"


def _build_group_order() -> tuple[str, ...]:
    groups = ["all_negative"]
    for forecast_sign in _FORECAST_SIGN_ORDER[1:]:
        for cfo_sign in _CFO_SIGN_ORDER[1:]:
            groups.append(_group_key(forecast_sign, cfo_sign))
    return tuple(groups)


_GROUP_ORDER: tuple[str, ...] = _build_group_order()


@dataclass(frozen=True)
class StandardNegativeEpsRightTailResult:
    db_path: str
    selected_market: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    scope_name: str
    adv_window: int
    liquidity_split_method: str
    event_summary_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame
    tail_concentration_df: pd.DataFrame
    liquidity_thresholds_df: pd.DataFrame
    top_winner_events_df: pd.DataFrame
    event_ledger_df: pd.DataFrame


def _connect_duckdb(db_path: str, *, read_only: bool = True) -> Any:
    return _shared_connect_duckdb(db_path, read_only=read_only)


def _open_analysis_connection(db_path: str):
    return open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="negative-eps-right-tail-",
        connect_fn=_connect_duckdb,
    )


def _placeholder_sql(size: int) -> str:
    if size <= 0:
        raise ValueError("placeholder size must be positive")
    return ",".join("?" for _ in range(size))


def _empty_result_df(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _normalize_market(market: str) -> str:
    normalized = normalize_market_scope(market, default=str(market).strip().lower())
    if normalized not in _EXPERIMENT_ID_BY_MARKET:
        supported = ", ".join(sorted(_EXPERIMENT_ID_BY_MARKET))
        raise ValueError(f"Unsupported market: {market!r}. Supported markets: {supported}")
    return str(normalized)


def _query_market_codes(market: str) -> tuple[str, ...]:
    normalized_market = _normalize_market(market)
    return tuple(expand_market_codes([normalized_market]))


def _scope_name_for_market(market: str) -> str:
    return _SCOPE_NAME_BY_MARKET[_normalize_market(market)]


def _experiment_id_for_market(market: str) -> str:
    return _EXPERIMENT_ID_BY_MARKET[_normalize_market(market)]


def _query_canonical_stocks(conn: Any, *, market_codes: Sequence[str]) -> pd.DataFrame:
    placeholders = _placeholder_sql(len(market_codes))
    normalized_code = _normalize_code_sql("code")
    prefer_4digit = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"
    df = conn.execute(
        f"""
        WITH stocks_canonical AS (
            SELECT
                code,
                company_name,
                market_code,
                market_name,
                sector_33_name,
                listed_date,
                normalized_code
            FROM (
                SELECT
                    code,
                    company_name,
                    market_code,
                    market_name,
                    sector_33_name,
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
        )
        SELECT
            code,
            company_name,
            market_code,
            market_name,
            sector_33_name,
            listed_date,
            normalized_code
        FROM stocks_canonical
        ORDER BY code
        """,
        [str(code).lower() for code in market_codes],
    ).fetchdf()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "code",
                "company_name",
                "market_code",
                "market_name",
                "sector_33_name",
                "listed_date",
                "normalized_code",
                "market",
            ]
        )
    df["code"] = df["code"].astype(str)
    df["normalized_code"] = df["normalized_code"].astype(str)
    df["market_code"] = df["market_code"].astype(str)
    df["listed_date"] = df["listed_date"].astype(str)
    df["market"] = df["market_code"].map(
        lambda value: normalize_market_scope(value, default=str(value).lower())
    )
    return df


def _query_statement_rows(conn: Any, *, market_codes: Sequence[str]) -> pd.DataFrame:
    placeholders = _placeholder_sql(len(market_codes))
    normalized_code = _normalize_code_sql("code")
    prefer_4digit = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"
    df = conn.execute(
        f"""
        WITH stocks_canonical AS (
            SELECT
                code,
                company_name,
                market_code,
                market_name,
                sector_33_name,
                listed_date,
                normalized_code
            FROM (
                SELECT
                    code,
                    company_name,
                    market_code,
                    market_name,
                    sector_33_name,
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
        ),
        statements_canonical AS (
            SELECT
                normalized_code,
                disclosed_date,
                type_of_current_period,
                type_of_document,
                earnings_per_share,
                profit,
                equity,
                forecast_eps,
                next_year_forecast_earnings_per_share,
                sales,
                operating_profit,
                operating_cash_flow,
                total_assets,
                shares_outstanding
            FROM (
                SELECT
                    {normalized_code} AS normalized_code,
                    disclosed_date,
                    type_of_current_period,
                    type_of_document,
                    earnings_per_share,
                    profit,
                    equity,
                    forecast_eps,
                    next_year_forecast_earnings_per_share,
                    sales,
                    operating_profit,
                    operating_cash_flow,
                    total_assets,
                    shares_outstanding,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized_code}, disclosed_date
                        ORDER BY
                            {prefer_4digit},
                            CASE
                                WHEN type_of_document LIKE '%FinancialStatements%' THEN 0
                                WHEN type_of_document IS NULL OR type_of_document = '' THEN 1
                                ELSE 2
                            END
                    ) AS rn
                FROM statements
            )
            WHERE rn = 1
        )
        SELECT
            s.code,
            s.company_name,
            s.market_code,
            s.market_name,
            s.sector_33_name,
            s.listed_date,
            st.disclosed_date,
            st.type_of_current_period,
            st.type_of_document,
            st.earnings_per_share,
            st.profit,
            st.equity,
            st.forecast_eps,
            st.next_year_forecast_earnings_per_share,
            st.sales,
            st.operating_profit,
            st.operating_cash_flow,
            st.total_assets,
            st.shares_outstanding
        FROM statements_canonical st
        JOIN stocks_canonical s
          ON s.normalized_code = st.normalized_code
        ORDER BY s.code, st.disclosed_date
        """,
        [str(code).lower() for code in market_codes],
    ).fetchdf()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "code",
                "company_name",
                "market_code",
                "market_name",
                "sector_33_name",
                "listed_date",
                "disclosed_date",
                "type_of_current_period",
                "type_of_document",
                "earnings_per_share",
                "profit",
                "equity",
                "forecast_eps",
                "next_year_forecast_earnings_per_share",
                "sales",
                "operating_profit",
                "operating_cash_flow",
                "total_assets",
                "shares_outstanding",
                "market",
                "period_type",
                "is_actual_fy_statement",
                "fy_cycle_key",
            ]
        )
    df["code"] = df["code"].astype(str)
    df["listed_date"] = df["listed_date"].astype(str)
    df["disclosed_date"] = df["disclosed_date"].astype(str)
    df["market_code"] = df["market_code"].astype(str)
    df["market"] = df["market_code"].map(
        lambda value: normalize_market_scope(value, default=str(value).lower())
    )
    df["period_type"] = df["type_of_current_period"].map(normalize_period_label)
    df["is_actual_fy_statement"] = [
        is_actual_fy_financial_statement(
            period_type,
            type_of_document,
            allow_unknown_document=True,
        )
        for period_type, type_of_document in zip(
            df["period_type"],
            df["type_of_document"],
            strict=False,
        )
    ]
    df["fy_cycle_key"] = df["disclosed_date"].map(resolve_fy_cycle_key)
    return df


def _query_price_rows(
    conn: Any,
    *,
    market_codes: Sequence[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    placeholders = _placeholder_sql(len(market_codes))
    normalized_code = _normalize_code_sql("code")
    prefer_4digit = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"
    df = conn.execute(
        f"""
        WITH stocks_canonical AS (
            SELECT
                code,
                company_name,
                market_code,
                normalized_code
            FROM (
                SELECT
                    code,
                    company_name,
                    market_code,
                    {normalized_code} AS normalized_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized_code}
                        ORDER BY {prefer_4digit}
                    ) AS rn
                FROM stocks
                WHERE lower(market_code) IN ({placeholders})
            )
            WHERE rn = 1
        ),
        stock_data_canonical AS (
            SELECT
                normalized_code,
                date,
                open,
                close,
                volume
            FROM (
                SELECT
                    {normalized_code} AS normalized_code,
                    date,
                    open,
                    close,
                    volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized_code}, date
                        ORDER BY {prefer_4digit}
                    ) AS rn
                FROM stock_data
                WHERE date >= ? AND date <= ?
            )
            WHERE rn = 1
        )
        SELECT
            s.code,
            s.company_name,
            s.market_code,
            sd.date,
            sd.open,
            sd.close,
            sd.volume
        FROM stock_data_canonical sd
        JOIN stocks_canonical s
          ON s.normalized_code = sd.normalized_code
        ORDER BY s.code, sd.date
        """,
        [*[str(code).lower() for code in market_codes], start_date, end_date],
    ).fetchdf()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "code",
                "company_name",
                "market_code",
                "date",
                "open",
                "close",
                "volume",
                "market",
            ]
        )
    df["code"] = df["code"].astype(str)
    df["date"] = df["date"].astype(str)
    df["market_code"] = df["market_code"].astype(str)
    df["market"] = df["market_code"].map(
        lambda value: normalize_market_scope(value, default=str(value).lower())
    )
    return df


def _build_statement_rows_by_code(statement_df: pd.DataFrame) -> dict[str, list[StatementRow]]:
    rows_by_code: dict[str, list[StatementRow]] = defaultdict(list)
    for row in statement_df.itertuples(index=False):
        rows_by_code[str(row.code)].append(
            StatementRow(
                code=str(row.code),
                disclosed_date=str(row.disclosed_date),
                period_type=str(row.period_type),
                earnings_per_share=_to_nullable_float(row.earnings_per_share),
                forecast_eps=_to_nullable_float(row.forecast_eps),
                next_year_forecast_earnings_per_share=_to_nullable_float(
                    row.next_year_forecast_earnings_per_share
                ),
                shares_outstanding=_to_nullable_float(row.shares_outstanding),
                fy_cycle_key=str(row.fy_cycle_key) if row.fy_cycle_key is not None else None,
            )
        )
    return rows_by_code


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


def _classify_forecast_sign(value: float | None) -> ForecastSign:
    if value is None:
        return "forecast_missing"
    if value > 0:
        return "forecast_positive"
    return "forecast_non_positive"


def _classify_cfo_sign(value: float | None) -> CfoSign:
    if value is None:
        return "cfo_missing"
    if value > 0:
        return "cfo_positive"
    return "cfo_non_positive"


def _group_label(group_key: str) -> str:
    if group_key == "all_negative":
        return "All negative EPS"
    forecast_sign, cfo_sign = group_key.split("__", maxsplit=1)
    forecast_label = {
        "forecast_positive": "Forecast > 0",
        "forecast_non_positive": "Forecast <= 0",
        "forecast_missing": "Forecast missing",
    }[forecast_sign]
    cfo_label = {
        "cfo_positive": "CFO > 0",
        "cfo_non_positive": "CFO <= 0",
        "cfo_missing": "CFO missing",
    }[cfo_sign]
    return f"{forecast_label} / {cfo_label}"


def _compute_entry_adv(
    price_frame: pd.DataFrame,
    *,
    entry_idx: int,
    adv_window: int,
) -> tuple[float | None, int]:
    if entry_idx <= 0:
        return None, 0
    start_idx = max(0, entry_idx - adv_window)
    history_df = price_frame.iloc[start_idx:entry_idx].copy()
    if history_df.empty:
        return None, 0
    close_values = pd.to_numeric(history_df["close"], errors="coerce")
    volume_values = pd.to_numeric(history_df["volume"], errors="coerce")
    trading_value = close_values * volume_values
    trading_value = trading_value.replace([np.inf, -np.inf], np.nan).dropna()
    if trading_value.empty:
        return None, 0
    return float(trading_value.mean()), int(len(trading_value))


def _build_event_ledger(
    *,
    stock_df: pd.DataFrame,
    statement_df: pd.DataFrame,
    price_df: pd.DataFrame,
    adv_window: int,
) -> pd.DataFrame:
    columns = [
        "event_id",
        "code",
        "company_name",
        "market",
        "market_code",
        "sector_33_name",
        "listed_date",
        "disclosed_date",
        "disclosed_year",
        "fy_cycle_key",
        "next_fy_disclosed_date",
        "baseline_shares",
        "raw_actual_eps",
        "actual_eps",
        "forecast_eps",
        "forecast_source",
        "forecast_sign",
        "operating_cash_flow",
        "cfo_sign",
        "group_key",
        "group_label",
        "entry_date",
        "entry_adv",
        "entry_adv_window_observations",
        "liquidity_threshold",
        "liquidity_state",
        "status",
        "entry_open",
        "entry_close",
        "exit_date",
        "exit_close",
        "event_return",
        "event_return_pct",
        "holding_trading_days",
        "holding_calendar_days",
        "entry_to_exit_price_path_rows",
    ]
    if stock_df.empty or statement_df.empty:
        return _empty_result_df(columns)

    stock_meta_by_code = stock_df.set_index("code").to_dict(orient="index")
    statement_rows_by_code = _build_statement_rows_by_code(statement_df)
    price_by_code: dict[str, pd.DataFrame] = {
        str(code): frame.reset_index(drop=True)
        for code, frame in price_df.groupby("code", sort=False)
    }
    calculator = FundamentalRankingCalculator()
    records: list[dict[str, Any]] = []

    for code, code_statement_df in statement_df.groupby("code", sort=False):
        code_str = str(code)
        statement_rows = statement_rows_by_code.get(code_str, [])
        if not statement_rows:
            continue
        fy_anchor_df = (
            code_statement_df[code_statement_df["is_actual_fy_statement"]]
            .sort_values("disclosed_date", kind="stable")
            .groupby("fy_cycle_key", sort=False, observed=True)
            .head(1)
            .reset_index(drop=True)
        )
        if fy_anchor_df.empty:
            continue

        stock_meta = stock_meta_by_code.get(code_str, {})
        market = str(stock_meta.get("market", ""))
        market_code = str(stock_meta.get("market_code", ""))
        company_name = str(stock_meta.get("company_name", ""))
        sector_33_name = str(stock_meta.get("sector_33_name", ""))
        listed_date = str(stock_meta.get("listed_date", ""))
        price_frame = price_by_code.get(code_str)

        for row_pos, anchor in enumerate(fy_anchor_df.itertuples(index=False)):
            disclosed_date = str(anchor.disclosed_date)
            next_fy_disclosed_date = (
                str(fy_anchor_df.iloc[row_pos + 1]["disclosed_date"])
                if row_pos + 1 < len(fy_anchor_df)
                else None
            )
            baseline_shares = calculator.resolve_baseline_shares(
                statement_rows,
                as_of_date=disclosed_date,
            )
            raw_actual_eps = _to_nullable_float(anchor.earnings_per_share)
            actual_eps = adjust_per_share_value(
                raw_actual_eps,
                _to_nullable_float(anchor.shares_outstanding),
                baseline_shares,
            )
            if actual_eps is None or not math.isfinite(actual_eps) or actual_eps >= 0.0:
                continue

            forecast_snapshot = calculator.resolve_latest_forecast_snapshot(
                statement_rows,
                baseline_shares,
                as_of_date=disclosed_date,
            )
            forecast_eps = forecast_snapshot.value if forecast_snapshot is not None else None
            forecast_source = forecast_snapshot.source if forecast_snapshot is not None else None
            forecast_sign = _classify_forecast_sign(forecast_eps)
            operating_cash_flow = _to_nullable_float(anchor.operating_cash_flow)
            cfo_sign = _classify_cfo_sign(operating_cash_flow)
            group_key = _group_key(forecast_sign, cfo_sign)

            record: dict[str, Any] = {
                "event_id": f"{code_str}:{disclosed_date}",
                "code": code_str,
                "company_name": company_name,
                "market": market,
                "market_code": market_code,
                "sector_33_name": sector_33_name,
                "listed_date": listed_date,
                "disclosed_date": disclosed_date,
                "disclosed_year": disclosed_date[:4],
                "fy_cycle_key": str(anchor.fy_cycle_key),
                "next_fy_disclosed_date": next_fy_disclosed_date,
                "baseline_shares": baseline_shares,
                "raw_actual_eps": raw_actual_eps,
                "actual_eps": actual_eps,
                "forecast_eps": forecast_eps,
                "forecast_source": forecast_source,
                "forecast_sign": forecast_sign,
                "operating_cash_flow": operating_cash_flow,
                "cfo_sign": cfo_sign,
                "group_key": group_key,
                "group_label": _group_label(group_key),
                "entry_date": None,
                "entry_adv": None,
                "entry_adv_window_observations": 0,
                "liquidity_threshold": None,
                "liquidity_state": "missing_liquidity",
                "status": None,
                "entry_open": None,
                "entry_close": None,
                "exit_date": None,
                "exit_close": None,
                "event_return": None,
                "event_return_pct": None,
                "holding_trading_days": None,
                "holding_calendar_days": None,
                "entry_to_exit_price_path_rows": None,
            }

            if price_frame is None or price_frame.empty:
                record["status"] = "missing_price_history"
                records.append(record)
                continue

            dates = price_frame["date"].to_numpy(dtype=str)
            entry_idx = int(np.searchsorted(dates, disclosed_date, side="right"))
            if entry_idx < len(price_frame):
                entry_date = str(price_frame.iloc[entry_idx]["date"])
                entry_adv, adv_obs = _compute_entry_adv(
                    price_frame,
                    entry_idx=entry_idx,
                    adv_window=adv_window,
                )
                record["entry_date"] = entry_date
                record["entry_adv"] = entry_adv
                record["entry_adv_window_observations"] = adv_obs

            if next_fy_disclosed_date is None:
                record["status"] = "no_next_fy"
                records.append(record)
                continue
            if entry_idx >= len(price_frame):
                record["status"] = "missing_entry_session"
                records.append(record)
                continue

            exit_idx = int(np.searchsorted(dates, next_fy_disclosed_date, side="left")) - 1
            if exit_idx < 0:
                record["status"] = "missing_exit_session"
                records.append(record)
                continue
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
            exit_close = _to_nullable_float(path_df.iloc[-1]["close"])
            close_values = pd.to_numeric(path_df["close"], errors="coerce").astype(float)

            if entry_open is None or not math.isfinite(entry_open) or math.isclose(entry_open, 0.0):
                record["status"] = "invalid_entry_open"
                records.append(record)
                continue
            if exit_close is None or not math.isfinite(exit_close):
                record["status"] = "invalid_exit_close"
                records.append(record)
                continue
            if close_values.isna().any():
                record["status"] = "invalid_price_path"
                records.append(record)
                continue

            entry_date = str(path_df.iloc[0]["date"])
            exit_date = str(path_df.iloc[-1]["date"])
            event_return = exit_close / entry_open - 1.0
            holding_calendar_days = (
                pd.Timestamp(exit_date) - pd.Timestamp(entry_date)
            ).days

            record.update(
                {
                    "status": "realized",
                    "entry_date": entry_date,
                    "entry_open": entry_open,
                    "entry_close": entry_close,
                    "exit_date": exit_date,
                    "exit_close": exit_close,
                    "event_return": event_return,
                    "event_return_pct": event_return * 100.0,
                    "holding_trading_days": int(len(path_df)),
                    "holding_calendar_days": int(holding_calendar_days),
                    "entry_to_exit_price_path_rows": int(len(path_df)),
                }
            )
            records.append(record)

    event_ledger_df = pd.DataFrame(records, columns=columns)
    if event_ledger_df.empty:
        return event_ledger_df

    event_ledger_df["group_label"] = event_ledger_df["group_key"].map(_group_label)
    event_ledger_df["status"] = event_ledger_df["status"].astype(str)
    event_ledger_df["forecast_sign"] = event_ledger_df["forecast_sign"].astype(str)
    event_ledger_df["cfo_sign"] = event_ledger_df["cfo_sign"].astype(str)
    event_ledger_df = event_ledger_df.sort_values(
        ["disclosed_date", "code"],
        kind="stable",
    ).reset_index(drop=True)
    return event_ledger_df


def _assign_liquidity_state(
    event_ledger_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    threshold_columns = [
        "disclosed_year",
        "liquidity_threshold",
        "event_count_with_adv",
    ]
    if event_ledger_df.empty:
        return event_ledger_df.copy(), _empty_result_df(threshold_columns)

    result_df = event_ledger_df.copy()
    adv_series = pd.to_numeric(result_df["entry_adv"], errors="coerce")
    threshold_df = (
        result_df.loc[adv_series.notna(), ["disclosed_year", "entry_adv"]]
        .groupby("disclosed_year", observed=True, sort=True)
        .agg(
            liquidity_threshold=("entry_adv", "median"),
            event_count_with_adv=("entry_adv", "size"),
        )
        .reset_index()
    )
    if threshold_df.empty:
        result_df["liquidity_threshold"] = np.nan
        result_df["liquidity_state"] = "missing_liquidity"
        return result_df, _empty_result_df(threshold_columns)

    threshold_map = threshold_df.set_index("disclosed_year")["liquidity_threshold"].to_dict()
    result_df["liquidity_threshold"] = result_df["disclosed_year"].map(threshold_map)

    def _classify_row(row: pd.Series) -> str:
        entry_adv = _to_nullable_float(row["entry_adv"])
        threshold = _to_nullable_float(row["liquidity_threshold"])
        if entry_adv is None or threshold is None:
            return "missing_liquidity"
        if entry_adv >= threshold:
            return "high_liquidity"
        return "low_liquidity"

    result_df["liquidity_state"] = result_df.apply(_classify_row, axis=1)
    return result_df, threshold_df


def _expand_group_scope(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    specific_df = frame.copy()
    all_df = frame.copy()
    all_df["group_key"] = "all_negative"
    all_df["group_label"] = _group_label("all_negative")
    all_df["forecast_sign"] = "all"
    all_df["cfo_sign"] = "all"
    return pd.concat([all_df, specific_df], ignore_index=True)


def _expand_liquidity_scope(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    base_df = frame.copy()
    base_df["liquidity_filter"] = "all_liquidity"
    expanded_frames: list[pd.DataFrame] = [base_df]
    for liquidity_state, filter_key in (
        ("high_liquidity", "high_liquidity"),
        ("low_liquidity", "low_liquidity"),
    ):
        filtered_df = frame[frame["liquidity_state"] == liquidity_state].copy()
        if filtered_df.empty:
            continue
        filtered_df["liquidity_filter"] = filter_key
        expanded_frames.append(filtered_df)
    return pd.concat(expanded_frames, ignore_index=True)


def _series_stat(series: pd.Series, fn: str) -> float | None:
    if series.empty:
        return None
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
    if fn == "q90":
        return float(numeric.quantile(0.90))
    if fn == "q95":
        return float(numeric.quantile(0.95))
    raise ValueError(f"Unsupported fn: {fn}")


def _bool_ratio_pct(mask: pd.Series) -> float | None:
    if mask.empty:
        return None
    return float(mask.mean() * 100.0)


def _build_event_summary_df(event_ledger_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "group_key",
        "group_label",
        "forecast_sign",
        "cfo_sign",
        "liquidity_filter",
        "signed_event_count",
        "realized_event_count",
        "realized_ratio_pct",
        "forecast_positive_rate_pct",
        "cfo_positive_rate_pct",
        "no_next_fy_count",
        "missing_price_history_count",
        "missing_entry_session_count",
        "missing_exit_session_count",
        "other_unresolved_count",
        "mean_return_pct",
        "median_return_pct",
        "q25_return_pct",
        "q75_return_pct",
        "win_rate_pct",
        "mean_actual_eps",
        "mean_forecast_eps",
        "mean_operating_cash_flow",
        "mean_entry_adv",
    ]
    if event_ledger_df.empty:
        return _empty_result_df(columns)

    base_df = event_ledger_df.copy()
    base_df["raw_forecast_sign"] = base_df["forecast_sign"].astype(str)
    base_df["raw_cfo_sign"] = base_df["cfo_sign"].astype(str)
    expanded = _expand_liquidity_scope(_expand_group_scope(base_df))
    records: list[dict[str, Any]] = []
    for (group_key, forecast_sign, cfo_sign, liquidity_filter), group_df in expanded.groupby(
        ["group_key", "forecast_sign", "cfo_sign", "liquidity_filter"],
        observed=True,
        sort=False,
    ):
        realized_df = group_df[group_df["status"] == "realized"].copy()
        records.append(
            {
                "group_key": str(group_key),
                "group_label": _group_label(str(group_key)),
                "forecast_sign": str(forecast_sign),
                "cfo_sign": str(cfo_sign),
                "liquidity_filter": str(liquidity_filter),
                "signed_event_count": int(len(group_df)),
                "realized_event_count": int(len(realized_df)),
                "realized_ratio_pct": (
                    float(len(realized_df) / len(group_df) * 100.0) if len(group_df) else None
                ),
                "forecast_positive_rate_pct": _bool_ratio_pct(
                    group_df["raw_forecast_sign"] == "forecast_positive"
                ),
                "cfo_positive_rate_pct": _bool_ratio_pct(
                    group_df["raw_cfo_sign"] == "cfo_positive"
                ),
                "no_next_fy_count": int((group_df["status"] == "no_next_fy").sum()),
                "missing_price_history_count": int(
                    (group_df["status"] == "missing_price_history").sum()
                ),
                "missing_entry_session_count": int(
                    (group_df["status"] == "missing_entry_session").sum()
                ),
                "missing_exit_session_count": int(
                    (group_df["status"] == "missing_exit_session").sum()
                ),
                "other_unresolved_count": int(
                    group_df["status"].isin(
                        {
                            "invalid_entry_open",
                            "invalid_exit_close",
                            "invalid_price_path",
                            "empty_holding_window",
                        }
                    ).sum()
                ),
                "mean_return_pct": _series_stat(realized_df["event_return_pct"], "mean"),
                "median_return_pct": _series_stat(realized_df["event_return_pct"], "median"),
                "q25_return_pct": _series_stat(realized_df["event_return_pct"], "q25"),
                "q75_return_pct": _series_stat(realized_df["event_return_pct"], "q75"),
                "win_rate_pct": _bool_ratio_pct(realized_df["event_return"] > 0),
                "mean_actual_eps": _series_stat(realized_df["actual_eps"], "mean"),
                "mean_forecast_eps": _series_stat(realized_df["forecast_eps"], "mean"),
                "mean_operating_cash_flow": _series_stat(
                    realized_df["operating_cash_flow"], "mean"
                ),
                "mean_entry_adv": _series_stat(realized_df["entry_adv"], "mean"),
            }
        )

    summary_df = pd.DataFrame(records)
    if summary_df.empty:
        return _empty_result_df(columns)
    summary_df["group_key"] = pd.Categorical(
        summary_df["group_key"],
        categories=[group for group in _GROUP_ORDER if group in set(summary_df["group_key"])],
        ordered=True,
    )
    summary_df["forecast_sign"] = pd.Categorical(
        summary_df["forecast_sign"],
        categories=[value for value in _FORECAST_SIGN_ORDER if value in set(summary_df["forecast_sign"])],
        ordered=True,
    )
    summary_df["cfo_sign"] = pd.Categorical(
        summary_df["cfo_sign"],
        categories=[value for value in _CFO_SIGN_ORDER if value in set(summary_df["cfo_sign"])],
        ordered=True,
    )
    summary_df["liquidity_filter"] = pd.Categorical(
        summary_df["liquidity_filter"],
        categories=list(_LIQUIDITY_FILTER_ORDER),
        ordered=True,
    )
    return summary_df.sort_values(
        ["group_key", "liquidity_filter"],
        kind="stable",
    ).reset_index(drop=True)


def _build_portfolio_daily_df(
    *,
    event_ledger_df: pd.DataFrame,
    price_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "group_key",
        "group_label",
        "liquidity_filter",
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

    realized_df = _expand_liquidity_scope(_expand_group_scope(realized_df))
    price_by_code: dict[str, pd.DataFrame] = {
        str(code): frame.reset_index(drop=True)
        for code, frame in price_df.groupby("code", sort=False)
    }
    aggregate: dict[tuple[str, str, str], list[float]] = {}

    for event in realized_df.itertuples(index=False):
        code = str(event.code)
        price_frame = price_by_code.get(code)
        if price_frame is None or price_frame.empty:
            continue
        path_df = price_frame[
            (price_frame["date"] >= str(event.entry_date))
            & (price_frame["date"] <= str(event.exit_date))
        ][["date", "close"]].copy()
        if path_df.empty:
            continue
        close_values = pd.to_numeric(path_df["close"], errors="coerce").astype(float).to_numpy()
        if not np.isfinite(close_values).all():
            continue
        entry_open = _to_nullable_float(event.entry_open)
        if entry_open is None:
            continue
        previous_close = np.concatenate(([entry_open], close_values[:-1]))
        daily_returns = close_values / previous_close - 1.0
        for date_value, daily_return in zip(path_df["date"].astype(str), daily_returns, strict=True):
            key = (
                str(event.group_key),
                str(event.liquidity_filter),
                str(date_value),
            )
            bucket = aggregate.setdefault(key, [0.0, 0.0])
            bucket[0] += float(daily_return)
            bucket[1] += 1.0

    if not aggregate:
        return _empty_result_df(columns)

    records = [
        {
            "group_key": group_key,
            "group_label": _group_label(group_key),
            "liquidity_filter": liquidity_filter,
            "date": date_value,
            "active_positions": int(values[1]),
            "mean_daily_return": float(values[0] / values[1]),
        }
        for (group_key, liquidity_filter, date_value), values in aggregate.items()
    ]
    portfolio_daily_df = pd.DataFrame(records)
    portfolio_daily_df["mean_daily_return_pct"] = (
        portfolio_daily_df["mean_daily_return"] * 100.0
    )
    portfolio_daily_df["group_key"] = pd.Categorical(
        portfolio_daily_df["group_key"],
        categories=[group for group in _GROUP_ORDER if group in set(portfolio_daily_df["group_key"])],
        ordered=True,
    )
    portfolio_daily_df["liquidity_filter"] = pd.Categorical(
        portfolio_daily_df["liquidity_filter"],
        categories=list(_LIQUIDITY_FILTER_ORDER),
        ordered=True,
    )
    portfolio_daily_df = portfolio_daily_df.sort_values(
        ["group_key", "liquidity_filter", "date"],
        kind="stable",
    ).reset_index(drop=True)

    portfolio_daily_df["portfolio_value"] = np.nan
    portfolio_daily_df["drawdown_pct"] = np.nan
    for _, grouped_df in portfolio_daily_df.groupby(
        ["group_key", "liquidity_filter"],
        observed=True,
        sort=False,
    ):
        idx = list(grouped_df.index)
        values = (1.0 + portfolio_daily_df.loc[idx, "mean_daily_return"]).cumprod()
        peaks = values.cummax()
        drawdowns = (values / peaks - 1.0) * 100.0
        portfolio_daily_df.loc[idx, "portfolio_value"] = values.to_numpy()
        portfolio_daily_df.loc[idx, "drawdown_pct"] = drawdowns.to_numpy()

    return portfolio_daily_df


def _annualized_volatility_pct(daily_returns: pd.Series) -> float | None:
    numeric = pd.to_numeric(daily_returns, errors="coerce").dropna()
    if len(numeric) < 2:
        return None
    volatility = float(numeric.std(ddof=1) * math.sqrt(252.0) * 100.0)
    return volatility if math.isfinite(volatility) else None


def _annualized_sharpe(daily_returns: pd.Series) -> float | None:
    numeric = pd.to_numeric(daily_returns, errors="coerce").dropna()
    if len(numeric) < 2:
        return None
    std = float(numeric.std(ddof=1))
    if not math.isfinite(std) or math.isclose(std, 0.0, abs_tol=1e-12):
        return None
    mean = float(numeric.mean())
    sharpe = mean / std * math.sqrt(252.0)
    return float(sharpe) if math.isfinite(sharpe) else None


def _build_portfolio_summary_df(
    *,
    portfolio_daily_df: pd.DataFrame,
    event_ledger_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "group_key",
        "group_label",
        "liquidity_filter",
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
    ]
    if portfolio_daily_df.empty:
        return _empty_result_df(columns)

    realized_df = _expand_liquidity_scope(
        _expand_group_scope(event_ledger_df[event_ledger_df["status"] == "realized"].copy())
    )
    realized_counts = (
        realized_df.groupby(
            ["group_key", "liquidity_filter"], observed=True, sort=False
        )
        .size()
        .to_dict()
    )

    records: list[dict[str, Any]] = []
    for (group_key, liquidity_filter), group_df in portfolio_daily_df.groupby(
        ["group_key", "liquidity_filter"],
        observed=True,
        sort=False,
    ):
        start_date = str(group_df["date"].iloc[0])
        end_date = str(group_df["date"].iloc[-1])
        total_return = float(group_df["portfolio_value"].iloc[-1] - 1.0)
        drawdown_min = pd.to_numeric(group_df["drawdown_pct"], errors="coerce").min()
        cagr_pct: float | None = None
        period_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days
        if period_days > 0:
            cagr = (1.0 + total_return) ** (365.25 / period_days) - 1.0
            cagr_pct = float(cagr * 100.0) if math.isfinite(cagr) else None
        records.append(
            {
                "group_key": str(group_key),
                "group_label": _group_label(str(group_key)),
                "liquidity_filter": str(liquidity_filter),
                "realized_event_count": int(
                    realized_counts.get((str(group_key), str(liquidity_filter)), 0)
                ),
                "start_date": start_date,
                "end_date": end_date,
                "active_days": int(len(group_df)),
                "avg_active_positions": _series_stat(group_df["active_positions"], "mean"),
                "max_active_positions": int(pd.to_numeric(group_df["active_positions"]).max()),
                "total_return_pct": total_return * 100.0,
                "cagr_pct": cagr_pct,
                "max_drawdown_pct": float(drawdown_min) if pd.notna(drawdown_min) else None,
                "annualized_volatility_pct": _annualized_volatility_pct(
                    group_df["mean_daily_return"]
                ),
                "sharpe_ratio": _annualized_sharpe(group_df["mean_daily_return"]),
            }
        )

    summary_df = pd.DataFrame(records)
    if summary_df.empty:
        return _empty_result_df(columns)
    summary_df["group_key"] = pd.Categorical(
        summary_df["group_key"],
        categories=[group for group in _GROUP_ORDER if group in set(summary_df["group_key"])],
        ordered=True,
    )
    summary_df["liquidity_filter"] = pd.Categorical(
        summary_df["liquidity_filter"],
        categories=list(_LIQUIDITY_FILTER_ORDER),
        ordered=True,
    )
    return summary_df.sort_values(
        ["group_key", "liquidity_filter"],
        kind="stable",
    ).reset_index(drop=True)


def _gross_gain_share_pct(series: pd.Series, count: int) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    positive = numeric[numeric > 0].sort_values(ascending=False)
    if positive.empty:
        return None
    gross_gain = float(positive.sum())
    top_gain = float(positive.head(count).sum())
    if math.isclose(gross_gain, 0.0, abs_tol=1e-12):
        return None
    return top_gain / gross_gain * 100.0


def _build_tail_concentration_df(event_ledger_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "group_key",
        "group_label",
        "liquidity_filter",
        "realized_event_count",
        "positive_event_count",
        "p90_return_pct",
        "p95_return_pct",
        "max_return_pct",
        "top_1_gross_gain_share_pct",
        "top_5_gross_gain_share_pct",
        "top_decile_event_count",
        "top_decile_gross_gain_share_pct",
    ]
    realized_df = event_ledger_df[event_ledger_df["status"] == "realized"].copy()
    if realized_df.empty:
        return _empty_result_df(columns)

    expanded = _expand_liquidity_scope(_expand_group_scope(realized_df))
    records: list[dict[str, Any]] = []
    for (group_key, liquidity_filter), group_df in expanded.groupby(
        ["group_key", "liquidity_filter"],
        observed=True,
        sort=False,
    ):
        returns = pd.to_numeric(group_df["event_return"], errors="coerce").dropna()
        positive = returns[returns > 0].sort_values(ascending=False)
        top_decile_event_count = max(1, int(math.ceil(len(positive) * 0.1))) if len(positive) else 0
        records.append(
            {
                "group_key": str(group_key),
                "group_label": _group_label(str(group_key)),
                "liquidity_filter": str(liquidity_filter),
                "realized_event_count": int(len(group_df)),
                "positive_event_count": int(len(positive)),
                "p90_return_pct": _series_stat(group_df["event_return_pct"], "q90"),
                "p95_return_pct": _series_stat(group_df["event_return_pct"], "q95"),
                "max_return_pct": (
                    float(pd.to_numeric(group_df["event_return_pct"], errors="coerce").max())
                    if pd.to_numeric(group_df["event_return_pct"], errors="coerce").notna().any()
                    else None
                ),
                "top_1_gross_gain_share_pct": _gross_gain_share_pct(group_df["event_return"], 1),
                "top_5_gross_gain_share_pct": _gross_gain_share_pct(group_df["event_return"], 5),
                "top_decile_event_count": top_decile_event_count,
                "top_decile_gross_gain_share_pct": (
                    _gross_gain_share_pct(group_df["event_return"], top_decile_event_count)
                    if top_decile_event_count > 0
                    else None
                ),
            }
        )

    summary_df = pd.DataFrame(records)
    if summary_df.empty:
        return _empty_result_df(columns)
    summary_df["group_key"] = pd.Categorical(
        summary_df["group_key"],
        categories=[group for group in _GROUP_ORDER if group in set(summary_df["group_key"])],
        ordered=True,
    )
    summary_df["liquidity_filter"] = pd.Categorical(
        summary_df["liquidity_filter"],
        categories=list(_LIQUIDITY_FILTER_ORDER),
        ordered=True,
    )
    return summary_df.sort_values(
        ["group_key", "liquidity_filter"],
        kind="stable",
    ).reset_index(drop=True)


def _build_top_winner_events_df(
    event_ledger_df: pd.DataFrame,
    *,
    limit: int = 50,
) -> pd.DataFrame:
    columns = [
        "code",
        "company_name",
        "disclosed_date",
        "entry_date",
        "exit_date",
        "group_key",
        "group_label",
        "forecast_sign",
        "cfo_sign",
        "liquidity_state",
        "event_return_pct",
        "actual_eps",
        "forecast_eps",
        "operating_cash_flow",
        "entry_adv",
    ]
    realized_df = event_ledger_df[event_ledger_df["status"] == "realized"].copy()
    if realized_df.empty:
        return _empty_result_df(columns)
    top_df = (
        realized_df.sort_values("event_return_pct", ascending=False, kind="stable")
        .head(limit)
        .reset_index(drop=True)
    )
    top_df["group_label"] = top_df["group_key"].map(_group_label)
    return top_df[columns]


def _resolve_price_start_date(
    analysis_start_date: str | None,
    available_start_date: str | None,
    *,
    adv_window: int,
) -> str | None:
    if analysis_start_date is None:
        return available_start_date
    lookback_days = max(adv_window * 3, adv_window + 5)
    candidate = (pd.Timestamp(analysis_start_date) - pd.Timedelta(days=lookback_days)).strftime(
        "%Y-%m-%d"
    )
    if available_start_date is None:
        return candidate
    return max(str(available_start_date), candidate)


def run_standard_negative_eps_right_tail_decomposition(
    db_path: str,
    *,
    market: str = DEFAULT_MARKET,
    adv_window: int = DEFAULT_ADV_WINDOW,
) -> StandardNegativeEpsRightTailResult:
    if adv_window <= 0:
        raise ValueError("adv_window must be positive")

    selected_market = _normalize_market(market)
    market_codes = _query_market_codes(selected_market)
    scope_name = _scope_name_for_market(selected_market)
    liquidity_split_method = f"year_median_adv_{adv_window}"

    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        available_start_date, available_end_date = _fetch_date_range(
            conn,
            table_name="stock_data",
        )
        stock_df = _query_canonical_stocks(conn, market_codes=market_codes)
        statement_df = _query_statement_rows(conn, market_codes=market_codes)
        if statement_df.empty:
            empty_df = _empty_result_df([])
            return StandardNegativeEpsRightTailResult(
                db_path=db_path,
                selected_market=selected_market,
                source_mode=ctx.source_mode,
                source_detail=ctx.source_detail,
                available_start_date=available_start_date,
                available_end_date=available_end_date,
                analysis_start_date=None,
                analysis_end_date=None,
                scope_name=scope_name,
                adv_window=adv_window,
                liquidity_split_method=liquidity_split_method,
                event_summary_df=empty_df.copy(),
                portfolio_daily_df=empty_df.copy(),
                portfolio_summary_df=empty_df.copy(),
                tail_concentration_df=empty_df.copy(),
                liquidity_thresholds_df=empty_df.copy(),
                top_winner_events_df=empty_df.copy(),
                event_ledger_df=empty_df.copy(),
            )

        fy_df = statement_df[statement_df["is_actual_fy_statement"]].copy()
        analysis_start_date = str(fy_df["disclosed_date"].min()) if not fy_df.empty else None
        analysis_end_date = str(fy_df["disclosed_date"].max()) if not fy_df.empty else None

        price_start_date = _resolve_price_start_date(
            analysis_start_date,
            available_start_date,
            adv_window=adv_window,
        )
        max_next_fy_date = None
        if not fy_df.empty:
            fy_anchor_df = (
                fy_df.sort_values(["code", "disclosed_date"], kind="stable")
                .groupby(["code", "fy_cycle_key"], sort=False, observed=True)
                .head(1)
            )
            next_dates = (
                fy_anchor_df.sort_values(["code", "disclosed_date"], kind="stable")
                .groupby("code", sort=False, observed=True)["disclosed_date"]
                .shift(-1)
                .dropna()
            )
            if not next_dates.empty:
                max_next_fy_date = str(next_dates.max())
        price_end_date = max_next_fy_date or available_end_date or analysis_end_date
        price_df = (
            _query_price_rows(
                conn,
                market_codes=market_codes,
                start_date=str(price_start_date or available_start_date),
                end_date=str(price_end_date or available_end_date),
            )
            if price_start_date and price_end_date
            else pd.DataFrame()
        )

    event_ledger_df = _build_event_ledger(
        stock_df=stock_df,
        statement_df=statement_df,
        price_df=price_df,
        adv_window=adv_window,
    )
    event_ledger_df, liquidity_thresholds_df = _assign_liquidity_state(event_ledger_df)
    event_summary_df = _build_event_summary_df(event_ledger_df)
    portfolio_daily_df = _build_portfolio_daily_df(
        event_ledger_df=event_ledger_df,
        price_df=price_df,
    )
    portfolio_summary_df = _build_portfolio_summary_df(
        portfolio_daily_df=portfolio_daily_df,
        event_ledger_df=event_ledger_df,
    )
    tail_concentration_df = _build_tail_concentration_df(event_ledger_df)
    top_winner_events_df = _build_top_winner_events_df(event_ledger_df)

    realized_df = event_ledger_df[event_ledger_df["status"] == "realized"]
    if not realized_df.empty:
        analysis_start_date = str(realized_df["disclosed_date"].min())
        analysis_end_date = str(realized_df["exit_date"].max())

    return StandardNegativeEpsRightTailResult(
        db_path=db_path,
        selected_market=selected_market,
        source_mode=ctx.source_mode if "ctx" in locals() else "live",
        source_detail=ctx.source_detail if "ctx" in locals() else f"live DuckDB: {db_path}",
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        scope_name=scope_name,
        adv_window=adv_window,
        liquidity_split_method=liquidity_split_method,
        event_summary_df=event_summary_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_summary_df=portfolio_summary_df,
        tail_concentration_df=tail_concentration_df,
        liquidity_thresholds_df=liquidity_thresholds_df,
        top_winner_events_df=top_winner_events_df,
        event_ledger_df=event_ledger_df,
    )


def _fmt_num(value: float | int | None, digits: int = 1) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "-"
    if isinstance(value, int):
        return f"{value}"
    return f"{value:.{digits}f}"


def _build_summary_markdown(result: StandardNegativeEpsRightTailResult) -> str:
    lines = [
        f"# {result.selected_market.title()} Negative EPS Right Tail Decomposition",
        "",
        "## Setup",
        "",
        f"- Selected market: `{result.selected_market}`",
        f"- Scope: `{result.scope_name}`",
        "- Event: first FY disclosure per disclosed-year bucket",
        "- Entry: next trading session open",
        "- Exit: previous trading session close before the next FY disclosure",
        "- Decomposition: `forecast EPS > 0 / <= 0 / missing` x `OperatingCashFlow > 0 / <= 0 / missing` at the FY disclosure",
        f"- Liquidity overlay: trailing `{result.adv_window}`-session average trading value before the entry session, split by disclosed-year median",
        "- Market classification uses the current stock-master snapshot; historical market migrations are not stored in `market.duckdb`.",
        "",
        "## Event Summary",
        "",
    ]
    if result.event_summary_df.empty:
        lines.append("- No negative-EPS FY events matched the requested scope.")
    else:
        focus_groups = {
            "all_negative",
            "forecast_positive__cfo_positive",
            "forecast_positive__cfo_non_positive",
            "forecast_non_positive__cfo_positive",
            "forecast_non_positive__cfo_non_positive",
            "forecast_missing__cfo_positive",
            "forecast_missing__cfo_non_positive",
        }
        focus_filters = {"all_liquidity", "high_liquidity", "low_liquidity"}
        summary_df = result.event_summary_df[
            result.event_summary_df["group_key"].astype(str).isin(focus_groups)
            & result.event_summary_df["liquidity_filter"].astype(str).isin(focus_filters)
        ]
        for row in summary_df.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['group_key']}` / `{row['liquidity_filter']}`: "
                f"signed `{int(row['signed_event_count'])}`, "
                f"realized `{int(row['realized_event_count'])}`, "
                f"mean `{_fmt_num(row['mean_return_pct'])}%`, "
                f"median `{_fmt_num(row['median_return_pct'])}%`, "
                f"win rate `{_fmt_num(row['win_rate_pct'])}%`"
            )

    lines.extend(["", "## Portfolio Summary", ""])
    if result.portfolio_summary_df.empty:
        lines.append("- No calendar-time equal-weight portfolio could be built.")
    else:
        focus_groups = {
            "all_negative",
            "forecast_positive__cfo_positive",
            "forecast_positive__cfo_non_positive",
            "forecast_non_positive__cfo_positive",
            "forecast_non_positive__cfo_non_positive",
            "forecast_missing__cfo_positive",
            "forecast_missing__cfo_non_positive",
        }
        summary_df = result.portfolio_summary_df[
            result.portfolio_summary_df["group_key"].astype(str).isin(focus_groups)
        ]
        for row in summary_df.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['group_key']}` / `{row['liquidity_filter']}`: "
                f"events `{int(row['realized_event_count'])}`, "
                f"total return `{_fmt_num(row['total_return_pct'])}%`, "
                f"CAGR `{_fmt_num(row['cagr_pct'])}%`, "
                f"max drawdown `{_fmt_num(row['max_drawdown_pct'])}%`, "
                f"avg active `{_fmt_num(row['avg_active_positions'])}`"
            )

    lines.extend(["", "## Tail Concentration", ""])
    if result.tail_concentration_df.empty:
        lines.append("- No realized events were available for tail-concentration analysis.")
    else:
        focus_groups = {
            "all_negative",
            "forecast_positive__cfo_positive",
            "forecast_positive__cfo_non_positive",
            "forecast_non_positive__cfo_positive",
            "forecast_non_positive__cfo_non_positive",
            "forecast_missing__cfo_positive",
            "forecast_missing__cfo_non_positive",
        }
        focus_df = result.tail_concentration_df[
            result.tail_concentration_df["group_key"].astype(str).isin(focus_groups)
            & (result.tail_concentration_df["liquidity_filter"].astype(str) == "all_liquidity")
        ]
        for row in focus_df.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['group_key']}`: "
                f"p90 `{_fmt_num(row['p90_return_pct'])}%`, "
                f"p95 `{_fmt_num(row['p95_return_pct'])}%`, "
                f"max `{_fmt_num(row['max_return_pct'])}%`, "
                f"top1 gross-gain share `{_fmt_num(row['top_1_gross_gain_share_pct'])}%`, "
                f"top-decile gross-gain share `{_fmt_num(row['top_decile_gross_gain_share_pct'])}%`"
            )

    return "\n".join(lines)


def _build_published_summary(
    result: StandardNegativeEpsRightTailResult,
) -> dict[str, Any]:
    return {
        "selectedMarket": result.selected_market,
        "scopeName": result.scope_name,
        "advWindow": result.adv_window,
        "liquiditySplitMethod": result.liquidity_split_method,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "eventSummary": result.event_summary_df.to_dict(orient="records"),
        "portfolioSummary": result.portfolio_summary_df.to_dict(orient="records"),
        "tailConcentration": result.tail_concentration_df.to_dict(orient="records"),
        "liquidityThresholds": result.liquidity_thresholds_df.to_dict(orient="records"),
    }


def write_standard_negative_eps_right_tail_bundle(
    result: StandardNegativeEpsRightTailResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=_experiment_id_for_market(result.selected_market),
        module=__name__,
        function="run_standard_negative_eps_right_tail_decomposition",
        params={
            "market": result.selected_market,
            "scope": result.scope_name,
            "adv_window": result.adv_window,
            "liquidity_split_method": result.liquidity_split_method,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_standard_negative_eps_right_tail_bundle(
    bundle_path: str | Path,
) -> StandardNegativeEpsRightTailResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=StandardNegativeEpsRightTailResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_standard_negative_eps_right_tail_latest_bundle_path(
    *,
    market: str = DEFAULT_MARKET,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        _experiment_id_for_market(market),
        output_root=output_root,
    )


def get_standard_negative_eps_right_tail_bundle_path_for_run_id(
    run_id: str,
    *,
    market: str = DEFAULT_MARKET,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        _experiment_id_for_market(market),
        run_id,
        output_root=output_root,
    )
