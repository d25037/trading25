"""FY actual EPS sign to next-FY return event study for local market.duckdb."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pandas as pd

from src.shared.utils.market_code_alias import expand_market_codes, normalize_market_scope
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

EpsClassification = Literal["positive", "negative", "zero_eps", "missing_actual_eps"]
EventStatus = Literal[
    "realized",
    "excluded_zero_eps",
    "excluded_missing_actual_eps",
    "no_next_fy",
    "missing_price_history",
    "missing_entry_session",
    "missing_exit_session",
    "invalid_entry_open",
    "invalid_exit_close",
    "invalid_price_path",
    "empty_holding_window",
]

FY_EPS_SIGN_NEXT_FY_RETURN_EXPERIMENT_ID = "market-behavior/fy-eps-sign-next-fy-return"
DEFAULT_MARKETS: tuple[str, ...] = ("standard", "growth")
DEFAULT_FORECAST_RATIO_THRESHOLDS: tuple[float, ...] = (1.2, 1.4)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "classification_summary_df",
    "event_summary_df",
    "cross_summary_df",
    "cross_year_summary_df",
    "event_year_summary_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
    "event_ledger_df",
)
_EXTRA_SCOPE_ALIAS_TO_CANONICAL: dict[str, str] = {
    "topix500": "topix500",
    "primeextopix500": "primeExTopix500",
    "prime_ex_topix500": "primeExTopix500",
}
_TOPIX500_SCALE_CATEGORIES: tuple[str, ...] = (
    "TOPIX Core30",
    "TOPIX Large70",
    "TOPIX Mid400",
)
_MARKET_SCOPE_ORDER: tuple[str, ...] = (
    "all",
    "topix500",
    "standard",
    "growth",
    "prime",
    "primeExTopix500",
)
_EPS_SIGN_ORDER: tuple[str, ...] = ("positive", "negative")
_FORECAST_SIGN_ORDER: tuple[str, ...] = (
    "forecast_positive",
    "forecast_non_positive",
    "forecast_missing",
)
_CFO_SIGN_ORDER: tuple[str, ...] = (
    "cfo_positive",
    "cfo_non_positive",
    "cfo_missing",
)
_FORECAST_FILTER_ALL = "all_signed"
_CLASSIFICATION_ORDER: tuple[str, ...] = (
    "positive",
    "negative",
    "zero_eps",
    "missing_actual_eps",
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
    "excluded_zero_eps",
    "excluded_missing_actual_eps",
)


@dataclass(frozen=True)
class FyEpsSignNextFyReturnResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    selected_markets: tuple[str, ...]
    forecast_ratio_thresholds: tuple[float, ...]
    current_market_snapshot_only: bool
    entry_timing: str
    exit_timing: str
    classification_summary_df: pd.DataFrame
    event_summary_df: pd.DataFrame
    cross_summary_df: pd.DataFrame
    cross_year_summary_df: pd.DataFrame
    event_year_summary_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame
    event_ledger_df: pd.DataFrame
    uses_current_scale_category_proxy: bool = False


def _connect_duckdb(db_path: str, *, read_only: bool = True) -> Any:
    return _shared_connect_duckdb(db_path, read_only=read_only)


def _open_analysis_connection(db_path: str):
    return open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="fy-eps-sign-next-fy-",
        connect_fn=_connect_duckdb,
    )


def _normalize_selected_markets(markets: Sequence[str]) -> tuple[str, ...]:
    if not markets:
        raise ValueError("markets must not be empty")
    normalized: list[str] = []
    seen: set[str] = set()
    for market in markets:
        market_key = str(market).strip().lower()
        canonical = _EXTRA_SCOPE_ALIAS_TO_CANONICAL.get(market_key) or normalize_market_scope(
            market
        )
        if canonical is None:
            raise ValueError(f"Unsupported market: {market}")
        if canonical in seen:
            continue
        normalized.append(canonical)
        seen.add(canonical)
    if {"topix500", "primeExTopix500"} & seen and len(seen) > 1:
        raise ValueError("topix500 scopes cannot be combined with other markets in this study")
    return tuple(normalized)


def _market_query_codes(markets: Sequence[str]) -> tuple[str, ...]:
    market_inputs = [
        "prime" if str(market) in {"topix500", "primeExTopix500"} else str(market)
        for market in markets
    ]
    return tuple(expand_market_codes(market_inputs))


def _normalize_forecast_ratio_thresholds(
    thresholds: Sequence[float] | None,
) -> tuple[float, ...]:
    if thresholds is None:
        return DEFAULT_FORECAST_RATIO_THRESHOLDS
    normalized: list[float] = []
    seen: set[float] = set()
    for threshold in thresholds:
        value = float(threshold)
        if not math.isfinite(value) or value <= 0.0:
            raise ValueError("forecast_ratio_thresholds must be finite and > 0")
        if value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    if not normalized:
        return DEFAULT_FORECAST_RATIO_THRESHOLDS
    normalized.sort()
    return tuple(normalized)


def _placeholder_sql(size: int) -> str:
    if size <= 0:
        raise ValueError("placeholder size must be positive")
    return ",".join("?" for _ in range(size))


def _empty_result_df(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


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
        )
        SELECT
            code,
            company_name,
                market_code,
                market_name,
                sector_33_name,
                scale_category,
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
                "scale_category",
                "listed_date",
                "normalized_code",
                "market",
            ]
        )
    df["code"] = df["code"].astype(str)
    df["normalized_code"] = df["normalized_code"].astype(str)
    df["market_code"] = df["market_code"].astype(str)
    df["scale_category"] = df["scale_category"].fillna("").astype(str)
    df["listed_date"] = df["listed_date"].astype(str)
    df["market"] = df["market_code"].map(
        lambda value: normalize_market_scope(value, default=str(value).lower())
    )
    return df


def _filter_stock_scope(
    stock_df: pd.DataFrame,
    *,
    selected_markets: Sequence[str],
) -> pd.DataFrame:
    if stock_df.empty:
        return stock_df.copy()
    selected = tuple(selected_markets)
    if selected == ("topix500",):
        filtered = stock_df[
            (stock_df["market"] == "prime")
            & (stock_df["scale_category"].isin(_TOPIX500_SCALE_CATEGORIES))
        ].copy()
        filtered["market"] = "topix500"
        return filtered.reset_index(drop=True)
    if selected == ("primeExTopix500",):
        filtered = stock_df[
            (stock_df["market"] == "prime")
            & (~stock_df["scale_category"].isin(_TOPIX500_SCALE_CATEGORIES))
        ].copy()
        filtered["market"] = "primeExTopix500"
        return filtered.reset_index(drop=True)
    return stock_df[stock_df["market"].isin(selected)].copy().reset_index(drop=True)


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
                earnings_per_share,
                forecast_eps,
                next_year_forecast_earnings_per_share,
                operating_cash_flow,
                shares_outstanding
            FROM (
                SELECT
                    {normalized_code} AS normalized_code,
                    disclosed_date,
                    type_of_current_period,
                    earnings_per_share,
                    forecast_eps,
                    next_year_forecast_earnings_per_share,
                    operating_cash_flow,
                    shares_outstanding,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized_code}, disclosed_date
                        ORDER BY {prefer_4digit}
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
            st.earnings_per_share,
            st.forecast_eps,
            st.next_year_forecast_earnings_per_share,
            st.operating_cash_flow,
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
                "earnings_per_share",
                "forecast_eps",
                "next_year_forecast_earnings_per_share",
                "operating_cash_flow",
                "shares_outstanding",
                "market",
                "period_type",
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
                close
            FROM (
                SELECT
                    {normalized_code} AS normalized_code,
                    date,
                    open,
                    close,
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
            sd.close
        FROM stock_data_canonical sd
        JOIN stocks_canonical s
          ON s.normalized_code = sd.normalized_code
        ORDER BY s.code, sd.date
        """,
        [*[str(code).lower() for code in market_codes], start_date, end_date],
    ).fetchdf()
    if df.empty:
        return pd.DataFrame(
            columns=["code", "company_name", "market_code", "date", "open", "close", "market"]
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


def _classify_eps(value: float | None) -> EpsClassification:
    if value is None or not math.isfinite(value):
        return "missing_actual_eps"
    if math.isclose(value, 0.0, abs_tol=1e-12):
        return "zero_eps"
    if value > 0:
        return "positive"
    return "negative"


def _status_for_excluded_classification(classification: EpsClassification) -> EventStatus:
    if classification == "zero_eps":
        return "excluded_zero_eps"
    if classification == "missing_actual_eps":
        return "excluded_missing_actual_eps"
    raise ValueError(f"Unsupported excluded classification: {classification}")


def _classify_forecast_sign(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return "forecast_missing"
    if value > 0:
        return "forecast_positive"
    return "forecast_non_positive"


def _classify_cfo_sign(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return "cfo_missing"
    if value > 0:
        return "cfo_positive"
    return "cfo_non_positive"


def _forecast_filter_key(threshold: float) -> str:
    threshold_str = f"{threshold:.2f}".rstrip("0").rstrip(".").replace(".", "_")
    return f"forecast_ge_{threshold_str}x"


def _forecast_filter_categories(
    thresholds: Sequence[float],
) -> list[str]:
    return [_FORECAST_FILTER_ALL, *[_forecast_filter_key(threshold) for threshold in thresholds]]


def _expand_forecast_filter_scope(
    frame: pd.DataFrame,
    *,
    thresholds: Sequence[float],
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    expanded_frames: list[pd.DataFrame] = []

    base_df = frame.copy()
    base_df["forecast_filter"] = _FORECAST_FILTER_ALL
    base_df["forecast_ratio_threshold"] = np.nan
    expanded_frames.append(base_df)

    if "forecast_vs_actual_ratio" not in frame.columns:
        return pd.concat(expanded_frames, ignore_index=True)
    forecast_ratio = pd.to_numeric(frame["forecast_vs_actual_ratio"], errors="coerce")
    for threshold in thresholds:
        filtered_df = frame[forecast_ratio >= threshold].copy()
        if filtered_df.empty:
            continue
        filtered_df["forecast_filter"] = _forecast_filter_key(threshold)
        filtered_df["forecast_ratio_threshold"] = float(threshold)
        expanded_frames.append(filtered_df)

    return pd.concat(expanded_frames, ignore_index=True)


def _build_event_ledger(
    *,
    stock_df: pd.DataFrame,
    statement_df: pd.DataFrame,
    price_df: pd.DataFrame,
) -> pd.DataFrame:
    if stock_df.empty or statement_df.empty:
        return _empty_result_df(
            [
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
                "forecast_above_actual",
                "forecast_vs_actual_ratio",
                "forecast_sign",
                "operating_cash_flow",
                "cfo_sign",
                "classification",
                "eps_sign",
                "status",
                "entry_date",
                "exit_date",
                "entry_open",
                "entry_close",
                "exit_close",
                "event_return",
                "event_return_pct",
                "holding_trading_days",
                "holding_calendar_days",
                "entry_to_exit_price_path_rows",
            ]
        )

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
            code_statement_df[code_statement_df["period_type"] == "FY"]
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
            forecast_above_actual = (
                forecast_eps > actual_eps
                if forecast_eps is not None and actual_eps is not None
                else None
            )
            forecast_vs_actual_ratio = (
                forecast_eps / actual_eps
                if forecast_eps is not None
                and actual_eps is not None
                and math.isfinite(forecast_eps)
                and math.isfinite(actual_eps)
                and actual_eps > 0.0
                else None
            )
            classification = _classify_eps(actual_eps)
            eps_sign = (
                cast(Literal["positive", "negative"], classification)
                if classification in {"positive", "negative"}
                else None
            )
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
                "forecast_above_actual": forecast_above_actual,
                "forecast_vs_actual_ratio": forecast_vs_actual_ratio,
                "forecast_sign": forecast_sign,
                "operating_cash_flow": operating_cash_flow,
                "cfo_sign": cfo_sign,
                "classification": classification,
                "eps_sign": eps_sign,
                "status": None,
                "entry_date": None,
                "exit_date": None,
                "entry_open": None,
                "entry_close": None,
                "exit_close": None,
                "event_return": None,
                "event_return_pct": None,
                "holding_trading_days": None,
                "holding_calendar_days": None,
                "entry_to_exit_price_path_rows": None,
            }
            if classification not in {"positive", "negative"}:
                record["status"] = _status_for_excluded_classification(classification)
                records.append(record)
                continue
            if next_fy_disclosed_date is None:
                record["status"] = "no_next_fy"
                records.append(record)
                continue
            if price_frame is None or price_frame.empty:
                record["status"] = "missing_price_history"
                records.append(record)
                continue

            dates = price_frame["date"].to_numpy(dtype=str)
            entry_idx = int(np.searchsorted(dates, disclosed_date, side="right"))
            exit_idx = int(np.searchsorted(dates, next_fy_disclosed_date, side="left")) - 1
            if entry_idx >= len(price_frame):
                record["status"] = "missing_entry_session"
                records.append(record)
                continue
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
                    "exit_date": exit_date,
                    "entry_open": entry_open,
                    "entry_close": entry_close,
                    "exit_close": exit_close,
                    "event_return": event_return,
                    "event_return_pct": event_return * 100.0,
                    "holding_trading_days": int(len(path_df)),
                    "holding_calendar_days": int(holding_calendar_days),
                    "entry_to_exit_price_path_rows": int(len(path_df)),
                }
            )
            records.append(record)

    event_ledger_df = pd.DataFrame(records)
    if event_ledger_df.empty:
        return event_ledger_df

    event_ledger_df["market"] = event_ledger_df["market"].astype(str)
    event_ledger_df["disclosed_date"] = event_ledger_df["disclosed_date"].astype(str)
    event_ledger_df["disclosed_year"] = event_ledger_df["disclosed_year"].astype(str)
    event_ledger_df["status"] = event_ledger_df["status"].astype(str)
    event_ledger_df["classification"] = event_ledger_df["classification"].astype(str)
    event_ledger_df = event_ledger_df.sort_values(
        ["disclosed_date", "code"],
        kind="stable",
    ).reset_index(drop=True)
    return event_ledger_df


def _expand_market_scope(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    market_scope_df = frame.copy()
    market_scope_df["market_scope"] = market_scope_df["market"].astype(str)
    all_scope_df = frame.copy()
    all_scope_df["market_scope"] = "all"
    return pd.concat([all_scope_df, market_scope_df], ignore_index=True)


def _build_classification_summary_df(event_ledger_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["market_scope", "classification", "event_count"]
    if event_ledger_df.empty:
        return _empty_result_df(columns)

    expanded = _expand_market_scope(event_ledger_df)
    grouped = (
        expanded.groupby(["market_scope", "classification"], observed=True, sort=False)
        .size()
        .reset_index(name="event_count")
    )
    grouped["market_scope"] = pd.Categorical(
        grouped["market_scope"],
        categories=[scope for scope in _MARKET_SCOPE_ORDER if scope in set(grouped["market_scope"])],
        ordered=True,
    )
    grouped["classification"] = pd.Categorical(
        grouped["classification"],
        categories=[item for item in _CLASSIFICATION_ORDER if item in set(grouped["classification"])],
        ordered=True,
    )
    return grouped.sort_values(["market_scope", "classification"], kind="stable").reset_index(
        drop=True
    )


def _bool_ratio_pct(mask: pd.Series) -> float | None:
    if mask.empty:
        return None
    return float(mask.mean() * 100.0)


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
    raise ValueError(f"Unsupported fn: {fn}")


def _build_event_summary_df(
    event_ledger_df: pd.DataFrame,
    *,
    forecast_ratio_thresholds: Sequence[float],
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "eps_sign",
        "forecast_filter",
        "forecast_ratio_threshold",
        "signed_event_count",
        "realized_event_count",
        "realized_ratio_pct",
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
        "avg_holding_trading_days",
        "avg_holding_calendar_days",
        "mean_actual_eps",
        "mean_forecast_eps",
        "mean_forecast_vs_actual_ratio",
    ]
    if event_ledger_df.empty:
        return _empty_result_df(columns)

    signed_df = event_ledger_df[event_ledger_df["eps_sign"].isin(_EPS_SIGN_ORDER)].copy()
    if signed_df.empty:
        return _empty_result_df(columns)

    thresholds = tuple(forecast_ratio_thresholds)
    expanded = _expand_forecast_filter_scope(
        _expand_market_scope(signed_df),
        thresholds=thresholds,
    )
    records: list[dict[str, Any]] = []
    for (market_scope, eps_sign, forecast_filter), group_df in expanded.groupby(
        ["market_scope", "eps_sign", "forecast_filter"], observed=True, sort=False
    ):
        realized_df = group_df[group_df["status"] == "realized"].copy()
        forecast_ratio_threshold = (
            None
            if forecast_filter == _FORECAST_FILTER_ALL
            else float(pd.to_numeric(group_df["forecast_ratio_threshold"], errors="coerce").dropna().iloc[0])
        )
        records.append(
            {
                "market_scope": str(market_scope),
                "eps_sign": str(eps_sign),
                "forecast_filter": str(forecast_filter),
                "forecast_ratio_threshold": forecast_ratio_threshold,
                "signed_event_count": int(len(group_df)),
                "realized_event_count": int(len(realized_df)),
                "realized_ratio_pct": (
                    float(len(realized_df) / len(group_df) * 100.0) if len(group_df) else None
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
                "avg_holding_trading_days": _series_stat(
                    realized_df["holding_trading_days"], "mean"
                ),
                "avg_holding_calendar_days": _series_stat(
                    realized_df["holding_calendar_days"], "mean"
                ),
                "mean_actual_eps": _series_stat(realized_df["actual_eps"], "mean"),
                "mean_forecast_eps": _series_stat(realized_df["forecast_eps"], "mean"),
                "mean_forecast_vs_actual_ratio": _series_stat(
                    realized_df["forecast_vs_actual_ratio"], "mean"
                ),
            }
        )

    summary_df = pd.DataFrame(records)
    if summary_df.empty:
        return _empty_result_df(columns)
    summary_df["market_scope"] = pd.Categorical(
        summary_df["market_scope"],
        categories=[scope for scope in _MARKET_SCOPE_ORDER if scope in set(summary_df["market_scope"])],
        ordered=True,
    )
    summary_df["eps_sign"] = pd.Categorical(
        summary_df["eps_sign"],
        categories=list(_EPS_SIGN_ORDER),
        ordered=True,
    )
    summary_df["forecast_filter"] = pd.Categorical(
        summary_df["forecast_filter"],
        categories=_forecast_filter_categories(thresholds),
        ordered=True,
    )
    return summary_df.sort_values(
        ["market_scope", "eps_sign", "forecast_filter"],
        kind="stable",
    ).reset_index(drop=True)


def _build_cross_summary_df(event_ledger_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market_scope",
        "eps_sign",
        "forecast_sign",
        "cfo_sign",
        "signed_event_count",
        "realized_event_count",
        "realized_ratio_pct",
        "no_next_fy_count",
        "mean_return_pct",
        "median_return_pct",
        "q25_return_pct",
        "q75_return_pct",
        "win_rate_pct",
        "mean_actual_eps",
        "mean_forecast_eps",
        "mean_operating_cash_flow",
    ]
    if event_ledger_df.empty:
        return _empty_result_df(columns)

    signed_df = event_ledger_df[event_ledger_df["eps_sign"].isin(_EPS_SIGN_ORDER)].copy()
    if signed_df.empty:
        return _empty_result_df(columns)

    expanded = _expand_market_scope(signed_df)
    records: list[dict[str, Any]] = []
    for (market_scope, eps_sign, forecast_sign, cfo_sign), group_df in expanded.groupby(
        ["market_scope", "eps_sign", "forecast_sign", "cfo_sign"],
        observed=True,
        sort=False,
    ):
        realized_df = group_df[group_df["status"] == "realized"].copy()
        records.append(
            {
                "market_scope": str(market_scope),
                "eps_sign": str(eps_sign),
                "forecast_sign": str(forecast_sign),
                "cfo_sign": str(cfo_sign),
                "signed_event_count": int(len(group_df)),
                "realized_event_count": int(len(realized_df)),
                "realized_ratio_pct": (
                    float(len(realized_df) / len(group_df) * 100.0) if len(group_df) else None
                ),
                "no_next_fy_count": int((group_df["status"] == "no_next_fy").sum()),
                "mean_return_pct": _series_stat(realized_df["event_return_pct"], "mean"),
                "median_return_pct": _series_stat(realized_df["event_return_pct"], "median"),
                "q25_return_pct": _series_stat(realized_df["event_return_pct"], "q25"),
                "q75_return_pct": _series_stat(realized_df["event_return_pct"], "q75"),
                "win_rate_pct": _bool_ratio_pct(realized_df["event_return"] > 0),
                "mean_actual_eps": _series_stat(realized_df["actual_eps"], "mean"),
                "mean_forecast_eps": _series_stat(realized_df["forecast_eps"], "mean"),
                "mean_operating_cash_flow": _series_stat(
                    realized_df["operating_cash_flow"],
                    "mean",
                ),
            }
        )

    summary_df = pd.DataFrame(records)
    if summary_df.empty:
        return _empty_result_df(columns)
    summary_df["market_scope"] = pd.Categorical(
        summary_df["market_scope"],
        categories=[scope for scope in _MARKET_SCOPE_ORDER if scope in set(summary_df["market_scope"])],
        ordered=True,
    )
    summary_df["eps_sign"] = pd.Categorical(
        summary_df["eps_sign"],
        categories=list(_EPS_SIGN_ORDER),
        ordered=True,
    )
    summary_df["forecast_sign"] = pd.Categorical(
        summary_df["forecast_sign"],
        categories=list(_FORECAST_SIGN_ORDER),
        ordered=True,
    )
    summary_df["cfo_sign"] = pd.Categorical(
        summary_df["cfo_sign"],
        categories=list(_CFO_SIGN_ORDER),
        ordered=True,
    )
    return summary_df.sort_values(
        ["market_scope", "eps_sign", "forecast_sign", "cfo_sign"],
        kind="stable",
    ).reset_index(drop=True)


def _build_cross_year_summary_df(event_ledger_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "market_scope",
        "eps_sign",
        "forecast_sign",
        "cfo_sign",
        "disclosed_year",
        "signed_event_count",
        "realized_event_count",
        "mean_return_pct",
        "median_return_pct",
        "win_rate_pct",
    ]
    if event_ledger_df.empty:
        return _empty_result_df(columns)

    signed_df = event_ledger_df[event_ledger_df["eps_sign"].isin(_EPS_SIGN_ORDER)].copy()
    if signed_df.empty:
        return _empty_result_df(columns)

    expanded = _expand_market_scope(signed_df)
    records: list[dict[str, Any]] = []
    for (
        market_scope,
        eps_sign,
        forecast_sign,
        cfo_sign,
        disclosed_year,
    ), group_df in expanded.groupby(
        ["market_scope", "eps_sign", "forecast_sign", "cfo_sign", "disclosed_year"],
        observed=True,
        sort=False,
    ):
        realized_df = group_df[group_df["status"] == "realized"].copy()
        records.append(
            {
                "market_scope": str(market_scope),
                "eps_sign": str(eps_sign),
                "forecast_sign": str(forecast_sign),
                "cfo_sign": str(cfo_sign),
                "disclosed_year": str(disclosed_year),
                "signed_event_count": int(len(group_df)),
                "realized_event_count": int(len(realized_df)),
                "mean_return_pct": _series_stat(realized_df["event_return_pct"], "mean"),
                "median_return_pct": _series_stat(realized_df["event_return_pct"], "median"),
                "win_rate_pct": _bool_ratio_pct(realized_df["event_return"] > 0),
            }
        )

    summary_df = pd.DataFrame(records)
    if summary_df.empty:
        return _empty_result_df(columns)
    summary_df["market_scope"] = pd.Categorical(
        summary_df["market_scope"],
        categories=[scope for scope in _MARKET_SCOPE_ORDER if scope in set(summary_df["market_scope"])],
        ordered=True,
    )
    summary_df["eps_sign"] = pd.Categorical(
        summary_df["eps_sign"],
        categories=list(_EPS_SIGN_ORDER),
        ordered=True,
    )
    summary_df["forecast_sign"] = pd.Categorical(
        summary_df["forecast_sign"],
        categories=list(_FORECAST_SIGN_ORDER),
        ordered=True,
    )
    summary_df["cfo_sign"] = pd.Categorical(
        summary_df["cfo_sign"],
        categories=list(_CFO_SIGN_ORDER),
        ordered=True,
    )
    return summary_df.sort_values(
        ["market_scope", "eps_sign", "forecast_sign", "cfo_sign", "disclosed_year"],
        kind="stable",
    ).reset_index(drop=True)


def _build_event_year_summary_df(
    event_ledger_df: pd.DataFrame,
    *,
    forecast_ratio_thresholds: Sequence[float],
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "eps_sign",
        "forecast_filter",
        "forecast_ratio_threshold",
        "disclosed_year",
        "realized_event_count",
        "mean_return_pct",
        "median_return_pct",
        "win_rate_pct",
    ]
    if event_ledger_df.empty:
        return _empty_result_df(columns)

    realized_df = event_ledger_df[event_ledger_df["status"] == "realized"].copy()
    realized_df = realized_df[realized_df["eps_sign"].isin(_EPS_SIGN_ORDER)].copy()
    if realized_df.empty:
        return _empty_result_df(columns)

    thresholds = tuple(forecast_ratio_thresholds)
    expanded = _expand_forecast_filter_scope(
        _expand_market_scope(realized_df),
        thresholds=thresholds,
    )
    records: list[dict[str, Any]] = []
    for (market_scope, eps_sign, forecast_filter, disclosed_year), group_df in expanded.groupby(
        ["market_scope", "eps_sign", "forecast_filter", "disclosed_year"],
        observed=True,
        sort=False,
    ):
        forecast_ratio_threshold = (
            None
            if forecast_filter == _FORECAST_FILTER_ALL
            else float(pd.to_numeric(group_df["forecast_ratio_threshold"], errors="coerce").dropna().iloc[0])
        )
        records.append(
            {
                "market_scope": str(market_scope),
                "eps_sign": str(eps_sign),
                "forecast_filter": str(forecast_filter),
                "forecast_ratio_threshold": forecast_ratio_threshold,
                "disclosed_year": str(disclosed_year),
                "realized_event_count": int(len(group_df)),
                "mean_return_pct": _series_stat(group_df["event_return_pct"], "mean"),
                "median_return_pct": _series_stat(group_df["event_return_pct"], "median"),
                "win_rate_pct": _bool_ratio_pct(group_df["event_return"] > 0),
            }
        )

    summary_df = pd.DataFrame(records)
    if summary_df.empty:
        return _empty_result_df(columns)
    summary_df["market_scope"] = pd.Categorical(
        summary_df["market_scope"],
        categories=[scope for scope in _MARKET_SCOPE_ORDER if scope in set(summary_df["market_scope"])],
        ordered=True,
    )
    summary_df["eps_sign"] = pd.Categorical(
        summary_df["eps_sign"],
        categories=list(_EPS_SIGN_ORDER),
        ordered=True,
    )
    summary_df["forecast_filter"] = pd.Categorical(
        summary_df["forecast_filter"],
        categories=_forecast_filter_categories(thresholds),
        ordered=True,
    )
    return summary_df.sort_values(
        ["market_scope", "eps_sign", "forecast_filter", "disclosed_year"],
        kind="stable",
    ).reset_index(drop=True)


def _build_portfolio_daily_df(
    *,
    event_ledger_df: pd.DataFrame,
    price_df: pd.DataFrame,
    forecast_ratio_thresholds: Sequence[float],
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "eps_sign",
        "forecast_filter",
        "forecast_ratio_threshold",
        "date",
        "active_positions",
        "mean_daily_return",
        "mean_daily_return_pct",
        "portfolio_value",
        "drawdown_pct",
    ]
    realized_df = event_ledger_df[event_ledger_df["status"] == "realized"].copy()
    realized_df = realized_df[realized_df["eps_sign"].isin(_EPS_SIGN_ORDER)].copy()
    if realized_df.empty or price_df.empty:
        return _empty_result_df(columns)

    realized_df = _expand_forecast_filter_scope(
        realized_df,
        thresholds=forecast_ratio_thresholds,
    )

    price_by_code: dict[str, pd.DataFrame] = {
        str(code): frame.reset_index(drop=True)
        for code, frame in price_df.groupby("code", sort=False)
    }
    aggregate: dict[tuple[str, str, str, str], list[float]] = {}

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
        for market_scope in ("all", str(event.market)):
            for date_value, daily_return in zip(
                path_df["date"].astype(str),
                daily_returns,
                strict=True,
            ):
                key = (
                    market_scope,
                    str(event.eps_sign),
                    str(event.forecast_filter),
                    str(date_value),
                )
                bucket = aggregate.setdefault(key, [0.0, 0.0])
                bucket[0] += float(daily_return)
                bucket[1] += 1.0

    if not aggregate:
        return _empty_result_df(columns)

    records = [
        {
            "market_scope": market_scope,
            "eps_sign": eps_sign,
            "forecast_filter": forecast_filter,
            "forecast_ratio_threshold": (
                np.nan
                if forecast_filter == _FORECAST_FILTER_ALL
                else float(
                    next(
                        threshold
                        for threshold in forecast_ratio_thresholds
                        if _forecast_filter_key(threshold) == forecast_filter
                    )
                )
            ),
            "date": date_value,
            "active_positions": int(values[1]),
            "mean_daily_return": float(values[0] / values[1]),
        }
        for (market_scope, eps_sign, forecast_filter, date_value), values in aggregate.items()
    ]
    portfolio_daily_df = pd.DataFrame(records)
    portfolio_daily_df["mean_daily_return_pct"] = (
        portfolio_daily_df["mean_daily_return"] * 100.0
    )
    portfolio_daily_df["market_scope"] = pd.Categorical(
        portfolio_daily_df["market_scope"],
        categories=[scope for scope in _MARKET_SCOPE_ORDER if scope in set(portfolio_daily_df["market_scope"])],
        ordered=True,
    )
    portfolio_daily_df["eps_sign"] = pd.Categorical(
        portfolio_daily_df["eps_sign"],
        categories=list(_EPS_SIGN_ORDER),
        ordered=True,
    )
    portfolio_daily_df["forecast_filter"] = pd.Categorical(
        portfolio_daily_df["forecast_filter"],
        categories=_forecast_filter_categories(forecast_ratio_thresholds),
        ordered=True,
    )
    portfolio_daily_df = portfolio_daily_df.sort_values(
        ["market_scope", "eps_sign", "forecast_filter", "date"],
        kind="stable",
    ).reset_index(drop=True)

    portfolio_daily_df["portfolio_value"] = np.nan
    portfolio_daily_df["drawdown_pct"] = np.nan
    for _, grouped_df in portfolio_daily_df.groupby(
        ["market_scope", "eps_sign", "forecast_filter"],
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
    forecast_ratio_thresholds: Sequence[float],
) -> pd.DataFrame:
    columns = [
        "market_scope",
        "eps_sign",
        "forecast_filter",
        "forecast_ratio_threshold",
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

    realized_df = event_ledger_df[event_ledger_df["status"] == "realized"].copy()
    realized_df = realized_df[realized_df["eps_sign"].isin(_EPS_SIGN_ORDER)].copy()
    realized_df = _expand_forecast_filter_scope(
        _expand_market_scope(realized_df),
        thresholds=forecast_ratio_thresholds,
    )
    realized_counts = (
        realized_df.groupby(
            ["market_scope", "eps_sign", "forecast_filter"], observed=True, sort=False
        )
        .size()
        .to_dict()
    )

    records: list[dict[str, Any]] = []
    for (market_scope, eps_sign, forecast_filter), group_df in portfolio_daily_df.groupby(
        ["market_scope", "eps_sign", "forecast_filter"], observed=True, sort=False
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
                "market_scope": str(market_scope),
                "eps_sign": str(eps_sign),
                "forecast_filter": str(forecast_filter),
                "forecast_ratio_threshold": (
                    None
                    if forecast_filter == _FORECAST_FILTER_ALL
                    else float(
                        next(
                            threshold
                            for threshold in forecast_ratio_thresholds
                            if _forecast_filter_key(threshold) == str(forecast_filter)
                        )
                    )
                ),
                "realized_event_count": int(
                    realized_counts.get(
                        (str(market_scope), str(eps_sign), str(forecast_filter)),
                        0,
                    )
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
    summary_df["market_scope"] = pd.Categorical(
        summary_df["market_scope"],
        categories=[scope for scope in _MARKET_SCOPE_ORDER if scope in set(summary_df["market_scope"])],
        ordered=True,
    )
    summary_df["eps_sign"] = pd.Categorical(
        summary_df["eps_sign"],
        categories=list(_EPS_SIGN_ORDER),
        ordered=True,
    )
    summary_df["forecast_filter"] = pd.Categorical(
        summary_df["forecast_filter"],
        categories=_forecast_filter_categories(forecast_ratio_thresholds),
        ordered=True,
    )
    return summary_df.sort_values(
        ["market_scope", "eps_sign", "forecast_filter"],
        kind="stable",
    ).reset_index(drop=True)


def run_fy_eps_sign_next_fy_return(
    db_path: str,
    *,
    markets: Sequence[str] = DEFAULT_MARKETS,
    forecast_ratio_thresholds: Sequence[float] | None = None,
) -> FyEpsSignNextFyReturnResult:
    normalized_markets = _normalize_selected_markets(markets)
    normalized_forecast_ratio_thresholds = _normalize_forecast_ratio_thresholds(
        forecast_ratio_thresholds
    )
    uses_current_scale_category_proxy = normalized_markets in {
        ("topix500",),
        ("primeExTopix500",),
    }
    market_codes = _market_query_codes(normalized_markets)

    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        available_start_date, available_end_date = _fetch_date_range(
            conn,
            table_name="stock_data",
        )
        stock_df = _filter_stock_scope(
            _query_canonical_stocks(conn, market_codes=market_codes),
            selected_markets=normalized_markets,
        )
        allowed_codes = set(stock_df["code"].astype(str))
        statement_df = _query_statement_rows(conn, market_codes=market_codes)
        if allowed_codes:
            statement_df = statement_df[statement_df["code"].astype(str).isin(allowed_codes)].copy()
        if statement_df.empty:
            empty_df = _empty_result_df([])
            return FyEpsSignNextFyReturnResult(
                db_path=db_path,
                source_mode=ctx.source_mode,
                source_detail=ctx.source_detail,
                available_start_date=available_start_date,
                available_end_date=available_end_date,
                analysis_start_date=None,
                analysis_end_date=None,
                selected_markets=normalized_markets,
                forecast_ratio_thresholds=normalized_forecast_ratio_thresholds,
                current_market_snapshot_only=True,
                entry_timing="next_session_open",
                exit_timing="previous_session_close_before_next_fy",
                classification_summary_df=empty_df.copy(),
                event_summary_df=empty_df.copy(),
                cross_summary_df=empty_df.copy(),
                cross_year_summary_df=empty_df.copy(),
                event_year_summary_df=empty_df.copy(),
                portfolio_daily_df=empty_df.copy(),
                portfolio_summary_df=empty_df.copy(),
                event_ledger_df=empty_df.copy(),
                uses_current_scale_category_proxy=uses_current_scale_category_proxy,
            )

        signed_candidate_df = statement_df[statement_df["period_type"] == "FY"].copy()
        analysis_start_date = (
            str(signed_candidate_df["disclosed_date"].min()) if not signed_candidate_df.empty else None
        )
        analysis_end_date = (
            str(signed_candidate_df["disclosed_date"].max()) if not signed_candidate_df.empty else None
        )

        price_start_date = analysis_start_date or available_start_date
        max_next_fy_date = None
        if not statement_df.empty:
            fy_anchor_df = (
                statement_df[statement_df["period_type"] == "FY"]
                .sort_values(["code", "disclosed_date"], kind="stable")
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
        if not price_df.empty and allowed_codes:
            price_df = price_df[price_df["code"].astype(str).isin(allowed_codes)].copy()
            price_df["market"] = price_df["code"].map(
                stock_df.set_index("code")["market"].to_dict()
            ).fillna(price_df["market"])

    event_ledger_df = _build_event_ledger(
        stock_df=stock_df,
        statement_df=statement_df,
        price_df=price_df,
    )
    classification_summary_df = _build_classification_summary_df(event_ledger_df)
    event_summary_df = _build_event_summary_df(
        event_ledger_df,
        forecast_ratio_thresholds=normalized_forecast_ratio_thresholds,
    )
    cross_summary_df = _build_cross_summary_df(event_ledger_df)
    cross_year_summary_df = _build_cross_year_summary_df(event_ledger_df)
    event_year_summary_df = _build_event_year_summary_df(
        event_ledger_df,
        forecast_ratio_thresholds=normalized_forecast_ratio_thresholds,
    )
    portfolio_daily_df = _build_portfolio_daily_df(
        event_ledger_df=event_ledger_df,
        price_df=price_df,
        forecast_ratio_thresholds=normalized_forecast_ratio_thresholds,
    )
    portfolio_summary_df = _build_portfolio_summary_df(
        portfolio_daily_df=portfolio_daily_df,
        event_ledger_df=event_ledger_df,
        forecast_ratio_thresholds=normalized_forecast_ratio_thresholds,
    )

    realized_df = event_ledger_df[event_ledger_df["status"] == "realized"]
    if not realized_df.empty:
        analysis_start_date = str(realized_df["disclosed_date"].min())
        analysis_end_date = str(realized_df["exit_date"].max())

    return FyEpsSignNextFyReturnResult(
        db_path=db_path,
        source_mode=ctx.source_mode if "ctx" in locals() else "live",
        source_detail=ctx.source_detail if "ctx" in locals() else f"live DuckDB: {db_path}",
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        selected_markets=normalized_markets,
        forecast_ratio_thresholds=normalized_forecast_ratio_thresholds,
        current_market_snapshot_only=True,
        entry_timing="next_session_open",
        exit_timing="previous_session_close_before_next_fy",
        classification_summary_df=classification_summary_df,
        event_summary_df=event_summary_df,
        cross_summary_df=cross_summary_df,
        cross_year_summary_df=cross_year_summary_df,
        event_year_summary_df=event_year_summary_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_summary_df=portfolio_summary_df,
        event_ledger_df=event_ledger_df,
        uses_current_scale_category_proxy=uses_current_scale_category_proxy,
    )


def _fmt_num(value: float | int | None, digits: int = 1) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "-"
    if isinstance(value, int):
        return f"{value}"
    return f"{value:.{digits}f}"


def _build_summary_markdown(result: FyEpsSignNextFyReturnResult) -> str:
    forecast_ratio_thresholds = result.forecast_ratio_thresholds
    lines = [
        "# FY EPS Sign To Next FY Return",
        "",
        "## Setup",
        "",
        f"- Scope: `{', '.join(result.selected_markets)}`",
        "- Event: first FY disclosure per disclosed-year bucket",
        "- EPS classification: share-adjusted FY actual EPS at the FY disclosure date",
        f"- Forecast overlays: `{', '.join(f'{threshold:.1f}x' for threshold in forecast_ratio_thresholds)}` on `forecast EPS / actual EPS`, applied only when `actual EPS > 0` and forecast EPS is available",
        "- Entry: next trading session open",
        "- Exit: previous trading session close before the next FY disclosure",
        "- Market classification uses the current stock-master snapshot; historical market migrations are not stored in `market.duckdb`.",
        "",
        "## Classification Counts",
        "",
    ]
    if result.uses_current_scale_category_proxy:
        lines.insert(
            9,
            "- `topix500` / `primeExTopix500` use the latest `stocks.scale_category` snapshot; this is a current-universe retrospective proxy, not a historical committee reconstruction.",
        )
    if result.classification_summary_df.empty:
        lines.append("- No FY events matched the requested scope.")
    else:
        for row in result.classification_summary_df.to_dict(orient="records"):
            lines.append(
                f"- `{row['market_scope']}` / `{row['classification']}`: `{int(cast(int, row['event_count']))}` events"
            )

    lines.extend(["", "## Event Summary", ""])
    if result.event_summary_df.empty:
        lines.append("- No realized positive/negative EPS events were available.")
    else:
        for row in result.event_summary_df.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['market_scope']}` / `{row['eps_sign']}` / `{row['forecast_filter']}`: "
                f"signed `{int(cast(int, row['signed_event_count']))}`, "
                f"realized `{int(cast(int, row['realized_event_count']))}`, "
                f"mean `{_fmt_num(cast(float | int | None, row['mean_return_pct']))}%`, "
                f"median `{_fmt_num(cast(float | int | None, row['median_return_pct']))}%`, "
                f"win rate `{_fmt_num(cast(float | int | None, row['win_rate_pct']))}%`"
            )

    lines.extend(["", "## EPS x Forecast x CFO", ""])
    if result.cross_summary_df.empty:
        lines.append("- No EPS/forecast/CFO cross summary was available.")
    else:
        for row in result.cross_summary_df.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['market_scope']}` / `{row['eps_sign']}` / `{row['forecast_sign']}` / `{row['cfo_sign']}`: "
                f"signed `{int(cast(int, row['signed_event_count']))}`, "
                f"realized `{int(cast(int, row['realized_event_count']))}`, "
                f"mean `{_fmt_num(cast(float | int | None, row['mean_return_pct']))}%`, "
                f"median `{_fmt_num(cast(float | int | None, row['median_return_pct']))}%`, "
                f"win rate `{_fmt_num(cast(float | int | None, row['win_rate_pct']))}%`"
            )

    lines.extend(["", "## Portfolio Summary", ""])
    if result.portfolio_summary_df.empty:
        lines.append("- No calendar-time equal-weight portfolio could be built.")
    else:
        for row in result.portfolio_summary_df.to_dict(orient="records"):
            lines.append(
                "- "
                f"`{row['market_scope']}` / `{row['eps_sign']}` / `{row['forecast_filter']}`: "
                f"events `{int(cast(int, row['realized_event_count']))}`, "
                f"total return `{_fmt_num(cast(float | int | None, row['total_return_pct']))}%`, "
                f"CAGR `{_fmt_num(cast(float | int | None, row['cagr_pct']))}%`, "
                f"max drawdown `{_fmt_num(cast(float | int | None, row['max_drawdown_pct']))}%`, "
                f"avg active `{_fmt_num(cast(float | int | None, row['avg_active_positions']))}`"
            )

    return "\n".join(lines)


def _build_published_summary(result: FyEpsSignNextFyReturnResult) -> dict[str, Any]:
    return {
        "selectedMarkets": list(result.selected_markets),
        "forecastRatioThresholds": list(result.forecast_ratio_thresholds),
        "usesCurrentScaleCategoryProxy": result.uses_current_scale_category_proxy,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "entryTiming": result.entry_timing,
        "exitTiming": result.exit_timing,
        "eventSummary": result.event_summary_df.to_dict(orient="records"),
        "crossSummary": result.cross_summary_df.to_dict(orient="records"),
        "crossYearSummary": result.cross_year_summary_df.to_dict(orient="records"),
        "portfolioSummary": result.portfolio_summary_df.to_dict(orient="records"),
    }


def write_fy_eps_sign_next_fy_return_bundle(
    result: FyEpsSignNextFyReturnResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=FY_EPS_SIGN_NEXT_FY_RETURN_EXPERIMENT_ID,
        module=__name__,
        function="run_fy_eps_sign_next_fy_return",
        params={
            "markets": list(result.selected_markets),
            "forecast_ratio_thresholds": list(result.forecast_ratio_thresholds),
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_fy_eps_sign_next_fy_return_bundle(
    bundle_path: str | Path,
) -> FyEpsSignNextFyReturnResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=FyEpsSignNextFyReturnResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_fy_eps_sign_next_fy_return_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        FY_EPS_SIGN_NEXT_FY_RETURN_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_fy_eps_sign_next_fy_return_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        FY_EPS_SIGN_NEXT_FY_RETURN_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
