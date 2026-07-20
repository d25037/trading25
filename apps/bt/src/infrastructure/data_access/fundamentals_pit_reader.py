"""Canonical, fail-closed Fundamentals point-in-time snapshot resolver."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import Any

import pandas as pd

from src.application.contracts.fundamentals_pit import (
    FundamentalsPitSnapshot,
    FundamentalsPitSnapshotError,
)
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.infrastructure.external_api.dataset.helpers import (
    convert_dated_response,
    convert_ohlcv_response,
)
from src.infrastructure.external_api.jquants_client import StockInfo
from src.shared.utils.market_code_alias import expand_market_codes


_NORMALIZED_CODE_SQL = """
CASE
    WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
    THEN left(code, length(code) - 1)
    ELSE code
END
"""

_STATEMENT_FIELD_MAP = {
    "code": "code",
    "disclosedDate": "disclosed_date",
    "earningsPerShare": "earnings_per_share",
    "profit": "profit",
    "equity": "equity",
    "typeOfCurrentPeriod": "type_of_current_period",
    "typeOfDocument": "type_of_document",
    "nextYearForecastEarningsPerShare": "next_year_forecast_earnings_per_share",
    "bps": "bps",
    "sales": "sales",
    "forecastSales": "forecast_sales",
    "nextYearForecastSales": "next_year_forecast_sales",
    "operatingProfit": "operating_profit",
    "forecastOperatingProfit": "forecast_operating_profit",
    "nextYearForecastOperatingProfit": "next_year_forecast_operating_profit",
    "ordinaryProfit": "ordinary_profit",
    "operatingCashFlow": "operating_cash_flow",
    "dividendFY": "dividend_fy",
    "forecastDividendFY": "forecast_dividend_fy",
    "nextYearForecastDividendFY": "next_year_forecast_dividend_fy",
    "payoutRatio": "payout_ratio",
    "forecastPayoutRatio": "forecast_payout_ratio",
    "nextYearForecastPayoutRatio": "next_year_forecast_payout_ratio",
    "forecastEps": "forecast_eps",
    "investingCashFlow": "investing_cash_flow",
    "financingCashFlow": "financing_cash_flow",
    "cashAndEquivalents": "cash_and_equivalents",
    "totalAssets": "total_assets",
    "sharesOutstanding": "shares_outstanding",
    "treasuryShares": "treasury_shares",
}


def _records(rows: list[Any]) -> list[dict[str, Any]]:
    return [dict(row.items()) for row in rows]


def _as_date(value: object, *, field: str) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise FundamentalsPitSnapshotError(
            "pit_snapshot_inconsistent", f"invalid {field}: {value!r}"
        ) from exc


def _cutoff_timestamp(cutoff: date) -> str:
    return f"{cutoff.isoformat()}T23:59:59.999999+09:00"


def _resolve_provider_windows(
    reader: MarketDbReader,
    codes: Sequence[str],
    effective_market_date: date,
) -> dict[str, dict[str, Any]]:
    normalized_codes = sorted({normalize_stock_code(code) for code in codes if code})
    if not normalized_codes:
        return {}
    placeholders = ",".join("?" for _ in normalized_codes)
    rows = _records(
        reader.query(
            f"""
            SELECT code, coverage_start, coverage_end, provider_as_of,
                   source_fingerprint, updated_at
            FROM stock_provider_windows
            WHERE code IN ({placeholders})
            ORDER BY code
            """,
            tuple(normalized_codes),
        )
    )
    by_code = {normalize_stock_code(str(row["code"])): row for row in rows}
    missing = [code for code in normalized_codes if code not in by_code]
    if missing:
        raise FundamentalsPitSnapshotError(
            "provider_window_required",
            "market_db_sync recovery required: provider window is unavailable for "
            + ", ".join(missing),
        )

    pending = _records(
        reader.query(
            f"""
            SELECT DISTINCT {_NORMALIZED_CODE_SQL} AS code
            FROM current_basis_recompute_pending
            WHERE {_NORMALIZED_CODE_SQL} IN ({placeholders})
            ORDER BY code
            """,
            tuple(normalized_codes),
        )
    )
    if pending:
        pending_codes = ", ".join(str(row["code"]) for row in pending)
        raise FundamentalsPitSnapshotError(
            "current_adjusted_metrics_required",
            "market_db_sync recovery required: current-basis recompute is pending for "
            + pending_codes,
        )

    for code, window in by_code.items():
        coverage_start = _as_date(window["coverage_start"], field="provider coverage_start")
        coverage_end = _as_date(window["coverage_end"], field="provider coverage_end")
        if (
            coverage_start > effective_market_date
            or coverage_end < effective_market_date
            or coverage_start > coverage_end
            or not str(window["provider_as_of"] or "").strip()
            or not str(window["source_fingerprint"] or "").strip()
        ):
            raise FundamentalsPitSnapshotError(
                "provider_window_required",
                f"market_db_sync recovery required: provider window for {code} "
                f"does not cover {effective_market_date}",
            )
    return by_code


def _resolve_master(
    reader: MarketDbReader, code: str, effective_market_date: date
) -> tuple[StockInfo, dict[str, Any]]:
    value = effective_market_date.isoformat()
    count = reader.query_one(
        "SELECT COUNT(*) AS count FROM stock_master_daily WHERE date = ?", (value,)
    )
    if count is None or int(count["count"]) == 0:
        raise FundamentalsPitSnapshotError(
            "stock_master_snapshot_required",
            f"stock master snapshot is unavailable at {effective_market_date}",
        )
    row = reader.query_one(
        f"""
        SELECT date, code, company_name, company_name_english, market_code,
               market_name, sector_17_code, sector_17_name, sector_33_code,
               sector_33_name, scale_category, listed_date
        FROM stock_master_daily
        WHERE date = ? AND {_NORMALIZED_CODE_SQL} = ?
        ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
        LIMIT 1
        """,
        (value, normalize_stock_code(code)),
    )
    if row is None:
        raise FundamentalsPitSnapshotError(
            "stock_not_listed_as_of",
            f"{code} is not listed in the stock master at {effective_market_date}",
        )
    record = dict(row.items())
    return (
        StockInfo(
            code=str(record["code"]),
            companyName=str(record["company_name"]),
            companyNameEnglish=str(record["company_name_english"] or ""),
            marketCode=str(record["market_code"]),
            marketName=str(record["market_name"]),
            sector17Code=str(record["sector_17_code"]),
            sector17Name=str(record["sector_17_name"]),
            sector33Code=str(record["sector_33_code"]),
            sector33Name=str(record["sector_33_name"]),
            scaleCategory=str(record["scale_category"] or ""),
            listedDate=str(record["listed_date"] or ""),
        ),
        record,
    )


def _load_statement_rows(
    reader: MarketDbReader,
    codes: Sequence[str],
    knowledge_cutoff_date: date | None,
) -> list[dict[str, Any]]:
    normalized_codes = sorted({normalize_stock_code(code) for code in codes if code})
    if not normalized_codes:
        return []
    placeholders = ",".join("?" for _ in normalized_codes)
    cutoff_clause = " AND disclosed_at <= ?" if knowledge_cutoff_date else ""
    params: tuple[object, ...] = tuple(normalized_codes)
    if knowledge_cutoff_date is not None:
        params = (*params, _cutoff_timestamp(knowledge_cutoff_date))
    return _records(
        reader.query(
            f"""
            WITH ranked AS (
                SELECT *, {_NORMALIZED_CODE_SQL} AS normalized_code,
                       ROW_NUMBER() OVER (
                           PARTITION BY {_NORMALIZED_CODE_SQL}, statement_id
                           ORDER BY CASE WHEN code = {_NORMALIZED_CODE_SQL} THEN 0 ELSE 1 END,
                                    length(code), code
                       ) AS rn
                FROM statements
                WHERE {_NORMALIZED_CODE_SQL} IN ({placeholders})
                  {cutoff_clause}
            )
            SELECT * EXCLUDE (rn, code), normalized_code AS code
            FROM ranked WHERE rn = 1
            ORDER BY normalized_code, disclosed_at, statement_id
            """,
            params,
        )
    )


def _load_metric_rows(
    reader: MarketDbReader,
    codes: Sequence[str],
    knowledge_cutoff_date: date | None,
) -> list[dict[str, Any]]:
    normalized_codes = sorted({normalize_stock_code(code) for code in codes if code})
    if not normalized_codes:
        return []
    placeholders = ",".join("?" for _ in normalized_codes)
    cutoff_clause = " AND disclosed_at <= ?" if knowledge_cutoff_date else ""
    params: tuple[object, ...] = tuple(normalized_codes)
    if knowledge_cutoff_date is not None:
        params = (*params, _cutoff_timestamp(knowledge_cutoff_date))
    return _records(
        reader.query(
            f"""
            WITH ranked AS (
                SELECT *, {_NORMALIZED_CODE_SQL} AS normalized_code,
                       ROW_NUMBER() OVER (
                           PARTITION BY {_NORMALIZED_CODE_SQL}, statement_id
                           ORDER BY CASE WHEN code = {_NORMALIZED_CODE_SQL} THEN 0 ELSE 1 END,
                                    length(code), code
                       ) AS rn
                FROM statement_metrics_adjusted
                WHERE {_NORMALIZED_CODE_SQL} IN ({placeholders})
                  {cutoff_clause}
            )
            SELECT * EXCLUDE (rn, code), normalized_code AS code
            FROM ranked WHERE rn = 1
            ORDER BY normalized_code, disclosed_at, statement_id
            """,
            params,
        )
    )


def _validate_current_fundamentals_state(
    reader: MarketDbReader,
    codes: Sequence[str],
    windows: dict[str, dict[str, Any]],
) -> None:
    normalized_codes = sorted({normalize_stock_code(code) for code in codes if code})
    if not normalized_codes:
        return
    placeholders = ",".join("?" for _ in normalized_codes)
    state_rows = _records(
        reader.query(
            f"""
            SELECT code, fundamentals_adjustment_basis_date, source_fingerprint,
                   statement_count, materialized_at
            FROM current_basis_fundamentals_state
            WHERE code IN ({placeholders})
            ORDER BY code
            """,
            tuple(normalized_codes),
        )
    )
    state_by_code = {
        normalize_stock_code(str(row["code"])): row for row in state_rows
    }
    raw_rows = _load_statement_rows(reader, normalized_codes, None)
    metric_rows = _load_metric_rows(reader, normalized_codes, None)
    raw_by_key = {
        (normalize_stock_code(str(row["code"])), str(row["statement_id"])): row
        for row in raw_rows
    }
    metric_by_key = {
        (normalize_stock_code(str(row["code"])), str(row["statement_id"])): row
        for row in metric_rows
    }

    inconsistent = False
    for code in normalized_codes:
        state = state_by_code.get(code)
        raw_keys = {key for key in raw_by_key if key[0] == code}
        metric_keys = {key for key in metric_by_key if key[0] == code}
        if state is None:
            inconsistent = True
            continue
        basis_date = str(windows[code]["coverage_end"])
        state_fingerprint = str(state["source_fingerprint"] or "")
        inconsistent = inconsistent or (
            str(state["fundamentals_adjustment_basis_date"]) != basis_date
            or not state_fingerprint.strip()
            or not str(state["materialized_at"] or "").strip()
            or int(state["statement_count"]) != len(raw_keys)
            or int(state["statement_count"]) != len(metric_keys)
            or raw_keys != metric_keys
        )
        for key in raw_keys & metric_keys:
            source = raw_by_key[key]
            metric = metric_by_key[key]
            inconsistent = inconsistent or (
                str(metric["fundamentals_adjustment_basis_date"]) != basis_date
                or str(metric["source_fingerprint"] or "") != state_fingerprint
                or str(metric["disclosed_date"]) != str(source["disclosed_date"])
                or str(metric["disclosed_at"]) != str(source["disclosed_at"])
                or str(metric["period_end"]) != str(source["period_end"])
                or str(metric["period_type"] or "").upper()
                != str(source["type_of_current_period"] or "").upper()
            )
    if inconsistent:
        raise FundamentalsPitSnapshotError(
            "current_adjusted_metrics_required",
            "market_db_sync recovery required: current-basis fundamentals "
            "state does not match provider coverage and full statement identities",
        )


def _validate_current_metrics(
    codes: Sequence[str],
    statement_rows: Sequence[dict[str, Any]],
    metric_rows: Sequence[dict[str, Any]],
    windows: dict[str, dict[str, Any]],
) -> None:
    normalized_codes = {normalize_stock_code(code) for code in codes if code}
    expected = {
        (normalize_stock_code(str(row["code"])), str(row["statement_id"]))
        for row in statement_rows
        if normalize_stock_code(str(row["code"])) in normalized_codes
    }
    actual = {
        (normalize_stock_code(str(row["code"])), str(row["statement_id"]))
        for row in metric_rows
        if normalize_stock_code(str(row["code"])) in normalized_codes
    }
    inconsistent = expected != actual
    for row in metric_rows:
        code = normalize_stock_code(str(row["code"]))
        if code not in normalized_codes:
            continue
        expected_basis_date = _as_date(
            windows[code]["coverage_end"], field="provider coverage_end"
        )
        inconsistent = inconsistent or (
            _as_date(
                row["fundamentals_adjustment_basis_date"],
                field="fundamentals_adjustment_basis_date",
            )
            != expected_basis_date
            or not str(row["source_fingerprint"] or "").strip()
        )
    if inconsistent:
        raise FundamentalsPitSnapshotError(
            "current_adjusted_metrics_required",
            "market_db_sync recovery required: current-basis statement metrics "
            "do not match the bounded statement identities/provider windows",
        )


def _load_statements(
    reader: MarketDbReader, code: str, knowledge_cutoff_date: date
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    rows = _load_statement_rows(reader, [code], knowledge_cutoff_date)
    converted = [
        {target: row.get(source) for target, source in _STATEMENT_FIELD_MAP.items()}
        for row in rows
    ]
    return convert_dated_response(converted, date_column="disclosedDate"), rows


def _load_provider_ohlcv(
    reader: MarketDbReader,
    code: str,
    window: dict[str, Any],
    effective_market_date: date,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    rows = _records(
        reader.query(
            f"""
            SELECT normalized_code AS code, date, open, high, low, close, volume
            FROM (
                SELECT {_NORMALIZED_CODE_SQL} AS normalized_code,
                       date, open, high, low, close, volume,
                       ROW_NUMBER() OVER (
                           PARTITION BY {_NORMALIZED_CODE_SQL}, date
                           ORDER BY CASE WHEN code = {_NORMALIZED_CODE_SQL} THEN 0 ELSE 1 END,
                                    length(code), code
                       ) AS rn
                FROM stock_data
                WHERE {_NORMALIZED_CODE_SQL} = ?
                  AND date >= ? AND date <= ?
            )
            WHERE rn = 1
            ORDER BY date
            """,
            (
                normalize_stock_code(code),
                str(window["coverage_start"]),
                effective_market_date.isoformat(),
            ),
        )
    )
    return convert_ohlcv_response(rows), rows


def _load_valuation(
    reader: MarketDbReader,
    code: str,
    window: dict[str, Any],
    knowledge_cutoff_date: date,
    effective_market_date: date,
    statement_ids: set[str],
) -> list[dict[str, Any]]:
    rows = _records(
        reader.query(
            """
            SELECT * EXCLUDE (rn, source_code, normalized_code),
                   normalized_code AS code, ? AS provider_as_of
            FROM (
                SELECT valuation.*, valuation.code AS source_code,
                       CASE
                           WHEN length(valuation.code) IN (5, 6)
                                AND right(valuation.code, 1) = '0'
                           THEN left(valuation.code, length(valuation.code) - 1)
                           ELSE valuation.code
                       END AS normalized_code,
                       ROW_NUMBER() OVER (
                           PARTITION BY normalized_code, valuation.date
                           ORDER BY CASE WHEN valuation.code = normalized_code THEN 0 ELSE 1 END,
                                    length(valuation.code), valuation.code
                       ) AS rn
                FROM daily_valuation AS valuation
                WHERE normalized_code = ? AND valuation.date >= ? AND valuation.date <= ?
            )
            WHERE rn = 1
            ORDER BY date
            """,
            (
                str(window["provider_as_of"]),
                normalize_stock_code(code),
                str(window["coverage_start"]),
                effective_market_date.isoformat(),
            ),
        )
    )
    cutoff_timestamp = _cutoff_timestamp(knowledge_cutoff_date)
    for row in rows:
        row_date = _as_date(row["date"], field="valuation date")
        disclosed_at = row.get("statement_disclosed_at")
        if (
            row_date > effective_market_date
            or row["price_basis_date"] != row["date"]
            or (disclosed_at is not None and str(disclosed_at) > cutoff_timestamp)
            or (row.get("statement_id") is not None and str(row["statement_id"]) not in statement_ids)
        ):
            raise FundamentalsPitSnapshotError(
                "pit_snapshot_inconsistent",
                "canonical daily valuation escaped its price/knowledge cutoff",
            )
    return rows


def _load_prime_panel(reader: MarketDbReader, effective_market_date: date) -> pd.DataFrame:
    market_codes = tuple(expand_market_codes(["prime"]))
    placeholders = ",".join("?" for _ in market_codes)
    master_rows = _records(
        reader.query(
            f"""
            SELECT DISTINCT {_NORMALIZED_CODE_SQL} AS code
            FROM stock_master_daily
            WHERE date = ? AND lower(trim(market_code)) IN ({placeholders})
            ORDER BY code
            """,
            (effective_market_date.isoformat(), *market_codes),
        )
    )
    codes = [str(row["code"]) for row in master_rows]
    if not codes:
        return pd.DataFrame()
    windows = _resolve_provider_windows(reader, codes, effective_market_date)
    _validate_current_fundamentals_state(reader, codes, windows)
    statements = _load_statement_rows(reader, codes, effective_market_date)
    metrics = _load_metric_rows(reader, codes, effective_market_date)
    _validate_current_metrics(codes, statements, metrics, windows)
    code_placeholders = ",".join("?" for _ in codes)
    rows = _records(
        reader.query(
            f"""
            WITH normalized_prices AS (
                SELECT normalized_code AS code, date, close, volume
                FROM (
                    SELECT {_NORMALIZED_CODE_SQL} AS normalized_code,
                           date, close, volume,
                           ROW_NUMBER() OVER (
                               PARTITION BY {_NORMALIZED_CODE_SQL}, date
                               ORDER BY CASE WHEN code = {_NORMALIZED_CODE_SQL} THEN 0 ELSE 1 END,
                                        length(code), code
                           ) AS rn
                    FROM stock_data
                    WHERE {_NORMALIZED_CODE_SQL} IN ({code_placeholders})
                      AND date <= ?
                )
                WHERE rn = 1
            ), recent AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS recency_rank
                FROM normalized_prices
            ), aggregates AS (
                SELECT code,
                       MAX(CASE WHEN date = ? THEN close END) AS close,
                       MAX(CASE WHEN date = ? THEN volume END) AS volume,
                       CASE WHEN COUNT(*) FILTER (WHERE recency_rank <= 20) >= 20
                            THEN MEDIAN(CASE WHEN recency_rank <= 20 THEN close * volume END) END AS adv20_jpy,
                       CASE WHEN COUNT(*) FILTER (WHERE recency_rank <= 60) >= 60
                            THEN MEDIAN(CASE WHEN recency_rank <= 60 THEN close * volume END) END AS adv60_jpy
                FROM recent WHERE recency_rank <= 60 GROUP BY code
            ), valuation_canonical AS (
                SELECT * EXCLUDE (rn, code, normalized_code), normalized_code AS code
                FROM (
                    SELECT valuation.*,
                           CASE
                               WHEN length(valuation.code) IN (5, 6)
                                    AND right(valuation.code, 1) = '0'
                               THEN left(valuation.code, length(valuation.code) - 1)
                               ELSE valuation.code
                           END AS normalized_code,
                           ROW_NUMBER() OVER (
                               PARTITION BY normalized_code, valuation.date
                               ORDER BY CASE WHEN valuation.code = normalized_code THEN 0 ELSE 1 END,
                                        length(valuation.code), valuation.code
                           ) AS rn
                    FROM daily_valuation AS valuation
                    WHERE valuation.date = ?
                ) WHERE rn = 1
            )
            SELECT aggregates.code, ? AS date, aggregates.close, aggregates.volume,
                   valuation.free_float_market_cap,
                   ? AS stock_master_snapshot_date,
                   aggregates.adv20_jpy, aggregates.adv60_jpy,
                   valuation.fundamentals_adjustment_basis_date,
                   provider.provider_as_of,
                   valuation.statement_disclosed_date,
                   valuation.forward_eps_disclosed_date,
                   valuation.forward_sales_disclosed_date
            FROM aggregates
            JOIN valuation_canonical AS valuation ON valuation.code = aggregates.code
            JOIN stock_provider_windows AS provider ON provider.code = aggregates.code
            WHERE aggregates.close IS NOT NULL
            ORDER BY aggregates.code
            """,
            (
                *codes,
                effective_market_date.isoformat(),
                effective_market_date.isoformat(),
                effective_market_date.isoformat(),
                effective_market_date.isoformat(),
                effective_market_date.isoformat(),
                effective_market_date.isoformat(),
            ),
        )
    )
    return pd.DataFrame(rows)


def _resolve_inside_snapshot(
    reader: MarketDbReader, symbol: str, cutoff_date: date | None
) -> FundamentalsPitSnapshot:
    if cutoff_date is None:
        frontier = reader.query_one("SELECT MAX(date) AS date FROM topix_data")
        if frontier is None or frontier["date"] is None:
            raise FundamentalsPitSnapshotError(
                "pit_snapshot_inconsistent", "local market frontier is unavailable"
            )
        knowledge_cutoff_date = _as_date(frontier["date"], field="market frontier")
    else:
        knowledge_cutoff_date = cutoff_date

    session = reader.query_one(
        "SELECT MAX(date) AS date FROM topix_data WHERE date <= ?",
        (knowledge_cutoff_date.isoformat(),),
    )
    if session is None or session["date"] is None:
        raise FundamentalsPitSnapshotError(
            "pit_snapshot_inconsistent",
            f"no local market session exists on or before {knowledge_cutoff_date}",
        )
    effective_market_date = _as_date(session["date"], field="effective market date")
    code = normalize_stock_code(symbol)
    stock_info, _master = _resolve_master(reader, code, effective_market_date)
    window = _resolve_provider_windows(reader, [code], effective_market_date)[code]
    _validate_current_fundamentals_state(reader, [code], {code: window})
    statements, statement_rows = _load_statements(reader, code, knowledge_cutoff_date)
    metrics = _load_metric_rows(reader, [code], knowledge_cutoff_date)
    _validate_current_metrics([code], statement_rows, metrics, {code: window})
    statement_ids = {str(row["statement_id"]) for row in statement_rows}
    valuation = _load_valuation(
        reader,
        code,
        window,
        knowledge_cutoff_date,
        effective_market_date,
        statement_ids,
    )
    ohlcv, ohlcv_rows = _load_provider_ohlcv(
        reader, code, window, effective_market_date
    )
    valuation_dates = [str(row["date"]) for row in valuation]
    ohlcv_dates = [str(row["date"]) for row in ohlcv_rows]
    if valuation_dates != ohlcv_dates or any(
        valuation_row["close"] != price_row["close"]
        for valuation_row, price_row in zip(valuation, ohlcv_rows, strict=True)
    ):
        raise FundamentalsPitSnapshotError(
            "pit_snapshot_inconsistent",
            "canonical daily valuation does not equal provider-adjusted stock_data",
        )
    prime_panel = _load_prime_panel(reader, effective_market_date)
    coverage_end = _as_date(window["coverage_end"], field="provider coverage_end")
    return FundamentalsPitSnapshot(
        requested_cutoff_date=cutoff_date,
        knowledge_cutoff_date=knowledge_cutoff_date,
        effective_market_date=effective_market_date,
        stock_master_snapshot_date=effective_market_date,
        fundamentals_adjustment_basis_date=coverage_end,
        provider_as_of=str(window["provider_as_of"]),
        provider_coverage_start=_as_date(
            window["coverage_start"], field="provider coverage_start"
        ),
        provider_coverage_end=coverage_end,
        stock_info=stock_info,
        statements=statements,
        adjusted_statement_metrics=tuple(metrics),
        daily_valuation=tuple(valuation),
        ohlcv=ohlcv,
        prime_liquidity_panel=prime_panel,
    )


def resolve_fundamentals_pit_snapshot(
    reader: MarketDbReader,
    symbol: str,
    cutoff_date: date | None,
) -> FundamentalsPitSnapshot:
    """Resolve all Fundamentals inputs inside exactly one DuckDB read snapshot."""
    with reader.read_snapshot():
        return _resolve_inside_snapshot(reader, symbol, cutoff_date)
