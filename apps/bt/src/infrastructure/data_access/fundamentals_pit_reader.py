"""Canonical, fail-closed Fundamentals point-in-time snapshot resolver."""

from __future__ import annotations

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


def _basis_rows(
    reader: MarketDbReader, code: str, effective_market_date: date
) -> list[dict[str, Any]]:
    value = effective_market_date.isoformat()
    return _records(
        reader.query(
            """
            SELECT code, basis_id, valid_from, valid_to_exclusive,
                   adjustment_through_date, source_fingerprint,
                   materialized_through_date, status, created_at, updated_at
            FROM stock_adjustment_bases
            WHERE code = ?
              AND status = 'ready'
              AND valid_from <= ?
              AND (valid_to_exclusive IS NULL OR ? < valid_to_exclusive)
            ORDER BY valid_from
            """,
            (normalize_stock_code(code), value, value),
        )
    )


def _resolve_basis(
    reader: MarketDbReader, code: str, effective_market_date: date
) -> dict[str, Any]:
    rows = _basis_rows(reader, code, effective_market_date)
    if not rows:
        raise FundamentalsPitSnapshotError(
            "historical_adjustment_basis_required",
            f"no ready adjustment basis contains {code} at {effective_market_date}",
        )
    if len(rows) != 1:
        raise FundamentalsPitSnapshotError(
            "pit_snapshot_inconsistent",
            f"multiple ready adjustment bases contain {code} at {effective_market_date}",
        )
    basis = rows[0]
    _validate_basis(basis, code, effective_market_date)
    return basis


def _validate_basis(
    basis: dict[str, Any], code: str, effective_market_date: date
) -> None:
    materialized = _as_date(
        basis["materialized_through_date"], field="materialized_through_date"
    )
    if materialized < effective_market_date:
        raise FundamentalsPitSnapshotError(
            "historical_adjustment_basis_required",
            f"adjustment basis for {code} is materialized only through {materialized}",
        )
    adjustment_through = _as_date(
        basis["adjustment_through_date"], field="adjustment_through_date"
    )
    valid_from = _as_date(basis["valid_from"], field="valid_from")
    valid_to_value = basis.get("valid_to_exclusive")
    valid_to = (
        _as_date(valid_to_value, field="valid_to_exclusive")
        if valid_to_value is not None
        else None
    )
    if (
        normalize_stock_code(str(basis["code"])) != normalize_stock_code(code)
        or basis["basis_id"]
        != f"event-pit-v1:{normalize_stock_code(code)}:{valid_from.isoformat()}"
        or not str(basis["source_fingerprint"] or "").strip()
        or basis["status"] != "ready"
        or valid_from > effective_market_date
        or (valid_to is not None and effective_market_date >= valid_to)
        or adjustment_through != valid_from
        or adjustment_through > effective_market_date
    ):
        raise FundamentalsPitSnapshotError(
            "pit_snapshot_inconsistent", f"invalid adjustment basis for {code}"
        )


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
    if _as_date(record["date"], field="stock master date") != effective_market_date:
        raise FundamentalsPitSnapshotError(
            "pit_snapshot_inconsistent", "stock master date does not match market date"
        )
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


def _load_statements(
    reader: MarketDbReader, code: str, knowledge_cutoff_date: date
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    rows = _records(
        reader.query(
            f"""
            WITH ranked AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY disclosed_date
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END,
                             CASE WHEN type_of_document LIKE '%FinancialStatements%' THEN 0
                                  WHEN type_of_document IS NULL OR type_of_document = '' THEN 1
                                  ELSE 2 END
                ) AS rn
                FROM statements
                WHERE {_NORMALIZED_CODE_SQL} = ? AND disclosed_date <= ?
            )
            SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1 ORDER BY disclosed_date
            """,
            (normalize_stock_code(code), knowledge_cutoff_date.isoformat()),
        )
    )
    for row in rows:
        if (
            normalize_stock_code(str(row["code"])) != normalize_stock_code(code)
            or _as_date(row["disclosed_date"], field="statement disclosed_date")
            > knowledge_cutoff_date
        ):
            raise FundamentalsPitSnapshotError(
                "pit_snapshot_inconsistent", "statement escaped the knowledge cutoff"
            )
    converted = [
        {target: row.get(source) for target, source in _STATEMENT_FIELD_MAP.items()}
        for row in rows
    ]
    return (
        convert_dated_response(converted, date_column="disclosedDate"),
        rows,
    )


def _load_adjusted_metrics(
    reader: MarketDbReader,
    code: str,
    basis: dict[str, Any],
    knowledge_cutoff_date: date,
    statement_disclosure_dates: set[date],
) -> list[dict[str, Any]]:
    rows = _records(
        reader.query(
            """
            SELECT * FROM statement_metrics_adjusted
            WHERE code = ? AND basis_version = ? AND disclosed_date <= ?
            ORDER BY disclosed_date, period_end, period_type
            """,
            (
                normalize_stock_code(code),
                str(basis["basis_id"]),
                knowledge_cutoff_date.isoformat(),
            ),
        )
    )
    expected_basis = str(basis["basis_id"])
    adjustment_through = _as_date(
        basis["adjustment_through_date"], field="adjustment_through_date"
    )
    for row in rows:
        price_basis = _as_date(row["price_basis_date"], field="metric price_basis_date")
        if (
            normalize_stock_code(str(row["code"])) != normalize_stock_code(code)
            or row["basis_version"] != expected_basis
            or price_basis != adjustment_through
            or (
                disclosed := _as_date(
                    row["disclosed_date"], field="metric disclosed_date"
                )
            )
            > knowledge_cutoff_date
            or disclosed not in statement_disclosure_dates
        ):
            raise FundamentalsPitSnapshotError(
                "pit_snapshot_inconsistent",
                "adjusted statement metric provenance is inconsistent",
            )
    return rows


def _load_valuation(
    reader: MarketDbReader,
    code: str,
    basis: dict[str, Any],
    knowledge_cutoff_date: date,
    effective_market_date: date,
    statement_disclosure_dates: set[date],
) -> list[dict[str, Any]]:
    rows = _records(
        reader.query(
            """
            SELECT * FROM daily_valuation
            WHERE code = ? AND basis_version = ? AND date <= ?
            ORDER BY date
            """,
            (
                normalize_stock_code(code),
                str(basis["basis_id"]),
                effective_market_date.isoformat(),
            ),
        )
    )
    expected_basis = str(basis["basis_id"])
    adjustment_through = _as_date(
        basis["adjustment_through_date"], field="adjustment_through_date"
    )
    for row in rows:
        row_date = _as_date(row["date"], field="valuation date")
        price_basis = _as_date(
            row["price_basis_date"], field="valuation price_basis_date"
        )
        if (
            normalize_stock_code(str(row["code"])) != normalize_stock_code(code)
            or row["basis_version"] != expected_basis
            or price_basis != adjustment_through
            or row_date > effective_market_date
        ):
            raise FundamentalsPitSnapshotError(
                "pit_snapshot_inconsistent",
                "daily valuation basis or date is inconsistent",
            )
        for field in (
            "statement_disclosed_date",
            "forward_eps_disclosed_date",
            "forward_sales_disclosed_date",
        ):
            value = row.get(field)
            if value is None:
                continue
            disclosed = _as_date(value, field=field)
            if disclosed > row_date or disclosed > knowledge_cutoff_date:
                raise FundamentalsPitSnapshotError(
                    "pit_snapshot_inconsistent",
                    f"daily valuation contains future {field}",
                )
            if disclosed not in statement_disclosure_dates:
                raise FundamentalsPitSnapshotError(
                    "pit_snapshot_inconsistent",
                    f"daily valuation {field} is absent from bounded statements",
                )
    return rows


def _load_basis_ohlcv(
    reader: MarketDbReader,
    code: str,
    basis: dict[str, Any],
    effective_market_date: date,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    rows = _records(
        reader.query(
            f"""
            WITH normalized_raw AS (
                SELECT {_NORMALIZED_CODE_SQL} AS normalized_code, code AS source_code,
                       date, open, high, low, close, volume,
                       ROW_NUMBER() OVER (
                           PARTITION BY {_NORMALIZED_CODE_SQL}, date
                           ORDER BY CASE WHEN code = {_NORMALIZED_CODE_SQL} THEN 0 ELSE 1 END,
                                    length(code), code
                       ) AS alias_rank
                FROM stock_data_raw
            )
            SELECT raw.normalized_code AS code, raw.date,
                   raw.open * segment.cumulative_factor AS open,
                   raw.high * segment.cumulative_factor AS high,
                   raw.low * segment.cumulative_factor AS low,
                   raw.close * segment.cumulative_factor AS close,
                   CAST(ROUND(raw.volume / segment.cumulative_factor) AS BIGINT) AS volume,
                   basis.basis_id
            FROM normalized_raw AS raw
            JOIN stock_adjustment_bases AS basis
              ON basis.code = raw.normalized_code AND basis.basis_id = ? AND basis.status = 'ready'
            JOIN stock_adjustment_basis_segments AS segment
              ON segment.code = basis.code AND segment.basis_id = basis.basis_id
             AND raw.date >= segment.source_date_from
             AND (segment.source_date_to_exclusive IS NULL OR raw.date < segment.source_date_to_exclusive)
            WHERE raw.alias_rank = 1 AND raw.normalized_code = ?
              AND raw.date <= ? AND raw.date <= basis.materialized_through_date
            ORDER BY raw.date
            """,
            (
                str(basis["basis_id"]),
                normalize_stock_code(code),
                effective_market_date.isoformat(),
            ),
        )
    )
    for row in rows:
        if (
            normalize_stock_code(str(row["code"])) != normalize_stock_code(code)
            or row["basis_id"] != basis["basis_id"]
            or _as_date(row["date"], field="OHLCV date") > effective_market_date
        ):
            raise FundamentalsPitSnapshotError(
                "pit_snapshot_inconsistent", "OHLCV basis or date is inconsistent"
            )
    frame = convert_ohlcv_response(
        [
            {
                key: row.get(key)
                for key in ("date", "open", "high", "low", "close", "volume")
            }
            for row in rows
        ]
    )
    return frame, rows


def _load_prime_panel(
    reader: MarketDbReader, effective_market_date: date
) -> pd.DataFrame:
    market_codes = tuple(expand_market_codes(["prime"]))
    placeholders = ",".join("?" for _ in market_codes)
    master_rows = _records(
        reader.query(
            f"""
            SELECT date, code FROM stock_master_daily
            WHERE date = ? AND lower(trim(market_code)) IN ({placeholders})
            ORDER BY code
            """,
            (effective_market_date.isoformat(), *market_codes),
        )
    )
    prime_codes = sorted(
        {normalize_stock_code(str(master["code"])) for master in master_rows}
    )
    if not prime_codes:
        return pd.DataFrame()
    code_placeholders = ",".join("?" for _ in prime_codes)
    basis_rows = _records(
        reader.query(
            f"""
            SELECT code, basis_id, valid_from, valid_to_exclusive,
                   adjustment_through_date, source_fingerprint,
                   materialized_through_date, status, created_at, updated_at
            FROM stock_adjustment_bases
            WHERE code IN ({code_placeholders})
              AND status = 'ready'
              AND valid_from <= ?
              AND (valid_to_exclusive IS NULL OR ? < valid_to_exclusive)
            ORDER BY code, valid_from
            """,
            (
                *prime_codes,
                effective_market_date.isoformat(),
                effective_market_date.isoformat(),
            ),
        )
    )
    bases_by_code: dict[str, list[dict[str, Any]]] = {code: [] for code in prime_codes}
    for basis in basis_rows:
        bases_by_code.setdefault(normalize_stock_code(str(basis["code"])), []).append(
            basis
        )
    selected_bases: list[dict[str, Any]] = []
    for code in prime_codes:
        candidates = bases_by_code[code]
        if not candidates:
            raise FundamentalsPitSnapshotError(
                "historical_adjustment_basis_required",
                f"no ready adjustment basis contains {code} at {effective_market_date}",
            )
        if len(candidates) != 1:
            raise FundamentalsPitSnapshotError(
                "pit_snapshot_inconsistent",
                f"multiple ready adjustment bases contain {code} at {effective_market_date}",
            )
        _validate_basis(candidates[0], code, effective_market_date)
        selected_bases.append(candidates[0])

    values_sql = ",".join("(?, ?, ?)" for _ in selected_bases)
    basis_params: list[Any] = []
    for basis in selected_bases:
        basis_params.extend(
            [basis["code"], basis["basis_id"], basis["adjustment_through_date"]]
        )
    panel = _records(
        reader.query(
            f"""
            WITH selected_bases(code, basis_id, adjustment_through_date) AS (
                VALUES {values_sql}
            ),
            normalized_raw AS (
                SELECT {_NORMALIZED_CODE_SQL} AS normalized_code, code AS source_code,
                       date, close, volume,
                       ROW_NUMBER() OVER (
                           PARTITION BY {_NORMALIZED_CODE_SQL}, date
                           ORDER BY CASE WHEN code = {_NORMALIZED_CODE_SQL} THEN 0 ELSE 1 END,
                                    length(code), code
                       ) AS alias_rank
                FROM stock_data_raw
                WHERE date <= ?
            ),
            adjusted_prices AS (
                SELECT raw.normalized_code AS code, raw.date,
                       raw.close * segment.cumulative_factor AS close,
                       CAST(ROUND(raw.volume / segment.cumulative_factor) AS BIGINT) AS volume,
                       selected.basis_id,
                       selected.adjustment_through_date
                FROM normalized_raw AS raw
                JOIN selected_bases AS selected ON selected.code = raw.normalized_code
                JOIN stock_adjustment_basis_segments AS segment
                  ON segment.code = selected.code AND segment.basis_id = selected.basis_id
                 AND raw.date >= segment.source_date_from
                 AND (segment.source_date_to_exclusive IS NULL OR raw.date < segment.source_date_to_exclusive)
                WHERE raw.alias_rank = 1
            ),
            recent AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS recency_rank
                FROM adjusted_prices
            ),
            aggregates AS (
                SELECT code, basis_id, adjustment_through_date,
                       MAX(CASE WHEN date = ? THEN close END) AS close,
                       MAX(CASE WHEN date = ? THEN volume END) AS volume,
                       CASE WHEN COUNT(*) FILTER (WHERE recency_rank <= 20) >= 20
                            THEN MEDIAN(CASE WHEN recency_rank <= 20 THEN close * volume END) END AS adv20_jpy,
                       CASE WHEN COUNT(*) FILTER (WHERE recency_rank <= 60) >= 60
                            THEN MEDIAN(CASE WHEN recency_rank <= 60 THEN close * volume END) END AS adv60_jpy
                FROM recent
                WHERE recency_rank <= 60
                GROUP BY code, basis_id, adjustment_through_date
            )
            SELECT aggregates.code, ? AS date, aggregates.close, aggregates.volume,
                   valuation.free_float_market_cap, aggregates.basis_id,
                   valuation.price_basis_date, ? AS stock_master_snapshot_date,
                   aggregates.adv20_jpy, aggregates.adv60_jpy,
                   aggregates.adjustment_through_date,
                   valuation.statement_disclosed_date,
                   valuation.forward_eps_disclosed_date,
                   valuation.forward_sales_disclosed_date,
                   CASE WHEN valuation.statement_disclosed_date IS NULL THEN TRUE ELSE EXISTS (
                       SELECT 1 FROM statements AS source_statement
                       WHERE CASE
                                 WHEN length(source_statement.code) IN (5, 6)
                                      AND right(source_statement.code, 1) = '0'
                                 THEN left(source_statement.code, length(source_statement.code) - 1)
                                 ELSE source_statement.code
                             END = aggregates.code
                         AND source_statement.disclosed_date = valuation.statement_disclosed_date
                   ) END AS statement_disclosure_exists,
                   CASE WHEN valuation.forward_eps_disclosed_date IS NULL THEN TRUE ELSE EXISTS (
                       SELECT 1 FROM statements AS source_statement
                       WHERE CASE
                                 WHEN length(source_statement.code) IN (5, 6)
                                      AND right(source_statement.code, 1) = '0'
                                 THEN left(source_statement.code, length(source_statement.code) - 1)
                                 ELSE source_statement.code
                             END = aggregates.code
                         AND source_statement.disclosed_date = valuation.forward_eps_disclosed_date
                   ) END AS forward_eps_disclosure_exists,
                   CASE WHEN valuation.forward_sales_disclosed_date IS NULL THEN TRUE ELSE EXISTS (
                       SELECT 1 FROM statements AS source_statement
                       WHERE CASE
                                 WHEN length(source_statement.code) IN (5, 6)
                                      AND right(source_statement.code, 1) = '0'
                                 THEN left(source_statement.code, length(source_statement.code) - 1)
                                 ELSE source_statement.code
                             END = aggregates.code
                         AND source_statement.disclosed_date = valuation.forward_sales_disclosed_date
                   ) END AS forward_sales_disclosure_exists
            FROM aggregates
            JOIN daily_valuation AS valuation
              ON valuation.code = aggregates.code
             AND valuation.date = ?
             AND valuation.basis_version = aggregates.basis_id
            WHERE aggregates.close IS NOT NULL
            ORDER BY aggregates.code
            """,
            (
                *basis_params,
                effective_market_date.isoformat(),
                effective_market_date.isoformat(),
                effective_market_date.isoformat(),
                effective_market_date.isoformat(),
                effective_market_date.isoformat(),
                effective_market_date.isoformat(),
            ),
        )
    )
    for row in panel:
        if (
            _as_date(row["date"], field="liquidity valuation date")
            != effective_market_date
            or _as_date(
                row["stock_master_snapshot_date"], field="liquidity master date"
            )
            != effective_market_date
            or _as_date(row["price_basis_date"], field="liquidity price_basis_date")
            != _as_date(row["adjustment_through_date"], field="adjustment_through_date")
        ):
            raise FundamentalsPitSnapshotError(
                "pit_snapshot_inconsistent",
                "Prime liquidity valuation provenance is inconsistent",
            )
        for field, exists_field in (
            ("statement_disclosed_date", "statement_disclosure_exists"),
            ("forward_eps_disclosed_date", "forward_eps_disclosure_exists"),
            ("forward_sales_disclosed_date", "forward_sales_disclosure_exists"),
        ):
            value = row.get(field)
            if value is not None and (
                _as_date(value, field=f"liquidity {field}") > effective_market_date
                or not bool(row.get(exists_field))
            ):
                raise FundamentalsPitSnapshotError(
                    "pit_snapshot_inconsistent",
                    f"Prime liquidity valuation contains inconsistent {field}",
                )
            row.pop(exists_field)
        row.pop("adjustment_through_date")
    frame = pd.DataFrame(panel)
    if not frame.empty and (
        set(frame["date"]) != {effective_market_date.isoformat()}
        or set(frame["stock_master_snapshot_date"])
        != {effective_market_date.isoformat()}
    ):
        raise FundamentalsPitSnapshotError(
            "pit_snapshot_inconsistent", "Prime liquidity panel dates are inconsistent"
        )
    return frame


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
    basis = _resolve_basis(reader, code, effective_market_date)
    stock_info, _master = _resolve_master(reader, code, effective_market_date)
    statements, raw_statement_rows = _load_statements(
        reader, code, knowledge_cutoff_date
    )
    statement_disclosure_dates = {
        _as_date(row["disclosed_date"], field="statement disclosed_date")
        for row in raw_statement_rows
    }
    adjusted_metrics = _load_adjusted_metrics(
        reader,
        code,
        basis,
        knowledge_cutoff_date,
        statement_disclosure_dates,
    )
    valuation = _load_valuation(
        reader,
        code,
        basis,
        knowledge_cutoff_date,
        effective_market_date,
        statement_disclosure_dates,
    )
    ohlcv, _ohlcv_rows = _load_basis_ohlcv(reader, code, basis, effective_market_date)
    expected_disclosures = [
        _as_date(row["disclosed_date"], field="statement disclosed_date")
        for row in raw_statement_rows
    ]
    actual_disclosures = [
        _as_date(row["disclosed_date"], field="metric disclosed_date")
        for row in adjusted_metrics
    ]
    if sorted(actual_disclosures) != sorted(expected_disclosures):
        raise FundamentalsPitSnapshotError(
            "pit_snapshot_inconsistent",
            "bounded statements are not represented exactly once in the selected adjustment basis",
        )
    valuation_dates = [_as_date(row["date"], field="valuation date") for row in valuation]
    ohlcv_dates = [_as_date(row["date"], field="OHLCV date") for row in _ohlcv_rows]
    if sorted(valuation_dates) != sorted(ohlcv_dates):
        raise FundamentalsPitSnapshotError(
            "pit_snapshot_inconsistent",
            "daily valuation does not cover the selected basis price history exactly",
        )
    prime_panel = _load_prime_panel(reader, effective_market_date)
    return FundamentalsPitSnapshot(
        requested_cutoff_date=cutoff_date,
        knowledge_cutoff_date=knowledge_cutoff_date,
        effective_market_date=effective_market_date,
        stock_master_snapshot_date=effective_market_date,
        basis_id=str(basis["basis_id"]),
        adjustment_through_date=_as_date(
            basis["adjustment_through_date"], field="adjustment_through_date"
        ),
        materialized_through_date=_as_date(
            basis["materialized_through_date"], field="materialized_through_date"
        ),
        stock_info=stock_info,
        statements=statements,
        adjusted_statement_metrics=tuple(adjusted_metrics),
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
