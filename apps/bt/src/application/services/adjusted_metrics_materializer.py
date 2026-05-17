"""Materialize adjusted fundamentals and daily valuation into market.duckdb."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from src.domains.fundamentals.adjusted_metrics import (
    AdjustedStatementInput,
    AdjustedStatementMetric,
    DailyValuationInput,
    build_adjusted_statement_metric,
    build_daily_valuation_metric,
)
from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.shared.utils.share_adjustment import ShareAdjustmentEvent


@dataclass(frozen=True)
class AdjustedMetricsBuildResult:
    statement_rows: int
    daily_valuation_rows: int
    price_basis_date: str | None
    basis_version: str | None


class AdjustedMetricsMaterializer:
    """Build canonical adjusted metrics from raw market DB tables."""

    def __init__(self, market_db: MarketDb) -> None:
        self._market_db = market_db

    def rebuild_all(self) -> AdjustedMetricsBuildResult:
        return self._rebuild(codes=None)

    def rebuild_codes(self, codes: list[str]) -> AdjustedMetricsBuildResult:
        normalized_codes = sorted({normalize_stock_code(code) for code in codes if code})
        return self._rebuild(codes=normalized_codes)

    def _rebuild(self, *, codes: list[str] | None) -> AdjustedMetricsBuildResult:
        price_basis_date = self._market_db.get_latest_stock_data_date()
        if price_basis_date is None:
            return AdjustedMetricsBuildResult(
                statement_rows=0,
                daily_valuation_rows=0,
                price_basis_date=None,
                basis_version=None,
            )
        basis_version = f"adjusted-v1:{price_basis_date}"

        statement_rows = self._load_statement_rows(codes)
        events_by_code = self._load_adjustment_events_by_code(codes, price_basis_date)
        adjusted_metrics = [
            build_adjusted_statement_metric(
                _statement_input_from_row(row),
                events=events_by_code.get(normalize_stock_code(str(row["code"])), []),
                price_basis_date=price_basis_date,
                basis_version=basis_version,
            )
            for row in statement_rows
        ]
        adjusted_payload = [_statement_metric_to_row(metric) for metric in adjusted_metrics]
        stored_statement_rows = self._market_db.upsert_statement_metrics_adjusted(
            adjusted_payload
        )

        stored_daily_rows = 0
        if adjusted_metrics and self._market_db._table_exists("stock_data"):
            stored_daily_rows = self._market_db.upsert_daily_valuation_from_adjusted_metrics(
                basis_version=basis_version,
                price_basis_date=price_basis_date,
                codes=codes,
            )
        return AdjustedMetricsBuildResult(
            statement_rows=stored_statement_rows,
            daily_valuation_rows=stored_daily_rows,
            price_basis_date=price_basis_date,
            basis_version=basis_version,
        )

    def _load_statement_rows(self, codes: list[str] | None) -> list[dict[str, Any]]:
        if not self._market_db._table_exists("statements"):
            return []
        where_clause, params = _code_filter("code", codes)
        return self._market_db._fetchall_dicts(
            f"""
            SELECT
                code,
                disclosed_date,
                disclosed_date AS period_end,
                type_of_current_period,
                earnings_per_share,
                bps,
                COALESCE(forecast_eps, next_year_forecast_earnings_per_share) AS forecast_eps,
                dividend_fy,
                shares_outstanding,
                treasury_shares
            FROM statements
            {where_clause}
            ORDER BY code, disclosed_date
            """,
            params,
        )

    def _load_adjustment_events_by_code(
        self,
        codes: list[str] | None,
        price_basis_date: str,
    ) -> dict[str, list[ShareAdjustmentEvent]]:
        if not self._market_db._table_exists("stock_data_raw"):
            return {}
        code_where, params = _code_filter("code", codes, prefix="AND")
        rows = self._market_db._fetchall_dicts(
            f"""
            SELECT code, date, adjustment_factor
            FROM stock_data_raw
            WHERE adjustment_factor IS NOT NULL
              AND adjustment_factor != 1.0
              AND date <= ?
              {code_where}
            ORDER BY code, date
            """,
            [price_basis_date, *params],
        )
        events_by_code: dict[str, list[ShareAdjustmentEvent]] = {}
        for row in rows:
            code = normalize_stock_code(str(row["code"]))
            events_by_code.setdefault(code, []).append(
                ShareAdjustmentEvent(
                    date=str(row["date"]),
                    adjustment_factor=float(row["adjustment_factor"]),
                )
            )
        return events_by_code

    def _build_daily_valuation_rows(
        self,
        *,
        codes: list[str] | None,
        adjusted_metrics: list[AdjustedStatementMetric],
        price_basis_date: str,
        basis_version: str,
    ) -> list[dict[str, Any]]:
        if not adjusted_metrics or not self._market_db._table_exists("stock_data"):
            return []
        metrics_by_code = _group_metrics_by_code(adjusted_metrics)
        code_where, params = _code_filter("code", codes)
        price_rows = self._market_db._fetchall_dicts(
            f"""
            SELECT code, date, close
            FROM stock_data
            {code_where}
            ORDER BY code, date
            """,
            params,
        )

        valuation_rows: list[dict[str, Any]] = []
        for price_row in price_rows:
            code = normalize_stock_code(str(price_row["code"]))
            metric = _latest_metric_as_of(metrics_by_code.get(code, []), str(price_row["date"]))
            if metric is None:
                continue
            valuation = build_daily_valuation_metric(
                DailyValuationInput(
                    code=code,
                    date=str(price_row["date"]),
                    price_basis_date=price_basis_date,
                    close=float(price_row["close"]),
                    eps=metric.adjusted_eps,
                    bps=metric.adjusted_bps,
                    forward_eps=metric.adjusted_forecast_eps,
                    operating_profit=None,
                    forward_operating_profit=None,
                    shares_outstanding=metric.adjusted_shares_outstanding,
                    treasury_shares=metric.adjusted_treasury_shares,
                    statement_disclosed_date=metric.disclosed_date,
                    forward_eps_disclosed_date=(
                        metric.disclosed_date
                        if metric.adjusted_forecast_eps is not None
                        else None
                    ),
                    forward_eps_source=(
                        "fy" if metric.adjusted_forecast_eps is not None else None
                    ),
                    basis_version=basis_version,
                )
            )
            valuation_rows.append(_daily_valuation_metric_to_row(valuation))
        return valuation_rows


def _code_filter(
    column: str,
    codes: list[str] | None,
    *,
    prefix: str = "WHERE",
) -> tuple[str, list[Any]]:
    if not codes:
        return "", []
    placeholders = ", ".join("?" for _ in codes)
    return f"{prefix} {column} IN ({placeholders})", [*codes]


def _statement_input_from_row(row: dict[str, Any]) -> AdjustedStatementInput:
    disclosed_date = str(row["disclosed_date"])
    return AdjustedStatementInput(
        code=normalize_stock_code(str(row["code"])),
        disclosed_date=disclosed_date,
        period_end=str(row.get("period_end") or disclosed_date),
        period_type=str(row.get("type_of_current_period") or ""),
        eps=_optional_float(row.get("earnings_per_share")),
        bps=_optional_float(row.get("bps")),
        forecast_eps=_optional_float(row.get("forecast_eps")),
        dividend_fy=_optional_float(row.get("dividend_fy")),
        shares_outstanding=_optional_float(row.get("shares_outstanding")),
        treasury_shares=_optional_float(row.get("treasury_shares")),
    )


def _statement_metric_to_row(metric: AdjustedStatementMetric) -> dict[str, Any]:
    return {
        "code": metric.code,
        "disclosed_date": metric.disclosed_date,
        "period_end": metric.period_end,
        "period_type": metric.period_type,
        "price_basis_date": metric.price_basis_date,
        "raw_eps": metric.raw_eps,
        "adjusted_eps": metric.adjusted_eps,
        "raw_bps": metric.raw_bps,
        "adjusted_bps": metric.adjusted_bps,
        "raw_forecast_eps": metric.raw_forecast_eps,
        "adjusted_forecast_eps": metric.adjusted_forecast_eps,
        "raw_dividend_fy": metric.raw_dividend_fy,
        "adjusted_dividend_fy": metric.adjusted_dividend_fy,
        "raw_shares_outstanding": metric.raw_shares_outstanding,
        "adjusted_shares_outstanding": metric.adjusted_shares_outstanding,
        "raw_treasury_shares": metric.raw_treasury_shares,
        "adjusted_treasury_shares": metric.adjusted_treasury_shares,
        "adjustment_factor_cumulative": metric.adjustment_factor_cumulative,
        "basis_version": metric.basis_version,
    }


def _daily_valuation_metric_to_row(metric: Any) -> dict[str, Any]:
    return {
        "code": metric.code,
        "date": metric.date,
        "price_basis_date": metric.price_basis_date,
        "close": metric.close,
        "eps": metric.eps,
        "bps": metric.bps,
        "forward_eps": metric.forward_eps,
        "per": metric.per,
        "forward_per": metric.forward_per,
        "p_op": metric.p_op,
        "forward_p_op": metric.forward_p_op,
        "pbr": metric.pbr,
        "market_cap": metric.market_cap,
        "free_float_market_cap": metric.free_float_market_cap,
        "statement_disclosed_date": metric.statement_disclosed_date,
        "forward_eps_disclosed_date": metric.forward_eps_disclosed_date,
        "forward_eps_source": metric.forward_eps_source,
        "basis_version": metric.basis_version,
    }


def _group_metrics_by_code(
    metrics: Iterable[AdjustedStatementMetric],
) -> dict[str, list[AdjustedStatementMetric]]:
    grouped: dict[str, list[AdjustedStatementMetric]] = {}
    for metric in metrics:
        grouped.setdefault(metric.code, []).append(metric)
    for values in grouped.values():
        values.sort(key=lambda metric: (metric.disclosed_date, metric.period_end))
    return grouped


def _latest_metric_as_of(
    metrics: list[AdjustedStatementMetric],
    date: str,
) -> AdjustedStatementMetric | None:
    latest: AdjustedStatementMetric | None = None
    for metric in metrics:
        if metric.disclosed_date <= date:
            latest = metric
        else:
            break
    return latest


def _optional_float(value: Any) -> float | None:
    return float(value) if value is not None else None
