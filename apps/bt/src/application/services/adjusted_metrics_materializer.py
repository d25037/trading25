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
from src.infrastructure.db.market.market_schema import STATEMENT_METRICS_ADJUSTED_COLUMNS
from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.shared.utils.share_adjustment import ShareAdjustmentEvent


_STATEMENT_METRIC_KEY_COLUMNS = (
    "code",
    "disclosed_date",
    "period_end",
    "period_type",
    "basis_version",
)
_STATEMENT_METRIC_COMPARE_COLUMNS = tuple(
    column
    for column in STATEMENT_METRICS_ADJUSTED_COLUMNS
    if column != "created_at"
)


@dataclass(frozen=True)
class AdjustedMetricsBuildResult:
    statement_rows: int
    daily_valuation_rows: int
    daily_technical_metric_rows: int
    daily_valuation_latest_date: str | None
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
        latest_price_basis_date = self._market_db.get_latest_stock_data_date()
        if latest_price_basis_date is None:
            return AdjustedMetricsBuildResult(
                statement_rows=0,
                daily_valuation_rows=0,
                daily_technical_metric_rows=0,
                daily_valuation_latest_date=None,
                price_basis_date=None,
                basis_version=None,
            )
        basis_version, price_basis_date, reuse_existing_basis = (
            self._resolve_materialization_basis(codes, latest_price_basis_date)
        )

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
        changed_start_dates_by_code = (
            self._changed_statement_metric_start_dates(adjusted_payload, basis_version, codes)
            if reuse_existing_basis
            else {}
        )
        changed_start_date = min(changed_start_dates_by_code.values()) if changed_start_dates_by_code else None
        if reuse_existing_basis and not changed_start_dates_by_code:
            stored_statement_rows = self._statement_metrics_count(basis_version, codes)
        else:
            stored_statement_rows = self._market_db.upsert_statement_metrics_adjusted(
                adjusted_payload
            )

        stored_daily_rows = 0
        if adjusted_metrics and self._market_db._table_exists("stock_data"):
            existing_daily_max_date = (
                self._latest_daily_valuation_date(basis_version, codes)
                if reuse_existing_basis
                else None
            )
            latest_coverage_is_sparse = (
                self._latest_daily_valuation_coverage_is_sparse(basis_version, codes)
                if existing_daily_max_date is not None
                else False
            )
            sales_materialization_is_stale = (
                self._daily_valuation_sales_materialization_is_stale(
                    basis_version,
                    codes,
                )
                if existing_daily_max_date is not None
                else False
            )
            if reuse_existing_basis and codes is None and changed_start_dates_by_code:
                for code, start_date in sorted(changed_start_dates_by_code.items()):
                    self._market_db.upsert_daily_valuation_from_adjusted_metrics(
                        basis_version=basis_version,
                        price_basis_date=price_basis_date,
                        codes=[code],
                        start_date=start_date,
                        start_date_inclusive=True,
                        replace_existing=False,
                    )
                if (
                    existing_daily_max_date is not None
                    and existing_daily_max_date < latest_price_basis_date
                ):
                    self._market_db.upsert_daily_valuation_from_adjusted_metrics(
                        basis_version=basis_version,
                        price_basis_date=price_basis_date,
                        codes=None,
                        start_date=existing_daily_max_date,
                        start_date_inclusive=False,
                        replace_existing=False,
                    )
                elif existing_daily_max_date is not None and latest_coverage_is_sparse:
                    self._market_db.upsert_daily_valuation_from_adjusted_metrics(
                        basis_version=basis_version,
                        price_basis_date=price_basis_date,
                        codes=None,
                        start_date=existing_daily_max_date,
                        start_date_inclusive=True,
                        replace_existing=False,
                    )
                elif sales_materialization_is_stale:
                    self._market_db.upsert_daily_valuation_from_adjusted_metrics(
                        basis_version=basis_version,
                        price_basis_date=price_basis_date,
                        codes=None,
                        start_date=None,
                        start_date_inclusive=True,
                        replace_existing=False,
                    )
            else:
                start_date = changed_start_date
                start_date_inclusive = True
                should_upsert_daily_valuation = True
                if sales_materialization_is_stale:
                    start_date = None
                    start_date_inclusive = True
                elif start_date is None and existing_daily_max_date is not None:
                    if existing_daily_max_date < latest_price_basis_date:
                        start_date = existing_daily_max_date
                        start_date_inclusive = False
                    elif latest_coverage_is_sparse:
                        start_date = existing_daily_max_date
                        start_date_inclusive = True
                    else:
                        should_upsert_daily_valuation = False
                if should_upsert_daily_valuation:
                    self._market_db.upsert_daily_valuation_from_adjusted_metrics(
                        basis_version=basis_version,
                        price_basis_date=price_basis_date,
                        codes=codes,
                        start_date=start_date,
                        start_date_inclusive=start_date_inclusive,
                        replace_existing=not reuse_existing_basis,
                    )
            stored_daily_rows = self._daily_valuation_count(basis_version, codes)
        stored_daily_technical_rows = (
            self._market_db.rebuild_daily_technical_metrics_from_stock_data()
            if codes is None and self._market_db._table_exists("stock_data")
            else 0
        )
        self._market_db.prune_adjusted_metric_basis_versions(
            basis_version=basis_version,
            codes=codes,
        )
        daily_valuation_latest_date = self._latest_daily_valuation_date(
            basis_version,
            codes,
        )
        return AdjustedMetricsBuildResult(
            statement_rows=stored_statement_rows,
            daily_valuation_rows=stored_daily_rows,
            daily_technical_metric_rows=stored_daily_technical_rows,
            daily_valuation_latest_date=daily_valuation_latest_date,
            price_basis_date=price_basis_date,
            basis_version=basis_version,
        )

    def _resolve_materialization_basis(
        self,
        codes: list[str] | None,
        latest_price_basis_date: str,
    ) -> tuple[str, str, bool]:
        snapshot = self._market_db.get_adjusted_metrics_snapshot()
        existing_basis = snapshot.get("basisVersion")
        existing_price_basis_date = snapshot.get("priceBasisDate")
        if (
            codes is None
            and isinstance(existing_basis, str)
            and existing_basis.startswith("adjusted-v1:")
            and isinstance(existing_price_basis_date, str)
            and existing_price_basis_date <= latest_price_basis_date
            and not self._has_adjustment_events_after(
                start=existing_price_basis_date,
                end=latest_price_basis_date,
                codes=None,
            )
        ):
            return existing_basis, existing_price_basis_date, True
        return (
            f"adjusted-v1:{latest_price_basis_date}",
            latest_price_basis_date,
            False,
        )

    def _has_adjustment_events_after(
        self,
        *,
        start: str,
        end: str,
        codes: list[str] | None,
    ) -> bool:
        if not self._market_db._table_exists("stock_data_raw"):
            return False
        code_where, params = _code_filter("code", codes, prefix="AND")
        row = self._market_db._fetchone(
            f"""
            SELECT 1
            FROM stock_data_raw
            WHERE adjustment_factor IS NOT NULL
              AND adjustment_factor != 1.0
              AND date > ?
              AND date <= ?
              {code_where}
            LIMIT 1
            """,
            [start, end, *params],
        )
        return row is not None

    def _latest_daily_valuation_date(
        self,
        basis_version: str,
        codes: list[str] | None,
    ) -> str | None:
        if not self._market_db._table_exists("daily_valuation"):
            return None
        code_where, params = _code_filter("code", codes, prefix="AND")
        row = self._market_db._fetchone(
            f"""
            SELECT MAX(date)
            FROM daily_valuation
            WHERE basis_version = ?
              {code_where}
            """,
            [basis_version, *params],
        )
        if not row or row[0] is None:
            return None
        return str(row[0])

    def _latest_daily_valuation_coverage_is_sparse(
        self,
        basis_version: str,
        codes: list[str] | None,
    ) -> bool:
        if not self._market_db._table_exists("daily_valuation"):
            return False
        code_where, params = _code_filter("code", codes, prefix="AND")
        row = self._market_db._fetchone(
            f"""
            WITH daily_counts AS (
                SELECT date, COUNT(DISTINCT code) AS code_count
                FROM daily_valuation
                WHERE basis_version = ?
                  {code_where}
                GROUP BY date
            ),
            ranked AS (
                SELECT
                    date,
                    code_count,
                    ROW_NUMBER() OVER (ORDER BY date DESC) AS rn
                FROM daily_counts
            )
            SELECT
                MAX(CASE WHEN rn = 1 THEN code_count END),
                MAX(CASE WHEN rn = 2 THEN code_count END)
            FROM ranked
            WHERE rn <= 2
            """,
            [basis_version, *params],
        )
        if not row:
            return False
        latest_count = int(row[0] or 0)
        previous_count = int(row[1] or 0)
        return previous_count > 0 and latest_count < max(1, int(previous_count * 0.5))

    def _daily_valuation_sales_materialization_is_stale(
        self,
        basis_version: str,
        codes: list[str] | None,
    ) -> bool:
        if (
            not self._market_db._table_exists("daily_valuation")
            or not self._market_db._table_exists("statements")
        ):
            return False
        anchor_code_where, anchor_params = _code_filter("m.code", codes, prefix="AND")
        daily_code_where, daily_params = _code_filter("d.code", codes, prefix="AND")
        row = self._market_db._fetchone(
            f"""
            WITH fy_cycle_anchors AS (
                SELECT m.code, m.disclosed_date
                FROM statement_metrics_adjusted AS m
                LEFT JOIN statements AS s
                  ON s.code = m.code
                 AND s.disclosed_date = m.disclosed_date
                WHERE m.basis_version = ?
                  AND upper(m.period_type) = 'FY'
                  AND (
                      m.adjusted_eps > 0
                      OR m.adjusted_bps > 0
                      OR s.sales > 0
                  )
                  AND (
                      s.type_of_document IS NULL
                      OR s.type_of_document NOT LIKE '%EarnForecastRevision%'
                  )
                  {anchor_code_where}
                ORDER BY m.code, m.disclosed_date
            ),
            actual_sales_metrics AS (
                SELECT st.code, st.disclosed_date, st.sales
                FROM statements AS st
                JOIN fy_cycle_anchors AS fy
                  ON fy.code = st.code
                 AND fy.disclosed_date = st.disclosed_date
                WHERE upper(st.type_of_current_period) = 'FY'
                  AND st.sales IS NOT NULL
                ORDER BY st.code, st.disclosed_date
            ),
            expected_sales AS (
                SELECT
                    d.code,
                    d.date,
                    d.sales AS current_sales,
                    sales.sales AS expected_sales
                FROM daily_valuation AS d
                ASOF LEFT JOIN actual_sales_metrics AS sales
                  ON d.code = sales.code
                 AND d.date >= sales.disclosed_date
                WHERE d.basis_version = ?
                  {daily_code_where}
            )
            SELECT 1
            FROM expected_sales
            WHERE expected_sales IS NOT NULL
              AND current_sales IS NULL
            LIMIT 1
            """,
            [basis_version, *anchor_params, basis_version, *daily_params],
        )
        return row is not None

    def _daily_valuation_count(
        self,
        basis_version: str,
        codes: list[str] | None,
    ) -> int:
        if not self._market_db._table_exists("daily_valuation"):
            return 0
        code_where, params = _code_filter("code", codes, prefix="AND")
        row = self._market_db._fetchone(
            f"""
            SELECT COUNT(*)
            FROM daily_valuation
            WHERE basis_version = ?
              {code_where}
            """,
            [basis_version, *params],
        )
        return int(row[0] or 0) if row else 0

    def _statement_metrics_count(
        self,
        basis_version: str,
        codes: list[str] | None,
    ) -> int:
        if not self._market_db._table_exists("statement_metrics_adjusted"):
            return 0
        code_where, params = _code_filter("code", codes, prefix="AND")
        row = self._market_db._fetchone(
            f"""
            SELECT COUNT(*)
            FROM statement_metrics_adjusted
            WHERE basis_version = ?
              {code_where}
            """,
            [basis_version, *params],
        )
        return int(row[0] or 0) if row else 0

    def _earliest_changed_statement_metric_date(
        self,
        adjusted_payload: list[dict[str, Any]],
        basis_version: str,
        codes: list[str] | None,
    ) -> str | None:
        changed_dates_by_code = self._changed_statement_metric_start_dates(
            adjusted_payload,
            basis_version,
            codes,
        )
        return min(changed_dates_by_code.values()) if changed_dates_by_code else None

    def _changed_statement_metric_start_dates(
        self,
        adjusted_payload: list[dict[str, Any]],
        basis_version: str,
        codes: list[str] | None,
    ) -> dict[str, str]:
        if not adjusted_payload or not self._market_db._table_exists(
            "statement_metrics_adjusted"
        ):
            return {}
        code_where, params = _code_filter("code", codes, prefix="AND")
        existing_rows = self._market_db._fetchall_dicts(
            f"""
            SELECT {', '.join(STATEMENT_METRICS_ADJUSTED_COLUMNS)}
            FROM statement_metrics_adjusted
            WHERE basis_version = ?
              {code_where}
            """,
            [basis_version, *params],
        )
        existing_by_key = {
            _statement_metric_key(row): row
            for row in existing_rows
        }
        changed_dates_by_code: dict[str, str] = {}
        for row in adjusted_payload:
            if not self._statement_metric_changed(row, existing_by_key):
                continue
            code = normalize_stock_code(str(row["code"]))
            disclosed_date = str(row["disclosed_date"])
            current = changed_dates_by_code.get(code)
            if current is None or disclosed_date < current:
                changed_dates_by_code[code] = disclosed_date
        return changed_dates_by_code

    @staticmethod
    def _statement_metric_changed(
        row: dict[str, Any],
        existing_by_key: dict[tuple[Any, ...], dict[str, Any]],
    ) -> bool:
        existing = existing_by_key.get(_statement_metric_key(row))
        if existing is None:
            return True
        return any(
            existing.get(column) != row.get(column)
            for column in _STATEMENT_METRIC_COMPARE_COLUMNS
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
                CASE
                    WHEN type_of_document LIKE '%EarnForecastRevision%'
                    THEN COALESCE(forecast_eps, next_year_forecast_earnings_per_share)
                    WHEN upper(type_of_current_period) = 'FY'
                    THEN COALESCE(next_year_forecast_earnings_per_share, forecast_eps)
                    ELSE forecast_eps
                END AS forecast_eps,
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
                    sales=None,
                    forward_sales=None,
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
                    forward_sales_disclosed_date=None,
                    forward_sales_source=None,
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


def _statement_metric_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in _STATEMENT_METRIC_KEY_COLUMNS)


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
        "sales": metric.sales,
        "forward_sales": metric.forward_sales,
        "psr": metric.psr,
        "forward_psr": metric.forward_psr,
        "p_op": metric.p_op,
        "forward_p_op": metric.forward_p_op,
        "pbr": metric.pbr,
        "market_cap": metric.market_cap,
        "free_float_market_cap": metric.free_float_market_cap,
        "statement_disclosed_date": metric.statement_disclosed_date,
        "forward_eps_disclosed_date": metric.forward_eps_disclosed_date,
        "forward_eps_source": metric.forward_eps_source,
        "forward_sales_disclosed_date": metric.forward_sales_disclosed_date,
        "forward_sales_source": metric.forward_sales_source,
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
