"""Rebuild current-provider-basis adjusted fundamentals for affected codes."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from src.domains.fundamentals.adjusted_metrics import (
    AdjustedStatementInput,
    AdjustedStatementMetric,
    build_adjusted_statement_metric,
)
from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.market_mutations import MarketMutationStats
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.shared.utils.share_adjustment import ShareAdjustmentEvent


@dataclass(frozen=True)
class AdjustedMetricsBuildResult:
    completed_codes: int
    total_codes: int
    current_basis_statement_count: int
    pending_current_basis_code_count: int
    daily_valuation_rows: int
    daily_technical_metric_rows: int
    daily_valuation_latest_date: str | None
    fundamentals_adjustment_basis_date: str | None
    plan_counts: dict[str, int] = field(default_factory=dict, compare=False)
    mutation_stats: dict[str, MarketMutationStats] = field(
        default_factory=dict, compare=False
    )
    final_semantic_counts: dict[str, int] = field(default_factory=dict, compare=False)


class MissingCurrentProviderBasisError(RuntimeError):
    """Raised when an affected code has no current provider window."""


class AdjustedMetricsMaterializer:
    """Reconcile current-basis statement metrics for explicit affected codes only."""

    def __init__(self, market_db: MarketDb) -> None:
        self._market_db = market_db

    def rebuild_current_basis(
        self,
        codes: Iterable[str],
        *,
        cancel_requested: Callable[[], bool] | None = None,
        on_progress: Callable[[int, int, str | None, int], None] | None = None,
    ) -> AdjustedMetricsBuildResult:
        requested_codes = {
            normalize_stock_code(code) for code in codes if normalize_stock_code(code)
        }
        target_codes = sorted(
            requested_codes
            | {
                normalize_stock_code(code)
                for code in self._market_db.list_current_basis_recompute_pending_codes()
                if normalize_stock_code(code)
            }
        )
        completed_codes = 0
        statement_rows = 0
        active_basis_date: str | None = None
        aggregate_stats = MarketMutationStats.empty()
        final_count = 0
        for code in target_codes:
            if cancel_requested is not None and cancel_requested():
                break
            if on_progress is not None:
                on_progress(completed_codes, len(target_codes), code, statement_rows)
            source = self._market_db.load_current_basis_fundamentals_source(code)
            if source is None:
                raise MissingCurrentProviderBasisError(
                    f"current provider basis is missing for affected code {code}"
                )
            events = [
                ShareAdjustmentEvent(
                    date=str(row["date"]),
                    adjustment_factor=float(row["adjustment_factor"]),
                )
                for row in source.adjustment_events
            ]
            rows = [
                _statement_metric_row(
                    build_adjusted_statement_metric(
                        _statement_input(row),
                        events=events,
                        fundamentals_adjustment_basis_date=(
                            source.fundamentals_adjustment_basis_date
                        ),
                        source_fingerprint=source.fingerprint,
                    )
                )
                for row in source.statement_rows
            ]
            publish = self._market_db.publish_current_basis_statement_metrics(
                code,
                rows,
                expected_source_fingerprint=source.fingerprint,
            )
            aggregate_stats = _add_stats(aggregate_stats, publish.stats)
            final_count += publish.final_count
            statement_rows += len(rows)
            completed_codes += 1
            active_basis_date = max(
                active_basis_date or "",
                source.fundamentals_adjustment_basis_date,
            )
            if on_progress is not None:
                on_progress(completed_codes, len(target_codes), code, statement_rows)

        return AdjustedMetricsBuildResult(
            completed_codes=completed_codes,
            total_codes=len(target_codes),
            current_basis_statement_count=statement_rows,
            pending_current_basis_code_count=(len(target_codes) - completed_codes),
            daily_valuation_rows=0,
            daily_technical_metric_rows=0,
            daily_valuation_latest_date=None,
            fundamentals_adjustment_basis_date=active_basis_date or None,
            mutation_stats={"statements": aggregate_stats},
            final_semantic_counts={"statements": final_count},
        )

def _statement_input(row: dict[str, Any]) -> AdjustedStatementInput:
    period_type = str(row.get("type_of_current_period") or "")
    document_type = str(row.get("type_of_document") or "")
    is_revision = "ForecastRevision" in document_type
    if is_revision:
        forecast_eps = row.get("forecast_eps")
        forecast_dividend = row.get("forecast_dividend_fy")
    elif period_type.upper() == "FY":
        forecast_eps = row.get("next_year_forecast_earnings_per_share")
        forecast_dividend = row.get("next_year_forecast_dividend_fy")
        if forecast_eps is None:
            forecast_eps = row.get("forecast_eps")
        if forecast_dividend is None:
            forecast_dividend = row.get("forecast_dividend_fy")
    else:
        forecast_eps = row.get("forecast_eps")
        forecast_dividend = row.get("forecast_dividend_fy")
    return AdjustedStatementInput(
        code=normalize_stock_code(str(row["code"])),
        statement_id=str(row["statement_id"]),
        disclosed_date=str(row["disclosed_date"]),
        disclosed_at=str(row["disclosed_at"]),
        period_end=str(row["period_end"]),
        period_type=period_type,
        eps=_optional_float(row.get("earnings_per_share")),
        diluted_eps=_optional_float(row.get("diluted_earnings_per_share")),
        bps=_optional_float(row.get("bps")),
        forecast_eps=_optional_float(
            forecast_eps
            if forecast_eps is not None
            else row.get("next_year_forecast_earnings_per_share")
        ),
        dividend_fy=_optional_float(row.get("dividend_fy")),
        forecast_dividend_fy=_optional_float(
            forecast_dividend
            if forecast_dividend is not None
            else row.get("next_year_forecast_dividend_fy")
        ),
        shares_outstanding=_optional_float(row.get("shares_outstanding")),
        treasury_shares=_optional_float(row.get("treasury_shares")),
    )


def _statement_metric_row(metric: AdjustedStatementMetric) -> dict[str, Any]:
    return {
        "code": metric.code,
        "statement_id": metric.statement_id,
        "disclosed_date": metric.disclosed_date,
        "disclosed_at": metric.disclosed_at,
        "period_end": metric.period_end,
        "period_type": metric.period_type,
        "fundamentals_adjustment_basis_date": (
            metric.fundamentals_adjustment_basis_date
        ),
        "raw_eps": metric.raw_eps,
        "adjusted_eps": metric.adjusted_eps,
        "raw_diluted_eps": metric.raw_diluted_eps,
        "adjusted_diluted_eps": metric.adjusted_diluted_eps,
        "raw_bps": metric.raw_bps,
        "adjusted_bps": metric.adjusted_bps,
        "raw_forecast_eps": metric.raw_forecast_eps,
        "adjusted_forecast_eps": metric.adjusted_forecast_eps,
        "raw_dividend_fy": metric.raw_dividend_fy,
        "adjusted_dividend_fy": metric.adjusted_dividend_fy,
        "raw_forecast_dividend_fy": metric.raw_forecast_dividend_fy,
        "adjusted_forecast_dividend_fy": metric.adjusted_forecast_dividend_fy,
        "raw_shares_outstanding": metric.raw_shares_outstanding,
        "adjusted_shares_outstanding": metric.adjusted_shares_outstanding,
        "raw_treasury_shares": metric.raw_treasury_shares,
        "adjusted_treasury_shares": metric.adjusted_treasury_shares,
        "adjustment_factor_cumulative": metric.adjustment_factor_cumulative,
        "source_fingerprint": metric.source_fingerprint,
    }


def _optional_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _add_stats(left: MarketMutationStats, right: MarketMutationStats) -> MarketMutationStats:
    return MarketMutationStats(
        input=left.input + right.input,
        inserted=left.inserted + right.inserted,
        updated=left.updated + right.updated,
        unchanged=left.unchanged + right.unchanged,
        deleted=left.deleted + right.deleted,
    )
