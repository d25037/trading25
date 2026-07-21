"""Canonical adjusted fundamentals and valuation metric builders."""

from __future__ import annotations

from dataclasses import dataclass
from src.shared.utils.share_adjustment import (
    ShareAdjustmentEvent,
    adjust_share_count_to_price_basis,
    cumulative_adjustment_factor_after,
)


@dataclass(frozen=True)
class AdjustedStatementInput:
    code: str
    statement_id: str
    disclosed_date: str
    disclosed_at: str
    period_end: str
    period_type: str
    eps: float | None = None
    diluted_eps: float | None = None
    bps: float | None = None
    forecast_eps: float | None = None
    dividend_fy: float | None = None
    forecast_dividend_fy: float | None = None
    shares_outstanding: float | None = None
    treasury_shares: float | None = None


@dataclass(frozen=True)
class AdjustedStatementMetric:
    code: str
    statement_id: str
    disclosed_date: str
    disclosed_at: str
    period_end: str
    period_type: str
    fundamentals_adjustment_basis_date: str
    raw_eps: float | None
    adjusted_eps: float | None
    raw_diluted_eps: float | None
    adjusted_diluted_eps: float | None
    raw_bps: float | None
    adjusted_bps: float | None
    raw_forecast_eps: float | None
    adjusted_forecast_eps: float | None
    raw_dividend_fy: float | None
    adjusted_dividend_fy: float | None
    raw_forecast_dividend_fy: float | None
    adjusted_forecast_dividend_fy: float | None
    raw_shares_outstanding: float | None
    adjusted_shares_outstanding: float | None
    raw_treasury_shares: float | None
    adjusted_treasury_shares: float | None
    adjustment_factor_cumulative: float
    source_fingerprint: str


def build_adjusted_statement_metric(
    statement: AdjustedStatementInput,
    *,
    events: list[ShareAdjustmentEvent],
    fundamentals_adjustment_basis_date: str,
    source_fingerprint: str,
) -> AdjustedStatementMetric:
    adjustment_factor = cumulative_adjustment_factor_after(
        events,
        from_date=statement.disclosed_date,
        through_date=fundamentals_adjustment_basis_date,
    )
    adjusted_shares = adjust_share_count_to_price_basis(
        statement.shares_outstanding,
        events,
        from_date=statement.disclosed_date,
        through_date=fundamentals_adjustment_basis_date,
    )
    adjusted_treasury_shares = adjust_share_count_to_price_basis(
        statement.treasury_shares or 0.0,
        events,
        from_date=statement.disclosed_date,
        through_date=fundamentals_adjustment_basis_date,
        allow_zero=True,
    )

    return AdjustedStatementMetric(
        code=statement.code,
        statement_id=statement.statement_id,
        disclosed_date=statement.disclosed_date,
        disclosed_at=statement.disclosed_at,
        period_end=statement.period_end,
        period_type=statement.period_type,
        fundamentals_adjustment_basis_date=fundamentals_adjustment_basis_date,
        raw_eps=statement.eps,
        adjusted_eps=_adjust_per_share_value(statement.eps, adjustment_factor),
        raw_diluted_eps=statement.diluted_eps,
        adjusted_diluted_eps=_adjust_per_share_value(
            statement.diluted_eps, adjustment_factor
        ),
        raw_bps=statement.bps,
        adjusted_bps=_adjust_per_share_value(statement.bps, adjustment_factor),
        raw_forecast_eps=statement.forecast_eps,
        adjusted_forecast_eps=_adjust_per_share_value(
            statement.forecast_eps,
            adjustment_factor,
        ),
        raw_dividend_fy=statement.dividend_fy,
        adjusted_dividend_fy=_adjust_per_share_value(
            statement.dividend_fy,
            adjustment_factor,
        ),
        raw_forecast_dividend_fy=statement.forecast_dividend_fy,
        adjusted_forecast_dividend_fy=_adjust_per_share_value(
            statement.forecast_dividend_fy,
            adjustment_factor,
        ),
        raw_shares_outstanding=statement.shares_outstanding,
        adjusted_shares_outstanding=adjusted_shares,
        raw_treasury_shares=statement.treasury_shares,
        adjusted_treasury_shares=adjusted_treasury_shares,
        adjustment_factor_cumulative=adjustment_factor,
        source_fingerprint=source_fingerprint,
    )


def _adjust_per_share_value(value: float | None, adjustment_factor: float) -> float | None:
    if value is None:
        return None
    return value * adjustment_factor
