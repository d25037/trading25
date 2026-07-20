"""Canonical adjusted fundamentals and valuation metric builders."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from src.domains.fundamentals.valuation_primitives import (
    market_cap_from_price_and_shares,
    valuation_ratio,
)
from src.shared.utils.share_adjustment import (
    ShareAdjustmentEvent,
    adjust_share_count_to_price_basis,
    cumulative_adjustment_factor_after,
)


ForwardEpsSource = Literal["revised", "fy"]


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


@dataclass(frozen=True)
class DailyValuationInput:
    code: str
    date: str
    price_basis_date: str
    close: float
    eps: float | None
    bps: float | None
    forward_eps: float | None
    sales: float | None
    forward_sales: float | None
    operating_profit: float | None
    forward_operating_profit: float | None
    shares_outstanding: float | None
    treasury_shares: float | None
    statement_disclosed_date: str | None
    forward_eps_disclosed_date: str | None
    forward_eps_source: ForwardEpsSource | None
    forward_sales_disclosed_date: str | None
    forward_sales_source: ForwardEpsSource | None
    basis_version: str


@dataclass(frozen=True)
class DailyValuationMetric:
    code: str
    date: str
    price_basis_date: str
    close: float
    eps: float | None
    bps: float | None
    forward_eps: float | None
    per: float | None
    forward_per: float | None
    sales: float | None
    forward_sales: float | None
    psr: float | None
    forward_psr: float | None
    p_op: float | None
    forward_p_op: float | None
    pbr: float | None
    market_cap: float | None
    free_float_market_cap: float | None
    statement_disclosed_date: str | None
    forward_eps_disclosed_date: str | None
    forward_eps_source: ForwardEpsSource | None
    forward_sales_disclosed_date: str | None
    forward_sales_source: ForwardEpsSource | None
    basis_version: str


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


def build_daily_valuation_metric(
    valuation: DailyValuationInput,
) -> DailyValuationMetric:
    free_float_shares = _free_float_shares(
        valuation.shares_outstanding,
        valuation.treasury_shares,
    )
    market_cap = market_cap_from_price_and_shares(
        valuation.close,
        valuation.shares_outstanding,
    )
    return DailyValuationMetric(
        code=valuation.code,
        date=valuation.date,
        price_basis_date=valuation.price_basis_date,
        close=valuation.close,
        eps=valuation.eps,
        bps=valuation.bps,
        forward_eps=valuation.forward_eps,
        per=valuation_ratio(valuation.close, valuation.eps),
        forward_per=valuation_ratio(valuation.close, valuation.forward_eps),
        sales=valuation.sales,
        forward_sales=valuation.forward_sales,
        psr=valuation_ratio(market_cap, valuation.sales),
        forward_psr=valuation_ratio(market_cap, valuation.forward_sales),
        p_op=valuation_ratio(market_cap, valuation.operating_profit),
        forward_p_op=valuation_ratio(market_cap, valuation.forward_operating_profit),
        pbr=valuation_ratio(valuation.close, valuation.bps),
        market_cap=market_cap,
        free_float_market_cap=market_cap_from_price_and_shares(
            valuation.close,
            free_float_shares,
        ),
        statement_disclosed_date=valuation.statement_disclosed_date,
        forward_eps_disclosed_date=valuation.forward_eps_disclosed_date,
        forward_eps_source=valuation.forward_eps_source,
        forward_sales_disclosed_date=valuation.forward_sales_disclosed_date,
        forward_sales_source=valuation.forward_sales_source,
        basis_version=valuation.basis_version,
    )


def _adjust_per_share_value(value: float | None, adjustment_factor: float) -> float | None:
    if value is None:
        return None
    return value * adjustment_factor


def _free_float_shares(
    shares_outstanding: float | None,
    treasury_shares: float | None,
) -> float | None:
    if shares_outstanding is None:
        return None
    if treasury_shares is None:
        return shares_outstanding
    free_float = shares_outstanding - treasury_shares
    return free_float if free_float > 0 else None
