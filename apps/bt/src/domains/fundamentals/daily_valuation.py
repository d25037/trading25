"""Daily valuation helpers for fundamentals calculations."""

from __future__ import annotations

from typing import Literal, Protocol

from src.infrastructure.external_api.jquants_client import JQuantsStatement
from src.shared.models.types import normalize_period_type
from src.shared.utils.financial import calc_market_cap_scalar
from src.shared.utils.share_adjustment import ShareAdjustmentEvent, ShareCountSnapshot

from .models import DailyValuationDataPoint, FYDataPoint
from .valuation_primitives import market_cap_from_price_and_shares, valuation_ratio


class DailyValuationOps(Protocol):
    def _resolve_baseline_share_snapshot_from_latest_quarter(
        self, statements: list[JQuantsStatement]
    ) -> ShareCountSnapshot | None: ...

    def _resolve_latest_treasury_share_snapshot_from_latest_quarter(
        self, statements: list[JQuantsStatement]
    ) -> ShareCountSnapshot | None: ...

    def _adjust_snapshot_shares_to_price_basis(
        self,
        snapshot: ShareCountSnapshot | None,
        share_adjustment_events: list[ShareAdjustmentEvent],
        *,
        through_date: str | None,
        allow_zero: bool = False,
    ) -> float | None: ...

    def _is_actual_fy_statement(self, stmt: JQuantsStatement) -> bool: ...
    def _calculate_eps(self, stmt: JQuantsStatement, prefer_consolidated: bool) -> float | None: ...
    def _calculate_bps(self, stmt: JQuantsStatement, prefer_consolidated: bool) -> float | None: ...
    def _get_operating_profit(self, stmt: JQuantsStatement, prefer_consolidated: bool) -> float | None: ...
    def _get_forecast_eps(
        self,
        stmt: JQuantsStatement,
        actual_eps: float | None,
        prefer_consolidated: bool,
    ) -> tuple[float | None, float | None]: ...

    def _get_forecast_operating_profit(
        self,
        stmt: JQuantsStatement,
        actual_operating_profit: float | None,
        prefer_consolidated: bool,
    ) -> tuple[float | None, float | None]: ...

    def _compute_adjusted_value(
        self,
        value: float | None,
        current_shares: float | None,
        base_shares: float | None,
    ) -> float | None: ...

    def _round_or_none(self, value: float | None) -> float | None: ...


def calculate_daily_valuation(
    ops: DailyValuationOps,
    statements: list[JQuantsStatement],
    daily_prices: dict[str, float],
    prefer_consolidated: bool,
    share_adjustment_events: list[ShareAdjustmentEvent] | None = None,
    price_basis_date: str | None = None,
) -> list[DailyValuationDataPoint]:
    if not daily_prices:
        return []

    baseline_snapshot = ops._resolve_baseline_share_snapshot_from_latest_quarter(statements)
    treasury_snapshot = ops._resolve_latest_treasury_share_snapshot_from_latest_quarter(
        statements
    )
    adjustment_events = share_adjustment_events or []

    result: list[DailyValuationDataPoint] = []
    sorted_dates = sorted(daily_prices.keys())
    effective_price_basis_date = price_basis_date or sorted_dates[-1]
    for date_str in sorted_dates:
        close = daily_prices[date_str]
        baseline_shares = ops._adjust_snapshot_shares_to_price_basis(
            baseline_snapshot,
            adjustment_events,
            through_date=effective_price_basis_date,
        )
        baseline_treasury_shares = ops._adjust_snapshot_shares_to_price_basis(
            treasury_snapshot,
            adjustment_events,
            through_date=effective_price_basis_date,
            allow_zero=True,
        )
        fy_data_points = get_applicable_fy_data(
            ops, statements, prefer_consolidated, baseline_shares
        )
        if not fy_data_points:
            continue
        applicable_fy = find_applicable_fy(fy_data_points, date_str)
        if applicable_fy is None:
            continue

        forward_eps, forward_eps_disclosed_date, forward_eps_source = (
            resolve_forward_eps_for_daily_valuation(
                ops,
                statements,
                applicable_fy,
                prefer_consolidated,
                baseline_shares,
                date_str,
            )
        )
        per = ops._round_or_none(valuation_ratio(close, applicable_fy.eps))
        forward_per = ops._round_or_none(valuation_ratio(close, forward_eps))
        pbr = ops._round_or_none(valuation_ratio(close, applicable_fy.bps))
        market_cap = ops._round_or_none(
            market_cap_from_price_and_shares(close, baseline_shares)
        )
        forward_operating_profit = resolve_forward_operating_profit_for_daily_valuation(
            ops,
            statements,
            applicable_fy,
            prefer_consolidated,
            date_str,
        )
        p_op = ops._round_or_none(
            valuation_ratio(market_cap, applicable_fy.operating_profit)
        )
        forward_p_op = ops._round_or_none(
            valuation_ratio(market_cap, forward_operating_profit)
        )
        free_float_market_cap = None
        if baseline_shares is not None:
            free_float_market_cap = ops._round_or_none(
                calc_market_cap_scalar(close, baseline_shares, baseline_treasury_shares)
            )

        result.append(
            DailyValuationDataPoint(
                date=date_str,
                close=close,
                per=per,
                forwardPer=forward_per,
                pOp=p_op,
                forwardPOp=forward_p_op,
                pbr=pbr,
                marketCap=market_cap,
                freeFloatMarketCap=free_float_market_cap,
                forwardEps=forward_eps,
                forwardEpsDisclosedDate=forward_eps_disclosed_date,
                forwardEpsSource=forward_eps_source,
            )
        )
    return result


def get_applicable_fy_data(
    ops: DailyValuationOps,
    statements: list[JQuantsStatement],
    prefer_consolidated: bool,
    baseline_shares: float | None,
) -> list[FYDataPoint]:
    fy_data: list[FYDataPoint] = []
    for stmt in statements:
        if not ops._is_actual_fy_statement(stmt):
            continue
        eps = ops._calculate_eps(stmt, prefer_consolidated)
        bps = ops._calculate_bps(stmt, prefer_consolidated)
        operating_profit = ops._get_operating_profit(stmt, prefer_consolidated)
        forecast_eps, _ = ops._get_forecast_eps(stmt, eps, prefer_consolidated)
        forecast_operating_profit, _ = ops._get_forecast_operating_profit(
            stmt,
            operating_profit,
            prefer_consolidated,
        )
        adjusted_eps = ops._compute_adjusted_value(eps, stmt.ShOutFY, baseline_shares)
        adjusted_bps = ops._compute_adjusted_value(bps, stmt.ShOutFY, baseline_shares)
        adjusted_forecast_eps = ops._compute_adjusted_value(
            forecast_eps,
            stmt.ShOutFY,
            baseline_shares,
        )
        display_eps = adjusted_eps if adjusted_eps is not None else eps
        display_bps = adjusted_bps if adjusted_bps is not None else bps
        display_forecast_eps = (
            adjusted_forecast_eps
            if adjusted_forecast_eps is not None
            else ops._round_or_none(forecast_eps)
        )
        if not has_valid_valuation_metrics(display_eps, display_bps):
            continue
        fy_data.append(
            FYDataPoint(
                disclosed_date=stmt.DiscDate,
                eps=display_eps,
                bps=display_bps,
                operating_profit=operating_profit,
                forward_eps=display_forecast_eps,
                forward_operating_profit=forecast_operating_profit,
                forward_eps_disclosed_date=(
                    stmt.DiscDate if display_forecast_eps is not None else None
                ),
                forward_eps_source="fy" if display_forecast_eps is not None else None,
            )
        )

    fy_data.sort(key=lambda x: x.disclosed_date)
    return fy_data


def has_valid_valuation_metrics(eps: float | None, bps: float | None) -> bool:
    eps_valid = eps is not None and eps > 0
    bps_valid = bps is not None and bps > 0
    return eps_valid or bps_valid


def resolve_forward_eps_for_daily_valuation(
    ops: DailyValuationOps,
    statements: list[JQuantsStatement],
    applicable_fy: FYDataPoint,
    prefer_consolidated: bool,
    baseline_shares: float | None,
    date_str: str,
) -> tuple[float | None, str | None, Literal["revised", "fy"] | None]:
    quarterly_statements = [
        stmt
        for stmt in statements
        if normalize_period_type(stmt.CurPerType) in {"1Q", "2Q", "3Q"}
        and applicable_fy.disclosed_date < stmt.DiscDate <= date_str
    ]
    quarterly_statements.sort(key=lambda stmt: stmt.DiscDate, reverse=True)
    for stmt in quarterly_statements:
        forecast_eps = stmt.FEPS if prefer_consolidated else stmt.FNCEPS
        if forecast_eps is None:
            continue
        adjusted_forecast_eps = ops._compute_adjusted_value(
            forecast_eps,
            stmt.ShOutFY,
            baseline_shares,
        )
        return (
            adjusted_forecast_eps
            if adjusted_forecast_eps is not None
            else ops._round_or_none(forecast_eps),
            stmt.DiscDate,
            "revised",
        )

    return (
        applicable_fy.forward_eps,
        applicable_fy.forward_eps_disclosed_date,
        applicable_fy.forward_eps_source,
    )


def resolve_forward_operating_profit_for_daily_valuation(
    _ops: DailyValuationOps,
    statements: list[JQuantsStatement],
    applicable_fy: FYDataPoint,
    prefer_consolidated: bool,
    date_str: str,
) -> float | None:
    if not prefer_consolidated:
        return None
    quarterly_statements = [
        stmt
        for stmt in statements
        if normalize_period_type(stmt.CurPerType) in {"1Q", "2Q", "3Q"}
        and applicable_fy.disclosed_date < stmt.DiscDate <= date_str
    ]
    quarterly_statements.sort(key=lambda stmt: stmt.DiscDate, reverse=True)
    for stmt in quarterly_statements:
        if stmt.FOP is not None:
            return stmt.FOP
    return applicable_fy.forward_operating_profit


def find_applicable_fy(
    fy_data_points: list[FYDataPoint], date_str: str
) -> FYDataPoint | None:
    applicable_fy: FYDataPoint | None = None
    for fy in fy_data_points:
        if fy.disclosed_date <= date_str:
            applicable_fy = fy
        else:
            break
    return applicable_fy
