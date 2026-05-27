"""Share-adjusted fundamentals helpers."""

from __future__ import annotations

import math
from collections.abc import Callable

from src.infrastructure.external_api.jquants_client import JQuantsStatement
from src.shared.models.types import normalize_period_type
from src.shared.utils.share_adjustment import (
    ShareAdjustmentEvent,
    ShareCountSnapshot,
    adjust_share_count_to_price_basis,
    is_valid_share_count,
    resolve_latest_quarterly_baseline_shares,
    resolve_latest_quarterly_share_snapshot,
)

from .models import FundamentalDataPoint

RoundFunc = Callable[[float | None], float | None]
RatioFunc = Callable[[float | None, float | None], float | None]
ChangeRateFunc = Callable[[float | None, float | None], float | None]
HasActualFinancialDataFunc = Callable[[FundamentalDataPoint], bool]


def is_valid_share_metric(value: float | None, *, allow_zero: bool = False) -> bool:
    if value is None or not math.isfinite(value):
        return False
    if allow_zero:
        return value >= 0
    return value > 0


def build_shares_map(
    statements: list[JQuantsStatement],
) -> dict[tuple[str, str, str | None], float | None]:
    shares_map: dict[tuple[str, str, str | None], float | None] = {}
    for stmt in statements:
        period_type = normalize_period_type(stmt.CurPerType)
        key = (stmt.CurPerEn, stmt.DiscDate, period_type)
        shares_map[key] = stmt.ShOutFY
    return shares_map


def get_shares_for_datapoint(
    data_point: FundamentalDataPoint,
    shares_map: dict[tuple[str, str, str | None], float | None],
) -> float | None:
    period_type = normalize_period_type(data_point.periodType)
    key = (data_point.date, data_point.disclosedDate, period_type)
    return shares_map.get(key)


def resolve_baseline_shares_from_latest_quarter(
    statements: list[JQuantsStatement],
) -> float | None:
    snapshots = [(stmt.CurPerType, stmt.DiscDate, stmt.ShOutFY) for stmt in statements]
    return resolve_latest_quarterly_baseline_shares(snapshots)


def resolve_baseline_share_snapshot_from_latest_quarter(
    statements: list[JQuantsStatement],
) -> ShareCountSnapshot | None:
    snapshots = [(stmt.CurPerType, stmt.DiscDate, stmt.ShOutFY) for stmt in statements]
    return resolve_latest_quarterly_share_snapshot(snapshots)


def resolve_latest_treasury_shares_from_latest_quarter(
    statements: list[JQuantsStatement],
) -> float | None:
    snapshot = resolve_latest_treasury_share_snapshot_from_latest_quarter(statements)
    return snapshot.shares if snapshot is not None else None


def resolve_latest_treasury_share_snapshot_from_latest_quarter(
    statements: list[JQuantsStatement],
) -> ShareCountSnapshot | None:
    latest_quarter_key: str | None = None
    latest_quarter_snapshot: ShareCountSnapshot | None = None
    latest_any_key: str | None = None
    latest_any_snapshot: ShareCountSnapshot | None = None

    for stmt in statements:
        treasury_shares = stmt.TrShFY
        if not is_valid_share_metric(treasury_shares, allow_zero=True):
            continue
        assert treasury_shares is not None

        disclosed_key = str(stmt.DiscDate) if stmt.DiscDate is not None else ""
        treasury_value = float(treasury_shares)
        normalized_period = normalize_period_type(stmt.CurPerType)
        snapshot = ShareCountSnapshot(
            period_type=normalized_period,
            disclosed_date=disclosed_key,
            shares=treasury_value,
        )

        if normalized_period in {"1Q", "2Q", "3Q"}:
            if latest_quarter_key is None or disclosed_key > latest_quarter_key:
                latest_quarter_key = disclosed_key
                latest_quarter_snapshot = snapshot

        if latest_any_key is None or disclosed_key > latest_any_key:
            latest_any_key = disclosed_key
            latest_any_snapshot = snapshot

    return latest_quarter_snapshot if latest_quarter_snapshot is not None else latest_any_snapshot


def adjust_snapshot_shares_to_price_basis(
    snapshot: ShareCountSnapshot | None,
    share_adjustment_events: list[ShareAdjustmentEvent],
    *,
    through_date: str | None,
    allow_zero: bool = False,
) -> float | None:
    if snapshot is None:
        return None
    return adjust_share_count_to_price_basis(
        snapshot.shares,
        share_adjustment_events,
        from_date=snapshot.disclosed_date,
        through_date=through_date,
        allow_zero=allow_zero,
    )


def compute_adjusted_value(
    value: float | None,
    current_shares: float | None,
    base_shares: float | None,
    *,
    round_or_none: RoundFunc,
) -> float | None:
    if (
        value is None
        or not is_valid_share_count(current_shares)
        or not is_valid_share_count(base_shares)
    ):
        return None
    assert current_shares is not None
    assert base_shares is not None
    adjusted = value * (current_shares / base_shares)
    return round_or_none(adjusted)


def build_adjusted_datapoint(
    item: FundamentalDataPoint,
    eps_shares: float | None,
    bps_shares: float | None,
    forecast_shares: float | None,
    dividend_shares: float | None,
    base_shares: float | None,
    *,
    round_or_none: RoundFunc,
    calculate_per: RatioFunc,
    calculate_pbr: RatioFunc,
    calculate_change_rate: ChangeRateFunc,
) -> FundamentalDataPoint:
    adjusted_eps = compute_adjusted_value(
        item.eps, eps_shares, base_shares, round_or_none=round_or_none
    )
    adjusted_bps = compute_adjusted_value(
        item.bps, bps_shares, base_shares, round_or_none=round_or_none
    )
    adjusted_forecast = compute_adjusted_value(
        item.forecastEps, forecast_shares, base_shares, round_or_none=round_or_none
    )
    adjusted_dividend = compute_adjusted_value(
        item.dividendFy, dividend_shares, base_shares, round_or_none=round_or_none
    )
    adjusted_forecast_dividend = compute_adjusted_value(
        item.forecastDividendFy,
        dividend_shares,
        base_shares,
        round_or_none=round_or_none,
    )
    display_eps = adjusted_eps if adjusted_eps is not None else item.eps
    display_bps = adjusted_bps if adjusted_bps is not None else item.bps
    display_dividend = adjusted_dividend if adjusted_dividend is not None else item.dividendFy
    display_forecast_dividend = (
        adjusted_forecast_dividend
        if adjusted_forecast_dividend is not None
        else item.forecastDividendFy
    )
    forecast_dividend_change_rate = calculate_change_rate(
        display_dividend, display_forecast_dividend
    )
    forecast_payout_change_rate = calculate_change_rate(
        item.payoutRatio, item.forecastPayoutRatio
    )

    return FundamentalDataPoint(
        **{
            **item.model_dump(),
            "adjustedEps": adjusted_eps,
            "adjustedForecastEps": adjusted_forecast,
            "adjustedBps": adjusted_bps,
            "adjustedDividendFy": adjusted_dividend,
            "adjustedForecastDividendFy": adjusted_forecast_dividend,
            "forecastDividendFyChangeRate": round_or_none(forecast_dividend_change_rate),
            "forecastPayoutRatioChangeRate": round_or_none(forecast_payout_change_rate),
            "per": round_or_none(calculate_per(display_eps, item.stockPrice)),
            "pbr": round_or_none(calculate_pbr(display_bps, item.stockPrice)),
        }
    )


def apply_share_adjustments(
    data: list[FundamentalDataPoint],
    statements: list[JQuantsStatement],
    latest_metrics: FundamentalDataPoint | None,
    *,
    share_adjustment_events: list[ShareAdjustmentEvent] | None = None,
    through_date: str | None = None,
    round_or_none: RoundFunc,
    calculate_per: RatioFunc,
    calculate_pbr: RatioFunc,
    calculate_change_rate: ChangeRateFunc,
    has_actual_financial_data: HasActualFinancialDataFunc,
) -> tuple[list[FundamentalDataPoint], FundamentalDataPoint | None]:
    shares_map = build_shares_map(statements)
    base_snapshot = resolve_baseline_share_snapshot_from_latest_quarter(statements)
    base_shares = adjust_snapshot_shares_to_price_basis(
        base_snapshot,
        share_adjustment_events or [],
        through_date=through_date,
    )

    updated_data: list[FundamentalDataPoint] = []
    for item in data:
        current_shares = get_shares_for_datapoint(item, shares_map)
        updated_data.append(
            build_adjusted_datapoint(
                item,
                current_shares,
                current_shares,
                current_shares,
                current_shares,
                base_shares,
                round_or_none=round_or_none,
                calculate_per=calculate_per,
                calculate_pbr=calculate_pbr,
                calculate_change_rate=calculate_change_rate,
            )
        )

    updated_latest = apply_adjusted_to_latest_metrics(
        latest_metrics,
        updated_data,
        shares_map,
        base_shares,
        round_or_none=round_or_none,
        calculate_per=calculate_per,
        calculate_pbr=calculate_pbr,
        calculate_change_rate=calculate_change_rate,
        has_actual_financial_data=has_actual_financial_data,
    )
    return updated_data, updated_latest


def apply_adjusted_to_latest_metrics(
    metrics: FundamentalDataPoint | None,
    data: list[FundamentalDataPoint],
    shares_map: dict[tuple[str, str, str | None], float | None],
    base_shares: float | None,
    *,
    round_or_none: RoundFunc,
    calculate_per: RatioFunc,
    calculate_pbr: RatioFunc,
    calculate_change_rate: ChangeRateFunc,
    has_actual_financial_data: HasActualFinancialDataFunc,
) -> FundamentalDataPoint | None:
    if metrics is None:
        return None

    latest_fy = next(
        (d for d in data if d.periodType == "FY" and has_actual_financial_data(d)),
        None,
    )
    fy_shares = get_shares_for_datapoint(latest_fy, shares_map) if latest_fy else None
    metrics_shares = get_shares_for_datapoint(metrics, shares_map)
    eps_bps_shares = fy_shares or metrics_shares

    return build_adjusted_datapoint(
        metrics,
        eps_bps_shares,
        eps_bps_shares,
        metrics_shares,
        metrics_shares,
        base_shares,
        round_or_none=round_or_none,
        calculate_per=calculate_per,
        calculate_pbr=calculate_pbr,
        calculate_change_rate=calculate_change_rate,
    )
