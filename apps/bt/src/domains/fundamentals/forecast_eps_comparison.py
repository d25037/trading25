"""Forecast EPS comparison helpers."""

from __future__ import annotations

import math

from src.shared.models.types import normalize_period_type

from .models import FundamentalDataPoint


def resolve_display_actual_eps(point: FundamentalDataPoint) -> float | None:
    return point.adjustedEps if point.adjustedEps is not None else point.eps


def resolve_display_forecast_eps(point: FundamentalDataPoint) -> float | None:
    if point.revisedForecastEps is not None:
        return point.revisedForecastEps
    if point.adjustedForecastEps is not None:
        return point.adjustedForecastEps
    return point.forecastEps


def collect_recent_fy_actual_eps_values(
    data: list[FundamentalDataPoint],
    lookback_fy_count: int,
) -> list[float]:
    if lookback_fy_count < 1:
        raise ValueError("lookback_fy_count must be >= 1")
    fy_rows = sorted(
        (
            item
            for item in data
            if normalize_period_type(item.periodType) == "FY"
            and resolve_display_actual_eps(item) is not None
        ),
        key=lambda item: (item.date, item.disclosedDate),
        reverse=True,
    )
    recent_values: list[float] = []
    seen_period_ends: set[str] = set()
    for item in fy_rows:
        if item.date in seen_period_ends:
            continue
        actual_eps = resolve_display_actual_eps(item)
        if actual_eps is None or not math.isfinite(actual_eps):
            continue
        seen_period_ends.add(item.date)
        recent_values.append(actual_eps)
        if len(recent_values) >= lookback_fy_count:
            break
    return recent_values


def calculate_forecast_eps_above_recent_fy_actuals(
    metrics: FundamentalDataPoint,
    data: list[FundamentalDataPoint],
    lookback_fy_count: int,
) -> bool | None:
    forecast_eps = resolve_display_forecast_eps(metrics)
    fy_rows = sorted(
        (
            item
            for item in data
            if normalize_period_type(item.periodType) == "FY"
            and resolve_display_actual_eps(item) is not None
        ),
        key=lambda item: (item.date, item.disclosedDate),
        reverse=True,
    )
    latest_fy_with_actual = fy_rows[0] if fy_rows else None
    if latest_fy_with_actual is not None and latest_fy_with_actual.revisedForecastEps is not None:
        forecast_eps = latest_fy_with_actual.revisedForecastEps
    elif forecast_eps is None and latest_fy_with_actual is not None:
        forecast_eps = resolve_display_forecast_eps(latest_fy_with_actual)

    if forecast_eps is None or not math.isfinite(forecast_eps):
        return None

    recent_actual_eps_values = collect_recent_fy_actual_eps_values(data, lookback_fy_count)
    if len(recent_actual_eps_values) < lookback_fy_count:
        return None
    return forecast_eps > max(recent_actual_eps_values)


def apply_forecast_eps_above_recent_fy_actuals(
    metrics: FundamentalDataPoint | None,
    data: list[FundamentalDataPoint],
    lookback_fy_count: int,
) -> FundamentalDataPoint | None:
    if metrics is None:
        return None
    comparison = calculate_forecast_eps_above_recent_fy_actuals(
        metrics,
        data,
        lookback_fy_count,
    )
    return FundamentalDataPoint(
        **{
            **metrics.model_dump(),
            "forecastEpsAboveRecentFyActuals": comparison,
        }
    )
