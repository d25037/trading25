"""Canonical EPS snapshot semantics shared by ranking and fundamentals views."""

from __future__ import annotations

import math
from dataclasses import dataclass

from src.domains.fundamentals.valuation_primitives import positive_ratio


@dataclass(frozen=True)
class EpsMetricSnapshot:
    actual_eps: float | None
    forecast_eps: float | None
    forecast_to_actual_ratio: float | None
    forecast_eps_change_rate: float | None


def _finite(value: float | None) -> float | None:
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def build_eps_metric_snapshot(
    *,
    actual_eps: float | None,
    forecast_eps: float | None,
) -> EpsMetricSnapshot:
    """Build the only UI-facing actual/forecast EPS relationship contract."""
    actual = _finite(actual_eps)
    forecast = _finite(forecast_eps)
    ratio = positive_ratio(forecast, actual)
    change_rate = None
    if actual is not None and forecast is not None and actual != 0:
        change_rate = ((forecast - actual) / abs(actual)) * 100
    return EpsMetricSnapshot(
        actual_eps=actual,
        forecast_eps=forecast,
        forecast_to_actual_ratio=round(ratio, 4) if ratio is not None else None,
        forecast_eps_change_rate=(
            round(change_rate, 4) if change_rate is not None else None
        ),
    )
