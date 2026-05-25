"""Liquidity classification helpers for market rankings."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import cast

from src.application.services.ranking_value_composite_config import (
    OVERHEAT_RISK_FLAG,
    SHORT_TERM_OVERHEAT_RETURN_20D_THRESHOLD_PCT,
)
from src.entrypoints.http.schemas.ranking import LiquidityRegime, RankingRiskFlag


@dataclass(frozen=True)
class PrimeLiquidityMetrics:
    liquidity_residual_z: float
    liquidity_regime: LiquidityRegime
    adv60_to_free_float_pct: float
    risk_flags: tuple[RankingRiskFlag, ...]


def fit_log_liquidity_regression(
    samples: list[dict[str, float | str | None]],
) -> tuple[float, float, float] | None:
    x_values = [math.log(cast(float, sample["free_float_market_cap"])) for sample in samples]
    y_values = [math.log(cast(float, sample["adv60"])) for sample in samples]
    count = len(x_values)
    if count < 3:
        return None
    x_mean = sum(x_values) / count
    y_mean = sum(y_values) / count
    x_var = sum((value - x_mean) ** 2 for value in x_values)
    if x_var <= 0:
        return None
    xy_cov = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, y_values, strict=True))
    beta = xy_cov / x_var
    alpha = y_mean - beta * x_mean
    residual_sum_sq = sum(
        (y - (alpha + beta * x)) ** 2
        for x, y in zip(x_values, y_values, strict=True)
    )
    dof = count - 2
    if dof <= 0:
        return None
    residual_std = math.sqrt(residual_sum_sq / dof)
    if not all(math.isfinite(value) for value in (alpha, beta, residual_std)):
        return None
    return alpha, beta, residual_std


def classify_prime_liquidity_regime(
    residual_z: float,
    recent_return_20d_pct: float | None,
    recent_return_60d_pct: float | None,
) -> LiquidityRegime:
    valid_returns = [
        value
        for value in (recent_return_20d_pct, recent_return_60d_pct)
        if value is not None
    ]
    has_persistent_runup = len(valid_returns) == 2 and all(
        value > 0 for value in valid_returns
    )
    if residual_z >= 1.0 and len(valid_returns) == 2:
        if has_persistent_runup:
            return "crowded_rerating"
        if any(value <= 0 for value in valid_returns):
            return "distribution_stress"
    if residual_z <= -1.0:
        return "stale_liquidity"
    if -1.0 < residual_z < 1.0 and has_persistent_runup:
        return "neutral_rerating"
    return "neutral"


def classify_risk_flags(recent_return_20d_pct: float | None) -> tuple[RankingRiskFlag, ...]:
    if (
        recent_return_20d_pct is not None
        and recent_return_20d_pct >= SHORT_TERM_OVERHEAT_RETURN_20D_THRESHOLD_PCT
    ):
        return (OVERHEAT_RISK_FLAG,)
    return ()
