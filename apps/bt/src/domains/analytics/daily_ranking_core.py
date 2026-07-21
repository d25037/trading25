"""Pure, shared Daily Ranking signal semantics.

This module intentionally contains no data access or outcome-aware concepts.
Production services adapt its states to HTTP contracts, while research uses the
same policy through the SQL-expression builders below.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

LiquidityRegime = Literal[
    "neutral_rerating",
    "crowded_rerating",
    "distribution_stress",
    "stale_liquidity",
    "neutral",
    "missing",
]
DailyRankingPercentilePopulation = Literal[
    "prime",
    "requested_union",
    "per_market",
    "non_prime_unsupported",
]
DailyRankingValuationSignal = Literal[
    "strong_value_confirmation",
    "medium_value_confirmation",
    "overvalued_warning",
    "very_overvalued_warning",
    "no_positive_earnings_valuation",
    "unsupported",
] | None

LIQUIDITY_MIN_OBSERVATIONS = 100
SHORT_TERM_OVERHEAT_RETURN_20D_THRESHOLD_PCT = 30.0
ATR20_ACCELERATION_CHANGE_20D_THRESHOLD_PCT = 25.0
ATR20_ACCELERATION_MAX_ATR20_TO_ATR60 = 1.25
MOMENTUM_TOP20_PERCENTILE_THRESHOLD = 0.8
ATR20_ACCELERATION_INTERNAL_FLAG = "atr20_acceleration_ex_overheat"
ATR20_ACCELERATION_API_FLAG = "atr20_acceleration"
MOMENTUM_20_60_TOP20_API_FLAG = "momentum_20_60_top20"
OVERHEAT_RISK_FLAG = "overheat"


@dataclass(frozen=True)
class DailyRankingValuationMetrics:
    """Signal-time valuation inputs and their declared percentile population."""

    percentile_population: str
    per_percentile: float | None
    forward_per_percentile: float | None
    forward_p_op_percentile: float | None
    pbr_percentile: float | None
    per: float | None
    forward_per: float | None
    psr_percentile: float | None = None
    forward_psr_percentile: float | None = None


@dataclass(frozen=True)
class DailyRankingValuationState:
    signal: DailyRankingValuationSignal
    strong_value_confirmation: bool
    medium_value_confirmation: bool
    overvalued_warning: bool
    very_overvalued_warning: bool
    no_positive_earnings_valuation: bool
    no_value_confirmation: bool
    expensive_per_or_psr: bool


@dataclass(frozen=True)
class DailyRankingLiquidityInputs:
    residual_z: float | None
    recent_return_20d_pct: float | None
    recent_return_60d_pct: float | None


@dataclass(frozen=True)
class DailyRankingLiquidityState:
    regime: LiquidityRegime


@dataclass(frozen=True)
class DailyRankingTechnicalInputs:
    atr20_change_20d_pct: float | None
    atr20_to_atr60: float | None
    recent_return_20d_pct: float | None
    recent_return_20d_percentile: float | None
    recent_return_60d_percentile: float | None


@dataclass(frozen=True)
class DailyRankingTechnicalState:
    atr20_acceleration_ex_overheat: bool
    momentum_20_60_top20: bool

    @property
    def api_flags(self) -> tuple[str, ...]:
        flags: list[str] = []
        if self.atr20_acceleration_ex_overheat:
            flags.append(ATR20_ACCELERATION_API_FLAG)
        if self.momentum_20_60_top20:
            flags.append(MOMENTUM_20_60_TOP20_API_FLAG)
        return tuple(flags)


@dataclass(frozen=True)
class DailyRankingValuationSqlExpressions:
    signal: str
    strong_value_confirmation: str
    medium_value_confirmation: str
    overvalued_warning: str
    very_overvalued_warning: str
    no_positive_earnings_valuation: str
    no_value_confirmation: str


def normalize_percentile_population(value: str | None) -> DailyRankingPercentilePopulation:
    """Resolve an explicit percentile population; implicit populations are invalid."""

    normalized = "" if value is None else value.strip().lower()
    aliases: dict[str, DailyRankingPercentilePopulation] = {
        "prime": "prime",
        "exact_date_prime": "prime",
        "requested_union": "requested_union",
        "union": "requested_union",
        "per_market": "per_market",
        "non_prime_unsupported": "non_prime_unsupported",
        "unsupported": "non_prime_unsupported",
    }
    try:
        return aliases[normalized]
    except KeyError as error:
        raise ValueError(
            f"Unsupported Daily Ranking percentile population: {value!r}"
        ) from error


def classify_valuation_state(
    metrics: DailyRankingValuationMetrics,
) -> DailyRankingValuationState:
    """Classify valuation confirmations and warnings from declared percentiles."""

    try:
        population = normalize_percentile_population(metrics.percentile_population)
    except ValueError:
        population = "non_prime_unsupported"
    if population == "non_prime_unsupported":
        return DailyRankingValuationState(
            signal="unsupported",
            strong_value_confirmation=False,
            medium_value_confirmation=False,
            overvalued_warning=False,
            very_overvalued_warning=False,
            no_positive_earnings_valuation=False,
            no_value_confirmation=False,
            expensive_per_or_psr=False,
        )

    low_per = _at_or_below(metrics.per_percentile, 0.2)
    low_forward_per = _at_or_below(metrics.forward_per_percentile, 0.2)
    low_pbr = _at_or_below(metrics.pbr_percentile, 0.2)
    forward_per_to_per_ratio = _positive_ratio(metrics.forward_per, metrics.per)
    strong = (low_pbr and low_forward_per) or (
        low_per and forward_per_to_per_ratio is not None and forward_per_to_per_ratio <= 0.8
    )
    medium = strong or low_pbr or (
        low_per and forward_per_to_per_ratio is not None and forward_per_to_per_ratio <= 1.0
    )
    overvalued = any(
        _at_or_above(value, 0.8)
        for value in (
            metrics.per_percentile,
            metrics.forward_per_percentile,
            metrics.forward_p_op_percentile,
            metrics.pbr_percentile,
        )
    )
    very_overvalued = any(
        _at_or_above(value, 0.9)
        for value in (
            metrics.per_percentile,
            metrics.forward_per_percentile,
            metrics.forward_p_op_percentile,
            metrics.pbr_percentile,
        )
    )
    no_earnings = (
        metrics.per_percentile is None and metrics.forward_per_percentile is None
    )
    expensive_per_or_psr = any(
        _at_or_above(value, 0.8)
        for value in (
            metrics.per_percentile,
            metrics.forward_per_percentile,
            metrics.psr_percentile,
            metrics.forward_psr_percentile,
        )
    )
    signal: DailyRankingValuationSignal
    if strong:
        signal = "strong_value_confirmation"
    elif very_overvalued:
        signal = "very_overvalued_warning"
    elif overvalued:
        signal = "overvalued_warning"
    elif no_earnings:
        signal = "no_positive_earnings_valuation"
    elif medium:
        signal = "medium_value_confirmation"
    else:
        signal = None
    return DailyRankingValuationState(
        signal=signal,
        strong_value_confirmation=strong,
        medium_value_confirmation=medium,
        overvalued_warning=overvalued,
        very_overvalued_warning=very_overvalued,
        no_positive_earnings_valuation=no_earnings,
        no_value_confirmation=not medium,
        expensive_per_or_psr=expensive_per_or_psr,
    )


def classify_liquidity_state(
    inputs: DailyRankingLiquidityInputs,
) -> DailyRankingLiquidityState:
    """Apply the production residual-z liquidity regime boundaries."""

    returns = (inputs.recent_return_20d_pct, inputs.recent_return_60d_pct)
    complete = all(value is not None and math.isfinite(value) for value in returns)
    runup = complete and all(value is not None and value > 0.0 for value in returns)
    if inputs.residual_z is None or not math.isfinite(inputs.residual_z):
        return DailyRankingLiquidityState("missing")
    if inputs.residual_z >= 1.0 and complete:
        return DailyRankingLiquidityState(
            "crowded_rerating" if runup else "distribution_stress"
        )
    if inputs.residual_z <= -1.0:
        return DailyRankingLiquidityState("stale_liquidity")
    if -1.0 < inputs.residual_z < 1.0 and runup:
        return DailyRankingLiquidityState("neutral_rerating")
    return DailyRankingLiquidityState("neutral")


def classify_technical_state(
    inputs: DailyRankingTechnicalInputs,
) -> DailyRankingTechnicalState:
    """Classify technical confirmation flags before API-string adaptation."""

    atr20_acceleration_ex_overheat = (
        _is_finite_below(
            inputs.recent_return_20d_pct,
            SHORT_TERM_OVERHEAT_RETURN_20D_THRESHOLD_PCT,
        )
        and _is_finite_at_or_above(
            inputs.atr20_change_20d_pct,
            ATR20_ACCELERATION_CHANGE_20D_THRESHOLD_PCT,
        )
        and _is_finite_below(
            inputs.atr20_to_atr60,
            ATR20_ACCELERATION_MAX_ATR20_TO_ATR60,
        )
    )
    momentum_20_60_top20 = _is_finite_at_or_above(
        inputs.recent_return_20d_percentile,
        MOMENTUM_TOP20_PERCENTILE_THRESHOLD,
    ) and _is_finite_at_or_above(
        inputs.recent_return_60d_percentile,
        MOMENTUM_TOP20_PERCENTILE_THRESHOLD,
    )
    return DailyRankingTechnicalState(
        atr20_acceleration_ex_overheat=atr20_acceleration_ex_overheat,
        momentum_20_60_top20=momentum_20_60_top20,
    )


def classify_risk_flags(recent_return_20d_pct: float | None) -> tuple[str, ...]:
    if _is_finite_at_or_above(
        recent_return_20d_pct,
        SHORT_TERM_OVERHEAT_RETURN_20D_THRESHOLD_PCT,
    ):
        return (OVERHEAT_RISK_FLAG,)
    return ()


def percent_rank_sql(*, value_sql: str, partition_by_sql: str) -> str:
    """Return standard SQL percentile semantics, where equal values stay equal."""

    return f"percent_rank() OVER (PARTITION BY {partition_by_sql} ORDER BY {value_sql})"


def valuation_state_sql(
    *,
    percentile_population_sql: str,
    per_percentile_sql: str,
    forward_per_percentile_sql: str,
    forward_p_op_percentile_sql: str,
    pbr_percentile_sql: str,
    per_sql: str,
    forward_per_sql: str,
) -> str:
    """Return a DuckDB expression with the same valuation-state precedence."""

    expressions = valuation_sql_expressions(
        percentile_population_sql=percentile_population_sql,
        per_percentile_sql=per_percentile_sql,
        forward_per_percentile_sql=forward_per_percentile_sql,
        forward_p_op_percentile_sql=forward_p_op_percentile_sql,
        pbr_percentile_sql=pbr_percentile_sql,
        per_sql=per_sql,
        forward_per_sql=forward_per_sql,
    )
    return expressions.signal


def valuation_sql_expressions(
    *,
    percentile_population_sql: str,
    per_percentile_sql: str,
    forward_per_percentile_sql: str,
    forward_p_op_percentile_sql: str,
    pbr_percentile_sql: str,
    per_sql: str,
    forward_per_sql: str,
) -> DailyRankingValuationSqlExpressions:
    """Return DuckDB expressions for the complete valuation state contract."""

    ratio = f"({forward_per_sql} / {per_sql})"
    positive_ratio = f"({forward_per_sql} > 0 AND {per_sql} > 0)"
    strong = (
        f"(({pbr_percentile_sql} <= 0.2 AND {forward_per_percentile_sql} <= 0.2) "
        f"OR ({per_percentile_sql} <= 0.2 AND {positive_ratio} AND {ratio} <= 0.8))"
    )
    medium = (
        f"({strong} OR {pbr_percentile_sql} <= 0.2 "
        f"OR ({per_percentile_sql} <= 0.2 AND {positive_ratio} AND {ratio} <= 1.0))"
    )
    overvalued = " OR ".join(
        f"{value} >= 0.8"
        for value in (
            per_percentile_sql,
            forward_per_percentile_sql,
            forward_p_op_percentile_sql,
            pbr_percentile_sql,
        )
    )
    very_overvalued = " OR ".join(
        f"{value} >= 0.9"
        for value in (
            per_percentile_sql,
            forward_per_percentile_sql,
            forward_p_op_percentile_sql,
            pbr_percentile_sql,
        )
    )
    no_earnings = f"({per_percentile_sql} IS NULL AND {forward_per_percentile_sql} IS NULL)"
    supported = (
        f"({percentile_population_sql} IN ('prime', 'requested_union', 'per_market'))"
    )
    signal = f"""
        CASE
            WHEN NOT {supported} OR {percentile_population_sql} IS NULL THEN 'unsupported'
            WHEN {strong} THEN 'strong_value_confirmation'
            WHEN ({very_overvalued}) THEN 'very_overvalued_warning'
            WHEN ({overvalued}) THEN 'overvalued_warning'
            WHEN {no_earnings} THEN 'no_positive_earnings_valuation'
            WHEN {medium} THEN 'medium_value_confirmation'
        END
    """
    return DailyRankingValuationSqlExpressions(
        signal=signal,
        strong_value_confirmation=f"({supported} AND coalesce({strong}, FALSE))",
        medium_value_confirmation=f"({supported} AND coalesce({medium}, FALSE))",
        overvalued_warning=f"({supported} AND coalesce(({overvalued}), FALSE))",
        very_overvalued_warning=f"({supported} AND coalesce(({very_overvalued}), FALSE))",
        no_positive_earnings_valuation=f"({supported} AND {no_earnings})",
        no_value_confirmation=f"({supported} AND NOT coalesce({medium}, FALSE))",
    )


def liquidity_state_sql(
    *,
    residual_z_sql: str,
    recent_return_20d_pct_sql: str,
    recent_return_60d_pct_sql: str,
) -> str:
    """Return a DuckDB expression matching :func:`classify_liquidity_state`."""

    complete = (
        f"(isfinite({recent_return_20d_pct_sql}) AND "
        f"isfinite({recent_return_60d_pct_sql}))"
    )
    runup = (
        f"({complete} AND {recent_return_20d_pct_sql} > 0.0 "
        f"AND {recent_return_60d_pct_sql} > 0.0)"
    )
    return f"""
        CASE
            WHEN NOT isfinite({residual_z_sql}) OR {residual_z_sql} IS NULL THEN 'missing'
            WHEN {residual_z_sql} >= 1.0 AND {complete}
                THEN CASE WHEN {runup} THEN 'crowded_rerating' ELSE 'distribution_stress' END
            WHEN {residual_z_sql} <= -1.0 THEN 'stale_liquidity'
            WHEN {residual_z_sql} > -1.0 AND {residual_z_sql} < 1.0 AND {runup}
                THEN 'neutral_rerating'
            ELSE 'neutral'
        END
    """


def technical_state_sql(
    *,
    atr20_change_20d_pct_sql: str,
    atr20_to_atr60_sql: str,
    recent_return_20d_pct_sql: str,
    recent_return_20d_percentile_sql: str,
    recent_return_60d_percentile_sql: str,
) -> str:
    """Return comma-separated API flags matching :func:`classify_technical_state`."""

    acceleration = (
        f"(isfinite({recent_return_20d_pct_sql}) "
        f"AND {recent_return_20d_pct_sql} < {SHORT_TERM_OVERHEAT_RETURN_20D_THRESHOLD_PCT} "
        f"AND isfinite({atr20_change_20d_pct_sql}) "
        f"AND {atr20_change_20d_pct_sql} >= {ATR20_ACCELERATION_CHANGE_20D_THRESHOLD_PCT} "
        f"AND isfinite({atr20_to_atr60_sql}) "
        f"AND {atr20_to_atr60_sql} < {ATR20_ACCELERATION_MAX_ATR20_TO_ATR60})"
    )
    momentum = (
        f"(isfinite({recent_return_20d_percentile_sql}) "
        f"AND {recent_return_20d_percentile_sql} >= {MOMENTUM_TOP20_PERCENTILE_THRESHOLD} "
        f"AND isfinite({recent_return_60d_percentile_sql}) "
        f"AND {recent_return_60d_percentile_sql} >= {MOMENTUM_TOP20_PERCENTILE_THRESHOLD})"
    )
    return f"""
        concat_ws(
            ',',
            CASE WHEN {acceleration} THEN '{ATR20_ACCELERATION_API_FLAG}' END,
            CASE WHEN {momentum} THEN '{MOMENTUM_20_60_TOP20_API_FLAG}' END
        )
    """


def _positive_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if not _is_finite_positive(numerator) or not _is_finite_positive(denominator):
        return None
    if numerator is None or denominator is None:
        return None
    return float(numerator) / float(denominator)


def _at_or_below(value: float | None, threshold: float) -> bool:
    return value is not None and math.isfinite(value) and float(value) <= threshold


def _at_or_above(value: float | None, threshold: float) -> bool:
    return value is not None and math.isfinite(value) and float(value) >= threshold


def _is_finite_positive(value: float | None) -> bool:
    return value is not None and math.isfinite(value) and float(value) > 0.0


def _is_finite_at_or_above(value: float | None, threshold: float) -> bool:
    return value is not None and math.isfinite(value) and float(value) >= threshold


def _is_finite_below(value: float | None, threshold: float) -> bool:
    return value is not None and math.isfinite(value) and float(value) < threshold
