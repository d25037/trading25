"""Liquidity classification helpers for market rankings."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import cast

import pandas as pd

from src.application.contracts import ranking as ranking_contracts
from src.application.services.ranking_fundamental_queries import (
    provider_price_cte,
    resolve_provider_windows,
)
from src.application.services.ranking_collection_filters import (
    group_ranking_items_by_normalized_code,
)
from src.application.services.ranking_query_helpers import (
    normalize_equity_code,
    normalized_code_sql,
    prefer_4digit_order_sql,
)
from src.application.services.ranking_response_items import finite_or_none
from src.application.services.ranking_state_flags import STALE_RALLY_FADE_RISK_FLAG
from src.domains.analytics.daily_ranking_core import (
    LIQUIDITY_MIN_OBSERVATIONS,
    DailyRankingLiquidityInputs,
    classify_liquidity_state,
    classify_risk_flags as classify_core_risk_flags,
)
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.shared.utils.market_code_alias import resolve_market_codes


@dataclass(frozen=True)
class PrimeLiquidityMetrics:
    liquidity_residual_z: float
    liquidity_regime: ranking_contracts.LiquidityRegime
    adv60_to_free_float_pct: float
    risk_flags: tuple[ranking_contracts.RankingRiskFlag, ...]
    recent_return_20d_pct: float | None = None
    recent_return_60d_pct: float | None = None


def enrich_ranking_collections_with_prime_liquidity(
    reader: MarketDbReader,
    collections: tuple[list[ranking_contracts.RankingItem], ...],
    *,
    target_date: str,
) -> None:
    items_by_code = group_ranking_items_by_normalized_code(collections)
    if not items_by_code:
        return

    liquidity_by_code = load_prime_liquidity_metrics(
        reader,
        target_date,
    )
    for code, items in items_by_code.items():
        metrics = liquidity_by_code.get(code)
        if metrics is None:
            continue
        for item in items:
            item.liquidityResidualZ = metrics.liquidity_residual_z
            item.liquidityRegime = metrics.liquidity_regime
            item.adv60ToFreeFloatPct = metrics.adv60_to_free_float_pct
            item.riskFlags = list(
                dict.fromkeys(
                    [
                        *item.riskFlags,
                        *metrics.risk_flags,
                        *_classify_stale_overvalued_or_no_earnings_flags(
                            item,
                            metrics,
                        ),
                    ]
                )
            )


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
) -> ranking_contracts.LiquidityRegime:
    state = classify_liquidity_state(
        DailyRankingLiquidityInputs(
            residual_z=residual_z,
            recent_return_20d_pct=recent_return_20d_pct,
            recent_return_60d_pct=recent_return_60d_pct,
        )
    )
    return "neutral" if state.regime == "missing" else state.regime


def classify_risk_flags(recent_return_20d_pct: float | None) -> tuple[ranking_contracts.RankingRiskFlag, ...]:
    return tuple(
        cast(ranking_contracts.RankingRiskFlag, flag)
        for flag in classify_core_risk_flags(recent_return_20d_pct)
    )


def _classify_stale_overvalued_or_no_earnings_flags(
    item: ranking_contracts.RankingItem,
    metrics: PrimeLiquidityMetrics,
) -> tuple[ranking_contracts.RankingRiskFlag, ...]:
    if metrics.liquidity_regime != "stale_liquidity":
        return ()
    if not _has_overvalued_or_no_earnings_warning(item):
        return ()
    if not _has_recent_positive_20d_60d(metrics):
        return ()
    return (STALE_RALLY_FADE_RISK_FLAG,)


def _has_overvalued_or_no_earnings_warning(item: ranking_contracts.RankingItem) -> bool:
    percentiles = (
        item.perPercentile,
        item.forwardPerPercentile,
        item.forwardPOpPercentile,
        item.pbrPercentile,
    )
    if any(
        value is not None and math.isfinite(value) and value >= 0.8
        for value in percentiles
    ):
        return True
    return item.perPercentile is None and item.forwardPerPercentile is None


def _has_recent_positive_20d_60d(metrics: PrimeLiquidityMetrics) -> bool:
    return (
        metrics.recent_return_20d_pct is not None
        and metrics.recent_return_60d_pct is not None
        and metrics.recent_return_20d_pct > 0
        and metrics.recent_return_60d_pct > 0
    )


def load_prime_liquidity_metrics(
    reader: MarketDbReader,
    target_date: str,
) -> dict[str, PrimeLiquidityMetrics]:
    """Build Prime ADV60-vs-free-float residuals using data as of target_date."""
    if not target_date:
        return {}

    _, prime_market_codes = resolve_market_codes("prime")
    if not prime_market_codes:
        return {}

    start_date = (
        pd.Timestamp(target_date) - pd.Timedelta(days=60 * 4 + 30)
    ).strftime("%Y-%m-%d")
    market_placeholders = ",".join("?" for _ in prime_market_codes)
    stock_code = normalized_code_sql("s.code")
    prime_code_rows = reader.query(
        f"""
        SELECT DISTINCT {stock_code} AS code
        FROM stock_master_daily AS s
        WHERE s.date = ?
          AND lower(trim(s.market_code)) IN ({market_placeholders})
        ORDER BY code
        """,
        (target_date, *prime_market_codes),
    )
    if not prime_code_rows:
        return {}
    resolve_provider_windows(
        reader,
        [str(row["code"]) for row in prime_code_rows],
        target_date,
    )
    price_ctes = provider_price_cte()
    valuation_code = normalized_code_sql("valuation.code")
    valuation_order = prefer_4digit_order_sql("valuation.code")
    rows = reader.query(
        f"""
        WITH {price_ctes},
        price_features AS (
            SELECT
                normalized_code AS code,
                date,
                close,
                volume,
                MEDIAN(close * volume) OVER (
                    PARTITION BY normalized_code ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS adv60_jpy,
                COUNT(*) OVER (
                    PARTITION BY normalized_code ORDER BY date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS adv60_count,
                LAG(close, 20) OVER (PARTITION BY normalized_code ORDER BY date) AS close_20d_ago,
                LAG(close, 60) OVER (PARTITION BY normalized_code ORDER BY date) AS close_60d_ago
            FROM provider_price
            WHERE date >= ? AND date <= ? AND close > 0 AND volume IS NOT NULL
        ),
        exact_valuation AS (
            SELECT code, free_float_market_cap
            FROM (
                SELECT
                    {valuation_code} AS code,
                    valuation.free_float_market_cap,
                    ROW_NUMBER() OVER (
                        PARTITION BY {valuation_code}, valuation.date
                        ORDER BY {valuation_order}
                    ) AS rn
                FROM daily_valuation AS valuation
                WHERE valuation.date = ?
            )
            WHERE rn = 1
        )
        SELECT
            pf.code,
            pf.close,
            CASE WHEN pf.adv60_count >= 60 THEN pf.adv60_jpy ELSE NULL END AS adv60_jpy,
            pf.close_20d_ago,
            pf.close_60d_ago,
            valuation.free_float_market_cap
        FROM price_features pf
        JOIN exact_valuation AS valuation USING (code)
        WHERE pf.date = ?
        ORDER BY pf.code
        """,
        (start_date, target_date, target_date, target_date),
    )

    samples: list[dict[str, float | str | None]] = []
    for row in rows:
        code = normalize_equity_code(row["code"])
        adv60 = finite_or_none(row["adv60_jpy"])
        close = finite_or_none(row["close"])
        free_float_market_cap = finite_or_none(row["free_float_market_cap"])
        if (
            adv60 is None
            or free_float_market_cap is None
            or close is None
            or adv60 <= 0
            or free_float_market_cap <= 0
        ):
            continue
        close_20d_ago = finite_or_none(row["close_20d_ago"])
        close_60d_ago = finite_or_none(row["close_60d_ago"])
        recent_return_20d_pct = (
            (close / close_20d_ago - 1.0) * 100.0
            if close_20d_ago is not None and close_20d_ago > 0
            else None
        )
        recent_return_60d_pct = (
            (close / close_60d_ago - 1.0) * 100.0
            if close_60d_ago is not None and close_60d_ago > 0
            else None
        )
        samples.append(
            {
                "code": code,
                "adv60": adv60,
                "free_float_market_cap": free_float_market_cap,
                "recent_return_20d_pct": recent_return_20d_pct,
                "recent_return_60d_pct": recent_return_60d_pct,
            }
        )

    if len(samples) < LIQUIDITY_MIN_OBSERVATIONS:
        return {}

    regression = fit_log_liquidity_regression(samples)
    if regression is None:
        return {}
    alpha, beta, residual_std = regression
    if beta <= 0 or residual_std <= 0:
        return {}

    metrics_by_code: dict[str, PrimeLiquidityMetrics] = {}
    for sample in samples:
        adv60 = cast(float, sample["adv60"])
        free_float_market_cap = cast(float, sample["free_float_market_cap"])
        recent_return_20d_pct = cast(float | None, sample["recent_return_20d_pct"])
        recent_return_60d_pct = cast(float | None, sample["recent_return_60d_pct"])
        expected_log_adv = alpha + beta * math.log(free_float_market_cap)
        residual = math.log(adv60) - expected_log_adv
        residual_z = residual / residual_std
        metrics_by_code[cast(str, sample["code"])] = PrimeLiquidityMetrics(
            liquidity_residual_z=round(residual_z, 4),
            liquidity_regime=classify_prime_liquidity_regime(
                residual_z,
                recent_return_20d_pct,
                recent_return_60d_pct,
            ),
            adv60_to_free_float_pct=round(
                (adv60 / free_float_market_cap) * 100.0,
                4,
            ),
            risk_flags=classify_risk_flags(recent_return_20d_pct),
            recent_return_20d_pct=recent_return_20d_pct,
            recent_return_60d_pct=recent_return_60d_pct,
        )
    return metrics_by_code
