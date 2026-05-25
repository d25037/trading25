"""Response item builders for ranking services."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any, Literal, cast

from src.domains.analytics.fundamental_ranking import (
    FundamentalItem,
    to_nullable_float,
)
from src.domains.analytics.value_composite_scoring import VALUE_COMPOSITE_SCORE_COLUMN
from src.entrypoints.http.schemas.ranking import (
    FundamentalRankingItem,
    RankingItem,
    ValueCompositeRankingItem,
    ValueCompositeScoreResponse,
    ValueCompositeForwardEpsMode,
    ValueCompositeScoreMethod,
    ValueCompositeScoreUnavailableReason,
    ValueCompositeTechnicalMetrics,
)


def finite_or_none(value: Any) -> float | None:
    number = to_nullable_float(value)
    if number is None or not math.isfinite(number):
        return None
    return number


def int_or_none(value: Any) -> int | None:
    number = finite_or_none(value)
    if number is None:
        return None
    return int(number)


def str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return str(value)


def row_get(row: Mapping[str, Any], key: str) -> Any:
    try:
        return row[key]
    except KeyError:
        return None


def build_ranking_item(
    row: Mapping[str, Any],
    rank: int,
    **extra: Any,
) -> RankingItem:
    return RankingItem(
        rank=rank,
        code=row["code"],
        companyName=row["company_name"],
        marketCode=row["market_code"],
        sector33Name=row["sector_33_name"],
        currentPrice=row["current_price"],
        volume=row["volume"],
        **{key: value for key, value in extra.items() if value is not None},
    )


def build_fundamental_ranking_item(
    item: FundamentalItem,
    rank: int,
) -> FundamentalRankingItem:
    return FundamentalRankingItem(
        rank=rank,
        code=item.code,
        companyName=item.company_name,
        marketCode=item.market_code,
        sector33Name=item.sector_33_name,
        currentPrice=item.current_price,
        volume=item.volume,
        epsValue=item.eps_value,
        disclosedDate=item.disclosed_date,
        periodType=item.period_type,
        source=item.source,
    )


def build_value_composite_item(
    row: Mapping[str, Any],
    rank: int,
) -> ValueCompositeRankingItem:
    raw_source = str_or_none(row.get("forward_eps_source"))
    source = raw_source if raw_source in {"revised", "fy"} else None
    return ValueCompositeRankingItem(
        rank=rank,
        code=str(row["code"]),
        companyName=str(row["company_name"]),
        marketCode=str(row["market_code"]),
        sector33Name=str(row["sector_33_name"]),
        currentPrice=float(row["current_price"]),
        volume=float(row["volume"]),
        score=float(row[VALUE_COMPOSITE_SCORE_COLUMN]),
        scoreBeforeBoost=finite_or_none(row.get("score_before_boost")),
        breakoutBoost=finite_or_none(row.get("breakout_boost")),
        liquidityEligible=(
            bool(row.get("liquidity_eligible"))
            if row.get("liquidity_eligible") is not None
            else None
        ),
        avgTradingValue60dMilJpy=finite_or_none(
            row.get("avg_trading_value_60d_mil_jpy")
        ),
        lowPbrScore=float(row["low_pbr_score"]),
        smallMarketCapScore=float(row["small_market_cap_score"]),
        lowForwardPerScore=float(row["low_forward_per_score"]),
        pbr=float(row["pbr"]),
        forwardPer=float(row["forward_per"]),
        marketCapBilJpy=float(row["market_cap_bil_jpy"]),
        bps=finite_or_none(row.get("bps")),
        forwardEps=finite_or_none(row.get("forward_eps")),
        latestFyDisclosedDate=str_or_none(row.get("latest_fy_disclosed_date")),
        forwardEpsDisclosedDate=str_or_none(row.get("forward_eps_disclosed_date")),
        forwardEpsSource=cast(Literal["revised", "fy"] | None, source),
        technicalMetrics=ValueCompositeTechnicalMetrics(
            featureDate=str_or_none(row.get("technical_feature_date")),
            breakoutFeatureDate=str_or_none(row.get("breakout_feature_date")),
            reboundFrom252dLowPct=finite_or_none(
                row.get("rebound_from_252d_low_pct")
            ),
            return252dPct=finite_or_none(row.get("return_252d_pct")),
            volatility20dPct=finite_or_none(row.get("volatility_20d_pct")),
            volatility60dPct=finite_or_none(row.get("volatility_60d_pct")),
            downsideVolatility60dPct=finite_or_none(
                row.get("downside_volatility_60d_pct")
            ),
            avgTradingValue60dMilJpy=finite_or_none(
                row.get("avg_trading_value_60d_mil_jpy")
            ),
            avgTradingValue60dSourceSessions=int_or_none(
                row.get("avg_trading_value_60d_source_sessions")
            ),
            newHigh20d=(
                bool(row.get("new_high_20d"))
                if row.get("new_high_20d") is not None
                else None
            ),
            daysSinceNewHigh20d=int_or_none(row.get("days_since_new_high_20d")),
            closeToPriorHigh20dPct=finite_or_none(
                row.get("close_to_prior_high_20d_pct")
            ),
            newHigh120d=(
                bool(row.get("new_high_120d"))
                if row.get("new_high_120d") is not None
                else None
            ),
            daysSinceNewHigh120d=int_or_none(row.get("days_since_new_high_120d")),
            closeToPriorHigh120dPct=finite_or_none(
                row.get("close_to_prior_high_120d_pct")
            ),
        ),
    )


def build_value_composite_score_response(
    *,
    date: str,
    code: str,
    forward_eps_mode: ValueCompositeForwardEpsMode,
    score_available: bool,
    last_updated: str,
    target_stock: Mapping[str, Any] | None = None,
    market: str | None = None,
    score_method: ValueCompositeScoreMethod | None = None,
    score_policy: str | None = None,
    weights: Mapping[str, float] | None = None,
    universe_count: int = 0,
    item: ValueCompositeRankingItem | None = None,
    unsupported_reason: ValueCompositeScoreUnavailableReason | None = None,
) -> ValueCompositeScoreResponse:
    stock_payload: dict[str, Any] = {"code": code}
    if target_stock is not None:
        stock_payload = {
            "code": str(target_stock["code"]),
            "companyName": str(target_stock["company_name"]),
            "marketCode": str(target_stock["market_code"]),
        }
    return ValueCompositeScoreResponse(
        date=date,
        **stock_payload,
        market=market,
        scoreMethod=score_method,
        forwardEpsMode=forward_eps_mode,
        scorePolicy=score_policy,
        weights=dict(weights or {}),
        universeCount=universe_count,
        scoreAvailable=score_available,
        unsupportedReason=unsupported_reason,
        item=item,
        lastUpdated=last_updated,
    )
