"""Valuation enrichment helpers for market rankings."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Literal

import pandas as pd

from src.application.contracts import ranking as ranking_contracts
from src.application.services.ranking_fundamental_queries import (
    load_adjusted_daily_valuation_frame,
)
from src.application.services.ranking_collection_filters import (
    group_ranking_items_by_normalized_code,
)
from src.application.services.ranking_query_helpers import canonical_market_label
from src.application.services.ranking_query_helpers import normalize_equity_code
from src.application.services.ranking_response_items import finite_or_none, str_or_none
from src.application.services.ranking_value_composite_config import (
    PRIME_VALUATION_PERCENTILE_COLUMNS,
)
from src.domains.fundamentals import FundamentalsCalculator
from src.infrastructure.db.market.market_reader import MarketDbReader


def with_prime_valuation_percentiles(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    result = frame.copy()
    for _, percentile_column in PRIME_VALUATION_PERCENTILE_COLUMNS:
        result[percentile_column] = None
    result["value_composite_score"] = None
    result["overvaluation_composite_score"] = None
    if "market_code" not in result.columns:
        return result

    prime_mask = result["market_code"].map(
        lambda value: canonical_market_label(str(value)) == "prime"
    )
    if not bool(prime_mask.any()):
        return result

    for value_column, percentile_column in PRIME_VALUATION_PERCENTILE_COLUMNS:
        if value_column not in result.columns:
            continue
        values = pd.to_numeric(result.loc[prime_mask, value_column], errors="coerce")
        valid_mask = values.map(
            lambda value: pd.notna(value)
            and math.isfinite(float(value))
            and float(value) > 0
        )
        valid_values = values[valid_mask]
        if valid_values.empty:
            continue
        if len(valid_values) == 1:
            percentiles = pd.Series(0.0, index=valid_values.index)
        else:
            percentiles = (valid_values.rank(method="min") - 1.0) / (
                len(valid_values) - 1.0
            )
        result.loc[percentiles.index, percentile_column] = percentiles.astype(float)
    _append_forward_per_pbr_composite_scores(result)
    return result


def _append_forward_per_pbr_composite_scores(frame: pd.DataFrame) -> None:
    if "forward_per_percentile" not in frame.columns or "pbr_percentile" not in frame.columns:
        return
    forward_per_percentile = pd.to_numeric(
        frame["forward_per_percentile"],
        errors="coerce",
    )
    pbr_percentile = pd.to_numeric(frame["pbr_percentile"], errors="coerce")
    valid_mask = forward_per_percentile.notna() & pbr_percentile.notna()
    if not bool(valid_mask.any()):
        return
    overvaluation_score = (forward_per_percentile + pbr_percentile) / 2.0
    value_score = 1.0 - overvaluation_score
    frame.loc[valid_mask, "overvaluation_composite_score"] = overvaluation_score.loc[
        valid_mask
    ].astype(float)
    frame.loc[valid_mask, "value_composite_score"] = value_score.loc[
        valid_mask
    ].astype(float)


def _forecast_operating_profit_growth_ratio(
    p_op: object,
    forward_p_op: object,
) -> float | None:
    p_op_value = finite_or_none(p_op)
    forward_p_op_value = finite_or_none(forward_p_op)
    if p_op_value is None or forward_p_op_value is None:
        return None
    if p_op_value <= 0 or forward_p_op_value <= 0:
        return None
    ratio = p_op_value / forward_p_op_value
    return ratio if math.isfinite(ratio) else None


def _set_forecast_operating_profit_growth_fields(
    item: ranking_contracts.RankingItem,
    *,
    p_op: object,
    forward_p_op: object,
) -> None:
    ratio = _forecast_operating_profit_growth_ratio(p_op, forward_p_op)
    item.forecastOperatingProfitGrowthRatio = ratio
    item.forecastOperatingProfitGrowthPct = (
        (ratio - 1.0) * 100.0 if ratio is not None else None
    )


def enrich_items_from_adjusted_daily_valuation(
    reader: MarketDbReader,
    items_by_code: Mapping[str, list[ranking_contracts.RankingItem]],
    *,
    target_date: str,
    query_market_codes: list[str],
) -> set[str]:
    valuation_frame = load_adjusted_daily_valuation_frame(
        reader,
        target_date,
        query_market_codes,
    )
    if valuation_frame.empty:
        return set()
    valuation_frame = with_prime_valuation_percentiles(valuation_frame)

    enriched_codes: set[str] = set()
    for row in valuation_frame.to_dict("records"):
        code = normalize_equity_code(row.get("code"))
        items = items_by_code.get(code)
        if not items:
            continue
        raw_source = str_or_none(row.get("forward_eps_source"))
        source: Literal["revised", "fy"] | None = (
            raw_source if raw_source in ("revised", "fy") else None
        )
        for item in items:
            item.per = finite_or_none(row.get("per"))
            item.perPercentile = finite_or_none(row.get("per_percentile"))
            item.forwardPer = finite_or_none(row.get("forward_per"))
            item.forwardPerPercentile = finite_or_none(
                row.get("forward_per_percentile")
            )
            item.pOp = finite_or_none(row.get("p_op"))
            item.forwardPOp = finite_or_none(row.get("forward_p_op"))
            item.forwardPOpPercentile = finite_or_none(
                row.get("forward_p_op_percentile")
            )
            _set_forecast_operating_profit_growth_fields(
                item,
                p_op=row.get("p_op"),
                forward_p_op=row.get("forward_p_op"),
            )
            item.psr = finite_or_none(row.get("psr"))
            item.psrPercentile = finite_or_none(row.get("psr_percentile"))
            item.forwardPsr = finite_or_none(row.get("forward_psr"))
            item.forwardPsrPercentile = finite_or_none(
                row.get("forward_psr_percentile")
            )
            item.forwardEpsDisclosedDate = str_or_none(
                row.get("forward_eps_disclosed_date")
            )
            item.forwardEpsSource = source
            item.pbr = finite_or_none(row.get("pbr"))
            item.pbrPercentile = finite_or_none(row.get("pbr_percentile"))
            item.valueCompositeScore = finite_or_none(row.get("value_composite_score"))
            item.overvaluationCompositeScore = finite_or_none(
                row.get("overvaluation_composite_score")
            )
            item.marketCap = finite_or_none(row.get("market_cap"))
        enriched_codes.add(code)
    return enriched_codes


def enrich_ranking_collections_with_valuation(
    reader: MarketDbReader,
    calculator: FundamentalsCalculator,
    collections: tuple[list[ranking_contracts.RankingItem], ...],
    *,
    target_date: str,
    query_market_codes: list[str],
    price_basis_date: str,
) -> None:
    items_by_code = group_ranking_items_by_normalized_code(collections)
    if not items_by_code:
        return

    del calculator, price_basis_date
    enrich_items_from_adjusted_daily_valuation(
        reader,
        items_by_code,
        target_date=target_date,
        query_market_codes=query_market_codes,
    )
