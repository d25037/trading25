"""Valuation enrichment helpers for market rankings."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Literal

import pandas as pd

from src.application.services.ranking_fundamental_queries import (
    load_adjusted_daily_valuation_frame,
)
from src.application.services.ranking_query_helpers import canonical_market_label
from src.application.services.ranking_query_helpers import normalize_equity_code
from src.application.services.ranking_response_items import finite_or_none, str_or_none
from src.application.services.ranking_value_composite_config import (
    PRIME_VALUATION_PERCENTILE_COLUMNS,
)
from src.entrypoints.http.schemas.ranking import RankingItem
from src.infrastructure.db.market.market_reader import MarketDbReader


def with_prime_valuation_percentiles(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    result = frame.copy()
    for _, percentile_column in PRIME_VALUATION_PERCENTILE_COLUMNS:
        result[percentile_column] = None
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
    return result


def enrich_items_from_adjusted_daily_valuation(
    reader: MarketDbReader,
    items_by_code: Mapping[str, list[RankingItem]],
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
            item.forwardEpsDisclosedDate = str_or_none(
                row.get("forward_eps_disclosed_date")
            )
            item.forwardEpsSource = source
            item.pbr = finite_or_none(row.get("pbr"))
            item.pbrPercentile = finite_or_none(row.get("pbr_percentile"))
            item.marketCap = finite_or_none(row.get("market_cap"))
        enriched_codes.add(code)
    return enriched_codes
