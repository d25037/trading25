"""Valuation enrichment helpers for market rankings."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any
from typing import Literal

import pandas as pd

from src.application.services.ranking_fundamental_queries import (
    load_adjustment_events_by_code,
    load_adjusted_daily_valuation_frame,
    load_fundamental_statement_rows,
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
from src.domains.fundamentals import (
    FundamentalsCalculator,
    market_statement_row_to_jquants_statement,
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


def enrich_ranking_collections_with_valuation(
    reader: MarketDbReader,
    calculator: FundamentalsCalculator,
    collections: tuple[list[RankingItem], ...],
    *,
    target_date: str,
    query_market_codes: list[str],
    price_basis_date: str,
) -> None:
    items_by_code = group_ranking_items_by_normalized_code(collections)
    if not items_by_code:
        return

    enriched_codes = enrich_items_from_adjusted_daily_valuation(
        reader,
        items_by_code,
        target_date=target_date,
        query_market_codes=query_market_codes,
    )
    if len(enriched_codes) == len(items_by_code):
        return

    enrich_items_from_statement_valuation(
        reader,
        calculator,
        items_by_code,
        enriched_codes,
        target_date=target_date,
        query_market_codes=query_market_codes,
        price_basis_date=price_basis_date,
    )


def enrich_items_from_statement_valuation(
    reader: MarketDbReader,
    calculator: FundamentalsCalculator,
    items_by_code: Mapping[str, list[RankingItem]],
    enriched_codes: set[str],
    *,
    target_date: str,
    query_market_codes: list[str],
    price_basis_date: str,
) -> None:
    statement_rows = load_fundamental_statement_rows(
        reader,
        target_date,
        query_market_codes,
    )
    raw_statements_by_code: dict[str, list[Mapping[str, Any]]] = {}
    for row in statement_rows:
        code = normalize_equity_code(row["code"])
        if code in items_by_code and code not in enriched_codes:
            raw_statements_by_code.setdefault(code, []).append(row)

    adjustment_events_by_code = load_adjustment_events_by_code(
        reader,
        through_date=price_basis_date,
        market_codes=query_market_codes,
        as_of_date=target_date,
    )

    for code, items in items_by_code.items():
        raw_statements = raw_statements_by_code.get(code)
        if not raw_statements:
            continue
        statements = [
            market_statement_row_to_jquants_statement(row, code_fallback=code)
            for row in raw_statements
        ]
        reference_item = items[0]
        valuation = calculator.calculate_latest_valuation(
            statements,
            close=reference_item.currentPrice,
            price_date=target_date,
            prefer_consolidated=True,
            share_adjustment_events=adjustment_events_by_code.get(code, []),
            price_basis_date=price_basis_date,
        )
        if valuation is None:
            continue
        for item in items:
            item.per = valuation.per
            item.forwardPer = valuation.forwardPer
            item.pOp = valuation.pOp
            item.forwardPOp = valuation.forwardPOp
            item.forwardEpsDisclosedDate = valuation.forwardEpsDisclosedDate
            item.forwardEpsSource = valuation.forwardEpsSource
            item.pbr = valuation.pbr
            item.marketCap = valuation.marketCap
