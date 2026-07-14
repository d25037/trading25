"""Value-composite ranking metric query helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

import pandas as pd

from src.application.contracts import ranking as ranking_contracts
from src.domains.analytics.fundamental_ranking import (
    ForecastValue,
    FundamentalRankingCalculator,
    StatementRow,
    adjust_per_share_value,
    normalize_period_label,
    to_nullable_float,
)
from src.domains.analytics.value_composite_scoring import (
    VALUE_COMPOSITE_REQUIRED_POSITIVE_COLUMNS,
    VALUE_COMPOSITE_SCORE_COLUMN,
    build_value_composite_score_frame,
)
from src.domains.fundamentals import (
    FundamentalsCalculator,
    market_statement_row_to_jquants_statement,
)
from src.application.services.ranking_fundamental_queries import (
    load_adjustment_events_by_code,
    load_adjusted_daily_valuation_frame,
    load_fundamental_statement_rows,
    load_fundamental_stock_rows,
    resolve_baseline_share_snapshot,
    resolve_latest_stock_data_date,
)
from src.application.services.ranking_query_helpers import (
    canonical_market_label,
    equity_code_variants,
    normalize_equity_code,
    positive_ratio,
)
from src.application.services.ranking_response_items import (
    build_value_composite_item,
    finite_or_none,
    str_or_none,
)
from src.application.services.ranking_statement_selection import (
    latest_actual_fy_disclosed_date,
    latest_value_bps_statement,
)
from src.application.services.ranking_statement_rows import (
    statement_rows_from_mappings,
)
from src.application.services.ranking_value_composite_config import ValueCompositeProfileSpec
from src.application.services.ranking_value_composite_features import (
    append_value_composite_profile_metrics,
    append_value_composite_technical_metrics,
)
from src.shared.utils.share_adjustment import (
    ShareAdjustmentEvent,
    adjust_share_count_to_price_basis,
)
from src.infrastructure.db.market.market_reader import MarketDbReader


def resolve_value_composite_target_date(
    reader: MarketDbReader,
    date: str | None,
) -> str:
    if date:
        return date
    return resolve_latest_stock_data_date(reader)


def resolve_value_composite_symbol_target_date(
    reader: MarketDbReader,
    code: str,
    target_date: str,
) -> str:
    code_variants = equity_code_variants(code)
    placeholders = ",".join("?" for _ in code_variants)
    row = reader.query_one(
        f"""
        SELECT MAX(date) AS max_date
        FROM stock_data
        WHERE date <= ?
          AND code IN ({placeholders})
        """,
        (target_date, *code_variants),
    )
    if row is None or row["max_date"] is None:
        return target_date
    return str(row["max_date"])


def resolve_value_composite_forecast_snapshot(
    calculator: FundamentalRankingCalculator,
    rows: list[StatementRow],
    baseline_shares: float | None,
    *,
    forward_eps_mode: ranking_contracts.ValueCompositeForwardEpsMode,
    as_of_date: str | None = None,
) -> ForecastValue | None:
    if forward_eps_mode == "latest":
        return calculator.resolve_latest_forecast_snapshot(
            rows,
            baseline_shares,
            as_of_date=as_of_date,
        )
    latest_fy = calculator.resolve_latest_fy_row(rows, as_of_date=as_of_date)
    return calculator.resolve_latest_fy_forecast_snapshot(
        latest_fy,
        baseline_shares,
    )


def load_value_composite_scored_frame(
    reader: MarketDbReader,
    *,
    target_date: str,
    query_market_codes: list[str],
    weights: Mapping[str, float],
    forward_eps_mode: ranking_contracts.ValueCompositeForwardEpsMode,
    valuation_calculator: FundamentalsCalculator,
) -> pd.DataFrame:
    if forward_eps_mode == "latest":
        adjusted = load_adjusted_daily_valuation_frame(
            reader,
            target_date,
            query_market_codes,
        )
        if not adjusted.empty:
            scored = build_value_composite_score_frame_from_adjusted(
                adjusted,
                weights=weights,
            )
            if not scored.empty:
                return scored

    stock_rows = load_fundamental_stock_rows(
        reader,
        target_date,
        query_market_codes,
    )
    statement_rows = load_fundamental_statement_rows(
        reader,
        target_date,
        query_market_codes,
    )
    price_basis_date = resolve_latest_stock_data_date(reader)
    adjustment_events_by_code = load_adjustment_events_by_code(
        reader,
        through_date=price_basis_date,
        market_codes=query_market_codes,
        as_of_date=target_date,
    )

    return build_value_composite_score_frame_from_statement_rows(
        stock_rows,
        statement_rows,
        target_date=target_date,
        price_basis_date=price_basis_date,
        adjustment_events_by_code=adjustment_events_by_code,
        valuation_calculator=valuation_calculator,
        weights=weights,
        forward_eps_mode=forward_eps_mode,
    )


def resolve_value_composite_unavailable_reason(
    reader: MarketDbReader,
    calculator: FundamentalRankingCalculator,
    *,
    target_stock: Mapping[str, Any],
    target_date: str,
    query_market_codes: list[str],
    forward_eps_mode: ranking_contracts.ValueCompositeForwardEpsMode,
    price_basis_date: str,
) -> ranking_contracts.ValueCompositeScoreUnavailableReason:
    price = to_nullable_float(target_stock["current_price"])
    if price is None or price <= 0:
        return "not_rankable"

    target_code = normalize_equity_code(target_stock["code"])
    statement_rows = load_fundamental_statement_rows(
        reader,
        target_date,
        query_market_codes,
    )
    raw_statements = [
        row
        for row in statement_rows
        if normalize_equity_code(row["code"]) == target_code
    ]
    statements = statement_rows_from_mappings(raw_statements)
    if not statements:
        return "not_rankable"

    adjustment_events_by_code = load_adjustment_events_by_code(
        reader,
        through_date=price_basis_date,
        market_codes=query_market_codes,
        as_of_date=target_date,
    )
    baseline_snapshot = resolve_baseline_share_snapshot(
        statements,
        as_of_date=target_date,
    )
    baseline_shares = adjust_share_count_to_price_basis(
        baseline_snapshot.shares if baseline_snapshot is not None else None,
        adjustment_events_by_code.get(str(target_stock["code"]), []),
        from_date=(
            baseline_snapshot.disclosed_date
            if baseline_snapshot is not None
            else None
        ),
        through_date=price_basis_date,
    )
    forecast_snapshot = resolve_value_composite_forecast_snapshot(
        calculator,
        statements,
        baseline_shares,
        forward_eps_mode=forward_eps_mode,
        as_of_date=target_date,
    )
    if forecast_snapshot is None or forecast_snapshot.value <= 0:
        return "forward_eps_missing"

    latest_fy = latest_value_bps_statement(
        raw_statements,
        baseline_shares,
        as_of_date=target_date,
    )
    if latest_fy is None:
        return "bps_missing"
    bps = adjust_per_share_value(
        to_nullable_float(latest_fy["bps"]),
        to_nullable_float(latest_fy["shares_outstanding"]),
        baseline_shares,
    )
    if positive_ratio(price, bps) is None:
        return "bps_missing"
    if positive_ratio(price, forecast_snapshot.value) is None:
        return "forward_eps_missing"
    return "not_rankable"


def apply_value_composite_profile(
    frame: pd.DataFrame,
    profile: ValueCompositeProfileSpec,
    *,
    apply_liquidity_filter: bool,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    base_score = pd.to_numeric(result[VALUE_COMPOSITE_SCORE_COLUMN], errors="coerce")
    result["score_before_boost"] = base_score
    result["breakout_boost"] = 0.0
    if profile.breakout_window is not None and profile.breakout_lookback_sessions is not None:
        days_column = f"days_since_new_high_{profile.breakout_window}d"
        if days_column in result.columns and profile.breakout_score_boost is not None:
            denominator = max(int(profile.breakout_lookback_sessions), 1)
            days_since = pd.to_numeric(result[days_column], errors="coerce")
            recency = (
                (denominator - days_since.clip(lower=0, upper=denominator))
                / denominator
            ).fillna(0.0)
            result["breakout_boost"] = recency * float(profile.breakout_score_boost)
    result[VALUE_COMPOSITE_SCORE_COLUMN] = base_score + pd.to_numeric(
        result["breakout_boost"],
        errors="coerce",
    ).fillna(0.0)
    if profile.min_adv60_mil_jpy is not None:
        adv60 = pd.to_numeric(result["avg_trading_value_60d_mil_jpy"], errors="coerce")
        result["liquidity_eligible"] = adv60 >= float(profile.min_adv60_mil_jpy)
        if apply_liquidity_filter:
            result = result[result["liquidity_eligible"]].copy()
    return result


def apply_value_composite_profile_if_requested(
    scored: pd.DataFrame,
    reader: MarketDbReader,
    *,
    target_date: str,
    profile: ValueCompositeProfileSpec | None,
    apply_liquidity_filter: bool,
) -> pd.DataFrame:
    if profile is None or scored.empty:
        return scored
    scored = append_value_composite_profile_metrics(
        scored,
        reader,
        target_date=target_date,
        profile=profile,
    )
    scored = apply_value_composite_profile(
        scored,
        profile,
        apply_liquidity_filter=apply_liquidity_filter,
    )
    return scored.sort_values(
        [VALUE_COMPOSITE_SCORE_COLUMN, "code"],
        ascending=[False, True],
        kind="stable",
    ).reset_index(drop=True)


def build_value_composite_score_frame_from_adjusted(
    adjusted: pd.DataFrame,
    *,
    weights: Mapping[str, float],
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for row in adjusted.to_dict(orient="records"):
        price = finite_or_none(row.get("current_price"))
        volume = finite_or_none(row.get("volume"))
        pbr = finite_or_none(row.get("pbr"))
        forward_per = finite_or_none(row.get("forward_per"))
        market_cap = finite_or_none(row.get("market_cap"))
        if price is None or price <= 0:
            continue
        records.append(
            {
                "code": str(row["code"]),
                "company_name": str(row["company_name"]),
                "market_code": str(row["market_code"]),
                "market": canonical_market_label(str(row["market_code"])),
                "sector_33_name": str(row["sector_33_name"]),
                "current_price": price,
                "volume": volume if volume is not None else 0.0,
                "pbr": pbr,
                "forward_per": forward_per,
                "market_cap_bil_jpy": (
                    market_cap / 1_000_000_000.0
                    if market_cap is not None
                    else None
                ),
                "bps": finite_or_none(row.get("bps")),
                "forward_eps": finite_or_none(row.get("forward_eps")),
                "latest_fy_disclosed_date": str_or_none(
                    row.get("statement_disclosed_date")
                ),
                "forward_eps_disclosed_date": str_or_none(
                    row.get("forward_eps_disclosed_date")
                ),
                "forward_eps_source": str_or_none(row.get("forward_eps_source")),
            }
        )
    return score_value_composite_records(records, weights=weights)


def score_value_composite_records(
    records: list[dict[str, Any]],
    *,
    weights: Mapping[str, float],
) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    scored = build_value_composite_score_frame(
        pd.DataFrame.from_records(records),
        group_columns=("market",),
        required_positive_columns=VALUE_COMPOSITE_REQUIRED_POSITIVE_COLUMNS,
        score_column=VALUE_COMPOSITE_SCORE_COLUMN,
        weights=weights,
    )
    scored = scored[
        pd.to_numeric(scored[VALUE_COMPOSITE_SCORE_COLUMN], errors="coerce").notna()
    ].copy()
    return scored.sort_values(
        [VALUE_COMPOSITE_SCORE_COLUMN, "code"],
        ascending=[False, True],
        kind="stable",
    ).reset_index(drop=True)


def build_value_composite_score_frame_from_statement_rows(
    stock_rows: list[Mapping[str, Any]],
    statement_rows: list[Mapping[str, Any]],
    *,
    target_date: str,
    price_basis_date: str,
    adjustment_events_by_code: Mapping[str, list[ShareAdjustmentEvent]],
    valuation_calculator: FundamentalsCalculator,
    weights: Mapping[str, float],
    forward_eps_mode: ranking_contracts.ValueCompositeForwardEpsMode,
) -> pd.DataFrame:
    raw_statements_by_code: dict[str, list[Mapping[str, Any]]] = {}
    for row in statement_rows:
        code = str(row["code"])
        raw_statements_by_code.setdefault(code, []).append(row)

    records: list[dict[str, Any]] = []
    for stock in stock_rows:
        code = str(stock["code"])
        raw_statements = raw_statements_by_code.get(code, [])
        if not raw_statements:
            continue
        price = to_nullable_float(stock["current_price"])
        volume = to_nullable_float(stock["volume"])
        if price is None or price <= 0:
            continue

        valuation_rows = (
            raw_statements
            if forward_eps_mode == "latest"
            else [
                row
                for row in raw_statements
                if normalize_period_label(row["type_of_current_period"]) == "FY"
            ]
        )
        valuation_statements = [
            market_statement_row_to_jquants_statement(row, code_fallback=code)
            for row in valuation_rows
        ]
        valuation = valuation_calculator.calculate_latest_valuation(
            valuation_statements,
            close=price,
            price_date=target_date,
            prefer_consolidated=True,
            share_adjustment_events=adjustment_events_by_code.get(code, []),
            price_basis_date=price_basis_date,
        )
        if valuation is None:
            continue

        pbr = valuation.pbr
        forward_per = valuation.forwardPer
        market_cap_bil_jpy = (
            valuation.marketCap / 1_000_000_000.0
            if valuation.marketCap is not None
            else None
        )
        bps = price / pbr if pbr is not None and pbr > 0 else None

        records.append(
            {
                "code": code,
                "company_name": str(stock["company_name"]),
                "market_code": str(stock["market_code"]),
                "market": canonical_market_label(str(stock["market_code"])),
                "sector_33_name": str(stock["sector_33_name"]),
                "current_price": price,
                "volume": volume if volume is not None else 0.0,
                "pbr": pbr,
                "forward_per": forward_per,
                "market_cap_bil_jpy": market_cap_bil_jpy,
                "bps": bps,
                "forward_eps": valuation.forwardEps,
                "latest_fy_disclosed_date": latest_actual_fy_disclosed_date(
                    raw_statements,
                    as_of_date=target_date,
                ),
                "forward_eps_disclosed_date": valuation.forwardEpsDisclosedDate,
                "forward_eps_source": valuation.forwardEpsSource,
            }
        )

    return score_value_composite_records(records, weights=weights)


def build_value_composite_ranking_items(
    scored: pd.DataFrame,
    reader: MarketDbReader,
    *,
    target_date: str,
) -> list[ranking_contracts.ValueCompositeRankingItem]:
    scored = append_value_composite_technical_metrics(
        scored,
        reader,
        target_date=target_date,
    )
    return [
        build_value_composite_item(cast(Mapping[str, Any], row), rank)
        for rank, row in enumerate(scored.to_dict(orient="records"), start=1)
    ]


def find_value_composite_score_item(
    scored: pd.DataFrame,
    reader: MarketDbReader,
    *,
    normalized_target_code: str,
    target_date: str,
) -> tuple[ranking_contracts.ValueCompositeRankingItem | None, int]:
    rows = scored.to_dict(orient="records")
    for rank, row in enumerate(rows, start=1):
        if normalize_equity_code(row["code"]) != normalized_target_code:
            continue
        row_payload: dict[str, Any] = {str(key): value for key, value in row.items()}
        row_df = append_value_composite_technical_metrics(
            pd.DataFrame.from_records([row_payload]),
            reader,
            target_date=target_date,
        )
        return (
            build_value_composite_item(
                cast(Mapping[str, Any], row_df.iloc[0].to_dict()),
                rank,
            ),
            len(rows),
        )
    return None, len(rows)


def find_value_composite_target_stock(
    stock_rows: list[Mapping[str, Any]],
    code: str,
) -> Mapping[str, Any] | None:
    normalized_target_code = normalize_equity_code(code)
    for row in stock_rows:
        if normalize_equity_code(row["code"]) == normalized_target_code:
            return row
    return None
