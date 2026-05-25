"""
Ranking Service

DuckDB market data からランキングデータを取得するサービス。
"""

from __future__ import annotations

import math
from datetime import UTC, date as calendar_date, datetime, timedelta
from collections.abc import Mapping
from typing import Any, Literal, cast

import pandas as pd

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.shared.utils.market_code_alias import resolve_market_codes
from src.application.services.ranking_query_helpers import (
    canonical_market_label as _canonical_market_label,
    normalize_equity_code as _normalize_equity_code,
    normalized_code_sql as _normalized_code_sql,
    positive_ratio as _positive_ratio,
    prefer_4digit_order_sql as _prefer_4digit_order_sql,
)
from src.application.services.ranking_daily_queries import (
    ranking_by_period_high as _ranking_by_period_high_query,
    ranking_by_period_low as _ranking_by_period_low_query,
    ranking_by_price_change as _ranking_by_price_change_query,
    ranking_by_price_change_from_days as _ranking_by_price_change_from_days_query,
    ranking_by_trading_value as _ranking_by_trading_value_query,
    ranking_by_trading_value_average as _ranking_by_trading_value_average_query,
)
from src.application.services.ranking_fundamental_queries import (
    load_adjustment_events_by_code as _load_adjustment_events_by_code_query,
    load_adjusted_daily_valuation_frame as _load_adjusted_daily_valuation_frame_query,
    load_adjusted_statement_metric_rows as _load_adjusted_statement_metric_rows_query,
    load_fundamental_statement_rows as _load_fundamental_statement_rows_query,
    load_fundamental_stock_rows as _load_fundamental_stock_rows_query,
    resolve_latest_stock_data_date as _resolve_latest_stock_data_date_query,
    table_exists as _table_exists_query,
)
from src.shared.utils.share_adjustment import (
    ShareAdjustmentEvent,
    adjust_free_float_shares_to_price_basis,
    adjust_share_count_to_price_basis,
    resolve_latest_quarterly_share_snapshot,
)
from src.domains.fundamentals import (
    FundamentalsCalculator,
    market_statement_row_to_jquants_statement,
)
from src.domains.analytics.fundamental_ranking import (
    FundamentalItem,
    FundamentalRankingCalculator,
    ForecastValue as _ForecastValue,
    StatementRow as _StatementRow,
    adjust_per_share_value as _adjust_per_share_value,
    normalize_period_label as _normalize_period_label,
    to_nullable_float as _to_nullable_float,
)
from src.domains.analytics.value_composite_scoring import (
    VALUE_COMPOSITE_SCORE_COLUMN,
)
from src.application.services.ranking_value_composite_config import (
    VALUE_COMPOSITE_AUTO_SCORE_METHOD_BY_MARKET as _VALUE_COMPOSITE_AUTO_SCORE_METHOD_BY_MARKET,
    VALUE_COMPOSITE_METRIC_KEY as _VALUE_COMPOSITE_METRIC_KEY,
    VALUE_COMPOSITE_WEIGHTS_BY_METHOD as _VALUE_COMPOSITE_WEIGHTS_BY_METHOD,
    ValueCompositeProfileSpec as _ValueCompositeProfileSpec,
    ensure_supported_value_composite_forward_eps_mode as _ensure_supported_value_composite_forward_eps_mode,
    normalize_value_composite_weights as _normalize_value_composite_weights,
    resolve_value_composite_profile_and_score_method as _resolve_value_composite_profile_and_score_method,
    value_composite_ranking_score_policy as _value_composite_ranking_score_policy,
    value_composite_response_weights as _value_composite_response_weights,
    value_composite_score_policy as _value_composite_score_policy,
)
from src.application.services.ranking_value_composite_metrics import (
    append_value_composite_profile_metrics as _append_value_composite_profile_metrics_query,
    apply_value_composite_profile as _apply_value_composite_profile_frame,
    build_value_composite_score_frame_from_adjusted as _build_value_composite_score_frame_from_adjusted,
    build_value_composite_score_frame_from_statement_rows as _build_value_composite_score_frame_from_statement_rows,
    build_value_composite_ranking_items as _build_value_composite_ranking_items,
    find_value_composite_score_item as _find_value_composite_score_item,
    find_value_composite_target_stock as _find_value_composite_target_stock,
    resolve_value_composite_symbol_target_date as _resolve_value_composite_symbol_target_date_query,
    resolve_value_composite_target_date as _resolve_value_composite_target_date_query,
)
from src.application.services.ranking_valuation import (
    with_prime_valuation_percentiles,
)
from src.application.services.ranking_response_items import (
    build_fundamental_ranking_item,
    build_value_composite_score_response,
    finite_or_none as _finite_or_none,
    str_or_none as _str_or_none,
)
from src.application.services.ranking_statement_selection import (
    latest_value_bps_statement,
)
from src.application.services.ranking_statement_rows import (
    statement_rows_by_code,
    statement_rows_from_mappings,
)
from src.application.services.ranking_index_performance import (
    load_index_performance,
)
from src.application.services.ranking_liquidity import (
    PrimeLiquidityMetrics,
    classify_prime_liquidity_regime,
    classify_risk_flags,
    fit_log_liquidity_regression,
)
from src.entrypoints.http.schemas.ranking import (
    FundamentalRankingItem,
    FundamentalRankings,
    MarketFundamentalRankingResponse,
    MarketRankingResponse,
    RankingItem,
    RankingStateFilter,
    Rankings,
    ValueCompositeRankingResponse,
    ValueCompositeScoreResponse,
    ValueCompositeForwardEpsMode,
    ValueCompositeProfileId,
    ValueCompositeScoreUnavailableReason,
    ValueCompositeScoreMethod,
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


_SUPPORTED_FUNDAMENTAL_RATIO_METRIC_KEY = "eps_forecast_to_actual"


class RankingService:
    """マーケットランキングサービス"""

    def __init__(self, reader: MarketDbReader) -> None:
        self._reader = reader
        self._fundamental_calculator = FundamentalRankingCalculator()
        self._valuation_calculator = FundamentalsCalculator()

    def _table_exists(self, table_name: str) -> bool:
        return _table_exists_query(self._reader, table_name)

    def get_rankings(
        self,
        date: str | None = None,
        limit: int = 20,
        markets: str = "prime",
        lookback_days: int = 1,
        period_days: int = 250,
        sector33_name: str | None = None,
        sector17_name: str | None = None,
        include_valuation: bool = False,
        forward_eps_disclosed_within_days: int = 0,
        liquidity_state: RankingStateFilter | None = None,
    ) -> MarketRankingResponse:
        """ランキングデータを取得"""
        requested_market_codes, query_market_codes = resolve_market_codes(markets)

        # 対象日を決定
        if date:
            target_date = date
        else:
            target_date = _resolve_latest_stock_data_date_query(self._reader)

        apply_forward_eps_filter = include_valuation and forward_eps_disclosed_within_days > 0
        apply_liquidity_state_filter = include_valuation and liquidity_state is not None
        query_limit = 0 if apply_forward_eps_filter or apply_liquidity_state_filter else limit

        # 5種類のランキングを取得
        if lookback_days > 1:
            trading_value = _ranking_by_trading_value_average_query(
                self._reader,
                target_date,
                lookback_days,
                query_limit,
                query_market_codes,
                sector33_name=sector33_name,
                sector17_name=sector17_name,
            )
        else:
            trading_value = _ranking_by_trading_value_query(
                self._reader,
                target_date,
                query_limit,
                query_market_codes,
                sector33_name=sector33_name,
                sector17_name=sector17_name,
            )

        if lookback_days > 1:
            gainers = _ranking_by_price_change_from_days_query(
                self._reader,
                target_date,
                lookback_days,
                query_limit,
                query_market_codes,
                "DESC",
                sector33_name=sector33_name,
                sector17_name=sector17_name,
            )
            losers = _ranking_by_price_change_from_days_query(
                self._reader,
                target_date,
                lookback_days,
                query_limit,
                query_market_codes,
                "ASC",
                sector33_name=sector33_name,
                sector17_name=sector17_name,
            )
        else:
            gainers = _ranking_by_price_change_query(
                self._reader,
                target_date,
                query_limit,
                query_market_codes,
                "DESC",
                sector33_name=sector33_name,
                sector17_name=sector17_name,
            )
            losers = _ranking_by_price_change_query(
                self._reader,
                target_date,
                query_limit,
                query_market_codes,
                "ASC",
                sector33_name=sector33_name,
                sector17_name=sector17_name,
            )

        period_high = _ranking_by_period_high_query(
            self._reader,
            target_date,
            period_days,
            query_limit,
            query_market_codes,
            sector33_name=sector33_name,
            sector17_name=sector17_name,
        )
        period_low = _ranking_by_period_low_query(
            self._reader,
            target_date,
            period_days,
            query_limit,
            query_market_codes,
            sector33_name=sector33_name,
            sector17_name=sector17_name,
        )
        ranking_collections = (trading_value, gainers, losers, period_high, period_low)
        if include_valuation:
            price_basis_date = _resolve_latest_stock_data_date_query(self._reader)
            self._enrich_ranking_collections_with_valuation(
                ranking_collections,
                target_date=target_date,
                query_market_codes=query_market_codes,
                price_basis_date=price_basis_date,
            )
            self._filter_ranking_collections_by_forward_eps_source_date(
                ranking_collections,
                target_date=target_date,
                forward_eps_disclosed_within_days=forward_eps_disclosed_within_days,
            )
            if apply_liquidity_state_filter:
                self._enrich_ranking_collections_with_prime_liquidity(
                    ranking_collections,
                    target_date=target_date,
                    price_basis_date=price_basis_date,
                )
                self._filter_ranking_collections_by_liquidity_state(
                    ranking_collections,
                    liquidity_state=liquidity_state,
                )
                self._limit_and_rerank_ranking_collections(ranking_collections, limit)
            else:
                self._limit_and_rerank_ranking_collections(ranking_collections, limit)
                self._enrich_ranking_collections_with_prime_liquidity(
                    ranking_collections,
                    target_date=target_date,
                    price_basis_date=price_basis_date,
                )
        index_performance = load_index_performance(
            self._reader,
            table_exists=self._table_exists,
            date=target_date,
            lookback_days=lookback_days,
        )

        return MarketRankingResponse(
            date=target_date,
            markets=requested_market_codes,
            lookbackDays=lookback_days,
            periodDays=period_days,
            rankings=Rankings(
                tradingValue=trading_value,
                gainers=gainers,
                losers=losers,
                periodHigh=period_high,
                periodLow=period_low,
            ),
            indexPerformance=index_performance,
            lastUpdated=_now_iso(),
        )

    def get_fundamental_rankings(
        self,
        limit: int = 20,
        markets: str = "prime",
        metric_key: str = _SUPPORTED_FUNDAMENTAL_RATIO_METRIC_KEY,
        forecast_above_recent_fy_actuals: bool = False,
        forecast_lookback_fy_count: int = 3,
    ) -> MarketFundamentalRankingResponse:
        """最新の予想EPS / 最新の実績EPS 比率ランキングを取得"""
        if metric_key != _SUPPORTED_FUNDAMENTAL_RATIO_METRIC_KEY:
            raise ValueError(f"Unsupported metricKey: {metric_key}")
        if forecast_lookback_fy_count < 1:
            raise ValueError("forecast_lookback_fy_count must be >= 1")

        requested_market_codes, query_market_codes = resolve_market_codes(markets)
        target_date = _resolve_latest_stock_data_date_query(self._reader)

        stock_rows = _load_fundamental_stock_rows_query(
            self._reader,
            target_date,
            query_market_codes,
        )
        adjusted_valuation = _load_adjusted_daily_valuation_frame_query(
            self._reader,
            target_date,
            query_market_codes,
        )
        can_use_adjusted_valuation = not adjusted_valuation.empty and (
            not forecast_above_recent_fy_actuals
            or self._table_exists("statement_metrics_adjusted")
        )
        if can_use_adjusted_valuation:
            ratio_candidates = self._build_adjusted_fundamental_ratio_candidates(
                adjusted_valuation,
                target_date=target_date,
                market_codes=query_market_codes,
                forecast_above_recent_fy_actuals=forecast_above_recent_fy_actuals,
                forecast_lookback_fy_count=forecast_lookback_fy_count,
            )
            ratio_high = self._rank_fundamental_items(
                ratio_candidates, limit, descending=True
            )
            ratio_low = self._rank_fundamental_items(
                ratio_candidates, limit, descending=False
            )
            return MarketFundamentalRankingResponse(
                date=target_date,
                markets=requested_market_codes,
                metricKey=metric_key,
                rankings=FundamentalRankings(
                    ratioHigh=ratio_high,
                    ratioLow=ratio_low,
                ),
                lastUpdated=_now_iso(),
            )

        statement_rows = _load_fundamental_statement_rows_query(
            self._reader,
            target_date,
            query_market_codes,
        )

        statements_by_code = statement_rows_by_code(statement_rows)

        ratio_candidates: list[FundamentalItem] = []
        for stock in stock_rows:
            code = str(stock["code"])
            statements = statements_by_code.get(code)
            if not statements:
                continue

            baseline_shares = self._fundamental_calculator.resolve_baseline_shares(
                statements,
                as_of_date=target_date,
            )
            actual_snapshot = (
                self._fundamental_calculator.resolve_latest_actual_snapshot(
                    statements,
                    baseline_shares,
                    as_of_date=target_date,
                )
            )
            forecast_snapshot = (
                self._fundamental_calculator.resolve_latest_forecast_snapshot(
                    statements,
                    baseline_shares,
                    as_of_date=target_date,
                )
            )

            if forecast_above_recent_fy_actuals:
                if forecast_snapshot is None:
                    continue
                recent_max_actual_eps = (
                    self._fundamental_calculator.resolve_recent_actual_eps_max(
                        statements,
                        baseline_shares,
                        forecast_lookback_fy_count,
                        as_of_date=target_date,
                    )
                )
                if (
                    recent_max_actual_eps is None
                    or forecast_snapshot.value <= recent_max_actual_eps
                ):
                    continue

            ratio_snapshot = self._fundamental_calculator.resolve_latest_ratio_snapshot(
                actual_snapshot, forecast_snapshot
            )

            if ratio_snapshot is None:
                continue
            ratio_candidates.append(
                self._fundamental_calculator.build_fundamental_item(
                    stock, ratio_snapshot
                )
            )

        ratio_high = self._rank_fundamental_items(
            ratio_candidates, limit, descending=True
        )
        ratio_low = self._rank_fundamental_items(
            ratio_candidates, limit, descending=False
        )

        return MarketFundamentalRankingResponse(
            date=target_date,
            markets=requested_market_codes,
            metricKey=metric_key,
            rankings=FundamentalRankings(
                ratioHigh=ratio_high,
                ratioLow=ratio_low,
            ),
            lastUpdated=_now_iso(),
        )

    def get_value_composite_ranking(
        self,
        date: str | None = None,
        limit: int = 50,
        markets: str = "standard",
        score_method: ValueCompositeScoreMethod | None = None,
        profile_id: ValueCompositeProfileId | None = None,
        forward_eps_mode: ValueCompositeForwardEpsMode = "latest",
        apply_liquidity_filter: bool = True,
    ) -> ValueCompositeRankingResponse:
        """Standard市場向けの小型バリュー複合スコアランキングを取得"""

        profile, resolved_score_method = (
            _resolve_value_composite_profile_and_score_method(
                profile_id=profile_id,
                score_method=score_method,
            )
        )
        _ensure_supported_value_composite_forward_eps_mode(forward_eps_mode)
        weights = _normalize_value_composite_weights(
            _VALUE_COMPOSITE_WEIGHTS_BY_METHOD[resolved_score_method]
        )
        requested_market_codes, query_market_codes = resolve_market_codes(
            markets,
            fallback=["standard"],
        )
        target_date = _resolve_value_composite_target_date_query(self._reader, date)
        scored = self._load_value_composite_scored_frame(
            target_date=target_date,
            query_market_codes=query_market_codes,
            weights=weights,
            forward_eps_mode=forward_eps_mode,
        )
        scored = self._apply_value_composite_profile_if_requested(
            scored,
            target_date=target_date,
            profile=profile,
            apply_liquidity_filter=apply_liquidity_filter,
        )
        items = _build_value_composite_ranking_items(
            scored.head(limit),
            self._reader,
            target_date=target_date,
        )

        return ValueCompositeRankingResponse(
            date=target_date,
            markets=requested_market_codes,
            metricKey=_VALUE_COMPOSITE_METRIC_KEY,
            profileId=profile.profile_id if profile is not None else None,
            profileLabel=profile.label if profile is not None else None,
            scoreMethod=resolved_score_method,
            forwardEpsMode=forward_eps_mode,
            rebalanceMonths=profile.rebalance_months if profile is not None else None,
            breakoutWindow=profile.breakout_window if profile is not None else None,
            breakoutLookbackSessions=(
                profile.breakout_lookback_sessions if profile is not None else None
            ),
            breakoutScoreBoost=profile.breakout_score_boost if profile is not None else None,
            applyLiquidityFilter=apply_liquidity_filter,
            scorePolicy=_value_composite_ranking_score_policy(
                resolved_score_method,
                forward_eps_mode,
                profile=profile,
                apply_liquidity_filter=apply_liquidity_filter,
            ),
            weights=_value_composite_response_weights(weights),
            itemCount=len(items),
            items=items,
            lastUpdated=_now_iso(),
        )

    def _apply_value_composite_profile_if_requested(
        self,
        scored: pd.DataFrame,
        *,
        target_date: str,
        profile: _ValueCompositeProfileSpec | None,
        apply_liquidity_filter: bool,
    ) -> pd.DataFrame:
        if profile is None or scored.empty:
            return scored
        scored = _append_value_composite_profile_metrics_query(
            scored,
            self._reader,
            target_date=target_date,
            profile=profile,
        )
        scored = _apply_value_composite_profile_frame(
            scored,
            profile,
            apply_liquidity_filter=apply_liquidity_filter,
        )
        return scored.sort_values(
            [VALUE_COMPOSITE_SCORE_COLUMN, "code"],
            ascending=[False, True],
            kind="stable",
        ).reset_index(drop=True)

    def get_value_composite_score(
        self,
        code: str,
        date: str | None = None,
        forward_eps_mode: ValueCompositeForwardEpsMode = "latest",
    ) -> ValueCompositeScoreResponse:
        """単一銘柄の market-specific value composite score を取得"""

        _ensure_supported_value_composite_forward_eps_mode(forward_eps_mode)
        target_date = _resolve_value_composite_target_date_query(self._reader, date)
        target_date = _resolve_value_composite_symbol_target_date_query(
            self._reader,
            code,
            target_date,
        )
        normalized_target_code = _normalize_equity_code(code)
        _, target_market_codes = resolve_market_codes(
            "prime,standard,growth",
            fallback=["prime", "standard", "growth"],
        )
        target_stock = _find_value_composite_target_stock(
            _load_fundamental_stock_rows_query(
                self._reader,
                target_date,
                target_market_codes,
            ),
            code,
        )
        last_updated = _now_iso()
        if target_stock is None:
            return build_value_composite_score_response(
                date=target_date,
                code=code,
                forward_eps_mode=forward_eps_mode,
                score_available=False,
                unsupported_reason="not_found",
                last_updated=last_updated,
            )

        market = _canonical_market_label(str(target_stock["market_code"]))
        score_method = _VALUE_COMPOSITE_AUTO_SCORE_METHOD_BY_MARKET.get(market)
        if score_method is None:
            return build_value_composite_score_response(
                date=target_date,
                code=str(target_stock["code"]),
                target_stock=target_stock,
                market=market,
                forward_eps_mode=forward_eps_mode,
                score_available=False,
                unsupported_reason="unsupported_market",
                last_updated=last_updated,
            )

        weights = _normalize_value_composite_weights(
            _VALUE_COMPOSITE_WEIGHTS_BY_METHOD[score_method]
        )
        _, query_market_codes = resolve_market_codes(market, fallback=[market])
        scored = self._load_value_composite_scored_frame(
            target_date=target_date,
            query_market_codes=query_market_codes,
            weights=weights,
            forward_eps_mode=forward_eps_mode,
        )
        item, universe_count = _find_value_composite_score_item(
            scored,
            self._reader,
            normalized_target_code=normalized_target_code,
            target_date=target_date,
        )
        if item is not None:
            return build_value_composite_score_response(
                date=target_date,
                code=str(target_stock["code"]),
                target_stock=target_stock,
                market=market,
                score_method=score_method,
                forward_eps_mode=forward_eps_mode,
                score_policy=_value_composite_score_policy(
                    score_method,
                    forward_eps_mode,
                ),
                weights=_value_composite_response_weights(weights),
                universe_count=universe_count,
                score_available=True,
                item=item,
                last_updated=last_updated,
            )

        unsupported_reason = self._resolve_value_composite_unavailable_reason(
            target_stock=target_stock,
            target_date=target_date,
            query_market_codes=query_market_codes,
            forward_eps_mode=forward_eps_mode,
            price_basis_date=_resolve_latest_stock_data_date_query(self._reader),
        )
        return build_value_composite_score_response(
            date=target_date,
            code=str(target_stock["code"]),
            target_stock=target_stock,
            market=market,
            score_method=score_method,
            forward_eps_mode=forward_eps_mode,
            score_policy=_value_composite_score_policy(
                score_method,
                forward_eps_mode,
            ),
            weights=_value_composite_response_weights(weights),
            universe_count=universe_count,
            score_available=False,
            unsupported_reason=unsupported_reason,
            last_updated=last_updated,
        )

    # --- Private ranking methods ---

    def _load_value_composite_scored_frame(
        self,
        *,
        target_date: str,
        query_market_codes: list[str],
        weights: Mapping[str, float],
        forward_eps_mode: ValueCompositeForwardEpsMode,
    ) -> pd.DataFrame:
        if forward_eps_mode == "latest":
            adjusted = _load_adjusted_daily_valuation_frame_query(
                self._reader,
                target_date,
                query_market_codes,
            )
            if not adjusted.empty:
                scored = _build_value_composite_score_frame_from_adjusted(
                    adjusted,
                    weights=weights,
                )
                if not scored.empty:
                    return scored

        stock_rows = _load_fundamental_stock_rows_query(
            self._reader,
            target_date,
            query_market_codes,
        )
        statement_rows = _load_fundamental_statement_rows_query(
            self._reader,
            target_date,
            query_market_codes,
        )
        price_basis_date = _resolve_latest_stock_data_date_query(self._reader)
        adjustment_events_by_code = _load_adjustment_events_by_code_query(
            self._reader,
            through_date=price_basis_date,
            market_codes=query_market_codes,
        )

        return _build_value_composite_score_frame_from_statement_rows(
            stock_rows,
            statement_rows,
            target_date=target_date,
            price_basis_date=price_basis_date,
            adjustment_events_by_code=adjustment_events_by_code,
            valuation_calculator=self._valuation_calculator,
            weights=weights,
            forward_eps_mode=forward_eps_mode,
        )

    def _enrich_ranking_collections_with_valuation(
        self,
        collections: tuple[list[RankingItem], ...],
        *,
        target_date: str,
        query_market_codes: list[str],
        price_basis_date: str,
    ) -> None:
        items_by_code: dict[str, list[RankingItem]] = {}
        for collection in collections:
            for item in collection:
                items_by_code.setdefault(_normalize_equity_code(item.code), []).append(item)
        if not items_by_code:
            return

        enriched_codes = self._enrich_items_from_adjusted_daily_valuation(
            items_by_code,
            target_date=target_date,
            query_market_codes=query_market_codes,
        )
        if len(enriched_codes) == len(items_by_code):
            return

        statement_rows = _load_fundamental_statement_rows_query(
            self._reader,
            target_date,
            query_market_codes,
        )
        raw_statements_by_code: dict[str, list[Mapping[str, Any]]] = {}
        for row in statement_rows:
            code = _normalize_equity_code(row["code"])
            if code in items_by_code and code not in enriched_codes:
                raw_statements_by_code.setdefault(code, []).append(row)

        adjustment_events_by_code = _load_adjustment_events_by_code_query(
            self._reader,
            through_date=price_basis_date,
            market_codes=query_market_codes,
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
            valuation = self._valuation_calculator.calculate_latest_valuation(
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

    def _enrich_items_from_adjusted_daily_valuation(
        self,
        items_by_code: Mapping[str, list[RankingItem]],
        *,
        target_date: str,
        query_market_codes: list[str],
    ) -> set[str]:
        valuation_frame = _load_adjusted_daily_valuation_frame_query(
            self._reader,
            target_date,
            query_market_codes,
        )
        if valuation_frame.empty:
            return set()
        valuation_frame = with_prime_valuation_percentiles(valuation_frame)

        enriched_codes: set[str] = set()
        for row in valuation_frame.to_dict("records"):
            code = _normalize_equity_code(row.get("code"))
            items = items_by_code.get(code)
            if not items:
                continue
            raw_source = _str_or_none(row.get("forward_eps_source"))
            source = raw_source if raw_source in ("revised", "fy") else None
            for item in items:
                item.per = _finite_or_none(row.get("per"))
                item.perPercentile = _finite_or_none(row.get("per_percentile"))
                item.forwardPer = _finite_or_none(row.get("forward_per"))
                item.forwardPerPercentile = _finite_or_none(
                    row.get("forward_per_percentile")
                )
                item.pOp = _finite_or_none(row.get("p_op"))
                item.forwardPOp = _finite_or_none(row.get("forward_p_op"))
                item.forwardPOpPercentile = _finite_or_none(
                    row.get("forward_p_op_percentile")
                )
                item.forwardEpsDisclosedDate = _str_or_none(
                    row.get("forward_eps_disclosed_date")
                )
                item.forwardEpsSource = cast(Literal["revised", "fy"] | None, source)
                item.pbr = _finite_or_none(row.get("pbr"))
                item.pbrPercentile = _finite_or_none(row.get("pbr_percentile"))
                item.marketCap = _finite_or_none(row.get("market_cap"))
            enriched_codes.add(code)
        return enriched_codes

    @staticmethod
    def _filter_ranking_collections_by_forward_eps_source_date(
        collections: tuple[list[RankingItem], ...],
        *,
        target_date: str,
        forward_eps_disclosed_within_days: int,
    ) -> None:
        if forward_eps_disclosed_within_days <= 0:
            return

        try:
            max_date = datetime.fromisoformat(target_date).date()
        except ValueError:
            return
        min_date = max_date - timedelta(days=forward_eps_disclosed_within_days)

        for collection in collections:
            collection[:] = [
                item
                for item in collection
                if RankingService._is_forward_eps_source_date_in_window(
                    item.forwardEpsDisclosedDate,
                    min_date=min_date,
                    max_date=max_date,
                )
            ]

    @staticmethod
    def _is_forward_eps_source_date_in_window(
        disclosed_date: str | None,
        *,
        min_date: calendar_date,
        max_date: calendar_date,
    ) -> bool:
        if disclosed_date is None:
            return False
        try:
            source_date = datetime.fromisoformat(disclosed_date).date()
        except ValueError:
            return False
        return min_date <= source_date <= max_date

    @staticmethod
    def _limit_and_rerank_ranking_collections(
        collections: tuple[list[RankingItem], ...],
        limit: int,
    ) -> None:
        for collection in collections:
            if limit > 0:
                del collection[limit:]
            for rank, item in enumerate(collection, start=1):
                item.rank = rank

    @staticmethod
    def _filter_ranking_collections_by_liquidity_state(
        collections: tuple[list[RankingItem], ...],
        *,
        liquidity_state: RankingStateFilter | None,
    ) -> None:
        if liquidity_state is None:
            return

        for collection in collections:
            if liquidity_state == "overheat":
                collection[:] = [
                    item for item in collection if liquidity_state in item.riskFlags
                ]
            else:
                collection[:] = [
                    item
                    for item in collection
                    if item.liquidityRegime == liquidity_state
                ]

    def _enrich_ranking_collections_with_prime_liquidity(
        self,
        collections: tuple[list[RankingItem], ...],
        *,
        target_date: str,
        price_basis_date: str,
    ) -> None:
        items_by_code: dict[str, list[RankingItem]] = {}
        for collection in collections:
            for item in collection:
                items_by_code.setdefault(_normalize_equity_code(item.code), []).append(item)
        if not items_by_code:
            return

        liquidity_by_code = self._load_prime_liquidity_metrics(
            target_date,
            price_basis_date,
        )
        for code, items in items_by_code.items():
            metrics = liquidity_by_code.get(code)
            if metrics is None:
                continue
            for item in items:
                item.liquidityResidualZ = metrics.liquidity_residual_z
                item.liquidityRegime = metrics.liquidity_regime
                item.adv60ToFreeFloatPct = metrics.adv60_to_free_float_pct
                item.riskFlags = list(dict.fromkeys([*item.riskFlags, *metrics.risk_flags]))

    def _load_prime_liquidity_metrics(
        self,
        target_date: str,
        price_basis_date: str,
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
        stock_code = _normalized_code_sql("s.code")
        price_code = _normalized_code_sql("sd.code")
        statement_code = _normalized_code_sql("st.code")
        price_order = _prefer_4digit_order_sql("sd.code")
        statement_order = _prefer_4digit_order_sql("st.code")
        adjustment_events_by_code = _load_adjustment_events_by_code_query(
            self._reader,
            through_date=price_basis_date,
            market_codes=prime_market_codes,
        )
        rows = self._reader.query(
            f"""
            WITH prime_codes AS (
                SELECT DISTINCT {stock_code} AS code
                FROM stocks s
                WHERE lower(trim(s.market_code)) IN ({market_placeholders})
            ),
            price_base AS (
                SELECT code, date, close, volume
                FROM (
                    SELECT
                        {price_code} AS code,
                        sd.date,
                        sd.close,
                        sd.volume,
                        ROW_NUMBER() OVER (
                            PARTITION BY {price_code}, sd.date
                            ORDER BY {price_order}
                        ) AS rn
                    FROM stock_data sd
                    WHERE sd.date >= ?
                      AND sd.date <= ?
                      AND sd.close > 0
                      AND sd.volume IS NOT NULL
                )
                WHERE rn = 1
            ),
            prime_price AS (
                SELECT price_base.*
                FROM price_base
                JOIN prime_codes USING (code)
            ),
            price_features AS (
                SELECT
                    *,
                    MEDIAN(close * volume) OVER (
                        PARTITION BY code ORDER BY date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) AS adv60_jpy,
                    COUNT(*) OVER (
                        PARTITION BY code ORDER BY date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) AS adv60_count,
                    LAG(close, 20) OVER (PARTITION BY code ORDER BY date) AS close_20d_ago,
                    LAG(close, 60) OVER (PARTITION BY code ORDER BY date) AS close_60d_ago
                FROM prime_price
            ),
            statement_base AS (
                SELECT code, disclosed_date, shares_outstanding, treasury_shares
                FROM (
                    SELECT
                        {statement_code} AS code,
                        st.disclosed_date,
                        st.shares_outstanding,
                        st.treasury_shares,
                        ROW_NUMBER() OVER (
                            PARTITION BY {statement_code}, st.disclosed_date
                            ORDER BY {statement_order}
                        ) AS rn
                    FROM statements st
                    WHERE st.disclosed_date <= ?
                      AND st.shares_outstanding > 0
                )
                WHERE rn = 1
            ),
            statement_asof AS (
                SELECT code, disclosed_date, shares_outstanding, treasury_shares
                FROM (
                    SELECT
                        code,
                        disclosed_date,
                        shares_outstanding,
                        treasury_shares,
                        ROW_NUMBER() OVER (
                            PARTITION BY code
                            ORDER BY disclosed_date DESC
                        ) AS asof_rn
                    FROM statement_base
                )
                WHERE asof_rn = 1
            )
            SELECT
                pf.code,
                pf.close,
                CASE WHEN pf.adv60_count >= 60 THEN pf.adv60_jpy ELSE NULL END AS adv60_jpy,
                pf.close_20d_ago,
                pf.close_60d_ago,
                st.disclosed_date,
                st.shares_outstanding,
                st.treasury_shares
            FROM price_features pf
            JOIN statement_asof st USING (code)
            WHERE pf.date = ?
              AND st.shares_outstanding - coalesce(st.treasury_shares, 0) > 0
            ORDER BY pf.code
            """,
            (*prime_market_codes, start_date, target_date, target_date, target_date),
        )

        samples: list[dict[str, float | str | None]] = []
        for row in rows:
            code = _normalize_equity_code(row["code"])
            adv60 = _finite_or_none(row["adv60_jpy"])
            close = _finite_or_none(row["close"])
            shares_outstanding = _finite_or_none(row["shares_outstanding"])
            free_float_shares = adjust_free_float_shares_to_price_basis(
                shares_outstanding,
                _finite_or_none(row["treasury_shares"]),
                adjustment_events_by_code.get(code, []),
                from_date=_str_or_none(row["disclosed_date"]),
                through_date=price_basis_date,
            )
            free_float_market_cap = (
                close * free_float_shares
                if close is not None and free_float_shares is not None
                else None
            )
            if (
                adv60 is None
                or free_float_market_cap is None
                or close is None
                or adv60 <= 0
                or free_float_market_cap <= 0
            ):
                continue
            close_20d_ago = _finite_or_none(row["close_20d_ago"])
            close_60d_ago = _finite_or_none(row["close_60d_ago"])
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

        if len(samples) < 100:
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
            )
        return metrics_by_code

    def _resolve_value_composite_unavailable_reason(
        self,
        *,
        target_stock: Mapping[str, Any],
        target_date: str,
        query_market_codes: list[str],
        forward_eps_mode: ValueCompositeForwardEpsMode,
        price_basis_date: str,
    ) -> ValueCompositeScoreUnavailableReason:
        price = _to_nullable_float(target_stock["current_price"])
        if price is None or price <= 0:
            return "not_rankable"

        target_code = _normalize_equity_code(target_stock["code"])
        statement_rows = _load_fundamental_statement_rows_query(
            self._reader,
            target_date,
            query_market_codes,
        )
        raw_statements = [
            row
            for row in statement_rows
            if _normalize_equity_code(row["code"]) == target_code
        ]
        statements = statement_rows_from_mappings(raw_statements)
        if not statements:
            return "not_rankable"

        adjustment_events_by_code = _load_adjustment_events_by_code_query(
            self._reader,
            through_date=price_basis_date,
            market_codes=query_market_codes,
        )
        baseline_snapshot = self._resolve_baseline_share_snapshot(
            statements,
            as_of_date=target_date,
        )
        baseline_shares = self._adjust_shares_to_price_basis(
            baseline_snapshot.shares if baseline_snapshot is not None else None,
            disclosed_date=(
                baseline_snapshot.disclosed_date
                if baseline_snapshot is not None
                else None
            ),
            events_by_code=adjustment_events_by_code,
            code=str(target_stock["code"]),
            target_date=price_basis_date,
        )
        forecast_snapshot = self._resolve_value_composite_forecast_snapshot(
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
        bps = _adjust_per_share_value(
            _to_nullable_float(latest_fy["bps"]),
            _to_nullable_float(latest_fy["shares_outstanding"]),
            baseline_shares,
        )
        if _positive_ratio(price, bps) is None:
            return "bps_missing"
        if _positive_ratio(price, forecast_snapshot.value) is None:
            return "forward_eps_missing"
        return "not_rankable"

    def _build_adjusted_fundamental_ratio_candidates(
        self,
        adjusted_valuation: pd.DataFrame,
        *,
        target_date: str,
        market_codes: list[str],
        forecast_above_recent_fy_actuals: bool,
        forecast_lookback_fy_count: int,
    ) -> list[FundamentalItem]:
        recent_actual_max_by_code: dict[str, float | None] = {}
        if forecast_above_recent_fy_actuals:
            recent_actual_max_by_code = self._adjusted_recent_actual_eps_max_by_code(
                target_date=target_date,
                market_codes=market_codes,
                lookback_fy_count=forecast_lookback_fy_count,
            )

        candidates: list[FundamentalItem] = []
        for row in adjusted_valuation.to_dict(orient="records"):
            eps = _finite_or_none(row.get("eps"))
            forward_eps = _finite_or_none(row.get("forward_eps"))
            ratio = _positive_ratio(forward_eps, eps)
            if ratio is None:
                continue
            code = str(row["code"])
            if forecast_above_recent_fy_actuals:
                recent_max = recent_actual_max_by_code.get(code)
                if recent_max is None or forward_eps is None or forward_eps <= recent_max:
                    continue
            source_raw = _str_or_none(row.get("forward_eps_source"))
            source = source_raw if source_raw in {"revised", "fy"} else "fy"
            candidates.append(
                FundamentalItem(
                    code=code,
                    company_name=str(row["company_name"]),
                    market_code=str(row["market_code"]),
                    sector_33_name=str(row["sector_33_name"]),
                    current_price=float(row["current_price"]),
                    volume=float(row["volume"]),
                    eps_value=round(ratio, 4),
                    disclosed_date=(
                        _str_or_none(row.get("forward_eps_disclosed_date"))
                        or _str_or_none(row.get("statement_disclosed_date"))
                        or target_date
                    ),
                    period_type="FY",
                    source=cast(Literal["revised", "fy"], source),
                )
            )
        return candidates

    def _adjusted_recent_actual_eps_max_by_code(
        self,
        *,
        target_date: str,
        market_codes: list[str],
        lookback_fy_count: int,
    ) -> dict[str, float | None]:
        rows = _load_adjusted_statement_metric_rows_query(
            self._reader,
            target_date,
            market_codes,
        )
        values_by_code: dict[str, list[float]] = {}
        seen_by_code: dict[str, set[str]] = {}
        for row in rows:
            period_type = _normalize_period_label(_str_or_none(row["period_type"]))
            if period_type != "FY":
                continue
            code = str(row["code"])
            disclosed_date = str(row["disclosed_date"])
            seen = seen_by_code.setdefault(code, set())
            if disclosed_date in seen:
                continue
            eps = _finite_or_none(row["adjusted_eps"])
            if eps is None:
                continue
            seen.add(disclosed_date)
            bucket = values_by_code.setdefault(code, [])
            if len(bucket) < lookback_fy_count:
                bucket.append(eps)
        return {
            code: max(values) if len(values) >= lookback_fy_count else None
            for code, values in values_by_code.items()
        }

    def _resolve_baseline_share_snapshot(
        self,
        rows: list[_StatementRow],
        *,
        as_of_date: str | None = None,
    ):
        eligible_rows = self._fundamental_calculator._rows_as_of(
            rows,
            as_of_date=as_of_date,
        )
        snapshots = [
            (row.period_type, row.disclosed_date, row.shares_outstanding)
            for row in eligible_rows
        ]
        return resolve_latest_quarterly_share_snapshot(snapshots)

    def _adjust_shares_to_price_basis(
        self,
        shares: float | None,
        *,
        disclosed_date: str | None,
        events_by_code: Mapping[str, list[ShareAdjustmentEvent]],
        code: str,
        target_date: str,
        allow_zero: bool = False,
    ) -> float | None:
        return adjust_share_count_to_price_basis(
            shares,
            events_by_code.get(code, []),
            from_date=disclosed_date,
            through_date=target_date,
            allow_zero=allow_zero,
        )

    def _resolve_value_composite_forecast_snapshot(
        self,
        rows: list[_StatementRow],
        baseline_shares: float | None,
        *,
        forward_eps_mode: ValueCompositeForwardEpsMode,
        as_of_date: str | None = None,
    ) -> _ForecastValue | None:
        if forward_eps_mode == "latest":
            return self._fundamental_calculator.resolve_latest_forecast_snapshot(
                rows,
                baseline_shares,
                as_of_date=as_of_date,
            )
        latest_fy = self._fundamental_calculator.resolve_latest_fy_row(
            rows, as_of_date=as_of_date
        )
        return self._fundamental_calculator.resolve_latest_fy_forecast_snapshot(
            latest_fy,
            baseline_shares,
        )

    def _rank_fundamental_items(
        self,
        items: list[FundamentalItem],
        limit: int,
        *,
        descending: bool,
    ) -> list[FundamentalRankingItem]:
        sorted_items = self._fundamental_calculator.rank_fundamental_items(
            items,
            limit,
            descending=descending,
        )
        return [
            build_fundamental_ranking_item(item, index)
            for index, item in enumerate(sorted_items, start=1)
        ]
