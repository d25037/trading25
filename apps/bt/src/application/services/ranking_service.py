"""
Ranking Service

DuckDB market data からランキングデータを取得するサービス。
"""

from __future__ import annotations

from datetime import UTC, datetime
from collections.abc import Mapping
from typing import Any

import pandas as pd

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.shared.utils.market_code_alias import resolve_market_codes
from src.application.services.ranking_query_helpers import (
    canonical_market_label as _canonical_market_label,
    normalize_equity_code as _normalize_equity_code,
    positive_ratio as _positive_ratio,
)
from src.application.services.ranking_daily_queries import (
    ranking_by_period_high as _ranking_by_period_high_query,
    ranking_by_period_low as _ranking_by_period_low_query,
    ranking_by_price_change as _ranking_by_price_change_query,
    ranking_by_price_change_from_days as _ranking_by_price_change_from_days_query,
    ranking_by_trading_value as _ranking_by_trading_value_query,
    ranking_by_trading_value_average as _ranking_by_trading_value_average_query,
)
from src.application.services.ranking_collection_filters import (
    filter_ranking_collections_by_forward_eps_source_date as _filter_ranking_collections_by_forward_eps_source_date,
    filter_ranking_collections_by_liquidity_state as _filter_ranking_collections_by_liquidity_state,
    group_ranking_items_by_normalized_code as _group_ranking_items_by_normalized_code,
    limit_and_rerank_ranking_collections as _limit_and_rerank_ranking_collections,
)
from src.application.services.ranking_fundamental_queries import (
    build_adjusted_fundamental_ratio_candidates as _build_adjusted_fundamental_ratio_candidates,
    load_adjustment_events_by_code as _load_adjustment_events_by_code_query,
    load_adjusted_daily_valuation_frame as _load_adjusted_daily_valuation_frame_query,
    load_fundamental_statement_rows as _load_fundamental_statement_rows_query,
    load_fundamental_stock_rows as _load_fundamental_stock_rows_query,
    resolve_baseline_share_snapshot as _resolve_baseline_share_snapshot,
    resolve_latest_stock_data_date as _resolve_latest_stock_data_date_query,
    table_exists as _table_exists_query,
)
from src.shared.utils.share_adjustment import (
    adjust_share_count_to_price_basis,
)
from src.domains.fundamentals import (
    FundamentalsCalculator,
)
from src.domains.analytics.fundamental_ranking import (
    FundamentalItem,
    FundamentalRankingCalculator,
    adjust_per_share_value as _adjust_per_share_value,
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
    build_value_composite_ranking_items as _build_value_composite_ranking_items,
    find_value_composite_score_item as _find_value_composite_score_item,
    find_value_composite_target_stock as _find_value_composite_target_stock,
    load_value_composite_scored_frame as _load_value_composite_scored_frame,
    resolve_value_composite_forecast_snapshot as _resolve_value_composite_forecast_snapshot,
    resolve_value_composite_symbol_target_date as _resolve_value_composite_symbol_target_date_query,
    resolve_value_composite_target_date as _resolve_value_composite_target_date_query,
)
from src.application.services.ranking_valuation import (
    enrich_items_from_adjusted_daily_valuation as _enrich_items_from_adjusted_daily_valuation,
    enrich_items_from_statement_valuation as _enrich_items_from_statement_valuation,
)
from src.application.services.ranking_response_items import (
    build_ranked_fundamental_items as _build_ranked_fundamental_items,
    build_value_composite_score_response,
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
    enrich_ranking_collections_with_prime_liquidity as _enrich_ranking_collections_with_prime_liquidity,
)
from src.entrypoints.http.schemas.ranking import (
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
            _filter_ranking_collections_by_forward_eps_source_date(
                ranking_collections,
                target_date=target_date,
                forward_eps_disclosed_within_days=forward_eps_disclosed_within_days,
            )
            if apply_liquidity_state_filter:
                _enrich_ranking_collections_with_prime_liquidity(
                    self._reader,
                    ranking_collections,
                    target_date=target_date,
                    price_basis_date=price_basis_date,
                )
                _filter_ranking_collections_by_liquidity_state(
                    ranking_collections,
                    liquidity_state=liquidity_state,
                )
                _limit_and_rerank_ranking_collections(ranking_collections, limit)
            else:
                _limit_and_rerank_ranking_collections(ranking_collections, limit)
                _enrich_ranking_collections_with_prime_liquidity(
                    self._reader,
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
            ratio_candidates = _build_adjusted_fundamental_ratio_candidates(
                self._reader,
                adjusted_valuation,
                target_date=target_date,
                market_codes=query_market_codes,
                forecast_above_recent_fy_actuals=forecast_above_recent_fy_actuals,
                forecast_lookback_fy_count=forecast_lookback_fy_count,
            )
            ratio_high = _build_ranked_fundamental_items(
                self._fundamental_calculator,
                ratio_candidates,
                limit,
                descending=True,
            )
            ratio_low = _build_ranked_fundamental_items(
                self._fundamental_calculator,
                ratio_candidates,
                limit,
                descending=False,
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

        ratio_high = _build_ranked_fundamental_items(
            self._fundamental_calculator,
            ratio_candidates,
            limit,
            descending=True,
        )
        ratio_low = _build_ranked_fundamental_items(
            self._fundamental_calculator,
            ratio_candidates,
            limit,
            descending=False,
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
        scored = _load_value_composite_scored_frame(
            self._reader,
            target_date=target_date,
            query_market_codes=query_market_codes,
            weights=weights,
            forward_eps_mode=forward_eps_mode,
            valuation_calculator=self._valuation_calculator,
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
        scored = _load_value_composite_scored_frame(
            self._reader,
            target_date=target_date,
            query_market_codes=query_market_codes,
            weights=weights,
            forward_eps_mode=forward_eps_mode,
            valuation_calculator=self._valuation_calculator,
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

    def _enrich_ranking_collections_with_valuation(
        self,
        collections: tuple[list[RankingItem], ...],
        *,
        target_date: str,
        query_market_codes: list[str],
        price_basis_date: str,
    ) -> None:
        items_by_code = _group_ranking_items_by_normalized_code(collections)
        if not items_by_code:
            return

        enriched_codes = _enrich_items_from_adjusted_daily_valuation(
            self._reader,
            items_by_code,
            target_date=target_date,
            query_market_codes=query_market_codes,
        )
        if len(enriched_codes) == len(items_by_code):
            return

        _enrich_items_from_statement_valuation(
            self._reader,
            self._valuation_calculator,
            items_by_code,
            enriched_codes,
            target_date=target_date,
            query_market_codes=query_market_codes,
            price_basis_date=price_basis_date,
        )

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
        baseline_snapshot = _resolve_baseline_share_snapshot(
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
        forecast_snapshot = _resolve_value_composite_forecast_snapshot(
            self._fundamental_calculator,
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
