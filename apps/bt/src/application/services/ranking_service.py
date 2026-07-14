"""
Ranking Service

DuckDB market data からランキングデータを取得するサービス。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from src.application.contracts import ranking as ranking_contracts
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.shared.utils.market_code_alias import resolve_market_codes
from src.application.services.ranking_query_helpers import (
    canonical_market_label as _canonical_market_label,
    normalize_equity_code as _normalize_equity_code,
    normalize_sector_filter_name as _normalize_sector_filter_name,
    stocks_canonical_cte,
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
    filter_ranking_collections_by_fundamental_state as _filter_ranking_collections_by_fundamental_state,
    filter_ranking_collections_by_regime_state as _filter_ranking_collections_by_regime_state,
    filter_ranking_collections_by_risk_state as _filter_ranking_collections_by_risk_state,
    filter_ranking_collections_by_technical_state as _filter_ranking_collections_by_technical_state,
    limit_and_rerank_ranking_collections as _limit_and_rerank_ranking_collections,
)
from src.application.services.ranking_fundamental_queries import (
    build_adjusted_fundamental_ratio_candidates as _build_adjusted_fundamental_ratio_candidates,
    load_adjusted_daily_valuation_frame as _load_adjusted_daily_valuation_frame_query,
    load_fundamental_stock_rows as _load_fundamental_stock_rows_query,
    resolve_latest_stock_data_date as _resolve_latest_stock_data_date_query,
    table_exists as _table_exists_query,
)
from src.domains.fundamentals import (
    FundamentalsCalculator,
)
from src.domains.analytics.fundamental_ranking import FundamentalRankingCalculator
from src.application.services.ranking_value_composite_config import (
    VALUE_COMPOSITE_AUTO_SCORE_METHOD_BY_MARKET as _VALUE_COMPOSITE_AUTO_SCORE_METHOD_BY_MARKET,
    VALUE_COMPOSITE_METRIC_KEY as _VALUE_COMPOSITE_METRIC_KEY,
    VALUE_COMPOSITE_WEIGHTS_BY_METHOD as _VALUE_COMPOSITE_WEIGHTS_BY_METHOD,
    ensure_supported_value_composite_forward_eps_mode as _ensure_supported_value_composite_forward_eps_mode,
    normalize_value_composite_weights as _normalize_value_composite_weights,
    resolve_value_composite_profile_and_score_method as _resolve_value_composite_profile_and_score_method,
    value_composite_ranking_score_policy as _value_composite_ranking_score_policy,
    value_composite_response_weights as _value_composite_response_weights,
    value_composite_score_policy as _value_composite_score_policy,
)
from src.application.services.ranking_value_composite_metrics import (
    apply_value_composite_profile_if_requested as _apply_value_composite_profile_if_requested,
    build_value_composite_ranking_items as _build_value_composite_ranking_items,
    find_value_composite_score_item as _find_value_composite_score_item,
    find_value_composite_target_stock as _find_value_composite_target_stock,
    load_value_composite_scored_frame as _load_value_composite_scored_frame,
    resolve_value_composite_symbol_target_date as _resolve_value_composite_symbol_target_date_query,
    resolve_value_composite_target_date as _resolve_value_composite_target_date_query,
    resolve_value_composite_unavailable_reason as _resolve_value_composite_unavailable_reason,
)
from src.application.services.ranking_valuation import (
    enrich_ranking_collections_with_valuation as _enrich_ranking_collections_with_valuation,
)
from src.application.services.ranking_daily_technical_metrics import (
    enrich_ranking_collections_with_daily_technical_metrics as _enrich_ranking_collections_with_daily_technical_metrics,
)
from src.application.services.ranking_response_items import (
    build_ranked_fundamental_items as _build_ranked_fundamental_items,
    build_value_composite_score_response,
)
from src.application.services.ranking_index_performance import (
    load_sector_score_by_name,
    load_index_performance,
)
from src.application.services.ranking_liquidity import (
    enrich_ranking_collections_with_prime_liquidity as _enrich_ranking_collections_with_prime_liquidity,
)
from src.application.services.ranking_technical_flags import (
    enrich_ranking_collections_with_technical_flags as _enrich_ranking_collections_with_technical_flags,
)
def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


_SUPPORTED_FUNDAMENTAL_RATIO_METRIC_KEY = "eps_forecast_to_actual"


def _enrich_ranking_collections_with_sector_strength(
    collections: tuple[list[ranking_contracts.RankingItem], ...],
    *,
    sector_strength_by_name: dict[str, dict[str, object]],
) -> None:
    for collection in collections:
        for item in collection:
            sector_name = _normalize_sector_filter_name(item.sector33Name)
            strength = sector_strength_by_name.get(sector_name)
            if not strength:
                continue
            score = strength.get("sectorStrengthScore")
            if isinstance(score, (int, float)):
                item.sectorStrengthScore = float(score)
            bucket = strength.get("sectorStrengthBucket")
            if bucket in {"sector_strong", "sector_neutral", "sector_weak"}:
                item.sectorStrengthBucket = cast(ranking_contracts.SectorStrengthBucket, bucket)


class RankingService:
    """マーケットランキングサービス"""

    def __init__(self, reader: MarketDbReader) -> None:
        self._reader = reader
        self._fundamental_calculator = FundamentalRankingCalculator()
        self._valuation_calculator = FundamentalsCalculator()

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
        regime_state: ranking_contracts.RankingRegimeStateFilter | None = None,
        fundamental_state: ranking_contracts.RankingFundamentalStateFilter | None = None,
        risk_state: ranking_contracts.RankingRiskStateFilter | None = None,
        technical_state: ranking_contracts.RankingTechnicalStateFilter | None = None,
        include_sector_strength: bool = False,
        sector_strength_family: ranking_contracts.SectorStrengthFamily = "balanced_sector_strength",
    ) -> ranking_contracts.MarketRankingResponse:
        """ランキングデータを取得"""
        sector_strength_family = ranking_contracts.normalize_sector_strength_family(sector_strength_family)
        requested_market_codes, query_market_codes = resolve_market_codes(markets)

        # 対象日を決定
        if date:
            target_date = date
        else:
            target_date = _resolve_latest_stock_data_date_query(self._reader)

        apply_forward_eps_filter = include_valuation and forward_eps_disclosed_within_days > 0
        apply_regime_or_risk_filter = include_valuation and (
            regime_state is not None or risk_state is not None
        )
        apply_fundamental_state_filter = include_valuation and fundamental_state is not None
        apply_technical_state_filter = technical_state is not None
        query_limit = (
            0
            if apply_forward_eps_filter
            or apply_regime_or_risk_filter
            or apply_fundamental_state_filter
            or apply_technical_state_filter
            else limit
        )

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
        _enrich_ranking_collections_with_daily_technical_metrics(
            self._reader,
            ranking_collections,
            target_date=target_date,
        )
        if include_valuation:
            price_basis_date = _resolve_latest_stock_data_date_query(self._reader)
            _enrich_ranking_collections_with_valuation(
                self._reader,
                self._valuation_calculator,
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
            if apply_regime_or_risk_filter:
                _enrich_ranking_collections_with_prime_liquidity(
                    self._reader,
                    ranking_collections,
                    target_date=target_date,
                )
            if apply_technical_state_filter:
                _enrich_ranking_collections_with_technical_flags(
                    self._reader,
                    ranking_collections,
                    target_date=target_date,
                    market_codes=query_market_codes,
                )
            if apply_fundamental_state_filter:
                _filter_ranking_collections_by_fundamental_state(
                    ranking_collections,
                    fundamental_state=fundamental_state,
                )
            if apply_regime_or_risk_filter:
                _filter_ranking_collections_by_regime_state(
                    ranking_collections,
                    regime_state=regime_state,
                )
                _filter_ranking_collections_by_risk_state(
                    ranking_collections,
                    risk_state=risk_state,
                )
            if apply_technical_state_filter:
                _filter_ranking_collections_by_technical_state(
                    ranking_collections,
                    technical_state=technical_state,
                )
            _limit_and_rerank_ranking_collections(ranking_collections, limit)
            if not apply_regime_or_risk_filter:
                _enrich_ranking_collections_with_prime_liquidity(
                    self._reader,
                    ranking_collections,
                    target_date=target_date,
                )
            if not apply_technical_state_filter:
                _enrich_ranking_collections_with_technical_flags(
                    self._reader,
                    ranking_collections,
                    target_date=target_date,
                    market_codes=query_market_codes,
                )
        elif apply_technical_state_filter:
            _enrich_ranking_collections_with_technical_flags(
                self._reader,
                ranking_collections,
                target_date=target_date,
                market_codes=query_market_codes,
            )
            _filter_ranking_collections_by_technical_state(
                ranking_collections,
                technical_state=technical_state,
            )
            _limit_and_rerank_ranking_collections(ranking_collections, limit)
        else:
            _enrich_ranking_collections_with_technical_flags(
                self._reader,
                ranking_collections,
                target_date=target_date,
                market_codes=query_market_codes,
            )
        sector_strength_by_name = (
            load_sector_score_by_name(
                self._reader,
                table_exists=lambda table_name: _table_exists_query(
                    self._reader, table_name
                ),
                date=target_date,
                market_codes=query_market_codes,
                sector_strength_family=sector_strength_family,
            )
            if include_sector_strength
            else {}
        )
        if sector_strength_by_name:
            _enrich_ranking_collections_with_sector_strength(
                ranking_collections,
                sector_strength_by_name=sector_strength_by_name,
            )
        index_performance = load_index_performance(
            self._reader,
            table_exists=lambda table_name: _table_exists_query(self._reader, table_name),
            date=target_date,
            lookback_days=lookback_days,
            market_codes=query_market_codes,
            include_sector_strength=include_sector_strength,
            sector_strength_by_name=sector_strength_by_name,
            sector_strength_family=sector_strength_family,
        )

        return ranking_contracts.MarketRankingResponse(
            date=target_date,
            markets=requested_market_codes,
            lookbackDays=lookback_days,
            periodDays=period_days,
            sectorStrengthFamily=sector_strength_family,
            rankings=ranking_contracts.Rankings(
                tradingValue=trading_value,
                gainers=gainers,
                losers=losers,
                periodHigh=period_high,
                periodLow=period_low,
            ),
            indexPerformance=index_performance,
            lastUpdated=_now_iso(),
        )

    def get_symbol_ranking_snapshot(self, code: str) -> ranking_contracts.MarketRankingSymbolResponse:
        """単一銘柄の最新 Daily Ranking スナップショットを取得。"""
        normalized_code = _normalize_equity_code(code.strip().upper())
        try:
            target_date = _resolve_latest_stock_data_date_query(self._reader)
        except ValueError as error:
            if str(error) != "No trading data available in database":
                raise
            return ranking_contracts.MarketRankingSymbolResponse(
                date=None,
                item=None,
                lastUpdated=_now_iso(),
            )

        stock = self._reader.query_one(
            f"""
            WITH {stocks_canonical_cte()}
            SELECT code, market_code
            FROM stocks_canonical
            WHERE normalized_code = ?
            LIMIT 1
            """,
            (target_date, normalized_code),
        )
        if stock is None:
            return ranking_contracts.MarketRankingSymbolResponse(
                date=target_date,
                item=None,
                lastUpdated=_now_iso(),
            )

        response = self.get_rankings(
            date=target_date,
            markets=_canonical_market_label(str(stock["market_code"])),
            limit=0,
            lookback_days=1,
            period_days=250,
            include_valuation=True,
            include_sector_strength=True,
            sector_strength_family="balanced_sector_strength",
        )
        item = next(
            (
                row
                for row in response.rankings.tradingValue
                if _normalize_equity_code(row.code) == normalized_code
            ),
            None,
        )
        return ranking_contracts.MarketRankingSymbolResponse(
            date=response.date,
            item=item,
            lastUpdated=response.lastUpdated,
        )

    def get_fundamental_rankings(
        self,
        limit: int = 20,
        markets: str = "prime",
        metric_key: str = _SUPPORTED_FUNDAMENTAL_RATIO_METRIC_KEY,
        forecast_above_recent_fy_actuals: bool = False,
        forecast_lookback_fy_count: int = 3,
    ) -> ranking_contracts.MarketFundamentalRankingResponse:
        """最新の予想EPS / 最新の実績EPS 比率ランキングを取得"""
        if metric_key != _SUPPORTED_FUNDAMENTAL_RATIO_METRIC_KEY:
            raise ValueError(f"Unsupported metricKey: {metric_key}")
        if forecast_lookback_fy_count < 1:
            raise ValueError("forecast_lookback_fy_count must be >= 1")

        requested_market_codes, query_market_codes = resolve_market_codes(markets)
        target_date = _resolve_latest_stock_data_date_query(self._reader)

        adjusted_valuation = _load_adjusted_daily_valuation_frame_query(
            self._reader,
            target_date,
            query_market_codes,
        )
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

        return ranking_contracts.MarketFundamentalRankingResponse(
            date=target_date,
            markets=requested_market_codes,
            metricKey=metric_key,
            rankings=ranking_contracts.FundamentalRankings(
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
        score_method: ranking_contracts.ValueCompositeScoreMethod | None = None,
        profile_id: ranking_contracts.ValueCompositeProfileId | None = None,
        forward_eps_mode: ranking_contracts.ValueCompositeForwardEpsMode = "latest",
        apply_liquidity_filter: bool = True,
    ) -> ranking_contracts.ValueCompositeRankingResponse:
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
        scored = _apply_value_composite_profile_if_requested(
            scored,
            self._reader,
            target_date=target_date,
            profile=profile,
            apply_liquidity_filter=apply_liquidity_filter,
        )
        items = _build_value_composite_ranking_items(
            scored.head(limit),
            self._reader,
            target_date=target_date,
        )

        return ranking_contracts.ValueCompositeRankingResponse(
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

    def get_value_composite_score(
        self,
        code: str,
        date: str | None = None,
        forward_eps_mode: ranking_contracts.ValueCompositeForwardEpsMode = "latest",
    ) -> ranking_contracts.ValueCompositeScoreResponse:
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

        unsupported_reason = _resolve_value_composite_unavailable_reason(
            self._reader,
            self._fundamental_calculator,
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
