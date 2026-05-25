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
    build_market_filter as _build_market_filter,
    canonical_market_label as _canonical_market_label,
    equity_code_variants as _equity_code_variants,
    normalize_equity_code as _normalize_equity_code,
    normalized_code_sql as _normalized_code_sql,
    positive_ratio as _positive_ratio,
    prefer_4digit_order_sql as _prefer_4digit_order_sql,
    stock_data_dedup_cte as _stock_data_dedup_cte,
    stocks_canonical_cte as _stocks_canonical_cte,
)
from src.application.services.ranking_daily_queries import (
    ranking_by_period_high as _ranking_by_period_high_query,
    ranking_by_period_low as _ranking_by_period_low_query,
    ranking_by_price_change as _ranking_by_price_change_query,
    ranking_by_price_change_from_days as _ranking_by_price_change_from_days_query,
    ranking_by_trading_value as _ranking_by_trading_value_query,
    ranking_by_trading_value_average as _ranking_by_trading_value_average_query,
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
    VALUE_COMPOSITE_REQUIRED_POSITIVE_COLUMNS,
    build_value_composite_score_frame,
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
    load_value_composite_profile_metrics as _load_value_composite_profile_metrics_query,
)
from src.application.services.ranking_valuation import (
    with_prime_valuation_percentiles,
)
from src.application.services.ranking_response_items import (
    build_fundamental_ranking_item,
    build_value_composite_item,
    build_value_composite_score_response,
    finite_or_none as _finite_or_none,
    int_or_none as _int_or_none,
    str_or_none as _str_or_none,
)
from src.application.services.ranking_statement_selection import (
    latest_actual_fy_disclosed_date,
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
    ValueCompositeRankingItem,
    ValueCompositeRankingResponse,
    ValueCompositeScoreResponse,
    ValueCompositeForwardEpsMode,
    ValueCompositeProfileId,
    ValueCompositeScoreUnavailableReason,
    ValueCompositeScoreMethod,
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


FUNDAMENTAL_BASE_COLUMNS = (
    "s.code, s.company_name, s.market_code, s.sector_33_name, "
    "sd.close as current_price, sd.volume"
)

_SUPPORTED_FUNDAMENTAL_RATIO_METRIC_KEY = "eps_forecast_to_actual"


class RankingService:
    """マーケットランキングサービス"""

    def __init__(self, reader: MarketDbReader) -> None:
        self._reader = reader
        self._fundamental_calculator = FundamentalRankingCalculator()
        self._valuation_calculator = FundamentalsCalculator()

    def _table_exists(self, table_name: str) -> bool:
        row = self._reader.query_one(
            """
            SELECT 1 AS exists
            FROM information_schema.tables
            WHERE lower(table_name) = lower(?)
            LIMIT 1
            """,
            (table_name,),
        )
        return row is not None

    def _load_adjustment_events_by_code(
        self,
        *,
        through_date: str,
        market_codes: list[str],
    ) -> dict[str, list[ShareAdjustmentEvent]]:
        if not self._table_exists("stock_data_raw"):
            return {}

        market_clause, market_params = _build_market_filter(market_codes)
        raw_normalized = _normalized_code_sql("raw.code")
        stocks_normalized = _normalized_code_sql("s.code")
        raw_prefer_4digit = _prefer_4digit_order_sql("raw.code")
        stocks_prefer_4digit = _prefer_4digit_order_sql("s.code")
        sql = f"""
            WITH stocks_canonical AS (
                SELECT code, normalized_code, market_code
                FROM (
                    SELECT
                        code,
                        market_code,
                        {stocks_normalized} AS normalized_code,
                        ROW_NUMBER() OVER (
                            PARTITION BY {stocks_normalized}
                            ORDER BY {stocks_prefer_4digit}
                        ) AS rn
                    FROM stocks s
                )
                WHERE rn = 1
            ),
            adjustment_canonical AS (
                SELECT
                    s.code,
                    raw.date,
                    raw.adjustment_factor,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.code, raw.date
                        ORDER BY {raw_prefer_4digit}
                    ) AS rn
                FROM stock_data_raw raw
                JOIN stocks_canonical s
                    ON s.normalized_code = {raw_normalized}
                WHERE raw.date <= ?
                  AND raw.adjustment_factor IS NOT NULL
                  AND raw.adjustment_factor != 1.0
                  {market_clause}
            )
            SELECT code, date, adjustment_factor
            FROM adjustment_canonical
            WHERE rn = 1
            ORDER BY code, date
        """
        grouped: dict[str, list[ShareAdjustmentEvent]] = {}
        for row in self._reader.query(sql, (through_date, *market_params)):
            code = _normalize_equity_code(row["code"])
            grouped.setdefault(code, []).append(
                ShareAdjustmentEvent(
                    date=str(row["date"]),
                    adjustment_factor=float(row["adjustment_factor"]),
                )
            )
        return grouped

    def _resolve_stock_price_basis_date(self) -> str:
        row = self._reader.query_one("SELECT MAX(date) as max_date FROM stock_data")
        if row is None or row["max_date"] is None:
            raise ValueError("No trading data available in database")
        return str(row["max_date"])

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
            target_date = self._resolve_stock_price_basis_date()

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
            price_basis_date = self._resolve_stock_price_basis_date()
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
        date_row = self._reader.query_one(
            "SELECT MAX(date) as max_date FROM stock_data"
        )
        if date_row is None or date_row["max_date"] is None:
            raise ValueError("No trading data available in database")
        target_date = date_row["max_date"]

        stock_rows = self._load_fundamental_stock_rows(target_date, query_market_codes)
        adjusted_valuation = self._load_adjusted_daily_valuation_frame(
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

        statement_rows = self._load_fundamental_statement_rows(
            target_date, query_market_codes
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
        target_date = self._resolve_value_composite_target_date(date)
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
        items = self._build_value_composite_ranking_items(
            scored.head(limit),
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

    def _apply_value_composite_profile(
        self,
        frame: pd.DataFrame,
        profile: _ValueCompositeProfileSpec,
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
        scored = self._append_value_composite_profile_metrics(
            scored,
            target_date=target_date,
            profile=profile,
        )
        scored = self._apply_value_composite_profile(
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
        target_date = self._resolve_value_composite_target_date(date)
        target_date = self._resolve_value_composite_symbol_target_date(
            code,
            target_date,
        )
        normalized_target_code = _normalize_equity_code(code)
        target_stock = self._load_value_composite_target_stock(
            code,
            target_date,
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
        item, universe_count = self._find_value_composite_score_item(
            scored,
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
            price_basis_date=self._resolve_stock_price_basis_date(),
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

    def _build_value_composite_ranking_items(
        self,
        scored: pd.DataFrame,
        *,
        target_date: str,
    ) -> list[ValueCompositeRankingItem]:
        scored = self._append_value_composite_technical_metrics(
            scored,
            target_date=target_date,
        )
        return [
            build_value_composite_item(cast(Mapping[str, Any], row), rank)
            for rank, row in enumerate(scored.to_dict(orient="records"), start=1)
        ]

    def _find_value_composite_score_item(
        self,
        scored: pd.DataFrame,
        *,
        normalized_target_code: str,
        target_date: str,
    ) -> tuple[ValueCompositeRankingItem | None, int]:
        rows = scored.to_dict(orient="records")
        for rank, row in enumerate(rows, start=1):
            if _normalize_equity_code(row["code"]) != normalized_target_code:
                continue
            row_payload: dict[str, Any] = {str(key): value for key, value in row.items()}
            row_df = self._append_value_composite_technical_metrics(
                pd.DataFrame.from_records([row_payload]),
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

    def _resolve_value_composite_target_date(self, date: str | None) -> str:
        if date:
            return date
        date_row = self._reader.query_one(
            "SELECT MAX(date) as max_date FROM stock_data"
        )
        if date_row is None or date_row["max_date"] is None:
            raise ValueError("No trading data available in database")
        return str(date_row["max_date"])

    def _resolve_value_composite_symbol_target_date(
        self,
        code: str,
        target_date: str,
    ) -> str:
        code_variants = _equity_code_variants(code)
        placeholders = ",".join("?" for _ in code_variants)
        row = self._reader.query_one(
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

    def _load_value_composite_target_stock(
        self,
        code: str,
        target_date: str,
    ) -> Mapping[str, Any] | None:
        _, query_market_codes = resolve_market_codes(
            "prime,standard,growth",
            fallback=["prime", "standard", "growth"],
        )
        normalized_target_code = _normalize_equity_code(code)
        for row in self._load_fundamental_stock_rows(target_date, query_market_codes):
            if _normalize_equity_code(row["code"]) == normalized_target_code:
                return row
        return None

    def _load_value_composite_scored_frame(
        self,
        *,
        target_date: str,
        query_market_codes: list[str],
        weights: Mapping[str, float],
        forward_eps_mode: ValueCompositeForwardEpsMode,
    ) -> pd.DataFrame:
        if forward_eps_mode == "latest":
            adjusted = self._load_adjusted_daily_valuation_frame(
                target_date,
                query_market_codes,
            )
            if not adjusted.empty:
                scored = self._build_value_composite_score_frame_from_adjusted(
                    adjusted,
                    weights=weights,
                )
                if not scored.empty:
                    return scored

        stock_rows = self._load_fundamental_stock_rows(target_date, query_market_codes)
        statement_rows = self._load_fundamental_statement_rows(
            target_date, query_market_codes
        )
        price_basis_date = self._resolve_stock_price_basis_date()
        adjustment_events_by_code = self._load_adjustment_events_by_code(
            through_date=price_basis_date,
            market_codes=query_market_codes,
        )

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
            price = _to_nullable_float(stock["current_price"])
            volume = _to_nullable_float(stock["volume"])
            if price is None or price <= 0:
                continue

            valuation_rows = (
                raw_statements
                if forward_eps_mode == "latest"
                else [
                    row
                    for row in raw_statements
                    if _normalize_period_label(row["type_of_current_period"]) == "FY"
                ]
            )
            valuation_statements = [
                market_statement_row_to_jquants_statement(row, code_fallback=code)
                for row in valuation_rows
            ]
            valuation = self._valuation_calculator.calculate_latest_valuation(
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
            forward_eps = valuation.forwardEps
            market_cap_bil_jpy = (
                valuation.marketCap / 1_000_000_000.0
                if valuation.marketCap is not None
                else None
            )
            bps = (
                price / pbr
                if pbr is not None and pbr > 0
                else None
            )

            records.append(
                {
                    "code": code,
                    "company_name": str(stock["company_name"]),
                    "market_code": str(stock["market_code"]),
                    "market": _canonical_market_label(str(stock["market_code"])),
                    "sector_33_name": str(stock["sector_33_name"]),
                    "current_price": price,
                    "volume": volume if volume is not None else 0.0,
                    "pbr": pbr,
                    "forward_per": forward_per,
                    "market_cap_bil_jpy": market_cap_bil_jpy,
                    "bps": bps,
                    "forward_eps": forward_eps,
                    "latest_fy_disclosed_date": latest_actual_fy_disclosed_date(
                        raw_statements,
                        as_of_date=target_date,
                    ),
                    "forward_eps_disclosed_date": valuation.forwardEpsDisclosedDate,
                    "forward_eps_source": valuation.forwardEpsSource,
                }
            )

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

    def _build_value_composite_score_frame_from_adjusted(
        self,
        adjusted: pd.DataFrame,
        *,
        weights: Mapping[str, float],
    ) -> pd.DataFrame:
        records: list[dict[str, Any]] = []
        for row in adjusted.to_dict(orient="records"):
            price = _finite_or_none(row.get("current_price"))
            volume = _finite_or_none(row.get("volume"))
            pbr = _finite_or_none(row.get("pbr"))
            forward_per = _finite_or_none(row.get("forward_per"))
            market_cap = _finite_or_none(row.get("market_cap"))
            if price is None or price <= 0:
                continue
            records.append(
                {
                    "code": str(row["code"]),
                    "company_name": str(row["company_name"]),
                    "market_code": str(row["market_code"]),
                    "market": _canonical_market_label(str(row["market_code"])),
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
                    "bps": _finite_or_none(row.get("bps")),
                    "forward_eps": _finite_or_none(row.get("forward_eps")),
                    "latest_fy_disclosed_date": _str_or_none(
                        row.get("statement_disclosed_date")
                    ),
                    "forward_eps_disclosed_date": _str_or_none(
                        row.get("forward_eps_disclosed_date")
                    ),
                    "forward_eps_source": _str_or_none(row.get("forward_eps_source")),
                }
            )
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

        statement_rows = self._load_fundamental_statement_rows(
            target_date, query_market_codes
        )
        raw_statements_by_code: dict[str, list[Mapping[str, Any]]] = {}
        for row in statement_rows:
            code = _normalize_equity_code(row["code"])
            if code in items_by_code and code not in enriched_codes:
                raw_statements_by_code.setdefault(code, []).append(row)

        adjustment_events_by_code = self._load_adjustment_events_by_code(
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
        valuation_frame = self._load_adjusted_daily_valuation_frame(
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
        adjustment_events_by_code = self._load_adjustment_events_by_code(
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

    def _append_value_composite_technical_metrics(
        self,
        frame: pd.DataFrame,
        *,
        target_date: str,
    ) -> pd.DataFrame:
        if frame.empty:
            return frame.copy()
        result = frame.copy()
        technical_metrics = self._load_value_composite_technical_metrics(
            target_date=target_date,
            codes=[str(code) for code in result["code"].tolist()],
        )
        technical_columns = (
            "technical_feature_date",
            "breakout_feature_date",
            "rebound_from_252d_low_pct",
            "return_252d_pct",
            "volatility_20d_pct",
            "volatility_60d_pct",
            "downside_volatility_60d_pct",
            "avg_trading_value_60d_mil_jpy",
            "avg_trading_value_60d_source_sessions",
            "new_high_20d",
            "days_since_new_high_20d",
            "close_to_prior_high_20d_pct",
            "new_high_120d",
            "days_since_new_high_120d",
            "close_to_prior_high_120d_pct",
        )
        normalized_codes = result["code"].map(_normalize_equity_code)
        for column in technical_columns:
            result[column] = normalized_codes.map(
                lambda code, column=column: technical_metrics.get(str(code), {}).get(column)
            )
        return result

    def _append_value_composite_profile_metrics(
        self,
        frame: pd.DataFrame,
        *,
        target_date: str,
        profile: _ValueCompositeProfileSpec,
    ) -> pd.DataFrame:
        if frame.empty:
            return frame.copy()
        result = frame.copy()
        profile_metrics = _load_value_composite_profile_metrics_query(
            self._reader,
            target_date=target_date,
            codes=[str(code) for code in result["code"].tolist()],
            profile=profile,
        )
        technical_columns = [
            "avg_trading_value_60d_mil_jpy",
            "avg_trading_value_60d_source_sessions",
        ]
        if profile.breakout_window is not None:
            technical_columns.extend(
                [
                    f"new_high_{profile.breakout_window}d",
                    f"days_since_new_high_{profile.breakout_window}d",
                    f"close_to_prior_high_{profile.breakout_window}d_pct",
                ]
            )
        normalized_codes = result["code"].map(_normalize_equity_code)
        for column in technical_columns:
            result[column] = normalized_codes.map(
                lambda code, column=column: profile_metrics.get(str(code), {}).get(column)
            )
        return result

    def _load_value_composite_technical_metrics(
        self,
        *,
        target_date: str,
        codes: list[str],
    ) -> dict[str, dict[str, Any]]:
        normalized_codes = sorted({_normalize_equity_code(code) for code in codes})
        if not normalized_codes:
            return {}
        placeholders = ",".join("?" for _ in normalized_codes)
        normalized = _normalized_code_sql("code")
        order = _prefer_4digit_order_sql("code")
        sql = f"""
            WITH stock_history AS (
                SELECT
                    normalized_code,
                    date,
                    close
                FROM (
                    SELECT
                        {normalized} AS normalized_code,
                        date,
                        close,
                        ROW_NUMBER() OVER (
                            PARTITION BY {normalized}, date
                            ORDER BY {order}
                        ) AS rn
                    FROM stock_data
                    WHERE date <= ?
                      AND {normalized} IN ({placeholders})
                )
                WHERE rn = 1
            ),
            returns AS (
                SELECT
                    normalized_code,
                    date,
                    close,
                    close / NULLIF(LAG(close, 252) OVER (
                        PARTITION BY normalized_code ORDER BY date
                    ), 0) - 1 AS return_252d,
                    close / NULLIF(MIN(close) OVER (
                        PARTITION BY normalized_code
                        ORDER BY date
                        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                    ), 0) - 1 AS rebound_from_252d_low,
                    close / NULLIF(LAG(close) OVER (
                        PARTITION BY normalized_code ORDER BY date
                    ), 0) - 1 AS daily_return
                FROM stock_history
            ),
            metrics AS (
                SELECT
                    normalized_code,
                    date,
                    rebound_from_252d_low * 100.0 AS rebound_from_252d_low_pct,
                    return_252d * 100.0 AS return_252d_pct,
                    STDDEV_SAMP(daily_return) OVER (
                        PARTITION BY normalized_code
                        ORDER BY date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) * SQRT(252.0) * 100.0 AS volatility_20d_pct,
                    COUNT(daily_return) OVER (
                        PARTITION BY normalized_code
                        ORDER BY date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS volatility_20d_count,
                    STDDEV_SAMP(daily_return) OVER (
                        PARTITION BY normalized_code
                        ORDER BY date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) * SQRT(252.0) * 100.0 AS volatility_60d_pct,
                    COUNT(daily_return) OVER (
                        PARTITION BY normalized_code
                        ORDER BY date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) AS volatility_60d_count,
                    STDDEV_SAMP(CASE WHEN daily_return < 0 THEN daily_return ELSE NULL END) OVER (
                        PARTITION BY normalized_code
                        ORDER BY date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) * SQRT(252.0) * 100.0 AS downside_volatility_60d_pct,
                    COUNT(CASE WHEN daily_return < 0 THEN daily_return ELSE NULL END) OVER (
                        PARTITION BY normalized_code
                        ORDER BY date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) AS downside_volatility_60d_count,
                    ROW_NUMBER() OVER (
                        PARTITION BY normalized_code ORDER BY date DESC
                    ) AS latest_rank
                FROM returns
            ),
            latest_metrics AS (
                SELECT
                    normalized_code,
                    date AS technical_feature_date,
                    rebound_from_252d_low_pct,
                    return_252d_pct,
                    CASE WHEN volatility_20d_count >= 20 THEN volatility_20d_pct ELSE NULL END AS volatility_20d_pct,
                    CASE WHEN volatility_60d_count >= 60 THEN volatility_60d_pct ELSE NULL END AS volatility_60d_pct,
                    CASE
                        WHEN downside_volatility_60d_count >= 2 THEN downside_volatility_60d_pct
                        ELSE NULL
                    END AS downside_volatility_60d_pct
                FROM metrics
                WHERE latest_rank = 1
            ),
            signal_history AS (
                SELECT
                    normalized_code,
                    date,
                    high,
                    close,
                    volume,
                    close * volume AS trading_value,
                    ROW_NUMBER() OVER (
                        PARTITION BY normalized_code ORDER BY date
                    ) AS signal_row_number
                FROM (
                    SELECT
                        {normalized} AS normalized_code,
                        date,
                        high,
                        close,
                        volume,
                        ROW_NUMBER() OVER (
                            PARTITION BY {normalized}, date
                            ORDER BY {order}
                        ) AS rn
                    FROM stock_data
                    WHERE date < ?
                      AND {normalized} IN ({placeholders})
                )
                WHERE rn = 1
            ),
            signal_metrics AS (
                SELECT
                    normalized_code,
                    date,
                    high,
                    close,
                    signal_row_number,
                    MAX(high) OVER (
                        PARTITION BY normalized_code
                        ORDER BY date
                        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                    ) AS prior_high_20d,
                    MAX(high) OVER (
                        PARTITION BY normalized_code
                        ORDER BY date
                        ROWS BETWEEN 120 PRECEDING AND 1 PRECEDING
                    ) AS prior_high_120d,
                    AVG(trading_value) OVER (
                        PARTITION BY normalized_code
                        ORDER BY date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) AS avg_trading_value_60d,
                    COUNT(trading_value) OVER (
                        PARTITION BY normalized_code
                        ORDER BY date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) AS avg_trading_value_60d_source_sessions,
                    ROW_NUMBER() OVER (
                        PARTITION BY normalized_code ORDER BY date DESC
                    ) AS latest_signal_rank
                FROM signal_history
            ),
            signal_flags AS (
                SELECT
                    *,
                    prior_high_20d IS NOT NULL AND high > prior_high_20d AS new_high_20d,
                    prior_high_120d IS NOT NULL AND high > prior_high_120d AS new_high_120d
                FROM signal_metrics
            ),
            latest_signal AS (
                SELECT *
                FROM signal_flags
                WHERE latest_signal_rank = 1
            ),
            latest_breakout AS (
                SELECT
                    normalized_code,
                    MAX(CASE WHEN new_high_20d THEN signal_row_number ELSE NULL END)
                        AS latest_new_high_20d_row_number,
                    MAX(CASE WHEN new_high_120d THEN signal_row_number ELSE NULL END)
                        AS latest_new_high_120d_row_number
                FROM signal_flags
                GROUP BY normalized_code
            )
            SELECT
                latest_metrics.*,
                latest_signal.date AS breakout_feature_date,
                CASE
                    WHEN latest_signal.avg_trading_value_60d_source_sessions >= 60
                    THEN latest_signal.avg_trading_value_60d / 1000000.0
                    ELSE NULL
                END AS avg_trading_value_60d_mil_jpy,
                latest_signal.avg_trading_value_60d_source_sessions,
                latest_signal.new_high_20d,
                latest_signal.signal_row_number - latest_breakout.latest_new_high_20d_row_number
                    AS days_since_new_high_20d,
                CASE
                    WHEN latest_signal.prior_high_20d IS NULL OR latest_signal.prior_high_20d = 0
                    THEN NULL
                    ELSE (latest_signal.close / latest_signal.prior_high_20d - 1.0) * 100.0
                END AS close_to_prior_high_20d_pct,
                latest_signal.new_high_120d,
                latest_signal.signal_row_number - latest_breakout.latest_new_high_120d_row_number
                    AS days_since_new_high_120d,
                CASE
                    WHEN latest_signal.prior_high_120d IS NULL OR latest_signal.prior_high_120d = 0
                    THEN NULL
                    ELSE (latest_signal.close / latest_signal.prior_high_120d - 1.0) * 100.0
                END AS close_to_prior_high_120d_pct
            FROM latest_metrics
            LEFT JOIN latest_signal USING (normalized_code)
            LEFT JOIN latest_breakout USING (normalized_code)
        """
        rows = self._reader.query(
            sql,
            (target_date, *normalized_codes, target_date, *normalized_codes),
        )
        return {
            str(row["normalized_code"]): {
                "technical_feature_date": _str_or_none(row["technical_feature_date"]),
                "breakout_feature_date": _str_or_none(row["breakout_feature_date"]),
                "rebound_from_252d_low_pct": _finite_or_none(
                    row["rebound_from_252d_low_pct"]
                ),
                "return_252d_pct": _finite_or_none(row["return_252d_pct"]),
                "volatility_20d_pct": _finite_or_none(row["volatility_20d_pct"]),
                "volatility_60d_pct": _finite_or_none(row["volatility_60d_pct"]),
                "downside_volatility_60d_pct": _finite_or_none(
                    row["downside_volatility_60d_pct"]
                ),
                "avg_trading_value_60d_mil_jpy": _finite_or_none(
                    row["avg_trading_value_60d_mil_jpy"]
                ),
                "avg_trading_value_60d_source_sessions": _int_or_none(
                    row["avg_trading_value_60d_source_sessions"]
                ),
                "new_high_20d": (
                    bool(row["new_high_20d"]) if row["new_high_20d"] is not None else None
                ),
                "days_since_new_high_20d": _int_or_none(
                    row["days_since_new_high_20d"]
                ),
                "close_to_prior_high_20d_pct": _finite_or_none(
                    row["close_to_prior_high_20d_pct"]
                ),
                "new_high_120d": (
                    bool(row["new_high_120d"])
                    if row["new_high_120d"] is not None
                    else None
                ),
                "days_since_new_high_120d": _int_or_none(
                    row["days_since_new_high_120d"]
                ),
                "close_to_prior_high_120d_pct": _finite_or_none(
                    row["close_to_prior_high_120d_pct"]
                ),
            }
            for row in rows
        }

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
        statement_rows = self._load_fundamental_statement_rows(
            target_date, query_market_codes
        )
        raw_statements = [
            row
            for row in statement_rows
            if _normalize_equity_code(row["code"]) == target_code
        ]
        statements = statement_rows_from_mappings(raw_statements)
        if not statements:
            return "not_rankable"

        adjustment_events_by_code = self._load_adjustment_events_by_code(
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

    def _load_fundamental_stock_rows(
        self,
        date: str,
        market_codes: list[str],
    ) -> list[Mapping[str, Any]]:
        market_clause, market_params = _build_market_filter(market_codes)
        stocks_cte = _stocks_canonical_cte()
        stock_daily_cte = _stock_data_dedup_cte("stock_daily", where_clause="date = ?")
        sql = f"""
            WITH
            {stocks_cte},
            {stock_daily_cte}
            SELECT {FUNDAMENTAL_BASE_COLUMNS}
            FROM stocks_canonical s
            JOIN stock_daily sd
                ON sd.normalized_code = s.normalized_code
            WHERE 1 = 1{market_clause}
        """
        return self._reader.query(sql, (date, *market_params))

    def _load_adjusted_daily_valuation_frame(
        self,
        date: str,
        market_codes: list[str],
    ) -> pd.DataFrame:
        if not self._table_exists("daily_valuation"):
            return pd.DataFrame()
        market_clause, market_params = _build_market_filter(market_codes)
        stocks_cte = _stocks_canonical_cte()
        stock_daily_cte = _stock_data_dedup_cte("stock_daily", where_clause="date = ?")
        valuation_norm = _normalized_code_sql("code")
        valuation_order = _prefer_4digit_order_sql("code")
        sql = f"""
            WITH
            {stocks_cte},
            {stock_daily_cte},
            valuation_canonical AS (
                SELECT
                    normalized_code,
                    date,
                    price_basis_date,
                    close,
                    eps,
                    bps,
                    forward_eps,
                    per,
                    forward_per,
                    p_op,
                    forward_p_op,
                    pbr,
                    market_cap,
                    free_float_market_cap,
                    statement_disclosed_date,
                    forward_eps_disclosed_date,
                    forward_eps_source,
                    basis_version
                FROM (
                    SELECT
                        {valuation_norm} AS normalized_code,
                        date,
                        price_basis_date,
                        close,
                        eps,
                        bps,
                        forward_eps,
                        per,
                        forward_per,
                        p_op,
                        forward_p_op,
                        pbr,
                        market_cap,
                        free_float_market_cap,
                        statement_disclosed_date,
                        forward_eps_disclosed_date,
                        forward_eps_source,
                        basis_version,
                        ROW_NUMBER() OVER (
                            PARTITION BY {valuation_norm}, date
                            ORDER BY
                                price_basis_date DESC NULLS LAST,
                                basis_version DESC,
                                {valuation_order}
                        ) AS rn
                    FROM daily_valuation
                    WHERE date = ?
                )
                WHERE rn = 1
            )
            SELECT
                s.code,
                s.company_name,
                s.market_code,
                s.sector_33_name,
                COALESCE(v.close, sd.close) AS current_price,
                sd.volume,
                v.eps,
                v.bps,
                v.forward_eps,
                v.per,
                v.forward_per,
                v.p_op,
                v.forward_p_op,
                v.pbr,
                v.market_cap,
                v.free_float_market_cap,
                v.statement_disclosed_date,
                v.forward_eps_disclosed_date,
                v.forward_eps_source,
                v.price_basis_date,
                v.basis_version
            FROM valuation_canonical v
            JOIN stocks_canonical s
                ON s.normalized_code = v.normalized_code
            JOIN stock_daily sd
                ON sd.normalized_code = v.normalized_code
            WHERE 1 = 1{market_clause}
        """
        rows = self._reader.query(sql, (date, date, *market_params))
        return pd.DataFrame([dict(row.items()) for row in rows])

    def _load_adjusted_statement_metric_rows(
        self,
        date: str,
        market_codes: list[str],
    ) -> list[Mapping[str, Any]]:
        if not self._table_exists("statement_metrics_adjusted"):
            return []
        market_clause, market_params = _build_market_filter(market_codes)
        stocks_cte = _stocks_canonical_cte()
        stock_daily_cte = _stock_data_dedup_cte("stock_daily", where_clause="date = ?")
        metrics_norm = _normalized_code_sql("code")
        metrics_order = _prefer_4digit_order_sql("code")
        sql = f"""
            WITH
            {stocks_cte},
            {stock_daily_cte},
            metrics_canonical AS (
                SELECT
                    normalized_code,
                    disclosed_date,
                    period_type,
                    adjusted_eps,
                    adjusted_bps,
                    adjusted_forecast_eps,
                    basis_version
                FROM (
                    SELECT
                        {metrics_norm} AS normalized_code,
                        disclosed_date,
                        period_type,
                        adjusted_eps,
                        adjusted_bps,
                        adjusted_forecast_eps,
                        basis_version,
                        ROW_NUMBER() OVER (
                            PARTITION BY {metrics_norm}, disclosed_date
                            ORDER BY
                                price_basis_date DESC NULLS LAST,
                                basis_version DESC,
                                {metrics_order}
                        ) AS rn
                    FROM statement_metrics_adjusted
                    WHERE disclosed_date <= ?
                )
                WHERE rn = 1
            )
            SELECT
                s.code,
                m.disclosed_date,
                m.period_type,
                m.adjusted_eps,
                m.adjusted_bps,
                m.adjusted_forecast_eps,
                m.basis_version
            FROM metrics_canonical m
            JOIN stocks_canonical s
                ON s.normalized_code = m.normalized_code
            JOIN stock_daily sd
                ON sd.normalized_code = m.normalized_code
            WHERE 1 = 1{market_clause}
            ORDER BY s.code, m.disclosed_date DESC
        """
        return self._reader.query(sql, (date, date, *market_params))

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
        rows = self._load_adjusted_statement_metric_rows(target_date, market_codes)
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

    def _load_fundamental_statement_rows(
        self,
        date: str,
        market_codes: list[str],
    ) -> list[Mapping[str, Any]]:
        market_clause, market_params = _build_market_filter(market_codes)
        stocks_cte = _stocks_canonical_cte()
        stock_daily_cte = _stock_data_dedup_cte("stock_daily", where_clause="date = ?")
        statements_norm = _normalized_code_sql("code")
        statements_order = _prefer_4digit_order_sql("code")
        statement_columns = self._statement_table_columns()
        forecast_operating_profit_expr = self._optional_statement_double_expr(
            "forecast_operating_profit",
            statement_columns,
        )
        next_year_forecast_operating_profit_expr = self._optional_statement_double_expr(
            "next_year_forecast_operating_profit",
            statement_columns,
        )
        sql = f"""
            WITH
            {stocks_cte},
            {stock_daily_cte},
            statements_canonical AS (
                SELECT
                    normalized_code,
                    disclosed_date,
                    type_of_current_period,
                    type_of_document,
                    earnings_per_share,
                    bps,
                    forecast_eps,
                    next_year_forecast_earnings_per_share,
                    operating_profit,
                    forecast_operating_profit,
                    next_year_forecast_operating_profit,
                    shares_outstanding,
                    treasury_shares
                FROM (
                    SELECT
                        {statements_norm} AS normalized_code,
                        disclosed_date,
                        type_of_current_period,
                        type_of_document,
                        earnings_per_share,
                        bps,
                        forecast_eps,
                        next_year_forecast_earnings_per_share,
                        operating_profit,
                        {forecast_operating_profit_expr},
                        {next_year_forecast_operating_profit_expr},
                        shares_outstanding,
                        treasury_shares,
                        ROW_NUMBER() OVER (
                            PARTITION BY {statements_norm}, disclosed_date
                            ORDER BY {statements_order}
                        ) AS rn
                    FROM statements
                )
                WHERE rn = 1
            )
            SELECT
                s.code,
                st.disclosed_date,
                st.type_of_current_period,
                st.type_of_document,
                st.earnings_per_share,
                st.bps,
                st.forecast_eps,
                st.next_year_forecast_earnings_per_share,
                st.operating_profit,
                st.forecast_operating_profit,
                st.next_year_forecast_operating_profit,
                st.shares_outstanding,
                st.treasury_shares
            FROM statements_canonical st
            JOIN stocks_canonical s
                ON s.normalized_code = st.normalized_code
            JOIN stock_daily sd
                ON sd.normalized_code = st.normalized_code
            WHERE st.disclosed_date <= ?{market_clause}
            ORDER BY s.code, st.disclosed_date DESC
        """
        return self._reader.query(sql, (date, date, *market_params))

    def _statement_table_columns(self) -> set[str]:
        try:
            rows = self._reader.query("SELECT name FROM pragma_table_info('statements')")
        except Exception:  # noqa: BLE001 - main statement query will surface the real failure
            return set()
        return {str(row["name"]) for row in rows}

    @staticmethod
    def _optional_statement_double_expr(column: str, columns: set[str]) -> str:
        if column in columns:
            return column
        return f"CAST(NULL AS DOUBLE) AS {column}"

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
