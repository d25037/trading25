"""
Ranking Service

DuckDB market data からランキングデータを取得するサービス。
Hono MarketRankingService 互換。
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
from collections.abc import Mapping
from typing import Any, Literal, cast

import pandas as pd

from src.infrastructure.db.market.market_reader import MarketDbReadable, MarketDbReader
from src.shared.utils.market_code_alias import (
    normalize_market_scope,
    resolve_market_codes,
)
from src.domains.analytics.fundamental_ranking import (
    FundamentalItem,
    FundamentalRankingCalculator,
    ForecastValue as _ForecastValue,
    LatestFyRow as _LatestFyRow,
    StatementRow as _StatementRow,
    adjust_per_share_value as _adjust_per_share_value,
    is_valid_share_count as _is_valid_share_count,
    normalize_period_label as _normalize_period_label,
    resolve_fy_cycle_key as _resolve_fy_cycle_key,
    to_nullable_float as _to_nullable_float,
)
from src.domains.analytics.annual_value_composite_selection import (
    EQUAL_VALUE_COMPOSITE_WEIGHTS,
    PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS,
    STANDARD_PBR_TILT_VALUE_COMPOSITE_WEIGHTS,
    VALUE_COMPOSITE_SCORE_COLUMN,
    VALUE_COMPOSITE_REQUIRED_POSITIVE_COLUMNS,
    build_value_composite_score_frame,
)
from src.domains.analytics.topix100_streak_353_transfer import (
    DEFAULT_LONG_WINDOW_STREAKS,
    DEFAULT_SHORT_WINDOW_STREAKS,
)
from src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm import (
    DEFAULT_RUNTIME_TRAIN_LOOKBACK_DAYS,
    DEFAULT_RUNTIME_CATEGORICAL_FEATURE_COLUMNS,
    score_topix100_streak_353_next_session_intraday_lightgbm_snapshot,
)
from src.domains.analytics.topix100_streak_353_next_session_open_to_open_5d_lightgbm import (
    score_topix100_streak_353_next_session_open_to_open_5d_lightgbm_snapshot,
)
from src.entrypoints.http.schemas.ranking import (
    IndexPerformanceItem,
    FundamentalRankingItem,
    FundamentalRankings,
    MarketFundamentalRankingResponse,
    MarketRankingResponse,
    RankingItem,
    Rankings,
    Topix100RankingItem,
    Topix100RankingMetric,
    Topix100PriceSmaWindow,
    Topix100RankingResponse,
    Topix100StudyMode,
    ValueCompositeRankingItem,
    ValueCompositeRankingResponse,
    ValueCompositeScoreResponse,
    ValueCompositeForwardEpsMode,
    ValueCompositeScoreUnavailableReason,
    ValueCompositeScoreMethod,
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


Topix100PriceBucket = Literal["q1", "q10", "q234", "other"]
_TOPIX100_RANKING_METRIC_SQL: dict[Topix100RankingMetric, str] = {
    "price_vs_sma_gap": "price_vs_sma_gap",
    "price_sma_20_80": "price_sma_20_80",
}
_TOPIX100_PRICE_SMA_WINDOWS: frozenset[int] = frozenset({20, 50, 100})
_TOPIX100_VOLUME_SHORT_WINDOW = 5
_TOPIX100_VOLUME_LONG_WINDOW = 20
_TOPIX100_SHORT_WINDOW_STREAKS = DEFAULT_SHORT_WINDOW_STREAKS
_TOPIX100_LONG_WINDOW_STREAKS = DEFAULT_LONG_WINDOW_STREAKS


def _build_market_filter(market_codes: list[str]) -> tuple[str, list[str]]:
    """マーケットコードのWHERE句を構築"""
    if not market_codes:
        return "", []
    placeholders = ",".join("?" for _ in market_codes)
    return f" AND s.market_code IN ({placeholders})", market_codes


def _normalized_code_sql(column_ref: str) -> str:
    """4桁/5桁コード混在を吸収する正規化SQL式。"""
    return (
        "CASE "
        f"WHEN length({column_ref}) = 5 AND right({column_ref}, 1) = '0' "
        f"THEN left({column_ref}, 4) "
        f"ELSE {column_ref} "
        "END"
    )


def _prefer_4digit_order_sql(column_ref: str) -> str:
    return f"CASE WHEN length({column_ref}) = 4 THEN 0 ELSE 1 END"


def _canonical_market_label(market_code: str) -> str:
    return str(normalize_market_scope(market_code, default=market_code))


def _positive_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None:
        return None
    if denominator <= 0:
        return None
    ratio = numerator / denominator
    return ratio if ratio > 0 else None


def _normalize_equity_code(code: object) -> str:
    text = str(code).strip()
    if len(text) == 5 and text.endswith("0"):
        return text[:4]
    return text


def _finite_or_none(value: Any) -> float | None:
    number = _to_nullable_float(value)
    if number is None or not math.isfinite(number):
        return None
    return number


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return str(value)


def _normalize_value_composite_weights(
    weights: Mapping[str, float],
) -> dict[str, float]:
    weight_sum = sum(float(value) for value in weights.values())
    if not math.isfinite(weight_sum) or weight_sum <= 0:
        raise ValueError("value composite weights must sum to a positive finite value")
    return {column: float(value) / weight_sum for column, value in weights.items()}


def _stocks_canonical_cte() -> str:
    normalized = _normalized_code_sql("code")
    order = _prefer_4digit_order_sql("code")
    return f"""
        stocks_canonical AS (
            SELECT
                code,
                company_name,
                market_code,
                sector_33_name,
                normalized_code
            FROM (
                SELECT
                    code,
                    company_name,
                    market_code,
                    sector_33_name,
                    {normalized} AS normalized_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized}
                        ORDER BY {order}
                    ) AS rn
                FROM stocks
            )
            WHERE rn = 1
        )
    """


def _stock_data_dedup_cte(
    cte_name: str,
    *,
    where_clause: str,
    code_ref: str = "code",
    include_ohlc: bool = True,
) -> str:
    normalized = _normalized_code_sql(code_ref)
    order = _prefer_4digit_order_sql(code_ref)
    select_ohlc = (
        ", open, high, low, close, volume" if include_ohlc else ", close, volume"
    )
    return f"""
        {cte_name} AS (
            SELECT
                normalized_code,
                date
                {select_ohlc}
            FROM (
                SELECT
                    {normalized} AS normalized_code,
                    date
                    {select_ohlc},
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized}, date
                        ORDER BY {order}
                    ) AS rn
                FROM stock_data
                WHERE {where_clause}
            )
            WHERE rn = 1
        )
    """


RANKING_BASE_COLUMNS = "s.code, s.company_name, s.market_code, s.sector_33_name"
FUNDAMENTAL_BASE_COLUMNS = (
    "s.code, s.company_name, s.market_code, s.sector_33_name, "
    "sd.close as current_price, sd.volume"
)
_TOPIX100_SCALE_CATEGORIES = ("TOPIX Core30", "TOPIX Large70")


def _resolve_latest_stock_master_date(reader: MarketDbReadable) -> str:
    if _reader_table_exists(reader, "stock_master_daily"):
        row = reader.query_one("SELECT MAX(date) AS max_date FROM stock_master_daily")
        if row is not None and row["max_date"] is not None:
            return str(row["max_date"])

    row = reader.query_one("SELECT MAX(date) AS max_date FROM stock_data")
    if row is None or row["max_date"] is None:
        raise ValueError("No stock master or stock_data date available in database")
    return str(row["max_date"])


def _reader_table_exists(reader: MarketDbReadable, table_name: str) -> bool:
    row = reader.query_one(
        """
        SELECT 1 AS exists
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        LIMIT 1
        """,
        (table_name,),
    )
    return row is not None


def _stock_master_source(
    reader: MarketDbReadable, as_of_date: str
) -> tuple[str, str, tuple[str, ...]]:
    if _reader_table_exists(reader, "stock_master_daily"):
        row = reader.query_one(
            "SELECT 1 AS exists FROM stock_master_daily WHERE date = ? LIMIT 1",
            (as_of_date,),
        )
        if row is not None:
            return "stock_master_daily", "date = ? AND ", (as_of_date,)
    return "stocks", "", ()


_QUARTER_PERIODS = {"1Q", "2Q", "3Q"}
_SUPPORTED_FUNDAMENTAL_RATIO_METRIC_KEY = "eps_forecast_to_actual"
_VALUE_COMPOSITE_METRIC_KEY = "standard_value_composite"
_VALUE_COMPOSITE_SCORE_POLICY_SUFFIX = (
    "requires PBR > 0 and forward PER > 0; no ADV60 floor"
)
_VALUE_COMPOSITE_WEIGHTS_BY_METHOD: dict[
    ValueCompositeScoreMethod, dict[str, float]
] = {
    "standard_pbr_tilt": STANDARD_PBR_TILT_VALUE_COMPOSITE_WEIGHTS,
    "prime_size_tilt": PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS,
    "equal_weight": EQUAL_VALUE_COMPOSITE_WEIGHTS,
}
_VALUE_COMPOSITE_AUTO_SCORE_METHOD_BY_MARKET: dict[str, ValueCompositeScoreMethod] = {
    "prime": "prime_size_tilt",
    "standard": "standard_pbr_tilt",
}
_VALUE_COMPOSITE_SCORE_POLICY_BY_METHOD: dict[ValueCompositeScoreMethod, str] = {
    "standard_pbr_tilt": (
        "Standard PBR tilt research weights: 35% small market cap + 40% low PBR + "
        f"25% low forward PER; {_VALUE_COMPOSITE_SCORE_POLICY_SUFFIX}"
    ),
    "prime_size_tilt": (
        "Prime size tilt research weights: 45% small market cap + 20% low PBR + "
        f"35% low forward PER; {_VALUE_COMPOSITE_SCORE_POLICY_SUFFIX}"
    ),
    "equal_weight": (
        "Equal weight across small market cap, low PBR, and low forward PER; "
        f"{_VALUE_COMPOSITE_SCORE_POLICY_SUFFIX}"
    ),
}
_VALUE_COMPOSITE_FORWARD_EPS_MODE_LABELS: dict[ValueCompositeForwardEpsMode, str] = {
    "latest": "latest revised forecast EPS when available, otherwise FY forecast EPS",
    "fy": "latest FY forecast EPS only",
}


def _row_to_item(row: Mapping[str, Any], rank: int, **extra: Any) -> RankingItem:
    """DB行をRankingItemに変換"""
    return RankingItem(
        rank=rank,
        code=row["code"],
        companyName=row["company_name"],
        marketCode=row["market_code"],
        sector33Name=row["sector_33_name"],
        currentPrice=row["current_price"],
        volume=row["volume"],
        **{k: v for k, v in extra.items() if v is not None},
    )


def _build_optional_desc_ranks(
    items: list[Topix100RankingItem],
    field_name: str,
) -> dict[str, int]:
    scoped = [
        (item.code, getattr(item, field_name))
        for item in items
        if getattr(item, field_name) is not None
    ]
    scoped.sort(key=lambda pair: (cast(float, pair[1]), pair[0]), reverse=True)
    return {code: rank for rank, (code, _value) in enumerate(scoped, start=1)}


class RankingService:
    """マーケットランキングサービス"""

    def __init__(self, reader: MarketDbReader) -> None:
        self._reader = reader
        self._fundamental_calculator = FundamentalRankingCalculator()

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

    def get_rankings(
        self,
        date: str | None = None,
        limit: int = 20,
        markets: str = "prime",
        lookback_days: int = 1,
        period_days: int = 250,
    ) -> MarketRankingResponse:
        """ランキングデータを取得"""
        requested_market_codes, query_market_codes = resolve_market_codes(markets)

        # 対象日を決定
        if date:
            target_date = date
        else:
            row = self._reader.query_one("SELECT MAX(date) as max_date FROM stock_data")
            if row is None or row["max_date"] is None:
                raise ValueError("No trading data available in database")
            target_date = row["max_date"]

        # 5種類のランキングを取得
        if lookback_days > 1:
            trading_value = self._ranking_by_trading_value_average(
                target_date, lookback_days, limit, query_market_codes
            )
        else:
            trading_value = self._ranking_by_trading_value(
                target_date, limit, query_market_codes
            )

        if lookback_days > 1:
            gainers = self._ranking_by_price_change_from_days(
                target_date, lookback_days, limit, query_market_codes, "DESC"
            )
            losers = self._ranking_by_price_change_from_days(
                target_date, lookback_days, limit, query_market_codes, "ASC"
            )
        else:
            gainers = self._ranking_by_price_change(
                target_date, limit, query_market_codes, "DESC"
            )
            losers = self._ranking_by_price_change(
                target_date, limit, query_market_codes, "ASC"
            )

        period_high = self._ranking_by_period_high(
            target_date, period_days, limit, query_market_codes
        )
        period_low = self._ranking_by_period_low(
            target_date, period_days, limit, query_market_codes
        )
        index_performance = self._load_index_performance(
            target_date, lookback_days=lookback_days
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

    def get_topix100_ranking(
        self,
        date: str | None = None,
        metric: Topix100RankingMetric = "price_vs_sma_gap",
        sma_window: int = 50,
        study_mode: Topix100StudyMode = "intraday",
    ) -> Topix100RankingResponse:
        """TOPIX100 の snapshot ランキングを返す。"""
        if metric not in _TOPIX100_RANKING_METRIC_SQL:
            raise ValueError(f"Unsupported TOPIX100 ranking metric: {metric}")
        if sma_window not in _TOPIX100_PRICE_SMA_WINDOWS:
            raise ValueError(f"Unsupported TOPIX100 SMA window: {sma_window}")
        validated_sma_window = cast(Topix100PriceSmaWindow, sma_window)

        target_date = self._resolve_topix100_ranking_date(date)
        rows = self._load_topix100_ranking_rows(
            target_date,
            metric,
            validated_sma_window,
        )
        if not rows:
            raise ValueError(
                f"No TOPIX100 ranking data available for date: {target_date}"
            )
        if study_mode == "swing_5d":
            return self._get_topix100_swing_5d_ranking(
                target_date=target_date,
                metric=metric,
                sma_window=validated_sma_window,
                rows=rows,
            )
        return self._get_topix100_intraday_ranking(
            target_date=target_date,
            metric=metric,
            sma_window=validated_sma_window,
            rows=rows,
        )

    def _get_topix100_intraday_ranking(
        self,
        *,
        target_date: str,
        metric: Topix100RankingMetric,
        sma_window: Topix100PriceSmaWindow,
        rows: list[Mapping[str, Any]],
    ) -> Topix100RankingResponse:
        realized_rows_by_code = self._load_topix100_next_session_intraday_returns(
            target_date
        )
        score_snapshot = (
            score_topix100_streak_353_next_session_intraday_lightgbm_snapshot(
                self._reader.db_path,
                target_date=target_date,
                short_window_streaks=_TOPIX100_SHORT_WINDOW_STREAKS,
                long_window_streaks=_TOPIX100_LONG_WINDOW_STREAKS,
                categorical_feature_columns=DEFAULT_RUNTIME_CATEGORICAL_FEATURE_COLUMNS,
                train_lookback_days=DEFAULT_RUNTIME_TRAIN_LOOKBACK_DAYS,
                connection=self._reader.conn,
            )
        )

        items = []
        for row in rows:
            code = str(row["code"])
            state_snapshot = score_snapshot.rows_by_code.get(code)
            realized_row = realized_rows_by_code.get(code)
            items.append(
                self._build_topix100_ranking_item(
                    row=row,
                    intraday_score=(
                        state_snapshot.intraday_score
                        if state_snapshot is not None
                        else None
                    ),
                    next_session_date=(
                        str(realized_row["next_session_date"])
                        if realized_row is not None
                        and realized_row["next_session_date"] is not None
                        else None
                    ),
                    next_session_intraday_return=(
                        float(realized_row["next_session_intraday_return"])
                        if realized_row is not None
                        and realized_row["next_session_intraday_return"] is not None
                        else None
                    ),
                )
            )

        intraday_long_ranks = _build_optional_desc_ranks(items, "intradayScore")
        intraday_short_ranks = _build_optional_desc_ranks(
            [
                item.model_copy(
                    update={
                        "intradayScore": (
                            -item.intradayScore
                            if item.intradayScore is not None
                            else None
                        ),
                    }
                )
                for item in items
            ],
            "intradayScore",
        )
        items = [
            item.model_copy(
                update={
                    "intradayLongRank": intraday_long_ranks.get(item.code),
                    "intradayShortRank": intraday_short_ranks.get(item.code),
                }
            )
            for item in items
        ]

        return Topix100RankingResponse(
            date=target_date,
            studyMode="intraday",
            rankingMetric=metric,
            smaWindow=sma_window,
            shortWindowStreaks=_TOPIX100_SHORT_WINDOW_STREAKS,
            longWindowStreaks=_TOPIX100_LONG_WINDOW_STREAKS,
            scoreTarget="next_session_open_close",
            intradayScoreTarget="next_session_open_close",
            scoreModelType=score_snapshot.score_model_type,
            scoreTrainWindowDays=score_snapshot.train_window_days,
            scoreTestWindowDays=score_snapshot.test_window_days,
            scoreStepDays=score_snapshot.step_days,
            scoreSplitTrainStart=score_snapshot.split_train_start,
            scoreSplitTrainEnd=score_snapshot.split_train_end,
            scoreSplitTestStart=score_snapshot.split_test_start,
            scoreSplitTestEnd=score_snapshot.split_test_end,
            scoreSplitPartialTail=score_snapshot.split_is_partial_tail,
            scoreSourceRunId=score_snapshot.score_source_run_id,
            itemCount=len(items),
            items=items,
            lastUpdated=_now_iso(),
        )

    def _get_topix100_swing_5d_ranking(
        self,
        *,
        target_date: str,
        metric: Topix100RankingMetric,
        sma_window: Topix100PriceSmaWindow,
        rows: list[Mapping[str, Any]],
    ) -> Topix100RankingResponse:
        realized_rows_by_code = (
            self._load_topix100_next_session_open_to_open_5d_returns(target_date)
        )
        score_snapshot = (
            score_topix100_streak_353_next_session_open_to_open_5d_lightgbm_snapshot(
                self._reader.db_path,
                target_date=target_date,
                short_window_streaks=_TOPIX100_SHORT_WINDOW_STREAKS,
                long_window_streaks=_TOPIX100_LONG_WINDOW_STREAKS,
                train_lookback_days=DEFAULT_RUNTIME_TRAIN_LOOKBACK_DAYS,
                connection=self._reader.conn,
            )
        )

        items = []
        for row in rows:
            code = str(row["code"])
            state_snapshot = score_snapshot.rows_by_code.get(code)
            realized_row = realized_rows_by_code.get(code)
            items.append(
                self._build_topix100_ranking_item(
                    row=row,
                    long_score_5d=(
                        state_snapshot.long_score_5d
                        if state_snapshot is not None
                        else None
                    ),
                    swing_entry_date=(
                        str(realized_row["entry_date"])
                        if realized_row is not None
                        and realized_row["entry_date"] is not None
                        else None
                    ),
                    swing_exit_date=(
                        str(realized_row["exit_date"])
                        if realized_row is not None
                        and realized_row["exit_date"] is not None
                        else None
                    ),
                    open_to_open_5d_return=(
                        float(realized_row["open_to_open_5d_return"])
                        if realized_row is not None
                        and realized_row["open_to_open_5d_return"] is not None
                        else None
                    ),
                )
            )

        long_score_ranks = _build_optional_desc_ranks(items, "longScore5d")
        items = [
            item.model_copy(
                update={
                    "longScore5dRank": long_score_ranks.get(item.code),
                }
            )
            for item in items
        ]

        benchmark_row = self._load_topix_open_to_open_5d_benchmark_return(target_date)
        realized_returns = [
            cast(float, item.openToOpen5dReturn)
            for item in items
            if item.openToOpen5dReturn is not None
        ]
        secondary_benchmark_return = (
            sum(realized_returns) / len(realized_returns) if realized_returns else None
        )

        return Topix100RankingResponse(
            date=target_date,
            studyMode="swing_5d",
            rankingMetric=metric,
            smaWindow=sma_window,
            shortWindowStreaks=_TOPIX100_SHORT_WINDOW_STREAKS,
            longWindowStreaks=_TOPIX100_LONG_WINDOW_STREAKS,
            longScoreHorizonDays=score_snapshot.long_target_horizon_days,
            shortScoreHorizonDays=score_snapshot.short_target_horizon_days,
            scoreTarget="next_session_open_to_open_5d",
            intradayScoreTarget="next_session_open_to_open_5d",
            scoreModelType="daily_refit",
            scoreTrainWindowDays=DEFAULT_RUNTIME_TRAIN_LOOKBACK_DAYS,
            scoreTestWindowDays=1,
            scoreStepDays=1,
            scoreSourceRunId=score_snapshot.score_source_run_id,
            primaryBenchmark="topix",
            secondaryBenchmark="topix100_universe",
            primaryBenchmarkReturn=(
                float(benchmark_row["benchmark_return"])
                if benchmark_row is not None
                and benchmark_row["benchmark_return"] is not None
                else None
            ),
            secondaryBenchmarkReturn=secondary_benchmark_return,
            benchmarkEntryDate=(
                str(benchmark_row["entry_date"])
                if benchmark_row is not None and benchmark_row["entry_date"] is not None
                else None
            ),
            benchmarkExitDate=(
                str(benchmark_row["exit_date"])
                if benchmark_row is not None and benchmark_row["exit_date"] is not None
                else None
            ),
            itemCount=len(items),
            items=items,
            lastUpdated=_now_iso(),
        )

    def _build_topix100_ranking_item(
        self,
        *,
        row: Mapping[str, Any],
        intraday_score: float | None = None,
        long_score_5d: float | None = None,
        next_session_date: str | None = None,
        next_session_intraday_return: float | None = None,
        swing_entry_date: str | None = None,
        swing_exit_date: str | None = None,
        open_to_open_5d_return: float | None = None,
    ) -> Topix100RankingItem:
        return Topix100RankingItem(
            rank=int(row["rank"]),
            code=str(row["code"]),
            companyName=str(row["company_name"]),
            marketCode=str(row["market_code"]),
            sector33Name=str(row["sector_33_name"]),
            scaleCategory=str(row["scale_category"] or ""),
            currentPrice=float(row["current_price"]),
            volume=float(row["volume"]),
            priceVsSmaGap=float(row["price_vs_sma_gap"]),
            priceSma20_80=float(row["price_sma_20_80"]),
            volumeSma5_20=float(row["volume_sma_5_20"]),
            priceDecile=int(row["price_decile"]),
            priceBucket=cast(Topix100PriceBucket, row["price_bucket"]),
            intradayScore=intraday_score,
            longScore5d=long_score_5d,
            nextSessionDate=next_session_date,
            nextSessionIntradayReturn=next_session_intraday_return,
            swingEntryDate=swing_entry_date,
            swingExitDate=swing_exit_date,
            openToOpen5dReturn=open_to_open_5d_return,
        )

    def get_fundamental_rankings(
        self,
        limit: int = 20,
        markets: str = "prime",
        metric_key: str = _SUPPORTED_FUNDAMENTAL_RATIO_METRIC_KEY,
        forecast_above_recent_fy_actuals: bool = False,
        forecast_lookback_fy_count: int = 3,
        # Backward compatibility for legacy caller
        forecast_above_all_actuals: bool | None = None,
    ) -> MarketFundamentalRankingResponse:
        """最新の予想EPS / 最新の実績EPS 比率ランキングを取得"""
        if metric_key != _SUPPORTED_FUNDAMENTAL_RATIO_METRIC_KEY:
            raise ValueError(f"Unsupported metricKey: {metric_key}")
        if forecast_lookback_fy_count < 1:
            raise ValueError("forecast_lookback_fy_count must be >= 1")

        if (
            forecast_above_all_actuals is not None
            and not forecast_above_recent_fy_actuals
        ):
            forecast_above_recent_fy_actuals = forecast_above_all_actuals

        requested_market_codes, query_market_codes = resolve_market_codes(markets)
        date_row = self._reader.query_one(
            "SELECT MAX(date) as max_date FROM stock_data"
        )
        if date_row is None or date_row["max_date"] is None:
            raise ValueError("No trading data available in database")
        target_date = date_row["max_date"]

        stock_rows = self._load_fundamental_stock_rows(target_date, query_market_codes)
        statement_rows = self._load_fundamental_statement_rows(
            target_date, query_market_codes
        )

        statements_by_code: dict[str, list[_StatementRow]] = {}
        for row in statement_rows:
            code = str(row["code"])
            period_type = _normalize_period_label(row["type_of_current_period"])
            statements_by_code.setdefault(code, []).append(
                _StatementRow(
                    code=code,
                    disclosed_date=str(row["disclosed_date"]),
                    period_type=period_type,
                    earnings_per_share=_to_nullable_float(row["earnings_per_share"]),
                    forecast_eps=_to_nullable_float(row["forecast_eps"]),
                    next_year_forecast_earnings_per_share=_to_nullable_float(
                        row["next_year_forecast_earnings_per_share"]
                    ),
                    shares_outstanding=_to_nullable_float(row["shares_outstanding"]),
                    fy_cycle_key=_resolve_fy_cycle_key(str(row["disclosed_date"])),
                )
            )

        ratio_candidates: list[FundamentalItem] = []
        for stock in stock_rows:
            code = str(stock["code"])
            statements = statements_by_code.get(code)
            if not statements:
                continue

            baseline_shares = self._resolve_baseline_shares(
                statements,
                as_of_date=target_date,
            )
            actual_snapshot = self._resolve_latest_actual_snapshot(
                statements,
                baseline_shares,
                as_of_date=target_date,
            )
            forecast_snapshot = self._resolve_latest_forecast_snapshot(
                statements,
                baseline_shares,
                as_of_date=target_date,
            )

            if forecast_above_recent_fy_actuals:
                if forecast_snapshot is None:
                    continue
                recent_max_actual_eps = self._resolve_recent_actual_eps_max(
                    statements,
                    baseline_shares,
                    forecast_lookback_fy_count,
                    as_of_date=target_date,
                )
                if (
                    recent_max_actual_eps is None
                    or forecast_snapshot.value <= recent_max_actual_eps
                ):
                    continue

            ratio_snapshot = self._resolve_latest_ratio_snapshot(
                actual_snapshot, forecast_snapshot
            )

            if ratio_snapshot is None:
                continue
            ratio_candidates.append(self._build_fundamental_item(stock, ratio_snapshot))

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
        score_method: ValueCompositeScoreMethod = "standard_pbr_tilt",
        forward_eps_mode: ValueCompositeForwardEpsMode = "latest",
    ) -> ValueCompositeRankingResponse:
        """Standard市場向けの小型バリュー複合スコアランキングを取得"""

        if score_method not in _VALUE_COMPOSITE_WEIGHTS_BY_METHOD:
            raise ValueError(f"Unsupported scoreMethod: {score_method}")
        if forward_eps_mode not in _VALUE_COMPOSITE_FORWARD_EPS_MODE_LABELS:
            raise ValueError(f"Unsupported forwardEpsMode: {forward_eps_mode}")
        weights = _normalize_value_composite_weights(
            _VALUE_COMPOSITE_WEIGHTS_BY_METHOD[score_method]
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
        ).head(limit)

        items = [
            self._build_value_composite_item(cast(Mapping[str, Any], row), rank)
            for rank, row in enumerate(scored.to_dict(orient="records"), start=1)
        ]

        return ValueCompositeRankingResponse(
            date=target_date,
            markets=requested_market_codes,
            metricKey=_VALUE_COMPOSITE_METRIC_KEY,
            scoreMethod=score_method,
            forwardEpsMode=forward_eps_mode,
            scorePolicy=(
                f"{_VALUE_COMPOSITE_SCORE_POLICY_BY_METHOD[score_method]}; "
                f"forward EPS basis: {_VALUE_COMPOSITE_FORWARD_EPS_MODE_LABELS[forward_eps_mode]}"
            ),
            weights={
                "smallMarketCap": weights["small_market_cap_score"],
                "lowPbr": weights["low_pbr_score"],
                "lowForwardPer": weights["low_forward_per_score"],
            },
            itemCount=len(items),
            items=items,
            lastUpdated=_now_iso(),
        )

    def get_value_composite_score(
        self,
        code: str,
        date: str | None = None,
        forward_eps_mode: ValueCompositeForwardEpsMode = "latest",
    ) -> ValueCompositeScoreResponse:
        """単一銘柄の market-specific value composite score を取得"""

        if forward_eps_mode not in _VALUE_COMPOSITE_FORWARD_EPS_MODE_LABELS:
            raise ValueError(f"Unsupported forwardEpsMode: {forward_eps_mode}")
        target_date = self._resolve_value_composite_target_date(date)
        _, all_query_market_codes = resolve_market_codes(
            "prime,standard,growth",
            fallback=["prime", "standard", "growth"],
        )
        stock_rows = self._load_fundamental_stock_rows(
            target_date, all_query_market_codes
        )
        normalized_target_code = _normalize_equity_code(code)
        target_stock = next(
            (
                row
                for row in stock_rows
                if _normalize_equity_code(row["code"]) == normalized_target_code
            ),
            None,
        )
        last_updated = _now_iso()
        if target_stock is None:
            return ValueCompositeScoreResponse(
                date=target_date,
                code=code,
                forwardEpsMode=forward_eps_mode,
                scoreAvailable=False,
                unsupportedReason="not_found",
                lastUpdated=last_updated,
            )

        market = _canonical_market_label(str(target_stock["market_code"]))
        score_method = _VALUE_COMPOSITE_AUTO_SCORE_METHOD_BY_MARKET.get(market)
        if score_method is None:
            return ValueCompositeScoreResponse(
                date=target_date,
                code=str(target_stock["code"]),
                companyName=str(target_stock["company_name"]),
                marketCode=str(target_stock["market_code"]),
                market=market,
                forwardEpsMode=forward_eps_mode,
                scoreAvailable=False,
                unsupportedReason="unsupported_market",
                lastUpdated=last_updated,
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
        rows = scored.to_dict(orient="records")
        universe_count = len(rows)
        for rank, row in enumerate(rows, start=1):
            if _normalize_equity_code(row["code"]) != normalized_target_code:
                continue
            item = self._build_value_composite_item(cast(Mapping[str, Any], row), rank)
            return ValueCompositeScoreResponse(
                date=target_date,
                code=str(target_stock["code"]),
                companyName=str(target_stock["company_name"]),
                marketCode=str(target_stock["market_code"]),
                market=market,
                metricKey=_VALUE_COMPOSITE_METRIC_KEY,
                scoreMethod=score_method,
                forwardEpsMode=forward_eps_mode,
                scorePolicy=(
                    f"{_VALUE_COMPOSITE_SCORE_POLICY_BY_METHOD[score_method]}; "
                    f"forward EPS basis: {_VALUE_COMPOSITE_FORWARD_EPS_MODE_LABELS[forward_eps_mode]}"
                ),
                weights={
                    "smallMarketCap": weights["small_market_cap_score"],
                    "lowPbr": weights["low_pbr_score"],
                    "lowForwardPer": weights["low_forward_per_score"],
                },
                universeCount=universe_count,
                scoreAvailable=True,
                item=item,
                lastUpdated=last_updated,
            )

        unsupported_reason = self._resolve_value_composite_unavailable_reason(
            target_stock=target_stock,
            target_date=target_date,
            query_market_codes=query_market_codes,
            forward_eps_mode=forward_eps_mode,
        )
        return ValueCompositeScoreResponse(
            date=target_date,
            code=str(target_stock["code"]),
            companyName=str(target_stock["company_name"]),
            marketCode=str(target_stock["market_code"]),
            market=market,
            metricKey=_VALUE_COMPOSITE_METRIC_KEY,
            scoreMethod=score_method,
            forwardEpsMode=forward_eps_mode,
            scorePolicy=(
                f"{_VALUE_COMPOSITE_SCORE_POLICY_BY_METHOD[score_method]}; "
                f"forward EPS basis: {_VALUE_COMPOSITE_FORWARD_EPS_MODE_LABELS[forward_eps_mode]}"
            ),
            weights={
                "smallMarketCap": weights["small_market_cap_score"],
                "lowPbr": weights["low_pbr_score"],
                "lowForwardPer": weights["low_forward_per_score"],
            },
            universeCount=universe_count,
            scoreAvailable=False,
            unsupportedReason=unsupported_reason,
            lastUpdated=last_updated,
        )

    # --- Private ranking methods ---

    def _resolve_value_composite_target_date(self, date: str | None) -> str:
        if date:
            return date
        date_row = self._reader.query_one(
            "SELECT MAX(date) as max_date FROM stock_data"
        )
        if date_row is None or date_row["max_date"] is None:
            raise ValueError("No trading data available in database")
        return str(date_row["max_date"])

    def _load_value_composite_scored_frame(
        self,
        *,
        target_date: str,
        query_market_codes: list[str],
        weights: Mapping[str, float],
        forward_eps_mode: ValueCompositeForwardEpsMode,
    ) -> pd.DataFrame:
        stock_rows = self._load_fundamental_stock_rows(target_date, query_market_codes)
        statement_rows = self._load_fundamental_statement_rows(
            target_date, query_market_codes
        )

        statements_by_code: dict[str, list[_StatementRow]] = {}
        raw_statements_by_code: dict[str, list[Mapping[str, Any]]] = {}
        for row in statement_rows:
            code = str(row["code"])
            period_type = _normalize_period_label(row["type_of_current_period"])
            statements_by_code.setdefault(code, []).append(
                _StatementRow(
                    code=code,
                    disclosed_date=str(row["disclosed_date"]),
                    period_type=period_type,
                    earnings_per_share=_to_nullable_float(row["earnings_per_share"]),
                    forecast_eps=_to_nullable_float(row["forecast_eps"]),
                    next_year_forecast_earnings_per_share=_to_nullable_float(
                        row["next_year_forecast_earnings_per_share"]
                    ),
                    shares_outstanding=_to_nullable_float(row["shares_outstanding"]),
                    fy_cycle_key=_resolve_fy_cycle_key(str(row["disclosed_date"])),
                )
            )
            raw_statements_by_code.setdefault(code, []).append(row)

        records: list[dict[str, Any]] = []
        for stock in stock_rows:
            code = str(stock["code"])
            statements = statements_by_code.get(code)
            raw_statements = raw_statements_by_code.get(code, [])
            if not statements:
                continue
            price = _to_nullable_float(stock["current_price"])
            volume = _to_nullable_float(stock["volume"])
            if price is None or price <= 0:
                continue

            baseline_shares = self._resolve_baseline_shares(
                statements,
                as_of_date=target_date,
            )
            forecast_snapshot = self._resolve_value_composite_forecast_snapshot(
                statements,
                baseline_shares,
                forward_eps_mode=forward_eps_mode,
                as_of_date=target_date,
            )
            latest_fy = self._latest_value_bps_statement(
                raw_statements,
                baseline_shares,
                as_of_date=target_date,
            )
            if latest_fy is None:
                continue

            bps = _adjust_per_share_value(
                _to_nullable_float(latest_fy["bps"]),
                _to_nullable_float(latest_fy["shares_outstanding"]),
                baseline_shares,
            )
            forward_eps = (
                forecast_snapshot.value if forecast_snapshot is not None else None
            )
            market_cap_bil_jpy = (
                price * baseline_shares / 1_000_000_000.0
                if baseline_shares is not None
                and _is_valid_share_count(baseline_shares)
                else None
            )
            pbr = _positive_ratio(price, bps)
            forward_per = _positive_ratio(price, forward_eps)

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
                    "latest_fy_disclosed_date": str(latest_fy["disclosed_date"]),
                    "forward_eps_disclosed_date": (
                        forecast_snapshot.disclosed_date
                        if forecast_snapshot is not None
                        else None
                    ),
                    "forward_eps_source": forecast_snapshot.source
                    if forecast_snapshot is not None
                    else None,
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

    def _resolve_value_composite_unavailable_reason(
        self,
        *,
        target_stock: Mapping[str, Any],
        target_date: str,
        query_market_codes: list[str],
        forward_eps_mode: ValueCompositeForwardEpsMode,
    ) -> ValueCompositeScoreUnavailableReason:
        price = _to_nullable_float(target_stock["current_price"])
        if price is None or price <= 0:
            return "not_rankable"

        target_code = _normalize_equity_code(target_stock["code"])
        statement_rows = self._load_fundamental_statement_rows(
            target_date, query_market_codes
        )
        statements: list[_StatementRow] = []
        raw_statements: list[Mapping[str, Any]] = []
        for row in statement_rows:
            if _normalize_equity_code(row["code"]) != target_code:
                continue
            raw_statements.append(row)
            statements.append(
                _StatementRow(
                    code=str(row["code"]),
                    disclosed_date=str(row["disclosed_date"]),
                    period_type=_normalize_period_label(row["type_of_current_period"]),
                    earnings_per_share=_to_nullable_float(row["earnings_per_share"]),
                    forecast_eps=_to_nullable_float(row["forecast_eps"]),
                    next_year_forecast_earnings_per_share=_to_nullable_float(
                        row["next_year_forecast_earnings_per_share"]
                    ),
                    shares_outstanding=_to_nullable_float(row["shares_outstanding"]),
                    fy_cycle_key=_resolve_fy_cycle_key(str(row["disclosed_date"])),
                )
            )
        if not statements:
            return "not_rankable"

        baseline_shares = self._resolve_baseline_shares(
            statements,
            as_of_date=target_date,
        )
        forecast_snapshot = self._resolve_value_composite_forecast_snapshot(
            statements,
            baseline_shares,
            forward_eps_mode=forward_eps_mode,
            as_of_date=target_date,
        )
        if forecast_snapshot is None or forecast_snapshot.value <= 0:
            return "forward_eps_missing"

        latest_fy = self._latest_value_bps_statement(
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

    def _resolve_topix100_ranking_date(self, date: str | None) -> str:
        if date:
            return date

        master_table, master_date_clause, master_params = _stock_master_source(
            self._reader, _resolve_latest_stock_master_date(self._reader)
        )
        row = self._reader.query_one(
            f"""
            WITH topix100_stocks AS (
                SELECT normalized_code
                FROM (
                    SELECT
                        {_normalized_code_sql("code")} AS normalized_code,
                        ROW_NUMBER() OVER (
                            PARTITION BY {_normalized_code_sql("code")}
                            ORDER BY {_prefer_4digit_order_sql("code")}
                        ) AS rn
                    FROM {master_table}
                    WHERE {master_date_clause}coalesce(scale_category, '') IN (?, ?)
                )
                WHERE rn = 1
            ),
            stock_data_dedup AS (
                SELECT normalized_code, date
                FROM (
                    SELECT
                        {_normalized_code_sql("code")} AS normalized_code,
                        date,
                        ROW_NUMBER() OVER (
                            PARTITION BY {_normalized_code_sql("code")}, date
                            ORDER BY {_prefer_4digit_order_sql("code")}
                        ) AS rn
                    FROM stock_data
                )
                WHERE rn = 1
            )
            SELECT MAX(sd.date) AS max_date
            FROM stock_data_dedup sd
            JOIN topix100_stocks s ON s.normalized_code = sd.normalized_code
            """,
            (*master_params, *_TOPIX100_SCALE_CATEGORIES),
        )
        if row is None or row["max_date"] is None:
            raise ValueError("No TOPIX100 trading data available in database")
        return str(row["max_date"])

    def _load_topix100_next_session_intraday_returns(
        self,
        target_date: str,
    ) -> dict[str, Mapping[str, Any]]:
        master_table, master_date_clause, master_params = _stock_master_source(
            self._reader, target_date
        )
        rows = self._reader.query(
            f"""
            WITH topix100_stocks AS (
                SELECT
                    code,
                    normalized_code
                FROM (
                    SELECT
                        code,
                        {_normalized_code_sql("code")} AS normalized_code,
                        ROW_NUMBER() OVER (
                            PARTITION BY {_normalized_code_sql("code")}
                            ORDER BY {_prefer_4digit_order_sql("code")}
                        ) AS rn
                    FROM {master_table}
                    WHERE {master_date_clause}coalesce(scale_category, '') IN (?, ?)
                )
                WHERE rn = 1
            ),
            future_stock_data AS (
                SELECT normalized_code, date, open, close
                FROM (
                    SELECT
                        {_normalized_code_sql("code")} AS normalized_code,
                        date,
                        open,
                        close,
                        ROW_NUMBER() OVER (
                            PARTITION BY {_normalized_code_sql("code")}, date
                            ORDER BY {_prefer_4digit_order_sql("code")}
                        ) AS rn
                    FROM stock_data
                    WHERE date > ?
                )
                WHERE rn = 1
            ),
            next_sessions AS (
                SELECT
                    normalized_code,
                    date AS next_session_date,
                    close / NULLIF(open, 0) - 1 AS next_session_intraday_return,
                    ROW_NUMBER() OVER (
                        PARTITION BY normalized_code
                        ORDER BY date ASC
                    ) AS session_rank
                FROM future_stock_data
            )
            SELECT
                s.code,
                ns.next_session_date,
                ns.next_session_intraday_return
            FROM topix100_stocks s
            LEFT JOIN next_sessions ns
                ON s.normalized_code = ns.normalized_code
               AND ns.session_rank = 1
            """,
            (*master_params, *_TOPIX100_SCALE_CATEGORIES, target_date),
        )
        return {str(row["code"]): row for row in rows}

    def _load_topix100_next_session_open_to_open_5d_returns(
        self,
        target_date: str,
    ) -> dict[str, Mapping[str, Any]]:
        master_table, master_date_clause, master_params = _stock_master_source(
            self._reader, target_date
        )
        rows = self._reader.query(
            f"""
            WITH topix100_stocks AS (
                SELECT
                    code,
                    normalized_code
                FROM (
                    SELECT
                        code,
                        {_normalized_code_sql("code")} AS normalized_code,
                        ROW_NUMBER() OVER (
                            PARTITION BY {_normalized_code_sql("code")}
                            ORDER BY {_prefer_4digit_order_sql("code")}
                        ) AS rn
                    FROM {master_table}
                    WHERE {master_date_clause}coalesce(scale_category, '') IN (?, ?)
                )
                WHERE rn = 1
            ),
            future_stock_data AS (
                SELECT normalized_code, date, open, close
                FROM (
                    SELECT
                        {_normalized_code_sql("code")} AS normalized_code,
                        date,
                        open,
                        close,
                        ROW_NUMBER() OVER (
                            PARTITION BY {_normalized_code_sql("code")}, date
                            ORDER BY {_prefer_4digit_order_sql("code")}
                        ) AS rn
                    FROM stock_data
                    WHERE date > ?
                )
                WHERE rn = 1
            ),
            ranked_future_sessions AS (
                SELECT
                    normalized_code,
                    date,
                    open,
                    close,
                    ROW_NUMBER() OVER (
                        PARTITION BY normalized_code
                        ORDER BY date ASC
                    ) AS session_rank
                FROM future_stock_data
            ),
            entry_sessions AS (
                SELECT normalized_code, date AS entry_date, open AS entry_open
                FROM ranked_future_sessions
                WHERE session_rank = 1
            ),
            exit_sessions AS (
                SELECT normalized_code, date AS exit_date, open AS exit_open
                FROM ranked_future_sessions
                WHERE session_rank = 6
            )
            SELECT
                s.code,
                e.entry_date,
                x.exit_date,
                x.exit_open / NULLIF(e.entry_open, 0) - 1 AS open_to_open_5d_return
            FROM topix100_stocks s
            LEFT JOIN entry_sessions e
                ON s.normalized_code = e.normalized_code
            LEFT JOIN exit_sessions x
                ON s.normalized_code = x.normalized_code
            """,
            (*master_params, *_TOPIX100_SCALE_CATEGORIES, target_date),
        )
        return {str(row["code"]): row for row in rows}

    def _load_topix_open_to_open_5d_benchmark_return(
        self,
        target_date: str,
    ) -> Mapping[str, Any] | None:
        if self._table_exists("topix_data"):
            return self._reader.query_one(
                """
                WITH ranked_future_sessions AS (
                    SELECT
                        date,
                        open,
                        ROW_NUMBER() OVER (ORDER BY date ASC) AS session_rank
                    FROM topix_data
                    WHERE date > ?
                ),
                entry_session AS (
                    SELECT date AS entry_date, open AS entry_open
                    FROM ranked_future_sessions
                    WHERE session_rank = 1
                ),
                exit_session AS (
                    SELECT date AS exit_date, open AS exit_open
                    FROM ranked_future_sessions
                    WHERE session_rank = 6
                )
                SELECT
                    e.entry_date,
                    x.exit_date,
                    x.exit_open / NULLIF(e.entry_open, 0) - 1 AS benchmark_return
                FROM entry_session e
                CROSS JOIN exit_session x
                """,
                (target_date,),
            )

        if not self._table_exists("index_master") or not self._table_exists(
            "indices_data"
        ):
            return None

        return self._reader.query_one(
            """
            WITH topix_code AS (
                SELECT code
                FROM index_master
                WHERE lower(coalesce(category, '')) = 'topix'
                ORDER BY CASE WHEN upper(code) = 'TOPIX' THEN 0 ELSE 1 END, code ASC
                LIMIT 1
            ),
            ranked_future_sessions AS (
                SELECT
                    id.date,
                    id.open,
                    ROW_NUMBER() OVER (ORDER BY id.date ASC) AS session_rank
                FROM indices_data id
                JOIN topix_code tc ON tc.code = id.code
                WHERE id.date > ?
            ),
            entry_session AS (
                SELECT date AS entry_date, open AS entry_open
                FROM ranked_future_sessions
                    WHERE session_rank = 1
                ),
                exit_session AS (
                    SELECT date AS exit_date, open AS exit_open
                    FROM ranked_future_sessions
                    WHERE session_rank = 6
                )
                SELECT
                    e.entry_date,
                    x.exit_date,
                    x.exit_open / NULLIF(e.entry_open, 0) - 1 AS benchmark_return
                FROM entry_session e
                CROSS JOIN exit_session x
            """,
            (target_date,),
        )

    def _load_topix100_ranking_rows(
        self,
        target_date: str,
        metric: Topix100RankingMetric,
        sma_window: int,
    ) -> list[Mapping[str, Any]]:
        metric_column = _TOPIX100_RANKING_METRIC_SQL[metric]
        master_table, master_date_clause, master_params = _stock_master_source(
            self._reader, target_date
        )
        required_price_history_rows = (
            80 if metric == "price_sma_20_80" else int(sma_window)
        )
        required_history_rows = max(
            required_price_history_rows,
            _TOPIX100_VOLUME_LONG_WINDOW,
        )
        return self._reader.query(
            f"""
            WITH topix100_stocks AS (
                SELECT
                    code,
                    company_name,
                    market_code,
                    sector_33_name,
                    scale_category,
                    normalized_code
                FROM (
                    SELECT
                        code,
                        company_name,
                        market_code,
                        sector_33_name,
                        coalesce(scale_category, '') AS scale_category,
                        {_normalized_code_sql("code")} AS normalized_code,
                        ROW_NUMBER() OVER (
                            PARTITION BY {_normalized_code_sql("code")}
                            ORDER BY {_prefer_4digit_order_sql("code")}
                        ) AS rn
                    FROM {master_table}
                    WHERE {master_date_clause}coalesce(scale_category, '') IN (?, ?)
                )
                WHERE rn = 1
            ),
            stock_data_dedup AS (
                SELECT normalized_code, date, close, volume
                FROM (
                    SELECT
                        {_normalized_code_sql("code")} AS normalized_code,
                        date,
                        close,
                        volume,
                        ROW_NUMBER() OVER (
                            PARTITION BY {_normalized_code_sql("code")}, date
                            ORDER BY {_prefer_4digit_order_sql("code")}
                        ) AS rn
                    FROM stock_data
                    WHERE date <= ?
                )
                WHERE rn = 1
            ),
            feature_history AS (
                SELECT
                    s.code,
                    s.company_name,
                    s.market_code,
                    s.sector_33_name,
                    s.scale_category,
                    sd.normalized_code,
                    sd.date,
                    sd.close AS current_price,
                    sd.volume,
                    AVG(sd.close) OVER (
                        PARTITION BY sd.normalized_code
                        ORDER BY sd.date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS price_sma_20,
                    AVG(sd.close) OVER (
                        PARTITION BY sd.normalized_code
                        ORDER BY sd.date
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    ) AS price_sma_50,
                    AVG(sd.close) OVER (
                        PARTITION BY sd.normalized_code
                        ORDER BY sd.date
                        ROWS BETWEEN 79 PRECEDING AND CURRENT ROW
                    ) AS price_sma_80,
                    AVG(sd.close) OVER (
                        PARTITION BY sd.normalized_code
                        ORDER BY sd.date
                        ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
                    ) AS price_sma_100,
                    AVG(sd.volume) OVER (
                        PARTITION BY sd.normalized_code
                        ORDER BY sd.date
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) AS volume_sma_5,
                    AVG(sd.volume) OVER (
                        PARTITION BY sd.normalized_code
                        ORDER BY sd.date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS volume_sma_20,
                    ROW_NUMBER() OVER (
                        PARTITION BY sd.normalized_code
                        ORDER BY sd.date
                    ) AS history_row_number
                FROM stock_data_dedup sd
                JOIN topix100_stocks s ON s.normalized_code = sd.normalized_code
            ),
            current_snapshot AS (
                SELECT
                    code,
                    company_name,
                    market_code,
                    sector_33_name,
                    scale_category,
                    current_price,
                    volume,
                    current_price / NULLIF(price_sma_{int(sma_window)}, 0) - 1 AS price_vs_sma_gap,
                    price_sma_20 / NULLIF(price_sma_80, 0) AS price_sma_20_80,
                    volume_sma_5 / NULLIF(volume_sma_20, 0) AS volume_sma_5_20
                FROM feature_history
                WHERE date = ? AND history_row_number >= {required_history_rows}
            ),
            price_ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        ORDER BY {metric_column} DESC, code ASC
                    ) AS rank,
                    NTILE(10) OVER (
                        ORDER BY {metric_column} DESC, code ASC
                    ) AS price_decile
                FROM current_snapshot
                WHERE {metric_column} IS NOT NULL AND volume_sma_5_20 IS NOT NULL
            ),
            bucketed AS (
                SELECT
                    *,
                    CASE
                        WHEN price_decile = 1 THEN 'q1'
                        WHEN price_decile = 10 THEN 'q10'
                        WHEN price_decile IN (2, 3, 4) THEN 'q234'
                        ELSE 'other'
                    END AS price_bucket
                FROM price_ranked
            )
            SELECT
                rank,
                code,
                company_name,
                market_code,
                sector_33_name,
                scale_category,
                current_price,
                volume,
                price_vs_sma_gap,
                price_sma_20_80,
                volume_sma_5_20,
                price_decile,
                price_bucket
            FROM bucketed
            ORDER BY rank
            """,
            (*master_params, *_TOPIX100_SCALE_CATEGORIES, target_date, target_date),
        )

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
        sql = f"""
            WITH
            {stocks_cte},
            {stock_daily_cte},
            statements_canonical AS (
                SELECT
                    normalized_code,
                    disclosed_date,
                    type_of_current_period,
                    earnings_per_share,
                    bps,
                    forecast_eps,
                    next_year_forecast_earnings_per_share,
                    shares_outstanding
                FROM (
                    SELECT
                        {statements_norm} AS normalized_code,
                        disclosed_date,
                        type_of_current_period,
                        earnings_per_share,
                        bps,
                        forecast_eps,
                        next_year_forecast_earnings_per_share,
                        shares_outstanding,
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
                st.earnings_per_share,
                st.bps,
                st.forecast_eps,
                st.next_year_forecast_earnings_per_share,
                st.shares_outstanding
            FROM statements_canonical st
            JOIN stocks_canonical s
                ON s.normalized_code = st.normalized_code
            JOIN stock_daily sd
                ON sd.normalized_code = st.normalized_code
            WHERE st.disclosed_date <= ?{market_clause}
            ORDER BY s.code, st.disclosed_date DESC
        """
        return self._reader.query(sql, (date, date, *market_params))

    def _resolve_baseline_shares(
        self,
        rows: list[_StatementRow],
        *,
        as_of_date: str | None = None,
    ) -> float | None:
        return self._fundamental_calculator.resolve_baseline_shares(
            rows,
            as_of_date=as_of_date,
        )

    def _resolve_latest_actual_snapshot(
        self,
        rows: list[_StatementRow],
        baseline_shares: float | None,
        *,
        as_of_date: str | None = None,
    ) -> _ForecastValue | None:
        return self._fundamental_calculator.resolve_latest_actual_snapshot(
            rows,
            baseline_shares,
            as_of_date=as_of_date,
        )

    def _resolve_recent_actual_eps_max(
        self,
        rows: list[_StatementRow],
        baseline_shares: float | None,
        lookback_fy_count: int,
        *,
        as_of_date: str | None = None,
    ) -> float | None:
        return self._fundamental_calculator.resolve_recent_actual_eps_max(
            rows,
            baseline_shares,
            lookback_fy_count,
            as_of_date=as_of_date,
        )

    def _resolve_latest_fy_row(
        self,
        rows: list[_StatementRow],
        *,
        as_of_date: str | None = None,
    ) -> _LatestFyRow | None:
        return self._fundamental_calculator.resolve_latest_fy_row(
            rows,
            as_of_date=as_of_date,
        )

    def _resolve_latest_fy_forecast_snapshot(
        self,
        fy_row: _LatestFyRow | None,
        baseline_shares: float | None,
    ) -> _ForecastValue | None:
        return self._fundamental_calculator.resolve_latest_fy_forecast_snapshot(
            fy_row,
            baseline_shares,
        )

    def _resolve_latest_revised_forecast_snapshot(
        self,
        rows: list[_StatementRow],
        baseline_shares: float | None,
        fy_disclosed_date: str,
        *,
        as_of_date: str | None = None,
    ) -> _ForecastValue | None:
        return self._fundamental_calculator.resolve_latest_revised_forecast_snapshot(
            rows,
            baseline_shares,
            fy_disclosed_date,
            as_of_date=as_of_date,
        )

    def _resolve_latest_forecast_snapshot(
        self,
        rows: list[_StatementRow],
        baseline_shares: float | None,
        *,
        as_of_date: str | None = None,
    ) -> _ForecastValue | None:
        return self._fundamental_calculator.resolve_latest_forecast_snapshot(
            rows,
            baseline_shares,
            as_of_date=as_of_date,
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
            return self._resolve_latest_forecast_snapshot(
                rows,
                baseline_shares,
                as_of_date=as_of_date,
            )
        latest_fy = self._fundamental_calculator.resolve_latest_fy_row(
            rows, as_of_date=as_of_date
        )
        return self._resolve_latest_fy_forecast_snapshot(latest_fy, baseline_shares)

    def _resolve_latest_ratio_snapshot(
        self,
        actual_snapshot: _ForecastValue | None,
        forecast_snapshot: _ForecastValue | None,
    ) -> _ForecastValue | None:
        return self._fundamental_calculator.resolve_latest_ratio_snapshot(
            actual_snapshot,
            forecast_snapshot,
        )

    def _build_fundamental_item(
        self,
        stock_row: Mapping[str, Any],
        snapshot: _ForecastValue,
    ) -> FundamentalItem:
        return self._fundamental_calculator.build_fundamental_item(stock_row, snapshot)

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
        ranked: list[FundamentalRankingItem] = []
        for index, item in enumerate(sorted_items, start=1):
            ranked.append(
                FundamentalRankingItem(
                    rank=index,
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
            )
        return ranked

    def _latest_value_bps_statement(
        self,
        rows: list[Mapping[str, Any]],
        baseline_shares: float | None,
        *,
        as_of_date: str,
    ) -> Mapping[str, Any] | None:
        eligible = [
            row
            for row in rows
            if _normalize_period_label(row["type_of_current_period"]) == "FY"
            and str(row["disclosed_date"]) <= str(as_of_date)
        ]
        for row in sorted(
            eligible, key=lambda row: str(row["disclosed_date"]), reverse=True
        ):
            bps = _adjust_per_share_value(
                _to_nullable_float(row["bps"]),
                _to_nullable_float(row["shares_outstanding"]),
                baseline_shares,
            )
            if bps is not None and bps > 0:
                return row
        return None

    def _build_value_composite_item(
        self,
        row: Mapping[str, Any],
        rank: int,
    ) -> ValueCompositeRankingItem:
        raw_source = _str_or_none(row.get("forward_eps_source"))
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
            lowPbrScore=float(row["low_pbr_score"]),
            smallMarketCapScore=float(row["small_market_cap_score"]),
            lowForwardPerScore=float(row["low_forward_per_score"]),
            pbr=float(row["pbr"]),
            forwardPer=float(row["forward_per"]),
            marketCapBilJpy=float(row["market_cap_bil_jpy"]),
            bps=_finite_or_none(row.get("bps")),
            forwardEps=_finite_or_none(row.get("forward_eps")),
            latestFyDisclosedDate=_str_or_none(row.get("latest_fy_disclosed_date")),
            forwardEpsDisclosedDate=_str_or_none(row.get("forward_eps_disclosed_date")),
            forwardEpsSource=cast(Literal["revised", "fy"] | None, source),
        )

    def _get_trading_date_before(self, date: str, offset: int) -> str | None:
        """N営業日前の取引日を取得"""
        row = self._reader.query_one(
            "SELECT DISTINCT date FROM stock_data WHERE date < ? ORDER BY date DESC LIMIT 1 OFFSET ?",
            (date, offset),
        )
        return row["date"] if row else None

    def _get_previous_trading_date(self, date: str) -> str | None:
        """前営業日を取得"""
        row = self._reader.query_one(
            "SELECT DISTINCT date FROM stock_data WHERE date < ? ORDER BY date DESC LIMIT 1",
            (date,),
        )
        return row["date"] if row else None

    def _load_index_performance(
        self,
        date: str,
        *,
        lookback_days: int,
    ) -> list[IndexPerformanceItem]:
        if lookback_days < 1:
            return []
        if not self._table_exists("index_master") or not self._table_exists(
            "indices_data"
        ):
            return []

        rows = self._reader.query(
            """
            WITH ranked_index_history AS (
                SELECT
                    m.code,
                    m.name,
                    m.category,
                    d.date,
                    d.close,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.code
                        ORDER BY d.date DESC
                    ) AS rn
                FROM index_master m
                JOIN indices_data d
                    ON d.code = m.code
                WHERE d.date <= ?
                    AND d.close IS NOT NULL
                    AND d.close > 0
            ),
            current_rows AS (
                SELECT
                    code,
                    name,
                    category,
                    date AS current_date,
                    close AS current_close
                FROM ranked_index_history
                WHERE rn = 1
            ),
            base_rows AS (
                SELECT
                    code,
                    date AS base_date,
                    close AS base_close
                FROM ranked_index_history
                WHERE rn = ?
            )
            SELECT
                c.code,
                c.name,
                c.category,
                c.current_date,
                b.base_date,
                c.current_close,
                b.base_close,
                (c.current_close - b.base_close) AS change_amount,
                ((c.current_close - b.base_close) / b.base_close * 100) AS change_percentage
            FROM current_rows c
            JOIN base_rows b
                ON b.code = c.code
            WHERE b.base_close > 0
            ORDER BY
                CASE c.category
                    WHEN 'synthetic' THEN 0
                    WHEN 'topix' THEN 1
                    WHEN 'sector17' THEN 2
                    WHEN 'sector33' THEN 3
                    WHEN 'market' THEN 4
                    WHEN 'style' THEN 5
                    WHEN 'growth' THEN 6
                    WHEN 'reit' THEN 7
                    ELSE 99
                END,
                c.code
            """,
            (date, lookback_days + 1),
        )
        return [
            IndexPerformanceItem(
                code=row["code"],
                name=row["name"],
                category=row["category"],
                currentDate=row["current_date"],
                baseDate=row["base_date"],
                currentClose=row["current_close"],
                baseClose=row["base_close"],
                changeAmount=row["change_amount"],
                changePercentage=row["change_percentage"],
                lookbackDays=lookback_days,
            )
            for row in rows
        ]

    def _ranking_by_trading_value(
        self, date: str, limit: int, market_codes: list[str]
    ) -> list[RankingItem]:
        """売買代金ランキング（単日）"""
        market_clause, market_params = _build_market_filter(market_codes)
        stocks_cte = _stocks_canonical_cte()
        stock_daily_cte = _stock_data_dedup_cte("stock_daily", where_clause="date = ?")
        sql = f"""
            WITH
            {stocks_cte},
            {stock_daily_cte}
            SELECT {RANKING_BASE_COLUMNS},
                sd.close as current_price,
                sd.volume,
                sd.close * sd.volume as trading_value
            FROM stock_daily sd
            JOIN stocks_canonical s
                ON s.normalized_code = sd.normalized_code
            WHERE 1 = 1{market_clause}
            ORDER BY trading_value DESC LIMIT ?
        """
        rows = self._reader.query(sql, (date, *market_params, limit))
        return [
            _row_to_item(row, i + 1, tradingValue=row["trading_value"])
            for i, row in enumerate(rows)
        ]

    def _ranking_by_trading_value_average(
        self, date: str, lookback_days: int, limit: int, market_codes: list[str]
    ) -> list[RankingItem]:
        """売買代金平均ランキング（N日平均）"""
        start_date = self._get_trading_date_before(date, lookback_days - 1)
        if not start_date:
            return []

        market_clause, market_params = _build_market_filter(market_codes)
        stocks_cte = _stocks_canonical_cte()
        stock_window_cte = _stock_data_dedup_cte(
            "stock_window",
            where_clause="date >= ? AND date <= ?",
        )
        sql = f"""
            WITH
            {stocks_cte},
            {stock_window_cte}
            SELECT {RANKING_BASE_COLUMNS},
                MAX(sd.close) as current_price,
                SUM(sd.volume) as volume,
                AVG(sd.close * sd.volume) as avg_trading_value
            FROM stock_window sd
            JOIN stocks_canonical s
                ON s.normalized_code = sd.normalized_code
            WHERE 1 = 1{market_clause}
            GROUP BY s.code, s.company_name, s.market_code, s.sector_33_name
            ORDER BY avg_trading_value DESC LIMIT ?
        """
        rows = self._reader.query(sql, (start_date, date, *market_params, limit))
        return [
            _row_to_item(
                row,
                i + 1,
                tradingValueAverage=row["avg_trading_value"],
                lookbackDays=lookback_days,
            )
            for i, row in enumerate(rows)
        ]

    def _ranking_by_price_change(
        self, date: str, limit: int, market_codes: list[str], order_dir: str
    ) -> list[RankingItem]:
        """騰落率ランキング（単日）"""
        prev_date = self._get_previous_trading_date(date)
        if not prev_date:
            return []

        market_clause, market_params = _build_market_filter(market_codes)
        stocks_cte = _stocks_canonical_cte()
        curr_cte = _stock_data_dedup_cte("curr_daily", where_clause="date = ?")
        prev_cte = _stock_data_dedup_cte("prev_daily", where_clause="date = ?")
        sql = f"""
            WITH
            {stocks_cte},
            {curr_cte},
            {prev_cte}
            SELECT {RANKING_BASE_COLUMNS},
                curr.close as current_price,
                curr.volume,
                prev.close as previous_price,
                (curr.close - prev.close) as change_amount,
                ((curr.close - prev.close) / prev.close * 100) as change_percentage
            FROM curr_daily curr
            JOIN prev_daily prev
                ON curr.normalized_code = prev.normalized_code
            JOIN stocks_canonical s
                ON s.normalized_code = curr.normalized_code
            WHERE 1 = 1
                AND prev.close > 0
                AND curr.close > 0
                AND curr.close != prev.close{market_clause}
            ORDER BY change_percentage {order_dir} LIMIT ?
        """
        rows = self._reader.query(sql, (date, prev_date, *market_params, limit))
        return [
            _row_to_item(
                row,
                i + 1,
                previousPrice=row["previous_price"],
                changeAmount=row["change_amount"],
                changePercentage=row["change_percentage"],
            )
            for i, row in enumerate(rows)
        ]

    def _ranking_by_price_change_from_days(
        self,
        date: str,
        lookback_days: int,
        limit: int,
        market_codes: list[str],
        order_dir: str,
    ) -> list[RankingItem]:
        """騰落率ランキング（N日前比較）"""
        base_date = self._get_trading_date_before(date, lookback_days)
        if not base_date:
            return []

        market_clause, market_params = _build_market_filter(market_codes)
        stocks_cte = _stocks_canonical_cte()
        curr_cte = _stock_data_dedup_cte("curr_daily", where_clause="date = ?")
        base_cte = _stock_data_dedup_cte("base_daily", where_clause="date = ?")
        sql = f"""
            WITH
            {stocks_cte},
            {curr_cte},
            {base_cte}
            SELECT {RANKING_BASE_COLUMNS},
                curr.close as current_price,
                curr.volume,
                base.close as base_price,
                (curr.close - base.close) as change_amount,
                ((curr.close - base.close) / base.close * 100) as change_percentage
            FROM curr_daily curr
            JOIN base_daily base
                ON curr.normalized_code = base.normalized_code
            JOIN stocks_canonical s
                ON s.normalized_code = curr.normalized_code
            WHERE 1 = 1
                AND base.close > 0
                AND curr.close > 0
                AND curr.close != base.close{market_clause}
            ORDER BY change_percentage {order_dir} LIMIT ?
        """
        rows = self._reader.query(sql, (date, base_date, *market_params, limit))
        return [
            _row_to_item(
                row,
                i + 1,
                basePrice=row["base_price"],
                changeAmount=row["change_amount"],
                changePercentage=row["change_percentage"],
                lookbackDays=lookback_days,
            )
            for i, row in enumerate(rows)
        ]

    def _ranking_by_period_high(
        self, date: str, period_days: int, limit: int, market_codes: list[str]
    ) -> list[RankingItem]:
        """期間高値ランキング"""
        start_date = self._get_trading_date_before(date, period_days)
        if not start_date:
            return []

        market_clause, market_params = _build_market_filter(market_codes)
        stocks_cte = _stocks_canonical_cte()
        stock_window_cte = _stock_data_dedup_cte(
            "stock_window",
            where_clause="date > ? AND date < ?",
        )
        curr_cte = _stock_data_dedup_cte("curr_daily", where_clause="date = ?")
        sql = f"""
            WITH
            {stocks_cte},
            {stock_window_cte},
            {curr_cte},
            period_high AS (
                SELECT normalized_code, MAX(high) as max_high
                FROM stock_window
                GROUP BY normalized_code
            )
            SELECT {RANKING_BASE_COLUMNS},
                curr.close as current_price,
                curr.volume,
                curr.close * curr.volume as trading_value,
                ph.max_high as base_price,
                (curr.close - ph.max_high) as change_amount,
                ((curr.close - ph.max_high) / ph.max_high * 100) as change_percentage
            FROM curr_daily curr
            JOIN stocks_canonical s
                ON s.normalized_code = curr.normalized_code
            JOIN period_high ph
                ON ph.normalized_code = curr.normalized_code
            WHERE curr.close >= ph.max_high
                AND ph.max_high > 0{market_clause}
            ORDER BY change_percentage DESC LIMIT ?
        """
        rows = self._reader.query(sql, (start_date, date, date, *market_params, limit))
        return [
            _row_to_item(
                row,
                i + 1,
                tradingValue=row["trading_value"],
                basePrice=row["base_price"],
                changeAmount=row["change_amount"],
                changePercentage=row["change_percentage"],
                lookbackDays=period_days,
            )
            for i, row in enumerate(rows)
        ]

    def _ranking_by_period_low(
        self, date: str, period_days: int, limit: int, market_codes: list[str]
    ) -> list[RankingItem]:
        """期間安値ランキング"""
        start_date = self._get_trading_date_before(date, period_days)
        if not start_date:
            return []

        market_clause, market_params = _build_market_filter(market_codes)
        stocks_cte = _stocks_canonical_cte()
        stock_window_cte = _stock_data_dedup_cte(
            "stock_window",
            where_clause="date > ? AND date < ?",
        )
        curr_cte = _stock_data_dedup_cte("curr_daily", where_clause="date = ?")
        sql = f"""
            WITH
            {stocks_cte},
            {stock_window_cte},
            {curr_cte},
            period_low AS (
                SELECT normalized_code, MIN(low) as min_low
                FROM stock_window
                GROUP BY normalized_code
            )
            SELECT {RANKING_BASE_COLUMNS},
                curr.close as current_price,
                curr.volume,
                curr.close * curr.volume as trading_value,
                pl.min_low as base_price,
                (curr.close - pl.min_low) as change_amount,
                ((curr.close - pl.min_low) / pl.min_low * 100) as change_percentage
            FROM curr_daily curr
            JOIN stocks_canonical s
                ON s.normalized_code = curr.normalized_code
            JOIN period_low pl
                ON pl.normalized_code = curr.normalized_code
            WHERE curr.close <= pl.min_low
                AND pl.min_low > 0{market_clause}
            ORDER BY change_percentage ASC LIMIT ?
        """
        rows = self._reader.query(sql, (start_date, date, date, *market_params, limit))
        return [
            _row_to_item(
                row,
                i + 1,
                tradingValue=row["trading_value"],
                basePrice=row["base_price"],
                changeAmount=row["change_amount"],
                changePercentage=row["change_percentage"],
                lookbackDays=period_days,
            )
            for i, row in enumerate(rows)
        ]
