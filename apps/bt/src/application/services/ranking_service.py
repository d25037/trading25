"""
Ranking Service

DuckDB market data からランキングデータを取得するサービス。
Hono MarketRankingService 互換。
"""

from __future__ import annotations

from datetime import UTC, datetime
from collections.abc import Mapping
from typing import Any, Literal, cast

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.application.services.market_code_alias import resolve_market_codes
from src.domains.analytics.fundamental_ranking import (
    FundamentalItem,
    FundamentalRankingCalculator,
    ForecastValue as _ForecastValue,
    LatestFyRow as _LatestFyRow,
    StatementRow as _StatementRow,
    normalize_period_label as _normalize_period_label,
    resolve_fy_cycle_key as _resolve_fy_cycle_key,
    to_nullable_float as _to_nullable_float,
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
    Topix100RankingResponse,
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


Topix100PriceBucket = Literal["q1", "q10", "q456", "other"]
Topix100VolumeBucket = Literal["high", "low"]
_TOPIX100_RANKING_METRIC_SQL: dict[Topix100RankingMetric, str] = {
    "price_vs_sma20_gap": "price_vs_sma20_gap",
    "price_sma_20_80": "price_sma_20_80",
}


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
    select_ohlc = ", open, high, low, close, volume" if include_ohlc else ", close, volume"
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
_QUARTER_PERIODS = {"1Q", "2Q", "3Q"}
_SUPPORTED_FUNDAMENTAL_RATIO_METRIC_KEY = "eps_forecast_to_actual"


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
            trading_value = self._ranking_by_trading_value(target_date, limit, query_market_codes)

        if lookback_days > 1:
            gainers = self._ranking_by_price_change_from_days(
                target_date, lookback_days, limit, query_market_codes, "DESC"
            )
            losers = self._ranking_by_price_change_from_days(
                target_date, lookback_days, limit, query_market_codes, "ASC"
            )
        else:
            gainers = self._ranking_by_price_change(target_date, limit, query_market_codes, "DESC")
            losers = self._ranking_by_price_change(target_date, limit, query_market_codes, "ASC")

        period_high = self._ranking_by_period_high(target_date, period_days, limit, query_market_codes)
        period_low = self._ranking_by_period_low(target_date, period_days, limit, query_market_codes)
        index_performance = self._load_index_performance(target_date, lookback_days=lookback_days)

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
        metric: Topix100RankingMetric = "price_vs_sma20_gap",
    ) -> Topix100RankingResponse:
        """TOPIX100 の snapshot ランキングを返す。"""
        if metric not in _TOPIX100_RANKING_METRIC_SQL:
            raise ValueError(f"Unsupported TOPIX100 ranking metric: {metric}")

        target_date = self._resolve_topix100_ranking_date(date)
        rows = self._load_topix100_ranking_rows(target_date, metric)
        if not rows:
            raise ValueError(f"No TOPIX100 ranking data available for date: {target_date}")

        items = [
            Topix100RankingItem(
                rank=int(row["rank"]),
                code=str(row["code"]),
                companyName=str(row["company_name"]),
                marketCode=str(row["market_code"]),
                sector33Name=str(row["sector_33_name"]),
                scaleCategory=str(row["scale_category"] or ""),
                currentPrice=float(row["current_price"]),
                volume=float(row["volume"]),
                priceVsSma20Gap=float(row["price_vs_sma20_gap"]),
                priceSma20_80=float(row["price_sma_20_80"]),
                volumeSma20_80=float(row["volume_sma_20_80"]),
                priceDecile=int(row["price_decile"]),
                priceBucket=cast(Topix100PriceBucket, row["price_bucket"]),
                volumeBucket=(
                    cast(Topix100VolumeBucket, row["volume_bucket"])
                    if row["volume_bucket"] is not None
                    else None
                ),
            )
            for row in rows
        ]

        return Topix100RankingResponse(
            date=target_date,
            rankingMetric=metric,
            itemCount=len(items),
            items=items,
            lastUpdated=_now_iso(),
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

        if forecast_above_all_actuals is not None and not forecast_above_recent_fy_actuals:
            forecast_above_recent_fy_actuals = forecast_above_all_actuals

        requested_market_codes, query_market_codes = resolve_market_codes(markets)
        date_row = self._reader.query_one("SELECT MAX(date) as max_date FROM stock_data")
        if date_row is None or date_row["max_date"] is None:
            raise ValueError("No trading data available in database")
        target_date = date_row["max_date"]

        stock_rows = self._load_fundamental_stock_rows(target_date, query_market_codes)
        statement_rows = self._load_fundamental_statement_rows(target_date, query_market_codes)

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

            baseline_shares = self._resolve_baseline_shares(statements)
            actual_snapshot = self._resolve_latest_actual_snapshot(statements, baseline_shares)
            forecast_snapshot = self._resolve_latest_forecast_snapshot(statements, baseline_shares)

            if forecast_above_recent_fy_actuals:
                if forecast_snapshot is None:
                    continue
                recent_max_actual_eps = self._resolve_recent_actual_eps_max(
                    statements,
                    baseline_shares,
                    forecast_lookback_fy_count,
                )
                if (
                    recent_max_actual_eps is None
                    or forecast_snapshot.value <= recent_max_actual_eps
                ):
                    continue

            ratio_snapshot = self._resolve_latest_ratio_snapshot(actual_snapshot, forecast_snapshot)

            if ratio_snapshot is None:
                continue
            ratio_candidates.append(self._build_fundamental_item(stock, ratio_snapshot))

        ratio_high = self._rank_fundamental_items(ratio_candidates, limit, descending=True)
        ratio_low = self._rank_fundamental_items(ratio_candidates, limit, descending=False)

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

    # --- Private ranking methods ---

    def _resolve_topix100_ranking_date(self, date: str | None) -> str:
        if date:
            return date

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
                    FROM stocks
                    WHERE coalesce(scale_category, '') IN (?, ?)
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
            _TOPIX100_SCALE_CATEGORIES,
        )
        if row is None or row["max_date"] is None:
            raise ValueError("No TOPIX100 trading data available in database")
        return str(row["max_date"])

    def _load_topix100_ranking_rows(
        self,
        target_date: str,
        metric: Topix100RankingMetric,
    ) -> list[Mapping[str, Any]]:
        metric_column = _TOPIX100_RANKING_METRIC_SQL[metric]
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
                    FROM stocks
                    WHERE coalesce(scale_category, '') IN (?, ?)
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
                        ROWS BETWEEN 79 PRECEDING AND CURRENT ROW
                    ) AS price_sma_80,
                    AVG(sd.volume) OVER (
                        PARTITION BY sd.normalized_code
                        ORDER BY sd.date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS volume_sma_20,
                    AVG(sd.volume) OVER (
                        PARTITION BY sd.normalized_code
                        ORDER BY sd.date
                        ROWS BETWEEN 79 PRECEDING AND CURRENT ROW
                    ) AS volume_sma_80,
                    COUNT(*) OVER (
                        PARTITION BY sd.normalized_code
                        ORDER BY sd.date
                        ROWS BETWEEN 79 PRECEDING AND CURRENT ROW
                    ) AS price_window_count_80
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
                    current_price / NULLIF(price_sma_20, 0) - 1 AS price_vs_sma20_gap,
                    price_sma_20 / NULLIF(price_sma_80, 0) AS price_sma_20_80,
                    volume_sma_20 / NULLIF(volume_sma_80, 0) AS volume_sma_20_80
                FROM feature_history
                WHERE date = ? AND price_window_count_80 >= 80
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
                WHERE {metric_column} IS NOT NULL AND volume_sma_20_80 IS NOT NULL
            ),
            bucketed AS (
                SELECT
                    *,
                    CASE
                        WHEN price_decile = 1 THEN 'q1'
                        WHEN price_decile = 10 THEN 'q10'
                        WHEN price_decile IN (4, 5, 6) THEN 'q456'
                        ELSE 'other'
                    END AS price_bucket
                FROM price_ranked
            ),
            volume_ranked AS (
                SELECT
                    *,
                    CASE
                        WHEN price_bucket IN ('q1', 'q10', 'q456')
                        THEN NTILE(2) OVER (
                            PARTITION BY price_bucket
                            ORDER BY volume_sma_20_80 DESC, code ASC
                        )
                        ELSE NULL
                    END AS volume_half_rank
                FROM bucketed
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
                price_vs_sma20_gap,
                price_sma_20_80,
                volume_sma_20_80,
                price_decile,
                price_bucket,
                CASE
                    WHEN volume_half_rank = 1 THEN 'high'
                    WHEN volume_half_rank = 2 THEN 'low'
                    ELSE NULL
                END AS volume_bucket
            FROM volume_ranked
            ORDER BY rank
            """,
            (*_TOPIX100_SCALE_CATEGORIES, target_date, target_date),
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
                    forecast_eps,
                    next_year_forecast_earnings_per_share,
                    shares_outstanding
                FROM (
                    SELECT
                        {statements_norm} AS normalized_code,
                        disclosed_date,
                        type_of_current_period,
                        earnings_per_share,
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
                st.forecast_eps,
                st.next_year_forecast_earnings_per_share,
                st.shares_outstanding
            FROM statements_canonical st
            JOIN stocks_canonical s
                ON s.normalized_code = st.normalized_code
            JOIN stock_daily sd
                ON sd.normalized_code = st.normalized_code
            WHERE 1 = 1{market_clause}
            ORDER BY s.code, st.disclosed_date DESC
        """
        return self._reader.query(sql, (date, *market_params))

    def _resolve_baseline_shares(self, rows: list[_StatementRow]) -> float | None:
        return self._fundamental_calculator.resolve_baseline_shares(rows)

    def _resolve_latest_actual_snapshot(
        self,
        rows: list[_StatementRow],
        baseline_shares: float | None,
    ) -> _ForecastValue | None:
        return self._fundamental_calculator.resolve_latest_actual_snapshot(rows, baseline_shares)

    def _resolve_recent_actual_eps_max(
        self,
        rows: list[_StatementRow],
        baseline_shares: float | None,
        lookback_fy_count: int,
    ) -> float | None:
        return self._fundamental_calculator.resolve_recent_actual_eps_max(
            rows,
            baseline_shares,
            lookback_fy_count,
        )

    def _resolve_latest_fy_row(self, rows: list[_StatementRow]) -> _LatestFyRow | None:
        return self._fundamental_calculator.resolve_latest_fy_row(rows)

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
    ) -> _ForecastValue | None:
        return self._fundamental_calculator.resolve_latest_revised_forecast_snapshot(
            rows,
            baseline_shares,
            fy_disclosed_date,
        )

    def _resolve_latest_forecast_snapshot(
        self,
        rows: list[_StatementRow],
        baseline_shares: float | None,
    ) -> _ForecastValue | None:
        return self._fundamental_calculator.resolve_latest_forecast_snapshot(
            rows,
            baseline_shares,
        )

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
        if not self._table_exists("index_master") or not self._table_exists("indices_data"):
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
                row, i + 1,
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
                row, i + 1,
                previousPrice=row["previous_price"],
                changeAmount=row["change_amount"],
                changePercentage=row["change_percentage"],
            )
            for i, row in enumerate(rows)
        ]

    def _ranking_by_price_change_from_days(
        self, date: str, lookback_days: int, limit: int, market_codes: list[str], order_dir: str
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
                row, i + 1,
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
                row, i + 1,
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
                row, i + 1,
                tradingValue=row["trading_value"],
                basePrice=row["base_price"],
                changeAmount=row["change_amount"],
                changePercentage=row["change_percentage"],
                lookbackDays=period_days,
            )
            for i, row in enumerate(rows)
        ]
