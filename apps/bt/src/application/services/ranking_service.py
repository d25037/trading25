"""
Ranking Service

market.db からランキングデータを取得するサービス。
Hono MarketRankingService 互換。
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.application.services.market_code_alias import resolve_market_codes
from src.entrypoints.http.schemas.ranking import (
    FundamentalRankingItem,
    FundamentalRankings,
    MarketFundamentalRankingResponse,
    MarketRankingResponse,
    RankingItem,
    Rankings,
)
from src.shared.models.types import normalize_period_type


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _build_market_filter(market_codes: list[str]) -> tuple[str, list[str]]:
    """マーケットコードのWHERE句を構築"""
    if not market_codes:
        return "", []
    placeholders = ",".join("?" for _ in market_codes)
    return f" AND s.market_code IN ({placeholders})", market_codes


RANKING_BASE_COLUMNS = "s.code, s.company_name, s.market_code, s.sector_33_name"
FUNDAMENTAL_BASE_COLUMNS = (
    "s.code, s.company_name, s.market_code, s.sector_33_name, "
    "sd.close as current_price, sd.volume"
)
_QUARTER_PERIODS = {"1Q", "2Q", "3Q"}


@dataclass
class _StatementRow:
    code: str
    disclosed_date: str
    period_type: str
    earnings_per_share: float | None
    forecast_eps: float | None
    next_year_forecast_earnings_per_share: float | None
    shares_outstanding: float | None


@dataclass
class _ForecastValue:
    value: float
    disclosed_date: str
    period_type: str
    source: Literal["revised", "fy"]


@dataclass
class _LatestFyRow:
    disclosed_date: str
    period_type: str
    shares_outstanding: float | None
    forecast_value: float | None


def _normalize_period_label(period_type: str | None) -> str:
    normalized = normalize_period_type(period_type)
    if normalized is None:
        return ""
    return normalized


def _round_eps(value: float) -> float:
    return round(value, 2)


def _is_valid_share_count(value: float | None) -> bool:
    if value is None:
        return False
    if value == 0:
        return False
    return not math.isnan(value)


def _adjust_per_share_value(
    raw_value: float | None,
    current_shares: float | None,
    baseline_shares: float | None,
) -> float | None:
    if raw_value is None:
        return None
    if not (_is_valid_share_count(current_shares) and _is_valid_share_count(baseline_shares)):
        return _round_eps(raw_value)
    assert current_shares is not None
    assert baseline_shares is not None
    adjusted = raw_value * (current_shares / baseline_shares)
    return _round_eps(adjusted)


def _to_nullable_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _row_to_item(row: sqlite3.Row, rank: int, **extra: Any) -> RankingItem:
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
            lastUpdated=_now_iso(),
        )

    def get_fundamental_rankings(
        self,
        limit: int = 20,
        markets: str = "prime",
    ) -> MarketFundamentalRankingResponse:
        """最新の実績EPS/予想EPSランキングを取得"""
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
                )
            )

        actual_candidates: list[FundamentalRankingItem] = []
        forecast_candidates: list[FundamentalRankingItem] = []
        for stock in stock_rows:
            code = str(stock["code"])
            statements = statements_by_code.get(code)
            if not statements:
                continue

            baseline_shares = self._resolve_baseline_shares(statements)
            actual_snapshot = self._resolve_latest_actual_snapshot(statements, baseline_shares)
            forecast_snapshot = self._resolve_latest_forecast_snapshot(statements, baseline_shares)

            if actual_snapshot is not None:
                actual_candidates.append(
                    self._build_fundamental_item(stock, actual_snapshot)
                )
            if forecast_snapshot is not None:
                forecast_candidates.append(
                    self._build_fundamental_item(stock, forecast_snapshot)
                )

        return MarketFundamentalRankingResponse(
            date=target_date,
            markets=requested_market_codes,
            rankings=FundamentalRankings(
                forecastHigh=self._rank_fundamental_items(
                    forecast_candidates, limit, descending=True
                ),
                forecastLow=self._rank_fundamental_items(
                    forecast_candidates, limit, descending=False
                ),
                actualHigh=self._rank_fundamental_items(
                    actual_candidates, limit, descending=True
                ),
                actualLow=self._rank_fundamental_items(
                    actual_candidates, limit, descending=False
                ),
            ),
            lastUpdated=_now_iso(),
        )

    # --- Private ranking methods ---

    def _load_fundamental_stock_rows(
        self,
        date: str,
        market_codes: list[str],
    ) -> list[sqlite3.Row]:
        market_clause, market_params = _build_market_filter(market_codes)
        sql = f"""
            SELECT {FUNDAMENTAL_BASE_COLUMNS}
            FROM stocks s
            JOIN stock_data sd ON sd.code = s.code
            WHERE sd.date = ?{market_clause}
        """
        return self._reader.query(sql, (date, *market_params))

    def _load_fundamental_statement_rows(
        self,
        date: str,
        market_codes: list[str],
    ) -> list[sqlite3.Row]:
        market_clause, market_params = _build_market_filter(market_codes)
        sql = f"""
            SELECT
                st.code,
                st.disclosed_date,
                st.type_of_current_period,
                st.earnings_per_share,
                st.forecast_eps,
                st.next_year_forecast_earnings_per_share,
                st.shares_outstanding
            FROM statements st
            JOIN stocks s ON s.code = st.code
            JOIN stock_data sd ON sd.code = st.code
            WHERE sd.date = ?{market_clause}
            ORDER BY st.code, st.disclosed_date DESC
        """
        return self._reader.query(sql, (date, *market_params))

    def _resolve_baseline_shares(self, rows: list[_StatementRow]) -> float | None:
        sorted_rows = sorted(rows, key=lambda row: row.disclosed_date, reverse=True)
        quarterly = [
            row.shares_outstanding
            for row in sorted_rows
            if row.period_type in _QUARTER_PERIODS and _is_valid_share_count(row.shares_outstanding)
        ]
        if quarterly:
            return quarterly[0]

        fallback = [
            row.shares_outstanding
            for row in sorted_rows
            if _is_valid_share_count(row.shares_outstanding)
        ]
        if fallback:
            return fallback[0]
        return None

    def _resolve_latest_actual_snapshot(
        self,
        rows: list[_StatementRow],
        baseline_shares: float | None,
    ) -> _ForecastValue | None:
        sorted_rows = sorted(rows, key=lambda row: row.disclosed_date, reverse=True)
        for row in sorted_rows:
            if row.period_type != "FY":
                continue
            adjusted = _adjust_per_share_value(
                row.earnings_per_share,
                row.shares_outstanding,
                baseline_shares,
            )
            if adjusted is None:
                continue
            return _ForecastValue(
                value=adjusted,
                disclosed_date=row.disclosed_date,
                period_type=row.period_type,
                source="fy",
            )
        return None

    def _resolve_latest_fy_row(self, rows: list[_StatementRow]) -> _LatestFyRow | None:
        sorted_rows = sorted(rows, key=lambda row: row.disclosed_date, reverse=True)
        for row in sorted_rows:
            if row.period_type != "FY":
                continue
            forecast_value = (
                row.next_year_forecast_earnings_per_share
                if row.next_year_forecast_earnings_per_share is not None
                else row.forecast_eps
            )
            return _LatestFyRow(
                disclosed_date=row.disclosed_date,
                period_type=row.period_type,
                shares_outstanding=row.shares_outstanding,
                forecast_value=forecast_value,
            )
        return None

    def _resolve_latest_fy_forecast_snapshot(
        self,
        fy_row: _LatestFyRow | None,
        baseline_shares: float | None,
    ) -> _ForecastValue | None:
        if fy_row is None:
            return None
        adjusted = _adjust_per_share_value(
            fy_row.forecast_value,
            fy_row.shares_outstanding,
            baseline_shares,
        )
        if adjusted is None:
            return None
        return _ForecastValue(
            value=adjusted,
            disclosed_date=fy_row.disclosed_date,
            period_type=fy_row.period_type,
            source="fy",
        )

    def _resolve_latest_revised_forecast_snapshot(
        self,
        rows: list[_StatementRow],
        baseline_shares: float | None,
        fy_disclosed_date: str,
    ) -> _ForecastValue | None:
        sorted_rows = sorted(rows, key=lambda row: row.disclosed_date, reverse=True)
        for row in sorted_rows:
            if row.period_type not in _QUARTER_PERIODS:
                continue
            if row.disclosed_date <= fy_disclosed_date:
                continue
            raw_revised = (
                row.forecast_eps
                if row.forecast_eps is not None
                else row.next_year_forecast_earnings_per_share
            )
            adjusted = _adjust_per_share_value(
                raw_revised,
                row.shares_outstanding,
                baseline_shares,
            )
            if adjusted is None:
                continue
            return _ForecastValue(
                value=adjusted,
                disclosed_date=row.disclosed_date,
                period_type=row.period_type,
                source="revised",
            )
        return None

    def _resolve_latest_forecast_snapshot(
        self,
        rows: list[_StatementRow],
        baseline_shares: float | None,
    ) -> _ForecastValue | None:
        latest_fy_row = self._resolve_latest_fy_row(rows)
        if latest_fy_row is None:
            return None

        revised = self._resolve_latest_revised_forecast_snapshot(
            rows,
            baseline_shares,
            latest_fy_row.disclosed_date,
        )
        if revised is not None:
            return revised
        return self._resolve_latest_fy_forecast_snapshot(latest_fy_row, baseline_shares)

    def _build_fundamental_item(
        self,
        stock_row: sqlite3.Row,
        snapshot: _ForecastValue,
    ) -> FundamentalRankingItem:
        return FundamentalRankingItem(
            rank=0,
            code=str(stock_row["code"]),
            companyName=str(stock_row["company_name"]),
            marketCode=str(stock_row["market_code"]),
            sector33Name=str(stock_row["sector_33_name"]),
            currentPrice=float(stock_row["current_price"]),
            volume=float(stock_row["volume"]),
            epsValue=snapshot.value,
            disclosedDate=snapshot.disclosed_date,
            periodType=snapshot.period_type,
            source=snapshot.source,
        )

    def _rank_fundamental_items(
        self,
        items: list[FundamentalRankingItem],
        limit: int,
        *,
        descending: bool,
    ) -> list[FundamentalRankingItem]:
        if descending:
            sorted_items = sorted(items, key=lambda item: (-item.epsValue, item.code))
        else:
            sorted_items = sorted(items, key=lambda item: (item.epsValue, item.code))

        ranked: list[FundamentalRankingItem] = []
        for index, item in enumerate(sorted_items[:limit], start=1):
            ranked.append(item.model_copy(update={"rank": index}))
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

    def _ranking_by_trading_value(
        self, date: str, limit: int, market_codes: list[str]
    ) -> list[RankingItem]:
        """売買代金ランキング（単日）"""
        market_clause, market_params = _build_market_filter(market_codes)
        sql = f"""
            SELECT {RANKING_BASE_COLUMNS},
                sd.close as current_price,
                sd.volume,
                sd.close * sd.volume as trading_value
            FROM stock_data sd
            JOIN stocks s ON s.code = sd.code
            WHERE sd.date = ?{market_clause}
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
        sql = f"""
            SELECT {RANKING_BASE_COLUMNS},
                MAX(sd.close) as current_price,
                SUM(sd.volume) as volume,
                AVG(sd.close * sd.volume) as avg_trading_value
            FROM stock_data sd
            JOIN stocks s ON s.code = sd.code
            WHERE sd.date >= ? AND sd.date <= ?{market_clause}
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
        sql = f"""
            SELECT {RANKING_BASE_COLUMNS},
                curr.close as current_price,
                curr.volume,
                prev.close as previous_price,
                (curr.close - prev.close) as change_amount,
                ((curr.close - prev.close) / prev.close * 100) as change_percentage
            FROM stock_data curr
            JOIN stock_data prev ON curr.code = prev.code
            JOIN stocks s ON s.code = curr.code
            WHERE curr.date = ?
                AND prev.date = ?
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
        sql = f"""
            SELECT {RANKING_BASE_COLUMNS},
                curr.close as current_price,
                curr.volume,
                base.close as base_price,
                (curr.close - base.close) as change_amount,
                ((curr.close - base.close) / base.close * 100) as change_percentage
            FROM stock_data curr
            JOIN stock_data base ON curr.code = base.code
            JOIN stocks s ON s.code = curr.code
            WHERE curr.date = ?
                AND base.date = ?
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
        sql = f"""
            WITH period_high AS (
                SELECT code, MAX(high) as max_high
                FROM stock_data
                WHERE date > ? AND date < ?
                GROUP BY code
            )
            SELECT {RANKING_BASE_COLUMNS},
                curr.close as current_price,
                curr.volume,
                curr.close * curr.volume as trading_value,
                ph.max_high as base_price,
                (curr.close - ph.max_high) as change_amount,
                ((curr.close - ph.max_high) / ph.max_high * 100) as change_percentage
            FROM stock_data curr
            JOIN stocks s ON s.code = curr.code
            JOIN period_high ph ON ph.code = curr.code
            WHERE curr.date = ?
                AND curr.close >= ph.max_high
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
        sql = f"""
            WITH period_low AS (
                SELECT code, MIN(low) as min_low
                FROM stock_data
                WHERE date > ? AND date < ?
                GROUP BY code
            )
            SELECT {RANKING_BASE_COLUMNS},
                curr.close as current_price,
                curr.volume,
                curr.close * curr.volume as trading_value,
                pl.min_low as base_price,
                (curr.close - pl.min_low) as change_amount,
                ((curr.close - pl.min_low) / pl.min_low * 100) as change_percentage
            FROM stock_data curr
            JOIN stocks s ON s.code = curr.code
            JOIN period_low pl ON pl.code = curr.code
            WHERE curr.date = ?
                AND curr.close <= pl.min_low
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
