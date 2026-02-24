"""
Ranking Service

market.db からランキングデータを取得するサービス。
Hono MarketRankingService 互換。
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.application.services.market_code_alias import resolve_market_codes
from src.entrypoints.http.schemas.ranking import (
    MarketRankingResponse,
    RankingItem,
    Rankings,
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _build_market_filter(market_codes: list[str]) -> tuple[str, list[str]]:
    """マーケットコードのWHERE句を構築"""
    if not market_codes:
        return "", []
    placeholders = ",".join("?" for _ in market_codes)
    return f" AND s.market_code IN ({placeholders})", market_codes


RANKING_BASE_COLUMNS = "s.code, s.company_name, s.market_code, s.sector_33_name"


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

    # --- Private ranking methods ---

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
