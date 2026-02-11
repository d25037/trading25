"""
Market Data Service

market.db から株式・TOPIX データを読み取るサービス。
Hono market-data-service.ts と同等のロジック。
"""

from __future__ import annotations

from src.lib.market_db.market_reader import MarketDbReader
from src.lib.market_db.query_helpers import stock_code_candidates
from src.server.services.market_code_alias import resolve_market_codes
from src.server.schemas.market_data import (
    MarketOHLCRecord,
    MarketOHLCVRecord,
    MarketStockData,
    StockInfo,
)


def _stock_code_candidates(code: str) -> tuple[str, ...]:
    """DB検索用の銘柄コード候補（4桁/5桁両対応）"""
    return stock_code_candidates(code)


class MarketDataService:
    """market.db 読み取りサービス"""

    def __init__(self, reader: MarketDbReader) -> None:
        self._reader = reader

    @staticmethod
    def _coerce_volume(value: object) -> int:
        """Convert DB volume value to int with safe fallback for null/invalid values."""
        if value is None:
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            try:
                return int(float(str(value)))
            except (TypeError, ValueError):
                return 0

    def get_stock_info(self, code: str) -> StockInfo | None:
        """単一銘柄の情報を取得"""
        codes = _stock_code_candidates(code)
        placeholders = ",".join("?" for _ in codes)
        row = self._reader.query_one(
            "SELECT code, company_name, company_name_english, market_code, market_name, "
            "sector_17_code, sector_17_name, sector_33_code, sector_33_name, "
            f"scale_category, listed_date FROM stocks WHERE code IN ({placeholders}) "
            "ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END LIMIT 1",
            tuple(codes),
        )
        if row is None:
            return None

        return StockInfo(
            code=row["code"],
            companyName=row["company_name"],
            companyNameEnglish=row["company_name_english"] or "",
            marketCode=row["market_code"],
            marketName=row["market_name"],
            sector17Code=row["sector_17_code"],
            sector17Name=row["sector_17_name"],
            sector33Code=row["sector_33_code"],
            sector33Name=row["sector_33_name"],
            scaleCategory=row["scale_category"] or "",
            listedDate=row["listed_date"],
        )

    def get_stock_ohlcv(
        self,
        code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[MarketOHLCVRecord] | None:
        """銘柄の OHLCV データを取得"""
        codes = _stock_code_candidates(code)
        placeholders = ",".join("?" for _ in codes)

        # 銘柄存在確認
        row = self._reader.query_one(
            f"SELECT code FROM stocks WHERE code IN ({placeholders}) "
            "ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END LIMIT 1",
            tuple(codes),
        )
        if row is None:
            return None
        db_code = row["code"]

        sql = "SELECT date, open, high, low, close, volume FROM stock_data WHERE code = ?"
        params: list[str] = [db_code]

        if start_date:
            sql += " AND date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND date <= ?"
            params.append(end_date)

        sql += " ORDER BY date"

        rows = self._reader.query(sql, tuple(params))
        return [
            MarketOHLCVRecord(
                date=row["date"],
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=self._coerce_volume(row["volume"]),
            )
            for row in rows
        ]

    def get_all_stocks(
        self,
        market: str = "prime",
        history_days: int = 300,
    ) -> list[MarketStockData] | None:
        """市場別全銘柄データを取得（スクリーニング用）"""
        _, query_market_codes = resolve_market_codes(market, fallback=["prime"])

        # 最新取引日を取得
        latest_row = self._reader.query_one("SELECT MAX(date) as max_date FROM stock_data")
        if latest_row is None or latest_row["max_date"] is None:
            return []

        latest_date = latest_row["max_date"]

        # history_days 日前の取引日を取得
        start_row = self._reader.query_one(
            "SELECT DISTINCT date FROM stock_data WHERE date < ? ORDER BY date DESC LIMIT 1 OFFSET ?",
            (latest_date, history_days - 1),
        )
        start_date = start_row["date"] if start_row else "1900-01-01"

        # 銘柄一覧
        placeholders = ",".join("?" for _ in query_market_codes)
        stock_rows = self._reader.query(
            f"SELECT code, company_name FROM stocks WHERE market_code IN ({placeholders}) ORDER BY code",
            tuple(query_market_codes),
        )
        if not stock_rows:
            return None

        result: list[MarketStockData] = []
        for stock in stock_rows:
            data_rows = self._reader.query(
                "SELECT date, open, high, low, close, volume FROM stock_data "
                "WHERE code = ? AND date >= ? AND date <= ? ORDER BY date",
                (stock["code"], start_date, latest_date),
            )
            if data_rows:
                result.append(
                    MarketStockData(
                        code=stock["code"],
                        company_name=stock["company_name"],
                        data=[
                            MarketOHLCVRecord(
                                date=r["date"],
                                open=r["open"],
                                high=r["high"],
                                low=r["low"],
                                close=r["close"],
                                volume=self._coerce_volume(r["volume"]),
                            )
                            for r in data_rows
                        ],
                    )
                )

        return result

    def get_topix(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[MarketOHLCRecord] | None:
        """TOPIX データを取得"""
        sql = "SELECT date, open, high, low, close FROM topix_data"
        params: list[str] = []
        conditions: list[str] = []

        if start_date:
            conditions.append("date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("date <= ?")
            params.append(end_date)

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY date"

        rows = self._reader.query(sql, tuple(params))
        if not rows:
            return None

        return [
            MarketOHLCRecord(
                date=row["date"],
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
            )
            for row in rows
        ]
