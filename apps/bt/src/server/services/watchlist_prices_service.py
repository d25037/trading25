"""
Watchlist Prices Service

ウォッチリスト銘柄の最新価格と前日比を算出する。
Hono watchlist/prices EP 互換。
"""

from __future__ import annotations

from src.server.db.market_reader import MarketDbReader
from src.server.db.portfolio_db import PortfolioDb
from src.server.schemas.portfolio_performance import WatchlistPricesResponse, WatchlistStockPrice


class WatchlistPricesService:
    """ウォッチリスト銘柄の最新価格を取得"""

    def __init__(self, reader: MarketDbReader, portfolio_db: PortfolioDb) -> None:
        self._reader = reader
        self._pdb = portfolio_db

    def get_prices(self, watchlist_id: int) -> WatchlistPricesResponse:
        """ウォッチリスト内銘柄の最新価格を取得"""
        watchlist = self._pdb.get_watchlist(watchlist_id)
        if watchlist is None:
            raise ValueError(f"Watchlist {watchlist_id} not found")

        items = self._pdb.list_watchlist_items(watchlist_id)
        if not items:
            return WatchlistPricesResponse(prices=[])

        # 4桁コードを5桁に変換してDB検索
        codes_4 = [item.code for item in items]
        codes_5 = [f"{c}0" for c in codes_4]

        # 各銘柄の直近2日分の終値を一括取得
        placeholders = ",".join("?" for _ in codes_5)
        rows = self._reader.query(
            f"""
            SELECT code, date, close, volume
            FROM stock_data
            WHERE code IN ({placeholders})
            AND date IN (
                SELECT DISTINCT date FROM stock_data
                WHERE code IN ({placeholders})
                ORDER BY date DESC LIMIT 2
            )
            ORDER BY code, date DESC
            """,
            tuple(codes_5) + tuple(codes_5),
        )

        # code -> [(date, close, volume), ...] 最新2件
        price_map: dict[str, list[tuple[str, float, int]]] = {}
        for r in rows:
            code = r["code"]
            if code not in price_map:
                price_map[code] = []
            if len(price_map[code]) < 2:
                price_map[code].append((r["date"], r["close"], r["volume"]))

        prices: list[WatchlistStockPrice] = []
        for code4, code5 in zip(codes_4, codes_5):
            entries = price_map.get(code5, [])
            if not entries:
                continue
            latest = entries[0]
            close = latest[1]
            volume = latest[2]
            date = latest[0]
            prev_close: float | None = None
            change_percent: float | None = None
            if len(entries) >= 2:
                prev_close = entries[1][1]
                if prev_close and prev_close > 0:
                    change_percent = round((close - prev_close) / prev_close * 100, 2)
            prices.append(
                WatchlistStockPrice(
                    code=code4,
                    close=close,
                    prevClose=prev_close,
                    changePercent=change_percent,
                    volume=volume,
                    date=date,
                )
            )

        return WatchlistPricesResponse(prices=prices)
