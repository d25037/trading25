"""
Watchlist Prices Service

ウォッチリスト銘柄の最新価格と前日比を算出する。
Hono watchlist/prices EP 互換。
"""

from __future__ import annotations

from src.server.db.query_helpers import stock_code_candidates
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

        prices: list[WatchlistStockPrice] = []
        for item in items:
            code4 = item.code
            candidates = stock_code_candidates(code4)
            placeholders = ",".join("?" for _ in candidates)
            rows = self._reader.query(
                f"""
                SELECT date, close, volume
                FROM stock_data
                WHERE code IN ({placeholders})
                ORDER BY date DESC, CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
                """,
                tuple(candidates),
            )

            # 同日の4桁/5桁重複がある場合は先頭（4桁優先）を採用
            entries: list[tuple[str, float, int]] = []
            seen_dates: set[str] = set()
            for row in rows:
                d = row["date"]
                if d in seen_dates:
                    continue
                seen_dates.add(d)
                entries.append((d, row["close"], row["volume"]))
                if len(entries) >= 2:
                    break

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
