"""DuckDB-backed chart service."""

from __future__ import annotations

from datetime import UTC, datetime

from src.infrastructure.db.market.query_helpers import stock_code_candidates
from src.infrastructure.db.market.market_reader import MarketDbReadable
from src.application.services.market_code_alias import resolve_market_codes
from src.application.services.market_data_errors import MarketDataError
from src.entrypoints.http.schemas.chart import (
    IndexDataResponse,
    IndexInfo,
    IndexOHLCRecord,
    IndicesListResponse,
    SectorStockItem,
    SectorStocksResponse,
    StockDataPoint,
    StockDataResponse,
    StockSearchResponse,
    StockSearchResultItem,
    TopixDataPoint,
    TopixDataResponse,
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _db_stock_code_candidates(code: str) -> tuple[str, ...]:
    """DB検索用の銘柄コード候補（4桁/5桁両対応）"""
    return stock_code_candidates(code)


def _normalize_middle_dot(text: str) -> str:
    """全角中黒 (・ U+30FB) を半角中黒 (･ U+FF65) に変換"""
    return text.replace("\u30fb", "\uff65")


class ChartService:
    """チャートデータサービス"""

    def __init__(
        self,
        reader: MarketDbReadable | None,
    ) -> None:
        self._reader = reader

    # --- Indices ---

    def get_indices_list(self) -> IndicesListResponse | None:
        """指数一覧を取得（index_master テーブルから）"""
        if self._reader is None:
            return None

        rows = self._reader.query(
            "SELECT code, name, name_english, category, data_start_date FROM index_master ORDER BY code"
        )

        indices = [
            IndexInfo(
                code=row["code"],
                name=row["name"],
                nameEnglish=row["name_english"],
                category=row["category"],
                dataStartDate=row["data_start_date"],
            )
            for row in rows
        ]

        return IndicesListResponse(indices=indices, lastUpdated=_now_iso())

    def get_index_data(self, code: str) -> IndexDataResponse | None:
        """指数チャートデータを取得"""
        if self._reader is None:
            return None

        # 指数メタデータ
        meta = self._reader.query_one(
            "SELECT code, name FROM index_master WHERE code = ? LIMIT 1",
            (code,),
        )
        if meta is None:
            return None

        # OHLC データ（日付昇順）
        rows = self._reader.query(
            "SELECT date, open, high, low, close FROM indices_data WHERE code = ? ORDER BY date",
            (code,),
        )

        data = [
            IndexOHLCRecord(
                date=row["date"],
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
            )
            for row in rows
            if row["open"] is not None and row["close"] is not None
        ]

        return IndexDataResponse(
            code=meta["code"],
            name=meta["name"],
            data=data,
            lastUpdated=_now_iso(),
        )

    # --- TOPIX ---

    async def get_topix_data(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> TopixDataResponse:
        """TOPIX データを DuckDB から取得する。"""
        return self._get_topix_from_db(from_date, to_date)

    def _get_topix_from_db(
        self,
        from_date: str | None,
        to_date: str | None,
    ) -> TopixDataResponse:
        """DuckDB から TOPIX データを取得"""
        if self._reader is None:
            raise MarketDataError(
                "TOPIX のローカルデータがありません",
                reason="topix_data_missing",
                recovery="market_db_sync",
            )

        sql = "SELECT date, open, high, low, close FROM topix_data"
        params: list[str] = []
        conditions: list[str] = []

        if from_date:
            conditions.append("date >= ?")
            params.append(from_date)
        if to_date:
            conditions.append("date <= ?")
            params.append(to_date)

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY date"

        rows = self._reader.query(sql, tuple(params))
        if not rows:
            raise MarketDataError(
                "TOPIX のローカルデータがありません",
                reason="topix_data_missing",
                recovery="market_db_sync",
            )

        topix = [
            TopixDataPoint(
                date=row["date"],
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=0,
            )
            for row in rows
        ]

        return TopixDataResponse(topix=topix, lastUpdated=_now_iso())

    # --- Stock Chart ---

    async def get_stock_data(
        self,
        symbol: str,
        timeframe: str = "daily",
        adjusted: bool = True,
    ) -> StockDataResponse:
        """銘柄チャートデータを DuckDB から取得する。"""
        del adjusted
        return self._get_stock_from_db(symbol, timeframe)

    def _get_stock_from_db(self, symbol: str, timeframe: str) -> StockDataResponse:
        """DuckDB から銘柄データを取得"""
        if self._reader is None:
            raise MarketDataError(
                f"銘柄 {symbol} のローカルOHLCVデータがありません",
                reason="local_stock_data_missing",
                recovery="market_db_sync",
            )

        codes = _db_stock_code_candidates(symbol)
        if not codes:
            raise MarketDataError(
                f"銘柄 {symbol} がローカル市場データに存在しません",
                reason="stock_not_found",
            )
        placeholders = ",".join("?" for _ in codes)

        # 銘柄情報
        stock = self._reader.query_one(
            f"SELECT code, company_name FROM stocks WHERE code IN ({placeholders}) "
            "ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END LIMIT 1",
            tuple(codes),
        )
        resolved_codes = _db_stock_code_candidates(stock["code"]) if stock is not None else codes
        resolved_placeholders = ",".join("?" for _ in resolved_codes)

        # OHLCV データ
        rows = self._reader.query(
            f"""
            WITH ranked AS (
                SELECT
                    date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY date
                        ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
                    ) AS rn
                FROM stock_data
                WHERE code IN ({resolved_placeholders})
            )
            SELECT date, open, high, low, close, volume
            FROM ranked
            WHERE rn = 1
            ORDER BY date
            """,
            tuple(resolved_codes),
        )
        if not rows:
            if stock is None:
                raise MarketDataError(
                    f"銘柄 {symbol} がローカル市場データに存在しません",
                    reason="stock_not_found",
                )
            raise MarketDataError(
                f"銘柄 {symbol} のローカルOHLCVデータがありません",
                reason="local_stock_data_missing",
                recovery="stock_refresh",
            )

        data = [
            StockDataPoint(
                time=row["date"],
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=float(row["volume"]),
            )
            for row in rows
        ]

        return StockDataResponse(
            symbol=symbol,
            companyName=stock["company_name"] if stock is not None else "",
            timeframe=timeframe,
            data=data,
            lastUpdated=_now_iso(),
        )

    def has_stock_metadata(self, symbol: str) -> bool:
        """銘柄マスタに symbol が存在するかを返す。"""
        if self._reader is None:
            return False

        codes = _db_stock_code_candidates(symbol)
        if not codes:
            return False
        placeholders = ",".join("?" for _ in codes)
        row = self._reader.query_one(
            f"SELECT code FROM stocks WHERE code IN ({placeholders}) "
            "ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END LIMIT 1",
            tuple(codes),
        )
        return row is not None

    # --- Stock Search ---

    def search_stocks(self, query: str, limit: int = 20) -> StockSearchResponse:
        """銘柄検索（DuckDB のみ）"""
        if self._reader is None or not query.strip():
            return StockSearchResponse(query=query, results=[], count=0)

        search_term = query.strip()
        search_pattern = f"%{search_term}%"

        rows = self._reader.query(
            """
            SELECT code, company_name, company_name_english, market_code, market_name, sector_33_name,
                CASE
                    WHEN code = ? THEN 1
                    WHEN code LIKE ? THEN 2
                    WHEN company_name LIKE ? THEN 3
                    WHEN company_name_english LIKE ? THEN 4
                    ELSE 5
                END as relevance
            FROM stocks
            WHERE code LIKE ? OR company_name LIKE ? OR company_name_english LIKE ?
            ORDER BY relevance, code
            LIMIT ?
            """,
            (
                search_term,
                f"{search_term}%",
                search_pattern,
                search_pattern,
                search_pattern,
                search_pattern,
                search_pattern,
                limit,
            ),
        )

        results = [
            StockSearchResultItem(
                code=row["code"][:4],
                companyName=row["company_name"],
                companyNameEnglish=row["company_name_english"],
                marketCode=row["market_code"],
                marketName=row["market_name"],
                sector33Name=row["sector_33_name"],
            )
            for row in rows
        ]

        return StockSearchResponse(query=query, results=results, count=len(results))

    # --- Sector Stocks ---

    def get_sector_stocks(
        self,
        sector33_name: str | None = None,
        sector17_name: str | None = None,
        markets: str = "prime,standard",
        lookback_days: int = 5,
        sort_by: str = "tradingValue",
        sort_order: str = "desc",
        limit: int = 100,
    ) -> SectorStocksResponse | None:
        """セクター別銘柄データを取得"""
        if self._reader is None:
            return None

        # 最新取引日
        latest_row = self._reader.query_one("SELECT MAX(date) as max_date FROM stock_data")
        if latest_row is None or latest_row["max_date"] is None:
            return None

        latest_date = latest_row["max_date"]

        # N日前の取引日
        base_row = self._reader.query_one(
            "SELECT DISTINCT date FROM stock_data WHERE date < ? ORDER BY date DESC LIMIT 1 OFFSET ?",
            (latest_date, lookback_days - 1),
        )
        base_date = base_row["date"] if base_row else None

        # 15日平均売買代金の基準日
        tv_base_row = self._reader.query_one(
            "SELECT DISTINCT date FROM stock_data WHERE date < ? ORDER BY date DESC LIMIT 1 OFFSET ?",
            (latest_date, 14),
        )
        tv_base_date = tv_base_row["date"] if tv_base_row else latest_date

        requested_market_codes, query_market_codes = resolve_market_codes(
            markets,
            fallback=["prime", "standard"],
        )

        # セクター名の中黒正規化
        norm_s33 = _normalize_middle_dot(sector33_name) if sector33_name else None
        norm_s17 = _normalize_middle_dot(sector17_name) if sector17_name else None

        # SQL 構築
        conditions = ["curr.date = ?"]

        # サブクエリパラメータ（SELECT句内）
        sub_params: list[str] = [tv_base_date, latest_date]

        # JOIN パラメータ
        join_params: list[str] = []
        if base_date:
            join_params.append(base_date)

        # WHERE パラメータ
        where_params: list[str | int] = [latest_date]

        if norm_s33:
            conditions.append("s.sector_33_name = ?")
            where_params.append(norm_s33)
        if norm_s17:
            conditions.append("s.sector_17_name = ?")
            where_params.append(norm_s17)
        if query_market_codes:
            placeholders = ",".join("?" for _ in query_market_codes)
            conditions.append(f"s.market_code IN ({placeholders})")
            where_params.extend(query_market_codes)

        # ソート
        order_map = {
            ("tradingValue", "asc"): "ORDER BY trading_value ASC",
            ("tradingValue", "desc"): "ORDER BY trading_value DESC",
            ("changePercentage", "asc"): "ORDER BY change_percentage ASC NULLS LAST",
            ("changePercentage", "desc"): "ORDER BY change_percentage DESC NULLS LAST",
            ("code", "asc"): "ORDER BY s.code ASC",
            ("code", "desc"): "ORDER BY s.code DESC",
        }
        order_clause = order_map.get((sort_by, sort_order), "ORDER BY trading_value DESC")

        tv_subquery = """(
            SELECT AVG(sd.close * sd.volume)
            FROM stock_data sd
            WHERE sd.code = curr.code AND sd.date > ? AND sd.date <= ?
        )"""

        base_join = "LEFT JOIN stock_data base ON curr.code = base.code AND base.date = ?" if base_date else ""

        if base_date:
            select_fields = f"""s.code, s.company_name, s.market_code, s.sector_33_name,
                curr.close as current_price, curr.volume,
                {tv_subquery} as trading_value, base.close as base_price,
                CASE WHEN base.close > 0 THEN (curr.close - base.close) ELSE NULL END as change_amount,
                CASE WHEN base.close > 0 THEN ((curr.close - base.close) / base.close * 100) ELSE NULL END as change_percentage"""
        else:
            select_fields = f"""s.code, s.company_name, s.market_code, s.sector_33_name,
                curr.close as current_price, curr.volume,
                {tv_subquery} as trading_value"""

        sql = f"""SELECT {select_fields}
            FROM stock_data curr
            JOIN stocks s ON s.code = curr.code
            {base_join}
            WHERE {' AND '.join(conditions)}
            {order_clause}
            LIMIT ?"""

        # パラメータ順: サブクエリ → JOIN → WHERE → LIMIT
        all_params: list[str | int] = [*sub_params, *join_params, *where_params, limit]

        rows = self._reader.query(sql, tuple(all_params))

        stocks = [
            SectorStockItem(
                rank=i + 1,
                code=row["code"],
                companyName=row["company_name"],
                marketCode=row["market_code"],
                sector33Name=row["sector_33_name"],
                currentPrice=row["current_price"],
                volume=row["volume"],
                tradingValue=row["trading_value"],
                tradingValueAverage=row["trading_value"],
                basePrice=row["base_price"] if base_date else None,
                changeAmount=row["change_amount"] if base_date else None,
                changePercentage=row["change_percentage"] if base_date else None,
                lookbackDays=lookback_days,
            )
            for i, row in enumerate(rows)
        ]

        return SectorStocksResponse(
            sector33Name=sector33_name,
            sector17Name=sector17_name,
            markets=requested_market_codes,
            lookbackDays=lookback_days,
            sortBy=sort_by,
            sortOrder=sort_order,
            stocks=stocks,
            lastUpdated=_now_iso(),
        )
