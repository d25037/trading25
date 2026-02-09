"""
Chart Service

チャートデータの提供サービス。market.db + JQuants fallback。
Hono chart/indices, chart/stocks 系ルートと同等のロジック。
"""

from __future__ import annotations

from datetime import UTC, datetime

from src.server.clients.jquants_client import JQuantsAsyncClient
from src.lib.market_db.query_helpers import expand_stock_code, stock_code_candidates
from src.lib.market_db.market_reader import MarketDbReader
from src.server.schemas.chart import (
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


def _api_stock_code(code: str) -> str:
    """JQuants API向けに銘柄コードを正規化"""
    return expand_stock_code(code)


def _normalize_middle_dot(text: str) -> str:
    """全角中黒 (・ U+30FB) を半角中黒 (･ U+FF65) に変換"""
    return text.replace("\u30fb", "\uff65")


class ChartService:
    """チャートデータサービス"""

    def __init__(
        self,
        reader: MarketDbReader | None,
        jquants_client: JQuantsAsyncClient,
    ) -> None:
        self._reader = reader
        self._jquants = jquants_client

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
    ) -> TopixDataResponse | None:
        """TOPIX データを取得（market.db → JQuants fallback）"""
        # market.db を試行
        if self._reader is not None:
            data = self._get_topix_from_db(from_date, to_date)
            if data is not None:
                return data

        # JQuants fallback
        return await self._get_topix_from_jquants(from_date, to_date)

    def _get_topix_from_db(
        self,
        from_date: str | None,
        to_date: str | None,
    ) -> TopixDataResponse | None:
        """market.db から TOPIX データを取得"""
        if self._reader is None:
            return None

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
            return None

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

    async def _get_topix_from_jquants(
        self,
        from_date: str | None,
        to_date: str | None,
    ) -> TopixDataResponse | None:
        """JQuants API から TOPIX データを取得"""
        params: dict[str, str] = {}
        if from_date:
            params["from"] = from_date.replace("-", "")
        if to_date:
            params["to"] = to_date.replace("-", "")

        try:
            body = await self._jquants.get("/indices/bars/daily/topix", params)
            raw = body.get("data", [])
        except Exception:
            return None

        if not raw:
            return None

        topix = [
            TopixDataPoint(
                date=item.get("Date", ""),
                open=float(item.get("O", 0) or 0),
                high=float(item.get("H", 0) or 0),
                low=float(item.get("L", 0) or 0),
                close=float(item.get("C", 0) or 0),
                volume=0,
            )
            for item in raw
            if item.get("C") is not None
        ]

        return TopixDataResponse(topix=topix, lastUpdated=_now_iso())

    # --- Stock Chart ---

    async def get_stock_data(
        self,
        symbol: str,
        timeframe: str = "daily",
        adjusted: bool = True,
    ) -> StockDataResponse | None:
        """銘柄チャートデータを取得（market.db → JQuants fallback）"""
        # market.db を試行
        if self._reader is not None:
            data = self._get_stock_from_db(symbol, timeframe)
            if data is not None:
                return data

        # JQuants fallback
        return await self._get_stock_from_jquants(symbol, timeframe, adjusted)

    def _get_stock_from_db(self, symbol: str, timeframe: str) -> StockDataResponse | None:
        """market.db から銘柄データを取得"""
        if self._reader is None:
            return None

        codes = _db_stock_code_candidates(symbol)
        placeholders = ",".join("?" for _ in codes)

        # 銘柄情報
        stock = self._reader.query_one(
            f"SELECT code, company_name FROM stocks WHERE code IN ({placeholders}) "
            "ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END LIMIT 1",
            tuple(codes),
        )
        if stock is None:
            return None
        db_code = stock["code"]

        # OHLCV データ
        rows = self._reader.query(
            "SELECT date, open, high, low, close, volume FROM stock_data WHERE code = ? ORDER BY date",
            (db_code,),
        )
        if not rows:
            return None

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
            companyName=stock["company_name"],
            timeframe=timeframe,
            data=data,
            lastUpdated=_now_iso(),
        )

    async def _get_stock_from_jquants(
        self,
        symbol: str,
        timeframe: str,
        adjusted: bool,
    ) -> StockDataResponse | None:
        """JQuants API から銘柄データを取得"""
        code5 = _api_stock_code(symbol)

        try:
            body = await self._jquants.get("/equities/bars/daily", {"code": code5})
            raw = body.get("data", [])
        except Exception:
            return None

        if not raw:
            return None

        # 会社名の取得
        company_name = ""
        try:
            info_body = await self._jquants.get("/equities/master", {"code": code5})
            info_list = info_body.get("data", [])
            if info_list:
                company_name = info_list[0].get("CoName", "")
        except Exception:
            pass

        data: list[StockDataPoint] = []
        for item in raw:
            if adjusted and item.get("AdjC") is not None:
                o = float(item.get("AdjO") or 0)
                h = float(item.get("AdjH") or 0)
                lo = float(item.get("AdjL") or 0)
                c = float(item.get("AdjC") or 0)
                v = float(item.get("AdjVo") or 0)
            else:
                o = float(item.get("O", 0) or 0)
                h = float(item.get("H", 0) or 0)
                lo = float(item.get("L", 0) or 0)
                c = float(item.get("C", 0) or 0)
                v = float(item.get("Vo", 0) or 0)

            if c <= 0:
                continue

            data.append(
                StockDataPoint(time=item.get("Date", ""), open=o, high=h, low=lo, close=c, volume=v)
            )

        if not data:
            return None

        return StockDataResponse(
            symbol=symbol,
            companyName=company_name,
            timeframe=timeframe,
            data=data,
            lastUpdated=_now_iso(),
        )

    # --- Stock Search ---

    def search_stocks(self, query: str, limit: int = 20) -> StockSearchResponse:
        """銘柄検索（market.db のみ）"""
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

        market_codes = [m.strip() for m in markets.split(",")]

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
        if market_codes:
            placeholders = ",".join("?" for _ in market_codes)
            conditions.append(f"s.market_code IN ({placeholders})")
            where_params.extend(market_codes)

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
            markets=market_codes,
            lookbackDays=lookback_days,
            sortBy=sort_by,
            sortOrder=sort_order,
            stocks=stocks,
            lastUpdated=_now_iso(),
        )
