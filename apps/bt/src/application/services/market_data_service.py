"""
Market Data Service

DuckDB market time-series から株式・TOPIX データを読み取るサービス。
Hono market-data-service.ts と同等のロジック。
"""

from __future__ import annotations

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.application.services.market_data_errors import MarketDataError
from src.application.services.options_225 import build_options_225_response
from src.infrastructure.db.market.query_helpers import stock_code_candidates
from src.shared.utils.market_code_alias import resolve_market_codes
from src.entrypoints.http.schemas.market_data import (
    MarketMinuteBarRecord,
    MarketOHLCRecord,
    MarketOHLCVRecord,
    MarketStockData,
    StockInfo,
)
from src.entrypoints.http.schemas.jquants import N225OptionsExplorerResponse


def _stock_code_candidates(code: str) -> tuple[str, ...]:
    """DB検索用の銘柄コード候補（4桁/5桁両対応）"""
    return stock_code_candidates(code)


class MarketDataService:
    """DuckDB market data 読み取りサービス"""

    def __init__(self, reader: MarketDbReader) -> None:
        self._reader = reader

    @staticmethod
    def _coerce_volume(value: object) -> int:
        """Convert DB volume value to int with safe fallback for null/invalid values."""
        if value is None:
            return 0

        if isinstance(value, int):
            return value

        if isinstance(value, float):
            return int(value)

        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                try:
                    return int(float(value))
                except ValueError:
                    return 0

        return 0

    def _table_exists(self, table_name: str) -> bool:
        try:
            row = self._reader.query_one(
                """
                SELECT 1 AS exists
                FROM information_schema.tables
                WHERE lower(table_name) = lower(?)
                LIMIT 1
                """,
                (table_name,),
            )
        except Exception:
            return False
        return row is not None

    def _table_has_rows(self, table_name: str) -> bool:
        if not self._table_exists(table_name):
            return False
        try:
            row = self._reader.query_one(f"SELECT 1 AS has_rows FROM {table_name} LIMIT 1")
        except Exception:
            return False
        return row is not None

    def get_stock_info(self, code: str, as_of_date: str | None = None) -> StockInfo | None:
        """単一銘柄の情報を取得"""
        codes = _stock_code_candidates(code)
        placeholders = ",".join("?" for _ in codes)
        if as_of_date and self._table_exists("stock_master_daily"):
            source_table = "stock_master_daily"
        elif self._table_has_rows("stocks_latest"):
            source_table = "stocks_latest"
        else:
            # Legacy/unit-test DBs predating v3 still expose only stocks.
            source_table = "stocks"
        date_clause = "date = ? AND " if as_of_date else ""
        if as_of_date and source_table == "stock_master_daily":
            params = (as_of_date, *codes)
        else:
            date_clause = ""
            params = tuple(codes)
        row = self._reader.query_one(
            "SELECT code, company_name, company_name_english, market_code, market_name, "
            "sector_17_code, sector_17_name, sector_33_code, sector_33_name, "
            f"scale_category, listed_date FROM {source_table} WHERE {date_clause}code IN ({placeholders}) "
            "ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END LIMIT 1",
            params,
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

        sql = f"""
            SELECT date, open, high, low, close, volume
            FROM (
                SELECT
                    date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY date
                        ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                    ) AS rn
                FROM stock_data
                WHERE code IN ({placeholders})
        """
        params: list[str] = list(codes)

        if start_date:
            sql += " AND date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND date <= ?"
            params.append(end_date)

        sql += """
            )
            WHERE rn = 1
            ORDER BY date
        """

        rows = self._reader.query(sql, tuple(params))
        if not rows:
            row = self._reader.query_one(
                f"SELECT code FROM stocks WHERE code IN ({placeholders}) "
                "ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END LIMIT 1",
                tuple(codes),
            )
            if row is None:
                row = self._reader.query_one(
                    f"SELECT code FROM stock_data WHERE code IN ({placeholders}) LIMIT 1",
                    tuple(codes),
                )
            if row is None:
                return None

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

    def get_stock_minute_bars(
        self,
        code: str,
        *,
        date: str,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[MarketMinuteBarRecord] | None:
        """銘柄の分足データを取得"""
        codes = _stock_code_candidates(code)
        placeholders = ",".join("?" for _ in codes)

        sql = (
            "SELECT code, date, time, open, high, low, close, volume, turnover_value "
            f"FROM stock_data_minute_raw WHERE code IN ({placeholders}) AND date = ?"
        )
        params: list[str] = [*codes, date]

        if start_time:
            sql += " AND time >= ?"
            params.append(start_time)
        if end_time:
            sql += " AND time <= ?"
            params.append(end_time)

        sql += " ORDER BY time, CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"
        rows = self._reader.query(sql, tuple(params))
        if rows:
            by_time: dict[str, MarketMinuteBarRecord] = {}
            for row in rows:
                time_value = str(row["time"])
                by_time.setdefault(
                    time_value,
                    MarketMinuteBarRecord(
                        date=str(row["date"]),
                        time=time_value,
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        volume=self._coerce_volume(row["volume"]),
                        turnoverValue=(
                            float(row["turnover_value"])
                            if row["turnover_value"] is not None
                            else None
                        ),
                    ),
                )
            return [by_time[key] for key in sorted(by_time)]

        minute_row = self._reader.query_one(
            f"SELECT code FROM stock_data_minute_raw WHERE code IN ({placeholders}) "
            "ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END LIMIT 1",
            tuple(codes),
        )
        if minute_row is not None:
            return []

        stock_row = self._reader.query_one(
            f"SELECT code FROM stocks WHERE code IN ({placeholders}) "
            "ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END LIMIT 1",
            tuple(codes),
        )
        if stock_row is None:
            return None
        return []

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
        stock_table = "stocks_latest" if self._table_has_rows("stocks_latest") else "stocks"
        stock_rows = self._reader.query(
            f"SELECT code, company_name FROM {stock_table} WHERE market_code IN ({placeholders}) ORDER BY code",
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

    def get_options_225(self, date: str | None = None) -> N225OptionsExplorerResponse:
        """日経225オプション四本値 explorer データを DuckDB から取得。"""
        table_exists = self._reader.query_one(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'options_225_data'
            LIMIT 1
            """
        )
        if table_exists is None:
            raise MarketDataError(
                "日経225オプションのローカルデータがありません",
                reason="options_225_data_missing",
                recovery="market_db_sync",
            )

        resolved_date = date
        if resolved_date is None:
            latest_row = self._reader.query_one("SELECT MAX(date) AS max_date FROM options_225_data")
            resolved_date = latest_row["max_date"] if latest_row is not None else None

        if not resolved_date:
            raise MarketDataError(
                "日経225オプションのローカルデータがありません",
                reason="options_225_data_missing",
                recovery="market_db_sync",
            )

        rows = self._reader.query(
            """
            SELECT
                date,
                code,
                whole_day_open,
                whole_day_high,
                whole_day_low,
                whole_day_close,
                night_session_open,
                night_session_high,
                night_session_low,
                night_session_close,
                day_session_open,
                day_session_high,
                day_session_low,
                day_session_close,
                volume,
                open_interest,
                turnover_value,
                contract_month,
                strike_price,
                only_auction_volume,
                emergency_margin_trigger_division,
                put_call_division,
                last_trading_day,
                special_quotation_day,
                settlement_price,
                theoretical_price,
                base_volatility,
                underlying_price,
                implied_volatility,
                interest_rate
            FROM options_225_data
            WHERE date = ?
            ORDER BY contract_month NULLS LAST, strike_price NULLS LAST, code
            """,
            (resolved_date,),
        )
        if not rows:
            raise MarketDataError(
                f"日経225オプションのローカルデータが {resolved_date} にありません",
                reason="options_225_data_missing",
                recovery="market_db_sync",
            )

        normalized_rows = [dict(row.items()) for row in rows]
        return build_options_225_response(
            requested_date=date,
            resolved_date=str(resolved_date),
            normalized_rows=normalized_rows,
            source_call_count=0,
        )
