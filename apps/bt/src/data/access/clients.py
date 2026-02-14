"""Mode-aware data clients for loader paths.

HTTP mode uses existing API clients.
Direct mode bypasses internal HTTP and reads SQLite via DatasetDb/MarketDbReader.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from src.api.dataset.helpers import (
    convert_dated_response,
    convert_index_response,
    convert_ohlcv_response,
)
from src.api.dataset_client import DatasetAPIClient
from src.api.market_client import MarketAPIClient
from src.config.settings import get_settings
from src.lib.market_db.dataset_db import DatasetDb
from src.lib.market_db.market_reader import MarketDbReader

from .mode import should_use_direct_db

_dataset_db_cache: dict[str, DatasetDb] = {}
_dataset_db_lock = threading.Lock()

_market_reader: MarketDbReader | None = None
_market_reader_lock = threading.Lock()


def _rows_to_records(
    rows: list[Any],
    field_map: dict[str, str],
) -> list[dict[str, Any]]:
    return [
        {output_field: getattr(row, source_attr) for output_field, source_attr in field_map.items()}
        for row in rows
    ]


def _resolve_dataset_db(dataset_name: str) -> DatasetDb:
    settings = get_settings()
    stem = Path(dataset_name).stem
    db_path = Path(settings.dataset_base_path) / f"{stem}.db"
    cache_key = str(db_path.resolve())

    with _dataset_db_lock:
        db = _dataset_db_cache.get(cache_key)
        if db is None:
            if not db_path.exists():
                raise FileNotFoundError(f"Dataset not found: {db_path}")
            db = DatasetDb(str(db_path))
            _dataset_db_cache[cache_key] = db
        return db


def _resolve_market_reader() -> MarketDbReader:
    global _market_reader
    settings = get_settings()
    market_db_path = Path(settings.market_db_path)
    if not market_db_path.exists():
        raise FileNotFoundError(f"market.db not found: {market_db_path}")

    with _market_reader_lock:
        if _market_reader is None:
            _market_reader = MarketDbReader(str(market_db_path))
        return _market_reader


def _to_ohlcv_df(rows: list[Any]) -> pd.DataFrame:
    records = _rows_to_records(
        rows,
        {
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        },
    )
    return convert_ohlcv_response(records)


def _to_ohlc_df(rows: list[Any]) -> pd.DataFrame:
    records = _rows_to_records(
        rows,
        {
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
        },
    )
    return convert_index_response(records)


def _to_margin_df(rows: list[Any]) -> pd.DataFrame:
    records = _rows_to_records(
        rows,
        {
            "date": "date",
            "longMarginVolume": "long_margin_volume",
            "shortMarginVolume": "short_margin_volume",
        },
    )
    return convert_dated_response(records)


def _to_statements_df(rows: list[Any]) -> pd.DataFrame:
    records = _rows_to_records(
        rows,
        {
            "code": "code",
            "disclosedDate": "disclosed_date",
            "earningsPerShare": "earnings_per_share",
            "profit": "profit",
            "equity": "equity",
            "typeOfCurrentPeriod": "type_of_current_period",
            "typeOfDocument": "type_of_document",
            "nextYearForecastEarningsPerShare": "next_year_forecast_earnings_per_share",
            "bps": "bps",
            "sales": "sales",
            "operatingProfit": "operating_profit",
            "ordinaryProfit": "ordinary_profit",
            "operatingCashFlow": "operating_cash_flow",
            "dividendFY": "dividend_fy",
            "forecastEps": "forecast_eps",
            "investingCashFlow": "investing_cash_flow",
            "financingCashFlow": "financing_cash_flow",
            "cashAndEquivalents": "cash_and_equivalents",
            "totalAssets": "total_assets",
            "sharesOutstanding": "shares_outstanding",
            "treasuryShares": "treasury_shares",
        },
    )
    return convert_dated_response(records, date_column="disclosedDate")


class DirectDatasetClient:
    """Dataset client backed by DatasetDb (no HTTP)."""

    def __init__(self, dataset_name: str) -> None:
        self.dataset_name = Path(dataset_name).stem
        self._db = _resolve_dataset_db(self.dataset_name)

    def __enter__(self) -> DirectDatasetClient:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: Any,
    ) -> None:
        # Cached DB stays open for reuse.
        return None

    def get_stock_ohlcv(
        self,
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        timeframe: Literal["daily", "weekly", "monthly"] = "daily",
    ) -> pd.DataFrame:
        _ = timeframe  # Current HTTP route ignores timeframe for dataset endpoint.
        return _to_ohlcv_df(
            self._db.get_stock_ohlcv(stock_code, start=start_date, end=end_date)
        )

    def get_stocks_ohlcv_batch(
        self,
        stock_codes: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        timeframe: Literal["daily", "weekly", "monthly"] = "daily",
    ) -> dict[str, pd.DataFrame]:
        _ = (start_date, end_date, timeframe)
        batch = self._db.get_ohlcv_batch(stock_codes)
        return {code: _to_ohlcv_df(rows) for code, rows in batch.items() if rows}

    def get_stock_list(
        self,
        min_records: int = 100,
        limit: int | None = None,
        detail: bool = False,
    ) -> pd.DataFrame:
        _ = detail
        rows = self._db.get_stock_list_with_counts(min_records=min_records)
        df = pd.DataFrame(
            [
                {
                    "stockCode": row.stockCode,
                    "record_count": row.record_count,
                    "start_date": row.start_date,
                    "end_date": row.end_date,
                }
                for row in rows
            ]
        )
        if limit is not None and not df.empty:
            return df.head(limit)
        return df

    def get_available_stocks(self, min_records: int = 100) -> pd.DataFrame:
        return self.get_stock_list(min_records=min_records, detail=True)

    def get_topix(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        return _to_ohlc_df(self._db.get_topix(start=start_date, end=end_date))

    def get_index(
        self,
        index_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        return _to_ohlc_df(
            self._db.get_index_data(index_code, start=start_date, end=end_date)
        )

    def get_index_list(
        self,
        min_records: int = 100,
        codes: list[str] | None = None,
    ) -> pd.DataFrame:
        rows = self._db.get_index_list_with_counts(min_records=min_records)
        df = pd.DataFrame(
            [
                {
                    "indexCode": row.indexCode,
                    "indexName": row.indexName,
                    "record_count": row.record_count,
                    "start_date": row.start_date,
                    "end_date": row.end_date,
                }
                for row in rows
            ]
        )
        if codes and not df.empty:
            code_set = set(codes)
            df = pd.DataFrame(
                [record for record in df.to_dict(orient="records") if record["indexCode"] in code_set]
            )
        return df

    def get_margin(
        self,
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        rows = self._db.get_margin(code=stock_code, start=start_date, end=end_date)
        return _to_margin_df(rows)

    def get_margin_batch(
        self,
        stock_codes: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        _ = (start_date, end_date)
        batch = self._db.get_margin_batch(stock_codes)
        return {code: _to_margin_df(rows) for code, rows in batch.items() if rows}

    def get_margin_list(
        self,
        min_records: int = 10,
        codes: list[str] | None = None,
    ) -> pd.DataFrame:
        rows = self._db.get_margin_list(min_records=min_records)
        df = pd.DataFrame(
            [
                {
                    "stockCode": row.stockCode,
                    "record_count": row.record_count,
                    "start_date": row.start_date,
                    "end_date": row.end_date,
                    "avg_long_margin": row.avg_long_margin,
                    "avg_short_margin": row.avg_short_margin,
                }
                for row in rows
            ]
        )
        if codes and not df.empty:
            code_set = set(codes)
            df = pd.DataFrame(
                [record for record in df.to_dict(orient="records") if record["stockCode"] in code_set]
            )
        return df

    def get_statements(
        self,
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        period_type: str = "all",
        actual_only: bool = True,
    ) -> pd.DataFrame:
        return _to_statements_df(
            self._db.get_statements(
                stock_code,
                start=start_date,
                end=end_date,
                period_type=period_type,
                actual_only=actual_only,
            )
        )

    def get_statements_batch(
        self,
        stock_codes: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        period_type: str = "all",
        actual_only: bool = True,
    ) -> dict[str, pd.DataFrame]:
        batch = self._db.get_statements_batch(
            stock_codes,
            start=start_date,
            end=end_date,
            period_type=period_type,
            actual_only=actual_only,
        )
        return {code: _to_statements_df(rows) for code, rows in batch.items() if rows}

    def get_sector_mapping(self) -> pd.DataFrame:
        sectors = self._db.get_sectors()
        indices = self._db.get_indices()
        sector_code_by_name = {item["name"]: item["code"] for item in sectors}

        records = [
            {
                "sector_code": sector_code_by_name.get(row.sector_name),
                "sector_name": row.sector_name,
                "index_code": row.code,
                "index_name": row.sector_name,
            }
            for row in indices
        ]
        return pd.DataFrame(records)

    def get_stock_sector_mapping(self) -> dict[str, str]:
        sector_to_stocks = self._db.get_sector_stock_mapping()
        result: dict[str, str] = {}
        for sector_name, codes in sector_to_stocks.items():
            for code in codes:
                result[code] = sector_name
        return result

    def get_sector_stocks(self, sector_name: str) -> list[str]:
        rows = self._db.get_sector_stocks(sector_name)
        return [row.code for row in rows]

    def get_all_sectors(self) -> pd.DataFrame:
        mapping_df = self.get_sector_mapping()
        counts = {row.sectorName: row.count for row in self._db.get_sectors_with_count()}

        if mapping_df.empty:
            return pd.DataFrame()

        mapping_df = mapping_df.copy()
        mapping_df["stock_count"] = mapping_df["sector_name"].map(counts).fillna(0).astype(int)
        return mapping_df


class DirectMarketClient:
    """Market client backed by market.db reader (no HTTP)."""

    def __enter__(self) -> DirectMarketClient:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: Any,
    ) -> None:
        return None

    def get_topix(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        reader = _resolve_market_reader()
        sql = "SELECT date, open, high, low, close FROM topix_data"
        params: list[str] = []
        conds: list[str] = []
        if start_date:
            conds.append("date >= ?")
            params.append(start_date)
        if end_date:
            conds.append("date <= ?")
            params.append(end_date)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY date"

        rows = reader.query(sql, tuple(params))
        records = [
            {
                "date": row["date"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
            }
            for row in rows
        ]
        return convert_index_response(records)


def _create_http_dataset_client(dataset_name: str) -> DatasetAPIClient:
    return DatasetAPIClient(dataset_name)


def _create_http_market_client() -> MarketAPIClient:
    return MarketAPIClient()


def get_dataset_client(dataset_name: str) -> DatasetAPIClient | DirectDatasetClient:
    """Return HTTP or direct dataset client based on active data-access mode."""
    if should_use_direct_db():
        return DirectDatasetClient(dataset_name)
    return _create_http_dataset_client(dataset_name)


def get_market_client() -> MarketAPIClient | DirectMarketClient:
    """Return HTTP or direct market client based on active data-access mode."""
    if should_use_direct_db():
        return DirectMarketClient()
    return _create_http_market_client()
