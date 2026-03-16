"""Mode-aware data clients for loader paths.

HTTP mode uses existing API clients.
Direct mode bypasses internal HTTP and reads local DuckDB snapshots directly.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from src.infrastructure.db.market.dataset_snapshot_reader import DatasetSnapshotReader
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.infrastructure.db.market.query_helpers import stock_code_candidates
from src.infrastructure.external_api.jquants_client import StockInfo
from src.infrastructure.external_api.dataset.helpers import (
    convert_dated_response,
    convert_index_response,
    convert_ohlcv_response,
)
from src.infrastructure.external_api.dataset_client import DatasetAPIClient
from src.infrastructure.external_api.market_client import MarketAPIClient
from src.shared.config.settings import get_settings
from src.shared.models.types import normalize_period_type
from src.shared.utils.snapshot_ids import (
    normalize_dataset_snapshot_name,
    normalize_market_snapshot_id,
)

from .mode import should_use_direct_db

_dataset_db_cache: dict[str, DatasetSnapshotReader] = {}
_dataset_db_lock = threading.Lock()

_market_reader_cache: dict[str, MarketDbReader] = {}
_market_reader_lock = threading.Lock()

_STATEMENTS_ACTUAL_ONLY_COLUMNS = (
    "earnings_per_share",
    "profit",
    "equity",
)


def close_all_cached_data_access_clients() -> None:
    """Close process-global direct-mode caches."""

    with _dataset_db_lock:
        dataset_dbs = list(_dataset_db_cache.values())
        _dataset_db_cache.clear()

    with _market_reader_lock:
        market_readers = list(_market_reader_cache.values())
        _market_reader_cache.clear()

    for db in dataset_dbs:
        close = getattr(db, "close", None)
        if callable(close):
            close()
    for reader in market_readers:
        close = getattr(reader, "close", None)
        if callable(close):
            close()


def _resolve_statements_period_filter_values(period_type: str) -> tuple[str, ...]:
    normalized = normalize_period_type(period_type) or "all"
    if normalized == "all":
        return ()
    if normalized == "1Q":
        return ("1Q", "Q1")
    if normalized == "2Q":
        return ("2Q", "Q2")
    if normalized == "3Q":
        return ("3Q", "Q3")
    return (normalized,)


def _rows_to_records(
    rows: list[Any],
    field_map: dict[str, str],
) -> list[dict[str, Any]]:
    return [
        {
            output_field: getattr(row, source_attr, None)
            for output_field, source_attr in field_map.items()
        }
        for row in rows
    ]


def _resolve_dataset_artifact(dataset_name: str) -> tuple[Literal["duckdb-parquet"], str, str]:
    normalized_dataset_name = normalize_dataset_snapshot_name(dataset_name)
    if normalized_dataset_name is None:
        raise FileNotFoundError(f"Dataset not found: {dataset_name}")

    settings = get_settings()
    dataset_root = Path(str(getattr(settings, "dataset_base_path", "") or "")).resolve()
    snapshot_root = dataset_root / normalized_dataset_name
    duckdb_path = snapshot_root / "dataset.duckdb"
    manifest_path = snapshot_root / "manifest.v2.json"

    if duckdb_path.exists() and manifest_path.exists():
        return "duckdb-parquet", str(snapshot_root.resolve()), str(duckdb_path.resolve())

    raise FileNotFoundError(f"Dataset not found: {dataset_name}")


def _resolve_dataset_db(dataset_name: str) -> DatasetSnapshotReader:
    _backend, snapshot_root, primary_path = _resolve_dataset_artifact(dataset_name)
    cache_key = primary_path
    with _dataset_db_lock:
        db = _dataset_db_cache.get(cache_key)
        if db is None:
            db = DatasetSnapshotReader(snapshot_root)
            _dataset_db_cache[cache_key] = db
        return db


def _resolve_market_reader(snapshot_id: str | None = None) -> MarketDbReader:
    normalized_snapshot_id = normalize_market_snapshot_id(snapshot_id)
    settings = get_settings()
    market_timeseries_dir = str(getattr(settings, "market_timeseries_dir", "") or "").strip()
    if not market_timeseries_dir:
        raise FileNotFoundError("MARKET_TIMESERIES_DIR is not configured")

    market_duckdb_path = (Path(market_timeseries_dir).resolve() / "market.duckdb").resolve()
    if not market_duckdb_path.exists():
        raise FileNotFoundError(f"market.duckdb not found: {market_duckdb_path}")

    cache_key = f"{normalized_snapshot_id}:{market_duckdb_path}"
    with _market_reader_lock:
        reader = _market_reader_cache.get(cache_key)
        if reader is None:
            reader = MarketDbReader(str(market_duckdb_path))
            _market_reader_cache[cache_key] = reader
        return reader


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
            "forecastDividendFY": "forecast_dividend_fy",
            "nextYearForecastDividendFY": "next_year_forecast_dividend_fy",
            "payoutRatio": "payout_ratio",
            "forecastPayoutRatio": "forecast_payout_ratio",
            "nextYearForecastPayoutRatio": "next_year_forecast_payout_ratio",
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
    """Dataset client backed by DatasetSnapshotReader (no HTTP)."""

    def __init__(self, dataset_name: str) -> None:
        normalized_dataset_name = normalize_dataset_snapshot_name(dataset_name)
        if normalized_dataset_name is None:
            raise FileNotFoundError(f"Dataset not found: {dataset_name}")
        self.dataset_name = normalized_dataset_name
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
    """Market client backed by DuckDB reader (no HTTP)."""

    def __enter__(self) -> DirectMarketClient:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: Any,
    ) -> None:
        return None

    def close(self) -> None:
        # Market readers are cached process-wide for reuse and closed on shutdown.
        return None

    def get_stock_info(self, stock_code: str) -> StockInfo | None:
        reader = _resolve_market_reader()
        candidates = stock_code_candidates(stock_code)
        if not candidates:
            return None

        placeholders = ",".join("?" for _ in candidates)
        row = reader.query_one(
            f"""
            SELECT
                code,
                company_name,
                company_name_english,
                market_code,
                market_name,
                sector_17_code,
                sector_17_name,
                sector_33_code,
                sector_33_name,
                scale_category,
                listed_date
            FROM stocks
            WHERE code IN ({placeholders})
            ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
            LIMIT 1
            """,
            tuple(candidates),
        )
        if row is None:
            return None

        return StockInfo(
            code=str(row["code"]),
            companyName=str(row["company_name"]),
            companyNameEnglish=str(row["company_name_english"] or ""),
            marketCode=str(row["market_code"]),
            marketName=str(row["market_name"]),
            sector17Code=str(row["sector_17_code"]),
            sector17Name=str(row["sector_17_name"]),
            sector33Code=str(row["sector_33_code"]),
            sector33Name=str(row["sector_33_name"]),
            scaleCategory=str(row["scale_category"] or ""),
            listedDate=str(row["listed_date"]),
        )

    def get_statements(
        self,
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        period_type: str = "all",
        actual_only: bool = True,
    ) -> pd.DataFrame:
        reader = _resolve_market_reader()
        candidates = stock_code_candidates(stock_code)
        if not candidates:
            return pd.DataFrame()

        code_placeholders = ",".join("?" for _ in candidates)
        params: list[str] = list(candidates)
        where_conditions = [f"code IN ({code_placeholders})"]

        if start_date:
            where_conditions.append("disclosed_date >= ?")
            params.append(start_date)
        if end_date:
            where_conditions.append("disclosed_date <= ?")
            params.append(end_date)

        period_values = _resolve_statements_period_filter_values(period_type)
        if period_values:
            period_placeholders = ",".join("?" for _ in period_values)
            where_conditions.append(
                f"type_of_current_period IN ({period_placeholders})"
            )
            params.extend(period_values)

        if actual_only:
            actual_clause = " OR ".join(
                f"{column} IS NOT NULL" for column in _STATEMENTS_ACTUAL_ONLY_COLUMNS
            )
            where_conditions.append(f"({actual_clause})")

        sql = f"""
            WITH ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY disclosed_date
                        ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
                    ) AS rn
                FROM statements
                WHERE {" AND ".join(where_conditions)}
            )
            SELECT *
            FROM ranked
            WHERE rn = 1
            ORDER BY disclosed_date
        """
        rows = reader.query(sql, tuple(params))
        return _to_statements_df(rows)

    def get_stock_ohlcv(
        self,
        stock_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        timeframe: Literal["daily", "weekly", "monthly"] = "daily",
    ) -> pd.DataFrame:
        _ = timeframe
        reader = _resolve_market_reader()
        candidates = stock_code_candidates(stock_code)
        if not candidates:
            return pd.DataFrame()

        placeholders = ",".join("?" for _ in candidates)
        params: list[str] = list(candidates)
        where_conditions = [f"code IN ({placeholders})"]
        if start_date:
            where_conditions.append("date >= ?")
            params.append(start_date)
        if end_date:
            where_conditions.append("date <= ?")
            params.append(end_date)
        sql = f"""
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
                WHERE {" AND ".join(where_conditions)}
            )
            SELECT date, open, high, low, close, volume
            FROM ranked
            WHERE rn = 1
            ORDER BY date
        """

        rows = reader.query(sql, tuple(params))
        records = [
            {
                "date": row["date"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
            }
            for row in rows
        ]
        return convert_ohlcv_response(records)

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
