"""
DB Stats Service

GET /api/db/stats のビジネスロジック。
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from src.infrastructure.db.market.time_series_store import TimeSeriesInspection
from src.infrastructure.db.market.market_db import METADATA_KEYS
from src.entrypoints.http.schemas.db import (
    DateRange,
    FundamentalsStats,
    IndicesStats,
    ListedMarketCoverage,
    MarginStats,
    MarketStatsResponse,
    PrimeCoverage,
    StockDataStats,
    StockStats,
    TopixStats,
)


class MarketDbStatsLike(Protocol):
    def is_initialized(self) -> bool: ...
    def get_sync_metadata(self, key: str) -> str | None: ...
    def get_stats(self) -> dict[str, int]: ...
    def get_stock_count_by_market(self) -> dict[str, int]: ...
    def get_prime_codes(self) -> set[str]: ...
    def get_fundamentals_target_codes(self) -> set[str]: ...


class TimeSeriesStoreStatsLike(Protocol):
    def inspect(self) -> TimeSeriesInspection: ...


def _resolve_duckdb_size_bytes(time_series_store: object) -> int:
    duckdb_path = getattr(time_series_store, "_duckdb_path", None)
    if not isinstance(duckdb_path, Path):
        return 0
    try:
        return int(duckdb_path.stat().st_size) if duckdb_path.exists() else 0
    except OSError:
        return 0


def get_market_stats(
    market_db: MarketDbStatsLike,
    *,
    time_series_store: TimeSeriesStoreStatsLike,
) -> MarketStatsResponse:
    """DuckDB 時系列 SoT と market metadata を統合した統計情報を返す。"""
    initialized = market_db.is_initialized()
    last_sync = market_db.get_sync_metadata(METADATA_KEYS["LAST_SYNC_DATE"])
    inspection = time_series_store.inspect()

    # Metadata / reference data (DuckDB metadata tables)
    basic = market_db.get_stats()
    by_market = market_db.get_stock_count_by_market()
    statement_codes = set(inspection.statement_codes)
    latest_disclosed_date = inspection.latest_statement_disclosed_date
    prime_codes = market_db.get_prime_codes()
    target_codes = market_db.get_fundamentals_target_codes()
    prime_count = len(prime_codes)
    target_count = len(target_codes)
    prime_covered_count = len(prime_codes & statement_codes)
    covered_count = len(target_codes & statement_codes)
    prime_missing_count = len(prime_codes - statement_codes)
    missing_count = len(target_codes - statement_codes)
    prime_coverage_ratio = round(prime_covered_count / prime_count, 4) if prime_count > 0 else 0.0
    coverage_ratio = round(covered_count / target_count, 4) if target_count > 0 else 0.0

    # Topix
    topix = TopixStats(
        count=inspection.topix_count,
        dateRange=DateRange(min=inspection.topix_min, max=inspection.topix_max)
        if inspection.topix_min and inspection.topix_max
        else None,
    )

    # Stocks
    stocks_stats = StockStats(
        total=basic.get("stocks", 0),
        byMarket=by_market,
    )

    # Stock data
    stock_data = StockDataStats(
        count=inspection.stock_count,
        dateCount=inspection.stock_date_count,
        dateRange=DateRange(min=inspection.stock_min, max=inspection.stock_max)
        if inspection.stock_min and inspection.stock_max
        else None,
        averageStocksPerDay=(
            round(inspection.stock_count / inspection.stock_date_count, 2)
            if inspection.stock_date_count > 0
            else 0.0
        ),
    )

    # Indices
    indices = IndicesStats(
        masterCount=basic.get("index_master", 0),
        dataCount=inspection.indices_count,
        dateCount=inspection.indices_date_count,
        dateRange=DateRange(min=inspection.indices_min, max=inspection.indices_max)
        if inspection.indices_min and inspection.indices_max
        else None,
        byCategory={},
    )

    margin = MarginStats(
        count=inspection.margin_count,
        uniqueStockCount=len(inspection.margin_codes),
        dateCount=inspection.margin_date_count,
        dateRange=DateRange(min=inspection.margin_min, max=inspection.margin_max)
        if inspection.margin_min and inspection.margin_max
        else None,
    )

    fundamentals = FundamentalsStats(
        count=inspection.statements_count,
        uniqueStockCount=len(statement_codes),
        latestDisclosedDate=latest_disclosed_date,
        primeCoverage=PrimeCoverage(
            primeStocks=prime_count,
            coveredStocks=prime_covered_count,
            missingStocks=prime_missing_count,
            coverageRatio=prime_coverage_ratio,
        ),
        listedMarketCoverage=ListedMarketCoverage(
            listedMarketStocks=target_count,
            coveredStocks=covered_count,
            missingStocks=missing_count,
            coverageRatio=coverage_ratio,
        ),
    )

    return MarketStatsResponse(
        initialized=initialized,
        lastSync=last_sync,
        timeSeriesSource=inspection.source,
        databaseSize=_resolve_duckdb_size_bytes(time_series_store),
        topix=topix,
        stocks=stocks_stats,
        stockData=stock_data,
        indices=indices,
        margin=margin,
        fundamentals=fundamentals,
        lastUpdated=datetime.now(UTC).isoformat(),
    )
