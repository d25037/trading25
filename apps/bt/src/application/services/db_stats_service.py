"""
DB Stats Service

GET /api/db/stats のビジネスロジック。
"""

from __future__ import annotations

from datetime import UTC, datetime

from src.infrastructure.db.market.market_db import METADATA_KEYS, MarketDb
from src.entrypoints.http.schemas.db import (
    DateRange,
    FundamentalsStats,
    IndicesStats,
    MarketStatsResponse,
    PrimeCoverage,
    StockDataStats,
    StockStats,
    TopixStats,
)


def get_market_stats(market_db: MarketDb) -> MarketStatsResponse:
    """market.db の統計情報を取得"""
    initialized = market_db.is_initialized()
    last_sync = market_db.get_sync_metadata(METADATA_KEYS["LAST_SYNC_DATE"])

    # Stats
    basic = market_db.get_stats()
    topix_range = market_db.get_topix_date_range()
    stock_data_range = market_db.get_stock_data_date_range()
    by_market = market_db.get_stock_count_by_market()
    indices_info = market_db.get_indices_data_range()
    statement_codes = market_db.get_statement_codes()
    latest_disclosed_date = market_db.get_latest_statement_disclosed_date()
    prime_coverage_info = market_db.get_prime_statement_coverage(limit_missing=0)
    db_size = market_db.get_db_file_size()

    # Topix
    topix = TopixStats(
        count=topix_range["count"] if topix_range else 0,
        dateRange=DateRange(min=topix_range["min"], max=topix_range["max"]) if topix_range else None,
    )

    # Stocks
    stocks_stats = StockStats(
        total=basic.get("stocks", 0),
        byMarket=by_market,
    )

    # Stock data
    stock_data = StockDataStats(
        count=stock_data_range["count"] if stock_data_range else 0,
        dateCount=stock_data_range["dateCount"] if stock_data_range else 0,
        dateRange=DateRange(min=stock_data_range["min"], max=stock_data_range["max"]) if stock_data_range else None,
        averageStocksPerDay=stock_data_range["averageStocksPerDay"] if stock_data_range else 0,
    )

    # Indices
    indices = IndicesStats(
        masterCount=indices_info["masterCount"] if indices_info else 0,
        dataCount=indices_info["dataCount"] if indices_info else 0,
        dateCount=indices_info["dateCount"] if indices_info else 0,
        dateRange=DateRange(**indices_info["dateRange"]) if indices_info and indices_info.get("dateRange") else None,
        byCategory=indices_info["byCategory"] if indices_info else {},
    )

    fundamentals = FundamentalsStats(
        count=basic.get("statements", 0),
        uniqueStockCount=len(statement_codes),
        latestDisclosedDate=latest_disclosed_date,
        primeCoverage=PrimeCoverage(
            primeStocks=prime_coverage_info.get("primeCount", 0),
            coveredStocks=prime_coverage_info.get("coveredCount", 0),
            missingStocks=prime_coverage_info.get("missingCount", 0),
            coverageRatio=prime_coverage_info.get("coverageRatio", 0),
        ),
    )

    return MarketStatsResponse(
        initialized=initialized,
        lastSync=last_sync,
        databaseSize=db_size,
        topix=topix,
        stocks=stocks_stats,
        stockData=stock_data,
        indices=indices,
        fundamentals=fundamentals,
        lastUpdated=datetime.now(UTC).isoformat(),
    )
