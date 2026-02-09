"""
DB Validation Service

GET /api/db/validate のビジネスロジック。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Literal

from src.lib.market_db.market_db import METADATA_KEYS, MarketDb
from src.server.schemas.db import (
    AdjustmentEvent,
    DateRange,
    MarketValidationResponse,
    StockDataValidation,
    StockStats,
    TopixStats,
)


def validate_market_db(market_db: MarketDb) -> MarketValidationResponse:
    """market.db の整合性検証"""
    initialized = market_db.is_initialized()
    last_sync = market_db.get_sync_metadata(METADATA_KEYS["LAST_SYNC_DATE"])
    last_refresh = market_db.get_sync_metadata(METADATA_KEYS["LAST_STOCKS_REFRESH"])

    basic = market_db.get_stats()
    topix_range = market_db.get_topix_date_range()
    by_market = market_db.get_stock_count_by_market()

    # Missing dates
    missing_dates = market_db.get_missing_stock_data_dates()

    # stock_data date info
    sd_date_count = market_db.get_stock_data_unique_date_count()
    stock_data_range = market_db.get_stock_data_date_range()

    # Adjustment events
    adjustment_events = market_db.get_adjustment_events(limit=20)
    all_needing = market_db.get_stocks_needing_refresh(limit=100)

    # Failed dates from metadata
    failed_dates_raw = market_db.get_sync_metadata(METADATA_KEYS["FAILED_DATES"])
    failed_dates: list[str] = []
    if failed_dates_raw:
        try:
            failed_dates = json.loads(failed_dates_raw)
        except json.JSONDecodeError:
            pass

    # Recommendations
    recommendations: list[str] = []
    if not initialized:
        recommendations.append("Run initial sync to populate the database")
    if missing_dates:
        recommendations.append(f"Run incremental sync to fill {len(missing_dates)} missing dates")
    if all_needing:
        recommendations.append(f"Run stock refresh for {len(all_needing)} stocks with adjustment events")
    if failed_dates:
        recommendations.append(f"Retry {len(failed_dates)} failed sync dates")

    # Status determination
    status: Literal["healthy", "warning", "error"] = "healthy"
    if not initialized:
        status = "error"
    elif missing_dates or failed_dates or all_needing:
        status = "warning"

    topix = TopixStats(
        count=topix_range["count"] if topix_range else 0,
        dateRange=DateRange(min=topix_range["min"], max=topix_range["max"]) if topix_range else None,
    )

    stocks_stats = StockStats(
        total=basic.get("stocks", 0),
        byMarket=by_market,
    )

    stock_data_val = StockDataValidation(
        count=sd_date_count,
        dateRange=DateRange(min=stock_data_range["min"], max=stock_data_range["max"]) if stock_data_range else None,
        missingDates=missing_dates[:20],
        missingDatesCount=len(missing_dates),
    )

    return MarketValidationResponse(
        status=status,
        initialized=initialized,
        lastSync=last_sync,
        lastStocksRefresh=last_refresh,
        topix=topix,
        stocks=stocks_stats,
        stockData=stock_data_val,
        failedDates=failed_dates[:10],
        failedDatesCount=len(failed_dates),
        adjustmentEvents=[
            AdjustmentEvent(**e) for e in adjustment_events
        ],
        adjustmentEventsCount=len(adjustment_events),
        stocksNeedingRefresh=all_needing[:20],
        stocksNeedingRefreshCount=len(all_needing),
        recommendations=recommendations,
        lastUpdated=datetime.now(UTC).isoformat(),
    )
