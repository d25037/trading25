"""Response payload builders for market DB validation."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from src.application.contracts.market_data_plane import (
    AdjustmentEvent,
    DateRange,
    FundamentalsValidation,
    MarginValidation,
    Options225CoverageStatusLiteral,
    Options225Validation,
    StockDataValidation,
    StockMasterCoverageStats,
    StockMinuteDataValidation,
    StockStats,
    TopixStats,
    ValidationSampleWindow,
    ValidationSampleWindows,
)
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection


def build_sample_window(
    *,
    total_count: int,
    returned_count: int,
    limit: int,
) -> ValidationSampleWindow:
    safe_total = max(total_count, 0)
    safe_returned = max(returned_count, 0)
    safe_limit = max(limit, 0)
    return ValidationSampleWindow(
        returnedCount=safe_returned,
        totalCount=safe_total,
        limit=safe_limit,
        truncated=safe_total > safe_returned and safe_total > safe_limit,
    )


def build_topix_stats(inspection: TimeSeriesInspection) -> TopixStats:
    return TopixStats(
        count=inspection.topix_count,
        dateRange=DateRange(min=inspection.topix_min, max=inspection.topix_max)
        if inspection.topix_min and inspection.topix_max
        else None,
    )


def build_stock_stats(*, total: int, by_market: dict[str, int]) -> StockStats:
    return StockStats(total=total, byMarket=by_market)


def build_stock_data_validation(
    *,
    inspection: TimeSeriesInspection,
    missing_dates: list[str],
    missing_dates_count: int,
    sample_limit: int,
) -> StockDataValidation:
    return StockDataValidation(
        count=inspection.stock_count,
        dateRange=DateRange(min=inspection.stock_min, max=inspection.stock_max)
        if inspection.stock_min and inspection.stock_max
        else None,
        missingDates=missing_dates[:sample_limit],
        missingDatesCount=missing_dates_count,
    )


def build_stock_minute_data_validation(
    inspection: TimeSeriesInspection,
) -> StockMinuteDataValidation:
    return StockMinuteDataValidation(
        count=inspection.stock_minute_count,
        uniqueStockCount=inspection.stock_minute_code_count,
        dateCount=inspection.stock_minute_date_count,
        dateRange=DateRange(
            min=inspection.stock_minute_min,
            max=inspection.stock_minute_max,
        )
        if inspection.stock_minute_min and inspection.stock_minute_max
        else None,
        latestTime=inspection.latest_stock_minute_time,
    )


def build_options_225_validation(
    *,
    inspection: TimeSeriesInspection,
    coverage_status: Options225CoverageStatusLiteral,
    allowed_topix_lag_dates: int,
    missing_topix_coverage_dates_count: int,
    missing_topix_coverage_dates: list[str],
    missing_underlying_dates_count: int,
    missing_underlying_dates: list[str],
    conflicting_underlying_dates_count: int,
    conflicting_underlying_dates: list[str],
) -> Options225Validation:
    return Options225Validation(
        count=inspection.options_225_count,
        dateCount=inspection.options_225_date_count,
        dateRange=DateRange(min=inspection.options_225_min, max=inspection.options_225_max)
        if inspection.options_225_min and inspection.options_225_max
        else None,
        coverageStatus=coverage_status,
        allowedTopixLagDates=allowed_topix_lag_dates,
        missingTopixCoverageDatesCount=missing_topix_coverage_dates_count,
        missingTopixCoverageDates=missing_topix_coverage_dates,
        missingUnderlyingPriceDatesCount=missing_underlying_dates_count,
        missingUnderlyingPriceDates=missing_underlying_dates,
        conflictingUnderlyingPriceDatesCount=conflicting_underlying_dates_count,
        conflictingUnderlyingPriceDates=conflicting_underlying_dates,
    )


def build_margin_validation(
    *,
    inspection: TimeSeriesInspection,
    empty_skipped_count: int,
    empty_skipped_codes: list[str],
    sample_limit: int,
) -> MarginValidation:
    return MarginValidation(
        count=inspection.margin_count,
        uniqueStockCount=len(inspection.margin_codes),
        dateCount=inspection.margin_date_count,
        dateRange=DateRange(min=inspection.margin_min, max=inspection.margin_max)
        if inspection.margin_min and inspection.margin_max
        else None,
        orphanCount=inspection.margin_orphan_count,
        emptySkippedCount=empty_skipped_count,
        emptySkippedCodes=empty_skipped_codes[:sample_limit],
    )


def build_fundamentals_validation(
    *,
    inspection: TimeSeriesInspection,
    statement_codes: set[str],
    latest_disclosed: str | None,
    missing_count: int,
    missing_codes: list[str],
    alias_covered_count: int,
    empty_skipped_count: int,
    empty_skipped_codes: list[str],
    empty_skipped_sample_limit: int,
    failed_dates_count: int,
    failed_codes_count: int,
) -> FundamentalsValidation:
    return FundamentalsValidation(
        count=inspection.statements_count,
        uniqueStockCount=len(statement_codes),
        latestDisclosedDate=latest_disclosed,
        missingListedMarketStocksCount=missing_count,
        missingListedMarketStocks=missing_codes,
        issuerAliasCoveredCount=alias_covered_count,
        emptySkippedCount=empty_skipped_count,
        emptySkippedCodes=empty_skipped_codes[:empty_skipped_sample_limit],
        failedDatesCount=failed_dates_count,
        failedCodesCount=failed_codes_count,
    )


def build_stock_master_coverage_stats(
    master_coverage: dict[str, Any],
    *,
    missing_dates_count: int,
    missing_dates: list[str],
) -> StockMasterCoverageStats:
    return StockMasterCoverageStats(
        dailyCount=int(master_coverage.get("dailyCount", 0) or 0),
        intervalCount=int(master_coverage.get("intervalCount", 0) or 0),
        latestCount=int(master_coverage.get("latestCount", 0) or 0),
        indexMembershipDailyCount=int(
            master_coverage.get("indexMembershipDailyCount", 0) or 0
        ),
        dateRange=DateRange(
            min=str(master_coverage.get("dateMin")),
            max=str(master_coverage.get("dateMax")),
        )
        if master_coverage.get("dateMin") and master_coverage.get("dateMax")
        else None,
        dateCount=int(master_coverage.get("dateCount", 0) or 0),
        codeCount=int(master_coverage.get("codeCount", 0) or 0),
        missingTopixDatesCount=missing_dates_count,
        missingTopixDates=missing_dates,
    )


def build_adjustment_events(events: Sequence[dict[str, Any]]) -> list[AdjustmentEvent]:
    return [AdjustmentEvent(**event) for event in events]


def build_validation_sample_windows(
    *,
    stock_data_missing_dates_count: int,
    stock_data_missing_dates_returned_count: int,
    stock_data_missing_dates_limit: int,
    stock_master_missing_dates_count: int,
    stock_master_missing_dates_returned_count: int,
    stock_master_missing_dates_limit: int,
    failed_dates_count: int,
    failed_dates_returned_count: int,
    failed_dates_limit: int,
    adjustment_events_count: int,
    adjustment_events_returned_count: int,
    adjustment_events_limit: int,
    stocks_needing_refresh_count: int,
    stocks_needing_refresh_returned_count: int,
    stocks_needing_refresh_limit: int,
    options_missing_topix_count: int,
    options_missing_topix_returned_count: int,
    options_missing_topix_limit: int,
    options_missing_underlying_count: int,
    options_missing_underlying_returned_count: int,
    options_missing_underlying_limit: int,
    options_conflicting_underlying_count: int,
    options_conflicting_underlying_returned_count: int,
    options_conflicting_underlying_limit: int,
    missing_listed_market_stocks_count: int,
    missing_listed_market_stocks_returned_count: int,
    missing_listed_market_stocks_limit: int,
    fundamentals_empty_skipped_count: int,
    fundamentals_empty_skipped_returned_count: int,
    fundamentals_empty_skipped_limit: int,
    margin_empty_skipped_count: int,
    margin_empty_skipped_returned_count: int,
    margin_empty_skipped_limit: int,
) -> ValidationSampleWindows:
    return ValidationSampleWindows(
        stockDataMissingDates=build_sample_window(
            total_count=stock_data_missing_dates_count,
            returned_count=stock_data_missing_dates_returned_count,
            limit=stock_data_missing_dates_limit,
        ),
        stockMasterMissingTopixDates=build_sample_window(
            total_count=stock_master_missing_dates_count,
            returned_count=stock_master_missing_dates_returned_count,
            limit=stock_master_missing_dates_limit,
        ),
        failedDates=build_sample_window(
            total_count=failed_dates_count,
            returned_count=failed_dates_returned_count,
            limit=failed_dates_limit,
        ),
        adjustmentEvents=build_sample_window(
            total_count=adjustment_events_count,
            returned_count=adjustment_events_returned_count,
            limit=adjustment_events_limit,
        ),
        stocksNeedingRefresh=build_sample_window(
            total_count=stocks_needing_refresh_count,
            returned_count=stocks_needing_refresh_returned_count,
            limit=stocks_needing_refresh_limit,
        ),
        options225MissingTopixCoverageDates=build_sample_window(
            total_count=options_missing_topix_count,
            returned_count=options_missing_topix_returned_count,
            limit=options_missing_topix_limit,
        ),
        options225MissingUnderlyingPriceDates=build_sample_window(
            total_count=options_missing_underlying_count,
            returned_count=options_missing_underlying_returned_count,
            limit=options_missing_underlying_limit,
        ),
        options225ConflictingUnderlyingPriceDates=build_sample_window(
            total_count=options_conflicting_underlying_count,
            returned_count=options_conflicting_underlying_returned_count,
            limit=options_conflicting_underlying_limit,
        ),
        missingListedMarketStocks=build_sample_window(
            total_count=missing_listed_market_stocks_count,
            returned_count=missing_listed_market_stocks_returned_count,
            limit=missing_listed_market_stocks_limit,
        ),
        fundamentalsEmptySkippedCodes=build_sample_window(
            total_count=fundamentals_empty_skipped_count,
            returned_count=fundamentals_empty_skipped_returned_count,
            limit=fundamentals_empty_skipped_limit,
        ),
        marginEmptySkippedCodes=build_sample_window(
            total_count=margin_empty_skipped_count,
            returned_count=margin_empty_skipped_returned_count,
            limit=margin_empty_skipped_limit,
        ),
    )
