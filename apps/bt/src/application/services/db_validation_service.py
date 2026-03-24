"""
DB Validation Service

GET /api/db/validate のビジネスロジック。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

from src.application.services.listed_market_targets import (
    build_fundamentals_coverage,
    build_fundamentals_target_map,
    normalize_frontier_date,
    resolve_frontier_cache_codes,
)
from src.domains.strategy.signals.feature_registry import resolve_feature_requirement_spec
from src.domains.strategy.signals.registry import SIGNAL_REGISTRY
from src.infrastructure.db.market.market_db import (
    LOCAL_STOCK_PRICE_ADJUSTMENT_MODE,
    METADATA_KEYS,
)
from src.infrastructure.db.market.time_series_store import (
    TimeSeriesInspection,
)
from src.entrypoints.http.schemas.db import (
    AdjustmentEvent,
    DateRange,
    FundamentalsValidation,
    IntegrityIssue,
    MarginValidation,
    MarketValidationResponse,
    Options225Validation,
    ValidationSampleWindow,
    ValidationSampleWindows,
    StockDataValidation,
    StockStats,
    TopixStats,
)

_INSPECT_MISSING_DATES_LIMIT = 100
_STOCK_DATA_MISSING_DATES_SAMPLE_LIMIT = 20
_FAILED_DATES_SAMPLE_LIMIT = 10
_ADJUSTMENT_EVENTS_SAMPLE_LIMIT = 20
_STOCKS_NEEDING_REFRESH_SAMPLE_LIMIT = 20
_OPTIONS_225_SAMPLE_LIMIT = 20
_MISSING_LISTED_MARKET_STOCKS_SAMPLE_LIMIT = 20
_EMPTY_SKIPPED_CODES_SAMPLE_LIMIT = 20

_SIGNAL_REQUIREMENTS = sorted(
    {
        requirement
        for signal_def in SIGNAL_REGISTRY
        for requirement in signal_def.data_requirements
        if requirement
    }
)

_STATEMENT_REQUIREMENT_COLUMN_ALTERNATIVES: dict[str, tuple[tuple[str, ...], ...]] = {
    "EPS": (("earnings_per_share",),),
    "BPS": (("bps",),),
    "ROE": (("profit", "equity"),),
    "ROA": (("profit", "total_assets"),),
    "ForwardForecastEPS": (
        ("forecast_eps",),
        ("next_year_forecast_earnings_per_share",),
    ),
    "DividendFY": (("dividend_fy",),),
    "ForwardForecastDividendFY": (
        ("forecast_dividend_fy",),
        ("next_year_forecast_dividend_fy",),
    ),
    "PayoutRatio": (("payout_ratio",),),
    "ForwardForecastPayoutRatio": (
        ("forecast_payout_ratio",),
        ("next_year_forecast_payout_ratio",),
    ),
    "Profit": (("profit",),),
    "Sales": (("sales",),),
    "OperatingMargin": (("operating_profit", "sales"),),
    "OperatingCashFlow": (("operating_cash_flow",),),
    "InvestingCashFlow": (("investing_cash_flow",),),
    "SharesOutstanding": (("shares_outstanding",),),
}

_SIGNAL_STATEMENT_COLUMNS = sorted(
    {
        column
        for requirement, alternatives in _STATEMENT_REQUIREMENT_COLUMN_ALTERNATIVES.items()
        if f"statements:{requirement}" in _SIGNAL_REQUIREMENTS
        for option in alternatives
        for column in option
    }
)


class ValidationMarketDbLike(Protocol):
    def is_initialized(self) -> bool: ...
    def is_legacy_stock_price_snapshot(self) -> bool: ...
    def get_stock_price_adjustment_mode(self) -> str | None: ...
    def get_sync_metadata(self, key: str) -> str | None: ...
    def get_stats(self) -> dict[str, int]: ...
    def get_stock_count_by_market(self) -> dict[str, int]: ...
    def get_adjustment_events(self, limit: int = 20) -> list[dict[str, Any]]: ...
    def get_adjustment_events_count(self) -> int: ...
    def get_stocks_needing_refresh(self, limit: int | None = 20) -> list[str]: ...
    def get_stocks_needing_refresh_count(self) -> int: ...
    def get_fundamentals_target_codes(self) -> set[str]: ...
    def get_fundamentals_target_stock_rows(self) -> list[dict[str, str]]: ...
    def get_options_225_underlying_price_issue_dates(self, *, issue_type: str, limit: int = 20) -> list[str]: ...
    def get_options_225_underlying_price_issue_count(self, *, issue_type: str) -> int: ...


class ValidationTimeSeriesStoreLike(Protocol):
    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
        missing_options_225_dates_limit: int = 0,
        statement_non_null_columns: list[str] | None = None,
    ) -> TimeSeriesInspection: ...


def validate_market_db(
    market_db: ValidationMarketDbLike,
    *,
    time_series_store: ValidationTimeSeriesStoreLike,
) -> MarketValidationResponse:
    """DuckDB 時系列 SoT を基準とした整合性検証。"""
    initialized = market_db.is_initialized()
    legacy_stock_snapshot = market_db.is_legacy_stock_price_snapshot()
    stock_price_adjustment_mode = market_db.get_stock_price_adjustment_mode()
    last_sync = market_db.get_sync_metadata(METADATA_KEYS["LAST_SYNC_DATE"])
    last_refresh = market_db.get_sync_metadata(METADATA_KEYS["LAST_STOCKS_REFRESH"])

    basic = market_db.get_stats()
    inspection = _resolve_time_series_inspection(time_series_store)
    by_market = market_db.get_stock_count_by_market()
    statement_codes = set(inspection.statement_codes)
    latest_disclosed = inspection.latest_statement_disclosed_date
    fundamentals_frontier = normalize_frontier_date(
        market_db.get_sync_metadata(METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"])
        or latest_disclosed
    )
    fundamentals_empty_skipped_codes = sorted(
        resolve_frontier_cache_codes(
            market_db.get_sync_metadata(METADATA_KEYS["FUNDAMENTALS_EMPTY_CODES"]),
            fundamentals_frontier,
        )
    )
    fundamentals_coverage = _build_statement_coverage(
        build_fundamentals_target_map(market_db.get_fundamentals_target_stock_rows()),
        statement_codes,
        empty_skipped_codes=set(fundamentals_empty_skipped_codes),
        limit_missing=_MISSING_LISTED_MARKET_STOCKS_SAMPLE_LIMIT,
    )
    missing_fundamentals_count = int(fundamentals_coverage.get("missingCount", 0) or 0)
    missing_fundamentals_codes = [
        str(code) for code in fundamentals_coverage.get("missingCodes", [])
    ]
    fundamentals_empty_skipped_count = int(
        fundamentals_coverage.get("emptySkippedCount", 0) or 0
    )
    fundamentals_alias_covered_count = int(
        fundamentals_coverage.get("issuerAliasCoveredCount", 0) or 0
    )
    missing_dates = list(inspection.missing_stock_dates)
    missing_dates_count = _resolve_missing_dates_count(inspection)
    margin_frontier = normalize_frontier_date(
        inspection.topix_max or inspection.stock_max or inspection.margin_max
    )
    margin_empty_skipped_codes = sorted(
        resolve_frontier_cache_codes(
            market_db.get_sync_metadata(METADATA_KEYS["MARGIN_EMPTY_CODES"]),
            margin_frontier,
        )
    )
    margin_empty_skipped_count = len(margin_empty_skipped_codes)
    options_225_missing_underlying_dates = market_db.get_options_225_underlying_price_issue_dates(
        issue_type="missing",
        limit=_OPTIONS_225_SAMPLE_LIMIT,
    )
    options_225_missing_underlying_dates_count = market_db.get_options_225_underlying_price_issue_count(
        issue_type="missing"
    )
    options_225_conflicting_underlying_dates = market_db.get_options_225_underlying_price_issue_dates(
        issue_type="conflicting",
        limit=_OPTIONS_225_SAMPLE_LIMIT,
    )
    options_225_conflicting_underlying_dates_count = market_db.get_options_225_underlying_price_issue_count(
        issue_type="conflicting"
    )
    options_225_missing_local_data = _is_options_225_local_data_missing(
        initialized=initialized,
        inspection=inspection,
    )
    options_225_stale_local_data = _is_options_225_local_data_stale(inspection)
    options_225_missing_topix_coverage_dates_count = (
        _resolve_options_225_missing_topix_coverage_dates_count(inspection)
    )
    options_225_missing_topix_coverage_dates = (
        _resolve_options_225_missing_topix_coverage_dates(inspection)
    )
    options_225_partial_local_data = options_225_missing_topix_coverage_dates_count > 0

    # Adjustment events
    adjustment_events = market_db.get_adjustment_events(limit=_ADJUSTMENT_EVENTS_SAMPLE_LIMIT)
    adjustment_events_count = market_db.get_adjustment_events_count()
    sample_needing = market_db.get_stocks_needing_refresh(
        limit=_STOCKS_NEEDING_REFRESH_SAMPLE_LIMIT
    )
    all_needing_count = market_db.get_stocks_needing_refresh_count()

    # Failed dates from metadata
    failed_dates_raw = market_db.get_sync_metadata(METADATA_KEYS["FAILED_DATES"])
    failed_dates: list[str] = []
    if failed_dates_raw:
        try:
            failed_dates = json.loads(failed_dates_raw)
        except json.JSONDecodeError:
            pass

    fundamentals_failed_dates = _load_metadata_list(
        market_db,
        METADATA_KEYS["FUNDAMENTALS_FAILED_DATES"],
    )
    fundamentals_failed_codes = _load_metadata_list(
        market_db,
        METADATA_KEYS["FUNDAMENTALS_FAILED_CODES"],
    )
    integrity_issues, readiness_recommendations = _build_readiness_issues(inspection)

    # Recommendations
    recommendations: list[str] = []
    if legacy_stock_snapshot:
        recommendations.append(
            "Run initial sync with reset enabled, or manually reset "
            "market-timeseries/market.duckdb and market-timeseries/parquet first, "
            "to rebuild stock_data_raw and local adjusted stock_data"
        )
    elif stock_price_adjustment_mode != LOCAL_STOCK_PRICE_ADJUSTMENT_MODE:
        recommendations.append(
            "Run initial sync with reset enabled to enable local stock price projection"
        )
    if not initialized:
        recommendations.append("Run initial sync to populate the database")
    if missing_dates_count > 0:
        recommendations.append(f"Run incremental sync to fill {missing_dates_count} missing dates")
    if failed_dates:
        recommendations.append(f"Retry {len(failed_dates)} failed sync dates")
    if missing_fundamentals_count > 0:
        recommendations.append(
            f"Run repair sync to backfill fundamentals for {missing_fundamentals_count} listed-market stocks"
        )
    if options_225_missing_local_data:
        recommendations.append(
            "Run incremental sync to ingest N225 options data into options_225_data"
        )
    if options_225_stale_local_data:
        recommendations.append(
            "Run incremental sync to refresh N225 options data "
            f"through {inspection.topix_max or 'the latest TOPIX date'} "
            f"(latest local options date: {inspection.options_225_max or 'n/a'})"
        )
    elif options_225_partial_local_data:
        recommendations.append(
            "Run incremental sync to backfill N225 options history for "
            f"{options_225_missing_topix_coverage_dates_count} TOPIX dates missing from "
            "options_225_data"
        )
    if options_225_missing_underlying_dates_count > 0:
        recommendations.append(
            f"Run sync again or inspect raw options data for {options_225_missing_underlying_dates_count} dates with missing UnderPx"
        )
    if options_225_conflicting_underlying_dates_count > 0:
        recommendations.append(
            f"Inspect options ingestion for {options_225_conflicting_underlying_dates_count} dates with conflicting UnderPx values"
        )
    if fundamentals_failed_dates:
        recommendations.append(
            f"Retry {len(fundamentals_failed_dates)} failed fundamentals dates"
        )
    if fundamentals_failed_codes:
        recommendations.append(
            f"Retry {len(fundamentals_failed_codes)} failed fundamentals codes"
        )
    recommendations.extend(readiness_recommendations)

    # Status determination
    status: Literal["healthy", "warning", "error"] = "healthy"
    if legacy_stock_snapshot or not initialized:
        status = "error"
    elif (
        missing_dates_count > 0
        or failed_dates
        or missing_fundamentals_count > 0
        or options_225_missing_local_data
        or options_225_stale_local_data
        or options_225_partial_local_data
        or options_225_missing_underlying_dates_count > 0
        or options_225_conflicting_underlying_dates_count > 0
        or fundamentals_failed_dates
        or fundamentals_failed_codes
        or integrity_issues
    ):
        status = "warning"

    topix = TopixStats(
        count=inspection.topix_count,
        dateRange=DateRange(
            min=inspection.topix_min,
            max=inspection.topix_max,
        )
        if inspection.topix_min and inspection.topix_max
        else None,
    )

    stocks_stats = StockStats(
        total=basic.get("stocks", 0),
        byMarket=by_market,
    )

    stock_data_val = StockDataValidation(
        count=inspection.stock_count,
        dateRange=DateRange(
            min=inspection.stock_min,
            max=inspection.stock_max,
        )
        if inspection.stock_min and inspection.stock_max
        else None,
        missingDates=missing_dates[:_STOCK_DATA_MISSING_DATES_SAMPLE_LIMIT],
        missingDatesCount=missing_dates_count,
    )

    options_225_val = Options225Validation(
        count=inspection.options_225_count,
        dateCount=inspection.options_225_date_count,
        dateRange=DateRange(
            min=inspection.options_225_min,
            max=inspection.options_225_max,
        )
        if inspection.options_225_min and inspection.options_225_max
        else None,
        missingTopixCoverageDatesCount=options_225_missing_topix_coverage_dates_count,
        missingTopixCoverageDates=options_225_missing_topix_coverage_dates,
        missingUnderlyingPriceDatesCount=options_225_missing_underlying_dates_count,
        missingUnderlyingPriceDates=options_225_missing_underlying_dates,
        conflictingUnderlyingPriceDatesCount=options_225_conflicting_underlying_dates_count,
        conflictingUnderlyingPriceDates=options_225_conflicting_underlying_dates,
    )

    margin_val = MarginValidation(
        count=inspection.margin_count,
        uniqueStockCount=len(inspection.margin_codes),
        dateCount=inspection.margin_date_count,
        dateRange=DateRange(
            min=inspection.margin_min,
            max=inspection.margin_max,
        )
        if inspection.margin_min and inspection.margin_max
        else None,
        orphanCount=inspection.margin_orphan_count,
        emptySkippedCount=margin_empty_skipped_count,
        emptySkippedCodes=margin_empty_skipped_codes[:_EMPTY_SKIPPED_CODES_SAMPLE_LIMIT],
    )

    fundamentals_val = FundamentalsValidation(
        count=inspection.statements_count,
        uniqueStockCount=len(statement_codes),
        latestDisclosedDate=latest_disclosed,
        missingListedMarketStocksCount=missing_fundamentals_count,
        missingListedMarketStocks=missing_fundamentals_codes,
        issuerAliasCoveredCount=fundamentals_alias_covered_count,
        emptySkippedCount=fundamentals_empty_skipped_count,
        emptySkippedCodes=fundamentals_empty_skipped_codes[:_EMPTY_SKIPPED_CODES_SAMPLE_LIMIT],
        failedDatesCount=len(fundamentals_failed_dates),
        failedCodesCount=len(fundamentals_failed_codes),
    )

    return MarketValidationResponse(
        status=status,
        initialized=initialized,
        lastSync=last_sync,
        lastStocksRefresh=last_refresh,
        timeSeriesSource=inspection.source,
        topix=topix,
        stocks=stocks_stats,
        stockData=stock_data_val,
        options225=options_225_val,
        margin=margin_val,
        fundamentals=fundamentals_val,
        failedDates=failed_dates[:_FAILED_DATES_SAMPLE_LIMIT],
        failedDatesCount=len(failed_dates),
        adjustmentEvents=[
            AdjustmentEvent(**e) for e in adjustment_events
        ],
        adjustmentEventsCount=adjustment_events_count,
        stocksNeedingRefresh=sample_needing,
        stocksNeedingRefreshCount=all_needing_count,
        integrityIssues=integrity_issues,
        integrityIssuesCount=len(integrity_issues),
        sampleWindows=ValidationSampleWindows(
            stockDataMissingDates=_build_sample_window(
                total_count=missing_dates_count,
                returned_count=len(stock_data_val.missingDates),
                limit=_STOCK_DATA_MISSING_DATES_SAMPLE_LIMIT,
            ),
            failedDates=_build_sample_window(
                total_count=len(failed_dates),
                returned_count=min(len(failed_dates), _FAILED_DATES_SAMPLE_LIMIT),
                limit=_FAILED_DATES_SAMPLE_LIMIT,
            ),
            adjustmentEvents=_build_sample_window(
                total_count=adjustment_events_count,
                returned_count=len(adjustment_events),
                limit=_ADJUSTMENT_EVENTS_SAMPLE_LIMIT,
            ),
            stocksNeedingRefresh=_build_sample_window(
                total_count=all_needing_count,
                returned_count=len(sample_needing),
                limit=_STOCKS_NEEDING_REFRESH_SAMPLE_LIMIT,
            ),
            options225MissingTopixCoverageDates=_build_sample_window(
                total_count=options_225_missing_topix_coverage_dates_count,
                returned_count=len(options_225_val.missingTopixCoverageDates),
                limit=_OPTIONS_225_SAMPLE_LIMIT,
            ),
            options225MissingUnderlyingPriceDates=_build_sample_window(
                total_count=options_225_missing_underlying_dates_count,
                returned_count=len(options_225_val.missingUnderlyingPriceDates),
                limit=_OPTIONS_225_SAMPLE_LIMIT,
            ),
            options225ConflictingUnderlyingPriceDates=_build_sample_window(
                total_count=options_225_conflicting_underlying_dates_count,
                returned_count=len(options_225_val.conflictingUnderlyingPriceDates),
                limit=_OPTIONS_225_SAMPLE_LIMIT,
            ),
            missingListedMarketStocks=_build_sample_window(
                total_count=missing_fundamentals_count,
                returned_count=len(fundamentals_val.missingListedMarketStocks),
                limit=_MISSING_LISTED_MARKET_STOCKS_SAMPLE_LIMIT,
            ),
            fundamentalsEmptySkippedCodes=_build_sample_window(
                total_count=fundamentals_empty_skipped_count,
                returned_count=len(fundamentals_val.emptySkippedCodes),
                limit=_EMPTY_SKIPPED_CODES_SAMPLE_LIMIT,
            ),
            marginEmptySkippedCodes=_build_sample_window(
                total_count=margin_empty_skipped_count,
                returned_count=len(margin_val.emptySkippedCodes),
                limit=_EMPTY_SKIPPED_CODES_SAMPLE_LIMIT,
            ),
        ),
        recommendations=recommendations,
        lastUpdated=datetime.now(UTC).isoformat(),
    )


def _resolve_time_series_inspection(
    time_series_store: ValidationTimeSeriesStoreLike,
) -> TimeSeriesInspection:
    return time_series_store.inspect(
        missing_stock_dates_limit=_INSPECT_MISSING_DATES_LIMIT,
        missing_options_225_dates_limit=_OPTIONS_225_SAMPLE_LIMIT,
        statement_non_null_columns=_SIGNAL_STATEMENT_COLUMNS,
    )


def _build_sample_window(
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


def _build_statement_coverage(
    target_map: dict[str, str],
    statement_codes: set[str],
    *,
    empty_skipped_codes: set[str] | None = None,
    limit_missing: int = 20,
) -> dict[str, Any]:
    return build_fundamentals_coverage(
        target_map,
        statement_codes,
        empty_skipped_codes=empty_skipped_codes,
        limit_missing=limit_missing,
        limit_empty=20,
    )


def _is_options_225_local_data_missing(
    *,
    initialized: bool,
    inspection: TimeSeriesInspection,
) -> bool:
    return (
        initialized
        and inspection.topix_count > 0
        and inspection.options_225_count <= 0
    )


def _is_options_225_local_data_stale(inspection: TimeSeriesInspection) -> bool:
    topix_max = normalize_frontier_date(inspection.topix_max)
    options_max = normalize_frontier_date(inspection.options_225_max)
    if (
        inspection.topix_count <= 0
        or inspection.options_225_count <= 0
        or topix_max is None
        or options_max is None
    ):
        return False
    return options_max < topix_max


def _resolve_options_225_missing_topix_coverage_dates_count(
    inspection: TimeSeriesInspection,
) -> int:
    if inspection.topix_count <= 0 or inspection.options_225_count <= 0:
        return 0
    return max(int(inspection.missing_options_225_dates_count or 0), 0)


def _resolve_options_225_missing_topix_coverage_dates(
    inspection: TimeSeriesInspection,
) -> list[str]:
    if inspection.topix_count <= 0 or inspection.options_225_count <= 0:
        return []
    return list(inspection.missing_options_225_dates[:_OPTIONS_225_SAMPLE_LIMIT])


def _build_readiness_issues(
    inspection: TimeSeriesInspection,
) -> tuple[list[IntegrityIssue], list[str]]:
    issues: list[IntegrityIssue] = []
    recommendations: list[str] = []
    missing_stock_dates_count = _resolve_missing_dates_count(inspection)

    if inspection.topix_count <= 0:
        issues.append(IntegrityIssue(code="chart.topix_data.missing", count=1))
        recommendations.append("Chart readiness: topix_data is missing in market time-series store")
    if inspection.stock_count <= 0:
        issues.append(IntegrityIssue(code="chart.stock_data.missing", count=1))
        recommendations.append("Chart readiness: stock_data is missing in market time-series store")
    if inspection.indices_count <= 0:
        issues.append(IntegrityIssue(code="chart.indices_data.missing", count=1))
        recommendations.append("Chart readiness: indices_data is missing in market time-series store")
    if missing_stock_dates_count > 0:
        issues.append(
            IntegrityIssue(
                code="chart.stock_data.missing_dates",
                count=missing_stock_dates_count,
            )
        )
        recommendations.append(
            "Chart readiness: stock_data is missing trading dates; run incremental sync to backfill"
        )
    if inspection.margin_orphan_count > 0:
        issues.append(
            IntegrityIssue(
                code="backtest.margin_data.orphans",
                count=inspection.margin_orphan_count,
            )
        )
        recommendations.append(
            "Backtest signal readiness: margin_data contains codes missing from stocks table"
        )

    missing_signal_requirements = _collect_missing_signal_requirements(inspection)
    if missing_signal_requirements:
        issues.append(
            IntegrityIssue(
                code="backtest.signal_requirements.missing",
                count=len(missing_signal_requirements),
            )
        )
        sample = ", ".join(missing_signal_requirements[:6])
        suffix = " ..." if len(missing_signal_requirements) > 6 else ""
        recommendations.append(
            f"Backtest signal readiness: unmet requirements ({sample}{suffix})"
        )

    return issues, recommendations


def _collect_missing_signal_requirements(inspection: TimeSeriesInspection) -> list[str]:
    missing: list[str] = []

    for requirement in _SIGNAL_REQUIREMENTS:
        domain = resolve_feature_requirement_spec(requirement).data_domain

        if domain == "market":
            if inspection.stock_count <= 0:
                missing.append(requirement)
            continue

        if domain == "margin":
            if inspection.margin_count <= 0:
                missing.append(requirement)
            continue

        if domain == "benchmark":
            if inspection.topix_count <= 0:
                missing.append(requirement)
            continue

        if domain == "sector":
            if inspection.indices_count <= 0:
                missing.append(requirement)
            continue

        if domain == "statements":
            metric = requirement.split(":", 1)[1]
            if not _is_statement_requirement_satisfied(
                metric,
                inspection.statements_count,
                inspection.statement_non_null_counts,
            ):
                missing.append(requirement)

    return sorted(set(missing))


def _is_statement_requirement_satisfied(
    metric: str,
    statements_count: int,
    statement_non_null_counts: dict[str, int],
) -> bool:
    if statements_count <= 0:
        return False

    alternatives = _STATEMENT_REQUIREMENT_COLUMN_ALTERNATIVES.get(metric)
    if not alternatives:
        return True

    for option in alternatives:
        if all(statement_non_null_counts.get(column, 0) > 0 for column in option):
            return True
    return False


def _load_metadata_list(market_db: ValidationMarketDbLike, key: str) -> list[str]:
    raw = market_db.get_sync_metadata(key)
    if not raw:
        return []
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, list):
        return []
    return [str(v) for v in loaded if isinstance(v, str)]


def _resolve_missing_dates_count(inspection: TimeSeriesInspection) -> int:
    return max(
        inspection.missing_stock_dates_count,
        len(inspection.missing_stock_dates),
    )
