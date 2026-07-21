"""
DB Validation Service

GET /api/db/validate のビジネスロジック。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

from src.application.services.listed_market_targets import (
    build_fundamentals_coverage,
    build_fundamentals_target_map,
    normalize_frontier_date,
    resolve_frontier_cache_codes,
)
from src.application.services.intraday_schedule import build_intraday_freshness
from src.application.services.db_validation_payloads import (
    build_adjustment_events,
    build_fundamentals_validation,
    build_margin_validation,
    build_options_225_validation,
    build_stock_data_validation,
    build_stock_master_coverage_stats,
    build_stock_minute_data_validation,
    build_stock_stats,
    build_topix_stats,
    build_validation_sample_windows,
)
from src.application.services.db_stats_service import _build_provider_vintage_stats
from src.domains.strategy.signals.feature_registry import resolve_feature_requirement_spec
from src.domains.strategy.signals.registry import SIGNAL_REGISTRY
from src.infrastructure.db.market.market_db import (
    MARKET_SCHEMA_VERSION,
    METADATA_KEYS,
)
from src.infrastructure.db.market.time_series_store import (
    TimeSeriesInspection,
)
from src.application.contracts.market_data_plane import (
    IntegrityIssue,
    IntradayFreshness,
    MarketValidationResponse,
    MarketSchemaStats,
    ValidationHealthStatusLiteral,
    ValidationHealthDomains,
)

_INSPECT_MISSING_DATES_LIMIT = 100
_STOCK_DATA_MISSING_DATES_SAMPLE_LIMIT = 20
_FAILED_DATES_SAMPLE_LIMIT = 10
_ADJUSTMENT_EVENTS_SAMPLE_LIMIT = 20
_STOCKS_NEEDING_REFRESH_SAMPLE_LIMIT = 20
_OPTIONS_225_SAMPLE_LIMIT = 20
_OPTIONS_225_TOPIX_LAG_GRACE_DATES = 1
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


@dataclass(frozen=True)
class _ValidationBaseSnapshot:
    initialized: bool
    legacy_stock_snapshot: bool
    stock_price_adjustment_mode: str | None
    last_sync: str | None
    last_intraday_sync: str | None
    last_refresh: str | None
    basic: dict[str, int]
    schema_version: int | None
    schema_current: bool
    reset_before_sync_eligible: bool
    master_coverage: dict[str, Any]
    missing_master_dates_count: int
    missing_master_dates: list[str]
    inspection: TimeSeriesInspection
    by_market: dict[str, int]


@dataclass(frozen=True)
class _FundamentalsValidationSnapshot:
    statement_codes: set[str]
    latest_disclosed: str | None
    missing_count: int
    missing_codes: list[str]
    empty_skipped_count: int
    empty_skipped_codes: list[str]
    alias_covered_count: int
    failed_dates: list[str]
    failed_codes: list[str]


@dataclass(frozen=True)
class _MarginValidationSnapshot:
    empty_skipped_count: int
    empty_skipped_codes: list[str]


@dataclass(frozen=True)
class _Options225ValidationSnapshot:
    missing_local_data: bool
    stale_local_data: bool
    pending_local_data: bool
    partial_local_data: bool
    coverage_status: Literal["in_sync", "missing", "pending", "stale", "partial"]
    missing_topix_coverage_dates_count: int
    missing_topix_coverage_dates: list[str]
    missing_underlying_dates_count: int
    missing_underlying_dates: list[str]
    conflicting_underlying_dates_count: int
    conflicting_underlying_dates: list[str]


@dataclass(frozen=True)
class _SourceQualitySnapshot:
    adjustment_events: list[dict[str, Any]]
    adjustment_events_count: int
    stocks_needing_refresh: list[str]
    stocks_needing_refresh_count: int


class ValidationMarketDbLike(Protocol):
    def is_initialized(self) -> bool: ...
    def is_legacy_stock_price_snapshot(self) -> bool: ...
    def get_stock_price_adjustment_mode(self) -> str | None: ...
    def get_sync_metadata(self, key: str) -> str | None: ...
    def get_stats(self) -> dict[str, int]: ...
    def get_market_schema_version(self) -> int | None: ...
    def is_market_schema_current(self) -> bool: ...
    def is_reset_before_sync_eligible(self) -> bool: ...
    def get_stock_master_coverage(self) -> dict[str, Any]: ...
    def get_stock_count_by_market(self) -> dict[str, int]: ...
    def get_adjustment_events(self, limit: int = 20) -> list[dict[str, Any]]: ...
    def get_adjustment_events_count(self) -> int: ...
    def get_stocks_needing_refresh(self, limit: int | None = 20) -> list[str]: ...
    def get_stocks_needing_refresh_count(self) -> int: ...
    def get_fundamentals_target_codes(self) -> set[str]: ...
    def get_fundamentals_target_stock_rows(self) -> list[dict[str, str]]: ...
    def get_options_225_underlying_price_issue_dates(self, *, issue_type: str, limit: int = 20) -> list[str]: ...
    def get_options_225_underlying_price_issue_count(self, *, issue_type: str) -> int: ...
    def get_adjusted_metrics_snapshot(self) -> dict[str, Any]: ...
    def get_adjusted_metrics_source_diagnostics(self) -> dict[str, int]: ...
    def get_provider_vintage_snapshot(self) -> dict[str, Any]: ...


class ValidationTimeSeriesStoreLike(Protocol):
    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
        missing_options_225_dates_limit: int = 0,
        statement_non_null_columns: list[str] | None = None,
    ) -> TimeSeriesInspection: ...


def _build_recommendations(
    *,
    schema_version: int | None,
    reset_before_sync_eligible: bool,
    initialized: bool,
    missing_dates_count: int,
    missing_master_dates_count: int,
    failed_dates_count: int,
    missing_fundamentals_count: int,
    options_225_missing_local_data: bool,
    options_225_stale_local_data: bool,
    options_225_pending_local_data: bool,
    options_225_partial_local_data: bool,
    options_225_missing_topix_coverage_dates_count: int,
    options_225_missing_underlying_dates_count: int,
    options_225_conflicting_underlying_dates_count: int,
    fundamentals_failed_dates_count: int,
    fundamentals_failed_codes_count: int,
    intraday_is_stale: bool,
    intraday_expected_date: str,
    intraday_latest_date: str | None,
    provider_vintage_status: str,
    readiness_recommendations: list[str],
    inspection: TimeSeriesInspection,
) -> list[str]:
    recommendations: list[str] = []
    if not reset_before_sync_eligible:
        observed = "missing" if schema_version is None else str(schema_version)
        return [
            "Run `bt market-cutover cutover` to rebuild the incompatible Market root "
            f"as schema v{MARKET_SCHEMA_VERSION} (current schema version: {observed})"
        ]
    if not initialized:
        recommendations.append("Run initial sync to populate the database")
    if missing_dates_count > 0:
        recommendations.append(f"Run incremental sync to fill {missing_dates_count} missing dates")
    if missing_master_dates_count > 0:
        recommendations.append(
            f"Run incremental sync to fill {missing_master_dates_count} TOPIX dates missing from stock_master_daily"
        )
    if failed_dates_count > 0:
        recommendations.append(f"Retry {failed_dates_count} failed sync dates")
    if missing_fundamentals_count > 0:
        recommendations.append(
            f"Run repair sync to backfill fundamentals for {missing_fundamentals_count} listed-market stocks"
        )
    if options_225_missing_local_data:
        recommendations.append(
            "Run incremental sync to ingest N225 options data into options_225_data"
        )
    if options_225_stale_local_data and not options_225_pending_local_data:
        recommendations.append(
            "Run incremental sync to refresh N225 options data "
            f"through {inspection.topix_max or 'the latest TOPIX date'} "
            f"(latest local options date: {inspection.options_225_max or 'n/a'})"
        )
    elif options_225_partial_local_data and not options_225_pending_local_data:
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
    if fundamentals_failed_dates_count > 0:
        recommendations.append(f"Retry {fundamentals_failed_dates_count} failed fundamentals dates")
    if fundamentals_failed_codes_count > 0:
        recommendations.append(f"Retry {fundamentals_failed_codes_count} failed fundamentals codes")
    if intraday_is_stale:
        recommendations.append(
            "Run intraday sync to ingest minute bars through "
            f"{intraday_expected_date} "
            f"(latest local minute date: {intraday_latest_date or 'n/a'})"
        )
    if provider_vintage_status in {"missing", "stale", "pending", "invalid"}:
        recommendations.append(
            "Run a normal Market DB sync to refresh the provider vintage and "
            "recompute pending current-basis fundamentals"
        )
    recommendations.extend(readiness_recommendations)
    return recommendations


def _resolve_validation_statuses(
    *,
    schema_current: bool,
    reset_before_sync_eligible: bool,
    legacy_stock_snapshot: bool,
    initialized: bool,
    missing_dates_count: int,
    missing_master_dates_count: int,
    failed_dates_count: int,
    missing_fundamentals_count: int,
    fundamentals_failed_dates_count: int,
    fundamentals_failed_codes_count: int,
    integrity_issues_count: int,
    provider_vintage_needs_sync: bool,
    provider_vintage_invalid: bool,
    options_225_missing_local_data: bool,
    options_225_stale_local_data: bool,
    options_225_pending_local_data: bool,
    options_225_partial_local_data: bool,
    intraday_is_stale: bool,
    adjustment_events_count: int,
    options_225_missing_underlying_dates_count: int,
    options_225_conflicting_underlying_dates_count: int,
) -> tuple[
    ValidationHealthStatusLiteral,
    ValidationHealthStatusLiteral,
    ValidationHealthStatusLiteral,
    ValidationHealthStatusLiteral,
    Literal["healthy", "warning", "error"],
]:
    core_daily_status = _resolve_core_daily_status(
        schema_current=schema_current,
        reset_before_sync_eligible=reset_before_sync_eligible,
        legacy_stock_snapshot=legacy_stock_snapshot,
        initialized=initialized,
        missing_dates_count=missing_dates_count,
        missing_master_dates_count=missing_master_dates_count,
        failed_dates_count=failed_dates_count,
        missing_fundamentals_count=missing_fundamentals_count,
        fundamentals_failed_dates_count=fundamentals_failed_dates_count,
        fundamentals_failed_codes_count=fundamentals_failed_codes_count,
        integrity_issues_count=integrity_issues_count,
        provider_vintage_needs_sync=provider_vintage_needs_sync,
        provider_vintage_invalid=provider_vintage_invalid,
    )
    derivatives_status = _resolve_derivatives_status(
        missing_local_data=options_225_missing_local_data,
        stale_local_data=options_225_stale_local_data,
        pending_local_data=options_225_pending_local_data,
        partial_local_data=options_225_partial_local_data,
    )
    intraday_status: ValidationHealthStatusLiteral = "warning" if intraday_is_stale else "healthy"
    source_quality_status: ValidationHealthStatusLiteral = (
        "info"
        if (
            adjustment_events_count > 0
            or options_225_missing_underlying_dates_count > 0
            or options_225_conflicting_underlying_dates_count > 0
        )
        else "healthy"
    )
    status: Literal["healthy", "warning", "error"] = _resolve_overall_status(
        core_daily_status=core_daily_status,
        derivatives_status=derivatives_status,
    )
    return (
        core_daily_status,
        derivatives_status,
        intraday_status,
        source_quality_status,
        status,
    )


def validate_market_db(
    market_db: ValidationMarketDbLike,
    *,
    time_series_store: ValidationTimeSeriesStoreLike,
) -> MarketValidationResponse:
    """DuckDB 時系列 SoT を基準とした整合性検証。"""
    base = _load_validation_base_snapshot(
        market_db=market_db,
        time_series_store=time_series_store,
    )
    fundamentals = _build_fundamentals_validation_snapshot(market_db, base.inspection)
    margin = _build_margin_validation_snapshot(market_db, base.inspection)
    options_225 = _build_options_225_validation_snapshot(
        market_db=market_db,
        initialized=base.initialized,
        inspection=base.inspection,
    )
    source_quality = _build_source_quality_snapshot(market_db)
    failed_dates = _load_failed_dates(market_db)
    intraday_freshness_snapshot = build_intraday_freshness(
        latest_date=base.inspection.stock_minute_max,
        latest_time=base.inspection.latest_stock_minute_time,
        last_intraday_sync=base.last_intraday_sync,
    )
    provider_vintage = _build_provider_vintage_stats(
        {
            **market_db.get_adjusted_metrics_snapshot(),
            **market_db.get_adjusted_metrics_source_diagnostics(),
            **market_db.get_provider_vintage_snapshot(),
        },
        source_stock_count=base.inspection.stock_count,
        source_statement_count=base.inspection.statements_count,
        provider_plan=market_db.get_sync_metadata(METADATA_KEYS["PROVIDER_PLAN"]),
    )
    integrity_issues, readiness_recommendations = _build_readiness_issues(base.inspection)

    recommendations = _build_recommendations(
        schema_version=base.schema_version,
        reset_before_sync_eligible=base.reset_before_sync_eligible,
        initialized=base.initialized,
        missing_dates_count=_resolve_missing_dates_count(base.inspection),
        missing_master_dates_count=base.missing_master_dates_count,
        failed_dates_count=len(failed_dates),
        missing_fundamentals_count=fundamentals.missing_count,
        options_225_missing_local_data=options_225.missing_local_data,
        options_225_stale_local_data=options_225.stale_local_data,
        options_225_pending_local_data=options_225.pending_local_data,
        options_225_partial_local_data=options_225.partial_local_data,
        options_225_missing_topix_coverage_dates_count=options_225.missing_topix_coverage_dates_count,
        options_225_missing_underlying_dates_count=options_225.missing_underlying_dates_count,
        options_225_conflicting_underlying_dates_count=options_225.conflicting_underlying_dates_count,
        fundamentals_failed_dates_count=len(fundamentals.failed_dates),
        fundamentals_failed_codes_count=len(fundamentals.failed_codes),
        intraday_is_stale=intraday_freshness_snapshot.status == "stale",
        intraday_expected_date=intraday_freshness_snapshot.expected_date,
        intraday_latest_date=intraday_freshness_snapshot.latest_date,
        provider_vintage_status=provider_vintage.status,
        readiness_recommendations=readiness_recommendations,
        inspection=base.inspection,
    )

    health_statuses = _resolve_validation_statuses(
        schema_current=base.schema_current,
        reset_before_sync_eligible=base.reset_before_sync_eligible,
        legacy_stock_snapshot=base.legacy_stock_snapshot,
        initialized=base.initialized,
        missing_dates_count=_resolve_missing_dates_count(base.inspection),
        missing_master_dates_count=base.missing_master_dates_count,
        failed_dates_count=len(failed_dates),
        missing_fundamentals_count=fundamentals.missing_count,
        fundamentals_failed_dates_count=len(fundamentals.failed_dates),
        fundamentals_failed_codes_count=len(fundamentals.failed_codes),
        integrity_issues_count=len(integrity_issues),
        provider_vintage_needs_sync=provider_vintage.status
        in {"missing", "stale", "pending"},
        provider_vintage_invalid=provider_vintage.status == "invalid",
        options_225_missing_local_data=options_225.missing_local_data,
        options_225_stale_local_data=options_225.stale_local_data,
        options_225_pending_local_data=options_225.pending_local_data,
        options_225_partial_local_data=options_225.partial_local_data,
        intraday_is_stale=intraday_freshness_snapshot.status == "stale",
        adjustment_events_count=source_quality.adjustment_events_count,
        options_225_missing_underlying_dates_count=options_225.missing_underlying_dates_count,
        options_225_conflicting_underlying_dates_count=options_225.conflicting_underlying_dates_count,
    )

    return _build_market_validation_response(
        base=base,
        fundamentals=fundamentals,
        margin=margin,
        options_225=options_225,
        source_quality=source_quality,
        failed_dates=failed_dates,
        provider_vintage=provider_vintage,
        intraday_freshness_snapshot=intraday_freshness_snapshot,
        integrity_issues=integrity_issues,
        recommendations=recommendations,
        health_statuses=health_statuses,
    )


def _load_validation_base_snapshot(
    *,
    market_db: ValidationMarketDbLike,
    time_series_store: ValidationTimeSeriesStoreLike,
) -> _ValidationBaseSnapshot:
    initialized = market_db.is_initialized()
    legacy_stock_snapshot = market_db.is_legacy_stock_price_snapshot()
    stock_price_adjustment_mode = market_db.get_stock_price_adjustment_mode()
    last_sync = market_db.get_sync_metadata(METADATA_KEYS["LAST_SYNC_DATE"])
    last_intraday_sync = market_db.get_sync_metadata(METADATA_KEYS["LAST_INTRADAY_SYNC"])
    last_refresh = market_db.get_sync_metadata(METADATA_KEYS["LAST_STOCKS_REFRESH"])

    basic = market_db.get_stats()
    schema_version = market_db.get_market_schema_version()
    schema_current = market_db.is_market_schema_current()
    reset_before_sync_eligible = market_db.is_reset_before_sync_eligible()
    master_coverage = market_db.get_stock_master_coverage()
    missing_master_dates_count = int(master_coverage.get("missingTopixDatesCount", 0) or 0)
    missing_master_dates = [str(d) for d in master_coverage.get("missingTopixDates", [])]
    inspection = _resolve_time_series_inspection(time_series_store)
    by_market = market_db.get_stock_count_by_market()
    return _ValidationBaseSnapshot(
        initialized=initialized,
        legacy_stock_snapshot=legacy_stock_snapshot,
        stock_price_adjustment_mode=stock_price_adjustment_mode,
        last_sync=last_sync,
        last_intraday_sync=last_intraday_sync,
        last_refresh=last_refresh,
        basic=basic,
        schema_version=schema_version,
        schema_current=schema_current,
        reset_before_sync_eligible=reset_before_sync_eligible,
        master_coverage=master_coverage,
        missing_master_dates_count=missing_master_dates_count,
        missing_master_dates=missing_master_dates,
        inspection=inspection,
        by_market=by_market,
    )


def _build_fundamentals_validation_snapshot(
    market_db: ValidationMarketDbLike,
    inspection: TimeSeriesInspection,
) -> _FundamentalsValidationSnapshot:
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
    fundamentals_failed_dates = _load_metadata_list(
        market_db,
        METADATA_KEYS["FUNDAMENTALS_FAILED_DATES"],
    )
    fundamentals_failed_codes = _load_metadata_list(
        market_db,
        METADATA_KEYS["FUNDAMENTALS_FAILED_CODES"],
    )
    return _FundamentalsValidationSnapshot(
        statement_codes=statement_codes,
        latest_disclosed=latest_disclosed,
        missing_count=missing_fundamentals_count,
        missing_codes=missing_fundamentals_codes,
        empty_skipped_count=fundamentals_empty_skipped_count,
        empty_skipped_codes=fundamentals_empty_skipped_codes,
        alias_covered_count=fundamentals_alias_covered_count,
        failed_dates=fundamentals_failed_dates,
        failed_codes=fundamentals_failed_codes,
    )


def _build_margin_validation_snapshot(
    market_db: ValidationMarketDbLike,
    inspection: TimeSeriesInspection,
) -> _MarginValidationSnapshot:
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
    return _MarginValidationSnapshot(
        empty_skipped_count=margin_empty_skipped_count,
        empty_skipped_codes=margin_empty_skipped_codes,
    )


def _build_options_225_validation_snapshot(
    *,
    market_db: ValidationMarketDbLike,
    initialized: bool,
    inspection: TimeSeriesInspection,
) -> _Options225ValidationSnapshot:
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
    options_225_pending_local_data = _is_options_225_local_data_pending(
        stale_local_data=options_225_stale_local_data,
        missing_topix_coverage_dates_count=options_225_missing_topix_coverage_dates_count,
    )
    options_225_coverage_status = _resolve_options_225_coverage_status(
        missing_local_data=options_225_missing_local_data,
        stale_local_data=options_225_stale_local_data,
        pending_local_data=options_225_pending_local_data,
        partial_local_data=options_225_partial_local_data,
    )
    return _Options225ValidationSnapshot(
        missing_local_data=options_225_missing_local_data,
        stale_local_data=options_225_stale_local_data,
        pending_local_data=options_225_pending_local_data,
        partial_local_data=options_225_partial_local_data,
        coverage_status=options_225_coverage_status,
        missing_topix_coverage_dates_count=options_225_missing_topix_coverage_dates_count,
        missing_topix_coverage_dates=options_225_missing_topix_coverage_dates,
        missing_underlying_dates_count=options_225_missing_underlying_dates_count,
        missing_underlying_dates=options_225_missing_underlying_dates,
        conflicting_underlying_dates_count=options_225_conflicting_underlying_dates_count,
        conflicting_underlying_dates=options_225_conflicting_underlying_dates,
    )


def _build_source_quality_snapshot(
    market_db: ValidationMarketDbLike,
) -> _SourceQualitySnapshot:
    adjustment_events = market_db.get_adjustment_events(limit=_ADJUSTMENT_EVENTS_SAMPLE_LIMIT)
    adjustment_events_count = market_db.get_adjustment_events_count()
    sample_needing = market_db.get_stocks_needing_refresh(
        limit=_STOCKS_NEEDING_REFRESH_SAMPLE_LIMIT
    )
    all_needing_count = market_db.get_stocks_needing_refresh_count()
    return _SourceQualitySnapshot(
        adjustment_events=adjustment_events,
        adjustment_events_count=adjustment_events_count,
        stocks_needing_refresh=sample_needing,
        stocks_needing_refresh_count=all_needing_count,
    )


def _load_failed_dates(market_db: ValidationMarketDbLike) -> list[str]:
    failed_dates_raw = market_db.get_sync_metadata(METADATA_KEYS["FAILED_DATES"])
    failed_dates: list[str] = []
    if failed_dates_raw:
        try:
            failed_dates = json.loads(failed_dates_raw)
        except json.JSONDecodeError:
            pass
    return failed_dates


def _build_market_validation_response(
    *,
    base: _ValidationBaseSnapshot,
    fundamentals: _FundamentalsValidationSnapshot,
    margin: _MarginValidationSnapshot,
    options_225: _Options225ValidationSnapshot,
    source_quality: _SourceQualitySnapshot,
    failed_dates: list[str],
    provider_vintage: Any,
    intraday_freshness_snapshot: Any,
    integrity_issues: list[IntegrityIssue],
    recommendations: list[str],
    health_statuses: tuple[
        ValidationHealthStatusLiteral,
        ValidationHealthStatusLiteral,
        ValidationHealthStatusLiteral,
        ValidationHealthStatusLiteral,
        Literal["healthy", "warning", "error"],
    ],
) -> MarketValidationResponse:
    (
        core_daily_status,
        derivatives_status,
        intraday_status,
        source_quality_status,
        status,
    ) = health_statuses
    missing_dates = list(base.inspection.missing_stock_dates)
    missing_dates_count = _resolve_missing_dates_count(base.inspection)

    topix = build_topix_stats(base.inspection)
    stocks_stats = build_stock_stats(
        total=base.basic.get("stocks", 0),
        by_market=base.by_market,
    )
    stock_data_val = build_stock_data_validation(
        inspection=base.inspection,
        missing_dates=missing_dates,
        missing_dates_count=missing_dates_count,
        sample_limit=_STOCK_DATA_MISSING_DATES_SAMPLE_LIMIT,
    )
    stock_minute_data_val = build_stock_minute_data_validation(base.inspection)
    options_225_val = build_options_225_validation(
        inspection=base.inspection,
        coverage_status=options_225.coverage_status,
        allowed_topix_lag_dates=_OPTIONS_225_TOPIX_LAG_GRACE_DATES,
        missing_topix_coverage_dates_count=options_225.missing_topix_coverage_dates_count,
        missing_topix_coverage_dates=options_225.missing_topix_coverage_dates,
        missing_underlying_dates_count=options_225.missing_underlying_dates_count,
        missing_underlying_dates=options_225.missing_underlying_dates,
        conflicting_underlying_dates_count=options_225.conflicting_underlying_dates_count,
        conflicting_underlying_dates=options_225.conflicting_underlying_dates,
    )
    margin_val = build_margin_validation(
        inspection=base.inspection,
        empty_skipped_count=margin.empty_skipped_count,
        empty_skipped_codes=margin.empty_skipped_codes,
        sample_limit=_EMPTY_SKIPPED_CODES_SAMPLE_LIMIT,
    )
    fundamentals_val = build_fundamentals_validation(
        inspection=base.inspection,
        statement_codes=fundamentals.statement_codes,
        latest_disclosed=fundamentals.latest_disclosed,
        missing_count=fundamentals.missing_count,
        missing_codes=fundamentals.missing_codes,
        alias_covered_count=fundamentals.alias_covered_count,
        empty_skipped_count=fundamentals.empty_skipped_count,
        empty_skipped_codes=fundamentals.empty_skipped_codes,
        empty_skipped_sample_limit=_EMPTY_SKIPPED_CODES_SAMPLE_LIMIT,
        failed_dates_count=len(fundamentals.failed_dates),
        failed_codes_count=len(fundamentals.failed_codes),
    )

    return MarketValidationResponse(
        status=status,
        healthDomains=ValidationHealthDomains(
            coreDailyStatus=core_daily_status,
            derivativesStatus=derivatives_status,
            intradayStatus=intraday_status,
            sourceQualityStatus=source_quality_status,
        ),
        initialized=base.initialized,
        lastSync=base.last_sync,
        lastIntradaySync=base.last_intraday_sync,
        lastStocksRefresh=base.last_refresh,
        timeSeriesSource=base.inspection.source,
        schema_=MarketSchemaStats(
            version=base.schema_version,
            current=base.schema_current,
            resetBeforeSyncEligible=base.reset_before_sync_eligible,
        ),
        stockMaster=build_stock_master_coverage_stats(
            base.master_coverage,
            missing_dates_count=base.missing_master_dates_count,
            missing_dates=base.missing_master_dates,
        ),
        topix=topix,
        stocks=stocks_stats,
        stockData=stock_data_val,
        stockMinuteData=stock_minute_data_val,
        options225=options_225_val,
        margin=margin_val,
        fundamentals=fundamentals_val,
        providerVintage=provider_vintage,
        failedDates=failed_dates[:_FAILED_DATES_SAMPLE_LIMIT],
        failedDatesCount=len(failed_dates),
        adjustmentEvents=build_adjustment_events(source_quality.adjustment_events),
        adjustmentEventsCount=source_quality.adjustment_events_count,
        stocksNeedingRefresh=source_quality.stocks_needing_refresh,
        stocksNeedingRefreshCount=source_quality.stocks_needing_refresh_count,
        integrityIssues=integrity_issues,
        integrityIssuesCount=len(integrity_issues),
        sampleWindows=build_validation_sample_windows(
            stock_data_missing_dates_count=missing_dates_count,
            stock_data_missing_dates_returned_count=len(stock_data_val.missingDates),
            stock_data_missing_dates_limit=_STOCK_DATA_MISSING_DATES_SAMPLE_LIMIT,
            stock_master_missing_dates_count=base.missing_master_dates_count,
            stock_master_missing_dates_returned_count=len(base.missing_master_dates),
            stock_master_missing_dates_limit=20,
            failed_dates_count=len(failed_dates),
            failed_dates_returned_count=min(len(failed_dates), _FAILED_DATES_SAMPLE_LIMIT),
            failed_dates_limit=_FAILED_DATES_SAMPLE_LIMIT,
            adjustment_events_count=source_quality.adjustment_events_count,
            adjustment_events_returned_count=len(source_quality.adjustment_events),
            adjustment_events_limit=_ADJUSTMENT_EVENTS_SAMPLE_LIMIT,
            stocks_needing_refresh_count=source_quality.stocks_needing_refresh_count,
            stocks_needing_refresh_returned_count=len(source_quality.stocks_needing_refresh),
            stocks_needing_refresh_limit=_STOCKS_NEEDING_REFRESH_SAMPLE_LIMIT,
            options_missing_topix_count=options_225.missing_topix_coverage_dates_count,
            options_missing_topix_returned_count=len(options_225_val.missingTopixCoverageDates),
            options_missing_topix_limit=_OPTIONS_225_SAMPLE_LIMIT,
            options_missing_underlying_count=options_225.missing_underlying_dates_count,
            options_missing_underlying_returned_count=len(options_225_val.missingUnderlyingPriceDates),
            options_missing_underlying_limit=_OPTIONS_225_SAMPLE_LIMIT,
            options_conflicting_underlying_count=options_225.conflicting_underlying_dates_count,
            options_conflicting_underlying_returned_count=len(options_225_val.conflictingUnderlyingPriceDates),
            options_conflicting_underlying_limit=_OPTIONS_225_SAMPLE_LIMIT,
            missing_listed_market_stocks_count=fundamentals.missing_count,
            missing_listed_market_stocks_returned_count=len(fundamentals_val.missingListedMarketStocks),
            missing_listed_market_stocks_limit=_MISSING_LISTED_MARKET_STOCKS_SAMPLE_LIMIT,
            fundamentals_empty_skipped_count=fundamentals.empty_skipped_count,
            fundamentals_empty_skipped_returned_count=len(fundamentals_val.emptySkippedCodes),
            fundamentals_empty_skipped_limit=_EMPTY_SKIPPED_CODES_SAMPLE_LIMIT,
            margin_empty_skipped_count=margin.empty_skipped_count,
            margin_empty_skipped_returned_count=len(margin_val.emptySkippedCodes),
            margin_empty_skipped_limit=_EMPTY_SKIPPED_CODES_SAMPLE_LIMIT,
        ),
        recommendations=recommendations,
        intradayFreshness=IntradayFreshness(
            status=intraday_freshness_snapshot.status,
            expectedDate=intraday_freshness_snapshot.expected_date,
            latestDate=intraday_freshness_snapshot.latest_date,
            latestTime=intraday_freshness_snapshot.latest_time,
            lastIntradaySync=intraday_freshness_snapshot.last_intraday_sync,
            readyTimeJst=intraday_freshness_snapshot.ready_time_jst,
            evaluatedAtJst=intraday_freshness_snapshot.evaluated_at_jst,
            calendarBasis=intraday_freshness_snapshot.calendar_basis,
        ),
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


def _is_options_225_local_data_pending(
    *,
    stale_local_data: bool,
    missing_topix_coverage_dates_count: int,
) -> bool:
    return (
        stale_local_data
        and 0 < missing_topix_coverage_dates_count <= _OPTIONS_225_TOPIX_LAG_GRACE_DATES
    )


def _resolve_options_225_coverage_status(
    *,
    missing_local_data: bool,
    stale_local_data: bool,
    pending_local_data: bool,
    partial_local_data: bool,
) -> Literal["in_sync", "missing", "pending", "stale", "partial"]:
    if missing_local_data:
        return "missing"
    if pending_local_data:
        return "pending"
    if stale_local_data:
        return "stale"
    if partial_local_data:
        return "partial"
    return "in_sync"


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


def _resolve_core_daily_status(
    *,
    schema_current: bool,
    reset_before_sync_eligible: bool,
    legacy_stock_snapshot: bool,
    initialized: bool,
    missing_dates_count: int,
    missing_master_dates_count: int,
    failed_dates_count: int,
    missing_fundamentals_count: int,
    fundamentals_failed_dates_count: int,
    fundamentals_failed_codes_count: int,
    integrity_issues_count: int,
    provider_vintage_needs_sync: bool,
    provider_vintage_invalid: bool,
) -> Literal["healthy", "warning", "error"]:
    if (
        not schema_current
        or not reset_before_sync_eligible
        or legacy_stock_snapshot
        or not initialized
        or provider_vintage_invalid
    ):
        return "error"
    if (
        missing_dates_count > 0
        or missing_master_dates_count > 0
        or failed_dates_count > 0
        or missing_fundamentals_count > 0
        or fundamentals_failed_dates_count > 0
        or fundamentals_failed_codes_count > 0
        or integrity_issues_count > 0
        or provider_vintage_needs_sync
    ):
        return "warning"
    return "healthy"


def _resolve_derivatives_status(
    *,
    missing_local_data: bool,
    stale_local_data: bool,
    pending_local_data: bool,
    partial_local_data: bool,
) -> Literal["healthy", "info", "warning"]:
    if missing_local_data:
        return "warning"
    if pending_local_data:
        return "info"
    if stale_local_data or partial_local_data:
        return "warning"
    return "healthy"


def _resolve_overall_status(
    *,
    core_daily_status: Literal["healthy", "warning", "error"],
    derivatives_status: Literal["healthy", "info", "warning"],
) -> Literal["healthy", "warning", "error"]:
    if core_daily_status == "error":
        return "error"
    if core_daily_status == "warning" or derivatives_status == "warning":
        return "warning"
    return "healthy"


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
