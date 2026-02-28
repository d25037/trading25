"""
DB Validation Service

GET /api/db/validate のビジネスロジック。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from typing import Literal

from src.domains.strategy.signals.registry import SIGNAL_REGISTRY
from src.infrastructure.db.market.market_db import METADATA_KEYS, MarketDb
from src.infrastructure.db.market.time_series_store import (
    MarketTimeSeriesStore,
    TimeSeriesInspection,
)
from src.entrypoints.http.schemas.db import (
    AdjustmentEvent,
    DateRange,
    FundamentalsValidation,
    IntegrityIssue,
    MarketValidationResponse,
    StockDataValidation,
    StockStats,
    TopixStats,
)

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


def validate_market_db(
    market_db: MarketDb,
    time_series_store: MarketTimeSeriesStore | None = None,
) -> MarketValidationResponse:
    """market.db の整合性検証"""
    initialized = market_db.is_initialized()
    last_sync = market_db.get_sync_metadata(METADATA_KEYS["LAST_SYNC_DATE"])
    last_refresh = market_db.get_sync_metadata(METADATA_KEYS["LAST_STOCKS_REFRESH"])

    basic = market_db.get_stats()
    inspection = _resolve_time_series_inspection(market_db, basic, time_series_store)
    by_market = market_db.get_stock_count_by_market()
    statement_codes = set(inspection.statement_codes)
    latest_disclosed = inspection.latest_statement_disclosed_date
    prime_coverage = _build_prime_statement_coverage(
        market_db,
        statement_codes,
        limit_missing=20,
    )
    missing_prime_count = int(prime_coverage.get("missingCount", 0) or 0)
    missing_prime_codes = [
        str(code) for code in prime_coverage.get("missingCodes", [])
    ]
    missing_dates = list(inspection.missing_stock_dates)
    missing_dates_count = _resolve_missing_dates_count(inspection)
    sd_date_count = inspection.stock_date_count

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
    if not initialized:
        recommendations.append("Run initial sync to populate the database")
    if missing_dates_count > 0:
        recommendations.append(f"Run incremental sync to fill {missing_dates_count} missing dates")
    if all_needing:
        recommendations.append(f"Run stock refresh for {len(all_needing)} stocks with adjustment events")
    if failed_dates:
        recommendations.append(f"Retry {len(failed_dates)} failed sync dates")
    if missing_prime_count > 0:
        recommendations.append(
            f"Backfill fundamentals for {missing_prime_count} Prime stocks"
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
    if not initialized:
        status = "error"
    elif (
        missing_dates_count > 0
        or failed_dates
        or all_needing
        or missing_prime_count > 0
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
        count=sd_date_count,
        dateRange=DateRange(
            min=inspection.stock_min,
            max=inspection.stock_max,
        )
        if inspection.stock_min and inspection.stock_max
        else None,
        missingDates=missing_dates[:20],
        missingDatesCount=missing_dates_count,
    )

    fundamentals_val = FundamentalsValidation(
        count=inspection.statements_count,
        uniqueStockCount=len(statement_codes),
        latestDisclosedDate=latest_disclosed,
        missingPrimeStocksCount=missing_prime_count,
        missingPrimeStocks=missing_prime_codes,
        failedDatesCount=len(fundamentals_failed_dates),
        failedCodesCount=len(fundamentals_failed_codes),
    )

    return MarketValidationResponse(
        status=status,
        initialized=initialized,
        lastSync=last_sync,
        lastStocksRefresh=last_refresh,
        topix=topix,
        stocks=stocks_stats,
        stockData=stock_data_val,
        fundamentals=fundamentals_val,
        failedDates=failed_dates[:10],
        failedDatesCount=len(failed_dates),
        adjustmentEvents=[
            AdjustmentEvent(**e) for e in adjustment_events
        ],
        adjustmentEventsCount=len(adjustment_events),
        stocksNeedingRefresh=all_needing[:20],
        stocksNeedingRefreshCount=len(all_needing),
        integrityIssues=integrity_issues,
        integrityIssuesCount=len(integrity_issues),
        recommendations=recommendations,
        lastUpdated=datetime.now(UTC).isoformat(),
    )


def _resolve_time_series_inspection(
    market_db: MarketDb,
    basic_stats: dict[str, int],
    time_series_store: MarketTimeSeriesStore | None,
) -> TimeSeriesInspection:
    if time_series_store is not None:
        try:
            return time_series_store.inspect(
                missing_stock_dates_limit=100,
                statement_non_null_columns=_SIGNAL_STATEMENT_COLUMNS,
            )
        except Exception:
            # 検証は継続し、SQLite統計へフォールバックする
            pass

    topix_range = market_db.get_topix_date_range() or {}
    stock_range = market_db.get_stock_data_date_range() or {}
    indices_range = market_db.get_indices_data_range() or {}
    statement_codes = market_db.get_statement_codes()

    return TimeSeriesInspection(
        source="sqlite-market-db",
        topix_count=int(topix_range.get("count") or 0),
        topix_min=topix_range.get("min"),
        topix_max=topix_range.get("max"),
        stock_count=int(stock_range.get("count") or 0),
        stock_min=stock_range.get("min"),
        stock_max=stock_range.get("max"),
        stock_date_count=int(stock_range.get("dateCount") or 0),
        missing_stock_dates=market_db.get_missing_stock_data_dates(limit=100),
        missing_stock_dates_count=market_db.get_missing_stock_data_dates_count(),
        indices_count=int(indices_range.get("dataCount") or 0),
        latest_indices_dates=market_db.get_latest_indices_data_dates(),
        statements_count=int(basic_stats.get("statements", 0)),
        latest_statement_disclosed_date=market_db.get_latest_statement_disclosed_date(),
        statement_codes=statement_codes,
        statement_non_null_counts=market_db.get_statement_non_null_counts(
            _SIGNAL_STATEMENT_COLUMNS
        ),
    )


def _build_prime_statement_coverage(
    market_db: MarketDb,
    statement_codes: set[str],
    *,
    limit_missing: int = 20,
) -> dict[str, Any]:
    prime_codes = market_db.get_prime_codes()
    covered_codes = sorted(prime_codes & statement_codes)
    missing_codes = sorted(prime_codes - statement_codes)
    prime_count = len(prime_codes)
    covered_count = len(covered_codes)

    coverage_ratio = round((covered_count / prime_count), 4) if prime_count > 0 else 0.0
    return {
        "primeCount": prime_count,
        "coveredCount": covered_count,
        "missingCount": len(missing_codes),
        "coverageRatio": coverage_ratio,
        "missingCodes": missing_codes[: max(limit_missing, 0)],
    }


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

    if "margin" in _SIGNAL_REQUIREMENTS:
        recommendations.append(
            "Margin signal readiness depends on margin data source and is excluded from this check"
        )

    return issues, recommendations


def _collect_missing_signal_requirements(inspection: TimeSeriesInspection) -> list[str]:
    missing: list[str] = []

    for requirement in _SIGNAL_REQUIREMENTS:
        if requirement == "margin":
            continue

        if requirement in {"ohlc", "volume"}:
            if inspection.stock_count <= 0:
                missing.append(requirement)
            continue

        if requirement == "benchmark":
            if inspection.topix_count <= 0:
                missing.append(requirement)
            continue

        if requirement == "sector":
            if inspection.indices_count <= 0:
                missing.append(requirement)
            continue

        if requirement.startswith("statements:"):
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


def _load_metadata_list(market_db: MarketDb, key: str) -> list[str]:
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
