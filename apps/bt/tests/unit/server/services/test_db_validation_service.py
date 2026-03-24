from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import src.application.services.db_validation_service as db_validation_service
from src.application.services.db_validation_service import (
    _SIGNAL_REQUIREMENTS,
    _build_readiness_issues,
    _collect_missing_signal_requirements,
    _is_options_225_local_data_stale,
    _is_statement_requirement_satisfied,
    _load_metadata_list,
    _resolve_missing_dates_count,
    _resolve_options_225_missing_topix_coverage_dates,
    _resolve_options_225_missing_topix_coverage_dates_count,
    validate_market_db,
)
from src.infrastructure.db.market.market_db import (
    LOCAL_STOCK_PRICE_ADJUSTMENT_MODE,
    METADATA_KEYS,
)
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection


class DummyTimeSeriesStore:
    def __init__(self, inspection: TimeSeriesInspection) -> None:
        self._inspection = inspection

    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
        missing_options_225_dates_limit: int = 0,
        statement_non_null_columns: list[str] | None = None,
    ) -> TimeSeriesInspection:
        del missing_stock_dates_limit, missing_options_225_dates_limit, statement_non_null_columns
        return self._inspection


class DummyMarketDb:
    def __init__(
        self,
        *,
        initialized: bool = True,
        adjustment_events: list[dict[str, Any]] | None = None,
        fundamentals_target_codes: set[str] | None = None,
        fundamentals_target_rows: list[dict[str, str]] | None = None,
        options_225_missing_underlying_dates: list[str] | None = None,
        options_225_conflicting_underlying_dates: list[str] | None = None,
        legacy_stock_snapshot: bool = False,
        stock_price_adjustment_mode: str | None = "local_projection_v1",
    ) -> None:
        self._initialized = initialized
        self._adjustment_events = adjustment_events or []
        self._fundamentals_target_codes = fundamentals_target_codes or {"1301", "7203"}
        self._fundamentals_target_rows = fundamentals_target_rows
        self._options_225_missing_underlying_dates = options_225_missing_underlying_dates or []
        self._options_225_conflicting_underlying_dates = options_225_conflicting_underlying_dates or []
        self._legacy_stock_snapshot = legacy_stock_snapshot
        self._stock_price_adjustment_mode = stock_price_adjustment_mode
        self._metadata = {
            "init_completed": "true",
            "last_sync_date": "2026-02-28T00:00:00+00:00",
            "last_stocks_refresh": "2026-02-28T00:00:00+00:00",
        }

    def is_initialized(self) -> bool:
        return self._initialized

    def is_legacy_stock_price_snapshot(self) -> bool:
        return self._legacy_stock_snapshot

    def get_stock_price_adjustment_mode(self) -> str | None:
        return self._stock_price_adjustment_mode

    def get_sync_metadata(self, key: str) -> str | None:
        return self._metadata.get(key)

    def get_stats(self) -> dict[str, int]:
        return {"stocks": 2, "statements": 4}

    def get_stock_count_by_market(self) -> dict[str, int]:
        return {"プライム": 2}

    def get_adjustment_events(self, limit: int = 20) -> list[dict[str, Any]]:
        return list(self._adjustment_events[:limit])

    def get_adjustment_events_count(self) -> int:
        return len(self._adjustment_events)

    def get_stocks_needing_refresh(self, limit: int | None = None) -> list[str]:
        del limit
        return []

    def get_stocks_needing_refresh_count(self) -> int:
        return 0

    def get_fundamentals_target_codes(self) -> set[str]:
        return set(self._fundamentals_target_codes)

    def get_fundamentals_target_stock_rows(self) -> list[dict[str, str]]:
        if self._fundamentals_target_rows is not None:
            return list(self._fundamentals_target_rows)
        return [
            {
                "code": code,
                "company_name": "",
                "market_code": "0111",
            }
            for code in sorted(self._fundamentals_target_codes)
        ]

    def get_options_225_underlying_price_issue_dates(
        self,
        *,
        issue_type: str,
        limit: int = 20,
    ) -> list[str]:
        issues = self._resolve_options_225_issue_dates(issue_type)
        return list(issues[:limit])

    def get_options_225_underlying_price_issue_count(self, *, issue_type: str) -> int:
        return len(self._resolve_options_225_issue_dates(issue_type))

    def _resolve_options_225_issue_dates(self, issue_type: str) -> list[str]:
        if issue_type == "missing":
            return list(self._options_225_missing_underlying_dates)
        if issue_type == "conflicting":
            return list(self._options_225_conflicting_underlying_dates)
        raise ValueError(f"Unsupported options_225 issue type: {issue_type}")


def test_validate_market_db_uses_missing_dates_total_count_from_inspection() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore(
        TimeSeriesInspection(
            source="duckdb-parquet",
            topix_count=3000,
            topix_min="2016-02-29",
            topix_max="2026-02-27",
            stock_count=1,
            stock_min="2016-02-29",
            stock_max="2016-03-04",
            stock_date_count=5,
            missing_stock_dates=["2026-02-27"],
            missing_stock_dates_count=2438,
            indices_count=100,
            latest_indices_dates={"0000": "2026-02-27"},
            statements_count=10,
            latest_statement_disclosed_date="2026-02-27",
            statement_codes={"1301", "7203"},
        )
    )

    result = validate_market_db(
        market_db=market_db,
        time_series_store=store,
    )

    assert result.margin.count == 0
    assert result.stockData.missingDatesCount == 2438
    assert result.sampleWindows.stockDataMissingDates.truncated is True
    issue = next(
        (item for item in result.integrityIssues if item.code == "chart.stock_data.missing_dates"),
        None,
    )
    assert issue is not None
    assert issue.count == 2438
    assert any("fill 2438 missing dates" in rec for rec in result.recommendations)


def test_validate_market_db_returns_error_and_recommendations_for_uninitialized_store() -> None:
    market_db = DummyMarketDb(
        initialized=False,
        adjustment_events=[
            {
                "code": "7203",
                "date": "2026-02-27",
                "adjustmentFactor": 0.5,
                "close": 1000.0,
                "eventType": "split",
            }
        ],
        fundamentals_target_codes={"1301", "7203"},
    )
    market_db._metadata[METADATA_KEYS["FAILED_DATES"]] = "{invalid-json"
    market_db._metadata[METADATA_KEYS["FUNDAMENTALS_FAILED_DATES"]] = '["2026-02-27"]'
    market_db._metadata[METADATA_KEYS["FUNDAMENTALS_FAILED_CODES"]] = '["7203"]'

    store = DummyTimeSeriesStore(
        TimeSeriesInspection(
            source="duckdb-parquet",
            topix_count=0,
            stock_count=0,
            stock_date_count=0,
            indices_count=0,
            statements_count=0,
            statement_codes=set(),
            missing_stock_dates=["2026-02-26", "2026-02-27"],
            missing_stock_dates_count=2,
        )
    )

    result = validate_market_db(market_db=market_db, time_series_store=store)

    assert result.status == "error"
    assert any("Run initial sync" in rec for rec in result.recommendations)
    assert any("repair sync to backfill fundamentals" in rec for rec in result.recommendations)
    assert any("failed fundamentals dates" in rec for rec in result.recommendations)
    assert any("failed fundamentals codes" in rec for rec in result.recommendations)
    assert result.adjustmentEventsCount == 1
    assert result.failedDatesCount == 0
    assert result.fundamentals.missingListedMarketStocksCount == 2


def test_validate_market_db_warns_when_failed_dates_metadata_exists() -> None:
    market_db = DummyMarketDb()
    market_db._metadata[METADATA_KEYS["FAILED_DATES"]] = '["2026-02-27"]'

    store = DummyTimeSeriesStore(
        TimeSeriesInspection(
            source="duckdb-parquet",
            topix_count=10,
            stock_count=10,
            stock_date_count=3,
            indices_count=10,
            statements_count=10,
            statement_codes={"1301", "7203"},
            statement_non_null_counts={"earnings_per_share": 10},
        )
    )

    result = validate_market_db(market_db=market_db, time_series_store=store)

    assert result.status == "warning"
    assert result.failedDatesCount == 1
    assert any("failed sync dates" in rec for rec in result.recommendations)


def test_validate_market_db_flags_legacy_stock_snapshot_as_reset_required() -> None:
    market_db = DummyMarketDb(
        initialized=True,
        legacy_stock_snapshot=True,
        stock_price_adjustment_mode=None,
    )
    store = DummyTimeSeriesStore(
        TimeSeriesInspection(
            source="duckdb-parquet",
            topix_count=10,
            stock_count=10,
            stock_date_count=3,
            indices_count=10,
            statements_count=10,
            statement_codes={"1301", "7203"},
            statement_non_null_counts={"earnings_per_share": 10},
        )
    )

    result = validate_market_db(market_db=market_db, time_series_store=store)

    assert result.status == "error"
    assert result.stocksNeedingRefreshCount == 0
    assert result.stocksNeedingRefresh == []
    assert result.sampleWindows.stocksNeedingRefresh.totalCount == 0
    assert any("Reset market-timeseries/market.duckdb" in rec for rec in result.recommendations)


def test_validate_market_db_recommends_reset_before_enabling_local_projection() -> None:
    market_db = DummyMarketDb(
        initialized=True,
        legacy_stock_snapshot=False,
        stock_price_adjustment_mode="server_adjusted_v0",
    )
    store = DummyTimeSeriesStore(
        TimeSeriesInspection(
            source="duckdb-parquet",
            topix_count=10,
            topix_max="2026-03-06",
            stock_count=10,
            stock_max="2026-03-06",
            stock_date_count=3,
            indices_count=10,
            options_225_count=4,
            options_225_max="2026-03-06",
            options_225_date_count=2,
            margin_count=2,
            margin_date_count=1,
            statements_count=2,
            latest_statement_disclosed_date="2026-03-06",
            statement_codes={"1301", "7203"},
            statement_non_null_counts={
                column: 1 for column in db_validation_service._SIGNAL_STATEMENT_COLUMNS
            },
        )
    )

    result = validate_market_db(market_db=market_db, time_series_store=store)

    assert result.status == "healthy"
    assert market_db.get_stock_price_adjustment_mode() != LOCAL_STOCK_PRICE_ADJUSTMENT_MODE
    assert any(
        "enable local stock price projection" in rec for rec in result.recommendations
    )
    assert not any(
        "Reset market-timeseries/market.duckdb" in rec for rec in result.recommendations
    )


def test_validate_market_db_limits_adjustment_event_samples_but_uses_total_count() -> None:
    adjustment_events = [
        {
            "code": f"{1000 + idx}",
            "date": f"2026-02-{(idx % 28) + 1:02d}",
            "adjustmentFactor": 0.5,
            "close": 1000.0 + idx,
            "eventType": "split",
        }
        for idx in range(25)
    ]
    market_db = DummyMarketDb(adjustment_events=adjustment_events)
    store = DummyTimeSeriesStore(
        TimeSeriesInspection(
            source="duckdb-parquet",
            topix_count=10,
            stock_count=10,
            stock_date_count=3,
            indices_count=10,
            statements_count=10,
            statement_codes={"1301", "7203"},
            statement_non_null_counts={"earnings_per_share": 10},
        )
    )

    result = validate_market_db(market_db=market_db, time_series_store=store)

    assert result.adjustmentEventsCount == 25
    assert len(result.adjustmentEvents) == 20
    assert result.sampleWindows.adjustmentEvents.truncated is True


def test_validate_market_db_reports_options_225_underlying_issues() -> None:
    market_db = DummyMarketDb(
        options_225_missing_underlying_dates=["2024-01-16"],
        options_225_conflicting_underlying_dates=["2024-01-17"],
    )
    store = DummyTimeSeriesStore(
        TimeSeriesInspection(
            source="duckdb-parquet",
            topix_count=10,
            stock_count=10,
            stock_date_count=3,
            indices_count=10,
            options_225_count=4,
            options_225_min="2024-01-16",
            options_225_max="2024-01-17",
            options_225_date_count=2,
            statements_count=10,
            statement_codes={"1301", "7203"},
            statement_non_null_counts={"earnings_per_share": 10},
        )
    )

    result = validate_market_db(market_db=market_db, time_series_store=store)

    assert result.status == "warning"
    assert result.options225.count == 4
    assert result.options225.missingUnderlyingPriceDatesCount == 1
    assert result.options225.conflictingUnderlyingPriceDatesCount == 1
    assert result.sampleWindows.options225MissingUnderlyingPriceDates.truncated is False
    assert result.sampleWindows.options225ConflictingUnderlyingPriceDates.truncated is False


def test_validate_market_db_treats_fundamentals_empty_skips_as_info() -> None:
    market_db = DummyMarketDb(
        fundamentals_target_codes={"25935", "464A"},
        fundamentals_target_rows=[
            {"code": "25935", "company_name": "伊藤園（優先株式）", "market_code": "0111"},
            {"code": "464A", "company_name": "ＱＰＳホールディングス", "market_code": "0113"},
        ],
    )
    market_db._metadata[METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"]] = "2026-03-06"
    market_db._metadata[METADATA_KEYS["FUNDAMENTALS_EMPTY_CODES"]] = (
        '{"frontier":"2026-03-06","codes":["464A"]}'
    )

    store = DummyTimeSeriesStore(
        TimeSeriesInspection(
            source="duckdb-parquet",
            topix_count=10,
            topix_max="2026-03-06",
            stock_count=10,
            stock_date_count=3,
            stock_max="2026-03-06",
            indices_count=10,
            options_225_count=4,
            options_225_max="2026-03-06",
            options_225_date_count=2,
            margin_count=1,
            margin_date_count=1,
            statements_count=1,
            latest_statement_disclosed_date="2026-03-06",
            statement_codes={"2593"},
            statement_non_null_counts={
                column: 1 for column in db_validation_service._SIGNAL_STATEMENT_COLUMNS
            },
        )
    )

    result = validate_market_db(market_db=market_db, time_series_store=store)

    assert result.status == "healthy"
    assert result.fundamentals.missingListedMarketStocksCount == 0
    assert result.fundamentals.emptySkippedCount == 1
    assert result.fundamentals.emptySkippedCodes == ["464A"]
    assert not any("Fundamentals backfill skipped" in rec for rec in result.recommendations)


def test_validate_market_db_warns_when_options_225_local_data_is_missing() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore(
        TimeSeriesInspection(
            source="duckdb-parquet",
            topix_count=10,
            topix_max="2026-03-06",
            stock_count=10,
            stock_date_count=3,
            indices_count=10,
            statements_count=10,
            latest_statement_disclosed_date="2026-03-06",
            statement_codes={"1301", "7203"},
            statement_non_null_counts={"earnings_per_share": 10},
        )
    )

    result = validate_market_db(market_db=market_db, time_series_store=store)

    assert result.status == "warning"
    assert result.options225.count == 0
    assert any(
        "Run indices-only sync to ingest N225 options data into options_225_data" in rec
        for rec in result.recommendations
    )


def test_validate_market_db_warns_when_options_225_local_data_is_stale() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore(
        TimeSeriesInspection(
            source="duckdb-parquet",
            topix_count=10,
            topix_max="2026-03-06",
            stock_count=10,
            stock_date_count=3,
            indices_count=10,
            options_225_count=4,
            options_225_max="2026-03-04",
            options_225_date_count=2,
            statements_count=10,
            latest_statement_disclosed_date="2026-03-06",
            statement_codes={"1301", "7203"},
            statement_non_null_counts={"earnings_per_share": 10},
        )
    )

    result = validate_market_db(market_db=market_db, time_series_store=store)

    assert result.status == "warning"
    assert any(
        "Run indices-only sync to refresh N225 options data through 2026-03-06" in rec
        for rec in result.recommendations
    )
    assert any("latest local options date: 2026-03-04" in rec for rec in result.recommendations)


def test_validate_market_db_warns_when_options_225_local_history_is_partial() -> None:
    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore(
        TimeSeriesInspection(
            source="duckdb-parquet",
            topix_count=10,
            topix_min="2026-03-01",
            topix_max="2026-03-06",
            stock_count=10,
            stock_date_count=3,
            indices_count=10,
            options_225_count=4,
            options_225_min="2026-03-06",
            options_225_max="2026-03-06",
            options_225_date_count=1,
            missing_options_225_dates_count=25,
            missing_options_225_dates=["2026-03-05", "2026-03-04", "2026-03-03"],
            statements_count=10,
            latest_statement_disclosed_date="2026-03-06",
            statement_codes={"1301", "7203"},
            statement_non_null_counts={"earnings_per_share": 10},
        )
    )

    result = validate_market_db(market_db=market_db, time_series_store=store)

    assert result.status == "warning"
    assert result.options225.missingTopixCoverageDatesCount == 25
    assert result.options225.missingTopixCoverageDates == [
        "2026-03-05",
        "2026-03-04",
        "2026-03-03",
    ]
    assert result.sampleWindows.options225MissingTopixCoverageDates.truncated is True
    assert any(
        "Run indices-only sync to backfill N225 options history for 25 TOPIX dates missing from options_225_data"
        in rec
        for rec in result.recommendations
    )


def test_validate_market_db_applies_alias_coverage_and_frontier_empty_caches() -> None:
    market_db = DummyMarketDb(
        fundamentals_target_codes={"25935", "464A", "94345"},
        fundamentals_target_rows=[
            {"code": "25935", "company_name": "伊藤園（優先株式）", "market_code": "0111"},
            {"code": "464A", "company_name": "ＱＰＳホールディングス", "market_code": "0113"},
            {"code": "94345", "company_name": "ソフトバンク（優先株式）", "market_code": "0111"},
        ],
    )
    market_db._metadata[METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"]] = "2026-03-06"
    market_db._metadata[METADATA_KEYS["FUNDAMENTALS_EMPTY_CODES"]] = (
        '{"frontier":"2026-03-06","codes":["464A"]}'
    )
    market_db._metadata[METADATA_KEYS["MARGIN_EMPTY_CODES"]] = (
        '{"frontier":"2026-03-06","codes":["4957"]}'
    )

    store = DummyTimeSeriesStore(
        TimeSeriesInspection(
            source="duckdb-parquet",
            topix_count=10,
            topix_max="2026-03-06",
            stock_count=10,
            stock_date_count=3,
            stock_max="2026-03-06",
            indices_count=10,
            options_225_count=4,
            options_225_max="2026-03-06",
            options_225_date_count=2,
            margin_count=1,
            margin_date_count=1,
            statements_count=2,
            latest_statement_disclosed_date="2026-03-06",
            statement_codes={"2593", "9434"},
            statement_non_null_counts={
                column: 2 for column in db_validation_service._SIGNAL_STATEMENT_COLUMNS
            },
        )
    )

    result = validate_market_db(market_db=market_db, time_series_store=store)

    assert result.status == "healthy"
    assert result.fundamentals.missingListedMarketStocksCount == 0
    assert result.fundamentals.issuerAliasCoveredCount == 2
    assert result.fundamentals.emptySkippedCount == 1
    assert result.fundamentals.emptySkippedCodes == ["464A"]
    assert result.sampleWindows.fundamentalsEmptySkippedCodes.truncated is False
    assert result.margin.emptySkippedCount == 1
    assert result.margin.emptySkippedCodes == ["4957"]
    assert not any("Fundamentals backfill skipped" in rec for rec in result.recommendations)
    assert not any("Margin backfill skipped" in rec for rec in result.recommendations)


def test_build_readiness_issues_marks_all_missing_branches() -> None:
    inspection = TimeSeriesInspection(
        source="duckdb-parquet",
        topix_count=0,
        stock_count=0,
        indices_count=0,
        statements_count=0,
        missing_stock_dates=["2026-02-27"],
        missing_stock_dates_count=1,
        statement_non_null_counts={},
    )

    issues, recommendations = _build_readiness_issues(inspection)
    codes = {issue.code for issue in issues}

    assert "chart.topix_data.missing" in codes
    assert "chart.stock_data.missing" in codes
    assert "chart.indices_data.missing" in codes
    assert "chart.stock_data.missing_dates" in codes
    if any(req.startswith("statements:") for req in _SIGNAL_REQUIREMENTS):
        assert "backtest.signal_requirements.missing" in codes
    if "margin" in _SIGNAL_REQUIREMENTS:
        assert any("unmet requirements" in rec and "margin" in rec for rec in recommendations)


def test_build_readiness_issues_reports_margin_orphans_and_truncated_signal_sample(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        db_validation_service,
        "_collect_missing_signal_requirements",
        lambda _inspection: [f"signal_req_{idx}" for idx in range(7)],
    )
    inspection = TimeSeriesInspection(
        source="duckdb-parquet",
        topix_count=1,
        stock_count=1,
        indices_count=1,
        margin_orphan_count=2,
        statements_count=1,
        statement_non_null_counts={"earnings_per_share": 1},
    )

    issues, recommendations = _build_readiness_issues(inspection)

    assert any(issue.code == "backtest.margin_data.orphans" for issue in issues)
    assert any(
        "signal_req_0" in rec and "..." in rec for rec in recommendations
    )


def test_build_readiness_issues_returns_empty_when_all_dependencies_are_ready(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        db_validation_service,
        "_collect_missing_signal_requirements",
        lambda _inspection: [],
    )
    inspection = TimeSeriesInspection(
        source="duckdb-parquet",
        topix_count=1,
        stock_count=1,
        indices_count=1,
        margin_count=1,
        statements_count=1,
        statement_non_null_counts={"earnings_per_share": 1},
    )

    issues, recommendations = _build_readiness_issues(inspection)

    assert issues == []
    assert recommendations == []


def test_collect_missing_signal_requirements_covers_ohlc_benchmark_sector_and_statements() -> None:
    inspection = TimeSeriesInspection(
        source="duckdb-parquet",
        topix_count=0,
        stock_count=0,
        indices_count=0,
        statements_count=1,
        statement_non_null_counts={},
    )

    missing = _collect_missing_signal_requirements(inspection)
    if "ohlc" in _SIGNAL_REQUIREMENTS:
        assert "ohlc" in missing
    if "benchmark" in _SIGNAL_REQUIREMENTS:
        assert "benchmark" in missing
    if "margin" in _SIGNAL_REQUIREMENTS:
        assert "margin" in missing
    if "sector" in _SIGNAL_REQUIREMENTS:
        assert "sector" in missing
    if any(req.startswith("statements:") for req in _SIGNAL_REQUIREMENTS):
        assert any(req.startswith("statements:") for req in missing)


def test_collect_missing_signal_requirements_covers_satisfied_and_unsatisfied_domains(
    monkeypatch: Any,
) -> None:
    requirements = [
        "market_req",
        "margin_req",
        "benchmark_req",
        "sector_req",
        "statements:EPS",
        "statements:ROE",
        "statements:ForwardForecastEPS",
    ]
    domain_map = {
        "market_req": "market",
        "margin_req": "margin",
        "benchmark_req": "benchmark",
        "sector_req": "sector",
    }
    monkeypatch.setattr(db_validation_service, "_SIGNAL_REQUIREMENTS", requirements)
    monkeypatch.setattr(
        db_validation_service,
        "resolve_feature_requirement_spec",
        lambda requirement: SimpleNamespace(
            data_domain=domain_map.get(requirement, "statements")
        ),
    )
    inspection = TimeSeriesInspection(
        source="duckdb-parquet",
        topix_count=1,
        stock_count=1,
        indices_count=0,
        margin_count=0,
        statements_count=1,
        statement_non_null_counts={
            "earnings_per_share": 1,
            "profit": 1,
            "next_year_forecast_earnings_per_share": 1,
        },
    )

    missing = _collect_missing_signal_requirements(inspection)

    assert missing == ["margin_req", "sector_req", "statements:ROE"]


def test_statement_requirement_satisfied_branches() -> None:
    assert _is_statement_requirement_satisfied("EPS", 0, {}) is False
    assert _is_statement_requirement_satisfied("UNKNOWN", 1, {}) is True
    assert _is_statement_requirement_satisfied("EPS", 1, {"earnings_per_share": 1}) is True
    assert _is_statement_requirement_satisfied("EPS", 1, {"earnings_per_share": 0}) is False
    assert _is_statement_requirement_satisfied(
        "ForwardForecastEPS",
        1,
        {"next_year_forecast_earnings_per_share": 1},
    ) is True
    assert _is_statement_requirement_satisfied(
        "ForwardForecastEPS",
        1,
        {
            "forecast_eps": 0,
            "next_year_forecast_earnings_per_share": 0,
        },
    ) is False


def test_load_metadata_list_handles_invalid_and_non_list_payloads() -> None:
    market_db = DummyMarketDb()
    assert _load_metadata_list(market_db, "missing") == []

    market_db._metadata["custom"] = "{broken"
    assert _load_metadata_list(market_db, "custom") == []

    market_db._metadata["custom"] = '{"v": 1}'
    assert _load_metadata_list(market_db, "custom") == []

    market_db._metadata["custom"] = '["a", 1, "b"]'
    assert _load_metadata_list(market_db, "custom") == ["a", "b"]


def test_signal_requirement_columns_for_branches(monkeypatch: Any) -> None:
    monkeypatch.setattr(db_validation_service, "_SIGNAL_REQUIREMENTS", ["statements:EPS"])
    inspection = TimeSeriesInspection(
        source="duckdb-parquet",
        statements_count=1,
        statement_non_null_counts={"earnings_per_share": 1},
    )
    missing = _collect_missing_signal_requirements(inspection)
    assert missing == []


def test_options_225_helpers_cover_stale_and_missing_history_branches() -> None:
    empty = TimeSeriesInspection(source="duckdb-parquet")
    assert _is_options_225_local_data_stale(empty) is False
    assert _resolve_options_225_missing_topix_coverage_dates_count(empty) == 0
    assert _resolve_options_225_missing_topix_coverage_dates(empty) == []

    populated = TimeSeriesInspection(
        source="duckdb-parquet",
        topix_count=2,
        topix_max="2026-03-06",
        options_225_count=1,
        options_225_max="2026-03-05",
        missing_options_225_dates_count=2,
        missing_options_225_dates=["2026-03-04"],
    )
    assert _is_options_225_local_data_stale(populated) is True
    assert _resolve_options_225_missing_topix_coverage_dates_count(populated) == 2
    assert _resolve_options_225_missing_topix_coverage_dates(populated) == ["2026-03-04"]


def test_resolve_missing_dates_count_prefers_sample_count_when_greater() -> None:
    inspection = TimeSeriesInspection(
        source="duckdb-parquet",
        missing_stock_dates=["2026-03-05", "2026-03-06"],
        missing_stock_dates_count=1,
    )

    assert _resolve_missing_dates_count(inspection) == 2
