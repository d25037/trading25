from __future__ import annotations

from typing import Any

import src.application.services.db_validation_service as db_validation_service
from src.application.services.db_validation_service import (
    _SIGNAL_REQUIREMENTS,
    _build_readiness_issues,
    _collect_missing_signal_requirements,
    _is_statement_requirement_satisfied,
    _load_metadata_list,
    validate_market_db,
)
from src.infrastructure.db.market.market_db import METADATA_KEYS
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection


class DummyTimeSeriesStore:
    def __init__(self, inspection: TimeSeriesInspection) -> None:
        self._inspection = inspection

    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
        statement_non_null_columns: list[str] | None = None,
    ) -> TimeSeriesInspection:
        del missing_stock_dates_limit, statement_non_null_columns
        return self._inspection


class DummyMarketDb:
    def __init__(
        self,
        *,
        initialized: bool = True,
        stocks_needing_refresh: list[str] | None = None,
        adjustment_events: list[dict[str, Any]] | None = None,
        fundamentals_target_codes: set[str] | None = None,
        fundamentals_target_rows: list[dict[str, str]] | None = None,
    ) -> None:
        self._initialized = initialized
        self._stocks_needing_refresh = stocks_needing_refresh or []
        self._adjustment_events = adjustment_events or []
        self._fundamentals_target_codes = fundamentals_target_codes or {"1301", "7203"}
        self._fundamentals_target_rows = fundamentals_target_rows
        self._metadata = {
            "init_completed": "true",
            "last_sync_date": "2026-02-28T00:00:00+00:00",
            "last_stocks_refresh": "2026-02-28T00:00:00+00:00",
        }

    def is_initialized(self) -> bool:
        return self._initialized

    def get_sync_metadata(self, key: str) -> str | None:
        return self._metadata.get(key)

    def get_stats(self) -> dict[str, int]:
        return {"stocks": 2, "statements": 4}

    def get_stock_count_by_market(self) -> dict[str, int]:
        return {"プライム": 2}

    def get_adjustment_events(self, limit: int = 20) -> list[dict[str, Any]]:
        del limit
        return list(self._adjustment_events)

    def get_adjustment_events_count(self) -> int:
        return len(self._adjustment_events)

    def get_stocks_needing_refresh(self, limit: int | None = None) -> list[str]:
        if limit is None:
            return list(self._stocks_needing_refresh)
        return list(self._stocks_needing_refresh[:limit])

    def get_stocks_needing_refresh_count(self) -> int:
        return len(self._stocks_needing_refresh)

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
        stocks_needing_refresh=["7203"],
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
    assert any("repair sync to refresh" in rec for rec in result.recommendations)
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


def test_validate_market_db_limits_refresh_samples_but_uses_total_count() -> None:
    stocks_needing_refresh = [f"{1000 + idx}" for idx in range(25)]
    market_db = DummyMarketDb(stocks_needing_refresh=stocks_needing_refresh)
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
    assert result.stocksNeedingRefreshCount == 25
    assert len(result.stocksNeedingRefresh) == 20
    assert result.sampleWindows.stocksNeedingRefresh.truncated is True
    assert any("refresh 25 stocks" in rec for rec in result.recommendations)


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
            statements_count=2,
            latest_statement_disclosed_date="2026-03-06",
            statement_codes={"2593", "9434"},
            statement_non_null_counts={"earnings_per_share": 2},
        )
    )

    result = validate_market_db(market_db=market_db, time_series_store=store)

    assert result.status == "warning"
    assert result.fundamentals.missingListedMarketStocksCount == 0
    assert result.fundamentals.issuerAliasCoveredCount == 2
    assert result.fundamentals.emptySkippedCount == 1
    assert result.fundamentals.emptySkippedCodes == ["464A"]
    assert result.sampleWindows.fundamentalsEmptySkippedCodes.truncated is False
    assert result.margin.emptySkippedCount == 1
    assert result.margin.emptySkippedCodes == ["4957"]
    assert any("Fundamentals backfill skipped for 1 listed-market stocks" in rec for rec in result.recommendations)
    assert any("Margin backfill skipped for 1 stocks" in rec for rec in result.recommendations)


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


def test_statement_requirement_satisfied_branches() -> None:
    assert _is_statement_requirement_satisfied("EPS", 0, {}) is False
    assert _is_statement_requirement_satisfied("UNKNOWN", 1, {}) is True
    assert _is_statement_requirement_satisfied("EPS", 1, {"earnings_per_share": 1}) is True
    assert _is_statement_requirement_satisfied("EPS", 1, {"earnings_per_share": 0}) is False


def test_load_metadata_list_handles_invalid_and_non_list_payloads() -> None:
    market_db = DummyMarketDb()
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
