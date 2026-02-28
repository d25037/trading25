from __future__ import annotations

from typing import Any

from src.application.services.db_validation_service import validate_market_db
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
    def __init__(self) -> None:
        self._metadata = {
            "init_completed": "true",
            "last_sync_date": "2026-02-28T00:00:00+00:00",
            "last_stocks_refresh": "2026-02-28T00:00:00+00:00",
        }

    def is_initialized(self) -> bool:
        return True

    def get_sync_metadata(self, key: str) -> str | None:
        return self._metadata.get(key)

    def get_stats(self) -> dict[str, int]:
        return {"stocks": 2, "statements": 4}

    def get_stock_count_by_market(self) -> dict[str, int]:
        return {"プライム": 2}

    def get_adjustment_events(self, limit: int = 20) -> list[dict[str, Any]]:
        del limit
        return []

    def get_stocks_needing_refresh(self, limit: int = 100) -> list[str]:
        del limit
        return []

    def get_prime_codes(self) -> set[str]:
        return {"1301", "7203"}


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

    result = validate_market_db(  # type: ignore[arg-type]
        market_db=market_db,
        time_series_store=store,  # type: ignore[arg-type]
    )

    assert result.stockData.missingDatesCount == 2438
    issue = next(
        (item for item in result.integrityIssues if item.code == "chart.stock_data.missing_dates"),
        None,
    )
    assert issue is not None
    assert issue.count == 2438
    assert any("fill 2438 missing dates" in rec for rec in result.recommendations)
