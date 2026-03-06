"""DB Stats + Validate ルートの統合テスト"""

from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection
from src.infrastructure.db.market.market_db import MarketDb


@pytest.fixture
def market_db_path(tmp_path):
    """テスト用 DuckDB market file"""
    db_path = os.path.join(str(tmp_path), "market.duckdb")
    db = MarketDb(db_path, read_only=False)
    db.upsert_stocks([
        {
            "code": "7203",
            "company_name": "トヨタ",
            "company_name_english": "TOYOTA",
            "market_code": "0111",
            "market_name": "プライム",
            "sector_17_code": "7",
            "sector_17_name": "輸送用機器",
            "sector_33_code": "3050",
            "sector_33_name": "輸送用機器",
            "scale_category": "TOPIX Core30",
            "listed_date": "1949-05-16",
        },
        {
            "code": "9984",
            "company_name": "SBG",
            "company_name_english": "SB",
            "market_code": "0112",
            "market_name": "スタンダード",
            "sector_17_code": "9",
            "sector_17_name": "情報・通信",
            "sector_33_code": "3700",
            "sector_33_name": "情報・通信",
            "listed_date": "1994-07-22",
        },
    ])
    db.upsert_stock_data([
        {"code": "7203", "date": "2024-01-04", "open": 100, "high": 110, "low": 90, "close": 105, "volume": 1000, "adjustment_factor": 1.0},
        {"code": "7203", "date": "2024-01-05", "open": 105, "high": 115, "low": 95, "close": 110, "volume": 1100, "adjustment_factor": 0.5},
        {"code": "9984", "date": "2024-01-04", "open": 200, "high": 210, "low": 190, "close": 205, "volume": 500, "adjustment_factor": 1.0},
    ])
    db.upsert_topix_data([
        {"date": "2024-01-04", "open": 2500, "high": 2520, "low": 2490, "close": 2510},
        {"date": "2024-01-05", "open": 2510, "high": 2530, "low": 2500, "close": 2520},
        {"date": "2024-01-06", "open": 2520, "high": 2540, "low": 2510, "close": 2530},
    ])
    db.upsert_indices_data([
        {"code": "0010", "date": "2024-01-04", "open": 100, "high": 102, "low": 99, "close": 101, "sector_name": "食料品"},
    ])
    db.upsert_margin_data([
        {"code": "7203", "date": "2024-01-04", "long_margin_volume": 50000, "short_margin_volume": 30000},
    ])
    db.upsert_index_master([
        {"code": "0010", "name": "食料品", "name_english": "Food", "category": "sector33"},
    ])
    db.set_sync_metadata("init_completed", "true")
    db.set_sync_metadata("last_sync_date", "2024-01-06T10:00:00")
    db.set_sync_metadata("failed_dates", "[\"2024-01-03\"]")
    db.close()
    return db_path


def _build_time_series_store(inspection: TimeSeriesInspection):
    class _Store:
        def inspect(
            self,
            *,
            missing_stock_dates_limit: int = 0,
            statement_non_null_columns: list[str] | None = None,
        ) -> TimeSeriesInspection:
            del missing_stock_dates_limit, statement_non_null_columns
            return inspection

        def close(self) -> None:
            return None

    return _Store()


@pytest.fixture
def client(market_db_path: str):
    app = create_app()
    app.state.market_db = MarketDb(market_db_path, read_only=False)
    app.state.market_time_series_store = _build_time_series_store(
        TimeSeriesInspection(
            source="duckdb-parquet",
            topix_count=3,
            topix_min="2024-01-04",
            topix_max="2024-01-06",
            stock_count=3,
            stock_min="2024-01-04",
            stock_max="2024-01-05",
            stock_date_count=2,
            missing_stock_dates=["2024-01-06"],
            missing_stock_dates_count=1,
            indices_count=1,
            indices_min="2024-01-04",
            indices_max="2024-01-04",
            indices_date_count=1,
            latest_indices_dates={"0010": "2024-01-04"},
            margin_count=1,
            margin_min="2024-01-04",
            margin_max="2024-01-04",
            margin_date_count=1,
            margin_codes={"7203"},
            statements_count=0,
            statement_codes=set(),
        )
    )
    return TestClient(app, raise_server_exceptions=False)


class TestDbStatsRoute:
    def test_stats_success(self, client: TestClient) -> None:
        resp = client.get("/api/db/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["initialized"] is True
        assert data["lastSync"] == "2024-01-06T10:00:00"
        assert data["topix"]["count"] == 3
        assert data["topix"]["dateRange"]["min"] == "2024-01-04"
        assert data["stocks"]["total"] == 2
        assert "プライム" in data["stocks"]["byMarket"]
        assert data["stockData"]["count"] == 3
        assert data["stockData"]["dateCount"] == 2
        assert data["indices"]["masterCount"] == 1
        assert data["indices"]["dataCount"] == 1
        assert data["margin"]["count"] == 1
        assert data["margin"]["uniqueStockCount"] == 1
        assert data["fundamentals"]["count"] == 0
        assert data["fundamentals"]["primeCoverage"]["primeStocks"] >= 1
        assert data["databaseSize"] >= 0

    def test_stats_no_db(self) -> None:
        app = create_app()
        app.state.market_db = None
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/db/stats")
        assert resp.status_code == 422


class TestDbValidateRoute:
    def test_validate_warning(self, client: TestClient) -> None:
        resp = client.get("/api/db/validate")
        assert resp.status_code == 200
        data = resp.json()
        # Has missing dates (topix has 2024-01-06 but stock_data doesn't)
        assert data["status"] in ["healthy", "warning"]
        assert data["initialized"] is True
        assert data["lastSync"] == "2024-01-06T10:00:00"
        assert data["topix"]["count"] == 3
        assert data["stocks"]["total"] == 2
        # stock_data has dates for 2024-01-04 and 2024-01-05 only
        # topix has 2024-01-06 too -> 1 missing date
        assert data["stockData"]["missingDatesCount"] >= 1
        # Adjustment events: 7203 on 2024-01-05 has adjustment_factor=0.5
        assert data["adjustmentEventsCount"] >= 1
        assert data["adjustmentEvents"][0]["adjustmentFactor"] == 0.5
        assert data["stocksNeedingRefreshCount"] == 0
        assert data["failedDatesCount"] == 1
        assert data["margin"]["count"] == 1
        assert "fundamentals" in data
        assert len(data["recommendations"]) > 0

    def test_validate_not_initialized(self, tmp_path) -> None:
        db_path = os.path.join(str(tmp_path), "empty.duckdb")
        db = MarketDb(db_path, read_only=False)
        db.close()
        app = create_app()
        app.state.market_db = MarketDb(db_path, read_only=False)
        app.state.market_time_series_store = _build_time_series_store(
            TimeSeriesInspection(source="duckdb-parquet")
        )
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/db/validate")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "error"
        assert data["initialized"] is False
