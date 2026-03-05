"""
Tests for MarketDb (DuckDB implementation).
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.infrastructure.db.market.market_db import MarketDb


@pytest.fixture()
def market_db(tmp_path: Path) -> MarketDb:
    db_path = str(tmp_path / "market.duckdb")
    db = MarketDb(db_path)
    yield db
    db.close()


def _query_one(db_path: str, sql: str) -> tuple | None:
    conn = duckdb.connect(db_path)
    try:
        return conn.execute(sql).fetchone()
    finally:
        conn.close()


class TestMarketDbBasics:
    def test_empty_stats_and_schema(self, market_db: MarketDb) -> None:
        stats = market_db.get_stats()
        assert stats["stocks"] == 0
        assert stats["stock_data"] == 0
        assert stats["topix_data"] == 0
        assert stats["indices_data"] == 0
        assert stats["sync_metadata"] == 0

        schema = market_db.validate_schema()
        assert schema["valid"] is True
        assert "stocks" in schema["required_tables"]
        assert "stock_data" in schema["required_tables"]
        assert "topix_data" in schema["required_tables"]
        assert "sync_metadata" in schema["required_tables"]

    def test_sync_metadata_roundtrip(self, market_db: MarketDb) -> None:
        assert market_db.get_sync_metadata("nonexistent") is None

        market_db.set_sync_metadata("last_sync", "2024-01-01")
        assert market_db.get_sync_metadata("last_sync") == "2024-01-01"

        market_db.set_sync_metadata("last_sync", "2024-01-02")
        assert market_db.get_sync_metadata("last_sync") == "2024-01-02"

    def test_is_initialized_falls_back_to_existing_data_when_metadata_is_missing(
        self, market_db: MarketDb
    ) -> None:
        assert market_db.get_sync_metadata("init_completed") is None
        assert market_db.is_initialized() is False

        market_db.upsert_stocks(
            [
                {
                    "code": "7203",
                    "company_name": "トヨタ",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "sector_17_code": "6",
                    "sector_17_name": "自動車",
                    "sector_33_code": "3700",
                    "sector_33_name": "輸送用機器",
                    "listed_date": "1949-05-16",
                }
            ]
        )
        market_db.upsert_stock_data(
            [
                {
                    "code": "7203",
                    "date": "2024-01-15",
                    "open": 2500.0,
                    "high": 2510.0,
                    "low": 2490.0,
                    "close": 2505.0,
                    "volume": 1000000,
                }
            ]
        )

        assert market_db.is_initialized() is True

    def test_is_initialized_prioritizes_explicit_metadata_flag(
        self, market_db: MarketDb
    ) -> None:
        market_db.upsert_stocks(
            [
                {
                    "code": "7203",
                    "company_name": "トヨタ",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "sector_17_code": "6",
                    "sector_17_name": "自動車",
                    "sector_33_code": "3700",
                    "sector_33_name": "輸送用機器",
                    "listed_date": "1949-05-16",
                }
            ]
        )
        market_db.upsert_stock_data(
            [
                {
                    "code": "7203",
                    "date": "2024-01-15",
                    "open": 2500.0,
                    "high": 2510.0,
                    "low": 2490.0,
                    "close": 2505.0,
                    "volume": 1000000,
                }
            ]
        )
        market_db.set_sync_metadata("init_completed", "false")

        assert market_db.is_initialized() is False


class TestMarketDbUpserts:
    def test_upsert_stocks_and_counts(self, market_db: MarketDb) -> None:
        count = market_db.upsert_stocks(
            [
                {
                    "code": "7203",
                    "company_name": "トヨタ",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "sector_17_code": "6",
                    "sector_17_name": "自動車",
                    "sector_33_code": "3700",
                    "sector_33_name": "輸送用機器",
                    "listed_date": "1949-05-16",
                }
            ]
        )
        assert count == 1
        assert market_db.get_stats()["stocks"] == 1

        market_db.upsert_stocks(
            [
                {
                    "code": "7203",
                    "company_name": "トヨタ自動車",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "sector_17_code": "6",
                    "sector_17_name": "自動車",
                    "sector_33_code": "3700",
                    "sector_33_name": "輸送用機器",
                    "listed_date": "1949-05-16",
                }
            ]
        )
        assert market_db.get_stats()["stocks"] == 1
        row = _query_one(
            market_db.db_path,
            "SELECT company_name FROM stocks WHERE code='7203'",
        )
        assert row is not None
        assert row[0] == "トヨタ自動車"

    def test_upsert_timeseries_rows(self, market_db: MarketDb) -> None:
        assert market_db.upsert_stock_data(
            [
                {
                    "code": "7203",
                    "date": "2024-01-15",
                    "open": 2500.0,
                    "high": 2510.0,
                    "low": 2490.0,
                    "close": 2505.0,
                    "volume": 1000000,
                }
            ]
        ) == 1
        assert market_db.upsert_topix_data(
            [{"date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0}]
        ) == 1
        assert market_db.upsert_indices_data(
            [{"code": "0000", "date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0}]
        ) == 1

    def test_upsert_statements_merges_non_null_fields_on_conflict(self, market_db: MarketDb) -> None:
        market_db.upsert_statements(
            [
                {
                    "code": "1899",
                    "disclosed_date": "2026-02-13",
                    "type_of_current_period": "FY",
                    "type_of_document": "EarnForecastRevision",
                    "forecast_eps": 580.0,
                }
            ]
        )
        market_db.upsert_statements(
            [
                {
                    "code": "1899",
                    "disclosed_date": "2026-02-13",
                    "type_of_current_period": "FY",
                    "type_of_document": "DividendForecastRevision",
                    "dividend_fy": 200.0,
                    "forecast_eps": None,
                }
            ]
        )

        row = _query_one(
            market_db.db_path,
            """
            SELECT forecast_eps, dividend_fy, type_of_document
            FROM statements
            WHERE code='1899' AND disclosed_date='2026-02-13'
            """,
        )
        assert row is not None
        assert row[0] == 580.0
        assert row[1] == 200.0
        assert row[2] == "DividendForecastRevision"

        market_db.upsert_statements(
            [
                {
                    "code": "1899",
                    "disclosed_date": "2026-02-13",
                    "type_of_current_period": "FY",
                    "type_of_document": "EarnForecastRevision",
                    "forecast_eps": 604.0,
                }
            ]
        )
        updated = _query_one(
            market_db.db_path,
            """
            SELECT forecast_eps, dividend_fy, type_of_document
            FROM statements
            WHERE code='1899' AND disclosed_date='2026-02-13'
            """,
        )
        assert updated is not None
        assert updated[0] == 604.0
        assert updated[1] == 200.0
        assert updated[2] == "EarnForecastRevision"

    def test_upsert_index_master_preserves_child_rows(self, market_db: MarketDb) -> None:
        market_db.upsert_index_master(
            [{"code": "0000", "name": "TOPIX", "category": "topix"}]
        )
        market_db.upsert_indices_data(
            [{"code": "0000", "date": "2026-02-10", "close": 100.0}]
        )
        market_db.upsert_index_master(
            [{"code": "0000", "name": "TOPIX Updated", "category": "topix"}]
        )

        name_row = _query_one(
            market_db.db_path,
            "SELECT name FROM index_master WHERE code='0000'",
        )
        count_row = _query_one(
            market_db.db_path,
            "SELECT COUNT(*) FROM indices_data WHERE code='0000'",
        )
        assert name_row is not None and name_row[0] == "TOPIX Updated"
        assert count_row is not None and count_row[0] == 1


class TestMarketDbDerivedStats:
    def test_latest_dates_and_ranges(self, market_db: MarketDb) -> None:
        assert market_db.get_latest_trading_date() is None
        assert market_db.get_latest_stock_data_date() is None
        assert market_db.get_latest_indices_data_dates() == {}
        assert market_db.get_topix_date_range() is None
        assert market_db.get_stock_data_date_range() is None

        market_db.upsert_topix_data(
            [
                {"date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0},
                {"date": "2024-01-16", "open": 2505.0, "high": 2520.0, "low": 2500.0, "close": 2515.0},
            ]
        )
        market_db.upsert_stock_data(
            [
                {"code": "7203", "date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0, "volume": 1000000},
                {"code": "7203", "date": "2024-01-16", "open": 2510.0, "high": 2520.0, "low": 2500.0, "close": 2515.0, "volume": 1200000},
            ]
        )
        market_db.upsert_index_master(
            [{"code": "0000", "name": "TOPIX", "category": "topix"}]
        )
        market_db.upsert_indices_data(
            [
                {"code": "0000", "date": "2024-01-15", "close": 2505.0},
                {"code": "0000", "date": "2024-01-16", "close": 2515.0},
                {"code": "0001", "date": "2024-01-14", "close": 1110.0},
            ]
        )

        assert market_db.get_latest_trading_date() == "2024-01-16"
        assert market_db.get_latest_stock_data_date() == "2024-01-16"
        assert market_db.get_latest_indices_data_dates() == {
            "0000": "2024-01-16",
            "0001": "2024-01-14",
        }

        topix_range = market_db.get_topix_date_range()
        assert topix_range == {"count": 2, "min": "2024-01-15", "max": "2024-01-16"}

        stock_range = market_db.get_stock_data_date_range()
        assert stock_range is not None
        assert stock_range["count"] == 2
        assert stock_range["dateCount"] == 2
        assert stock_range["averageStocksPerDay"] == 1.0

        indices_range = market_db.get_indices_data_range()
        assert indices_range is not None
        assert indices_range["masterCount"] == 1
        assert indices_range["dataCount"] == 3
        assert indices_range["dateRange"] == {"min": "2024-01-14", "max": "2024-01-16"}

    def test_stock_refresh_and_missing_date_helpers(self, market_db: MarketDb) -> None:
        market_db.upsert_topix_data(
            [
                {"date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0},
                {"date": "2024-01-16", "open": 2510.0, "high": 2520.0, "low": 2500.0, "close": 2515.0},
            ]
        )
        market_db.upsert_stock_data(
            [
                {
                    "code": "7203",
                    "date": "2024-01-16",
                    "open": 2510.0,
                    "high": 2520.0,
                    "low": 2500.0,
                    "close": 2515.0,
                    "volume": 1200000,
                    "adjustment_factor": 0.5,
                },
                {
                    "code": "6501",
                    "date": "2024-01-16",
                    "open": 8000.0,
                    "high": 8100.0,
                    "low": 7900.0,
                    "close": 8050.0,
                    "volume": 500000,
                    "adjustment_factor": 1.5,
                },
            ]
        )

        assert market_db.get_missing_stock_data_dates() == ["2024-01-15"]
        assert market_db.get_missing_stock_data_dates_count() == 1
        events = market_db.get_adjustment_events()
        assert len(events) == 2
        assert {event["eventType"] for event in events} == {"stock_split", "reverse_split"}
        assert set(market_db.get_stocks_needing_refresh()) == {"6501", "7203"}
        assert market_db.get_stock_data_unique_date_count() == 1
        assert market_db.get_db_file_size() > 0


class TestMarketDbFundamentals:
    def test_prime_coverage_helpers(self, market_db: MarketDb) -> None:
        market_db.upsert_stocks(
            [
                {
                    "code": "7203",
                    "company_name": "トヨタ",
                    "market_code": "0111",
                    "market_name": "プライム",
                    "sector_17_code": "6",
                    "sector_17_name": "自動車",
                    "sector_33_code": "3700",
                    "sector_33_name": "輸送用機器",
                    "listed_date": "1949-05-16",
                },
                {
                    "code": "6758",
                    "company_name": "ソニー",
                    "market_code": "prime",
                    "market_name": "プライム",
                    "sector_17_code": "8",
                    "sector_17_name": "電気機器",
                    "sector_33_code": "3600",
                    "sector_33_name": "電気機器",
                    "listed_date": "1958-12-01",
                },
                {
                    "code": "9999",
                    "company_name": "NonPrime",
                    "market_code": "0112",
                    "market_name": "スタンダード",
                    "sector_17_code": "8",
                    "sector_17_name": "電気機器",
                    "sector_33_code": "3600",
                    "sector_33_name": "電気機器",
                    "listed_date": "1958-12-01",
                },
            ]
        )
        market_db.upsert_statements(
            [
                {
                    "code": "7203",
                    "disclosed_date": "2024-05-10",
                    "earnings_per_share": 100.0,
                    "profit": 1000.0,
                    "equity": 2000.0,
                    "type_of_current_period": "FY",
                },
                {
                    "code": "9999",
                    "disclosed_date": "2024-05-09",
                    "earnings_per_share": 10.0,
                    "profit": 100.0,
                    "equity": 200.0,
                    "type_of_current_period": "FY",
                },
            ]
        )

        assert market_db.get_latest_statement_disclosed_date() == "2024-05-10"
        assert market_db.get_statement_codes() == {"7203", "9999"}
        assert market_db.get_prime_codes() == {"6758", "7203"}

        coverage = market_db.get_prime_statement_coverage()
        assert coverage["primeCount"] == 2
        assert coverage["coveredCount"] == 1
        assert coverage["missingCount"] == 1
        assert coverage["missingCodes"] == ["6758"]


class TestMarketDbReadOnly:
    def test_read_only_prevents_write(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "market_ro.duckdb")
        rw = MarketDb(db_path)
        rw.close()

        ro = MarketDb(db_path, read_only=True)
        with pytest.raises(PermissionError):
            ro.upsert_stocks(
                [
                    {
                        "code": "7203",
                        "company_name": "トヨタ",
                        "market_code": "0111",
                        "market_name": "プライム",
                        "sector_17_code": "6",
                        "sector_17_name": "自動車",
                        "sector_33_code": "3700",
                        "sector_33_name": "輸送用機器",
                        "listed_date": "1949-05-16",
                    }
                ]
            )
        ro.close()

    def test_get_db_file_size_handles_os_error(
        self, market_db: MarketDb, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _raise_os_error(_path: str) -> int:
            raise OSError("stat failed")

        monkeypatch.setattr(
            "src.infrastructure.db.market.market_db.os.path.getsize",
            _raise_os_error,
        )
        assert market_db.get_db_file_size() == 0


class TestMarketDbEdgeCases:
    def test_upsert_methods_return_zero_for_empty_rows(self, market_db: MarketDb) -> None:
        assert market_db.upsert_stocks([]) == 0
        assert market_db.upsert_stock_data([]) == 0
        assert market_db.upsert_topix_data([]) == 0
        assert market_db.upsert_indices_data([]) == 0
        assert market_db.upsert_statements([]) == 0
        assert market_db.upsert_index_master([]) == 0

    def test_methods_return_safe_defaults_when_tables_are_missing(self, market_db: MarketDb) -> None:
        for table in (
            "sync_metadata",
            "topix_data",
            "stock_data",
            "indices_data",
            "index_master",
            "statements",
            "stocks",
        ):
            market_db._execute(f"DROP TABLE IF EXISTS {table}")

        # Missing-table guards
        assert market_db.get_sync_metadata("any") is None
        assert market_db.get_latest_trading_date() is None
        assert market_db.get_latest_stock_data_date() is None
        assert market_db.get_latest_indices_data_dates() == {}
        assert market_db.get_index_master_codes() == set()
        assert market_db.get_latest_statement_disclosed_date() is None
        assert market_db.get_statement_codes() == set()
        assert market_db.get_prime_codes() == set()
        assert market_db.get_stock_count_by_market() == {}
        assert market_db.get_topix_date_range() is None
        assert market_db.get_stock_data_date_range() is None
        assert market_db.get_missing_stock_data_dates(limit=0) == []
        assert market_db.get_missing_stock_data_dates(limit=10) == []
        assert market_db.get_missing_stock_data_dates_count() == 0
        assert market_db.get_adjustment_events(limit=0) == []
        assert market_db.get_stocks_needing_refresh(limit=0) == []
        assert market_db.get_stock_data_unique_date_count() == 0

        # Coverage helper branches
        assert market_db.get_statement_non_null_counts([]) == {}
        assert market_db.get_statement_non_null_counts(["eps", "profit"]) == {
            "eps": 0,
            "profit": 0,
        }
        coverage = market_db.get_prime_statement_coverage(limit_missing=None)
        assert coverage["primeCount"] == 0
        assert coverage["coveredCount"] == 0
        assert coverage["missingCount"] == 0
        assert coverage["missingCodes"] == []

        indices_range = market_db.get_indices_data_range()
        assert indices_range == {
            "masterCount": 0,
            "dataCount": 0,
            "dateCount": 0,
            "dateRange": None,
            "byCategory": {},
        }
        assert market_db.get_stats()["index_master"] == 0

    def test_get_indices_data_range_handles_empty_table(self, market_db: MarketDb) -> None:
        market_db.upsert_index_master(
            [
                {"code": "0000", "name": "TOPIX", "category": "topix"},
                {"code": "0040", "name": "水産", "category": "sector33"},
            ]
        )
        result = market_db.get_indices_data_range()
        assert result is not None
        assert result["masterCount"] == 2
        assert result["dataCount"] == 0
        assert result["dateCount"] == 0
        assert result["dateRange"] is None
        assert result["byCategory"] == {"topix": 1, "sector33": 1}

    def test_ensure_schema_adds_missing_statements_columns(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "legacy-market.duckdb")
        conn = duckdb.connect(db_path)
        try:
            conn.execute(
                """
                CREATE TABLE statements (
                    code TEXT,
                    disclosed_date TEXT,
                    earnings_per_share DOUBLE,
                    profit DOUBLE,
                    equity DOUBLE,
                    type_of_current_period TEXT,
                    type_of_document TEXT,
                    next_year_forecast_earnings_per_share DOUBLE,
                    bps DOUBLE,
                    sales DOUBLE,
                    operating_profit DOUBLE,
                    ordinary_profit DOUBLE,
                    operating_cash_flow DOUBLE,
                    dividend_fy DOUBLE,
                    forecast_eps DOUBLE,
                    investing_cash_flow DOUBLE,
                    financing_cash_flow DOUBLE,
                    cash_and_equivalents DOUBLE,
                    total_assets DOUBLE,
                    shares_outstanding DOUBLE,
                    treasury_shares DOUBLE,
                    PRIMARY KEY (code, disclosed_date)
                )
                """
            )
        finally:
            conn.close()

        db = MarketDb(db_path)
        try:
            columns = {
                str(row[1])
                for row in db._execute("PRAGMA table_info('statements')").fetchall()
                if row and len(row) > 1
            }
            assert "forecast_dividend_fy" in columns
            assert "next_year_forecast_dividend_fy" in columns
            assert "payout_ratio" in columns
            assert "forecast_payout_ratio" in columns
            assert "next_year_forecast_payout_ratio" in columns
        finally:
            db.close()
