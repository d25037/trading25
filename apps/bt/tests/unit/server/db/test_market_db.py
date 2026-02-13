"""
Tests for MarketDb
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.lib.market_db.market_db import MarketDb
from src.lib.market_db.tables import market_meta


@pytest.fixture()
def market_db(tmp_path: Path) -> MarketDb:
    db_path = str(tmp_path / "market.db")
    db = MarketDb(db_path)
    # Create tables
    market_meta.create_all(db.engine)
    yield db  # type: ignore[misc]
    db.close()


class TestMarketDbStats:
    def test_empty_stats(self, market_db: MarketDb) -> None:
        stats = market_db.get_stats()
        assert stats["stocks"] == 0
        assert stats["stock_data"] == 0
        assert stats["topix_data"] == 0

    def test_stats_after_insert(self, market_db: MarketDb) -> None:
        market_db.upsert_stocks([
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
        ])
        stats = market_db.get_stats()
        assert stats["stocks"] == 1


class TestMarketDbValidateSchema:
    def test_valid_schema(self, market_db: MarketDb) -> None:
        result = market_db.validate_schema()
        assert result["valid"] is True
        assert len(result["missing_tables"]) == 0

    def test_required_tables_present(self, market_db: MarketDb) -> None:
        result = market_db.validate_schema()
        assert "stocks" in result["required_tables"]
        assert "stock_data" in result["required_tables"]


class TestMarketDbSyncMetadata:
    def test_get_nonexistent(self, market_db: MarketDb) -> None:
        assert market_db.get_sync_metadata("nonexistent") is None

    def test_set_and_get(self, market_db: MarketDb) -> None:
        market_db.set_sync_metadata("last_sync", "2024-01-01")
        assert market_db.get_sync_metadata("last_sync") == "2024-01-01"

    def test_set_overwrites(self, market_db: MarketDb) -> None:
        market_db.set_sync_metadata("key", "v1")
        market_db.set_sync_metadata("key", "v2")
        assert market_db.get_sync_metadata("key") == "v2"


class TestMarketDbLatestTradingDate:
    def test_empty(self, market_db: MarketDb) -> None:
        assert market_db.get_latest_trading_date() is None

    def test_with_data(self, market_db: MarketDb) -> None:
        market_db.upsert_topix_data([
            {"date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0},
            {"date": "2024-01-16", "open": 2505.0, "high": 2520.0, "low": 2500.0, "close": 2515.0},
        ])
        assert market_db.get_latest_trading_date() == "2024-01-16"


class TestMarketDbUpsert:
    def test_upsert_stocks(self, market_db: MarketDb) -> None:
        row = {
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
        count = market_db.upsert_stocks([row])
        assert count == 1

    def test_upsert_stocks_replace(self, market_db: MarketDb) -> None:
        row = {
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
        market_db.upsert_stocks([row])
        row["company_name"] = "トヨタ自動車"
        market_db.upsert_stocks([row])
        stats = market_db.get_stats()
        assert stats["stocks"] == 1

    def test_upsert_empty(self, market_db: MarketDb) -> None:
        assert market_db.upsert_stocks([]) == 0

    def test_upsert_stock_data(self, market_db: MarketDb) -> None:
        count = market_db.upsert_stock_data([
            {"code": "7203", "date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0, "volume": 1000000},
        ])
        assert count == 1

    def test_upsert_topix_data(self, market_db: MarketDb) -> None:
        count = market_db.upsert_topix_data([
            {"date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0},
        ])
        assert count == 1

    def test_upsert_indices_data(self, market_db: MarketDb) -> None:
        count = market_db.upsert_indices_data([
            {"code": "0000", "date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0},
        ])
        assert count == 1

    def test_upsert_empty_for_all_tables(self, market_db: MarketDb) -> None:
        assert market_db.upsert_stock_data([]) == 0
        assert market_db.upsert_topix_data([]) == 0
        assert market_db.upsert_indices_data([]) == 0
        assert market_db.upsert_index_master([]) == 0


class TestMarketDbAnalyticsAndValidation:
    def test_latest_dates_and_indices_latest_date_map(self, market_db: MarketDb) -> None:
        assert market_db.get_latest_stock_data_date() is None
        assert market_db.get_latest_indices_data_dates() == {}

        market_db.upsert_stock_data([
            {"code": "7203", "date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0, "volume": 1000000},
            {"code": "7203", "date": "2024-01-16", "open": 2510.0, "high": 2520.0, "low": 2500.0, "close": 2515.0, "volume": 1200000},
        ])
        market_db.upsert_indices_data([
            {"code": "0000", "date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0},
            {"code": "0000", "date": "2024-01-16", "open": 2510.0, "high": 2520.0, "low": 2500.0, "close": 2515.0},
            {"code": "0001", "date": "2024-01-14", "open": 1100.0, "high": 1120.0, "low": 1090.0, "close": 1110.0},
        ])

        assert market_db.get_latest_stock_data_date() == "2024-01-16"
        assert market_db.get_latest_indices_data_dates() == {
            "0000": "2024-01-16",
            "0001": "2024-01-14",
        }

    def test_range_stats_market_counts_and_indices_summary(self, market_db: MarketDb) -> None:
        assert market_db.get_topix_date_range() is None
        assert market_db.get_stock_data_date_range() is None
        empty_indices = market_db.get_indices_data_range()
        assert empty_indices["dataCount"] == 0
        assert empty_indices["dateRange"] is None

        market_db.upsert_stocks([
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
                "code": "6501",
                "company_name": "日立",
                "market_code": "0112",
                "market_name": "スタンダード",
                "sector_17_code": "8",
                "sector_17_name": "電気機器",
                "sector_33_code": "3600",
                "sector_33_name": "電気機器",
                "listed_date": "1949-05-16",
            },
        ])
        market_db.upsert_topix_data([
            {"date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0},
            {"date": "2024-01-16", "open": 2510.0, "high": 2520.0, "low": 2500.0, "close": 2515.0},
        ])
        market_db.upsert_stock_data([
            {"code": "7203", "date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0, "volume": 1000000},
            {"code": "6501", "date": "2024-01-15", "open": 8000.0, "high": 8100.0, "low": 7900.0, "close": 8050.0, "volume": 500000},
            {"code": "7203", "date": "2024-01-16", "open": 2510.0, "high": 2520.0, "low": 2500.0, "close": 2515.0, "volume": 1200000},
        ])
        market_db.upsert_index_master([
            {"code": "0000", "name": "TOPIX", "category": "topix"},
            {"code": "0001", "name": "電気機器", "category": "sector33"},
        ])
        market_db.upsert_indices_data([
            {"code": "0000", "date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0},
            {"code": "0001", "date": "2024-01-15", "open": 1200.0, "high": 1220.0, "low": 1190.0, "close": 1210.0},
        ])

        topix_range = market_db.get_topix_date_range()
        assert topix_range == {"count": 2, "min": "2024-01-15", "max": "2024-01-16"}

        stock_range = market_db.get_stock_data_date_range()
        assert stock_range["count"] == 3
        assert stock_range["dateCount"] == 2
        assert stock_range["averageStocksPerDay"] == 1.5

        by_market = market_db.get_stock_count_by_market()
        assert by_market == {"スタンダード": 1, "プライム": 1}

        indices_range = market_db.get_indices_data_range()
        assert indices_range["masterCount"] == 2
        assert indices_range["dataCount"] == 2
        assert indices_range["dateRange"] == {"min": "2024-01-15", "max": "2024-01-15"}
        assert indices_range["byCategory"] == {"sector33": 1, "topix": 1}

    def test_initialization_and_validation_helpers(self, market_db: MarketDb) -> None:
        assert market_db.is_initialized() is False
        market_db.set_sync_metadata("init_completed", "true")
        assert market_db.is_initialized() is True

        market_db.upsert_topix_data([
            {"date": "2024-01-15", "open": 2500.0, "high": 2510.0, "low": 2490.0, "close": 2505.0},
            {"date": "2024-01-16", "open": 2510.0, "high": 2520.0, "low": 2500.0, "close": 2515.0},
        ])
        market_db.upsert_stock_data([
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
        ])

        assert market_db.get_missing_stock_data_dates() == ["2024-01-15"]
        events = market_db.get_adjustment_events()
        assert len(events) == 2
        assert {event["eventType"] for event in events} == {"stock_split", "reverse_split"}
        assert set(market_db.get_stocks_needing_refresh()) == {"6501", "7203"}
        assert market_db.get_stock_data_unique_date_count() == 1
        assert market_db.get_db_file_size() > 0


class TestMarketDbReadOnly:
    def test_read_only_prevents_write(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "market_ro.db")
        # Create DB with tables first
        rw = MarketDb(db_path)
        market_meta.create_all(rw.engine)
        rw.close()

        # Open read-only
        ro = MarketDb(db_path, read_only=True)
        with pytest.raises(Exception):
            ro.upsert_stocks([{
                "code": "7203",
                "company_name": "トヨタ",
                "market_code": "0111",
                "market_name": "プライム",
                "sector_17_code": "6",
                "sector_17_name": "自動車",
                "sector_33_code": "3700",
                "sector_33_name": "輸送用機器",
                "listed_date": "1949-05-16",
            }])
        ro.close()

    def test_get_db_file_size_handles_os_error(self, market_db: MarketDb, monkeypatch: pytest.MonkeyPatch) -> None:
        def _raise_os_error(_path: str) -> int:
            raise OSError("stat failed")

        monkeypatch.setattr("src.lib.market_db.market_db.os.path.getsize", _raise_os_error)
        assert market_db.get_db_file_size() == 0
