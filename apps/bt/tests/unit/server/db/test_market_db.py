"""
Tests for MarketDb
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.server.db.market_db import MarketDb
from src.server.db.tables import market_meta


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
