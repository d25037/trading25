"""
Screening Service Unit Tests
"""

import sqlite3

import pytest

from src.lib.market_db.market_reader import MarketDbReader
from src.server.services.screening_service import ScreeningService


@pytest.fixture
def screening_db(tmp_path):
    db_path = str(tmp_path / "screening.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    conn.execute("""
        CREATE TABLE stocks (
            code TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            market_code TEXT NOT NULL,
            scale_category TEXT,
            sector_33_name TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE stock_data (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER NOT NULL,
            PRIMARY KEY (code, date)
        )
    """)

    stocks = [
        ("10010", "Numeric Prime", "0111"),
        ("10020", "Legacy Prime", "prime"),
        ("10030", "Numeric Standard", "0112"),
        ("10040", "Legacy Standard", "standard"),
    ]
    for code, company_name, market_code in stocks:
        conn.execute(
            "INSERT INTO stocks (code, company_name, market_code, scale_category, sector_33_name) VALUES (?, ?, ?, ?, ?)",
            (code, company_name, market_code, "TOPIX Small 1", "情報・通信業"),
        )
        conn.execute(
            "INSERT INTO stock_data (code, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (code, "2026-01-06", 100.0, 101.0, 99.0, 100.0, 1000),
        )

    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def service(screening_db):
    reader = MarketDbReader(screening_db)
    yield ScreeningService(reader)
    reader.close()


class TestMarketCodeCompatibility:
    def test_prime_query_matches_legacy_and_numeric_prime(self, service):
        result = service.run_screening(markets="prime")
        assert result.markets == ["prime"]
        assert result.summary.totalStocksScreened == 2
        assert result.summary.skippedCount == 2

    def test_numeric_prime_query_matches_legacy_and_numeric_prime(self, service):
        result = service.run_screening(markets="0111")
        assert result.markets == ["0111"]
        assert result.summary.totalStocksScreened == 2
        assert result.summary.skippedCount == 2

    def test_comma_separated_market_query_expands_all_aliases(self, service):
        result = service.run_screening(markets="prime,standard")
        assert result.markets == ["prime", "standard"]
        assert result.summary.totalStocksScreened == 4
        assert result.summary.skippedCount == 4
