"""
MarketDataService market-code alias tests.
"""

import sqlite3

import pytest

from src.lib.market_db.market_reader import MarketDbReader
from src.server.services.market_data_service import MarketDataService


@pytest.fixture
def market_alias_db(tmp_path):
    db_path = str(tmp_path / "market-alias.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    conn.execute("""
        CREATE TABLE stocks (
            code TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            market_code TEXT NOT NULL
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
        ("10010", "Legacy Prime", "prime"),
        ("10020", "Numeric Prime", "0111"),
        ("10030", "Legacy Standard", "standard"),
        ("10040", "Numeric Standard", "0112"),
    ]
    for i, (code, company_name, market_code) in enumerate(stocks):
        conn.execute(
            "INSERT INTO stocks (code, company_name, market_code) VALUES (?, ?, ?)",
            (code, company_name, market_code),
        )
        conn.execute(
            "INSERT INTO stock_data (code, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (code, "2026-02-06", 100 + i, 101 + i, 99 + i, 100 + i, 1000 + i),
        )

    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def service(market_alias_db):
    reader = MarketDbReader(market_alias_db)
    yield MarketDataService(reader)
    reader.close()


class TestGetAllStocksMarketCodeCompatibility:
    def test_prime_query_matches_legacy_and_numeric_prime(self, service):
        result = service.get_all_stocks(market="prime", history_days=30)
        assert result is not None
        assert len(result) == 2
        assert {item.code for item in result} == {"10010", "10020"}

    def test_standard_query_matches_legacy_and_numeric_standard(self, service):
        result = service.get_all_stocks(market="standard", history_days=30)
        assert result is not None
        assert len(result) == 2
        assert {item.code for item in result} == {"10030", "10040"}

    def test_numeric_prime_query_also_matches_legacy_prime(self, service):
        result = service.get_all_stocks(market="0111", history_days=30)
        assert result is not None
        assert len(result) == 2
        assert {item.code for item in result} == {"10010", "10020"}
