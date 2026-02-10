"""
Chart Service Unit Tests
"""

import sqlite3

import pytest

from src.lib.market_db.market_reader import MarketDbReader
from src.server.services.chart_service import ChartService


@pytest.fixture
def chart_db(tmp_path):
    db_path = str(tmp_path / "chart.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    conn.execute("""
        CREATE TABLE stocks (
            code TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            company_name_english TEXT,
            market_code TEXT NOT NULL,
            market_name TEXT NOT NULL,
            sector_17_code TEXT NOT NULL,
            sector_17_name TEXT NOT NULL,
            sector_33_code TEXT NOT NULL,
            sector_33_name TEXT NOT NULL,
            scale_category TEXT,
            listed_date TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT
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
            adjustment_factor REAL,
            created_at TEXT,
            PRIMARY KEY (code, date)
        )
    """)

    stocks = [
        ("10010", "Legacy Prime", "LPRIME", "prime"),
        ("10020", "Numeric Prime", "NPRIME", "0111"),
        ("10030", "Legacy Standard", "LSTD", "standard"),
        ("10040", "Numeric Standard", "NSTD", "0112"),
    ]
    for i, (code, company_name, company_name_english, market_code) in enumerate(stocks):
        conn.execute(
            "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                code,
                company_name,
                company_name_english,
                market_code,
                "Market",
                "S17",
                "セクター17",
                "S33",
                "セクター33",
                "TOPIX Small 1",
                "2020-01-01",
                None,
                None,
            ),
        )
        conn.execute(
            "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (code, "2026-02-06", 100 + i, 110 + i, 95 + i, 105 + i, 100_000 + i * 1000, 1.0, None),
        )

    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def service(chart_db):
    reader = MarketDbReader(chart_db)
    yield ChartService(reader, None)
    reader.close()


class TestSectorStocksMarketCodeCompatibility:
    def test_prime_query_matches_legacy_and_numeric_prime(self, service):
        result = service.get_sector_stocks(markets="prime")
        assert result is not None
        assert result.markets == ["prime"]
        assert len(result.stocks) == 2
        assert {item.marketCode for item in result.stocks} == {"prime", "0111"}

    def test_numeric_prime_query_matches_legacy_and_numeric_prime(self, service):
        result = service.get_sector_stocks(markets="0111")
        assert result is not None
        assert result.markets == ["0111"]
        assert len(result.stocks) == 2
        assert {item.marketCode for item in result.stocks} == {"prime", "0111"}

    def test_comma_separated_market_query_expands_all_aliases(self, service):
        result = service.get_sector_stocks(markets="prime,standard")
        assert result is not None
        assert result.markets == ["prime", "standard"]
        assert len(result.stocks) == 4
