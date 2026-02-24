"""Watchlist Prices Service テスト"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.infrastructure.db.market.portfolio_db import PortfolioDb
from src.application.services.watchlist_prices_service import WatchlistPricesService


def _create_market_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE stock_data (
            code TEXT NOT NULL, date TEXT NOT NULL,
            open REAL, high REAL, low REAL,
            close REAL NOT NULL, volume INTEGER NOT NULL,
            adjustment_factor REAL, created_at TEXT,
            PRIMARY KEY (code, date)
        );
        INSERT INTO stock_data VALUES ('72030', '2024-01-04', 100, 110, 90, 2500, 1000000, 1.0, NULL);
        INSERT INTO stock_data VALUES ('72030', '2024-01-05', 100, 110, 90, 2600, 1200000, 1.0, NULL);
        INSERT INTO stock_data VALUES ('67580', '2024-01-04', 100, 110, 90, 1500, 500000, 1.0, NULL);
        INSERT INTO stock_data VALUES ('67580', '2024-01-05', 100, 110, 90, 1450, 600000, 1.0, NULL);
    """)
    conn.close()


@pytest.fixture()
def service(tmp_path: Path) -> WatchlistPricesService:
    market_path = str(tmp_path / "market.db")
    _create_market_db(market_path)
    reader = MarketDbReader(market_path)
    pdb = PortfolioDb(str(tmp_path / "portfolio.db"))
    svc = WatchlistPricesService(reader, pdb)
    yield svc  # type: ignore[misc]
    reader.close()
    pdb.close()


@pytest.fixture()
def pdb(service: WatchlistPricesService) -> PortfolioDb:
    return service._pdb


class TestWatchlistPricesService:
    def test_basic(self, service: WatchlistPricesService, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("Tech")
        pdb.add_watchlist_item(1, "7203", "トヨタ")
        result = service.get_prices(1)
        assert len(result.prices) == 1
        assert result.prices[0].code == "7203"
        assert result.prices[0].close == 2600
        assert result.prices[0].prevClose == 2500
        assert result.prices[0].changePercent == 4.0  # (2600-2500)/2500*100

    def test_negative_change(self, service: WatchlistPricesService, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("Tech")
        pdb.add_watchlist_item(1, "6758", "ソニー")
        result = service.get_prices(1)
        assert len(result.prices) == 1
        p = result.prices[0]
        assert p.close == 1450
        assert p.prevClose == 1500
        assert p.changePercent is not None
        assert p.changePercent < 0

    def test_empty_watchlist(self, service: WatchlistPricesService, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("Empty")
        result = service.get_prices(1)
        assert result.prices == []

    def test_not_found(self, service: WatchlistPricesService) -> None:
        with pytest.raises(ValueError, match="not found"):
            service.get_prices(999)

    def test_multiple(self, service: WatchlistPricesService, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("Multi")
        pdb.add_watchlist_item(1, "7203", "トヨタ")
        pdb.add_watchlist_item(1, "6758", "ソニー")
        result = service.get_prices(1)
        assert len(result.prices) == 2
        codes = {p.code for p in result.prices}
        assert codes == {"7203", "6758"}
