"""Watchlist Prices ルートのテスト"""

from __future__ import annotations

from collections.abc import Generator
import duckdb
import shutil
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.infrastructure.db.market.portfolio_db import PortfolioDb


def _create_market_db(path: str) -> None:
    """テスト用 market.duckdb を作成"""
    conn = duckdb.connect(path)
    conn.execute("""
        CREATE TABLE stock_data (
            code TEXT NOT NULL, date TEXT NOT NULL,
            open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL,
            close REAL NOT NULL, volume INTEGER NOT NULL,
            adjustment_factor REAL, created_at TEXT,
            PRIMARY KEY (code, date)
        );

        INSERT INTO stock_data VALUES ('72030', '2024-01-04', 100, 110, 90, 2500, 1000000, 1.0, NULL);
        INSERT INTO stock_data VALUES ('72030', '2024-01-05', 100, 110, 90, 2550, 1100000, 1.0, NULL);
        INSERT INTO stock_data VALUES ('67580', '2024-01-04', 100, 110, 90, 1500, 500000, 1.0, NULL);
        INSERT INTO stock_data VALUES ('67580', '2024-01-05', 100, 110, 90, 1480, 600000, 1.0, NULL);
    """)
    conn.close()


@pytest.fixture(scope="module")
def market_db_template_path(tmp_path_factory) -> str:
    tmp_path = tmp_path_factory.mktemp("watchlist-prices")
    path = str(tmp_path / "market-template.duckdb")
    _create_market_db(path)
    return path


@pytest.fixture()
def market_db_path(tmp_path: Path, market_db_template_path: str) -> str:
    path = str(tmp_path / "market.duckdb")
    shutil.copyfile(market_db_template_path, path)
    return path


@pytest.fixture()
def pdb(tmp_path: Path) -> Generator[PortfolioDb, None, None]:
    db = PortfolioDb(str(tmp_path / "portfolio.db"))
    yield db
    db.close()


@pytest.fixture(scope="module")
def app_client() -> Generator[TestClient, None, None]:
    app = create_app()
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


@pytest.fixture()
def client(
    app_client: TestClient,
    pdb: PortfolioDb,
    market_db_path: str,
) -> Generator[TestClient, None, None]:
    reader = MarketDbReader(market_db_path)
    app_client.app.state.portfolio_db = pdb
    app_client.app.state.market_reader = reader
    try:
        yield app_client
    finally:
        reader.close()


class TestWatchlistPrices:
    def test_basic(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("Tech")
        pdb.add_watchlist_item(1, "7203", "トヨタ")
        resp = client.get("/api/watchlist/1/prices")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["prices"]) == 1
        p = data["prices"][0]
        assert p["code"] == "7203"
        assert p["close"] == 2550
        assert p["prevClose"] == 2500
        assert p["changePercent"] == 2.0  # (2550-2500)/2500*100

    def test_multiple_stocks(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("Multi")
        pdb.add_watchlist_item(1, "7203", "トヨタ")
        pdb.add_watchlist_item(1, "6758", "ソニー")
        resp = client.get("/api/watchlist/1/prices")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["prices"]) == 2

    def test_empty_watchlist(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_watchlist("Empty")
        resp = client.get("/api/watchlist/1/prices")
        assert resp.status_code == 200
        assert resp.json()["prices"] == []

    def test_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/watchlist/999/prices")
        assert resp.status_code == 404
