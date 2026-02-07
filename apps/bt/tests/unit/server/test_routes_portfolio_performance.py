"""Portfolio Performance ルートのテスト"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.server.app import create_app
from src.server.db.market_reader import MarketDbReader
from src.server.db.portfolio_db import PortfolioDb


def _create_market_db(path: str) -> None:
    """テスト用 market.db を作成"""
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE stocks (
            code TEXT PRIMARY KEY, company_name TEXT NOT NULL,
            company_name_english TEXT, market_code TEXT NOT NULL,
            market_name TEXT NOT NULL, sector_17_code TEXT NOT NULL,
            sector_17_name TEXT NOT NULL, sector_33_code TEXT NOT NULL,
            sector_33_name TEXT NOT NULL, scale_category TEXT,
            listed_date TEXT NOT NULL, created_at TEXT, updated_at TEXT
        );
        CREATE TABLE stock_data (
            code TEXT NOT NULL, date TEXT NOT NULL,
            open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL,
            close REAL NOT NULL, volume INTEGER NOT NULL,
            adjustment_factor REAL, created_at TEXT,
            PRIMARY KEY (code, date)
        );
        CREATE TABLE topix_data (
            date TEXT PRIMARY KEY, open REAL NOT NULL, high REAL NOT NULL,
            low REAL NOT NULL, close REAL NOT NULL, created_at TEXT
        );

        INSERT INTO stocks VALUES ('72030', 'トヨタ自動車', 'TOYOTA', '0111', 'プライム', '7', '輸送用機器', '3050', '輸送用機器', 'TOPIX Core30', '1949-05-16', NULL, NULL);
        INSERT INTO stocks VALUES ('67580', 'ソニー', 'SONY', '0111', 'プライム', '5', '電気機器', '3650', '電気機器', 'TOPIX Core30', '1958-12-01', NULL, NULL);
    """)
    # stock_data: 100日分の価格データ（現実的な値で）
    dates = [f"2024-{(i // 25) + 1:02d}-{(i % 25) + 1:02d}" for i in range(100)]
    for i, d in enumerate(dates):
        price_7203 = 2500 + i * 5  # 上昇トレンド
        price_6758 = 1500 - i * 2  # 下降トレンド
        topix = 2000 + i * 3
        conn.execute(
            "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)",
            ("72030", d, price_7203 - 10, price_7203 + 10, price_7203 - 15, price_7203, 1000000, 1.0),
        )
        conn.execute(
            "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)",
            ("67580", d, price_6758 - 5, price_6758 + 5, price_6758 - 8, price_6758, 500000, 1.0),
        )
        conn.execute(
            "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, NULL)",
            (d, topix - 10, topix + 10, topix - 15, topix),
        )
    conn.commit()
    conn.close()


@pytest.fixture()
def market_db_path(tmp_path: Path) -> str:
    path = str(tmp_path / "market.db")
    _create_market_db(path)
    return path


@pytest.fixture()
def pdb(tmp_path: Path) -> PortfolioDb:
    db = PortfolioDb(str(tmp_path / "portfolio.db"))
    yield db  # type: ignore[misc]
    db.close()


@pytest.fixture()
def client(pdb: PortfolioDb, market_db_path: str) -> TestClient:
    app = create_app()
    app.state.portfolio_db = pdb
    reader = MarketDbReader(market_db_path)
    app.state.market_reader = reader
    c = TestClient(app, raise_server_exceptions=False)
    yield c  # type: ignore[misc]
    reader.close()


class TestPortfolioPerformance:
    def test_basic(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Test")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-01")
        resp = client.get("/api/portfolio/1/performance")
        assert resp.status_code == 200
        data = resp.json()
        assert data["portfolioId"] == 1
        assert data["portfolioName"] == "Test"
        assert data["summary"]["totalCost"] > 0
        assert data["summary"]["currentValue"] > 0
        assert len(data["holdings"]) == 1
        assert data["holdings"][0]["code"] == "7203"
        assert "analysisDate" in data
        assert isinstance(data["warnings"], list)

    def test_multiple_holdings(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Multi")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-01")
        pdb.add_item(1, "6758", "ソニー", 50, 1500.0, "2024-01-01")
        resp = client.get("/api/portfolio/1/performance")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["holdings"]) == 2
        weights = [h["weight"] for h in data["holdings"]]
        assert abs(sum(weights) - 1.0) < 0.01  # weights sum to ~1

    def test_empty_portfolio(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Empty")
        resp = client.get("/api/portfolio/1/performance")
        assert resp.status_code == 200
        data = resp.json()
        assert data["summary"]["totalCost"] == 0.0
        assert data["holdings"] == []
        assert data["dataPoints"] == 0

    def test_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/portfolio/999/performance")
        assert resp.status_code == 404

    def test_with_benchmark(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Test")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-01")
        resp = client.get("/api/portfolio/1/performance?benchmarkCode=0000&lookbackDays=252")
        assert resp.status_code == 200
        data = resp.json()
        # With enough data, benchmark should be present
        if data["dataPoints"] >= 30:
            assert data["benchmark"] is not None
            assert "beta" in data["benchmark"]
            assert "rSquared" in data["benchmark"]

    def test_no_market_db(self, pdb: PortfolioDb) -> None:
        app = create_app()
        app.state.portfolio_db = pdb
        app.state.market_reader = None
        c = TestClient(app, raise_server_exceptions=False)
        pdb.create_portfolio("Test")
        resp = c.get("/api/portfolio/1/performance")
        assert resp.status_code == 422
