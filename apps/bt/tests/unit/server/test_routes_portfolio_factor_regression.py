"""Portfolio Factor Regression ルートのテスト"""

from __future__ import annotations

from collections.abc import Generator
import duckdb
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.infrastructure.db.market.portfolio_db import PortfolioDb


def _create_market_db(path: str) -> None:
    """テスト用 market.duckdb を作成（index_master + indices_data 含む）"""
    conn = duckdb.connect(path)
    conn.execute("""
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
        CREATE TABLE indices_data (
            code TEXT NOT NULL, date TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL,
            sector_name TEXT, created_at TEXT,
            PRIMARY KEY (code, date)
        );
        CREATE TABLE index_master (
            code TEXT PRIMARY KEY, name TEXT NOT NULL, category TEXT NOT NULL
        );

        INSERT INTO stocks VALUES ('72030', 'トヨタ自動車', 'TOYOTA', '0111', 'プライム', '7', '輸送用機器', '3050', '輸送用機器', 'TOPIX Core30', '1949-05-16', NULL, NULL);
        INSERT INTO stocks VALUES ('67580', 'ソニー', 'SONY', '0111', 'プライム', '5', '電気機器', '3650', '電気機器', 'TOPIX Core30', '1958-12-01', NULL, NULL);

        INSERT INTO index_master VALUES ('0000', 'TOPIX', 'topix');
        INSERT INTO index_master VALUES ('0001', 'TOPIX Core30', 'topix');
        INSERT INTO index_master VALUES ('1001', '食品', 'sector17');
        INSERT INTO index_master VALUES ('2001', '食料品', 'sector33');
    """)
    # stock_data: 100日分の価格データ
    dates = [f"2024-{(i // 25) + 1:02d}-{(i % 25) + 1:02d}" for i in range(100)]
    stock_rows: list[tuple[str, str, float, float, float, float, int, float]] = []
    topix_rows: list[tuple[str, float, float, float, float]] = []
    index_rows: list[tuple[str, str, float]] = []
    for i, d in enumerate(dates):
        price_7203 = 2500 + i * 5
        price_6758 = 1500 + i * 3
        topix = 2000 + i * 3
        stock_rows.extend([
            ("72030", d, price_7203 - 10, price_7203 + 10, price_7203 - 15, price_7203, 1000000, 1.0),
            ("67580", d, price_6758 - 5, price_6758 + 5, price_6758 - 8, price_6758, 500000, 1.0),
        ])
        topix_rows.append((d, topix - 10, topix + 10, topix - 15, topix))
        # indices_data for each index
        for idx_code in ["0000", "0001", "1001", "2001"]:
            idx_close = topix + int(idx_code) * 0.01 + i * 2
            index_rows.append((idx_code, d, idx_close))
    conn.executemany(
        "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)",
        stock_rows,
    )
    conn.executemany(
        "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, NULL)",
        topix_rows,
    )
    conn.executemany(
        "INSERT INTO indices_data VALUES (?, ?, NULL, NULL, NULL, ?, NULL, NULL)",
        index_rows,
    )
    conn.close()


@pytest.fixture(scope="module")
def market_db_path(tmp_path_factory) -> str:
    tmp_path = tmp_path_factory.mktemp("portfolio-factor-regression")
    path = str(tmp_path / "market.duckdb")
    _create_market_db(path)
    return path


@pytest.fixture()
def pdb(tmp_path: Path) -> Generator[PortfolioDb, None, None]:
    db = PortfolioDb(str(tmp_path / "portfolio.db"))
    yield db
    db.close()


@pytest.fixture(scope="module")
def market_reader(market_db_path: str) -> Generator[MarketDbReader, None, None]:
    reader = MarketDbReader(market_db_path)
    yield reader
    reader.close()


@pytest.fixture(scope="module")
def app_client() -> Generator[TestClient, None, None]:
    app = create_app()
    client = TestClient(app, raise_server_exceptions=False)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture()
def client(
    app_client: TestClient,
    pdb: PortfolioDb,
    market_reader: MarketDbReader,
) -> Generator[TestClient, None, None]:
    app_client.app.state.portfolio_db = pdb
    app_client.app.state.market_reader = market_reader
    yield app_client


class TestPortfolioFactorRegression:
    def test_basic(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Test")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-01")
        pdb.add_item(1, "6758", "ソニー", 50, 1500.0, "2024-01-01")
        resp = client.get("/api/analytics/portfolio-factor-regression/1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["portfolioId"] == 1
        assert data["portfolioName"] == "Test"
        assert len(data["weights"]) == 2
        assert data["stockCount"] == 2
        assert data["includedStockCount"] == 2
        assert "marketBeta" in data
        assert "marketRSquared" in data
        assert "dateRange" in data

    def test_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/analytics/portfolio-factor-regression/999")
        assert resp.status_code == 404

    def test_empty_portfolio(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Empty")
        resp = client.get("/api/analytics/portfolio-factor-regression/1")
        assert resp.status_code == 422

    def test_weights_sum(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Test")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-01")
        pdb.add_item(1, "6758", "ソニー", 50, 1500.0, "2024-01-01")
        resp = client.get("/api/analytics/portfolio-factor-regression/1")
        data = resp.json()
        weights = [w["weight"] for w in data["weights"]]
        assert abs(sum(weights) - 1.0) < 0.01

    def test_lookback_days(self, client: TestClient, pdb: PortfolioDb) -> None:
        pdb.create_portfolio("Test")
        pdb.add_item(1, "7203", "トヨタ", 100, 2500.0, "2024-01-01")
        resp = client.get("/api/analytics/portfolio-factor-regression/1?lookbackDays=60")
        assert resp.status_code == 200
        assert resp.json()["dataPoints"] <= 60

    def test_no_market_db(self, pdb: PortfolioDb) -> None:
        app = create_app()
        with TestClient(app, raise_server_exceptions=False) as client:
            app.state.portfolio_db = pdb
            app.state.market_reader = None
            resp = client.get("/api/analytics/portfolio-factor-regression/1")
            assert resp.status_code == 422
