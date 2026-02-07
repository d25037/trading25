"""
Complex Analytics Routes Unit Tests

Ranking, Factor Regression, Screening のルートテスト。
"""

import math
import sqlite3

import pytest
from fastapi.testclient import TestClient

from src.server.app import create_app


def _generate_dates(n: int, start: str = "2023-01-02") -> list[str]:
    """テスト用の営業日リストを生成"""
    from datetime import date, timedelta

    d = date.fromisoformat(start)
    dates: list[str] = []
    while len(dates) < n:
        if d.weekday() < 5:  # 平日のみ
            dates.append(d.isoformat())
        d += timedelta(days=1)
    return dates


@pytest.fixture
def analytics_db_path(tmp_path):
    """factor-regression/screening 用に十分なデータを持つ market.db"""
    db_path = str(tmp_path / "market.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    # テーブル作成
    conn.execute("""
        CREATE TABLE stocks (
            code TEXT PRIMARY KEY, company_name TEXT NOT NULL, company_name_english TEXT,
            market_code TEXT NOT NULL, market_name TEXT NOT NULL,
            sector_17_code TEXT, sector_17_name TEXT,
            sector_33_code TEXT, sector_33_name TEXT NOT NULL,
            scale_category TEXT, listed_date TEXT NOT NULL, created_at TEXT, updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE stock_data (
            code TEXT NOT NULL, date TEXT NOT NULL,
            open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
            volume INTEGER NOT NULL, adjustment_factor REAL, created_at TEXT,
            PRIMARY KEY (code, date)
        )
    """)
    conn.execute("""
        CREATE TABLE topix_data (
            date TEXT PRIMARY KEY, open REAL NOT NULL, high REAL NOT NULL,
            low REAL NOT NULL, close REAL NOT NULL, created_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE index_master (
            code TEXT PRIMARY KEY, name TEXT NOT NULL, name_english TEXT,
            category TEXT NOT NULL, data_start_date TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE indices_data (
            code TEXT NOT NULL, date TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL,
            sector_name TEXT, created_at TEXT,
            PRIMARY KEY (code, date)
        )
    """)

    # 銘柄
    conn.execute(
        "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("72030", "トヨタ自動車", "TOYOTA", "prime", "プライム", "S17", "輸送用機器", "S33", "輸送用機器", "TOPIX Large70", "1949-05-16", None, None),
    )
    conn.execute(
        "INSERT INTO stocks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("67580", "ソニーグループ", "SONY", "prime", "プライム", "S17", "電気機器", "S33", "電気機器", "TOPIX Large70", "1958-12-01", None, None),
    )

    # 300営業日分のデータ生成
    dates = _generate_dates(300)

    # 銘柄データ（ランダムウォーク的）
    import random
    random.seed(42)

    for code, base_price in [("72030", 2500.0), ("67580", 13000.0)]:
        price = base_price
        for d in dates:
            change = random.uniform(-0.02, 0.025)
            price = price * (1 + change)
            o = price * random.uniform(0.99, 1.01)
            h = price * random.uniform(1.0, 1.02)
            lo = price * random.uniform(0.98, 1.0)
            vol = random.randint(500000, 2000000)
            conn.execute(
                "INSERT INTO stock_data VALUES (?,?,?,?,?,?,?,?,?)",
                (code, d, o, h, lo, price, vol, 1.0, None),
            )

    # TOPIX データ
    topix_price = 2500.0
    for d in dates:
        change = random.uniform(-0.01, 0.015)
        topix_price = topix_price * (1 + change)
        conn.execute(
            "INSERT INTO topix_data VALUES (?,?,?,?,?,?)",
            (d, topix_price * 0.99, topix_price * 1.01, topix_price * 0.98, topix_price, None),
        )

    # 指数マスター
    conn.execute("INSERT INTO index_master VALUES (?,?,?,?,?)", ("0000", "TOPIX", "TOPIX", "topix", "2008-05-07"))
    conn.execute("INSERT INTO index_master VALUES (?,?,?,?,?)", ("0040", "水産農林", "Fishery", "sector33", "2010-01-04"))
    conn.execute("INSERT INTO index_master VALUES (?,?,?,?,?)", ("0080", "食品", "Foods", "sector17", "2010-01-04"))

    # 指数データ
    for idx_code, idx_base in [("0000", 2500.0), ("0040", 800.0), ("0080", 1200.0)]:
        idx_price = idx_base
        for d in dates:
            change = random.uniform(-0.012, 0.016)
            idx_price = idx_price * (1 + change)
            conn.execute(
                "INSERT INTO indices_data VALUES (?,?,?,?,?,?,?,?)",
                (idx_code, d, idx_price * 0.99, idx_price * 1.01, idx_price * 0.98, idx_price, None, None),
            )

    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def analytics_client(analytics_db_path, monkeypatch):
    """analytics テスト用クライアント"""
    monkeypatch.setenv("MARKET_DB_PATH", analytics_db_path)
    monkeypatch.setenv("JQUANTS_API_KEY", "test-api-key-12345678")
    monkeypatch.setenv("JQUANTS_PLAN", "free")
    from src.config.settings import reload_settings
    reload_settings()
    app = create_app()
    with TestClient(app) as client:
        yield client
    reload_settings()


# --- Ranking Tests ---


class TestRanking:
    def test_200_default(self, analytics_client):
        resp = analytics_client.get("/api/analytics/ranking")
        assert resp.status_code == 200
        data = resp.json()
        assert "date" in data
        assert "markets" in data
        assert "rankings" in data
        assert "lastUpdated" in data
        rankings = data["rankings"]
        assert "tradingValue" in rankings
        assert "gainers" in rankings
        assert "losers" in rankings
        assert "periodHigh" in rankings
        assert "periodLow" in rankings

    def test_response_shape(self, analytics_client):
        resp = analytics_client.get("/api/analytics/ranking")
        data = resp.json()
        assert data["lookbackDays"] == 1
        assert data["periodDays"] == 250
        assert data["markets"] == ["prime"]

        # tradingValue has items
        if data["rankings"]["tradingValue"]:
            item = data["rankings"]["tradingValue"][0]
            assert "rank" in item
            assert "code" in item
            assert "companyName" in item
            assert "currentPrice" in item

    def test_with_lookback_days(self, analytics_client):
        resp = analytics_client.get("/api/analytics/ranking?lookbackDays=5")
        assert resp.status_code == 200
        data = resp.json()
        assert data["lookbackDays"] == 5

    def test_with_limit(self, analytics_client):
        resp = analytics_client.get("/api/analytics/ranking?limit=1")
        assert resp.status_code == 200
        data = resp.json()
        for cat in ["tradingValue", "gainers", "losers"]:
            assert len(data["rankings"][cat]) <= 1

    def test_422_no_db(self, monkeypatch):
        """DB なしの場合 422"""
        monkeypatch.setenv("JQUANTS_API_KEY", "test-api-key-12345678")
        monkeypatch.setenv("JQUANTS_PLAN", "free")
        monkeypatch.delenv("MARKET_DB_PATH", raising=False)
        from src.config.settings import reload_settings
        reload_settings()
        app = create_app()
        with TestClient(app) as client:
            resp = client.get("/api/analytics/ranking")
            assert resp.status_code == 422
        reload_settings()


# --- Factor Regression Tests ---


class TestFactorRegression:
    def test_200(self, analytics_client):
        resp = analytics_client.get("/api/analytics/factor-regression/7203?lookbackDays=100")
        assert resp.status_code == 200
        data = resp.json()
        assert data["stockCode"] == "7203"
        assert data["companyName"] == "トヨタ自動車"
        assert "marketBeta" in data
        assert "marketRSquared" in data
        assert isinstance(data["sector17Matches"], list)
        assert isinstance(data["sector33Matches"], list)
        assert isinstance(data["topixStyleMatches"], list)
        assert "analysisDate" in data
        assert "dataPoints" in data
        assert "dateRange" in data

    def test_response_numeric_precision(self, analytics_client):
        resp = analytics_client.get("/api/analytics/factor-regression/7203?lookbackDays=100")
        data = resp.json()
        # 数値は3桁精度
        assert isinstance(data["marketBeta"], float)
        assert isinstance(data["marketRSquared"], float)
        assert 0 <= data["marketRSquared"] <= 1

    def test_date_range(self, analytics_client):
        resp = analytics_client.get("/api/analytics/factor-regression/7203?lookbackDays=100")
        data = resp.json()
        dr = data["dateRange"]
        assert "from" in dr
        assert "to" in dr
        assert dr["from"] < dr["to"]
        assert data["dataPoints"] <= 100

    def test_400_invalid_symbol(self, analytics_client):
        resp = analytics_client.get("/api/analytics/factor-regression/ABC")
        assert resp.status_code == 400

    def test_404_not_found(self, analytics_client):
        resp = analytics_client.get("/api/analytics/factor-regression/0000")
        assert resp.status_code == 404

    def test_422_no_db(self, monkeypatch):
        monkeypatch.setenv("JQUANTS_API_KEY", "test-api-key-12345678")
        monkeypatch.setenv("JQUANTS_PLAN", "free")
        monkeypatch.delenv("MARKET_DB_PATH", raising=False)
        from src.config.settings import reload_settings
        reload_settings()
        app = create_app()
        with TestClient(app) as client:
            resp = client.get("/api/analytics/factor-regression/7203")
            assert resp.status_code == 422
        reload_settings()


# --- Screening Tests ---


class TestScreening:
    def test_200_default(self, analytics_client):
        resp = analytics_client.get("/api/analytics/screening")
        assert resp.status_code == 200
        data = resp.json()
        assert "results" in data
        assert "summary" in data
        assert "markets" in data
        assert "recentDays" in data
        assert "lastUpdated" in data

    def test_summary_shape(self, analytics_client):
        resp = analytics_client.get("/api/analytics/screening")
        data = resp.json()
        summary = data["summary"]
        assert "totalStocksScreened" in summary
        assert "matchCount" in summary
        assert "byScreeningType" in summary
        assert isinstance(summary["byScreeningType"], dict)

    def test_with_params(self, analytics_client):
        resp = analytics_client.get(
            "/api/analytics/screening?rangeBreakFast=true&rangeBreakSlow=false&recentDays=5"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["recentDays"] == 5

    def test_sort_by(self, analytics_client):
        resp = analytics_client.get("/api/analytics/screening?sortBy=breakPercentage&order=desc")
        assert resp.status_code == 200

    def test_422_no_db(self, monkeypatch):
        monkeypatch.setenv("JQUANTS_API_KEY", "test-api-key-12345678")
        monkeypatch.setenv("JQUANTS_PLAN", "free")
        monkeypatch.delenv("MARKET_DB_PATH", raising=False)
        from src.config.settings import reload_settings
        reload_settings()
        app = create_app()
        with TestClient(app) as client:
            resp = client.get("/api/analytics/screening")
            assert resp.status_code == 422
        reload_settings()

    def test_result_item_shape(self, analytics_client):
        """結果がある場合のレスポンス形状"""
        resp = analytics_client.get("/api/analytics/screening")
        data = resp.json()
        if data["results"]:
            item = data["results"][0]
            assert "stockCode" in item
            assert "companyName" in item
            assert "screeningType" in item
            assert item["screeningType"] in ("rangeBreakFast", "rangeBreakSlow")
            assert "matchedDate" in item
            assert "details" in item
