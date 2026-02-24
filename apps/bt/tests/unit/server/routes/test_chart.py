"""
Chart Routes Unit Tests
"""

import pytest

from fastapi.testclient import TestClient

from src.server.app import create_app


@pytest.fixture
def client_with_market_db(market_db_path, monkeypatch):
    """market.db 付きテストクライアント"""
    monkeypatch.setenv("MARKET_DB_PATH", market_db_path)
    monkeypatch.setenv("JQUANTS_API_KEY", "dummy_token_value_0000")
    monkeypatch.setenv("JQUANTS_PLAN", "free")
    from src.config.settings import reload_settings
    reload_settings()
    app = create_app()
    with TestClient(app) as client:
        yield client
    reload_settings()


class TestGetIndicesList:
    def test_200(self, client_with_market_db):
        resp = client_with_market_db.get("/api/chart/indices")
        assert resp.status_code == 200
        data = resp.json()
        assert "indices" in data
        assert "lastUpdated" in data
        assert len(data["indices"]) == 2
        assert data["indices"][0]["code"] == "0000"
        assert data["indices"][0]["name"] == "TOPIX"

    def test_response_shape(self, client_with_market_db):
        resp = client_with_market_db.get("/api/chart/indices")
        data = resp.json()
        idx = data["indices"][0]
        assert set(idx.keys()) == {"code", "name", "nameEnglish", "category", "dataStartDate"}


class TestGetIndexData:
    def test_200(self, client_with_market_db):
        resp = client_with_market_db.get("/api/chart/indices/0000")
        assert resp.status_code == 200
        data = resp.json()
        assert data["code"] == "0000"
        assert data["name"] == "TOPIX"
        assert len(data["data"]) == 3
        assert "lastUpdated" in data

    def test_404(self, client_with_market_db):
        resp = client_with_market_db.get("/api/chart/indices/9999")
        assert resp.status_code == 404

    def test_response_shape(self, client_with_market_db):
        resp = client_with_market_db.get("/api/chart/indices/0001")
        data = resp.json()
        record = data["data"][0]
        assert set(record.keys()) == {"date", "open", "high", "low", "close"}


class TestGetTopixData:
    def test_200(self, client_with_market_db):
        """DB から TOPIX データ取得"""
        resp = client_with_market_db.get("/api/chart/indices/topix")
        assert resp.status_code == 200
        data = resp.json()
        assert "topix" in data
        assert len(data["topix"]) == 3
        assert "lastUpdated" in data

    def test_with_date_params(self, client_with_market_db):
        resp = client_with_market_db.get("/api/chart/indices/topix?from=2024-01-16&to=2024-01-17")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["topix"]) == 2

    def test_422_invalid_range(self, client_with_market_db):
        """from > to"""
        resp = client_with_market_db.get("/api/chart/indices/topix?from=2024-12-31&to=2024-01-01")
        assert resp.status_code == 422

    def test_response_shape(self, client_with_market_db):
        resp = client_with_market_db.get("/api/chart/indices/topix")
        data = resp.json()
        point = data["topix"][0]
        assert set(point.keys()) == {"date", "open", "high", "low", "close", "volume"}


class TestSearchStocks:
    def test_200_by_name(self, client_with_market_db):
        resp = client_with_market_db.get("/api/chart/stocks/search?q=トヨタ")
        assert resp.status_code == 200
        data = resp.json()
        assert data["query"] == "トヨタ"
        assert data["count"] >= 1
        assert data["results"][0]["companyName"] == "トヨタ自動車"

    def test_200_by_code(self, client_with_market_db):
        resp = client_with_market_db.get("/api/chart/stocks/search?q=7203")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] >= 1

    def test_empty_result(self, client_with_market_db):
        resp = client_with_market_db.get("/api/chart/stocks/search?q=zzzzz")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert data["results"] == []

    def test_limit(self, client_with_market_db):
        resp = client_with_market_db.get("/api/chart/stocks/search?q=&limit=1")
        # q が空 → 422 (min_length=1)
        assert resp.status_code == 422

    def test_response_shape(self, client_with_market_db):
        resp = client_with_market_db.get("/api/chart/stocks/search?q=SONY")
        data = resp.json()
        assert set(data.keys()) == {"query", "results", "count"}
        if data["results"]:
            item = data["results"][0]
            assert "code" in item
            assert "companyName" in item
            assert "sector33Name" in item

    def test_route_priority_search_before_symbol(self, client_with_market_db):
        """search ルートが {symbol} よりも先にマッチすること"""
        resp = client_with_market_db.get("/api/chart/stocks/search?q=test")
        assert resp.status_code == 200
        assert "results" in resp.json()


class TestGetStockData:
    def test_200_from_db(self, client_with_market_db):
        """market.db から銘柄チャートデータ取得"""
        resp = client_with_market_db.get("/api/chart/stocks/7203")
        assert resp.status_code == 200
        data = resp.json()
        assert data["symbol"] == "7203"
        assert data["companyName"] == "トヨタ自動車"
        assert data["timeframe"] == "daily"
        assert len(data["data"]) == 3

    def test_response_shape(self, client_with_market_db):
        resp = client_with_market_db.get("/api/chart/stocks/7203")
        data = resp.json()
        assert set(data.keys()) == {"symbol", "companyName", "timeframe", "data", "lastUpdated"}
        point = data["data"][0]
        assert set(point.keys()) == {"time", "open", "high", "low", "close", "volume"}

    def test_404_not_found(self, client_with_market_db):
        """存在しない銘柄"""
        resp = client_with_market_db.get("/api/chart/stocks/0000")
        assert resp.status_code == 404


class TestGetSectorStocks:
    def test_200(self, client_with_market_db):
        resp = client_with_market_db.get("/api/analytics/sector-stocks")
        assert resp.status_code == 200
        data = resp.json()
        assert "stocks" in data
        assert "markets" in data
        assert "lastUpdated" in data
        assert data["lookbackDays"] == 5
        assert data["sortBy"] == "tradingValue"
        assert data["sortOrder"] == "desc"

    def test_with_sector_filter(self, client_with_market_db):
        resp = client_with_market_db.get("/api/analytics/sector-stocks?sector33Name=輸送用機器")
        assert resp.status_code == 200
        data = resp.json()
        assert data["sector33Name"] == "輸送用機器"

    def test_with_market_filter(self, client_with_market_db):
        resp = client_with_market_db.get("/api/analytics/sector-stocks?markets=prime")
        assert resp.status_code == 200
        data = resp.json()
        assert data["markets"] == ["prime"]

    def test_response_shape(self, client_with_market_db):
        resp = client_with_market_db.get("/api/analytics/sector-stocks?markets=prime")
        data = resp.json()
        required_keys = {"markets", "lookbackDays", "sortBy", "sortOrder", "stocks", "lastUpdated"}
        assert required_keys.issubset(set(data.keys()))
        if data["stocks"]:
            stock = data["stocks"][0]
            assert "rank" in stock
            assert "code" in stock
            assert "companyName" in stock
            assert "currentPrice" in stock
