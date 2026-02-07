"""
Market Data Routes Unit Tests

sync_client を使用してルートの E2E テスト。
"""

import pytest

from fastapi.testclient import TestClient

from src.server.app import create_app


@pytest.fixture
def client_with_market_db(market_db_path, monkeypatch):
    """market.db 付きテストクライアント"""
    monkeypatch.setenv("MARKET_DB_PATH", market_db_path)
    monkeypatch.setenv("JQUANTS_API_KEY", "test-api-key-12345678")
    monkeypatch.setenv("JQUANTS_PLAN", "free")
    # settings cache をクリア
    from src.config.settings import reload_settings
    reload_settings()
    app = create_app()
    with TestClient(app) as client:
        yield client
    reload_settings()


class TestGetAllStocks:
    def test_200_prime(self, client_with_market_db):
        """プライム市場の全銘柄データ"""
        resp = client_with_market_db.get("/api/market/stocks")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 2
        assert "code" in data[0]
        assert "company_name" in data[0]
        assert "data" in data[0]

    def test_200_standard(self, client_with_market_db):
        """スタンダード市場"""
        resp = client_with_market_db.get("/api/market/stocks?market=standard")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        # standard にはデータなし
        assert len(data) == 0

    def test_422_invalid_market(self, client_with_market_db):
        """無効な market パラメータ"""
        resp = client_with_market_db.get("/api/market/stocks?market=invalid")
        assert resp.status_code == 422

    def test_422_invalid_history_days(self, client_with_market_db):
        """無効な history_days"""
        resp = client_with_market_db.get("/api/market/stocks?history_days=0")
        assert resp.status_code == 422

    def test_default_params(self, client_with_market_db):
        """デフォルトパラメータ (market=prime, history_days=300)"""
        resp = client_with_market_db.get("/api/market/stocks")
        assert resp.status_code == 200


class TestGetStockInfo:
    def test_200(self, client_with_market_db):
        """銘柄情報取得"""
        resp = client_with_market_db.get("/api/market/stocks/7203")
        assert resp.status_code == 200
        data = resp.json()
        assert data["code"] == "72030"
        assert data["companyName"] == "トヨタ自動車"
        assert data["companyNameEnglish"] == "TOYOTA MOTOR"
        assert data["marketCode"] == "prime"

    def test_200_5digit(self, client_with_market_db):
        """5桁コードでも取得可能"""
        resp = client_with_market_db.get("/api/market/stocks/72030")
        assert resp.status_code == 200
        assert resp.json()["code"] == "72030"

    def test_404(self, client_with_market_db):
        """存在しない銘柄"""
        resp = client_with_market_db.get("/api/market/stocks/0000")
        assert resp.status_code == 404
        data = resp.json()
        assert data["status"] == "error"
        assert "Stock not found" in data["message"]


class TestGetStockOhlcv:
    def test_200(self, client_with_market_db):
        """OHLCV データ取得"""
        resp = client_with_market_db.get("/api/market/stocks/7203/ohlcv")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 3
        assert data[0]["date"] == "2024-01-15"
        assert "open" in data[0]
        assert "volume" in data[0]
        assert isinstance(data[0]["volume"], int)

    def test_200_with_date_range(self, client_with_market_db):
        """日付範囲指定"""
        resp = client_with_market_db.get("/api/market/stocks/7203/ohlcv?start_date=2024-01-16&end_date=2024-01-16")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1

    def test_404(self, client_with_market_db):
        """存在しない銘柄"""
        resp = client_with_market_db.get("/api/market/stocks/0000/ohlcv")
        assert resp.status_code == 404

    def test_response_shape(self, client_with_market_db):
        """レスポンス形状が Hono 互換"""
        resp = client_with_market_db.get("/api/market/stocks/7203/ohlcv")
        data = resp.json()
        record = data[0]
        assert set(record.keys()) == {"date", "open", "high", "low", "close", "volume"}


class TestGetTopix:
    def test_200(self, client_with_market_db):
        """TOPIX データ取得"""
        resp = client_with_market_db.get("/api/market/topix")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 3
        assert data[0]["date"] == "2024-01-15"
        # TOPIX には volume がない
        assert "volume" not in data[0]

    def test_200_with_date_range(self, client_with_market_db):
        """日付範囲指定"""
        resp = client_with_market_db.get("/api/market/topix?start_date=2024-01-16")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2

    def test_response_shape(self, client_with_market_db):
        """レスポンス形状が Hono 互換（volume なし）"""
        resp = client_with_market_db.get("/api/market/topix")
        data = resp.json()
        record = data[0]
        assert set(record.keys()) == {"date", "open", "high", "low", "close"}
