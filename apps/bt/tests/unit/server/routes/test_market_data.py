"""
Market Data Routes Unit Tests

sync_client を使用してルートの E2E テスト。
"""

from pathlib import Path

import pytest

from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app


@pytest.fixture
def client_with_market_db(market_timeseries_dir, monkeypatch):
    """market.duckdb 付きテストクライアント"""
    monkeypatch.setenv("MARKET_TIMESERIES_DIR", market_timeseries_dir)
    monkeypatch.setenv("MARKET_DB_PATH", str(Path(market_timeseries_dir) / "market.duckdb"))
    monkeypatch.setenv("JQUANTS_API_KEY", "dummy_token_value_0000")
    monkeypatch.setenv("JQUANTS_PLAN", "free")
    # settings cache をクリア
    from src.shared.config.settings import reload_settings
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

    def test_200_numeric_prime_alias_matches_legacy(self, client_with_market_db):
        """current market code 0111 は legacy prime と同義"""
        legacy = client_with_market_db.get("/api/market/stocks?market=prime")
        numeric = client_with_market_db.get("/api/market/stocks?market=0111")
        assert legacy.status_code == 200
        assert numeric.status_code == 200
        assert numeric.json() == legacy.json()

    def test_200_numeric_standard_alias_matches_legacy(self, client_with_market_db):
        """current market code 0112 は legacy standard と同義"""
        legacy = client_with_market_db.get("/api/market/stocks?market=standard")
        numeric = client_with_market_db.get("/api/market/stocks?market=0112")
        assert legacy.status_code == 200
        assert numeric.status_code == 200
        assert numeric.json() == legacy.json()

    def test_growth_aliases_are_accepted(self, client_with_market_db):
        """growth / 0113 も API 入力境界で同義に扱われる"""
        legacy = client_with_market_db.get("/api/market/stocks?market=growth")
        numeric = client_with_market_db.get("/api/market/stocks?market=0113")
        assert legacy.status_code == numeric.status_code
        assert legacy.status_code in (200, 404)
        if legacy.status_code == 404:
            legacy_payload = legacy.json()
            numeric_payload = numeric.json()
            assert numeric_payload["status"] == legacy_payload["status"] == "error"
            assert numeric_payload["error"] == legacy_payload["error"]
            assert numeric_payload["message"] == legacy_payload["message"]
        else:
            assert numeric.json() == legacy.json()

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


class TestGetOptions225:
    def test_200_latest(self, client_with_market_db):
        resp = client_with_market_db.get("/api/market/options/225")

        assert resp.status_code == 200
        data = resp.json()
        assert data["requestedDate"] is None
        assert data["resolvedDate"] == "2024-01-17"
        assert data["sourceCallCount"] == 0
        assert len(data["items"]) == 2

    def test_200_requested_date(self, client_with_market_db):
        resp = client_with_market_db.get("/api/market/options/225?date=2024-01-16")

        assert resp.status_code == 200
        data = resp.json()
        assert data["requestedDate"] == "2024-01-16"
        assert data["resolvedDate"] == "2024-01-16"
        assert len(data["items"]) == 1
        assert data["items"][0]["underlyingPrice"] == 36100.0

    def test_404_missing_date(self, client_with_market_db):
        resp = client_with_market_db.get("/api/market/options/225?date=2024-01-18")

        assert resp.status_code == 404
        data = resp.json()
        assert data["status"] == "error"
        assert data["details"] == [
            {"field": "reason", "message": "options_225_data_missing"},
            {"field": "recovery", "message": "market_db_sync"},
        ]

    def test_422_invalid_date(self, client_with_market_db):
        resp = client_with_market_db.get("/api/market/options/225?date=202401")

        assert resp.status_code == 422
