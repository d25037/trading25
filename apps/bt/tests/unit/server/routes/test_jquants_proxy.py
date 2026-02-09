"""
JQuants Proxy Routes Tests

sync_client + mock を使用したルートテスト。
"""

from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from src.server.app import create_app
from src.server.schemas.jquants import (
    ApiIndicesResponse,
    ApiListedInfoResponse,
    ApiMarginInterestResponse,
    AuthStatusResponse,
    DailyQuotesResponse,
    RawStatementsResponse,
    StatementsResponse,
    TopixRawResponse,
)


@pytest.fixture
def app_client(mock_jquants_env):
    """JQuants mock 付きテストクライアント"""
    app = create_app()
    with TestClient(app) as client:
        yield client


class TestAuthStatus:
    def test_auth_status(self, app_client):
        resp = app_client.get("/api/jquants/auth/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "authenticated" in data
        assert "hasApiKey" in data


class TestDailyQuotes:
    def test_daily_quotes_requires_code(self, app_client):
        """code パラメータが必須"""
        resp = app_client.get("/api/jquants/daily-quotes")
        assert resp.status_code == 422

    def test_daily_quotes_with_code(self, app_client):
        """code パラメータ指定で 200 を返す（JQuants API はモック経由）"""
        mock_response = DailyQuotesResponse(data=[])
        with patch.object(
            app_client.app.state.jquants_proxy_service,  # type: ignore[union-attr]
            "get_daily_quotes",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            resp = app_client.get("/api/jquants/daily-quotes?code=7203")
            assert resp.status_code == 200
            assert resp.json()["data"] == []


class TestIndices:
    def test_indices_date_range_validation(self, app_client):
        """from > to の場合 422"""
        mock_response = ApiIndicesResponse(indices=[], lastUpdated="2024-01-01")
        with patch.object(
            app_client.app.state.jquants_proxy_service,  # type: ignore[union-attr]
            "get_indices",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            resp = app_client.get("/api/jquants/indices?from=2024-12-31&to=2024-01-01")
            assert resp.status_code == 422

    def test_indices_success(self, app_client):
        mock_response = ApiIndicesResponse(indices=[], lastUpdated="2024-01-01")
        with patch.object(
            app_client.app.state.jquants_proxy_service,  # type: ignore[union-attr]
            "get_indices",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            resp = app_client.get("/api/jquants/indices")
            assert resp.status_code == 200


class TestListedInfo:
    def test_listed_info_success(self, app_client):
        mock_response = ApiListedInfoResponse(info=[], lastUpdated="2024-01-01")
        with patch.object(
            app_client.app.state.jquants_proxy_service,  # type: ignore[union-attr]
            "get_listed_info",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            resp = app_client.get("/api/jquants/listed-info")
            assert resp.status_code == 200

    def test_listed_info_code_validation(self, app_client):
        """code は 4 文字"""
        resp = app_client.get("/api/jquants/listed-info?code=12")
        assert resp.status_code == 422


class TestMarginInterest:
    def test_margin_interest_success(self, app_client):
        mock_response = ApiMarginInterestResponse(
            marginInterest=[], symbol="7203", lastUpdated="2024-01-01"
        )
        with patch.object(
            app_client.app.state.jquants_proxy_service,  # type: ignore[union-attr]
            "get_margin_interest",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            resp = app_client.get("/api/jquants/stocks/7203/margin-interest")
            assert resp.status_code == 200
            assert resp.json()["symbol"] == "7203"


class TestStatements:
    def test_statements_requires_code(self, app_client):
        resp = app_client.get("/api/jquants/statements")
        assert resp.status_code == 422

    def test_statements_success(self, app_client):
        mock_response = StatementsResponse(data=[])
        with patch.object(
            app_client.app.state.jquants_proxy_service,  # type: ignore[union-attr]
            "get_statements",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            resp = app_client.get("/api/jquants/statements?code=7203")
            assert resp.status_code == 200

    def test_statements_raw_success(self, app_client):
        mock_response = RawStatementsResponse(data=[])
        with patch.object(
            app_client.app.state.jquants_proxy_service,  # type: ignore[union-attr]
            "get_statements_raw",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            resp = app_client.get("/api/jquants/statements/raw?code=7203")
            assert resp.status_code == 200

    def test_statements_raw_empty_numeric_strings(self, app_client):
        """JQuants の空文字数値を None として扱えること"""
        raw_body = {
            "data": [
                {
                    "DiscDate": "2024-05-10",
                    "Code": "72030",
                    "CurPerType": "FY",
                    "CurPerSt": "2023-04-01",
                    "CurPerEn": "2024-03-31",
                    "FEPS": "",
                    "FNCEPS": "",
                }
            ]
        }
        with patch.object(
            app_client.app.state.jquants_proxy_service._client,  # type: ignore[union-attr]
            "get",
            new_callable=AsyncMock,
            return_value=raw_body,
        ):
            resp = app_client.get("/api/jquants/statements/raw?code=7203")
            assert resp.status_code == 200
            data = resp.json()["data"][0]
            assert data["FEPS"] is None
            assert data["FNCEPS"] is None


class TestTopix:
    def test_topix_success(self, app_client):
        mock_response = TopixRawResponse(topix=[])
        with patch.object(
            app_client.app.state.jquants_proxy_service,  # type: ignore[union-attr]
            "get_topix",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            resp = app_client.get("/api/jquants/topix")
            assert resp.status_code == 200


class TestHealthAlias:
    def test_health_alias(self, app_client):
        """Hono 互換 /health エイリアス"""
        resp = app_client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["service"] == "trading25-bt"

    def test_api_health(self, app_client):
        """既存 /api/health"""
        resp = app_client.get("/api/health")
        assert resp.status_code == 200


class TestErrorFormat:
    def test_validation_error_format(self, app_client):
        """422 エラーが統一フォーマットで返る"""
        resp = app_client.get("/api/jquants/daily-quotes")
        assert resp.status_code == 422
        data = resp.json()
        assert data["status"] == "error"
        assert data["error"] == "Unprocessable Entity"
        assert "message" in data
        assert "timestamp" in data
        assert "correlationId" in data
