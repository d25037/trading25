"""
JQuants Proxy Routes Tests

sync_client + mock を使用したルートテスト。
"""

import os
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app
from src.entrypoints.http.schemas.jquants import (
    ApiIndicesResponse,
    ApiListedInfoResponse,
    ApiMarginInterestResponse,
    DailyQuotesResponse,
    MinuteBarsResponse,
    N225OptionsExplorerResponse,
    N225OptionsSummary,
    N225OptionsNumericRange,
    RawStatementsResponse,
    StatementsResponse,
    TopixRawResponse,
)


def _proxy_service(app_client: TestClient) -> Any:
    return cast(Any, app_client.app.state).jquants_proxy_service


def _proxy_client(app_client: TestClient) -> Any:
    return cast(Any, _proxy_service(app_client))._client


@pytest.fixture(scope="module")
def app_client():
    """JQuants mock 付きテストクライアント"""
    from src.shared.config.settings import reload_settings

    env_updates = {
        "JQUANTS_API_KEY": "dummy_token_value_0000",
        "JQUANTS_PLAN": "free",
    }
    original_env = {key: os.environ.get(key) for key in env_updates}

    for key, value in env_updates.items():
        os.environ[key] = value
    reload_settings()
    app = create_app()
    try:
        with TestClient(app) as client:
            yield client
    finally:
        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        reload_settings()


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
            _proxy_service(app_client),
            "get_daily_quotes",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            resp = app_client.get("/api/jquants/daily-quotes?code=7203")
            assert resp.status_code == 200
            assert resp.json()["data"] == []


class TestMinuteBars:
    def test_minute_bars_require_code_or_date(self, app_client):
        resp = app_client.get("/api/jquants/minute-bars")
        assert resp.status_code == 422

    def test_minute_bars_validate_date_range(self, app_client):
        mock_response = MinuteBarsResponse(data=[])
        with patch.object(
            _proxy_service(app_client),
            "get_minute_bars",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            resp = app_client.get("/api/jquants/minute-bars?code=7203&from=2024-12-31&to=2024-01-01")
            assert resp.status_code == 422

    def test_minute_bars_with_code(self, app_client):
        mock_response = MinuteBarsResponse(data=[])
        with patch.object(
            _proxy_service(app_client),
            "get_minute_bars",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            resp = app_client.get("/api/jquants/minute-bars?code=7203&date=2024-01-05")
            assert resp.status_code == 200
            assert resp.json()["data"] == []


class TestIndices:
    def test_indices_date_range_validation(self, app_client):
        """from > to の場合 422"""
        mock_response = ApiIndicesResponse(indices=[], lastUpdated="2024-01-01")
        with patch.object(
            _proxy_service(app_client),
            "get_indices",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            resp = app_client.get("/api/jquants/indices?from=2024-12-31&to=2024-01-01")
            assert resp.status_code == 422

    def test_indices_success(self, app_client):
        mock_response = ApiIndicesResponse(indices=[], lastUpdated="2024-01-01")
        with patch.object(
            _proxy_service(app_client),
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
            _proxy_service(app_client),
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
            _proxy_service(app_client),
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
            _proxy_service(app_client),
            "get_statements",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            resp = app_client.get("/api/jquants/statements?code=7203")
            assert resp.status_code == 200

    def test_statements_raw_success(self, app_client):
        mock_response = RawStatementsResponse(data=[])
        with patch.object(
            _proxy_service(app_client),
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
            _proxy_client(app_client),
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
            _proxy_service(app_client),
            "get_topix",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            resp = app_client.get("/api/jquants/topix")
            assert resp.status_code == 200


class TestN225Options:
    def test_options_225_success(self, app_client):
        mock_response = N225OptionsExplorerResponse(
            requestedDate="2026-03-18",
            resolvedDate="2026-03-18",
            lastUpdated="2026-03-18T00:00:00Z",
            sourceCallCount=2,
            availableContractMonths=["2026-04"],
            items=[],
            summary=N225OptionsSummary(
                totalCount=0,
                putCount=0,
                callCount=0,
                totalVolume=0,
                totalOpenInterest=0,
                strikePriceRange=N225OptionsNumericRange(),
                underlyingPriceRange=N225OptionsNumericRange(),
                settlementPriceRange=N225OptionsNumericRange(),
            ),
        )
        with patch.object(
            _proxy_service(app_client),
            "get_options_225",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            resp = app_client.get("/api/jquants/options/225?date=2026-03-18")
            assert resp.status_code == 200
            assert resp.json()["resolvedDate"] == "2026-03-18"

    def test_options_225_invalid_date_returns_422(self, app_client):
        resp = app_client.get("/api/jquants/options/225?date=2026-13-40")
        assert resp.status_code == 422


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
