"""Analytics fundamentals GET ルートのテスト"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from src.server.app import create_app
from src.server.schemas.analytics_margin import (
    MarginFlowPressureData,
    MarginLongPressureData,
    MarginPressureIndicatorsResponse,
    MarginTurnoverDaysData,
    MarginVolumeRatioData,
    MarginVolumeRatioResponse,
)
from src.server.schemas.analytics_roe import ROEMetadata, ROEResponse, ROEResultItem, ROESummary
from src.server.schemas.fundamentals import (
    FundamentalDataPoint,
    FundamentalsComputeResponse,
)
from src.server.routes import analytics_jquants


@pytest.fixture()
def client() -> TestClient:
    app = create_app()
    return TestClient(app, raise_server_exceptions=False)


def _make_response(symbol: str = "7203", data_count: int = 1) -> FundamentalsComputeResponse:
    """テスト用レスポンスを生成"""
    data = [
        FundamentalDataPoint(
            date="2024-03-31",
            disclosedDate="2024-05-10",
            periodType="FY",
            isConsolidated=True,
            roe=12.5,
            eps=350.0,
            bps=2800.0,
        )
    ] * data_count
    return FundamentalsComputeResponse(
        symbol=symbol,
        companyName="Toyota Motor",
        data=data,
        latestMetrics=data[0] if data else None,
        dailyValuation=None,
        tradingValuePeriod=15,
        lastUpdated="2024-06-01T00:00:00Z",
    )


class TestGetFundamentals:
    @patch("src.server.routes.analytics_jquants.fundamentals_service")
    def test_basic(self, mock_service: MagicMock, client: TestClient) -> None:
        mock_service.compute_fundamentals.return_value = _make_response()
        resp = client.get("/api/analytics/fundamentals/7203")
        assert resp.status_code == 200
        data = resp.json()
        assert data["symbol"] == "7203"
        assert data["companyName"] == "Toyota Motor"
        assert len(data["data"]) == 1
        assert data["data"][0]["periodType"] == "FY"

    @patch("src.server.routes.analytics_jquants.fundamentals_service")
    def test_not_found(self, mock_service: MagicMock, client: TestClient) -> None:
        mock_service.compute_fundamentals.return_value = FundamentalsComputeResponse(
            symbol="9999",
            companyName=None,
            data=[],
            latestMetrics=None,
            dailyValuation=None,
            tradingValuePeriod=15,
            lastUpdated="2024-06-01T00:00:00Z",
        )
        resp = client.get("/api/analytics/fundamentals/9999")
        assert resp.status_code == 404
        assert "9999" in resp.json()["message"]

    @patch("src.server.routes.analytics_jquants.fundamentals_service")
    def test_query_params(self, mock_service: MagicMock, client: TestClient) -> None:
        mock_service.compute_fundamentals.return_value = _make_response()
        resp = client.get(
            "/api/analytics/fundamentals/7203",
            params={
                "from": "2020-01-01",
                "to": "2024-12-31",
                "periodType": "FY",
                "tradingValuePeriod": 20,
            },
        )
        assert resp.status_code == 200
        call_args = mock_service.compute_fundamentals.call_args[0][0]
        assert call_args.from_date == "2020-01-01"
        assert call_args.to_date == "2024-12-31"
        assert call_args.period_type == "FY"
        assert call_args.trading_value_period == 20

    @patch("src.server.routes.analytics_jquants.fundamentals_service")
    def test_default_params(self, mock_service: MagicMock, client: TestClient) -> None:
        mock_service.compute_fundamentals.return_value = _make_response()
        resp = client.get("/api/analytics/fundamentals/7203")
        assert resp.status_code == 200
        call_args = mock_service.compute_fundamentals.call_args[0][0]
        assert call_args.from_date is None
        assert call_args.to_date is None
        assert call_args.period_type == "all"
        assert call_args.prefer_consolidated is True
        assert call_args.trading_value_period == 15

    @patch("src.server.routes.analytics_jquants.fundamentals_service")
    def test_prefer_consolidated_false(self, mock_service: MagicMock, client: TestClient) -> None:
        mock_service.compute_fundamentals.return_value = _make_response()
        resp = client.get(
            "/api/analytics/fundamentals/7203",
            params={"preferConsolidated": "false"},
        )
        assert resp.status_code == 200
        call_args = mock_service.compute_fundamentals.call_args[0][0]
        assert call_args.prefer_consolidated is False


def _make_roe_response() -> ROEResponse:
    return ROEResponse(
        results=[
            ROEResultItem(
                roe=12.5,
                netProfit=4000000.0,
                equity=30000000.0,
                metadata=ROEMetadata(
                    code="7203",
                    periodType="FY",
                    periodEnd="2024-03-31",
                    isConsolidated=True,
                    accountingStandard="JGAAP",
                    isAnnualized=False,
                ),
            )
        ],
        summary=ROESummary(averageROE=12.5, maxROE=12.5, minROE=12.5, totalCompanies=1),
        lastUpdated="2024-06-01T00:00:00Z",
    )


def _make_margin_pressure_response(empty: bool = False) -> MarginPressureIndicatorsResponse:
    return MarginPressureIndicatorsResponse(
        symbol="7203",
        averagePeriod=15,
        longPressure=[]
        if empty
        else [MarginLongPressureData(date="2024-06-01", pressure=1.2, longVol=1000, shortVol=500, avgVolume=2000)],
        flowPressure=[]
        if empty
        else [
            MarginFlowPressureData(
                date="2024-06-01",
                flowPressure=0.3,
                currentNetMargin=500,
                previousNetMargin=400,
                avgVolume=2000,
            )
        ],
        turnoverDays=[]
        if empty
        else [MarginTurnoverDaysData(date="2024-06-01", turnoverDays=2.1, longVol=1000, avgVolume=2000)],
        lastUpdated="2024-06-01T00:00:00Z",
    )


def _make_margin_ratio_response(empty: bool = False) -> MarginVolumeRatioResponse:
    return MarginVolumeRatioResponse(
        symbol="7203",
        longRatio=[]
        if empty
        else [MarginVolumeRatioData(date="2024-06-01", ratio=0.8, weeklyAvgVolume=1000, marginVolume=800)],
        shortRatio=[]
        if empty
        else [MarginVolumeRatioData(date="2024-06-01", ratio=0.3, weeklyAvgVolume=1000, marginVolume=300)],
        lastUpdated="2024-06-01T00:00:00Z",
    )


class TestAnalyticsRouteHelpers:
    def test_get_executor_recreates_when_shutdown(self) -> None:
        with patch.object(analytics_jquants, "_executor", SimpleNamespace(_shutdown=True)):
            executor = analytics_jquants._get_executor()
            assert isinstance(executor, ThreadPoolExecutor)
            executor.shutdown(wait=True)

    def test_get_roe_service_not_initialized(self) -> None:
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
        with pytest.raises(HTTPException) as exc:
            analytics_jquants._get_roe_service(request)  # type: ignore[arg-type]
        assert exc.value.status_code == 422

    def test_get_margin_service_not_initialized(self) -> None:
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
        with pytest.raises(HTTPException) as exc:
            analytics_jquants._get_margin_service(request)  # type: ignore[arg-type]
        assert exc.value.status_code == 422


class TestGetRoe:
    @patch("src.server.routes.analytics_jquants._get_roe_service")
    def test_requires_code_or_date(
        self, _mock_get_service: MagicMock, client: TestClient
    ) -> None:
        resp = client.get("/api/analytics/roe")
        assert resp.status_code == 400

    @patch("src.server.routes.analytics_jquants._get_roe_service")
    def test_get_roe_success(
        self, mock_get_service: MagicMock, client: TestClient
    ) -> None:
        service = AsyncMock()
        service.calculate_roe.return_value = _make_roe_response()
        mock_get_service.return_value = service

        resp = client.get(
            "/api/analytics/roe",
            params={
                "code": "7203",
                "annualize": "false",
                "preferConsolidated": "false",
                "minEquity": "2000",
                "sortBy": "date",
                "limit": "10",
            },
        )
        assert resp.status_code == 200
        service.calculate_roe.assert_awaited_once_with(
            code="7203",
            date=None,
            annualize=False,
            prefer_consolidated=False,
            min_equity=2000.0,
            sort_by="date",
            limit=10,
        )


class TestMarginRoutes:
    @patch("src.server.routes.analytics_jquants._get_margin_service")
    def test_get_margin_pressure_success(
        self, mock_get_service: MagicMock, client: TestClient
    ) -> None:
        service = AsyncMock()
        service.get_margin_pressure.return_value = _make_margin_pressure_response()
        mock_get_service.return_value = service

        resp = client.get(
            "/api/analytics/stocks/7203/margin-pressure", params={"period": 20}
        )
        assert resp.status_code == 200
        service.get_margin_pressure.assert_awaited_once_with("7203", 20)

    @patch("src.server.routes.analytics_jquants._get_margin_service")
    def test_get_margin_pressure_not_found(
        self, mock_get_service: MagicMock, client: TestClient
    ) -> None:
        service = AsyncMock()
        service.get_margin_pressure.return_value = _make_margin_pressure_response(empty=True)
        mock_get_service.return_value = service

        resp = client.get("/api/analytics/stocks/7203/margin-pressure")
        assert resp.status_code == 404

    @patch("src.server.routes.analytics_jquants._get_margin_service")
    def test_get_margin_ratio_success(
        self, mock_get_service: MagicMock, client: TestClient
    ) -> None:
        service = AsyncMock()
        service.get_margin_ratio.return_value = _make_margin_ratio_response()
        mock_get_service.return_value = service

        resp = client.get("/api/analytics/stocks/7203/margin-ratio")
        assert resp.status_code == 200
        service.get_margin_ratio.assert_awaited_once_with("7203")

    @patch("src.server.routes.analytics_jquants._get_margin_service")
    def test_get_margin_ratio_not_found(
        self, mock_get_service: MagicMock, client: TestClient
    ) -> None:
        service = AsyncMock()
        service.get_margin_ratio.return_value = _make_margin_ratio_response(empty=True)
        mock_get_service.return_value = service

        resp = client.get("/api/analytics/stocks/7203/margin-ratio")
        assert resp.status_code == 404
