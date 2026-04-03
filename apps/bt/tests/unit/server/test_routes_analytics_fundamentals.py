"""Analytics fundamentals GET ルートのテスト"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app
from src.entrypoints.http.schemas.analytics_margin import (
    MarginFlowPressureData,
    MarginLongPressureData,
    MarginPressureIndicatorsResponse,
    MarginTurnoverDaysData,
    MarginVolumeRatioData,
    MarginVolumeRatioResponse,
)
from src.entrypoints.http.schemas.analytics_common import DataProvenance, ResponseDiagnostics
from src.entrypoints.http.schemas.analytics_roe import ROEMetadata, ROEResponse, ROEResultItem, ROESummary
from src.entrypoints.http.schemas.cost_structure import (
    CostStructureDateRange,
    CostStructurePoint,
    CostStructureRegressionSummary,
    CostStructureResponse,
)
from src.entrypoints.http.schemas.fundamentals import (
    FundamentalDataPoint,
    FundamentalsComputeResponse,
)
from src.entrypoints.http.routes import analytics_market


@pytest.fixture(scope="module")
def client() -> TestClient:
    app = create_app()
    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client


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
        forecastEpsLookbackFyCount=3,
        lastUpdated="2024-06-01T00:00:00Z",
        provenance=DataProvenance(source_kind="market", loaded_domains=["stock_data", "statements"]),
        diagnostics=ResponseDiagnostics(),
    )


class TestGetFundamentals:
    @patch("src.entrypoints.http.routes.analytics_market.fundamentals_service")
    def test_basic(self, mock_service: MagicMock, client: TestClient) -> None:
        mock_service.compute_fundamentals.return_value = _make_response()
        resp = client.get("/api/analytics/fundamentals/7203")
        assert resp.status_code == 200
        data = resp.json()
        assert data["symbol"] == "7203"
        assert data["companyName"] == "Toyota Motor"
        assert len(data["data"]) == 1
        assert data["data"][0]["periodType"] == "FY"

    @patch("src.entrypoints.http.routes.analytics_market.fundamentals_service")
    def test_not_found(self, mock_service: MagicMock, client: TestClient) -> None:
        mock_service.compute_fundamentals.return_value = FundamentalsComputeResponse(
            symbol="9999",
            companyName=None,
            data=[],
            latestMetrics=None,
            dailyValuation=None,
            tradingValuePeriod=15,
            forecastEpsLookbackFyCount=3,
            lastUpdated="2024-06-01T00:00:00Z",
            provenance=DataProvenance(source_kind="market", loaded_domains=["stock_data", "statements"]),
            diagnostics=ResponseDiagnostics(),
        )
        resp = client.get("/api/analytics/fundamentals/9999")
        assert resp.status_code == 404
        assert "9999" in resp.json()["message"]

    @patch("src.entrypoints.http.routes.analytics_market.fundamentals_service")
    def test_query_params(self, mock_service: MagicMock, client: TestClient) -> None:
        mock_service.compute_fundamentals.return_value = _make_response()
        resp = client.get(
            "/api/analytics/fundamentals/7203",
            params={
                "from": "2020-01-01",
                "to": "2024-12-31",
                "periodType": "FY",
                "tradingValuePeriod": 20,
                "forecastEpsLookbackFyCount": 5,
            },
        )
        assert resp.status_code == 200
        call_args = mock_service.compute_fundamentals.call_args[0][0]
        assert call_args.from_date == "2020-01-01"
        assert call_args.to_date == "2024-12-31"
        assert call_args.period_type == "FY"
        assert call_args.trading_value_period == 20
        assert call_args.forecast_eps_lookback_fy_count == 5

    @patch("src.entrypoints.http.routes.analytics_market.fundamentals_service")
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
        assert call_args.forecast_eps_lookback_fy_count == 3

    @patch("src.entrypoints.http.routes.analytics_market.fundamentals_service")
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
        provenance=DataProvenance(source_kind="market", loaded_domains=["statements"]),
        diagnostics=ResponseDiagnostics(),
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
        provenance=DataProvenance(source_kind="market", loaded_domains=["margin_data", "stock_data"]),
        diagnostics=ResponseDiagnostics(),
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
        provenance=DataProvenance(source_kind="market", loaded_domains=["margin_data", "stock_data"]),
        diagnostics=ResponseDiagnostics(),
    )


def _make_cost_structure_response(symbol: str = "7203") -> CostStructureResponse:
    point = CostStructurePoint(
        periodEnd="2025-05-09",
        disclosedDate="2025-05-09",
        fiscalYear="2025",
        analysisPeriodType="4Q",
        sales=1500.0,
        operatingProfit=180.0,
        operatingMargin=12.0,
        isDerived=True,
    )
    return CostStructureResponse(
        symbol=symbol,
        companyName="Toyota Motor",
        points=[
            CostStructurePoint(
                periodEnd="2024-08-09",
                disclosedDate="2024-08-09",
                fiscalYear="2025",
                analysisPeriodType="1Q",
                sales=1000.0,
                operatingProfit=100.0,
                operatingMargin=10.0,
                isDerived=False,
            ),
            point,
        ],
        latestPoint=point,
        regression=CostStructureRegressionSummary(
            sampleCount=4,
            slope=0.43,
            intercept=-54.59,
            rSquared=0.939,
            contributionMarginRatio=0.43,
            variableCostRatio=0.57,
            fixedCost=54.59,
            breakEvenSales=126.95,
        ),
        dateRange=CostStructureDateRange(**{"from": "2024-08-09", "to": "2025-05-09"}),
        lastUpdated="2025-05-10T00:00:00Z",
        provenance=DataProvenance(source_kind="market", loaded_domains=["statements", "stocks"]),
        diagnostics=ResponseDiagnostics(),
    )


class TestAnalyticsRouteHelpers:
    def test_get_executor_recreates_when_shutdown(self) -> None:
        with patch.object(analytics_market, "_executor", SimpleNamespace(_shutdown=True)):
            executor = analytics_market._get_executor()
            assert isinstance(executor, ThreadPoolExecutor)
            executor.shutdown(wait=True)

    def test_get_roe_service_not_initialized(self) -> None:
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
        with pytest.raises(HTTPException) as exc:
            analytics_market._get_roe_service(cast(Any, request))
        assert exc.value.status_code == 422

    def test_get_margin_service_not_initialized(self) -> None:
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
        with pytest.raises(HTTPException) as exc:
            analytics_market._get_margin_service(cast(Any, request))
        assert exc.value.status_code == 422

    def test_get_market_reader_not_initialized(self) -> None:
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))
        with pytest.raises(HTTPException) as exc:
            analytics_market._get_market_reader(cast(Any, request))
        assert exc.value.status_code == 422


class TestGetRoe:
    @patch("src.entrypoints.http.routes.analytics_market._get_roe_service")
    def test_requires_code_or_date(
        self, _mock_get_service: MagicMock, client: TestClient
    ) -> None:
        resp = client.get("/api/analytics/roe")
        assert resp.status_code == 400

    @patch("src.entrypoints.http.routes.analytics_market._get_roe_service")
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
    @patch("src.entrypoints.http.routes.analytics_market._get_margin_service")
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

    @patch("src.entrypoints.http.routes.analytics_market._get_margin_service")
    def test_get_margin_pressure_not_found(
        self, mock_get_service: MagicMock, client: TestClient
    ) -> None:
        service = AsyncMock()
        service.get_margin_pressure.return_value = _make_margin_pressure_response(empty=True)
        mock_get_service.return_value = service

        resp = client.get("/api/analytics/stocks/7203/margin-pressure")
        assert resp.status_code == 404

    @patch("src.entrypoints.http.routes.analytics_market._get_margin_service")
    def test_get_margin_ratio_success(
        self, mock_get_service: MagicMock, client: TestClient
    ) -> None:
        service = AsyncMock()
        service.get_margin_ratio.return_value = _make_margin_ratio_response()
        mock_get_service.return_value = service

        resp = client.get("/api/analytics/stocks/7203/margin-ratio")
        assert resp.status_code == 200
        service.get_margin_ratio.assert_awaited_once_with("7203")

    @patch("src.entrypoints.http.routes.analytics_market._get_margin_service")
    def test_get_margin_ratio_not_found(
        self, mock_get_service: MagicMock, client: TestClient
    ) -> None:
        service = AsyncMock()
        service.get_margin_ratio.return_value = _make_margin_ratio_response(empty=True)
        mock_get_service.return_value = service

        resp = client.get("/api/analytics/stocks/7203/margin-ratio")
        assert resp.status_code == 404


class TestCostStructureRoute:
    @patch("src.entrypoints.http.routes.analytics_market._get_market_reader")
    @patch("src.entrypoints.http.routes.analytics_market.CostStructureAnalysisService")
    def test_get_cost_structure_success(
        self,
        mock_service_cls: MagicMock,
        mock_get_reader: MagicMock,
        client: TestClient,
    ) -> None:
        mock_get_reader.return_value = MagicMock()
        service = MagicMock()
        service.analyze_stock.return_value = _make_cost_structure_response()
        mock_service_cls.return_value = service

        resp = client.get("/api/analytics/stocks/7203/cost-structure")

        assert resp.status_code == 200
        data = resp.json()
        assert data["symbol"] == "7203"
        assert data["regression"]["sampleCount"] == 4
        service.analyze_stock.assert_called_once_with("7203", view="recent", window_quarters=12)

    @patch("src.entrypoints.http.routes.analytics_market._get_market_reader")
    @patch("src.entrypoints.http.routes.analytics_market.CostStructureAnalysisService")
    def test_get_cost_structure_forwards_query_params(
        self,
        mock_service_cls: MagicMock,
        mock_get_reader: MagicMock,
        client: TestClient,
    ) -> None:
        mock_get_reader.return_value = MagicMock()
        service = MagicMock()
        service.analyze_stock.return_value = _make_cost_structure_response()
        mock_service_cls.return_value = service

        resp = client.get(
            "/api/analytics/stocks/7203/cost-structure",
            params={"view": "same_quarter", "windowQuarters": 20},
        )

        assert resp.status_code == 200
        service.analyze_stock.assert_called_once_with("7203", view="same_quarter", window_quarters=20)

    @patch("src.entrypoints.http.routes.analytics_market._get_market_reader")
    @patch("src.entrypoints.http.routes.analytics_market.CostStructureAnalysisService")
    def test_get_cost_structure_not_found(
        self,
        mock_service_cls: MagicMock,
        mock_get_reader: MagicMock,
        client: TestClient,
    ) -> None:
        mock_get_reader.return_value = MagicMock()
        service = MagicMock()
        service.analyze_stock.side_effect = ValueError("Stock not found: 9999")
        mock_service_cls.return_value = service

        resp = client.get("/api/analytics/stocks/9999/cost-structure")

        assert resp.status_code == 404
        assert "9999" in resp.json()["message"]

    @patch("src.entrypoints.http.routes.analytics_market._get_market_reader")
    @patch("src.entrypoints.http.routes.analytics_market.CostStructureAnalysisService")
    def test_get_cost_structure_insufficient_data(
        self,
        mock_service_cls: MagicMock,
        mock_get_reader: MagicMock,
        client: TestClient,
    ) -> None:
        mock_get_reader.return_value = MagicMock()
        service = MagicMock()
        service.analyze_stock.side_effect = ValueError(
            "Insufficient usable data: 2 normalized points (minimum 3)"
        )
        mock_service_cls.return_value = service

        resp = client.get("/api/analytics/stocks/7203/cost-structure")

        assert resp.status_code == 422
        assert "minimum 3" in resp.json()["message"]
