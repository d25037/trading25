"""Analytics fundamentals GET ルートのテスト"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app
from src.application.contracts.margin_analytics import (
    MarginFlowPressureData,
    MarginLongPressureData,
    MarginPressureIndicatorsResponse,
    MarginTurnoverDaysData,
    MarginVolumeRatioData,
    MarginVolumeRatioResponse,
)
from src.application.contracts.analytics import DataProvenance, ResponseDiagnostics
from src.application.contracts.ranking import MarketRankingResponse, Rankings
from src.application.contracts.roe import (
    ROEMetadata,
    ROEResponse,
    ROEResultItem,
    ROESummary,
)
from src.application.contracts.fundamentals import (
    FundamentalDataPoint,
    FundamentalsComputeResponse,
)
from src.entrypoints.http.routes import analytics_market
from src.entrypoints.http.routes import analytics_complex
from src.application.contracts.fundamentals_pit import FundamentalsPitSnapshotError


@pytest.fixture(scope="module")
def client() -> TestClient:
    app = create_app()
    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client


def _make_response(
    symbol: str = "7203", data_count: int = 1
) -> FundamentalsComputeResponse:
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
        asOfDate="2024-06-28",
        companyName="Toyota Motor",
        data=data,
        latestMetrics=data[0] if data else None,
        dailyValuation=None,
        fundamentalsAdjustmentBasisDate="2024-06-27",
        tradingValuePeriod=15,
        forecastEpsLookbackFyCount=3,
        lastUpdated="2024-06-01T00:00:00Z",
        provenance=DataProvenance(
            source_kind="market", loaded_domains=["stock_data", "statements"]
        ),
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
    def test_listed_symbol_without_disclosure_returns_200_empty(
        self, mock_service: MagicMock, client: TestClient
    ) -> None:
        mock_service.compute_fundamentals.return_value = FundamentalsComputeResponse(
            symbol="9999",
            asOfDate="2024-06-28",
            companyName=None,
            data=[],
            latestMetrics=None,
            dailyValuation=None,
            tradingValuePeriod=15,
            forecastEpsLookbackFyCount=3,
            lastUpdated="2024-06-01T00:00:00Z",
            provenance=DataProvenance(
                source_kind="market", loaded_domains=["stock_data", "statements"]
            ),
            diagnostics=ResponseDiagnostics(),
        )
        resp = client.get("/api/analytics/fundamentals/9999")
        assert resp.status_code == 200
        assert resp.json()["data"] == []

    @pytest.mark.parametrize(
        ("reason", "status"),
        [
            ("stock_not_listed_as_of", 404),
            ("provider_window_required", 409),
            ("current_adjusted_metrics_required", 409),
            ("stock_master_snapshot_required", 409),
            ("pit_snapshot_inconsistent", 409),
        ],
    )
    def test_get_and_post_map_same_pit_error(
        self, client: TestClient, reason: str, status: int
    ) -> None:
        error = FundamentalsPitSnapshotError(reason, f"PIT failure: {reason}")
        calls = (
            (
                "src.entrypoints.http.routes.analytics_market.fundamentals_service.compute_fundamentals",
                "get",
                "/api/analytics/fundamentals/7203",
                None,
            ),
            (
                "src.entrypoints.http.routes.fundamentals.fundamentals_service.compute_fundamentals",
                "post",
                "/api/fundamentals/compute",
                {"symbol": "7203"},
            ),
        )
        for target, method, url, payload in calls:
            with patch(target, side_effect=error):
                resp = client.request(
                    method,
                    url,
                    json=payload,
                    headers={"x-correlation-id": "fundamentals-parity"},
                )
            assert resp.status_code == status
            body = resp.json()
            assert body["correlationId"] == "fundamentals-parity"
            assert resp.headers["x-correlation-id"] == "fundamentals-parity"
            assert {"field": "reason", "message": reason} in body["details"]
            recovery = [item for item in body["details"] if item["field"] == "recovery"]
            if status == 409:
                assert recovery == [{"field": "recovery", "message": "market_db_sync"}]
            else:
                assert recovery == []

    def test_get_and_post_map_provider_vintage_loss_to_normal_sync_recovery(
        self, client: TestClient
    ) -> None:
        error = FundamentalsPitSnapshotError(
            "pit_snapshot_inconsistent",
            "daily valuation does not cover the selected basis price history exactly",
        )
        calls = (
            (
                "src.entrypoints.http.routes.analytics_market.fundamentals_service.compute_fundamentals",
                "get",
                "/api/analytics/fundamentals/7203",
                None,
            ),
            (
                "src.entrypoints.http.routes.fundamentals.fundamentals_service.compute_fundamentals",
                "post",
                "/api/fundamentals/compute",
                {"symbol": "7203"},
            ),
        )
        for target, method, url, payload in calls:
            with patch(target, side_effect=error):
                response = client.request(method, url, json=payload)
            assert response.status_code == 409
            assert {
                "field": "reason",
                "message": "pit_snapshot_inconsistent",
            } in response.json()["details"]
            assert {
                "field": "recovery",
                "message": "market_db_sync",
            } in response.json()["details"]

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
        assert call_args.from_date == date(2020, 1, 1)
        assert call_args.to_date == date(2024, 12, 31)
        assert call_args.period_type == "FY"
        assert call_args.trading_value_period == 20
        assert call_args.forecast_eps_lookback_fy_count == 5
        body = resp.json()
        assert body["asOfDate"] == "2024-06-28"
        assert body["fundamentalsAdjustmentBasisDate"] == "2024-06-27"
        assert "priceBasisDate" not in body

    @pytest.mark.parametrize(
        "value",
        ["2024-02-30", "not-a-date", "2024-01-01T00:00:00", "0", 0],
    )
    @pytest.mark.parametrize("field", ["from", "to"])
    def test_rejects_invalid_iso_dates(
        self, client: TestClient, field: str, value: str
    ) -> None:
        resp = client.get(
            "/api/analytics/fundamentals/7203",
            params={field: value},
        )

        assert resp.status_code == 422
        assert resp.json()["error"] == "Unprocessable Entity"

    def test_rejects_reversed_date_range(self, client: TestClient) -> None:
        resp = client.get(
            "/api/analytics/fundamentals/7203",
            params={"from": "2024-07-01", "to": "2024-06-30"},
        )

        assert resp.status_code == 422

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
    def test_prefer_consolidated_false(
        self, mock_service: MagicMock, client: TestClient
    ) -> None:
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


def _make_margin_pressure_response(
    empty: bool = False,
) -> MarginPressureIndicatorsResponse:
    return MarginPressureIndicatorsResponse(
        symbol="7203",
        averagePeriod=15,
        longPressure=[]
        if empty
        else [
            MarginLongPressureData(
                date="2024-06-01",
                pressure=1.2,
                longVol=1000,
                shortVol=500,
                avgVolume=2000,
            )
        ],
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
        else [
            MarginTurnoverDaysData(
                date="2024-06-01", turnoverDays=2.1, longVol=1000, avgVolume=2000
            )
        ],
        lastUpdated="2024-06-01T00:00:00Z",
        provenance=DataProvenance(
            source_kind="market", loaded_domains=["margin_data", "stock_data"]
        ),
        diagnostics=ResponseDiagnostics(),
    )


def _make_margin_ratio_response(empty: bool = False) -> MarginVolumeRatioResponse:
    return MarginVolumeRatioResponse(
        symbol="7203",
        longRatio=[]
        if empty
        else [
            MarginVolumeRatioData(
                date="2024-06-01", ratio=0.8, weeklyAvgVolume=1000, marginVolume=800
            )
        ],
        shortRatio=[]
        if empty
        else [
            MarginVolumeRatioData(
                date="2024-06-01", ratio=0.3, weeklyAvgVolume=1000, marginVolume=300
            )
        ],
        lastUpdated="2024-06-01T00:00:00Z",
        provenance=DataProvenance(
            source_kind="market", loaded_domains=["margin_data", "stock_data"]
        ),
        diagnostics=ResponseDiagnostics(),
    )


class TestAnalyticsRouteHelpers:
    def test_get_executor_recreates_when_shutdown(self) -> None:
        with patch.object(
            analytics_market, "_executor", SimpleNamespace(_shutdown=True)
        ):
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
        service.get_margin_pressure.return_value = _make_margin_pressure_response(
            empty=True
        )
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


class TestRankingScopeRoute:
    @patch("src.application.services.ranking_service.RankingService.get_rankings")
    def test_scope_is_forwarded_to_ranking_service(
        self, mock_get_rankings: MagicMock, client: TestClient
    ) -> None:
        mock_get_rankings.return_value = MarketRankingResponse(
            date="2024-01-19",
            markets=["prime"],
            lookbackDays=1,
            periodDays=250,
            rankings=Rankings(),
            indexPerformance=[],
            lastUpdated="2024-01-19T00:00:00Z",
        )

        response = client.get("/api/analytics/ranking?scope=tradingValue")

        assert response.status_code == 200
        assert mock_get_rankings.call_args.kwargs["scope"] == "tradingValue"

    def test_invalid_scope_returns_422(self, client: TestClient) -> None:
        response = client.get("/api/analytics/ranking?scope=unknown")

        assert response.status_code == 422


class TestRankingThreadpoolBoundaries:
    @pytest.mark.asyncio
    async def test_symbol_snapshot_maps_constructor_error_to_route_500(self) -> None:
        request = SimpleNamespace(
            app=SimpleNamespace(state=SimpleNamespace(market_reader=MagicMock()))
        )

        with patch(
            "src.application.services.ranking_service.RankingService",
            side_effect=RuntimeError("reader setup failed"),
        ):
            with pytest.raises(HTTPException) as exc:
                await analytics_complex.get_ranking_symbol_snapshot(
                    request=request,
                    code="7203",
                )

        assert exc.value.status_code == 500
        assert exc.value.detail == (
            "Failed to get ranking symbol snapshot: reader setup failed"
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("route", "service_method", "route_kwargs"),
        [
            (
                analytics_complex.get_ranking_symbol_snapshot,
                "get_symbol_ranking_snapshot",
                {"code": "7203"},
            ),
            (
                analytics_complex.get_ranking,
                "get_rankings",
                {
                    "date": "2024-06-28",
                    "limit": 10,
                    "markets": "prime",
                    "lookbackDays": 5,
                    "periodDays": 60,
                    "scope": "tradingValue",
                    "sector33Name": "Transport Equipment",
                    "sector17Name": "Automobiles",
                    "includeValuation": True,
                    "includeSectorStrength": True,
                    "sectorStrengthFamily": "balanced_sector_strength",
                    "forwardEpsDisclosedWithinDays": 30,
                    "regimeState": "crowded_rerating",
                    "fundamentalState": "value_confirmed",
                    "riskState": "overheat",
                    "technicalState": "atr20_acceleration",
                },
            ),
            (
                analytics_complex.get_fundamental_ranking,
                "get_fundamental_rankings",
                {
                    "limit": 10,
                    "markets": "prime",
                    "metricKey": "eps_forecast_to_actual",
                    "forecastAboveRecentFyActuals": True,
                    "forecastLookbackFyCount": 5,
                },
            ),
            (
                analytics_complex.get_value_composite_ranking,
                "get_value_composite_ranking",
                {
                    "date": "2024-06-28",
                    "limit": 10,
                    "markets": "standard",
                    "profileId": "standard_pbr_tilt",
                    "scoreMethod": "equal_weight",
                    "forwardEpsMode": "fy",
                    "applyLiquidityFilter": False,
                },
            ),
            (
                analytics_complex.get_value_composite_score,
                "get_value_composite_score",
                {
                    "code": "7203",
                    "date": "2024-06-28",
                    "forwardEpsMode": "fy",
                },
            ),
        ],
    )
    async def test_ranking_route_runs_service_call_in_threadpool(
        self,
        route: Any,
        service_method: str,
        route_kwargs: dict[str, Any],
    ) -> None:
        request = SimpleNamespace(
            app=SimpleNamespace(state=SimpleNamespace(market_reader=MagicMock()))
        )
        expected_response = MagicMock()

        with (
            patch(
                "src.application.services.ranking_service.RankingService"
            ) as ranking_service_class,
            patch(
                "src.entrypoints.http.routes.analytics_complex.run_in_threadpool",
                new_callable=AsyncMock,
                create=True,
            ) as run_in_threadpool,
        ):
            service = ranking_service_class.return_value
            service_call = getattr(service, service_method)
            service_call.return_value = expected_response
            run_in_threadpool.return_value = expected_response

            response = await route(request=request, **route_kwargs)

        assert response is expected_response
        run_in_threadpool.assert_awaited_once()
        assert run_in_threadpool.await_args.args == (service_call,)
        assert run_in_threadpool.await_args.kwargs == self._expected_service_kwargs(
            service_method, route_kwargs
        )

    @staticmethod
    def _expected_service_kwargs(
        service_method: str, route_kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        if service_method == "get_symbol_ranking_snapshot":
            return {"code": route_kwargs["code"]}
        if service_method == "get_rankings":
            return {
                "date": route_kwargs["date"],
                "limit": route_kwargs["limit"],
                "markets": route_kwargs["markets"],
                "lookback_days": route_kwargs["lookbackDays"],
                "period_days": route_kwargs["periodDays"],
                "scope": route_kwargs["scope"],
                "sector33_name": route_kwargs["sector33Name"],
                "sector17_name": route_kwargs["sector17Name"],
                "include_valuation": route_kwargs["includeValuation"],
                "include_sector_strength": route_kwargs["includeSectorStrength"],
                "sector_strength_family": route_kwargs["sectorStrengthFamily"],
                "forward_eps_disclosed_within_days": route_kwargs[
                    "forwardEpsDisclosedWithinDays"
                ],
                "regime_state": route_kwargs["regimeState"],
                "fundamental_state": route_kwargs["fundamentalState"],
                "risk_state": route_kwargs["riskState"],
                "technical_state": route_kwargs["technicalState"],
            }
        if service_method == "get_fundamental_rankings":
            return {
                "limit": route_kwargs["limit"],
                "markets": route_kwargs["markets"],
                "metric_key": route_kwargs["metricKey"],
                "forecast_above_recent_fy_actuals": route_kwargs[
                    "forecastAboveRecentFyActuals"
                ],
                "forecast_lookback_fy_count": route_kwargs[
                    "forecastLookbackFyCount"
                ],
            }
        if service_method == "get_value_composite_ranking":
            return {
                "date": route_kwargs["date"],
                "limit": route_kwargs["limit"],
                "markets": route_kwargs["markets"],
                "score_method": route_kwargs["scoreMethod"],
                "profile_id": route_kwargs["profileId"],
                "forward_eps_mode": route_kwargs["forwardEpsMode"],
                "apply_liquidity_filter": route_kwargs["applyLiquidityFilter"],
            }
        return {
            "code": route_kwargs["code"],
            "date": route_kwargs["date"],
            "forward_eps_mode": route_kwargs["forwardEpsMode"],
        }
