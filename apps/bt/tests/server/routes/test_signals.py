"""Signals API route tests."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.application.services.market_data_errors import MarketDataError
from src.entrypoints.http.app import app
from src.entrypoints.http.schemas.analytics_common import DataProvenance, ResponseDiagnostics
from src.entrypoints.http.schemas.signals import (
    SignalComputeRequest,
    SignalComputeResponse,
    SignalResult,
    SignalSpec,
)


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _make_signal_response() -> dict[str, object]:
    return {
        "stock_code": "7203",
        "timeframe": "daily",
        "strategy_name": None,
        "signals": {
            "buy_and_hold": {
                "label": "Buy & Hold",
                "mode": "entry",
                "trigger_dates": ["2025-01-15"],
                "count": 1,
                "diagnostics": ResponseDiagnostics().model_dump(mode="json"),
            }
        },
        "combined_entry": None,
        "combined_exit": None,
        "provenance": DataProvenance(
            source_kind="market",
            loaded_domains=["stock_data"],
        ).model_dump(mode="json"),
        "diagnostics": ResponseDiagnostics().model_dump(mode="json"),
    }


class TestSignalComputeRequestSchema:
    def test_default_values(self) -> None:
        req = SignalComputeRequest(
            stock_code="7203",
            signals=[SignalSpec(type="volume_ratio_above")],
        )
        assert req.stock_code == "7203"
        assert req.source == "market"
        assert req.timeframe == "daily"

    def test_strategy_name_can_replace_signals(self) -> None:
        req = SignalComputeRequest(
            stock_code="7203",
            strategy_name="production/test_strategy",
        )
        assert req.strategy_name == "production/test_strategy"
        assert req.signals == []

    def test_empty_request_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="strategy_name または signals"):
            SignalComputeRequest(stock_code="7203", signals=[])

    def test_strategy_name_and_signals_are_mutually_exclusive(self) -> None:
        with pytest.raises(ValueError, match="同時指定できません"):
            SignalComputeRequest(
                stock_code="7203",
                strategy_name="production/test_strategy",
                signals=[SignalSpec(type="buy_and_hold")],
            )


class TestSignalComputeResponseSchema:
    def test_valid_response(self) -> None:
        response = SignalComputeResponse(
            stock_code="7203",
            timeframe="daily",
            signals={
                "buy_and_hold": SignalResult(
                    label="Buy & Hold",
                    mode="entry",
                    trigger_dates=["2025-01-15"],
                    count=1,
                )
            },
            provenance=DataProvenance(source_kind="market", loaded_domains=["stock_data"]),
        )
        assert response.signals["buy_and_hold"].count == 1


class TestSignalComputeEndpoint:
    def test_compute_success(self, client: TestClient) -> None:
        with patch(
            "src.entrypoints.http.routes.signal_reference.SignalService.compute_signals",
            return_value=_make_signal_response(),
        ) as mock_compute:
            response = client.post(
                "/api/signals/compute",
                json={
                    "stock_code": "7203",
                    "timeframe": "daily",
                    "signals": [{"type": "buy_and_hold", "params": {}, "mode": "entry"}],
                },
            )

        assert response.status_code == 200
        data = response.json()
        assert data["signals"]["buy_and_hold"]["count"] == 1
        assert data["provenance"]["source_kind"] == "market"
        assert mock_compute.called

    def test_compute_strategy_overlay_success(self, client: TestClient) -> None:
        payload = _make_signal_response() | {
            "strategy_name": "production/test_strategy",
            "combined_entry": {
                "label": "production/test_strategy entry",
                "mode": "entry",
                "trigger_dates": ["2025-01-15"],
                "count": 1,
                "diagnostics": ResponseDiagnostics().model_dump(mode="json"),
            },
            "provenance": DataProvenance(
                source_kind="market",
                loaded_domains=["stock_data"],
                strategy_name="production/test_strategy",
                strategy_fingerprint="fingerprint-123",
            ).model_dump(mode="json"),
        }
        with patch(
            "src.entrypoints.http.routes.signal_reference.SignalService.compute_signals",
            return_value=payload,
        ):
            response = client.post(
                "/api/signals/compute",
                json={
                    "stock_code": "7203",
                    "strategy_name": "production/test_strategy",
                },
            )

        assert response.status_code == 200
        data = response.json()
        assert data["strategy_name"] == "production/test_strategy"
        assert data["combined_entry"]["count"] == 1

    def test_market_data_error_returns_404(self, client: TestClient) -> None:
        with patch(
            "src.entrypoints.http.routes.signal_reference.SignalService.compute_signals",
            side_effect=MarketDataError(
                "銘柄 7203 のローカルOHLCVデータがありません",
                reason="local_stock_data_missing",
                recovery="stock_refresh",
            ),
        ):
            response = client.post(
                "/api/signals/compute",
                json={
                    "stock_code": "7203",
                    "signals": [{"type": "buy_and_hold"}],
                },
            )

        assert response.status_code == 404

    def test_compute_value_error_returns_400(self, client: TestClient) -> None:
        with patch(
            "src.entrypoints.http.routes.signal_reference.SignalService.compute_signals",
            side_effect=ValueError("bad params"),
        ):
            response = client.post(
                "/api/signals/compute",
                json={"stock_code": "7203", "signals": [{"type": "buy_and_hold"}]},
            )

        assert response.status_code == 400

    def test_compute_internal_error_returns_500(self, client: TestClient) -> None:
        with patch(
            "src.entrypoints.http.routes.signal_reference.SignalService.compute_signals",
            side_effect=RuntimeError("boom"),
        ):
            response = client.post(
                "/api/signals/compute",
                json={"stock_code": "7203", "signals": [{"type": "buy_and_hold"}]},
            )

        assert response.status_code == 500


class TestSignalReferenceEndpoint:
    def test_get_reference(self, client: TestClient) -> None:
        response = client.get("/api/signals/reference")
        assert response.status_code == 200
        data = response.json()
        assert "signals" in data
        assert len(data["signals"]) > 0
        first = data["signals"][0]
        assert "signal_type" in first
        assert "chart" in first

    def test_get_reference_handles_internal_error(self, client: TestClient) -> None:
        with patch(
            "src.entrypoints.http.routes.signal_reference.build_signal_reference",
            side_effect=RuntimeError("boom"),
        ):
            response = client.get("/api/signals/reference")

        assert response.status_code == 500
