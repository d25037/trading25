"""signal_reference.py ルートのテスト"""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.server.app import create_app


@pytest.fixture()
def client() -> TestClient:
    app = create_app()
    return TestClient(app)


class TestGetSignalReference:
    def test_success(self, client: TestClient) -> None:
        mock_data = {
            "signals": [],
            "categories": [],
            "total": 0,
        }
        with patch(
            "src.server.routes.signal_reference.build_signal_reference",
            return_value=mock_data,
        ):
            resp = client.get("/api/signals/reference")
            assert resp.status_code == 200
            data = resp.json()
            assert "signals" in data
            assert "total" in data

    def test_error_returns_500(self, client: TestClient) -> None:
        with patch(
            "src.server.routes.signal_reference.build_signal_reference",
            side_effect=Exception("fail"),
        ):
            resp = client.get("/api/signals/reference")
            assert resp.status_code == 500


class TestGetSignalSchema:
    def test_success(self, client: TestClient) -> None:
        resp = client.get("/api/signals/schema")
        assert resp.status_code == 200

    def test_error_returns_500(self, client: TestClient) -> None:
        with patch(
            "src.server.routes.signal_reference.SignalParams.model_json_schema",
            side_effect=Exception("schema-fail"),
        ):
            resp = client.get("/api/signals/schema")
            assert resp.status_code == 500


class TestComputeSignals:
    def test_value_error_returns_400(self, client: TestClient) -> None:
        with patch(
            "src.server.routes.signal_reference.SignalService.compute_signals",
            side_effect=ValueError("invalid input"),
        ):
            resp = client.post(
                "/api/signals/compute",
                json={
                    "stock_code": "7203",
                    "signals": [{"type": "volume", "params": {"threshold": 1.5}}],
                },
            )
            assert resp.status_code == 400

    def test_unexpected_error_returns_500(self, client: TestClient) -> None:
        with patch(
            "src.server.routes.signal_reference.SignalService.compute_signals",
            side_effect=RuntimeError("boom"),
        ):
            resp = client.post(
                "/api/signals/compute",
                json={
                    "stock_code": "7203",
                    "signals": [{"type": "volume", "params": {"threshold": 1.5}}],
                },
            )
            assert resp.status_code == 500
