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
