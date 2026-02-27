"""server/routes/optimize.py のエンドポイントテスト"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from src.entrypoints.http.routes.optimize import _build_optimization_job_response


@pytest.fixture
def mock_job_manager():
    with patch("src.entrypoints.http.routes.optimize.job_manager") as mock:
        yield mock


class TestBuildOptimizationJobResponse:
    def test_valid_job(self, mock_job_manager):
        from datetime import datetime
        mock_job = MagicMock()
        mock_job.job_id = "test-123"
        mock_job.status = "completed"
        mock_job.progress = 1.0
        mock_job.message = "Done"
        mock_job.created_at = datetime(2025, 1, 1)
        mock_job.started_at = datetime(2025, 1, 1)
        mock_job.completed_at = datetime(2025, 1, 1)
        mock_job.error = None
        mock_job.best_score = 0.85
        mock_job.best_params = {"period": 20, "threshold": 0.3}
        mock_job.worst_score = 0.12
        mock_job.worst_params = {"period": 5, "threshold": 0.9}
        mock_job.total_combinations = 100
        mock_job.html_path = "/path/to/result.html"
        mock_job_manager.get_job.return_value = mock_job

        response = _build_optimization_job_response("test-123")
        assert response.job_id == "test-123"
        assert response.best_score == 0.85
        assert response.best_params == {"period": 20, "threshold": 0.3}
        assert response.worst_score == 0.12
        assert response.worst_params == {"period": 5, "threshold": 0.9}

    def test_not_found(self, mock_job_manager):
        mock_job_manager.get_job.return_value = None
        with pytest.raises(HTTPException) as exc_info:
            _build_optimization_job_response("missing")
        assert exc_info.value.status_code == 404


class TestGridConfigEndpoints:
    @pytest.fixture
    def client(self):
        from src.entrypoints.http.app import create_app
        app = create_app()
        return TestClient(app)

    def test_get_grid_config_not_found(self, client):
        with patch("src.entrypoints.http.routes.optimize._find_grid_file", return_value=None):
            resp = client.get("/api/optimize/grid-configs/missing")
        assert resp.status_code == 404

    def test_get_grid_config_success(self, client, tmp_path):
        grid_file = tmp_path / "test_grid.yaml"
        grid_file.write_text("parameter_ranges:\n  period: [10, 20]\n")
        with patch("src.entrypoints.http.routes.optimize._find_grid_file", return_value=grid_file):
            resp = client.get("/api/optimize/grid-configs/test")
        assert resp.status_code == 200
        data = resp.json()
        assert data["strategy_name"] == "test"
        assert data["param_count"] == 1

    def test_save_grid_config_valid(self, client, tmp_path):
        grid_file = tmp_path / "test_grid.yaml"
        with patch("src.entrypoints.http.routes.optimize._get_grid_write_path", return_value=grid_file):
            resp = client.put(
                "/api/optimize/grid-configs/test",
                json={"content": "parameter_ranges:\n  period: [10, 20, 30]\n"},
            )
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        assert resp.json()["combinations"] == 3

    def test_save_grid_config_invalid_yaml(self, client):
        with patch("src.entrypoints.http.routes.optimize._get_grid_write_path", return_value=Path("/tmp/test.yaml")):
            resp = client.put(
                "/api/optimize/grid-configs/test",
                json={"content": "invalid: [yaml: bad"},
            )
        assert resp.status_code == 400

    def test_delete_grid_config_success(self, client, tmp_path):
        grid_file = tmp_path / "test_grid.yaml"
        grid_file.write_text("data")
        with patch("src.entrypoints.http.routes.optimize._find_grid_file", return_value=grid_file):
            resp = client.delete("/api/optimize/grid-configs/test")
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_delete_grid_config_not_found(self, client):
        with patch("src.entrypoints.http.routes.optimize._find_grid_file", return_value=None):
            resp = client.delete("/api/optimize/grid-configs/missing")
        assert resp.status_code == 404
