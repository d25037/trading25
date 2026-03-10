"""server/routes/optimize.py のエンドポイントテスト"""

from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from src.entrypoints.http.routes.optimize import _build_optimization_job_response
from src.entrypoints.http.schemas.common import JobStatus


@pytest.fixture
def mock_job_manager():
    with patch("src.entrypoints.http.routes.optimize.job_manager") as mock:
        yield mock


class TestBuildOptimizationJobResponse:
    def test_valid_job(self, mock_job_manager):
        from datetime import datetime

        mock_job = MagicMock()
        mock_job.job_id = "test-123"
        mock_job.job_type = "optimization"
        mock_job.status = "completed"
        mock_job.progress = 1.0
        mock_job.message = "Done"
        mock_job.created_at = datetime(2025, 1, 1)
        mock_job.started_at = datetime(2025, 1, 1)
        mock_job.completed_at = datetime(2025, 1, 1)
        mock_job.error = None
        mock_job.run_metadata = None
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
        assert response.execution_control is not None
        assert response.execution_control.cancel_requested is False

    def test_not_found(self, mock_job_manager):
        mock_job_manager.get_job.return_value = None
        with pytest.raises(HTTPException) as exc_info:
            _build_optimization_job_response("missing")
        assert exc_info.value.status_code == 404


def _make_job(job_id: str, status: JobStatus, job_type: str = "optimization") -> MagicMock:
    job = MagicMock()
    job.job_id = job_id
    job.job_type = job_type
    job.status = status
    job.progress = 1.0 if status in {JobStatus.CANCELLED, JobStatus.COMPLETED} else 0.0
    job.message = None
    job.created_at = datetime(2026, 1, 1)
    job.started_at = datetime(2026, 1, 1)
    job.completed_at = datetime(2026, 1, 1) if status in {JobStatus.CANCELLED, JobStatus.COMPLETED} else None
    job.error = None
    job.run_metadata = None
    job.best_score = None
    job.best_params = None
    job.worst_score = None
    job.worst_params = None
    job.total_combinations = None
    job.html_path = None
    return job


class TestOptimizationJobEndpoints:
    @pytest.fixture
    def client(self):
        from src.entrypoints.http.app import create_app

        app = create_app()
        return TestClient(app)

    def test_cancel_job_success(self, client):
        cancelled = _make_job("opt-1", JobStatus.CANCELLED)
        cancelled.message = "ジョブがキャンセルされました"

        with patch("src.entrypoints.http.routes.optimize.job_manager") as mock_jm:
            mock_jm.get_job.return_value = cancelled
            mock_jm.cancel_job = AsyncMock(return_value=cancelled)

            resp = client.post("/api/optimize/jobs/opt-1/cancel")

        assert resp.status_code == 200
        data = resp.json()
        assert data["job_id"] == "opt-1"
        assert data["status"] == "cancelled"
        assert data["execution_control"]["cancel_requested"] is False
        mock_jm.cancel_job.assert_awaited_once_with("opt-1")

    def test_cancel_job_conflict_when_already_terminal(self, client):
        completed = _make_job("opt-2", JobStatus.COMPLETED)

        with patch("src.entrypoints.http.routes.optimize.job_manager") as mock_jm:
            mock_jm.get_job.return_value = completed
            mock_jm.cancel_job = AsyncMock(return_value=None)

            resp = client.post("/api/optimize/jobs/opt-2/cancel")

        assert resp.status_code == 409
        assert "既に終了しています" in resp.json()["message"]

    def test_cancel_job_not_found(self, client):
        with patch("src.entrypoints.http.routes.optimize.job_manager") as mock_jm:
            mock_jm.get_job.return_value = None

            resp = client.post("/api/optimize/jobs/missing/cancel")

        assert resp.status_code == 404

    def test_get_status_rejects_non_optimization_job(self, client):
        non_opt_job = _make_job("job-foreign", JobStatus.RUNNING, job_type="backtest")
        with patch("src.entrypoints.http.routes.optimize.job_manager") as mock_jm:
            mock_jm.get_job.return_value = non_opt_job

            resp = client.get("/api/optimize/jobs/job-foreign")

        assert resp.status_code == 400
        assert "最適化ジョブではありません" in resp.json()["message"]

    def test_cancel_rejects_non_optimization_job(self, client):
        non_opt_job = _make_job("job-foreign", JobStatus.RUNNING, job_type="backtest")
        with patch("src.entrypoints.http.routes.optimize.job_manager") as mock_jm:
            mock_jm.get_job.return_value = non_opt_job

            resp = client.post("/api/optimize/jobs/job-foreign/cancel")

        assert resp.status_code == 400
        assert "最適化ジョブではありません" in resp.json()["message"]

    def test_stream_rejects_non_optimization_job(self, client):
        non_opt_job = _make_job("job-foreign", JobStatus.RUNNING, job_type="backtest")
        with patch("src.entrypoints.http.routes.optimize.job_manager") as mock_jm:
            mock_jm.get_job.return_value = non_opt_job

            resp = client.get("/api/optimize/jobs/job-foreign/stream")

        assert resp.status_code == 400
        assert "最適化ジョブではありません" in resp.json()["message"]


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

    def test_list_grid_configs_reads_real_files(self, client, tmp_path):
        (tmp_path / "alpha_grid.yaml").write_text("parameter_ranges:\n  period: [10, 20]\n", encoding="utf-8")
        (tmp_path / "nested_grid.yaml").write_text(
            "parameter_ranges:\n  entry:\n    rsi: [20, 30, 40]\n",
            encoding="utf-8",
        )

        with patch("src.entrypoints.http.routes.optimize.get_all_optimization_grid_dirs", return_value=[tmp_path]):
            resp = client.get("/api/optimize/grid-configs")

        assert resp.status_code == 200
        payload = resp.json()
        assert payload["total"] == 2
        strategy_names = {item["strategy_name"] for item in payload["configs"]}
        assert strategy_names == {"alpha", "nested"}

    def test_save_grid_config_file_write_error_returns_500(self, client, tmp_path):
        grid_file = tmp_path / "write_fail.yaml"
        with (
            patch("src.entrypoints.http.routes.optimize._get_grid_write_path", return_value=grid_file),
            patch.object(Path, "write_text", side_effect=OSError("disk full")),
        ):
            resp = client.put(
                "/api/optimize/grid-configs/test",
                json={"content": "parameter_ranges:\n  period: [10]\n"},
            )

        assert resp.status_code == 500
        assert "ファイル保存エラー" in resp.json()["message"]

    def test_delete_grid_config_file_remove_error_returns_500(self, client, tmp_path):
        grid_file = tmp_path / "test_grid.yaml"
        grid_file.write_text("parameter_ranges:\n  period: [10]\n", encoding="utf-8")
        with (
            patch("src.entrypoints.http.routes.optimize._find_grid_file", return_value=grid_file),
            patch("src.entrypoints.http.routes.optimize.os.remove", side_effect=OSError("permission denied")),
        ):
            resp = client.delete("/api/optimize/grid-configs/test")

        assert resp.status_code == 500
        assert "ファイル削除エラー" in resp.json()["message"]

    def test_save_grid_config_uses_default_write_path(self, client, tmp_path):
        with patch("src.shared.paths.resolver.get_optimization_grid_dir", return_value=tmp_path):
            resp = client.put(
                "/api/optimize/grid-configs/alpha",
                json={"content": "parameter_ranges:\n  period: [5, 10]\n"},
            )

        assert resp.status_code == 200
        saved = tmp_path / "alpha_grid.yaml"
        assert saved.exists()
        assert "parameter_ranges" in saved.read_text(encoding="utf-8")


class TestOptimizationRouteAdditionalCoverage:
    @pytest.fixture
    def client(self):
        from src.entrypoints.http.app import create_app

        app = create_app()
        return TestClient(app)

    def test_run_optimization_success(self, client):
        running_job = _make_job("opt-run-1", JobStatus.RUNNING)
        running_job.progress = 0.0
        running_job.message = "running"

        with (
            patch("src.entrypoints.http.routes.optimize.optimization_service") as mock_service,
            patch("src.entrypoints.http.routes.optimize.job_manager") as mock_jm,
        ):
            mock_service.submit_optimization = AsyncMock(return_value="opt-run-1")
            mock_jm.get_job.return_value = running_job

            resp = client.post("/api/optimize/run", json={"strategy_name": "production/range_break_v5"})

        assert resp.status_code == 200
        data = resp.json()
        assert data["job_id"] == "opt-run-1"
        assert data["status"] == "running"

    def test_run_optimization_failure_returns_500(self, client):
        with patch("src.entrypoints.http.routes.optimize.optimization_service") as mock_service:
            mock_service.submit_optimization = AsyncMock(side_effect=RuntimeError("submit failed"))

            resp = client.post("/api/optimize/run", json={"strategy_name": "production/range_break_v5"})

        assert resp.status_code == 500
        assert "submit failed" in resp.json()["message"]

    def test_stream_optimization_events_success(self, client):
        running_job = _make_job("opt-stream-1", JobStatus.RUNNING)
        with (
            patch("src.entrypoints.http.routes.optimize.job_manager") as mock_jm,
            patch("src.entrypoints.http.routes.optimize.sse_manager") as mock_sse,
        ):
            mock_jm.get_job.return_value = running_job
            mock_sse.job_event_generator.return_value = iter(())

            resp = client.get("/api/optimize/jobs/opt-stream-1/stream")

        assert resp.status_code == 200

    def test_list_optimization_html_files(self, client, tmp_path):
        created_at = datetime(2026, 1, 1)
        with (
            patch("src.entrypoints.http.routes.optimize.get_optimization_results_dir", return_value=tmp_path),
            patch(
                "src.entrypoints.http.routes.optimize.list_html_files_in_dir",
                return_value=(
                    [
                        {
                            "strategy_name": "production/range_break_v5",
                            "filename": "optimization_report.html",
                            "dataset_name": "dataset-1",
                            "created_at": created_at,
                            "size_bytes": 128,
                        }
                    ],
                    1,
                ),
            ),
        ):
            resp = client.get("/api/optimize/html-files?strategy=production/range_break_v5&limit=10")

        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["files"][0]["filename"] == "optimization_report.html"

    def test_get_optimization_html_file_content(self, client, tmp_path):
        with (
            patch("src.entrypoints.http.routes.optimize.get_optimization_results_dir", return_value=tmp_path),
            patch("src.entrypoints.http.routes.optimize.read_html_file", return_value="PGh0bWw+PC9odG1sPg=="),
        ):
            resp = client.get("/api/optimize/html-files/range_break_v5/optimization_report.html")

        assert resp.status_code == 200
        payload = resp.json()
        assert payload["strategy_name"] == "range_break_v5"
        assert payload["filename"] == "optimization_report.html"

    def test_rename_optimization_html_file(self, client, tmp_path):
        with (
            patch("src.entrypoints.http.routes.optimize.get_optimization_results_dir", return_value=tmp_path),
            patch("src.entrypoints.http.routes.optimize.rename_html_file") as mock_rename,
        ):
            resp = client.post(
                "/api/optimize/html-files/range_break_v5/old.html/rename",
                json={"new_filename": "new.html"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["old_filename"] == "old.html"
        assert data["new_filename"] == "new.html"
        mock_rename.assert_called_once()

    def test_delete_optimization_html_file(self, client, tmp_path):
        with (
            patch("src.entrypoints.http.routes.optimize.get_optimization_results_dir", return_value=tmp_path),
            patch("src.entrypoints.http.routes.optimize.delete_html_file") as mock_delete,
        ):
            resp = client.delete("/api/optimize/html-files/range_break_v5/optimization_report.html")

        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["filename"] == "optimization_report.html"
        mock_delete.assert_called_once()
