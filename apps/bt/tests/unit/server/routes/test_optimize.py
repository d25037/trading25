"""server/routes/optimize.py のエンドポイントテスト"""

from datetime import datetime
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
        mock_job.raw_result = None
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
    job.raw_result = None
    job.best_score = None
    job.best_params = None
    job.worst_score = None
    job.worst_params = None
    job.total_combinations = None
    job.html_path = None
    return job


class TestOptimizationJobEndpoints:
    @pytest.fixture(scope="module")
    def client(self):
        from src.entrypoints.http.app import create_app

        app = create_app()
        with TestClient(app) as test_client:
            yield test_client

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


class TestStrategyOptimizationEndpoints:
    @pytest.fixture(scope="module")
    def client(self):
        from src.entrypoints.http.app import create_app
        app = create_app()
        with TestClient(app) as test_client:
            yield test_client

    @staticmethod
    def _analysis(
        *,
        optimization=None,
        yaml_content="description: demo\nparameter_ranges: {}\n",
        valid=True,
        ready_to_run=False,
        param_count=0,
        combinations=0,
        errors=None,
        warnings=None,
        drift=None,
    ):
        return MagicMock(
            optimization=optimization,
            yaml_content=yaml_content,
            valid=valid,
            ready_to_run=ready_to_run,
            param_count=param_count,
            combinations=combinations,
            errors=errors or [],
            warnings=warnings or [],
            drift=drift or [],
        )

    def test_get_strategy_optimization_not_found(self, client):
        with patch(
            "src.entrypoints.http.routes.strategies.strategy_optimization_service.get_state",
            side_effect=FileNotFoundError("missing"),
        ):
            resp = client.get("/api/strategies/missing/optimization")
        assert resp.status_code == 404

    def test_get_strategy_optimization_success(self, client):
        analysis = self._analysis(
            optimization={"description": "demo", "parameter_ranges": {}},
            param_count=1,
            combinations=3,
            ready_to_run=True,
        )
        with patch(
            "src.entrypoints.http.routes.strategies.strategy_optimization_service.get_state",
            return_value=analysis,
        ):
            resp = client.get("/api/strategies/production/test/optimization")
        assert resp.status_code == 200
        data = resp.json()
        assert data["strategy_name"] == "production/test"
        assert data["persisted"] is True
        assert data["source"] == "saved"
        assert data["param_count"] == 1
        assert data["ready_to_run"] is True

    def test_generate_strategy_optimization_draft_success(self, client):
        analysis = self._analysis(
            optimization={"description": "draft", "parameter_ranges": {}},
            yaml_content="description: draft\nparameter_ranges: {}\n",
            warnings=[MagicMock(path="optimization.parameter_ranges", message="draft warning")],
        )
        with patch(
            "src.entrypoints.http.routes.strategies.strategy_optimization_service.generate_draft",
            return_value=analysis,
        ):
            resp = client.post("/api/strategies/production/test/optimization/draft")

        assert resp.status_code == 200
        data = resp.json()
        assert data["source"] == "draft"
        assert data["warnings"][0]["message"] == "draft warning"

    def test_save_strategy_optimization_valid(self, client):
        analysis = self._analysis(
            optimization={"description": "saved", "parameter_ranges": {}},
            yaml_content="description: saved\nparameter_ranges: {}\n",
            param_count=2,
            combinations=9,
            ready_to_run=True,
        )
        with (
            patch("src.entrypoints.http.routes.strategies._config_loader.is_updatable_category", return_value=True),
            patch(
                "src.entrypoints.http.routes.strategies.strategy_optimization_service.save",
                return_value=analysis,
            ),
        ):
            resp = client.put(
                "/api/strategies/production/test/optimization",
                json={
                    "yaml_content": (
                        "description: saved\n"
                        "parameter_ranges:\n"
                        "  entry_filter_params:\n"
                        "    breakout:\n"
                        "      period: [10, 20, 30]\n"
                    ),
                },
            )
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        assert resp.json()["combinations"] == 9
        assert resp.json()["ready_to_run"] is True

    def test_save_strategy_optimization_validation_error(self, client):
        with (
            patch("src.entrypoints.http.routes.strategies._config_loader.is_updatable_category", return_value=True),
            patch(
                "src.entrypoints.http.routes.strategies.strategy_optimization_service.save",
                side_effect=ValueError("invalid optimization yaml"),
            ),
        ):
            resp = client.put(
                "/api/strategies/production/test/optimization",
                json={"yaml_content": "invalid: [yaml: bad"},
            )
        assert resp.status_code == 400
        assert "invalid optimization yaml" in resp.json()["message"]

    def test_save_strategy_optimization_forbidden(self, client):
        with patch("src.entrypoints.http.routes.strategies._config_loader.is_updatable_category", return_value=False):
            resp = client.put(
                "/api/strategies/legacy/test/optimization",
                json={"yaml_content": "description: demo\nparameter_ranges: {}\n"},
            )
        assert resp.status_code == 403

    def test_delete_strategy_optimization_success(self, client):
        with (
            patch("src.entrypoints.http.routes.strategies._config_loader.is_updatable_category", return_value=True),
            patch("src.entrypoints.http.routes.strategies.strategy_optimization_service.delete"),
        ):
            resp = client.delete("/api/strategies/production/test/optimization")
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        assert resp.json()["strategy_name"] == "production/test"

    def test_delete_strategy_optimization_not_found(self, client):
        with (
            patch("src.entrypoints.http.routes.strategies._config_loader.is_updatable_category", return_value=True),
            patch(
                "src.entrypoints.http.routes.strategies.strategy_optimization_service.delete",
                side_effect=FileNotFoundError("missing"),
            ),
        ):
            resp = client.delete("/api/strategies/missing/optimization")
        assert resp.status_code == 404


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

    def test_run_optimization_validation_error_returns_400(self, client):
        with patch("src.entrypoints.http.routes.optimize.optimization_service") as mock_service:
            mock_service.submit_optimization = AsyncMock(side_effect=ValueError("grid invalid"))

            resp = client.post("/api/optimize/run", json={"strategy_name": "production/range_break_v5"})

        assert resp.status_code == 400
        assert "grid invalid" in resp.json()["message"]

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
