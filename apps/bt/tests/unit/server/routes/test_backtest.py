"""server/routes/backtest.py のテスト"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.server.schemas.backtest import BacktestResultSummary, JobStatus


@pytest.fixture
def mock_services():
    """backtest_service, job_manager を一括mock"""
    with (
        patch("src.server.routes.backtest.backtest_service") as mock_bt_svc,
        patch("src.server.routes.backtest.job_manager") as mock_jm,
    ):
        yield mock_bt_svc, mock_jm


def _make_job(
    job_id="test-1",
    status=JobStatus.COMPLETED,
    result=None,
    html_path=None,
    job_type="backtest",
    raw_result=None,
):
    job = MagicMock()
    job.job_id = job_id
    job.job_type = job_type
    job.status = status
    job.progress = 1.0 if status == JobStatus.COMPLETED else 0.0
    job.message = None
    job.created_at = datetime(2025, 1, 1)
    job.started_at = datetime(2025, 1, 1)
    job.completed_at = datetime(2025, 1, 1) if status == JobStatus.COMPLETED else None
    job.error = None
    job.result = result
    job.raw_result = raw_result
    job.html_path = html_path
    job.strategy_name = "test_strategy"
    job.dataset_name = "test_dataset"
    job.execution_time = 5.0
    return job


@pytest.fixture
def client():
    from src.server.app import create_app
    app = create_app()
    return TestClient(app)


class TestRunBacktest:
    def test_success(self, client, mock_services):
        mock_bt_svc, mock_jm = mock_services
        mock_bt_svc.submit_backtest = AsyncMock(return_value="job-1")
        mock_jm.get_job.return_value = _make_job("job-1", JobStatus.PENDING)
        resp = client.post(
            "/api/backtest/run",
            json={"strategy_name": "test"},
        )
        assert resp.status_code == 200
        assert resp.json()["job_id"] == "job-1"

    def test_not_found_job(self, client, mock_services):
        mock_bt_svc, mock_jm = mock_services
        mock_bt_svc.submit_backtest = AsyncMock(return_value="job-1")
        mock_jm.get_job.return_value = None
        resp = client.post(
            "/api/backtest/run",
            json={"strategy_name": "test"},
        )
        assert resp.status_code == 404


class TestCancelJob:
    def test_success(self, client, mock_services):
        _, mock_jm = mock_services
        mock_jm.get_job.return_value = _make_job("job-1", JobStatus.RUNNING)
        mock_jm.cancel_job = AsyncMock(return_value=_make_job("job-1", JobStatus.CANCELLED))
        resp = client.post("/api/backtest/jobs/job-1/cancel")
        assert resp.status_code == 200

    def test_conflict_completed(self, client, mock_services):
        _, mock_jm = mock_services
        completed_job = _make_job("job-1", JobStatus.COMPLETED)
        mock_jm.get_job.return_value = completed_job
        mock_jm.cancel_job = AsyncMock(return_value=None)
        resp = client.post("/api/backtest/jobs/job-1/cancel")
        assert resp.status_code == 409


class TestGetResult:
    def test_success_no_html(self, client, mock_services):
        _, mock_jm = mock_services
        result = BacktestResultSummary(
            total_return=10.0,
            sharpe_ratio=1.5,
            calmar_ratio=2.0,
            max_drawdown=-5.0,
            win_rate=60.0,
            trade_count=100,
        )
        mock_jm.get_job.return_value = _make_job("job-1", JobStatus.COMPLETED, result=result)
        resp = client.get("/api/backtest/result/job-1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["strategy_name"] == "test_strategy"

    def test_not_completed(self, client, mock_services):
        _, mock_jm = mock_services
        mock_jm.get_job.return_value = _make_job("job-1", JobStatus.RUNNING)
        resp = client.get("/api/backtest/result/job-1")
        assert resp.status_code == 400

    def test_not_found(self, client, mock_services):
        _, mock_jm = mock_services
        mock_jm.get_job.return_value = None
        resp = client.get("/api/backtest/result/missing")
        assert resp.status_code == 404


class TestSignalAttributionEndpoints:
    def _attribution_result(self):
        return {
            "baseline_metrics": {"total_return": 12.0, "sharpe_ratio": 1.2},
            "signals": [
                {
                    "signal_id": "entry.volume",
                    "scope": "entry",
                    "param_key": "volume",
                    "signal_name": "出来高",
                    "loo": {
                        "status": "ok",
                        "variant_metrics": {"total_return": 10.0, "sharpe_ratio": 1.0},
                        "delta_total_return": 2.0,
                        "delta_sharpe_ratio": 0.2,
                        "error": None,
                    },
                    "shapley": {
                        "status": "ok",
                        "total_return": 1.5,
                        "sharpe_ratio": 0.1,
                        "method": "exact",
                        "sample_size": 2,
                        "error": None,
                    },
                }
            ],
            "top_n_selection": {
                "top_n_requested": 5,
                "top_n_effective": 1,
                "selected_signal_ids": ["entry.volume"],
                "scores": [{"signal_id": "entry.volume", "score": 1.0}],
            },
            "timing": {
                "total_seconds": 1.0,
                "baseline_seconds": 0.2,
                "loo_seconds": 0.5,
                "shapley_seconds": 0.3,
            },
            "shapley": {
                "method": "exact",
                "sample_size": 2,
                "error": None,
                "evaluations": 2,
            },
        }

    def test_run_submit_success(self, client):
        with (
            patch("src.server.routes.backtest.backtest_attribution_service") as mock_attr_svc,
            patch("src.server.routes.backtest.job_manager") as mock_jm,
        ):
            mock_attr_svc.submit_attribution = AsyncMock(return_value="attr-1")
            mock_jm.get_job.return_value = _make_job(
                "attr-1",
                JobStatus.PENDING,
                job_type="backtest_attribution",
            )
            resp = client.post(
                "/api/backtest/attribution/run",
                json={"strategy_name": "test"},
            )
            assert resp.status_code == 200
            assert resp.json()["job_id"] == "attr-1"

    def test_get_job_success_completed_with_result(self, client):
        with patch("src.server.routes.backtest.job_manager") as mock_jm:
            mock_jm.get_job.return_value = _make_job(
                "attr-1",
                JobStatus.COMPLETED,
                job_type="backtest_attribution",
                raw_result=self._attribution_result(),
            )
            resp = client.get("/api/backtest/attribution/jobs/attr-1")
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "completed"
            assert data["result_data"]["baseline_metrics"]["total_return"] == 12.0

    def test_get_job_type_mismatch(self, client):
        with patch("src.server.routes.backtest.job_manager") as mock_jm:
            mock_jm.get_job.return_value = _make_job(
                "attr-1",
                JobStatus.RUNNING,
                job_type="backtest",
            )
            resp = client.get("/api/backtest/attribution/jobs/attr-1")
            assert resp.status_code == 400

    def test_cancel_success(self, client):
        with patch("src.server.routes.backtest.job_manager") as mock_jm:
            mock_jm.get_job.return_value = _make_job(
                "attr-1",
                JobStatus.RUNNING,
                job_type="backtest_attribution",
            )
            mock_jm.cancel_job = AsyncMock(
                return_value=_make_job(
                    "attr-1",
                    JobStatus.CANCELLED,
                    job_type="backtest_attribution",
                )
            )
            resp = client.post("/api/backtest/attribution/jobs/attr-1/cancel")
            assert resp.status_code == 200
            assert resp.json()["status"] == "cancelled"

    def test_stream_nonexistent_job(self, client):
        with patch("src.server.routes.backtest.job_manager") as mock_jm:
            mock_jm.get_job.return_value = None
            resp = client.get("/api/backtest/attribution/jobs/missing/stream")
            assert resp.status_code == 404

    def test_result_success(self, client):
        with patch("src.server.routes.backtest.job_manager") as mock_jm:
            mock_jm.get_job.return_value = _make_job(
                "attr-1",
                JobStatus.COMPLETED,
                job_type="backtest_attribution",
                raw_result=self._attribution_result(),
            )
            resp = client.get("/api/backtest/attribution/result/attr-1")
            assert resp.status_code == 200
            data = resp.json()
            assert data["job_id"] == "attr-1"
            assert data["result"]["signals"][0]["signal_id"] == "entry.volume"

    def test_result_not_completed(self, client):
        with patch("src.server.routes.backtest.job_manager") as mock_jm:
            mock_jm.get_job.return_value = _make_job(
                "attr-1",
                JobStatus.RUNNING,
                job_type="backtest_attribution",
            )
            resp = client.get("/api/backtest/attribution/result/attr-1")
            assert resp.status_code == 400
