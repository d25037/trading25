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
):
    job = MagicMock()
    job.job_id = job_id
    job.status = status
    job.progress = 1.0 if status == JobStatus.COMPLETED else 0.0
    job.message = None
    job.created_at = datetime(2025, 1, 1)
    job.started_at = datetime(2025, 1, 1)
    job.completed_at = datetime(2025, 1, 1) if status == JobStatus.COMPLETED else None
    job.error = None
    job.result = result
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
