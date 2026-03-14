"""Tests for dataset management routes (create and jobs)."""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app
from src.entrypoints.http.schemas.job import JobStatus
from src.application.services.dataset_builder_service import DatasetJobData, DatasetResult
from src.application.services.generic_job_manager import JobInfo


def _dataset_resolver(client: TestClient) -> Any:
    return cast(Any, client.app.state).dataset_resolver


@pytest.fixture
def client() -> TestClient:
    app = create_app()

    # Mock dataset_resolver
    resolver = MagicMock()
    resolver.resolve.return_value = None
    resolver.list_datasets.return_value = []
    resolver.get_artifact_paths.return_value = []
    app.state.dataset_resolver = resolver

    # Mock market reader
    app.state.market_reader = MagicMock()

    return TestClient(app)


# --- Create ---


def test_create_dataset_invalid_preset(client: TestClient) -> None:
    resp = client.post("/api/dataset", json={"name": "test", "preset": "invalid"})
    assert resp.status_code == 400
    assert "Unknown preset" in resp.json()["message"]


def test_create_dataset_existing_no_overwrite(client: TestClient) -> None:
    resolver = _dataset_resolver(client)
    resolver.get_artifact_paths.return_value = ["/tmp/test"]

    resp = client.post("/api/dataset", json={"name": "test", "preset": "quickTesting"})
    assert resp.status_code == 409
    assert "already exists" in resp.json()["message"]


def test_create_dataset_requires_market_reader(client: TestClient) -> None:
    client.app.state.market_reader = None

    resp = client.post("/api/dataset", json={"name": "test", "preset": "quickTesting"})
    assert resp.status_code == 422
    assert "Market database not initialized" in resp.json()["message"]


def test_create_dataset_success(client: TestClient) -> None:
    resolver = _dataset_resolver(client)
    resolver.get_artifact_paths.return_value = []

    mock_job = MagicMock()
    mock_job.job_id = "test-job-id"

    with patch("src.entrypoints.http.routes.dataset.start_dataset_build", new_callable=AsyncMock) as mock_start:
        mock_start.return_value = mock_job
        resp = client.post("/api/dataset", json={"name": "test", "preset": "quickTesting"})

    assert resp.status_code == 202
    mock_start.assert_awaited_once()
    data_arg: DatasetJobData = mock_start.await_args.args[0]
    assert data_arg == DatasetJobData(name="test", preset="quickTesting", overwrite=False)
    data = resp.json()
    assert data["jobId"] == "test-job-id"
    assert data["status"] == "pending"
    assert data["preset"] == "quickTesting"
    assert data["message"] == "Dataset creation job started"


def test_create_dataset_rejects_removed_timeout_minutes(client: TestClient) -> None:
    resolver = _dataset_resolver(client)
    resolver.get_artifact_paths.return_value = []

    resp = client.post("/api/dataset", json={"name": "test", "preset": "quickTesting", "timeoutMinutes": 90})
    assert resp.status_code == 422


def test_create_dataset_conflict(client: TestClient) -> None:
    resolver = _dataset_resolver(client)
    resolver.get_artifact_paths.return_value = []

    with patch("src.entrypoints.http.routes.dataset.start_dataset_build", new_callable=AsyncMock) as mock_start:
        mock_start.return_value = None
        resp = client.post("/api/dataset", json={"name": "test", "preset": "quickTesting"})

    assert resp.status_code == 409
    assert "already running" in resp.json()["message"]


# --- Get Job ---


def test_get_job_not_found(client: TestClient) -> None:
    resp = client.get("/api/dataset/jobs/nonexistent-id")
    assert resp.status_code == 404


def test_get_job_pending(client: TestClient) -> None:
    job_data = DatasetJobData(name="test", preset="quickTesting")
    job = JobInfo(job_id="test-123", status=JobStatus.PENDING, data=job_data)

    with patch("src.entrypoints.http.routes.dataset.dataset_job_manager") as mock_mgr:
        mock_mgr.get_job.return_value = job
        resp = client.get("/api/dataset/jobs/test-123")

    assert resp.status_code == 200
    data = resp.json()
    assert data["jobId"] == "test-123"
    assert data["status"] == "pending"
    assert data["name"] == "test"
    assert data["preset"] == "quickTesting"
    assert data["progress"] is None
    assert data["result"] is None


def test_get_job_completed(client: TestClient) -> None:
    from datetime import UTC, datetime

    job_data = DatasetJobData(name="test", preset="quickTesting")
    result = DatasetResult(success=True, totalStocks=3, processedStocks=3, outputPath="/data/test.db")
    job = JobInfo(
        job_id="test-456",
        status=JobStatus.COMPLETED,
        data=job_data,
        result=result,
        completed_at=datetime(2024, 1, 1, tzinfo=UTC),
    )

    with patch("src.entrypoints.http.routes.dataset.dataset_job_manager") as mock_mgr:
        mock_mgr.get_job.return_value = job
        resp = client.get("/api/dataset/jobs/test-456")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "completed"
    assert data["result"]["success"] is True
    assert data["result"]["totalStocks"] == 3


# --- Cancel Job ---


def test_cancel_job_not_found(client: TestClient) -> None:
    resp = client.delete("/api/dataset/jobs/nonexistent-id")
    assert resp.status_code == 404


def test_cancel_job_not_cancellable(client: TestClient) -> None:
    job_data = DatasetJobData(name="test", preset="quickTesting")
    job = JobInfo(job_id="test-789", status=JobStatus.COMPLETED, data=job_data)

    with patch("src.entrypoints.http.routes.dataset.dataset_job_manager") as mock_mgr:
        mock_mgr.get_job.return_value = job
        resp = client.delete("/api/dataset/jobs/test-789")

    assert resp.status_code == 400
    assert "cannot be cancelled" in resp.json()["message"]


def test_cancel_job_success(client: TestClient) -> None:
    job_data = DatasetJobData(name="test", preset="quickTesting")
    job = JobInfo(job_id="test-cancel", status=JobStatus.RUNNING, data=job_data)

    with patch("src.entrypoints.http.routes.dataset.dataset_job_manager") as mock_mgr:
        mock_mgr.get_job.return_value = job
        mock_mgr.cancel_job = AsyncMock()
        resp = client.delete("/api/dataset/jobs/test-cancel")

    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    assert data["jobId"] == "test-cancel"
