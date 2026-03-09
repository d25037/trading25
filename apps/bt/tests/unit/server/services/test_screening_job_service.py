"""
Unit tests for ScreeningJobService.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.domains.backtest.contracts import RunType
from src.entrypoints.http.schemas.backtest import JobStatus
from src.entrypoints.http.schemas.screening_job import ScreeningJobRequest
from src.application.services.screening_job_service import ScreeningJobService


@pytest.mark.asyncio
async def test_submit_screening_passes_explicit_run_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = MagicMock()
    manager.create_job.return_value = "job-1"
    manager.set_job_task = AsyncMock()
    service = ScreeningJobService(manager=manager, max_workers=1)

    fake_task = object()
    captured: dict[str, object] = {}

    def _fake_create_task(coro: object) -> object:
        captured["coro"] = coro
        coro.close()
        return fake_task

    monkeypatch.setattr(asyncio, "create_task", _fake_create_task)

    request = ScreeningJobRequest(markets="0111", recentDays=5, limit=20)
    job_id = await service.submit_screening(reader=MagicMock(), request=request)

    assert job_id == "job-1"
    _, kwargs = manager.create_job.call_args
    run_spec = kwargs["run_spec"]
    assert run_spec.run_type == RunType.SCREENING
    assert run_spec.parameters["markets"] == "0111"
    assert run_spec.parameters["recentDays"] == 5
    manager.set_job_task.assert_awaited_once_with("job-1", fake_task)

    service._executor.shutdown(wait=True)  # noqa: SLF001


@pytest.mark.asyncio
async def test_run_job_does_not_release_slot_when_acquire_is_cancelled() -> None:
    manager = MagicMock()
    manager.acquire_slot = AsyncMock(side_effect=asyncio.CancelledError())
    manager.update_job_status = AsyncMock()
    manager.set_job_raw_result = AsyncMock()
    manager.release_slot = MagicMock()

    service = ScreeningJobService(manager=manager, max_workers=1)
    request = ScreeningJobRequest()

    try:
        with pytest.raises(asyncio.CancelledError):
            await service._run_job("job-1", reader=MagicMock(), request=request)  # noqa: SLF001
    finally:
        service._executor.shutdown(wait=True)  # noqa: SLF001

    manager.release_slot.assert_not_called()
    manager.update_job_status.assert_awaited_with(
        "job-1",
        JobStatus.CANCELLED,
        message="Screening ジョブがキャンセルされました",
    )


@pytest.mark.asyncio
async def test_run_job_releases_slot_after_success(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = MagicMock()
    manager.acquire_slot = AsyncMock(return_value=None)
    manager.update_job_status = AsyncMock()
    manager.set_job_raw_result = AsyncMock()
    manager.release_slot = MagicMock()

    class _DummyResponse:
        def model_dump(self) -> dict[str, object]:
            return {
                "results": [],
                "summary": {
                    "totalStocksScreened": 0,
                    "matchCount": 0,
                    "skippedCount": 0,
                    "byStrategy": {},
                    "strategiesEvaluated": [],
                    "strategiesWithoutBacktestMetrics": [],
                    "warnings": [],
                },
                "markets": ["prime"],
                "recentDays": 10,
                "referenceDate": None,
                "sortBy": "matchedDate",
                "order": "desc",
                "lastUpdated": "2026-01-01T00:00:00Z",
            }

    class _DummyScreeningService:
        def __init__(self, reader: object) -> None:
            self._reader = reader

        def run_screening(self, **kwargs: object) -> _DummyResponse:
            return _DummyResponse()

    monkeypatch.setattr(
        "src.application.services.screening_job_service.ScreeningService",
        _DummyScreeningService,
    )

    service = ScreeningJobService(manager=manager, max_workers=1)
    request = ScreeningJobRequest()

    try:
        await service._run_job("job-2", reader=MagicMock(), request=request)  # noqa: SLF001
    finally:
        service._executor.shutdown(wait=True)  # noqa: SLF001

    manager.release_slot.assert_called_once()
    manager.set_job_raw_result.assert_awaited_once()
