"""
Unit tests for ScreeningJobService.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.domains.backtest.contracts import RunType
from src.application.services.screening_default_markets import ScreeningMarketResolution
from src.entrypoints.http.schemas.backtest import JobStatus
from src.entrypoints.http.schemas.screening_job import ScreeningJobRequest
from src.application.services.screening_job_service import ScreeningJobService, _read_positive_int_env


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
    request = ScreeningJobRequest(markets="prime")

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
                "entry_decidability": "pre_open_decidable",
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
    request = ScreeningJobRequest(markets="prime")

    try:
        await service._run_job("job-2", reader=MagicMock(), request=request)  # noqa: SLF001
    finally:
        service._executor.shutdown(wait=True)  # noqa: SLF001

    manager.release_slot.assert_called_once()
    manager.set_job_raw_result.assert_awaited_once()


@pytest.mark.asyncio
async def test_shutdown_does_not_mutate_job_state() -> None:
    manager = MagicMock()
    service = ScreeningJobService(manager=manager, max_workers=1)

    try:
        await service.shutdown()
    finally:
        if not bool(getattr(service._executor, "_shutdown", False)):  # noqa: SLF001
            service._executor.shutdown(wait=True)  # noqa: SLF001

    manager.list_jobs.assert_not_called()
    manager.cancel_job.assert_not_called()


def test_read_positive_int_env_handles_missing_and_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BT_SCREENING_MAX_CONCURRENT_JOBS", raising=False)
    assert _read_positive_int_env("BT_SCREENING_MAX_CONCURRENT_JOBS", 3) == 3

    monkeypatch.setenv("BT_SCREENING_MAX_CONCURRENT_JOBS", "-1")
    assert _read_positive_int_env("BT_SCREENING_MAX_CONCURRENT_JOBS", 3) == 3

    monkeypatch.setenv("BT_SCREENING_MAX_CONCURRENT_JOBS", "abc")
    assert _read_positive_int_env("BT_SCREENING_MAX_CONCURRENT_JOBS", 3) == 3

    monkeypatch.setenv("BT_SCREENING_MAX_CONCURRENT_JOBS", "4")
    assert _read_positive_int_env("BT_SCREENING_MAX_CONCURRENT_JOBS", 3) == 4


@pytest.mark.asyncio
async def test_get_job_request_returns_submitted_request(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = MagicMock()
    manager.create_job.return_value = "job-2"
    manager.set_job_task = AsyncMock()
    service = ScreeningJobService(manager=manager, max_workers=1)

    def _fake_create_task(coro: object) -> object:
        coro.close()
        return object()

    monkeypatch.setattr(asyncio, "create_task", _fake_create_task)
    request = ScreeningJobRequest(markets="0111", recentDays=3)

    await service.submit_screening(reader=MagicMock(), request=request)

    assert service.get_job_request("job-2") == request
    service._executor.shutdown(wait=True)  # noqa: SLF001


@pytest.mark.asyncio
async def test_submit_screening_infers_default_markets_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = MagicMock()
    manager.create_job.return_value = "job-auto"
    manager.set_job_task = AsyncMock()
    service = ScreeningJobService(manager=manager, max_workers=1)

    fake_task = object()

    def _fake_create_task(coro: object) -> object:
        coro.close()
        return fake_task

    monkeypatch.setattr(asyncio, "create_task", _fake_create_task)
    monkeypatch.setattr(
        "src.application.services.screening_job_service.resolve_default_screening_markets",
        lambda **_kwargs: ScreeningMarketResolution(
            strategy_names=["production/range_break_v15", "production/forward_eps_driven"],
            markets=["prime", "standard"],
            markets_param="prime,standard",
        ),
    )

    job_id = await service.submit_screening(
        reader=MagicMock(),
        request=ScreeningJobRequest(strategies="production/range_break_v15"),
    )

    assert job_id == "job-auto"
    _, kwargs = manager.create_job.call_args
    run_spec = kwargs["run_spec"]
    assert run_spec.parameters["markets"] == "prime,standard"
    assert service.get_job_request("job-auto") == ScreeningJobRequest(
        markets="prime,standard",
        strategies="production/range_break_v15",
    )
    manager.set_job_task.assert_awaited_once_with("job-auto", fake_task)

    service._executor.shutdown(wait=True)  # noqa: SLF001


@pytest.mark.asyncio
async def test_submit_screening_propagates_default_market_resolution_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = MagicMock()
    manager.create_job.return_value = "job-error"
    manager.set_job_task = AsyncMock()
    service = ScreeningJobService(manager=manager, max_workers=1)

    monkeypatch.setattr(
        "src.application.services.screening_job_service.resolve_default_screening_markets",
        lambda **_kwargs: (_ for _ in ()).throw(
            ValueError("Failed to resolve default markets for production/broken")
        ),
    )

    with pytest.raises(ValueError, match="production/broken"):
        await service.submit_screening(reader=MagicMock(), request=ScreeningJobRequest())

    manager.create_job.assert_not_called()
    service._executor.shutdown(wait=True)  # noqa: SLF001


@pytest.mark.asyncio
async def test_run_job_failure_marks_failed_and_releases_slot(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = MagicMock()
    manager.acquire_slot = AsyncMock(return_value=None)
    manager.update_job_status = AsyncMock()
    manager.set_job_raw_result = AsyncMock()
    manager.release_slot = MagicMock()

    class _FailingScreeningService:
        def __init__(self, reader: object) -> None:
            self._reader = reader

        def run_screening(self, **kwargs: object) -> object:
            kwargs["progress_callback"](0, 0)
            raise RuntimeError("screening failed")

    monkeypatch.setattr(
        "src.application.services.screening_job_service.ScreeningService",
        _FailingScreeningService,
    )

    service = ScreeningJobService(manager=manager, max_workers=1)
    request = ScreeningJobRequest(markets="prime")

    try:
        await service._run_job("job-3", reader=MagicMock(), request=request)  # noqa: SLF001
    finally:
        service._executor.shutdown(wait=True)  # noqa: SLF001

    manager.release_slot.assert_called_once()
    manager.update_job_status.assert_any_await(
        "job-3",
        JobStatus.FAILED,
        message="Screening ジョブに失敗しました",
        error="screening failed",
    )


@pytest.mark.asyncio
async def test_shutdown_skips_executor_when_already_shutdown() -> None:
    manager = MagicMock()
    service = ScreeningJobService(manager=manager, max_workers=1)
    service._executor.shutdown(wait=True)  # noqa: SLF001

    await service.shutdown()

    manager.list_jobs.assert_not_called()
