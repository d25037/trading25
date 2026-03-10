"""LabService worker watcher tests."""

import asyncio
import json
import sys
from types import SimpleNamespace

import pytest

from src.application.services.lab_service import LabService
from src.entrypoints.http.schemas.backtest import JobStatus


def test_build_worker_command_serializes_payload() -> None:
    service = LabService(worker_timeout_seconds=1500)

    command = service._build_worker_command(
        "job-1",
        {"lab_type": "generate", "count": 3, "allowed_categories": ["fundamental"]},
    )

    assert command[:3] == [sys.executable, "-m", "src.application.workers.lab_worker"]
    assert "--job-id" in command
    assert "--payload-json" in command
    assert command[command.index("--timeout-seconds") + 1] == "1500"
    payload = json.loads(command[command.index("--payload-json") + 1])
    assert payload == {
        "lab_type": "generate",
        "count": 3,
        "allowed_categories": ["fundamental"],
    }


@pytest.mark.asyncio
async def test_run_worker_job_success(monkeypatch: pytest.MonkeyPatch) -> None:
    service = LabService()
    statuses: list[JobStatus] = []
    events: list[str] = []

    async def _acquire_slot() -> None:
        events.append("acquire")

    async def _update_job_status(job_id: str, status: JobStatus, **kwargs) -> None:
        _ = (job_id, kwargs)
        statuses.append(status)

    service._manager.acquire_slot = _acquire_slot  # type: ignore[method-assign]
    service._manager.release_slot = lambda: events.append("release")  # type: ignore[method-assign]
    service._manager.update_job_status = _update_job_status  # type: ignore[method-assign]
    service._manager.reload_job_from_storage = lambda job_id, notify=False: asyncio.sleep(  # type: ignore[method-assign]
        0,
        result=SimpleNamespace(
            job_id=job_id,
            status=JobStatus.COMPLETED,
            progress=1.0,
            message="戦略生成完了",
        ),
    )
    service._manager.is_cancel_requested = lambda job_id: False  # type: ignore[method-assign]
    monkeypatch.setattr(
        service,
        "_start_worker_process",
        lambda job_id, payload: asyncio.sleep(
            0,
            result=SimpleNamespace(returncode=0, wait=lambda: asyncio.sleep(0, result=0)),
        ),
    )
    monkeypatch.setattr(
        service,
        "_wait_for_worker_completion",
        lambda job_id, process: asyncio.sleep(0, result=0),
    )

    await service._run_worker_job("job-1", {"lab_type": "generate"})

    assert events == ["acquire", "release"]
    assert statuses == [JobStatus.PENDING]


@pytest.mark.asyncio
async def test_run_worker_job_cancelled_does_not_terminalize_when_cancel_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = LabService()
    statuses: list[JobStatus] = []
    events: list[str] = []
    process = SimpleNamespace(returncode=None)

    async def _acquire_slot() -> None:
        events.append("acquire")

    async def _update_job_status(job_id: str, status: JobStatus, **kwargs) -> None:
        _ = (job_id, kwargs)
        statuses.append(status)

    service._manager.acquire_slot = _acquire_slot  # type: ignore[method-assign]
    service._manager.release_slot = lambda: events.append("release")  # type: ignore[method-assign]
    service._manager.update_job_status = _update_job_status  # type: ignore[method-assign]
    service._manager.reload_job_from_storage = lambda job_id, notify=False: asyncio.sleep(  # type: ignore[method-assign]
        0,
        result=SimpleNamespace(
            job_id=job_id,
            status=JobStatus.CANCELLED if notify else JobStatus.RUNNING,
            progress=1.0 if notify else 0.0,
            message="cancelled" if notify else "running",
        ),
    )
    service._manager.is_cancel_requested = lambda job_id: True  # type: ignore[method-assign]

    async def _start_worker_process(job_id: str, payload: dict[str, object]) -> object:
        _ = (job_id, payload)
        return process

    async def _wait_for_worker_completion(job_id: str, process_obj: object) -> int:
        _ = (job_id, process_obj)
        raise asyncio.CancelledError()

    async def _terminate_worker_process(process_obj: object, *, timeout_seconds: float = 3.0) -> None:
        _ = timeout_seconds
        assert process_obj is process
        process.returncode = -15
        events.append("terminate")

    monkeypatch.setattr(service, "_start_worker_process", _start_worker_process)
    monkeypatch.setattr(service, "_wait_for_worker_completion", _wait_for_worker_completion)
    monkeypatch.setattr(service, "_terminate_worker_process", _terminate_worker_process)

    await service._run_worker_job("job-1", {"lab_type": "generate"})

    assert events == ["acquire", "terminate", "release"]
    assert statuses == [JobStatus.PENDING]
