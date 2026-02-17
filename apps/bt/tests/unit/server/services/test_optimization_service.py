"""OptimizationService unit tests."""

import asyncio
from types import SimpleNamespace

import pytest

from src.server.services.optimization_service import OptimizationService


def _success_payload() -> dict[str, object]:
    return {
        "best_score": 0.9,
        "best_params": {"period": 20},
        "worst_score": 0.1,
        "worst_params": {"period": 5},
        "total_combinations": 9,
        "notebook_path": "/tmp/result.ipynb",
    }


def test_execute_optimization_sync_extracts_best_and_worst(monkeypatch):
    service = OptimizationService()

    class _FakeEngine:
        def __init__(self, strategy_name: str):
            assert strategy_name == "demo"

        def optimize(self):
            return SimpleNamespace(
                best_score=1.0,
                best_params={"period": 20},
                all_results=[
                    {"score": 1.0, "params": {"period": 20}},
                    {"score": 0.2, "params": {"period": 5}},
                ],
                notebook_path="/tmp/result.ipynb",
            )

    monkeypatch.setattr("src.optimization.engine.ParameterOptimizationEngine", _FakeEngine)

    payload = service._execute_optimization_sync("demo")

    assert payload["best_score"] == 1.0
    assert payload["best_params"] == {"period": 20}
    assert payload["worst_score"] == 0.2
    assert payload["worst_params"] == {"period": 5}
    assert payload["total_combinations"] == 2
    assert payload["notebook_path"] == "/tmp/result.ipynb"


def test_execute_optimization_sync_handles_empty_all_results(monkeypatch):
    service = OptimizationService()

    class _FakeEngine:
        def __init__(self, strategy_name: str):
            assert strategy_name == "demo"

        def optimize(self):
            return SimpleNamespace(
                best_score=0.0,
                best_params={},
                all_results=[],
                notebook_path="",
            )

    monkeypatch.setattr("src.optimization.engine.ParameterOptimizationEngine", _FakeEngine)

    payload = service._execute_optimization_sync("demo")

    assert payload["best_score"] == 0.0
    assert payload["best_params"] == {}
    assert payload["worst_score"] is None
    assert payload["worst_params"] is None
    assert payload["total_combinations"] == 0
    assert payload["notebook_path"] is None


@pytest.mark.asyncio
async def test_submit_optimization_creates_task_and_returns_job_id(monkeypatch):
    captured: dict[str, object] = {}

    async def _dummy_run_optimization(*args, **kwargs):  # noqa: ANN002
        _ = (args, kwargs)
        return None

    manager = SimpleNamespace(
        create_job=lambda _strategy_name, job_type="optimization": "opt-job-1",
    )
    service = OptimizationService(manager=manager)
    monkeypatch.setattr(service, "_run_optimization", _dummy_run_optimization)

    async def _set_job_task(job_id: str, task):
        captured["job_id"] = job_id
        captured["task"] = task
        await task

    manager.set_job_task = _set_job_task

    job_id = await service.submit_optimization("strategy-x")

    assert job_id == "opt-job-1"
    assert captured["job_id"] == "opt-job-1"
    assert captured["task"] is not None


@pytest.mark.asyncio
async def test_run_optimization_success_sets_job_fields(monkeypatch):
    job = SimpleNamespace(
        best_score=None,
        best_params=None,
        worst_score=None,
        worst_params=None,
        total_combinations=None,
        notebook_path=None,
    )
    statuses: list[str] = []
    events: list[str] = []

    async def _acquire_slot():
        events.append("acquire")

    async def _update_job_status(job_id: str, status, **kwargs):  # noqa: ANN001
        _ = (job_id, kwargs)
        statuses.append(status.value)

    manager = SimpleNamespace(
        acquire_slot=_acquire_slot,
        release_slot=lambda: events.append("release"),
        update_job_status=_update_job_status,
        get_job=lambda _job_id: job,
    )

    class _SuccessLoop:
        def run_in_executor(self, executor, fn, *args):  # noqa: ANN001
            _ = (executor, fn, args)
            fut = asyncio.Future()
            fut.set_result(_success_payload())
            return fut

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: _SuccessLoop())

    service = OptimizationService(manager=manager)
    await service._run_optimization("job-1", "strategy-1")

    assert events == ["acquire", "release"]
    assert statuses == ["running", "completed"]
    assert job.best_score == 0.9
    assert job.best_params == {"period": 20}
    assert job.worst_score == 0.1
    assert job.worst_params == {"period": 5}
    assert job.total_combinations == 9
    assert job.notebook_path == "/tmp/result.ipynb"


@pytest.mark.asyncio
async def test_run_optimization_success_without_job_object(monkeypatch):
    statuses: list[str] = []

    async def _acquire_slot():
        return None

    async def _update_job_status(job_id: str, status, **kwargs):  # noqa: ANN001
        _ = (job_id, kwargs)
        statuses.append(status.value)

    manager = SimpleNamespace(
        acquire_slot=_acquire_slot,
        release_slot=lambda: None,
        update_job_status=_update_job_status,
        get_job=lambda _job_id: None,
    )

    class _SuccessLoop:
        def run_in_executor(self, executor, fn, *args):  # noqa: ANN001
            _ = (executor, fn, args)
            fut = asyncio.Future()
            fut.set_result(_success_payload())
            return fut

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: _SuccessLoop())

    service = OptimizationService(manager=manager)
    await service._run_optimization("job-1", "strategy-1")

    assert statuses == ["running", "completed"]


@pytest.mark.asyncio
async def test_run_optimization_handles_cancelled(monkeypatch):
    statuses: list[str] = []
    events: list[str] = []

    async def _acquire_slot():
        events.append("acquire")

    async def _update_job_status(job_id: str, status, **kwargs):  # noqa: ANN001
        _ = (job_id, kwargs)
        statuses.append(status.value)

    manager = SimpleNamespace(
        acquire_slot=_acquire_slot,
        release_slot=lambda: events.append("release"),
        update_job_status=_update_job_status,
        get_job=lambda _job_id: None,
    )

    class _CancelledLoop:
        def run_in_executor(self, executor, fn, *args):  # noqa: ANN001
            _ = (executor, fn, args)
            fut = asyncio.Future()
            fut.set_exception(asyncio.CancelledError())
            return fut

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: _CancelledLoop())

    service = OptimizationService(manager=manager)
    await service._run_optimization("job-2", "strategy-2")

    assert events == ["acquire", "release"]
    assert statuses == ["running", "cancelled"]


@pytest.mark.asyncio
async def test_run_optimization_handles_failure(monkeypatch):
    statuses: list[str] = []
    errors: list[str | None] = []
    events: list[str] = []

    async def _acquire_slot():
        events.append("acquire")

    async def _update_job_status(job_id: str, status, **kwargs):  # noqa: ANN001
        _ = job_id
        statuses.append(status.value)
        errors.append(kwargs.get("error"))

    manager = SimpleNamespace(
        acquire_slot=_acquire_slot,
        release_slot=lambda: events.append("release"),
        update_job_status=_update_job_status,
        get_job=lambda _job_id: None,
    )

    class _FailedLoop:
        def run_in_executor(self, executor, fn, *args):  # noqa: ANN001
            _ = (executor, fn, args)
            fut = asyncio.Future()
            fut.set_exception(RuntimeError("boom"))
            return fut

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: _FailedLoop())

    service = OptimizationService(manager=manager)
    await service._run_optimization("job-3", "strategy-3")

    assert events == ["acquire", "release"]
    assert statuses == ["running", "failed"]
    assert errors[-1] == "boom"
