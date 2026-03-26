"""OptimizationService unit tests."""

import asyncio
import sys
from types import SimpleNamespace
from typing import cast

import pytest

from src.entrypoints.http.schemas.backtest import JobStatus
from src.application.services.job_manager import JobManager
from src.application.services.optimization_service import OptimizationService
from src.domains.backtest.contracts import EnginePolicyMode, RunType


def _manager(**kwargs: object) -> JobManager:
    return cast(JobManager, SimpleNamespace(**kwargs))


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
                html_path="/tmp/result.html",
            )

    monkeypatch.setattr("src.domains.optimization.engine.ParameterOptimizationEngine", _FakeEngine)

    payload = service._execute_optimization_sync("demo")

    assert payload["best_score"] == 1.0
    assert payload["best_params"] == {"period": 20}
    assert payload["worst_score"] == 0.2
    assert payload["worst_params"] == {"period": 5}
    assert payload["total_combinations"] == 2
    assert payload["html_path"] == "/tmp/result.html"


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
                html_path="",
            )

    monkeypatch.setattr("src.domains.optimization.engine.ParameterOptimizationEngine", _FakeEngine)

    payload = service._execute_optimization_sync("demo")

    assert payload["best_score"] == 0.0
    assert payload["best_params"] == {}
    assert payload["worst_score"] is None
    assert payload["worst_params"] is None
    assert payload["total_combinations"] == 0
    assert payload["html_path"] is None


@pytest.mark.asyncio
async def test_submit_optimization_creates_task_and_returns_job_id(monkeypatch):
    captured: dict[str, object] = {}

    async def _dummy_run_optimization(*args, **kwargs):  # noqa: ANN002
        _ = (args, kwargs)
        return None

    manager = _manager(
        create_job=lambda _strategy_name, job_type="optimization", run_spec=None: (
            captured.update({"job_type": job_type, "run_spec": run_spec}) or "opt-job-1"
        ),
    )
    service = OptimizationService(manager=manager)
    monkeypatch.setattr(service, "_validate_grid_ready", lambda _strategy_name: None)
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
    run_spec = captured["run_spec"]
    assert run_spec is not None
    assert run_spec.run_type == RunType.OPTIMIZATION
    assert run_spec.parameters == {
        "optimization_mode": "grid_search",
        "engine_policy": {
            "mode": EnginePolicyMode.FAST_ONLY.value,
            "verification_top_k": None,
        },
    }


@pytest.mark.asyncio
async def test_submit_optimization_resolves_dataset_from_base_strategy(monkeypatch):
    captured: dict[str, object] = {}

    async def _dummy_run_optimization(*args, **kwargs):  # noqa: ANN002
        _ = (args, kwargs)
        return None

    manager = _manager(
        create_job=lambda _strategy_name, job_type="optimization", run_spec=None: (
            captured.update({"job_type": job_type, "run_spec": run_spec}) or "opt-job-2"
        ),
    )
    service = OptimizationService(manager=manager)
    monkeypatch.setattr(service, "_validate_grid_ready", lambda _strategy_name: None)
    monkeypatch.setattr(service, "_run_optimization", _dummy_run_optimization)
    monkeypatch.setattr(
        service._config_loader,
        "load_strategy_config",
        lambda strategy_name: {"shared_config": {"dataset": "primeExTopix500"}}
        if strategy_name == "strategy-y"
        else {},
    )
    monkeypatch.setattr(
        service._config_loader,
        "merge_shared_config",
        lambda _strategy_config: {"dataset": "primeExTopix500"},
    )

    async def _set_job_task(job_id: str, task):
        captured["job_id"] = job_id
        await task

    manager.set_job_task = _set_job_task

    job_id = await service.submit_optimization("strategy-y")

    assert job_id == "opt-job-2"
    run_spec = captured["run_spec"]
    assert run_spec is not None
    assert run_spec.dataset_name == "primeExTopix500"
    assert run_spec.dataset_snapshot_id == "primeExTopix500"
    service._executor.shutdown(wait=False)


@pytest.mark.asyncio
async def test_submit_optimization_rejects_invalid_grid(monkeypatch):
    manager = _manager(create_job=lambda *_args, **_kwargs: "should-not-be-created")
    service = OptimizationService(manager=manager)
    monkeypatch.setattr(
        service,
        "_validate_grid_ready",
        lambda _strategy_name: (_ for _ in ()).throw(ValueError("grid invalid")),
    )

    with pytest.raises(ValueError, match="grid invalid"):
        await service.submit_optimization("strategy-bad")


@pytest.mark.asyncio
async def test_run_optimization_success_sets_job_fields(monkeypatch):
    statuses: list[str] = []
    events: list[str] = []

    async def _acquire_slot():
        events.append("acquire")

    async def _update_job_status(job_id: str, status, **kwargs):  # noqa: ANN001
        _ = (job_id, kwargs)
        statuses.append(status.value)

    manager = _manager(
        acquire_slot=_acquire_slot,
        release_slot=lambda: events.append("release"),
        update_job_status=_update_job_status,
        reload_job_from_storage=lambda job_id, notify=False: asyncio.sleep(
            0,
            result=SimpleNamespace(
                job_id=job_id,
                status=JobStatus.COMPLETED,
                progress=1.0,
                message="最適化完了",
                best_score=0.9,
                best_params={"period": 20},
                worst_score=0.1,
                worst_params={"period": 5},
                total_combinations=9,
                html_path="/tmp/result.html",
            ),
        ),
        is_cancel_requested=lambda _job_id: False,
    )

    service = OptimizationService(manager=manager)
    monkeypatch.setattr(
        service,
        "_start_worker_process",
        lambda job_id, strategy_name: asyncio.sleep(
            0,
            result=SimpleNamespace(returncode=0, wait=lambda: asyncio.sleep(0, result=0)),
        ),
    )
    monkeypatch.setattr(
        service,
        "_wait_for_worker_completion",
        lambda job_id, process: asyncio.sleep(0, result=0),
    )
    await service._run_optimization("job-1", "strategy-1")

    assert events == ["acquire", "release"]
    assert statuses == ["pending"]


@pytest.mark.asyncio
async def test_run_optimization_success_without_job_object(monkeypatch):
    statuses: list[str] = []

    async def _acquire_slot():
        return None

    async def _update_job_status(job_id: str, status, **kwargs):  # noqa: ANN001
        _ = (job_id, kwargs)
        statuses.append(status.value)

    manager = _manager(
        acquire_slot=_acquire_slot,
        release_slot=lambda: None,
        update_job_status=_update_job_status,
        reload_job_from_storage=lambda job_id, notify=False: asyncio.sleep(0, result=None),
        is_cancel_requested=lambda _job_id: False,
    )

    service = OptimizationService(manager=manager)
    monkeypatch.setattr(
        service,
        "_start_worker_process",
        lambda job_id, strategy_name: asyncio.sleep(
            0,
            result=SimpleNamespace(returncode=0, wait=lambda: asyncio.sleep(0, result=0)),
        ),
    )
    monkeypatch.setattr(
        service,
        "_wait_for_worker_completion",
        lambda job_id, process: asyncio.sleep(0, result=0),
    )
    await service._run_optimization("job-1", "strategy-1")

    assert statuses == ["pending"]


@pytest.mark.asyncio
async def test_run_optimization_handles_cancelled(monkeypatch):
    statuses: list[str] = []
    events: list[str] = []

    async def _acquire_slot():
        events.append("acquire")

    async def _update_job_status(job_id: str, status, **kwargs):  # noqa: ANN001
        _ = (job_id, kwargs)
        statuses.append(status.value)

    manager = _manager(
        acquire_slot=_acquire_slot,
        release_slot=lambda: events.append("release"),
        update_job_status=_update_job_status,
        reload_job_from_storage=lambda job_id, notify=False: asyncio.sleep(
            0,
            result=SimpleNamespace(
                job_id=job_id,
                status=JobStatus.CANCELLED if notify else JobStatus.RUNNING,
                progress=1.0 if notify else 0.0,
                message="cancelled" if notify else "running",
            ),
        ),
        is_cancel_requested=lambda _job_id: True,
    )

    service = OptimizationService(manager=manager)
    process = SimpleNamespace(returncode=None)

    async def _start_worker_process(job_id: str, strategy_name: str):
        _ = (job_id, strategy_name)
        return process

    async def _wait_for_worker_completion(job_id: str, process_obj):
        _ = (job_id, process_obj)
        raise asyncio.CancelledError()

    async def _terminate_worker_process(process_obj, *, timeout_seconds=3.0):
        _ = timeout_seconds
        process_obj.returncode = -15
        events.append("terminate")

    monkeypatch.setattr(service, "_start_worker_process", _start_worker_process)
    monkeypatch.setattr(service, "_wait_for_worker_completion", _wait_for_worker_completion)
    monkeypatch.setattr(service, "_terminate_worker_process", _terminate_worker_process)
    await service._run_optimization("job-2", "strategy-2")

    assert events == ["acquire", "terminate", "release"]
    assert statuses == ["pending"]


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

    manager = _manager(
        acquire_slot=_acquire_slot,
        release_slot=lambda: events.append("release"),
        update_job_status=_update_job_status,
        reload_job_from_storage=lambda job_id, notify=False: asyncio.sleep(
            0,
            result=SimpleNamespace(
                job_id=job_id,
                status=JobStatus.RUNNING,
                progress=0.5,
                message="running",
            ),
        ),
        is_cancel_requested=lambda _job_id: False,
    )

    service = OptimizationService(manager=manager)
    monkeypatch.setattr(
        service,
        "_start_worker_process",
        lambda job_id, strategy_name: asyncio.sleep(
            0,
            result=SimpleNamespace(returncode=7, wait=lambda: asyncio.sleep(0, result=7)),
        ),
    )
    monkeypatch.setattr(
        service,
        "_wait_for_worker_completion",
        lambda job_id, process: asyncio.sleep(0, result=7),
    )
    await service._run_optimization("job-3", "strategy-3")

    assert events == ["acquire", "release"]
    assert statuses == ["pending", "failed"]
    assert errors[-1] == "worker_exit_code=7"


@pytest.mark.asyncio
async def test_run_optimization_marks_failed_when_worker_exits_without_terminal_state(monkeypatch):
    statuses: list[tuple[str, str | None]] = []

    async def _acquire_slot():
        return None

    async def _update_job_status(job_id: str, status, **kwargs):  # noqa: ANN001
        _ = job_id
        statuses.append((status.value, kwargs.get("error")))

    manager = _manager(
        acquire_slot=_acquire_slot,
        release_slot=lambda: None,
        update_job_status=_update_job_status,
        reload_job_from_storage=lambda job_id, notify=False: asyncio.sleep(
            0,
            result=SimpleNamespace(job_id=job_id, status=JobStatus.RUNNING, progress=0.2, message="running"),
        ),
        is_cancel_requested=lambda _job_id: False,
    )

    service = OptimizationService(manager=manager)
    monkeypatch.setattr(
        service,
        "_start_worker_process",
        lambda job_id, strategy_name: asyncio.sleep(
            0,
            result=SimpleNamespace(returncode=0, wait=lambda: asyncio.sleep(0, result=0)),
        ),
    )
    monkeypatch.setattr(
        service,
        "_wait_for_worker_completion",
        lambda job_id, process: asyncio.sleep(0, result=0),
    )

    await service._run_optimization("job-no-terminal", "strategy")

    assert statuses == [
        ("pending", None),
        ("failed", "worker_exited_without_terminal_state"),
    ]


@pytest.mark.asyncio
async def test_run_optimization_cancelled_without_cancel_request_updates_cancelled(monkeypatch):
    statuses: list[str] = []
    process = SimpleNamespace(returncode=None)

    async def _acquire_slot():
        return None

    async def _update_job_status(job_id: str, status, **kwargs):  # noqa: ANN001
        _ = (job_id, kwargs)
        statuses.append(status.value)

    manager = _manager(
        acquire_slot=_acquire_slot,
        release_slot=lambda: None,
        update_job_status=_update_job_status,
        reload_job_from_storage=lambda job_id, notify=False: asyncio.sleep(
            0,
            result=SimpleNamespace(job_id=job_id, status=JobStatus.RUNNING, progress=0.2, message="running"),
        ),
        is_cancel_requested=lambda _job_id: False,
    )

    service = OptimizationService(manager=manager)
    monkeypatch.setattr(
        service,
        "_start_worker_process",
        lambda job_id, strategy_name: asyncio.sleep(0, result=process),
    )

    async def _wait_for_worker_completion(job_id: str, process_obj):
        _ = (job_id, process_obj)
        raise asyncio.CancelledError()

    async def _terminate_worker_process(process_obj, *, timeout_seconds=3.0):
        _ = timeout_seconds
        process_obj.returncode = -15

    monkeypatch.setattr(service, "_wait_for_worker_completion", _wait_for_worker_completion)
    monkeypatch.setattr(service, "_terminate_worker_process", _terminate_worker_process)

    await service._run_optimization("job-cancelled", "strategy")

    assert statuses == ["pending", "cancelled"]


@pytest.mark.asyncio
async def test_run_optimization_handles_start_worker_process_exception(monkeypatch):
    statuses: list[tuple[str, str | None]] = []

    async def _acquire_slot():
        return None

    async def _update_job_status(job_id: str, status, **kwargs):  # noqa: ANN001
        _ = job_id
        statuses.append((status.value, kwargs.get("error")))

    manager = _manager(
        acquire_slot=_acquire_slot,
        release_slot=lambda: None,
        update_job_status=_update_job_status,
    )

    service = OptimizationService(manager=manager)

    async def _start_worker_process(job_id: str, strategy_name: str):
        _ = (job_id, strategy_name)
        raise RuntimeError("spawn failed")

    monkeypatch.setattr(service, "_start_worker_process", _start_worker_process)

    await service._run_optimization("job-error", "strategy")

    assert statuses == [("pending", None), ("failed", "spawn failed")]


@pytest.mark.asyncio
async def test_wait_for_worker_completion_reloads_until_process_exits(monkeypatch):
    manager = _manager(reload_job_from_storage=pytest.fail)
    service = OptimizationService(manager=manager, worker_poll_interval_seconds=0.01)
    reload_calls: list[tuple[str, bool]] = []

    async def _reload_job_from_storage(job_id: str, notify: bool = False):
        reload_calls.append((job_id, notify))
        return None

    manager.reload_job_from_storage = _reload_job_from_storage

    class _FakeProcess:
        async def wait(self) -> int:
            return 0

    original_wait_for = asyncio.wait_for
    call_count = {"count": 0}

    async def _wait_for(awaitable, timeout):  # noqa: ANN001
        _ = timeout
        call_count["count"] += 1
        if call_count["count"] == 1:
            raise asyncio.TimeoutError()
        return await original_wait_for(awaitable, timeout=1.0)

    monkeypatch.setattr(asyncio, "wait_for", _wait_for)

    exit_code = await service._wait_for_worker_completion(
        "job-1",
        cast(asyncio.subprocess.Process, _FakeProcess()),
    )

    assert exit_code == 0
    assert reload_calls == [("job-1", True), ("job-1", True)]


@pytest.mark.asyncio
async def test_terminate_worker_process_kills_when_terminate_times_out(monkeypatch):
    service = OptimizationService()
    events: list[str] = []

    class _FakeProcess:
        returncode = None

        def terminate(self) -> None:
            events.append("terminate")

        def kill(self) -> None:
            events.append("kill")

        async def wait(self) -> int:
            events.append("wait")
            return 0

    original_wait_for = asyncio.wait_for
    call_count = {"count": 0}

    async def _wait_for(awaitable, timeout):  # noqa: ANN001
        call_count["count"] += 1
        if call_count["count"] == 1:
            raise asyncio.TimeoutError()
        return await original_wait_for(awaitable, timeout=timeout)

    monkeypatch.setattr(asyncio, "wait_for", _wait_for)

    await service._terminate_worker_process(
        cast(asyncio.subprocess.Process, _FakeProcess())
    )

    assert events == ["terminate", "kill", "wait"]


@pytest.mark.asyncio
async def test_start_worker_process_invokes_subprocess_exec(monkeypatch):
    service = OptimizationService()
    captured: dict[str, object] = {}

    async def _create_subprocess_exec(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _create_subprocess_exec)

    await service._start_worker_process("job-1", "strategy-1")

    assert captured["args"] == tuple(service._build_worker_command("job-1", "strategy-1"))


def test_build_worker_command() -> None:
    service = OptimizationService(worker_timeout_seconds=1200)

    command = service._build_worker_command("job-1", "strategy-1")

    assert command == [
        sys.executable,
        "-m",
        "src.application.workers.optimization_worker",
        "--job-id",
        "job-1",
        "--strategy-name",
        "strategy-1",
        "--engine-policy-json",
        '{"mode":"fast_only","verification_top_k":null}',
        "--timeout-seconds",
        "1200",
    ]
