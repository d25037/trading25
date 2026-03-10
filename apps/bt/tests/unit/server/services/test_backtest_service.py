"""
BacktestService unit tests
"""

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.domains.backtest.core.runner import BacktestResult
from src.application.services.backtest_service import BacktestService
from src.domains.backtest.contracts import RunType
from src.entrypoints.http.schemas.backtest import JobStatus


def test_execute_backtest_sync_uses_threadsafe_progress(monkeypatch, tmp_path: Path):
    service = BacktestService()

    async def _dummy_update_job_status(*args, **kwargs):
        return None

    monkeypatch.setattr(service._manager, "update_job_status", _dummy_update_job_status)

    result = BacktestResult(
        html_path=tmp_path / "result.html",
        elapsed_time=1.0,
        summary={},
        strategy_name="test",
        dataset_name="sample",
    )

    def _fake_execute(
        strategy: str,
        progress_callback=None,
        config_override=None,
        data_access_mode: str | None = None,
    ):
        _ = (config_override, data_access_mode)
        if progress_callback is not None:
            progress_callback("running", 0.1)
        return result

    monkeypatch.setattr(service._runner, "execute", _fake_execute)

    called = {}

    def _fake_run_coroutine_threadsafe(coro, loop):
        called["loop"] = loop
        called["coro"] = coro
        return object()

    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", _fake_run_coroutine_threadsafe)

    loop = asyncio.new_event_loop()
    try:
        service._execute_backtest_sync("job-1", "strategy-1", loop)
    finally:
        loop.close()

    assert called["loop"] is loop


def test_execute_backtest_sync_uses_runner_default_data_access_mode(monkeypatch, tmp_path: Path):
    service = BacktestService()

    result = BacktestResult(
        html_path=tmp_path / "result.html",
        elapsed_time=1.0,
        summary={},
        strategy_name="test",
        dataset_name="sample",
    )

    captured: dict[str, object] = {}

    def _fake_execute(**kwargs):
        captured.update(kwargs)
        return result

    monkeypatch.setattr(service._runner, "execute", _fake_execute)

    loop = asyncio.new_event_loop()
    try:
        service._execute_backtest_sync("job-1", "strategy-1", loop)
    finally:
        loop.close()

    assert "data_access_mode" not in captured


@pytest.mark.asyncio
async def test_submit_backtest_creates_task_and_returns_job_id(monkeypatch):
    service = BacktestService()
    captured: dict[str, object] = {}

    async def _dummy_run_backtest(*args, **kwargs):  # noqa: ANN002
        _ = (args, kwargs)
        return None

    monkeypatch.setattr(service, "_run_backtest", _dummy_run_backtest)
    monkeypatch.setattr(
        service._manager,
        "create_job",
        lambda _strategy_name, job_type="backtest", run_spec=None: (
            captured.update({"job_type": job_type, "run_spec": run_spec}) or "job-123"
        ),
    )

    async def _set_job_task(job_id: str, task):
        captured["job_id"] = job_id
        captured["task"] = task
        await task

    monkeypatch.setattr(service._manager, "set_job_task", _set_job_task)

    job_id = await service.submit_backtest(
        "strategy",
        config_override={"shared_config": {"dataset": "sample-dataset"}},
    )

    assert job_id == "job-123"
    assert captured["job_id"] == "job-123"
    run_spec = captured["run_spec"]
    assert run_spec is not None
    assert run_spec.run_type == RunType.BACKTEST
    assert run_spec.dataset_name == "sample-dataset"


@pytest.mark.asyncio
async def test_submit_backtest_resolves_dataset_from_base_strategy_when_override_missing(monkeypatch):
    service = BacktestService()
    captured: dict[str, object] = {}

    async def _dummy_run_backtest(*args, **kwargs):  # noqa: ANN002
        _ = (args, kwargs)
        return None

    monkeypatch.setattr(service, "_run_backtest", _dummy_run_backtest)
    monkeypatch.setattr(
        service._runner.config_loader,
        "load_strategy_config",
        lambda strategy_name: {"shared_config": {"dataset": "primeExTopix500"}}
        if strategy_name == "strategy"
        else {},
    )
    monkeypatch.setattr(
        service._runner.config_loader,
        "merge_shared_config",
        lambda _strategy_config: {"dataset": "primeExTopix500"},
    )
    monkeypatch.setattr(
        service._manager,
        "create_job",
        lambda _strategy_name, job_type="backtest", run_spec=None: (
            captured.update({"job_type": job_type, "run_spec": run_spec}) or "job-456"
        ),
    )

    async def _set_job_task(job_id: str, task):
        captured["job_id"] = job_id
        await task

    monkeypatch.setattr(service._manager, "set_job_task", _set_job_task)

    job_id = await service.submit_backtest("strategy")

    assert job_id == "job-456"
    run_spec = captured["run_spec"]
    assert run_spec is not None
    assert run_spec.dataset_name == "primeExTopix500"
    assert run_spec.dataset_snapshot_id == "primeExTopix500"


@pytest.mark.asyncio
async def test_submit_backtest_normalizes_blank_dataset_override_before_execution(monkeypatch):
    service = BacktestService()
    captured: dict[str, object] = {}

    async def _dummy_run_backtest(job_id: str, strategy_name: str, config_override=None):
        captured["run_args"] = (job_id, strategy_name, config_override)
        return None

    monkeypatch.setattr(service, "_run_backtest", _dummy_run_backtest)
    monkeypatch.setattr(
        service._runner.config_loader,
        "load_strategy_config",
        lambda strategy_name: {"shared_config": {"dataset": "primeExTopix500"}}
        if strategy_name == "strategy"
        else {},
    )
    monkeypatch.setattr(
        service._runner.config_loader,
        "merge_shared_config",
        lambda _strategy_config: {"dataset": "primeExTopix500", "direction": "longonly"},
    )
    monkeypatch.setattr(
        service._manager,
        "create_job",
        lambda _strategy_name, job_type="backtest", run_spec=None: (
            captured.update({"job_type": job_type, "run_spec": run_spec}) or "job-789"
        ),
    )

    async def _set_job_task(job_id: str, task):
        captured["job_id"] = job_id
        await task

    monkeypatch.setattr(service._manager, "set_job_task", _set_job_task)

    job_id = await service.submit_backtest(
        "strategy",
        config_override={"shared_config": {"dataset": "   ", "direction": "shortonly"}},
    )

    assert job_id == "job-789"
    run_spec = captured["run_spec"]
    assert run_spec is not None
    assert run_spec.dataset_name == "primeExTopix500"
    _, _, forwarded_override = captured["run_args"]
    assert forwarded_override == {"shared_config": {"direction": "shortonly"}}


@pytest.mark.asyncio
async def test_run_backtest_success_updates_result_and_status(monkeypatch, tmp_path: Path):
    service = BacktestService()
    events: list[tuple[str, object]] = []

    async def _acquire_slot():
        events.append(("acquire", None))

    async def _update_job_status(job_id: str, status, **kwargs):
        events.append(("status", (job_id, status, kwargs)))

    monkeypatch.setattr(service._manager, "acquire_slot", _acquire_slot)
    monkeypatch.setattr(service._manager, "update_job_status", _update_job_status)
    monkeypatch.setattr(service._manager, "release_slot", lambda: events.append(("release", None)))
    monkeypatch.setattr(
        service,
        "_start_worker_process",
        lambda job_id, strategy_name, config_override=None: asyncio.sleep(
            0,
            result=SimpleNamespace(returncode=0, wait=lambda: asyncio.sleep(0, result=0)),
        ),
    )
    monkeypatch.setattr(
        service,
        "_wait_for_worker_completion",
        lambda job_id, process: asyncio.sleep(0, result=0),
    )
    monkeypatch.setattr(
        service._manager,
        "reload_job_from_storage",
        lambda job_id, notify=False: asyncio.sleep(
            0,
            result=SimpleNamespace(
                job_id=job_id,
                status=JobStatus.COMPLETED,
                progress=1.0,
                message="バックテスト完了",
            ),
        ),
    )

    await service._run_backtest("job-1", "strategy-1")

    assert ("acquire", None) in events
    assert any(
        name == "status"
        and payload[1] == JobStatus.PENDING
        for name, payload in events
    )
    assert any(name == "release" for name, _ in events)


@pytest.mark.asyncio
async def test_run_backtest_handles_cancelled_and_failure(monkeypatch):
    service = BacktestService()
    events: list[tuple[str, object]] = []
    status_values: list[str] = []
    process = SimpleNamespace(returncode=None)

    async def _acquire_slot():
        events.append(("acquire", None))

    async def _update_job_status(job_id: str, status, **kwargs):
        events.append(("status", (job_id, status, kwargs)))
        status_values.append(status.value)

    monkeypatch.setattr(service._manager, "acquire_slot", _acquire_slot)
    monkeypatch.setattr(service._manager, "update_job_status", _update_job_status)
    monkeypatch.setattr(service._manager, "release_slot", lambda: events.append(("release", None)))
    monkeypatch.setattr(
        service._manager,
        "reload_job_from_storage",
        lambda job_id, notify=False: asyncio.sleep(
            0,
            result=SimpleNamespace(
                job_id=job_id,
                status=JobStatus.CANCELLED if notify else JobStatus.RUNNING,
                progress=1.0 if notify else 0.0,
                message="cancelled" if notify else "running",
            ),
        ),
    )
    monkeypatch.setattr(service._manager, "is_cancel_requested", lambda job_id: True)

    async def _cancelled_start_worker_process(job_id: str, strategy_name: str, config_override=None):
        _ = (job_id, strategy_name, config_override)
        return process

    async def _cancelled_wait_for_worker_completion(job_id: str, process_obj):
        _ = (job_id, process_obj)
        raise asyncio.CancelledError()

    async def _terminate_worker_process(process_obj, *, timeout_seconds=3.0):
        _ = timeout_seconds
        process_obj.returncode = -15
        events.append(("terminate", process_obj))

    monkeypatch.setattr(service, "_start_worker_process", _cancelled_start_worker_process)
    monkeypatch.setattr(service, "_wait_for_worker_completion", _cancelled_wait_for_worker_completion)
    monkeypatch.setattr(service, "_terminate_worker_process", _terminate_worker_process)

    await service._run_backtest("job-cancel", "strategy")
    assert "cancelled" not in status_values
    assert any(name == "terminate" for name, _ in events)

    events.clear()
    status_values.clear()
    monkeypatch.setattr(
        service,
        "_start_worker_process",
        lambda job_id, strategy_name, config_override=None: asyncio.sleep(
            0,
            result=SimpleNamespace(returncode=2, wait=lambda: asyncio.sleep(0, result=2)),
        ),
    )
    monkeypatch.setattr(
        service,
        "_wait_for_worker_completion",
        lambda job_id, process_obj: asyncio.sleep(0, result=2),
    )
    monkeypatch.setattr(
        service._manager,
        "reload_job_from_storage",
        lambda job_id, notify=False: asyncio.sleep(
            0,
            result=SimpleNamespace(
                job_id=job_id,
                status=JobStatus.RUNNING,
                progress=0.5,
                message="running",
            ),
        ),
    )
    monkeypatch.setattr(service._manager, "is_cancel_requested", lambda job_id: False)
    await service._run_backtest("job-fail", "strategy")
    assert "failed" in status_values


@pytest.mark.asyncio
async def test_run_backtest_marks_failed_when_worker_exits_without_terminal_state(monkeypatch):
    service = BacktestService()
    statuses: list[tuple[str, str | None]] = []

    monkeypatch.setattr(service._manager, "acquire_slot", lambda: asyncio.sleep(0))
    monkeypatch.setattr(service._manager, "release_slot", lambda: None)

    async def _update_job_status(job_id: str, status, **kwargs):
        _ = job_id
        statuses.append((status.value, kwargs.get("error")))

    monkeypatch.setattr(service._manager, "update_job_status", _update_job_status)
    monkeypatch.setattr(
        service,
        "_start_worker_process",
        lambda job_id, strategy_name, config_override=None: asyncio.sleep(
            0,
            result=SimpleNamespace(returncode=0, wait=lambda: asyncio.sleep(0, result=0)),
        ),
    )
    monkeypatch.setattr(
        service,
        "_wait_for_worker_completion",
        lambda job_id, process_obj: asyncio.sleep(0, result=0),
    )
    monkeypatch.setattr(
        service._manager,
        "reload_job_from_storage",
        lambda job_id, notify=False: asyncio.sleep(
            0,
            result=SimpleNamespace(job_id=job_id, status=JobStatus.RUNNING, progress=0.2, message="running"),
        ),
    )
    monkeypatch.setattr(service._manager, "is_cancel_requested", lambda job_id: False)

    await service._run_backtest("job-no-terminal", "strategy")

    assert statuses == [
        ("pending", None),
        ("failed", "worker_exited_without_terminal_state"),
    ]


@pytest.mark.asyncio
async def test_run_backtest_cancelled_without_cancel_request_updates_cancelled(monkeypatch):
    service = BacktestService()
    statuses: list[str] = []
    process = SimpleNamespace(returncode=None)

    monkeypatch.setattr(service._manager, "acquire_slot", lambda: asyncio.sleep(0))
    monkeypatch.setattr(service._manager, "release_slot", lambda: None)

    async def _update_job_status(job_id: str, status, **kwargs):
        _ = (job_id, kwargs)
        statuses.append(status.value)

    monkeypatch.setattr(service._manager, "update_job_status", _update_job_status)
    monkeypatch.setattr(
        service._manager,
        "reload_job_from_storage",
        lambda job_id, notify=False: asyncio.sleep(
            0,
            result=SimpleNamespace(job_id=job_id, status=JobStatus.RUNNING, progress=0.2, message="running"),
        ),
    )
    monkeypatch.setattr(service._manager, "is_cancel_requested", lambda job_id: False)
    monkeypatch.setattr(
        service,
        "_start_worker_process",
        lambda job_id, strategy_name, config_override=None: asyncio.sleep(0, result=process),
    )

    async def _wait_for_worker_completion(job_id: str, process_obj):
        _ = (job_id, process_obj)
        raise asyncio.CancelledError()

    async def _terminate_worker_process(process_obj, *, timeout_seconds=3.0):
        _ = timeout_seconds
        process_obj.returncode = -15

    monkeypatch.setattr(service, "_wait_for_worker_completion", _wait_for_worker_completion)
    monkeypatch.setattr(service, "_terminate_worker_process", _terminate_worker_process)

    await service._run_backtest("job-cancelled", "strategy")

    assert statuses == ["pending", "cancelled"]


@pytest.mark.asyncio
async def test_run_backtest_handles_start_worker_process_exception(monkeypatch):
    service = BacktestService()
    statuses: list[tuple[str, str | None]] = []

    monkeypatch.setattr(service._manager, "acquire_slot", lambda: asyncio.sleep(0))
    monkeypatch.setattr(service._manager, "release_slot", lambda: None)

    async def _update_job_status(job_id: str, status, **kwargs):
        _ = job_id
        statuses.append((status.value, kwargs.get("error")))

    monkeypatch.setattr(service._manager, "update_job_status", _update_job_status)

    async def _start_worker_process(job_id: str, strategy_name: str, config_override=None):
        _ = (job_id, strategy_name, config_override)
        raise RuntimeError("spawn failed")

    monkeypatch.setattr(service, "_start_worker_process", _start_worker_process)

    await service._run_backtest("job-error", "strategy")

    assert statuses == [("pending", None), ("failed", "spawn failed")]


@pytest.mark.asyncio
async def test_wait_for_worker_completion_reloads_until_process_exits(monkeypatch):
    service = BacktestService(worker_poll_interval_seconds=0.01)
    reload_calls: list[tuple[str, bool]] = []

    async def _reload_job_from_storage(job_id: str, notify: bool = False):
        reload_calls.append((job_id, notify))
        return None

    monkeypatch.setattr(service._manager, "reload_job_from_storage", _reload_job_from_storage)

    class _FakeProcess:
        def __init__(self) -> None:
            self.calls = 0

        async def wait(self) -> int:
            self.calls += 1
            return 0

    process = _FakeProcess()
    original_wait_for = asyncio.wait_for
    call_count = {"count": 0}

    async def _wait_for(awaitable, timeout):  # noqa: ANN001
        _ = timeout
        call_count["count"] += 1
        if call_count["count"] == 1:
            raise asyncio.TimeoutError()
        return await original_wait_for(awaitable, timeout=1.0)

    monkeypatch.setattr(asyncio, "wait_for", _wait_for)

    exit_code = await service._wait_for_worker_completion("job-1", process)

    assert exit_code == 0
    assert reload_calls == [("job-1", True), ("job-1", True)]


@pytest.mark.asyncio
async def test_terminate_worker_process_kills_when_terminate_times_out(monkeypatch):
    service = BacktestService()
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

    process = _FakeProcess()
    original_wait_for = asyncio.wait_for
    call_count = {"count": 0}

    async def _wait_for(awaitable, timeout):  # noqa: ANN001
        call_count["count"] += 1
        if call_count["count"] == 1:
            raise asyncio.TimeoutError()
        return await original_wait_for(awaitable, timeout=timeout)

    monkeypatch.setattr(asyncio, "wait_for", _wait_for)

    await service._terminate_worker_process(process)

    assert events == ["terminate", "kill", "wait"]


@pytest.mark.asyncio
async def test_start_worker_process_invokes_subprocess_exec(monkeypatch):
    service = BacktestService()
    captured: dict[str, object] = {}

    async def _create_subprocess_exec(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _create_subprocess_exec)

    await service._start_worker_process("job-1", "strategy-1", {"shared_config": {"dataset": "sample"}})

    assert captured["args"] == tuple(
        service._build_worker_command("job-1", "strategy-1", {"shared_config": {"dataset": "sample"}})
    )


def test_build_worker_command_embeds_config_override():
    service = BacktestService(worker_timeout_seconds=900)

    command = service._build_worker_command(
        "job-1",
        "strategy-1",
        {"shared_config": {"dataset": "sample"}},
    )

    assert command[:3] == [sys.executable, "-m", "src.application.workers.backtest_worker"]
    assert "--job-id" in command
    assert "--strategy-name" in command
    assert command[command.index("--timeout-seconds") + 1] == "900"
    json_arg = command[command.index("--config-override-json") + 1]
    assert json.loads(json_arg) == {"shared_config": {"dataset": "sample"}}


def test_extract_result_summary_prefers_html_metrics(tmp_path: Path):
    service = BacktestService()
    html_path = tmp_path / "result.html"
    html_path.write_text("<html></html>", encoding="utf-8")
    result = BacktestResult(
        html_path=html_path,
        elapsed_time=1.0,
        summary={},
        strategy_name="test",
        dataset_name="sample",
    )

    with patch("src.application.services.backtest_result_summary.extract_metrics_from_html") as mock_extract:
        mock_extract.return_value = type(
            "Metrics",
            (),
            {
                "total_return": 1.0,
                "sharpe_ratio": 2.0,
                "sortino_ratio": 2.5,
                "calmar_ratio": 3.0,
                "max_drawdown": 4.0,
                "win_rate": 5.0,
                "total_trades": 6,
            },
        )()

        summary = service._extract_result_summary(result)
        assert summary.total_return == 1.0
        assert summary.sortino_ratio == 2.5
        assert summary.trade_count == 6


def test_extract_result_summary_fallback_when_metrics_fail(tmp_path: Path):
    service = BacktestService()
    html_path = tmp_path / "result.html"
    html_path.write_text("<html></html>", encoding="utf-8")
    result = BacktestResult(
        html_path=html_path,
        elapsed_time=1.0,
        summary={
            "total_return": 7.0,
            "sharpe_ratio": 8.0,
            "sortino_ratio": 8.5,
            "calmar_ratio": 9.0,
            "max_drawdown": 10.0,
            "win_rate": 11.0,
            "trade_count": 12,
        },
        strategy_name="test",
        dataset_name="sample",
    )

    with patch("src.application.services.backtest_result_summary.extract_metrics_from_html", side_effect=RuntimeError("bad html")):
        summary = service._extract_result_summary(result)
        assert summary.total_return == 7.0
        assert summary.sortino_ratio == 8.5
        assert summary.trade_count == 12


def test_get_execution_info_delegates_to_runner(monkeypatch):
    service = BacktestService()
    monkeypatch.setattr(service._runner, "get_execution_info", lambda name: {"strategy": name})

    assert service.get_execution_info("abc") == {"strategy": "abc"}
