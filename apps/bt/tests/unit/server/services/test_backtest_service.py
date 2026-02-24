"""
BacktestService unit tests
"""

import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest

from src.domains.backtest.core.runner import BacktestResult
from src.application.services.backtest_service import BacktestService


def test_execute_backtest_sync_uses_threadsafe_progress(monkeypatch, tmp_path: Path):
    service = BacktestService()

    async def _dummy_update_job_status(*args, **kwargs):
        return None

    service._manager.update_job_status = _dummy_update_job_status  # type: ignore[assignment]

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

    service._runner.execute = _fake_execute  # type: ignore[assignment]

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


def test_execute_backtest_sync_uses_runner_default_data_access_mode(tmp_path: Path):
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

    service._runner.execute = _fake_execute  # type: ignore[assignment]

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
        lambda _strategy_name, job_type="backtest": "job-123",
    )

    async def _set_job_task(job_id: str, task):
        captured["job_id"] = job_id
        captured["task"] = task
        await task

    monkeypatch.setattr(service._manager, "set_job_task", _set_job_task)

    job_id = await service.submit_backtest("strategy")

    assert job_id == "job-123"
    assert captured["job_id"] == "job-123"


@pytest.mark.asyncio
async def test_run_backtest_success_updates_result_and_status(monkeypatch, tmp_path: Path):
    service = BacktestService()
    result = BacktestResult(
        html_path=tmp_path / "result.html",
        elapsed_time=1.5,
        summary={"total_return": 10.0},
        strategy_name="test",
        dataset_name="sample",
    )

    events: list[tuple[str, object]] = []

    async def _acquire_slot():
        events.append(("acquire", None))

    async def _update_job_status(job_id: str, status, **kwargs):
        events.append(("status", (job_id, status, kwargs)))

    async def _set_job_result(**kwargs):
        events.append(("result", kwargs))

    service._manager.acquire_slot = _acquire_slot  # type: ignore[assignment]
    service._manager.update_job_status = _update_job_status  # type: ignore[assignment]
    service._manager.set_job_result = _set_job_result  # type: ignore[assignment]
    service._manager.release_slot = lambda: events.append(("release", None))  # type: ignore[assignment]

    class _FakeLoop:
        def run_in_executor(self, executor, fn, *args):  # noqa: ANN001
            _ = (executor, fn, args)
            fut = asyncio.Future()
            fut.set_result(result)
            return fut

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: _FakeLoop())
    monkeypatch.setattr(service, "_extract_result_summary", lambda _result: {"ok": True})

    await service._run_backtest("job-1", "strategy-1")

    assert ("acquire", None) in events
    assert any(name == "result" for name, _ in events)
    assert any(name == "release" for name, _ in events)


@pytest.mark.asyncio
async def test_run_backtest_handles_cancelled_and_failure(monkeypatch):
    service = BacktestService()
    events: list[tuple[str, object]] = []
    status_values: list[str] = []

    async def _acquire_slot():
        events.append(("acquire", None))

    async def _update_job_status(job_id: str, status, **kwargs):
        events.append(("status", (job_id, status, kwargs)))
        status_values.append(status.value)

    service._manager.acquire_slot = _acquire_slot  # type: ignore[assignment]
    service._manager.update_job_status = _update_job_status  # type: ignore[assignment]
    service._manager.release_slot = lambda: events.append(("release", None))  # type: ignore[assignment]

    class _CancelledLoop:
        def run_in_executor(self, executor, fn, *args):  # noqa: ANN001
            _ = (executor, fn, args)
            fut = asyncio.Future()
            fut.set_exception(asyncio.CancelledError())
            return fut

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: _CancelledLoop())
    await service._run_backtest("job-cancel", "strategy")
    assert "cancelled" in status_values

    events.clear()
    status_values.clear()

    class _FailedLoop:
        def run_in_executor(self, executor, fn, *args):  # noqa: ANN001
            _ = (executor, fn, args)
            fut = asyncio.Future()
            fut.set_exception(RuntimeError("failed"))
            return fut

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: _FailedLoop())
    await service._run_backtest("job-fail", "strategy")
    assert "failed" in status_values


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


def test_get_execution_info_delegates_to_runner():
    service = BacktestService()
    service._runner.get_execution_info = lambda name: {"strategy": name}  # type: ignore[assignment]

    assert service.get_execution_info("abc") == {"strategy": "abc"}
