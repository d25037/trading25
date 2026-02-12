"""
BacktestService unit tests
"""

import asyncio
from pathlib import Path

from src.lib.backtest_core.runner import BacktestResult
from src.server.services.backtest_service import BacktestService


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


def test_execute_backtest_sync_passes_direct_data_access_mode(tmp_path: Path):
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

    assert captured["data_access_mode"] == "direct"
