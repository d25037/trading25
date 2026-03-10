"""backtest_worker.py のテスト"""

import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from src.application.services.job_manager import JobManager
from src.application.workers import backtest_worker as worker_mod
from src.application.workers.backtest_worker import run_backtest_worker
from src.domains.backtest.core.runner import BacktestResult
from src.entrypoints.http.schemas.backtest import JobStatus


@pytest.mark.asyncio
async def test_run_backtest_worker_completes_job(tmp_path: Path) -> None:
    manager = JobManager()
    job_id = manager.create_job("worker-strategy")

    class _FakeRunner:
        def execute(
            self,
            strategy: str,
            progress_callback=None,
            config_override=None,
            data_access_mode: str | None = "direct",
        ) -> BacktestResult:
            _ = (config_override, data_access_mode)
            if progress_callback is not None:
                progress_callback("running", 0.1)
            return BacktestResult(
                html_path=tmp_path / "result.html",
                elapsed_time=1.2,
                summary={"total_return": 12.3},
                strategy_name=strategy,
                dataset_name="dataset-a",
            )

    exit_code = await run_backtest_worker(
        job_id,
        "worker-strategy",
        manager=manager,
        runner=_FakeRunner(),
        heartbeat_seconds=60.0,
    )

    job = manager.get_job(job_id)
    assert exit_code == 0
    assert job is not None
    assert job.status == JobStatus.COMPLETED
    assert job.result is not None
    assert job.result.total_return == 12.3
    assert job.run_metadata is not None
    assert job.run_metadata.dataset_snapshot_id == "dataset-a"


@pytest.mark.asyncio
async def test_run_backtest_worker_marks_job_failed_on_error() -> None:
    manager = JobManager()
    job_id = manager.create_job("worker-strategy")

    class _FailingRunner:
        def execute(
            self,
            strategy: str,
            progress_callback=None,
            config_override=None,
            data_access_mode: str | None = "direct",
        ) -> BacktestResult:
            _ = (strategy, progress_callback, config_override, data_access_mode)
            raise RuntimeError("boom")

    exit_code = await run_backtest_worker(
        job_id,
        "worker-strategy",
        manager=manager,
        runner=_FailingRunner(),
        heartbeat_seconds=60.0,
    )

    job = manager.get_job(job_id)
    assert exit_code == 1
    assert job is not None
    assert job.status == JobStatus.FAILED
    assert job.error == "boom"


@pytest.mark.asyncio
async def test_run_backtest_worker_marks_job_failed_on_timeout() -> None:
    manager = JobManager()
    job_id = manager.create_job("worker-strategy")

    class _SlowRunner:
        def execute(
            self,
            strategy: str,
            progress_callback=None,
            config_override=None,
            data_access_mode: str | None = "direct",
        ) -> BacktestResult:
            _ = (strategy, progress_callback, config_override, data_access_mode)
            time.sleep(1.2)
            return BacktestResult(
                html_path=Path("/tmp/slow-result.html"),
                elapsed_time=1.2,
                summary={"total_return": 1.0},
                strategy_name="worker-strategy",
                dataset_name="dataset-a",
            )

    exit_code = await run_backtest_worker(
        job_id,
        "worker-strategy",
        manager=manager,
        runner=_SlowRunner(),
        heartbeat_seconds=0.05,
        timeout_seconds=1,
        exit_on_cancel=lambda _code: None,
    )

    job = manager.get_job(job_id)
    assert exit_code == 124
    assert job is not None
    assert job.status == JobStatus.FAILED
    assert job.error == "worker_timed_out"


@pytest.mark.asyncio
async def test_backtest_heartbeat_loop_handles_timeout_and_cancel() -> None:
    manager = JobManager()
    timeout_job_id = manager.create_job("worker-timeout")
    await manager.claim_job_execution(
        timeout_job_id,
        lease_owner="worker",
        timeout_seconds=1,
    )
    timeout_job = manager.get_job(timeout_job_id)
    assert timeout_job is not None
    timeout_job.timeout_at = datetime.now() - timedelta(seconds=1)

    exit_codes: list[int] = []
    await worker_mod._heartbeat_loop(  # noqa: SLF001
        manager,
        timeout_job_id,
        lease_owner="worker",
        heartbeat_seconds=0.01,
        exit_on_cancel=exit_codes.append,
    )
    timeout_job = manager.get_job(timeout_job_id)
    assert timeout_job is not None
    assert timeout_job.status == JobStatus.FAILED
    assert timeout_job.error == "worker_timed_out"
    assert exit_codes == [124]

    cancel_job_id = manager.create_job("worker-cancel")
    await manager.claim_job_execution(cancel_job_id, lease_owner="worker")
    await manager.request_job_cancel(cancel_job_id, reason="user_requested")
    exit_codes.clear()
    await worker_mod._heartbeat_loop(  # noqa: SLF001
        manager,
        cancel_job_id,
        lease_owner="worker",
        heartbeat_seconds=0.01,
        exit_on_cancel=exit_codes.append,
    )
    cancelled_job = manager.get_job(cancel_job_id)
    assert cancelled_job is not None
    assert cancelled_job.status == JobStatus.CANCELLED
    assert exit_codes == [0]


@pytest.mark.asyncio
async def test_backtest_heartbeat_loop_returns_for_missing_job() -> None:
    manager = JobManager()
    await worker_mod._heartbeat_loop(  # noqa: SLF001
        manager,
        "missing",
        lease_owner="worker",
        heartbeat_seconds=0.01,
        exit_on_cancel=lambda code: pytest.fail(f"unexpected exit {code}"),
    )


@pytest.mark.asyncio
async def test_run_backtest_worker_returns_two_when_claim_fails() -> None:
    class _Manager:
        default_lease_seconds = 60

        async def claim_job_execution(self, *args, **kwargs):  # noqa: ANN002
            _ = (args, kwargs)
            return None

        def set_portfolio_db(self, value):  # noqa: ANN001
            _ = value

    exit_code = await run_backtest_worker(
        "job-1",
        "worker-strategy",
        manager=_Manager(),  # type: ignore[arg-type]
        runner=object(),  # type: ignore[arg-type]
        heartbeat_seconds=60.0,
    )

    assert exit_code == 2


def test_backtest_worker_main_uses_cli_args(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def _run_backtest_worker(job_id: str, strategy_name: str, **kwargs):
        captured["job_id"] = job_id
        captured["strategy_name"] = strategy_name
        captured["kwargs"] = kwargs
        return 7

    monkeypatch.setattr(
        worker_mod,
        "_parse_args",
        lambda: type(
            "Args",
            (),
            {
                "job_id": "job-1",
                "strategy_name": "strategy-1",
                "config_override_json": '{"shared_config":{"dataset":"sample"}}',
                "timeout_seconds": 120,
            },
        )(),
    )
    monkeypatch.setattr(worker_mod, "run_backtest_worker", _run_backtest_worker)

    assert worker_mod.main() == 7
    assert captured["job_id"] == "job-1"
    assert captured["strategy_name"] == "strategy-1"
    assert captured["kwargs"] == {
        "config_override": {"shared_config": {"dataset": "sample"}},
        "timeout_seconds": 120,
    }


def test_backtest_worker_main_rejects_non_object_config_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        worker_mod,
        "_parse_args",
        lambda: type(
            "Args",
            (),
            {
                "job_id": "job-1",
                "strategy_name": "strategy-1",
                "config_override_json": '["invalid"]',
                "timeout_seconds": None,
            },
        )(),
    )

    with pytest.raises(ValueError, match="config override must be a JSON object"):
        worker_mod.main()
