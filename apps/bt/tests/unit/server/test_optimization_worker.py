"""optimization_worker.py のテスト"""

import time
from datetime import datetime, timedelta

import pytest

from src.application.services.job_manager import JobManager
from src.application.workers import optimization_worker as worker_mod
from src.application.workers.optimization_worker import run_optimization_worker
from src.entrypoints.http.schemas.backtest import JobStatus


@pytest.mark.asyncio
async def test_run_optimization_worker_completes_job() -> None:
    manager = JobManager()
    job_id = manager.create_job("worker-strategy", job_type="optimization")

    def _execute_sync(strategy_name: str) -> dict[str, object]:
        assert strategy_name == "worker-strategy"
        return {
            "best_score": 1.2,
            "best_params": {"period": 20},
            "worst_score": 0.1,
            "worst_params": {"period": 5},
            "total_combinations": 12,
            "html_path": "/tmp/optimization-result.html",
        }

    exit_code = await run_optimization_worker(
        job_id,
        "worker-strategy",
        manager=manager,
        execute_sync=_execute_sync,
        heartbeat_seconds=60.0,
    )

    job = manager.get_job(job_id)
    assert exit_code == 0
    assert job is not None
    assert job.status == JobStatus.COMPLETED
    assert job.best_score == 1.2
    assert job.best_params == {"period": 20}
    assert job.worst_score == 0.1
    assert job.worst_params == {"period": 5}
    assert job.total_combinations == 12
    assert job.html_path == "/tmp/optimization-result.html"
    assert job.raw_result == {
        "best_score": 1.2,
        "best_params": {"period": 20},
        "worst_score": 0.1,
        "worst_params": {"period": 5},
        "total_combinations": 12,
        "html_path": "/tmp/optimization-result.html",
    }


@pytest.mark.asyncio
async def test_run_optimization_worker_marks_job_failed_on_error() -> None:
    manager = JobManager()
    job_id = manager.create_job("worker-strategy", job_type="optimization")

    def _execute_sync(strategy_name: str) -> dict[str, object]:
        _ = strategy_name
        raise RuntimeError("boom")

    exit_code = await run_optimization_worker(
        job_id,
        "worker-strategy",
        manager=manager,
        execute_sync=_execute_sync,
        heartbeat_seconds=60.0,
    )

    job = manager.get_job(job_id)
    assert exit_code == 1
    assert job is not None
    assert job.status == JobStatus.FAILED
    assert job.error == "boom"


@pytest.mark.asyncio
async def test_run_optimization_worker_marks_job_failed_on_timeout() -> None:
    manager = JobManager()
    job_id = manager.create_job("worker-strategy", job_type="optimization")

    def _slow_execute_sync(strategy_name: str) -> dict[str, object]:
        assert strategy_name == "worker-strategy"
        time.sleep(1.2)
        return {"best_score": 1.0}

    exit_code = await run_optimization_worker(
        job_id,
        "worker-strategy",
        manager=manager,
        execute_sync=_slow_execute_sync,
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
async def test_optimization_heartbeat_loop_handles_timeout_and_cancel() -> None:
    manager = JobManager()
    timeout_job_id = manager.create_job("worker-strategy", job_type="optimization")
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

    cancel_job_id = manager.create_job("worker-strategy", job_type="optimization")
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
async def test_run_optimization_worker_returns_two_when_claim_fails() -> None:
    class _Manager:
        default_lease_seconds = 60

        async def claim_job_execution(self, *args, **kwargs):  # noqa: ANN002
            _ = (args, kwargs)
            return None

        def set_portfolio_db(self, value):  # noqa: ANN001
            _ = value

    exit_code = await run_optimization_worker(
        "job-1",
        "worker-strategy",
        manager=_Manager(),  # type: ignore[arg-type]
        execute_sync=lambda strategy_name: {"best_score": strategy_name},
        heartbeat_seconds=60.0,
    )

    assert exit_code == 2


def test_execute_optimization_sync_extracts_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeEngine:
        def __init__(self, strategy_name: str) -> None:
            assert strategy_name == "demo"

        def optimize(self):
            return type(
                "Result",
                (),
                {
                    "best_score": 1.2,
                    "best_params": {"period": 20},
                    "all_results": [
                        {"score": 1.2, "params": {"period": 20}},
                        {"score": 0.3, "params": {"period": 5}},
                    ],
                    "html_path": "/tmp/result.html",
                },
            )()

    monkeypatch.setattr(worker_mod, "ParameterOptimizationEngine", _FakeEngine)

    assert worker_mod._execute_optimization_sync("demo") == {  # noqa: SLF001
        "best_score": 1.2,
        "best_params": {"period": 20},
        "worst_score": 0.3,
        "worst_params": {"period": 5},
        "total_combinations": 2,
        "html_path": "/tmp/result.html",
    }


def test_optimization_worker_main_uses_cli_args(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def _run_optimization_worker(job_id: str, strategy_name: str, **kwargs):
        captured["job_id"] = job_id
        captured["strategy_name"] = strategy_name
        captured["kwargs"] = kwargs
        return 11

    monkeypatch.setattr(
        worker_mod,
        "_parse_args",
        lambda: type(
            "Args",
            (),
            {"job_id": "job-1", "strategy_name": "strategy-1", "timeout_seconds": 99},
        )(),
    )
    monkeypatch.setattr(worker_mod, "run_optimization_worker", _run_optimization_worker)

    assert worker_mod.main() == 11
    assert captured["job_id"] == "job-1"
    assert captured["strategy_name"] == "strategy-1"
    assert captured["kwargs"] == {"timeout_seconds": 99}
