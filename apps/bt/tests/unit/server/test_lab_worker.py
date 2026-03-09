"""lab_worker.py のテスト"""

import time
from datetime import datetime, timedelta

import pytest

from src.application.services.job_manager import JobManager
from src.application.services.lab_service import LabService
from src.application.workers import lab_worker as worker_mod
from src.application.workers.lab_worker import run_lab_worker
from src.entrypoints.http.schemas.backtest import JobStatus


@pytest.mark.asyncio
async def test_run_lab_worker_generate_completes_job() -> None:
    manager = JobManager()
    job_id = manager.create_job("generate(n=3,top=1)", job_type="lab_generate")
    service = LabService(manager=manager, max_workers=1)

    def _execute_generate_sync(*args):  # noqa: ANN002
        _ = args
        return {
            "lab_type": "generate",
            "results": [],
            "total_generated": 3,
            "saved_strategy_path": None,
        }

    service._execute_generate_sync = _execute_generate_sync  # type: ignore[method-assign]

    exit_code = await run_lab_worker(
        job_id,
        {
            "lab_type": "generate",
            "count": 3,
            "top": 1,
            "seed": None,
            "save": False,
            "direction": "longonly",
            "timeframe": "daily",
            "dataset": "primeExTopix500",
            "entry_filter_only": False,
            "allowed_categories": [],
        },
        manager=manager,
        service=service,
        heartbeat_seconds=60.0,
    )

    job = manager.get_job(job_id)
    assert exit_code == 0
    assert job is not None
    assert job.status == JobStatus.COMPLETED
    assert job.raw_result == {
        "lab_type": "generate",
        "results": [],
        "total_generated": 3,
        "saved_strategy_path": None,
    }


@pytest.mark.asyncio
async def test_run_lab_worker_evolve_uses_internal_message_override() -> None:
    manager = JobManager()
    job_id = manager.create_job("demo-strategy", job_type="lab_evolve")
    service = LabService(manager=manager, max_workers=1)

    def _execute_evolve_sync(*args):  # noqa: ANN002
        _ = args
        return {
            "lab_type": "evolve",
            "best_strategy_id": "base_demo-strategy",
            "best_score": 1.2,
            "history": [],
            "saved_strategy_path": None,
            "saved_history_path": None,
            "_job_message": "GA進化完了（ベース戦略が最良のためパラメータ変更なし）",
        }

    service._execute_evolve_sync = _execute_evolve_sync  # type: ignore[method-assign]

    exit_code = await run_lab_worker(
        job_id,
        {
            "lab_type": "evolve",
            "strategy_name": "demo-strategy",
            "generations": 5,
            "population": 20,
            "structure_mode": "params_only",
            "random_add_entry_signals": 1,
            "random_add_exit_signals": 1,
            "seed": None,
            "save": False,
            "entry_filter_only": False,
            "allowed_categories": [],
            "target_scope": "both",
        },
        manager=manager,
        service=service,
        heartbeat_seconds=60.0,
    )

    job = manager.get_job(job_id)
    assert exit_code == 0
    assert job is not None
    assert job.status == JobStatus.COMPLETED
    assert job.message == "GA進化完了（ベース戦略が最良のためパラメータ変更なし）"
    assert job.raw_result is not None
    assert "_job_message" not in job.raw_result


@pytest.mark.asyncio
async def test_run_lab_worker_optimize_completes_job() -> None:
    manager = JobManager()
    job_id = manager.create_job("demo-strategy", job_type="lab_optimize")
    service = LabService(manager=manager, max_workers=1)

    def _execute_optimize_sync(*args):  # noqa: ANN002
        progress_callback = args[-2]
        progress_callback(1, 4, 0.8)
        return {
            "lab_type": "optimize",
            "best_score": 0.8,
            "best_params": {"period": 20},
            "total_trials": 4,
            "history": [],
            "saved_strategy_path": None,
            "saved_history_path": None,
        }

    service._execute_optimize_sync = _execute_optimize_sync  # type: ignore[method-assign]

    exit_code = await run_lab_worker(
        job_id,
        {
            "lab_type": "optimize",
            "strategy_name": "demo-strategy",
            "trials": 4,
            "sampler": "tpe",
            "structure_mode": "params_only",
            "random_add_entry_signals": 1,
            "random_add_exit_signals": 1,
            "seed": None,
            "save": False,
            "entry_filter_only": False,
            "target_scope": "both",
            "allowed_categories": [],
            "scoring_weights": None,
        },
        manager=manager,
        service=service,
        heartbeat_seconds=60.0,
    )

    job = manager.get_job(job_id)
    assert exit_code == 0
    assert job is not None
    assert job.status == JobStatus.COMPLETED
    assert job.raw_result == {
        "lab_type": "optimize",
        "best_score": 0.8,
        "best_params": {"period": 20},
        "total_trials": 4,
        "history": [],
        "saved_strategy_path": None,
        "saved_history_path": None,
    }


@pytest.mark.asyncio
async def test_run_lab_worker_marks_job_failed_on_error() -> None:
    manager = JobManager()
    job_id = manager.create_job("demo-strategy", job_type="lab_improve")
    service = LabService(manager=manager, max_workers=1)

    def _execute_improve_sync(*args):  # noqa: ANN002
        _ = args
        raise RuntimeError("boom")

    service._execute_improve_sync = _execute_improve_sync  # type: ignore[method-assign]

    exit_code = await run_lab_worker(
        job_id,
        {
            "lab_type": "improve",
            "strategy_name": "demo-strategy",
            "auto_apply": False,
            "entry_filter_only": False,
            "allowed_categories": [],
        },
        manager=manager,
        service=service,
        heartbeat_seconds=60.0,
    )

    job = manager.get_job(job_id)
    assert exit_code == 1
    assert job is not None
    assert job.status == JobStatus.FAILED
    assert job.error == "boom"


@pytest.mark.asyncio
async def test_run_lab_worker_marks_job_failed_on_timeout() -> None:
    manager = JobManager()
    job_id = manager.create_job("generate(n=1,top=1)", job_type="lab_generate")
    service = LabService(manager=manager, max_workers=1)

    def _execute_generate_sync(*args):  # noqa: ANN002
        _ = args
        time.sleep(1.2)
        return {"lab_type": "generate", "results": []}

    service._execute_generate_sync = _execute_generate_sync  # type: ignore[method-assign]

    exit_code = await run_lab_worker(
        job_id,
        {
            "lab_type": "generate",
            "count": 1,
            "top": 1,
            "seed": None,
            "save": False,
            "direction": "longonly",
            "timeframe": "daily",
            "dataset": "primeExTopix500",
            "entry_filter_only": False,
            "allowed_categories": [],
        },
        manager=manager,
        service=service,
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
async def test_lab_heartbeat_loop_handles_timeout_and_cancel() -> None:
    manager = JobManager()
    timeout_job_id = manager.create_job("generate(n=1,top=1)", job_type="lab_generate")
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

    cancel_job_id = manager.create_job("generate(n=1,top=1)", job_type="lab_generate")
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
async def test_execute_lab_payload_rejects_unsupported_type() -> None:
    manager = JobManager()
    service = LabService(manager=manager, max_workers=1)

    with pytest.raises(ValueError, match="unsupported lab_type"):
        await worker_mod._execute_lab_payload(  # noqa: SLF001
            service,
            manager,
            "job-1",
            {"lab_type": "unknown"},
        )


@pytest.mark.asyncio
async def test_run_lab_worker_returns_two_when_claim_fails() -> None:
    class _Manager:
        default_lease_seconds = 60

        async def claim_job_execution(self, *args, **kwargs):  # noqa: ANN002
            _ = (args, kwargs)
            return None

        def set_portfolio_db(self, value):  # noqa: ANN001
            _ = value

    exit_code = await run_lab_worker(
        "job-1",
        {"lab_type": "generate"},
        manager=_Manager(),  # type: ignore[arg-type]
        service=LabService(manager=JobManager(), max_workers=1),
        heartbeat_seconds=60.0,
    )

    assert exit_code == 2


def test_lab_worker_main_uses_cli_args(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def _run_lab_worker(job_id: str, payload: dict[str, object], **kwargs):
        captured["job_id"] = job_id
        captured["payload"] = payload
        captured["kwargs"] = kwargs
        return 13

    monkeypatch.setattr(
        worker_mod,
        "_parse_args",
        lambda: type(
            "Args",
            (),
            {
                "job_id": "job-1",
                "payload_json": '{"lab_type":"generate","count":1}',
                "timeout_seconds": 45,
            },
        )(),
    )
    monkeypatch.setattr(worker_mod, "run_lab_worker", _run_lab_worker)

    assert worker_mod.main() == 13
    assert captured["job_id"] == "job-1"
    assert captured["payload"] == {"lab_type": "generate", "count": 1}
    assert captured["kwargs"] == {"timeout_seconds": 45}


def test_lab_worker_main_rejects_non_object_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        worker_mod,
        "_parse_args",
        lambda: type(
            "Args",
            (),
            {"job_id": "job-1", "payload_json": '["invalid"]', "timeout_seconds": None},
        )(),
    )

    with pytest.raises(ValueError, match="payload must be a JSON object"):
        worker_mod.main()
