"""optimization_worker.py のテスト"""

import time
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from src.application.services.verification_orchestrator import (
    INTERNAL_VERIFICATION_CANDIDATES_KEY,
    INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY,
    build_verification_seed,
    serialize_candidate_seeds,
)
from src.application.services.job_manager import JobManager
from src.application.workers import optimization_worker as worker_mod
from src.application.workers.optimization_worker import run_optimization_worker
from src.domains.backtest.contracts import (
    CanonicalExecutionMetrics,
    EnginePolicy,
    EnginePolicyMode,
    VerificationOverallStatus,
    VerificationSummary,
)
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
async def test_run_optimization_worker_marks_job_failed_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = JobManager()
    job_id = manager.create_job("worker-strategy", job_type="optimization")
    monkeypatch.setattr(
        manager,
        "_resolve_timeout_at",
        lambda started_at, timeout_seconds=None: started_at + timedelta(seconds=0.1),
    )

    def _slow_execute_sync(strategy_name: str) -> dict[str, object]:
        assert strategy_name == "worker-strategy"
        time.sleep(0.2)
        return {"best_score": 1.0}

    exit_code = await run_optimization_worker(
        job_id,
        "worker-strategy",
        manager=manager,
        execute_sync=_slow_execute_sync,
        heartbeat_seconds=0.01,
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
    timeout_child_id = manager.create_job("worker-strategy", job_type="backtest")
    await manager.claim_job_execution(
        timeout_job_id,
        lease_owner="worker",
        timeout_seconds=1,
    )
    timeout_job = manager.get_job(timeout_job_id)
    assert timeout_job is not None
    timeout_job.timeout_at = datetime.now() - timedelta(seconds=1)
    timeout_job.raw_result = serialize_candidate_seeds(
        {},
        [
            build_verification_seed(
                candidate_id="grid_0001",
                fast_rank=1,
                fast_score=1.2,
                fast_metrics=None,
                strategy_name="worker-strategy",
                config_override={"shared_config": {"dataset": "demo"}},
            ).model_copy(update={"verification_run_id": timeout_child_id})
        ],
        requested_top_k=1,
    )

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
    timeout_child = manager.get_job(timeout_child_id)
    assert timeout_child is not None
    assert timeout_child.status == JobStatus.CANCELLED
    assert exit_codes == [124]

    cancel_job_id = manager.create_job("worker-strategy", job_type="optimization")
    cancel_child_id = manager.create_job("worker-strategy", job_type="backtest")
    await manager.claim_job_execution(cancel_job_id, lease_owner="worker")
    cancel_job = manager.get_job(cancel_job_id)
    assert cancel_job is not None
    cancel_job.raw_result = serialize_candidate_seeds(
        {},
        [
            build_verification_seed(
                candidate_id="grid_0002",
                fast_rank=1,
                fast_score=1.1,
                fast_metrics=None,
                strategy_name="worker-strategy",
                config_override={"shared_config": {"dataset": "demo"}},
            ).model_copy(update={"verification_run_id": cancel_child_id})
        ],
        requested_top_k=1,
    )
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
    cancelled_child = manager.get_job(cancel_child_id)
    assert cancelled_child is not None
    assert cancelled_child.status == JobStatus.CANCELLED
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

        def build_config_override(self, params):  # noqa: ANN001
            return {"shared_config": {"dataset": "demo"}, "entry_filter_params": params, "exit_trigger_params": {}}

        def optimize(self):
            return type(
                "Result",
                (),
                {
                    "best_score": 1.2,
                    "best_params": {"period": 20},
                    "scoring_weights": {"sharpe_ratio": 0.5, "total_return": 0.5},
                    "all_results": [
                        {
                            "score": 1.2,
                            "params": {"period": 20},
                            "metric_values": {
                                "sharpe_ratio": 1.3,
                                "total_return": 12.0,
                                "max_drawdown": -5.0,
                                "trade_count": 8,
                            },
                        },
                        {
                            "score": 0.3,
                            "params": {"period": 5},
                            "metric_values": {
                                "sharpe_ratio": 0.2,
                                "total_return": 2.0,
                                "max_drawdown": -12.0,
                                "trade_count": 3,
                            },
                        },
                    ],
                    "html_path": "/tmp/result.html",
                },
            )()

    monkeypatch.setattr(worker_mod, "ParameterOptimizationEngine", _FakeEngine)

    payload = worker_mod._execute_optimization_sync("demo")  # noqa: SLF001
    assert payload["best_score"] == 1.2
    assert payload["best_params"] == {"period": 20}
    assert payload["worst_score"] == 0.3
    assert payload["worst_params"] == {"period": 5}
    assert payload["total_combinations"] == 2
    assert payload["html_path"] == "/tmp/result.html"
    assert payload["fast_candidates"][0]["candidate_id"] == "grid_0001"
    assert payload[INTERNAL_VERIFICATION_CANDIDATES_KEY][0]["candidate_id"] == "grid_0001"


@pytest.mark.asyncio
async def test_run_optimization_worker_runs_verification_when_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
            INTERNAL_VERIFICATION_CANDIDATES_KEY: [
                build_verification_seed(
                    candidate_id="grid_0001",
                    fast_rank=1,
                    fast_score=1.2,
                    fast_metrics=CanonicalExecutionMetrics(
                        total_return=10.0,
                        sharpe_ratio=1.1,
                        max_drawdown=-4.0,
                        trade_count=5,
                    ),
                    strategy_name="worker-strategy",
                    config_override={"shared_config": {"dataset": "demo"}},
                ).model_dump(mode="json"),
            ],
            INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY: {"sharpe_ratio": 0.5, "total_return": 0.5},
        }

    async def _run_verification_orchestrator(manager, **kwargs):  # noqa: ANN001
        _ = manager
        updated = dict(kwargs["raw_result"])
        updated["verification"] = VerificationSummary(
            overall_status=VerificationOverallStatus.COMPLETED,
            requested_top_k=1,
            completed_count=1,
            mismatch_count=0,
            authoritative_candidate_id="grid_0001",
            candidates=[],
        ).model_dump(mode="json")
        return updated, VerificationSummary.model_validate(updated["verification"])

    monkeypatch.setattr(worker_mod, "run_verification_orchestrator", _run_verification_orchestrator)

    exit_code = await run_optimization_worker(
        job_id,
        "worker-strategy",
        manager=manager,
        execute_sync=_execute_sync,
        engine_policy=EnginePolicy(
            mode=EnginePolicyMode.FAST_THEN_VERIFY,
            verification_top_k=1,
        ),
        heartbeat_seconds=60.0,
    )

    job = manager.get_job(job_id)
    assert exit_code == 0
    assert job is not None
    assert job.status == JobStatus.COMPLETED
    assert job.raw_result is not None
    assert job.raw_result["verification"]["authoritative_candidate_id"] == "grid_0001"


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
            {
                "job_id": "job-1",
                "strategy_name": "strategy-1",
                "engine_policy_json": '{"mode":"fast_then_verify","verification_top_k":3}',
                "timeout_seconds": 99,
            },
        )(),
    )
    monkeypatch.setattr(worker_mod, "run_optimization_worker", _run_optimization_worker)

    assert worker_mod.main() == 11
    assert captured["job_id"] == "job-1"
    assert captured["strategy_name"] == "strategy-1"
    assert captured["kwargs"] == {
        "engine_policy": EnginePolicy(
            mode=EnginePolicyMode.FAST_THEN_VERIFY,
            verification_top_k=3,
        ),
        "timeout_seconds": 99,
    }


def test_optimization_worker_main_rejects_non_object_engine_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        worker_mod,
        "_parse_args",
        lambda: SimpleNamespace(
            job_id="job-1",
            strategy_name="strategy-1",
            engine_policy_json='["bad"]',
            timeout_seconds=None,
        ),
    )

    with pytest.raises(ValueError, match="engine policy payload must be a JSON object"):
        worker_mod.main()


@pytest.mark.asyncio
async def test_optimization_heartbeat_loop_returns_for_missing_and_terminal_jobs() -> None:
    manager = JobManager()

    await worker_mod._heartbeat_loop(  # noqa: SLF001
        manager,
        "missing-job",
        lease_owner="worker",
        heartbeat_seconds=0.01,
        exit_on_cancel=lambda _code: None,
    )

    terminal_job_id = manager.create_job("worker-strategy", job_type="optimization")
    terminal_job = manager.get_job(terminal_job_id)
    assert terminal_job is not None
    terminal_job.status = JobStatus.COMPLETED

    await worker_mod._heartbeat_loop(  # noqa: SLF001
        manager,
        terminal_job_id,
        lease_owner="worker",
        heartbeat_seconds=0.01,
        exit_on_cancel=lambda _code: None,
    )


@pytest.mark.asyncio
async def test_run_optimization_worker_without_manager_closes_owned_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_portfolio_dbs: list[object] = []
    fake_managers: list[object] = []

    class _FakeSettings:
        portfolio_db_path = "/tmp/portfolio.db"

    class _FakePortfolioDb:
        def __init__(self, path: str) -> None:
            self.path = path
            self.closed = False
            fake_portfolio_dbs.append(self)

        def close(self) -> None:
            self.closed = True

    class _FakeManager:
        default_lease_seconds = 60

        def __init__(self) -> None:
            self.portfolio_db_values: list[object | None] = []
            fake_managers.append(self)

        def set_portfolio_db(self, value: object | None) -> None:
            self.portfolio_db_values.append(value)

        async def claim_job_execution(self, *args, **kwargs):  # noqa: ANN002
            _ = (args, kwargs)
            return None

    monkeypatch.setattr(worker_mod, "get_settings", lambda: _FakeSettings())
    monkeypatch.setattr(worker_mod, "PortfolioDb", _FakePortfolioDb)
    monkeypatch.setattr(worker_mod, "JobManager", _FakeManager)

    exit_code = await run_optimization_worker(
        "missing-job-id",
        "demo-strategy",
        manager=None,
        heartbeat_seconds=60.0,
        exit_on_cancel=lambda _code: None,
    )

    assert exit_code == 2
    assert len(fake_portfolio_dbs) == 1
    assert fake_portfolio_dbs[0].closed is True
    assert len(fake_managers) == 1
    assert fake_managers[0].portfolio_db_values[-1] is None
