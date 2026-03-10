"""BacktestAttributionService unit tests."""

import asyncio
import json
import threading
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from src.domains.backtest.contracts import RunType
from src.entrypoints.http.schemas.backtest import JobStatus
from src.application.services.backtest_attribution_service import BacktestAttributionService


def test_execute_attribution_sync_uses_threadsafe_progress(monkeypatch):
    service = BacktestAttributionService()

    async def _dummy_update_job_status(*args, **kwargs):
        return None

    monkeypatch.setattr(service._manager, "update_job_status", _dummy_update_job_status)

    called = {}
    init_args = {}

    def _fake_run_coroutine_threadsafe(coro, loop):
        called["loop"] = loop
        called["coro"] = coro
        return object()

    class _FakeAnalyzer:
        def __init__(self, **kwargs):
            init_args.update(kwargs)

        def run(self, progress_callback=None):
            assert progress_callback is not None
            progress_callback("running", 0.1)
            return {"baseline_metrics": {"total_return": 1.0, "sharpe_ratio": 1.0}}

    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", _fake_run_coroutine_threadsafe)
    monkeypatch.setattr(
        "src.application.services.backtest_attribution_service.SignalAttributionAnalyzer",
        _FakeAnalyzer,
    )

    loop = asyncio.new_event_loop()
    try:
        result = service._execute_attribution_sync(
            "job-1",
            "strategy-1",
            loop,
            {"shared_config": {"dataset": "sample"}},
            5,
            128,
            42,
        )
    finally:
        loop.close()

    assert called["loop"] is loop
    assert init_args["strategy_name"] == "strategy-1"
    assert init_args["shapley_top_n"] == 5
    assert init_args["shapley_permutations"] == 128
    assert init_args["random_seed"] == 42
    assert result["baseline_metrics"]["total_return"] == 1.0
    service._executor.shutdown(wait=False)


def test_execute_attribution_sync_skips_progress_when_cancelled(monkeypatch):
    service = BacktestAttributionService()

    async def _dummy_update_job_status(*args, **kwargs):
        return None

    monkeypatch.setattr(service._manager, "update_job_status", _dummy_update_job_status)
    called = {"count": 0}

    def _fake_run_coroutine_threadsafe(coro, loop):
        called["count"] += 1
        return object()

    class _FakeAnalyzer:
        def __init__(self, **_kwargs):
            pass

        def run(self, progress_callback=None):
            assert progress_callback is not None
            progress_callback("running", 0.1)
            return {"ok": True}

    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", _fake_run_coroutine_threadsafe)
    monkeypatch.setattr(
        "src.application.services.backtest_attribution_service.SignalAttributionAnalyzer",
        _FakeAnalyzer,
    )

    cancel_event = threading.Event()
    cancel_event.set()
    loop = asyncio.new_event_loop()
    try:
        result = service._execute_attribution_sync(
            "job-1",
            "strategy-1",
            loop,
            None,
            5,
            128,
            None,
            cancel_event,
        )
    finally:
        loop.close()

    assert result["ok"] is True
    assert called["count"] == 0
    service._executor.shutdown(wait=False)


class _DummyManager:
    def __init__(self, with_job: bool = True) -> None:
        self.job = (
            SimpleNamespace(
                raw_result=None,
                created_at=datetime.now(UTC),
                started_at=datetime.now(UTC),
                status=JobStatus.RUNNING,
            )
            if with_job
            else None
        )
        self.created_jobs: list[tuple[str, str, Any]] = []
        self.status_updates: list[dict[str, Any]] = []
        self.set_task_calls: list[tuple[str, Any]] = []
        self.acquire_count = 0
        self.release_count = 0

    def create_job(
        self,
        strategy_name: str,
        job_type: str = "backtest",
        run_spec: Any = None,
    ) -> str:
        self.created_jobs.append((strategy_name, job_type, run_spec))
        return "job-1"

    async def set_job_task(self, job_id: str, task: Any) -> None:
        self.set_task_calls.append((job_id, task))

    async def acquire_slot(self) -> None:
        self.acquire_count += 1

    def release_slot(self) -> None:
        self.release_count += 1

    async def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        message: str | None = None,
        progress: float | None = None,
        error: str | None = None,
    ) -> None:
        self.status_updates.append(
            {
                "job_id": job_id,
                "status": status,
                "message": message,
                "progress": progress,
                "error": error,
            }
        )

    def get_job(self, _job_id: str):
        return self.job


class _DummyLoop:
    def __init__(self, result: dict[str, Any] | None = None, exc: BaseException | None = None) -> None:
        self.result = result
        self.exc = exc
        self.calls: list[tuple[Any, Any, tuple[Any, ...]]] = []

    async def run_in_executor(self, executor: Any, fn: Any, *args: Any) -> dict[str, Any]:
        self.calls.append((executor, fn, args))
        if self.exc is not None:
            raise self.exc
        return self.result or {}


@pytest.mark.asyncio
async def test_submit_attribution_creates_job_and_registers_task(monkeypatch):
    manager = _DummyManager()
    service = BacktestAttributionService(manager=cast(Any, manager))

    fake_task = object()
    captured: dict[str, Any] = {}

    def _fake_create_task(coro: Any) -> Any:
        captured["coro"] = coro
        coro.close()
        return fake_task

    monkeypatch.setattr(asyncio, "create_task", _fake_create_task)

    job_id = await service.submit_attribution(
        strategy_name="strategy-1",
        config_override={"shared_config": {"dataset": "sample"}},
        shapley_top_n=3,
        shapley_permutations=64,
        random_seed=7,
    )

    assert job_id == "job-1"
    assert len(manager.created_jobs) == 1
    strategy_name, job_type, run_spec = manager.created_jobs[0]
    assert strategy_name == "strategy-1"
    assert job_type == "backtest_attribution"
    assert run_spec is not None
    assert run_spec.run_type == RunType.ATTRIBUTION
    assert run_spec.dataset_name == "sample"
    assert run_spec.parameters["shapley_top_n"] == 3
    assert manager.set_task_calls == [("job-1", fake_task)]
    assert "coro" in captured
    service._executor.shutdown(wait=False)


@pytest.mark.asyncio
async def test_submit_attribution_resolves_dataset_from_base_strategy_when_override_missing(monkeypatch):
    manager = _DummyManager()
    service = BacktestAttributionService(manager=cast(Any, manager))

    fake_task = object()

    def _fake_create_task(coro: Any) -> Any:
        coro.close()
        return fake_task

    monkeypatch.setattr(asyncio, "create_task", _fake_create_task)
    monkeypatch.setattr(
        service._runner.config_loader,
        "load_strategy_config",
        lambda strategy_name: {"shared_config": {"dataset": "primeExTopix500"}}
        if strategy_name == "strategy-2"
        else {},
    )
    monkeypatch.setattr(
        service._runner.config_loader,
        "merge_shared_config",
        lambda _strategy_config: {"dataset": "primeExTopix500"},
    )

    job_id = await service.submit_attribution(strategy_name="strategy-2")

    assert job_id == "job-1"
    _, _, run_spec = manager.created_jobs[0]
    assert run_spec is not None
    assert run_spec.dataset_name == "primeExTopix500"
    assert run_spec.dataset_snapshot_id == "primeExTopix500"
    service._executor.shutdown(wait=False)


@pytest.mark.asyncio
async def test_submit_attribution_normalizes_blank_dataset_override_before_execution(monkeypatch):
    manager = _DummyManager()
    service = BacktestAttributionService(manager=cast(Any, manager))
    captured: dict[str, Any] = {}

    async def _dummy_run_attribution(
        job_id: str,
        strategy_name: str,
        config_override: dict[str, Any] | None,
        shapley_top_n: int,
        shapley_permutations: int,
        random_seed: int | None,
        cancel_event=None,
    ) -> None:
        _ = (shapley_top_n, shapley_permutations, random_seed, cancel_event)
        captured["run_args"] = (job_id, strategy_name, config_override)
        return None
    monkeypatch.setattr(service, "_run_attribution", _dummy_run_attribution)
    monkeypatch.setattr(
        service._runner.config_loader,
        "load_strategy_config",
        lambda strategy_name: {"shared_config": {"dataset": "primeExTopix500"}}
        if strategy_name == "strategy-3"
        else {},
    )
    monkeypatch.setattr(
        service._runner.config_loader,
        "merge_shared_config",
        lambda _strategy_config: {"dataset": "primeExTopix500", "direction": "longonly"},
    )

    job_id = await service.submit_attribution(
        strategy_name="strategy-3",
        config_override={"shared_config": {"dataset": "   ", "direction": "shortonly"}},
    )
    await asyncio.sleep(0)

    assert job_id == "job-1"
    _, _, run_spec = manager.created_jobs[0]
    assert run_spec is not None
    assert run_spec.dataset_name == "primeExTopix500"
    assert run_spec.parameters["config_override"] == {
        "shared_config": {"direction": "shortonly"},
    }
    _, _, forwarded_override = captured["run_args"]
    assert forwarded_override == {"shared_config": {"direction": "shortonly"}}
    service._executor.shutdown(wait=False)


@pytest.mark.asyncio
async def test_run_attribution_success_updates_status_and_stores_raw_result(monkeypatch):
    manager = _DummyManager(with_job=True)
    service = BacktestAttributionService(manager=cast(Any, manager))
    loop = _DummyLoop(result={"baseline_metrics": {"total_return": 1.0, "sharpe_ratio": 1.0}})
    monkeypatch.setattr(asyncio, "get_running_loop", lambda: loop)
    monkeypatch.setattr(
        service,
        "_persist_attribution_artifact",
        lambda **_kwargs: Path("/tmp/attribution.json"),
    )

    await service._run_attribution(
        job_id="job-1",
        strategy_name="strategy-1",
        config_override=None,
        shapley_top_n=5,
        shapley_permutations=128,
        random_seed=42,
    )

    assert manager.acquire_count == 1
    assert manager.release_count == 1
    assert manager.job.raw_result == {
        "baseline_metrics": {"total_return": 1.0, "sharpe_ratio": 1.0},
        "_artifact_path": "/tmp/attribution.json",
    }
    assert manager.status_updates[0]["status"] == JobStatus.RUNNING
    assert manager.status_updates[-1]["status"] == JobStatus.COMPLETED
    assert len(loop.calls) == 1
    service._executor.shutdown(wait=False)


@pytest.mark.asyncio
async def test_run_attribution_success_without_job_entry(monkeypatch):
    manager = _DummyManager(with_job=False)
    service = BacktestAttributionService(manager=cast(Any, manager))
    loop = _DummyLoop(result={"ok": True})
    monkeypatch.setattr(asyncio, "get_running_loop", lambda: loop)

    await service._run_attribution(
        job_id="job-1",
        strategy_name="strategy-1",
        config_override=None,
        shapley_top_n=5,
        shapley_permutations=128,
        random_seed=None,
    )

    assert manager.release_count == 1
    assert manager.status_updates[-1]["status"] == JobStatus.COMPLETED
    service._executor.shutdown(wait=False)


@pytest.mark.asyncio
async def test_run_attribution_failure_sets_failed_status(monkeypatch):
    manager = _DummyManager(with_job=True)
    service = BacktestAttributionService(manager=cast(Any, manager))
    loop = _DummyLoop(exc=RuntimeError("boom"))
    monkeypatch.setattr(asyncio, "get_running_loop", lambda: loop)

    await service._run_attribution(
        job_id="job-1",
        strategy_name="strategy-1",
        config_override=None,
        shapley_top_n=5,
        shapley_permutations=128,
        random_seed=None,
    )

    assert manager.release_count == 1
    assert manager.status_updates[-1]["status"] == JobStatus.FAILED
    assert manager.status_updates[-1]["error"] == "boom"
    service._executor.shutdown(wait=False)


@pytest.mark.asyncio
async def test_run_attribution_cancelled_sets_cancelled_status(monkeypatch):
    manager = _DummyManager(with_job=True)
    service = BacktestAttributionService(manager=cast(Any, manager))
    loop = _DummyLoop(exc=asyncio.CancelledError())
    monkeypatch.setattr(asyncio, "get_running_loop", lambda: loop)
    cancel_event = threading.Event()

    await service._run_attribution(
        job_id="job-1",
        strategy_name="strategy-1",
        config_override=None,
        shapley_top_n=5,
        shapley_permutations=128,
        random_seed=None,
        cancel_event=cancel_event,
    )

    assert manager.release_count == 1
    assert manager.status_updates[-1]["status"] == JobStatus.CANCELLED
    assert cancel_event.is_set()
    service._executor.shutdown(wait=False)


def test_persist_attribution_artifact_writes_xdg_json(monkeypatch, tmp_path: Path):
    manager = _DummyManager(with_job=True)
    service = BacktestAttributionService(manager=cast(Any, manager))

    monkeypatch.setattr(
        "src.application.services.backtest_attribution_service.get_backtest_attribution_dir",
        lambda: tmp_path,
    )
    monkeypatch.setattr(
        "src.application.services.backtest_attribution_service.find_strategy_path",
        lambda _name: Path("/tmp/strategies/experimental/range_break_v18.yaml"),
    )
    monkeypatch.setattr(
        "src.application.services.backtest_attribution_service.get_settings",
        lambda: SimpleNamespace(
            market_db_path="/tmp/market.duckdb",
            portfolio_db_path="/tmp/portfolio.db",
            dataset_base_path="/tmp/datasets",
        ),
    )
    monkeypatch.setattr(
        service._runner.config_loader,
        "load_strategy_config",
        lambda _name: {"entry_filter_params": {"volume_ratio_above": {"enabled": True}}},
    )
    monkeypatch.setattr(
        service._runner,
        "build_parameters_for_strategy",
        lambda strategy, config_override: {
            "shared_config": {"dataset": "prime_202601"},
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
            "exit_trigger_params": {},
        },
    )

    saved_path = service._persist_attribution_artifact(
        job_id="job-1",
        strategy_name="experimental/range_break_v18",
        config_override={"shared_config": {"initial_cash": 123456}},
        shapley_top_n=5,
        shapley_permutations=128,
        random_seed=42,
        result={"baseline_metrics": {"total_return": 0.12, "sharpe_ratio": 1.34}},
    )

    assert saved_path.exists()
    assert saved_path.parent == (tmp_path / "experimental" / "range_break_v18")

    payload = json.loads(saved_path.read_text(encoding="utf-8"))
    assert payload["strategy"]["name"] == "experimental/range_break_v18"
    assert payload["strategy"]["yaml_path"] == "/tmp/strategies/experimental/range_break_v18.yaml"
    assert payload["strategy"]["effective_parameters"]["shared_config"]["dataset"] == "prime_202601"
    assert payload["runtime"]["shapley_top_n"] == 5
    assert payload["databases"]["market_db"]["name"] == "market.duckdb"
    assert payload["databases"]["dataset_name"] == "prime_202601"
    assert payload["result"]["baseline_metrics"]["total_return"] == pytest.approx(0.12)
    service._executor.shutdown(wait=False)


@pytest.mark.asyncio
async def test_run_attribution_persistence_failure_does_not_fail_job(monkeypatch):
    manager = _DummyManager(with_job=True)
    service = BacktestAttributionService(manager=cast(Any, manager))
    loop = _DummyLoop(result={"ok": True})
    monkeypatch.setattr(asyncio, "get_running_loop", lambda: loop)

    def _raise_persist(**_kwargs):
        raise RuntimeError("persist failed")

    monkeypatch.setattr(service, "_persist_attribution_artifact", _raise_persist)

    await service._run_attribution(
        job_id="job-1",
        strategy_name="strategy-1",
        config_override=None,
        shapley_top_n=5,
        shapley_permutations=128,
        random_seed=None,
    )

    assert manager.status_updates[-1]["status"] == JobStatus.COMPLETED
    service._executor.shutdown(wait=False)
