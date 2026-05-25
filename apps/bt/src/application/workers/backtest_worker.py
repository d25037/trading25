"""Out-of-process backtest worker."""

from __future__ import annotations

import argparse
import asyncio
import os
from contextlib import suppress
from datetime import datetime
from time import perf_counter
from typing import Any, Callable

from loguru import logger

from src.application.services.backtest_result_summary import resolve_backtest_result_summary
from src.application.services.job_status import TERMINAL_JOB_STATUSES
from src.application.services.job_manager import JobManager
from src.domains.backtest.contracts import EngineFamily
from src.domains.backtest.core.runner import BacktestResult, BacktestRunner
from src.domains.backtest.nautilus_adapter import NautilusVerificationRunner
from src.entrypoints.http.schemas.backtest import BacktestResultSummary, JobStatus
from src.infrastructure.db.market.portfolio_db import PortfolioDb
from src.application.workers.job_runtime import (
    DEFAULT_HEARTBEAT_SECONDS,
    WORKER_TIMED_OUT_ERROR,
    duration_ms_for_loaded_job,
    external_worker_lifecycle_fields,
    normalized_heartbeat_seconds,
    parse_json_object_arg,
    record_elapsed_job_duration,
    record_job_duration,
    terminal_worker_exit_code,
    worker_lease_owner,
)
from src.shared.config.settings import get_settings


def _extract_result_summary(result: BacktestResult) -> BacktestResultSummary:
    summary = resolve_backtest_result_summary(
        html_path=result.html_path,
        fallback=result.summary,
        metrics_path=result.metrics_path,
    )
    if summary is not None:
        return summary
    return BacktestResultSummary(
        total_return=0.0,
        sharpe_ratio=0.0,
        sortino_ratio=None,
        calmar_ratio=0.0,
        max_drawdown=0.0,
        win_rate=0.0,
        trade_count=0,
        html_path=str(result.html_path) if result.html_path else None,
    )


async def _heartbeat_loop(
    manager: JobManager,
    job_id: str,
    *,
    lease_owner: str,
    heartbeat_seconds: float,
    exit_on_cancel: Callable[[int], None],
) -> None:
    while True:
        await asyncio.sleep(heartbeat_seconds)
        job = await manager.reload_job_from_storage(job_id)
        if job is None:
            return
        if job.status in TERMINAL_JOB_STATUSES:
            return
        if job.timeout_at is not None and datetime.now() >= job.timeout_at:
            duration_ms = duration_ms_for_loaded_job(job)
            await manager.update_job_status(
                job_id,
                JobStatus.FAILED,
                message="バックテストがタイムアウトしました",
                error=WORKER_TIMED_OUT_ERROR,
            )
            record_job_duration("backtest", JobStatus.FAILED.value, duration_ms)
            logger.warning(
                f"backtest worker timed out: {job_id}",
                error=WORKER_TIMED_OUT_ERROR,
                **external_worker_lifecycle_fields(
                    "backtest",
                    job_id,
                    JobStatus.FAILED.value,
                    lease_owner=lease_owner,
                    durationMs=duration_ms,
                ),
            )
            exit_on_cancel(124)
            return
        if job.cancel_requested_at is not None:
            duration_ms = duration_ms_for_loaded_job(job)
            await manager.cancel_job(
                job_id,
                reason=job.cancel_reason or "controller_requested",
            )
            record_job_duration("backtest", JobStatus.CANCELLED.value, duration_ms)
            logger.info(
                f"backtest worker cancelled: {job_id}",
                **external_worker_lifecycle_fields(
                    "backtest",
                    job_id,
                    JobStatus.CANCELLED.value,
                    lease_owner=lease_owner,
                    durationMs=duration_ms,
                ),
            )
            exit_on_cancel(0)
            return
        await manager.heartbeat_job_execution(
            job_id,
            lease_owner=lease_owner,
            lease_seconds=manager.default_lease_seconds,
        )


async def run_backtest_worker(
    job_id: str,
    strategy_name: str,
    *,
    config_override: dict[str, Any] | None = None,
    manager: JobManager | None = None,
    runner: BacktestRunner | None = None,
    nautilus_runner: NautilusVerificationRunner | None = None,
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
    timeout_seconds: int | None = None,
    exit_on_cancel: Callable[[int], None] = os._exit,
) -> int:
    owns_portfolio_db = False
    portfolio_db: PortfolioDb | None = None
    resolved_manager = manager
    if resolved_manager is None:
        settings = get_settings()
        portfolio_db = PortfolioDb(settings.portfolio_db_path)
        resolved_manager = JobManager()
        resolved_manager.set_portfolio_db(portfolio_db)
        owns_portfolio_db = True
    resolved_runner = runner or BacktestRunner()
    resolved_nautilus_runner = nautilus_runner or NautilusVerificationRunner()
    lease_owner = worker_lease_owner("backtest-worker")

    heartbeat_task: asyncio.Task[None] | None = None
    started_at = perf_counter()
    try:
        claimed = await resolved_manager.claim_job_execution(
            job_id,
            lease_owner=lease_owner,
            lease_seconds=resolved_manager.default_lease_seconds,
            timeout_seconds=timeout_seconds,
            message="バックテストを開始しています...",
            progress=0.0,
        )
        if claimed is None:
            logger.error(f"backtest worker could not claim job: {job_id}")
            return 2
        logger.info(
            f"backtest worker claimed job: {job_id}",
            **external_worker_lifecycle_fields(
                "backtest",
                job_id,
                JobStatus.RUNNING.value,
                lease_owner=lease_owner,
                timeoutAt=claimed.timeout_at.isoformat() if claimed.timeout_at else None,
            ),
        )
        effective_run_spec = claimed.run_spec
        effective_strategy_name = (
            effective_run_spec.strategy_name
            if effective_run_spec is not None
            else strategy_name
        )
        effective_engine_family = (
            effective_run_spec.engine_family
            if effective_run_spec is not None
            else EngineFamily.VECTORBT
        )
        effective_config_override = _resolve_config_override(
            claimed,
            fallback=config_override,
        )

        loop = asyncio.get_running_loop()

        def progress_callback(status: str, elapsed: float) -> None:
            _ = elapsed
            asyncio.run_coroutine_threadsafe(
                resolved_manager.update_job_status(
                    job_id,
                    JobStatus.RUNNING,
                    message=status,
                ),
                loop,
            )

        heartbeat_task = asyncio.create_task(
            _heartbeat_loop(
                resolved_manager,
                job_id,
                lease_owner=lease_owner,
                heartbeat_seconds=normalized_heartbeat_seconds(heartbeat_seconds),
                exit_on_cancel=exit_on_cancel,
            )
        )

        if effective_engine_family == EngineFamily.NAUTILUS:
            if effective_run_spec is None:
                raise RuntimeError("Persisted run_spec is required for Nautilus verification.")
            result = await asyncio.to_thread(
                resolved_nautilus_runner.execute,
                strategy=effective_strategy_name,
                run_spec=effective_run_spec,
                run_id=job_id,
                progress_callback=progress_callback,
                config_override=effective_config_override,
            )
        elif effective_engine_family in (EngineFamily.VECTORBT, EngineFamily.UNKNOWN):
            result = await asyncio.to_thread(
                resolved_runner.execute,
                strategy=effective_strategy_name,
                progress_callback=progress_callback,
                config_override=effective_config_override,
            )
        else:
            raise ValueError(f"Unsupported backtest engine: {effective_engine_family}")
        current_job = await resolved_manager.reload_job_from_storage(job_id)
        if current_job is not None:
            exit_code = terminal_worker_exit_code(current_job.status, current_job.error)
            if exit_code is not None:
                return exit_code
        summary = _extract_result_summary(result)
        await resolved_manager.set_job_result(
            job_id=job_id,
            result_summary=summary,
            raw_result=result.summary,
            html_path=str(result.html_path) if result.html_path else None,
            dataset_name=result.dataset_name,
            execution_time=result.elapsed_time,
        )
        await resolved_manager.update_job_status(
            job_id,
            JobStatus.COMPLETED,
            message="バックテスト完了",
            progress=1.0,
        )
        duration_ms = record_elapsed_job_duration("backtest", JobStatus.COMPLETED.value, started_at=started_at)
        logger.info(
            f"backtest worker completed job: {job_id}",
            **external_worker_lifecycle_fields(
                "backtest",
                job_id,
                JobStatus.COMPLETED.value,
                lease_owner=lease_owner,
                durationMs=duration_ms,
            ),
        )
        return 0
    except Exception as exc:
        duration_ms = record_elapsed_job_duration("backtest", JobStatus.FAILED.value, started_at=started_at)
        logger.exception(
            f"backtest worker failed: {job_id}",
            **external_worker_lifecycle_fields(
                "backtest",
                job_id,
                JobStatus.FAILED.value,
                lease_owner=lease_owner,
                durationMs=duration_ms,
            ),
        )
        await resolved_manager.update_job_status(
            job_id,
            JobStatus.FAILED,
            message="バックテストに失敗しました",
            error=str(exc),
        )
        return 1
    finally:
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task
        if portfolio_db is not None:
            portfolio_db.close()
        if owns_portfolio_db and resolved_manager is not None:
            resolved_manager.set_portfolio_db(None)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a durable backtest worker")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--strategy-name", required=True)
    parser.add_argument("--config-override-json")
    parser.add_argument("--timeout-seconds", type=int)
    return parser.parse_args()


def _resolve_config_override(
    job: Any,
    *,
    fallback: dict[str, Any] | None,
) -> dict[str, Any] | None:
    run_spec = getattr(job, "run_spec", None)
    if run_spec is not None and isinstance(getattr(run_spec, "parameters", None), dict):
        persisted = run_spec.parameters.get("config_override")
        if isinstance(persisted, dict):
            return persisted
    return fallback


def main() -> int:
    args = _parse_args()
    config_override: dict[str, Any] | None = None
    if args.config_override_json:
        config_override = parse_json_object_arg(args.config_override_json, label="config override")
    return asyncio.run(
        run_backtest_worker(
            args.job_id,
            args.strategy_name,
            config_override=config_override,
            timeout_seconds=args.timeout_seconds,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
