"""Out-of-process backtest worker."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
from contextlib import suppress
from datetime import datetime
from time import perf_counter
from typing import Any, Callable

from loguru import logger

from src.application.services.backtest_result_summary import resolve_backtest_result_summary
from src.application.services.job_manager import JobManager
from src.domains.backtest.core.runner import BacktestResult, BacktestRunner
from src.entrypoints.http.schemas.backtest import BacktestResultSummary, JobStatus
from src.infrastructure.db.market.portfolio_db import PortfolioDb
from src.shared.config.settings import get_settings
from src.shared.observability.metrics import metrics_recorder

_HEARTBEAT_SECONDS = 5.0
_TIMED_OUT_ERROR = "worker_timed_out"
_TERMINAL_STATUSES = (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)


def _duration_ms_for_job(now: datetime, *, started_at: datetime | None, created_at: datetime | None) -> float:
    reference = started_at or created_at or now
    return round(max((now - reference).total_seconds(), 0.0) * 1000, 2)


def _extract_result_summary(result: BacktestResult) -> BacktestResultSummary:
    summary = resolve_backtest_result_summary(
        html_path=result.html_path,
        fallback=result.summary,
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
        html_path=str(result.html_path),
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
        if job.status in _TERMINAL_STATUSES:
            return
        if job.timeout_at is not None and datetime.now() >= job.timeout_at:
            now = datetime.now()
            duration_ms = _duration_ms_for_job(now, started_at=job.started_at, created_at=job.created_at)
            await manager.update_job_status(
                job_id,
                JobStatus.FAILED,
                message="バックテストがタイムアウトしました",
                error=_TIMED_OUT_ERROR,
            )
            metrics_recorder.record_job_duration("backtest", JobStatus.FAILED.value, duration_ms)
            logger.warning(
                f"backtest worker timed out: {job_id}",
                event="job_lifecycle",
                jobType="backtest",
                jobId=job_id,
                status=JobStatus.FAILED.value,
                error=_TIMED_OUT_ERROR,
                durationMs=duration_ms,
                leaseOwner=lease_owner,
                executionMode="external_worker",
            )
            exit_on_cancel(124)
            return
        if job.cancel_requested_at is not None:
            now = datetime.now()
            duration_ms = _duration_ms_for_job(now, started_at=job.started_at, created_at=job.created_at)
            await manager.cancel_job(
                job_id,
                reason=job.cancel_reason or "controller_requested",
            )
            metrics_recorder.record_job_duration("backtest", JobStatus.CANCELLED.value, duration_ms)
            logger.info(
                f"backtest worker cancelled: {job_id}",
                event="job_lifecycle",
                jobType="backtest",
                jobId=job_id,
                status=JobStatus.CANCELLED.value,
                durationMs=duration_ms,
                leaseOwner=lease_owner,
                executionMode="external_worker",
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
    heartbeat_seconds: float = _HEARTBEAT_SECONDS,
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
    lease_owner = f"backtest-worker:{socket.gethostname()}:{os.getpid()}"

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
            event="job_lifecycle",
            jobType="backtest",
            jobId=job_id,
            status=JobStatus.RUNNING.value,
            leaseOwner=lease_owner,
            timeoutAt=claimed.timeout_at.isoformat() if claimed.timeout_at else None,
            executionMode="external_worker",
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
                heartbeat_seconds=max(heartbeat_seconds, 0.1),
                exit_on_cancel=exit_on_cancel,
            )
        )

        result = await asyncio.to_thread(
            resolved_runner.execute,
            strategy=strategy_name,
            progress_callback=progress_callback,
            config_override=config_override,
        )
        current_job = await resolved_manager.reload_job_from_storage(job_id)
        if current_job is not None and current_job.status in _TERMINAL_STATUSES:
            if current_job.error == _TIMED_OUT_ERROR:
                return 124
            return 0 if current_job.status == JobStatus.CANCELLED else 1
        summary = _extract_result_summary(result)
        await resolved_manager.set_job_result(
            job_id=job_id,
            result_summary=summary,
            raw_result=result.summary,
            html_path=str(result.html_path),
            dataset_name=result.dataset_name,
            execution_time=result.elapsed_time,
        )
        await resolved_manager.update_job_status(
            job_id,
            JobStatus.COMPLETED,
            message="バックテスト完了",
            progress=1.0,
        )
        duration_ms = round((perf_counter() - started_at) * 1000, 2)
        metrics_recorder.record_job_duration("backtest", JobStatus.COMPLETED.value, duration_ms)
        logger.info(
            f"backtest worker completed job: {job_id}",
            event="job_lifecycle",
            jobType="backtest",
            jobId=job_id,
            status=JobStatus.COMPLETED.value,
            durationMs=duration_ms,
            leaseOwner=lease_owner,
            executionMode="external_worker",
        )
        return 0
    except Exception as exc:
        duration_ms = round((perf_counter() - started_at) * 1000, 2)
        metrics_recorder.record_job_duration("backtest", JobStatus.FAILED.value, duration_ms)
        logger.exception(
            f"backtest worker failed: {job_id}",
            event="job_lifecycle",
            jobType="backtest",
            jobId=job_id,
            status=JobStatus.FAILED.value,
            durationMs=duration_ms,
            leaseOwner=lease_owner,
            executionMode="external_worker",
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


def main() -> int:
    args = _parse_args()
    config_override: dict[str, Any] | None = None
    if args.config_override_json:
        parsed = json.loads(args.config_override_json)
        if not isinstance(parsed, dict):
            raise ValueError("config override must be a JSON object")
        config_override = parsed
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
