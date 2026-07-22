"""Out-of-process optimization worker."""

from __future__ import annotations

import argparse
import asyncio
import os
from contextlib import suppress
from datetime import datetime
from time import perf_counter
from typing import Any, Callable

from loguru import logger

from src.application.services.job_status import TERMINAL_JOB_STATUSES
from src.application.services.job_manager import JobManager
from src.application.services.run_contracts import build_canonical_metrics_from_payload
from src.domains.optimization.engine import ParameterOptimizationEngine
from src.application.contracts.jobs import JobStatus
from src.infrastructure.db.market.portfolio_db import PortfolioDb
from src.application.workers.job_runtime import (
    DEFAULT_HEARTBEAT_SECONDS,
    WORKER_TIMED_OUT_ERROR,
    duration_ms_for_loaded_job,
    external_worker_lifecycle_fields,
    normalized_heartbeat_seconds,
    record_elapsed_job_duration,
    record_job_duration,
    terminal_worker_exit_code,
    worker_cancel_reason,
    worker_lease_owner,
)
from src.shared.config.settings import get_settings


def _execute_optimization_sync(strategy_name: str) -> dict[str, Any]:
    engine = ParameterOptimizationEngine(strategy_name=strategy_name)
    opt_result = engine.optimize()

    best_score = opt_result.best_score
    best_params = opt_result.best_params
    total_combinations = len(opt_result.all_results) if opt_result.all_results else 0
    worst_result = opt_result.all_results[-1] if opt_result.all_results else None
    worst_score = worst_result.get("score") if worst_result else None
    worst_params = worst_result.get("params") if worst_result else None
    html_path = str(opt_result.html_path) if opt_result.html_path else None
    fast_candidates = []
    for rank, result in enumerate(opt_result.all_results, start=1):
        if rank > 10:
            break
        metrics = build_canonical_metrics_from_payload(result.get("metric_values"))
        fast_candidates.append(
            {
                "candidate_id": f"grid_{rank:04d}",
                "rank": rank,
                "score": float(result.get("score", 0.0)),
                "metrics": metrics.model_dump(mode="json") if metrics else None,
            }
        )

    return {
        "best_score": best_score,
        "best_params": best_params,
        "worst_score": worst_score,
        "worst_params": worst_params,
        "total_combinations": total_combinations,
        "html_path": html_path,
        "fast_candidates": fast_candidates,
    }


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
                message="最適化がタイムアウトしました",
                error=WORKER_TIMED_OUT_ERROR,
            )
            record_job_duration("optimization", JobStatus.FAILED.value, duration_ms)
            logger.warning(
                f"optimization worker timed out: {job_id}",
                error=WORKER_TIMED_OUT_ERROR,
                **external_worker_lifecycle_fields(
                    "optimization",
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
            cancel_reason = worker_cancel_reason(job.cancel_reason)
            await manager.cancel_job(
                job_id,
                reason=cancel_reason,
            )
            record_job_duration("optimization", JobStatus.CANCELLED.value, duration_ms)
            logger.info(
                f"optimization worker cancelled: {job_id}",
                **external_worker_lifecycle_fields(
                    "optimization",
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


async def run_optimization_worker(
    job_id: str,
    strategy_name: str,
    *,
    manager: JobManager | None = None,
    execute_sync: Callable[[str], dict[str, Any]] = _execute_optimization_sync,
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

    lease_owner = worker_lease_owner("optimization-worker")
    heartbeat_task: asyncio.Task[None] | None = None
    started_at = perf_counter()
    try:
        claimed = await resolved_manager.claim_job_execution(
            job_id,
            lease_owner=lease_owner,
            lease_seconds=resolved_manager.default_lease_seconds,
            timeout_seconds=timeout_seconds,
            message="最適化を開始しています...",
            progress=0.0,
        )
        if claimed is None:
            logger.error(f"optimization worker could not claim job: {job_id}")
            return 2
        logger.info(
            f"optimization worker claimed job: {job_id}",
            **external_worker_lifecycle_fields(
                "optimization",
                job_id,
                JobStatus.RUNNING.value,
                lease_owner=lease_owner,
                timeoutAt=claimed.timeout_at.isoformat() if claimed.timeout_at else None,
            ),
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

        result = await asyncio.to_thread(execute_sync, strategy_name)
        current_job = await resolved_manager.reload_job_from_storage(job_id)
        if current_job is not None:
            exit_code = terminal_worker_exit_code(current_job.status, current_job.error)
            if exit_code is not None:
                return exit_code
        await resolved_manager.set_job_optimization_result(
            job_id,
            raw_result=result,
            best_score=result.get("best_score"),
            best_params=result.get("best_params"),
            worst_score=result.get("worst_score"),
            worst_params=result.get("worst_params"),
            total_combinations=result.get("total_combinations"),
            html_path=result.get("html_path"),
        )
        await resolved_manager.update_job_status(
            job_id,
            JobStatus.COMPLETED,
            message="最適化完了",
            progress=1.0,
        )
        duration_ms = record_elapsed_job_duration("optimization", JobStatus.COMPLETED.value, started_at=started_at)
        logger.info(
            f"optimization worker completed job: {job_id}",
            **external_worker_lifecycle_fields(
                "optimization",
                job_id,
                JobStatus.COMPLETED.value,
                lease_owner=lease_owner,
                durationMs=duration_ms,
            ),
        )
        return 0
    except Exception as exc:
        duration_ms = record_elapsed_job_duration("optimization", JobStatus.FAILED.value, started_at=started_at)
        logger.exception(
            f"optimization worker failed: {job_id}",
            **external_worker_lifecycle_fields(
                "optimization",
                job_id,
                JobStatus.FAILED.value,
                lease_owner=lease_owner,
                durationMs=duration_ms,
            ),
        )
        await resolved_manager.update_job_status(
            job_id,
            JobStatus.FAILED,
            message="最適化に失敗しました",
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
    parser = argparse.ArgumentParser(description="Run a durable optimization worker")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--strategy-name", required=True)
    parser.add_argument("--timeout-seconds", type=int)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    return asyncio.run(
        run_optimization_worker(
            args.job_id,
            args.strategy_name,
            timeout_seconds=args.timeout_seconds,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
