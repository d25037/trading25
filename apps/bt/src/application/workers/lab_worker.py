"""Out-of-process lab worker."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
from contextlib import suppress
from datetime import datetime
from time import perf_counter
from typing import Any, Callable, cast

from loguru import logger

from src.application.services.job_manager import JobManager
from src.application.services.lab_service import (
    _INTERNAL_JOB_MESSAGE_KEY,
    _LAB_JOB_MESSAGES,
    LabService,
)
from src.entrypoints.http.schemas.backtest import JobStatus
from src.infrastructure.db.market.portfolio_db import PortfolioDb
from src.shared.config.settings import get_settings
from src.domains.lab_agent.models import LabStructureMode, LabTargetScope
from src.shared.observability.metrics import metrics_recorder

_HEARTBEAT_SECONDS = 5.0
_TIMED_OUT_ERROR = "worker_timed_out"
_TERMINAL_STATUSES = (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)


def _duration_ms_for_job(now: datetime, *, started_at: datetime | None, created_at: datetime | None) -> float:
    reference = started_at or created_at or now
    return round(max((now - reference).total_seconds(), 0.0) * 1000, 2)


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
            lab_type = job.job_type.removeprefix("lab_")
            timeout_message = _LAB_JOB_MESSAGES.get(lab_type, {}).get(
                "timeout",
                "Labジョブがタイムアウトしました",
            )
            await manager.update_job_status(
                job_id,
                JobStatus.FAILED,
                message=timeout_message,
                error=_TIMED_OUT_ERROR,
            )
            metrics_recorder.record_job_duration(job.job_type, JobStatus.FAILED.value, duration_ms)
            logger.warning(
                f"lab worker timed out: {job_id} ({lab_type})",
                event="job_lifecycle",
                jobType=job.job_type,
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
            metrics_recorder.record_job_duration(job.job_type, JobStatus.CANCELLED.value, duration_ms)
            logger.info(
                f"lab worker cancelled: {job_id} ({job.job_type.removeprefix('lab_')})",
                event="job_lifecycle",
                jobType=job.job_type,
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


async def _execute_lab_payload(
    service: LabService,
    manager: JobManager,
    job_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    lab_type = str(payload["lab_type"])
    if lab_type == "generate":
        return await asyncio.to_thread(
            service._execute_generate_sync,
            int(payload["count"]),
            int(payload["top"]),
            payload.get("seed"),
            bool(payload["save"]),
            str(payload["direction"]),
            str(payload["timeframe"]),
            str(payload["dataset"]),
            bool(payload.get("entry_filter_only", False)),
            list(payload.get("allowed_categories") or []),
        )
    if lab_type == "evolve":
        structure_mode = cast(LabStructureMode, payload["structure_mode"])
        target_scope = cast(LabTargetScope, payload.get("target_scope", "both"))
        return await asyncio.to_thread(
            service._execute_evolve_sync,
            str(payload["strategy_name"]),
            int(payload["generations"]),
            int(payload["population"]),
            structure_mode,
            int(payload["random_add_entry_signals"]),
            int(payload["random_add_exit_signals"]),
            payload.get("seed"),
            bool(payload["save"]),
            bool(payload.get("entry_filter_only", False)),
            list(payload.get("allowed_categories") or []),
            target_scope,
        )
    if lab_type == "optimize":
        structure_mode = cast(LabStructureMode, payload["structure_mode"])
        target_scope = cast(LabTargetScope, payload.get("target_scope", "both"))
        loop = asyncio.get_running_loop()

        def progress_callback(completed: int, total: int, best_score: float) -> None:
            progress = completed / total if total > 0 else 0.0
            message = f"Trial {completed}/{total} 完了 (best: {best_score:.4f})"
            asyncio.run_coroutine_threadsafe(
                manager.update_job_status(
                    job_id,
                    JobStatus.RUNNING,
                    message=message,
                    progress=progress,
                ),
                loop,
            )

        return await asyncio.to_thread(
            service._execute_optimize_sync,
            str(payload["strategy_name"]),
            int(payload["trials"]),
            str(payload["sampler"]),
            structure_mode,
            int(payload["random_add_entry_signals"]),
            int(payload["random_add_exit_signals"]),
            payload.get("seed"),
            bool(payload["save"]),
            bool(payload.get("entry_filter_only", False)),
            list(payload.get("allowed_categories") or []),
            payload.get("scoring_weights"),
            progress_callback,
            target_scope,
        )
    if lab_type == "improve":
        return await asyncio.to_thread(
            service._execute_improve_sync,
            str(payload["strategy_name"]),
            bool(payload["auto_apply"]),
            bool(payload.get("entry_filter_only", False)),
            list(payload.get("allowed_categories") or []),
        )
    raise ValueError(f"unsupported lab_type: {lab_type}")


async def run_lab_worker(
    job_id: str,
    payload: dict[str, Any],
    *,
    manager: JobManager | None = None,
    service: LabService | None = None,
    heartbeat_seconds: float = _HEARTBEAT_SECONDS,
    timeout_seconds: int | None = None,
    exit_on_cancel: Callable[[int], None] = os._exit,
) -> int:
    lab_type = str(payload["lab_type"])
    messages = _LAB_JOB_MESSAGES[lab_type]
    owns_portfolio_db = False
    portfolio_db: PortfolioDb | None = None
    resolved_manager = manager
    if resolved_manager is None:
        settings = get_settings()
        portfolio_db = PortfolioDb(settings.portfolio_db_path)
        resolved_manager = JobManager()
        resolved_manager.set_portfolio_db(portfolio_db)
        owns_portfolio_db = True
    resolved_service = service or LabService(manager=resolved_manager, max_workers=1)
    lease_owner = f"lab-worker:{socket.gethostname()}:{os.getpid()}"
    heartbeat_task: asyncio.Task[None] | None = None
    started_at = perf_counter()
    try:
        claimed = await resolved_manager.claim_job_execution(
            job_id,
            lease_owner=lease_owner,
            lease_seconds=resolved_manager.default_lease_seconds,
            timeout_seconds=timeout_seconds,
            message=messages["start"],
            progress=0.0,
        )
        if claimed is None:
            logger.error(f"lab worker could not claim job: {job_id}")
            return 2
        logger.info(
            f"lab worker claimed job: {job_id} ({lab_type})",
            event="job_lifecycle",
            jobType=claimed.job_type,
            jobId=job_id,
            status=JobStatus.RUNNING.value,
            leaseOwner=lease_owner,
            timeoutAt=claimed.timeout_at.isoformat() if claimed.timeout_at else None,
            executionMode="external_worker",
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
        result = await _execute_lab_payload(
            resolved_service,
            resolved_manager,
            job_id,
            payload,
        )
        current_job = await resolved_manager.reload_job_from_storage(job_id)
        if current_job is not None and current_job.status in _TERMINAL_STATUSES:
            if current_job.error == _TIMED_OUT_ERROR:
                return 124
            return 0 if current_job.status == JobStatus.CANCELLED else 1
        complete_message = messages["complete"]
        persisted_result = result
        if isinstance(result, dict):
            persisted_result = dict(result)
            raw_message = persisted_result.pop(_INTERNAL_JOB_MESSAGE_KEY, None)
            if isinstance(raw_message, str) and raw_message.strip():
                complete_message = raw_message
        await resolved_manager.set_job_raw_result(job_id, persisted_result)
        await resolved_manager.update_job_status(
            job_id,
            JobStatus.COMPLETED,
            message=complete_message,
            progress=1.0,
        )
        duration_ms = round((perf_counter() - started_at) * 1000, 2)
        metrics_recorder.record_job_duration(claimed.job_type, JobStatus.COMPLETED.value, duration_ms)
        logger.info(
            f"lab worker completed job: {job_id} ({lab_type})",
            event="job_lifecycle",
            jobType=claimed.job_type,
            jobId=job_id,
            status=JobStatus.COMPLETED.value,
            durationMs=duration_ms,
            leaseOwner=lease_owner,
            executionMode="external_worker",
        )
        return 0
    except Exception as exc:
        duration_ms = round((perf_counter() - started_at) * 1000, 2)
        metrics_recorder.record_job_duration(f"lab_{lab_type}", JobStatus.FAILED.value, duration_ms)
        logger.exception(
            f"lab worker failed: {job_id} ({lab_type})",
            event="job_lifecycle",
            jobType=f"lab_{lab_type}",
            jobId=job_id,
            status=JobStatus.FAILED.value,
            durationMs=duration_ms,
            leaseOwner=lease_owner,
            executionMode="external_worker",
        )
        await resolved_manager.update_job_status(
            job_id,
            JobStatus.FAILED,
            message=messages["fail"],
            error=str(exc),
        )
        return 1
    finally:
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task
        executor = resolved_service._executor
        if not bool(getattr(executor, "_shutdown", False)):
            executor.shutdown(wait=False)
        if portfolio_db is not None:
            portfolio_db.close()
        if owns_portfolio_db and resolved_manager is not None:
            resolved_manager.set_portfolio_db(None)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a durable lab worker")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--payload-json", required=True)
    parser.add_argument("--timeout-seconds", type=int)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    payload = json.loads(args.payload_json)
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object")
    return asyncio.run(
        run_lab_worker(
            args.job_id,
            payload,
            timeout_seconds=args.timeout_seconds,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
