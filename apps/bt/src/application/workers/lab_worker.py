"""Out-of-process lab worker."""

from __future__ import annotations

import argparse
import asyncio
import os
from contextlib import suppress
from datetime import datetime
from time import perf_counter
from typing import Any, Callable, cast

from loguru import logger

from src.application.services.job_status import TERMINAL_JOB_STATUSES
from src.application.services.job_manager import JobManager
from src.application.services.lab_service import (
    _INTERNAL_JOB_MESSAGE_KEY,
    _LAB_JOB_MESSAGES,
    LabService,
)
from src.application.contracts.jobs import JobStatus
from src.infrastructure.db.market.portfolio_db import PortfolioDb
from src.shared.config.settings import get_settings
from src.domains.lab_agent.models import LabStructureMode, LabTargetScope
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
    worker_cancel_reason,
    worker_lease_owner,
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
            lab_type = job.job_type.removeprefix("lab_")
            timeout_message = _LAB_JOB_MESSAGES.get(lab_type, {}).get(
                "timeout",
                "Labジョブがタイムアウトしました",
            )
            await manager.update_job_status(
                job_id,
                JobStatus.FAILED,
                message=timeout_message,
                error=WORKER_TIMED_OUT_ERROR,
            )
            record_job_duration(job.job_type, JobStatus.FAILED.value, duration_ms)
            logger.warning(
                f"lab worker timed out: {job_id} ({lab_type})",
                error=WORKER_TIMED_OUT_ERROR,
                **external_worker_lifecycle_fields(
                    job.job_type,
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
            record_job_duration(job.job_type, JobStatus.CANCELLED.value, duration_ms)
            logger.info(
                f"lab worker cancelled: {job_id} ({job.job_type.removeprefix('lab_')})",
                **external_worker_lifecycle_fields(
                    job.job_type,
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


async def _execute_lab_payload(
    service: LabService,
    manager: JobManager,
    job_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    lab_type = str(payload["lab_type"])
    save = bool(payload.get("save", False))
    if lab_type == "generate":
        return await asyncio.to_thread(
            service._execute_generate_sync,
            int(payload["count"]),
            int(payload["top"]),
            payload.get("seed"),
            save,
            str(payload["direction"]),
            str(payload["timeframe"]),
            payload.get("universe_preset"),
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
            save,
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
            save,
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
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
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
    lease_owner = worker_lease_owner("lab-worker")
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
            **external_worker_lifecycle_fields(
                claimed.job_type,
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
        result = await _execute_lab_payload(
            resolved_service,
            resolved_manager,
            job_id,
            payload,
        )
        current_job = await resolved_manager.reload_job_from_storage(job_id)
        if current_job is not None:
            exit_code = terminal_worker_exit_code(current_job.status, current_job.error)
            if exit_code is not None:
                return exit_code
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
        duration_ms = record_elapsed_job_duration(claimed.job_type, JobStatus.COMPLETED.value, started_at=started_at)
        logger.info(
            f"lab worker completed job: {job_id} ({lab_type})",
            **external_worker_lifecycle_fields(
                claimed.job_type,
                job_id,
                JobStatus.COMPLETED.value,
                lease_owner=lease_owner,
                durationMs=duration_ms,
            ),
        )
        return 0
    except Exception as exc:
        duration_ms = record_elapsed_job_duration(f"lab_{lab_type}", JobStatus.FAILED.value, started_at=started_at)
        logger.exception(
            f"lab worker failed: {job_id} ({lab_type})",
            **external_worker_lifecycle_fields(
                f"lab_{lab_type}",
                job_id,
                JobStatus.FAILED.value,
                lease_owner=lease_owner,
                durationMs=duration_ms,
            ),
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
    payload = parse_json_object_arg(args.payload_json, label="payload")
    return asyncio.run(
        run_lab_worker(
            args.job_id,
            payload,
            timeout_seconds=args.timeout_seconds,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
