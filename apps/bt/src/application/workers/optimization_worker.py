"""Out-of-process optimization worker."""

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

from src.application.services.job_manager import JobManager
from src.application.services.verification_orchestrator import (
    INTERNAL_VERIFICATION_CANDIDATES_KEY,
    INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY,
    build_canonical_metrics,
    build_verification_seed,
    cancel_verification_children,
    extract_candidate_seeds,
    run_verification_orchestrator,
    serialize_candidate_seeds,
    strip_verification_metadata,
)
from src.domains.backtest.contracts import EnginePolicy, EnginePolicyMode
from src.domains.optimization.engine import ParameterOptimizationEngine
from src.entrypoints.http.schemas.backtest import JobStatus
from src.infrastructure.db.market.portfolio_db import PortfolioDb
from src.shared.config.settings import get_settings
from src.shared.observability.metrics import metrics_recorder

_HEARTBEAT_SECONDS = 5.0
_TIMED_OUT_ERROR = "worker_timed_out"
_TERMINAL_STATUSES = (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)


def _duration_ms_for_job(now: datetime, *, started_at: datetime | None, created_at: datetime | None) -> float:
    reference = started_at or created_at or now
    return round(max((now - reference).total_seconds(), 0.0) * 1000, 2)


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
    verification_candidates = []
    for rank, result in enumerate(opt_result.all_results, start=1):
        params = result.get("params") or {}
        fast_metrics = build_canonical_metrics(result.get("metric_values"))
        candidate_id = f"grid_{rank:04d}"
        if rank <= 10:
            fast_candidates.append(
                {
                    "candidate_id": candidate_id,
                    "rank": rank,
                    "score": float(result.get("score", 0.0)),
                    "metrics": fast_metrics.model_dump(mode="json") if fast_metrics is not None else None,
                }
            )
        verification_candidates.append(
            build_verification_seed(
                candidate_id=candidate_id,
                fast_rank=rank,
                fast_score=float(result.get("score", 0.0)),
                fast_metrics=fast_metrics,
                strategy_name=strategy_name,
                config_override=engine.build_config_override(params),
            ).model_dump(mode="json")
        )

    return {
        "best_score": best_score,
        "best_params": best_params,
        "worst_score": worst_score,
        "worst_params": worst_params,
        "total_combinations": total_combinations,
        "html_path": html_path,
        "fast_candidates": fast_candidates,
        INTERNAL_VERIFICATION_CANDIDATES_KEY: verification_candidates,
        INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY: dict(opt_result.scoring_weights),
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
        if job.status in _TERMINAL_STATUSES:
            return
        if job.timeout_at is not None and datetime.now() >= job.timeout_at:
            now = datetime.now()
            duration_ms = _duration_ms_for_job(now, started_at=job.started_at, created_at=job.created_at)
            await manager.update_job_status(
                job_id,
                JobStatus.FAILED,
                message="最適化がタイムアウトしました",
                error=_TIMED_OUT_ERROR,
            )
            await cancel_verification_children(
                manager,
                job_id,
                reason="parent_job_timed_out",
            )
            metrics_recorder.record_job_duration("optimization", JobStatus.FAILED.value, duration_ms)
            logger.warning(
                f"optimization worker timed out: {job_id}",
                event="job_lifecycle",
                jobType="optimization",
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
            await cancel_verification_children(
                manager,
                job_id,
                reason=job.cancel_reason or "controller_requested",
            )
            metrics_recorder.record_job_duration("optimization", JobStatus.CANCELLED.value, duration_ms)
            logger.info(
                f"optimization worker cancelled: {job_id}",
                event="job_lifecycle",
                jobType="optimization",
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


async def run_optimization_worker(
    job_id: str,
    strategy_name: str,
    *,
    manager: JobManager | None = None,
    execute_sync: Callable[[str], dict[str, Any]] = _execute_optimization_sync,
    engine_policy: EnginePolicy | None = None,
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

    lease_owner = f"optimization-worker:{socket.gethostname()}:{os.getpid()}"
    heartbeat_task: asyncio.Task[None] | None = None
    started_at = perf_counter()
    resolved_engine_policy = engine_policy or EnginePolicy()
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
            event="job_lifecycle",
            jobType="optimization",
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

        result = await asyncio.to_thread(execute_sync, strategy_name)
        candidate_seeds = extract_candidate_seeds(result)
        requested_top_k = resolved_engine_policy.verification_top_k or 0
        if resolved_engine_policy.mode == EnginePolicyMode.FAST_THEN_VERIFY:
            candidate_seeds = candidate_seeds[:requested_top_k]
            result = serialize_candidate_seeds(
                result,
                candidate_seeds,
                requested_top_k=requested_top_k,
                scoring_weights=result.get(INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY),
            )
        else:
            result = strip_verification_metadata(result)
        current_job = await resolved_manager.reload_job_from_storage(job_id)
        if current_job is not None and current_job.status in _TERMINAL_STATUSES:
            if current_job.error == _TIMED_OUT_ERROR:
                return 124
            return 0 if current_job.status == JobStatus.CANCELLED else 1
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
        if resolved_engine_policy.mode == EnginePolicyMode.FAST_THEN_VERIFY:
            await resolved_manager.update_job_status(
                job_id,
                JobStatus.RUNNING,
                message="Fast path complete, starting Nautilus verification...",
                progress=0.5,
            )
            result, verification = await run_verification_orchestrator(
                resolved_manager,
                parent_job_id=job_id,
                raw_result=result,
                candidate_seeds=candidate_seeds,
                requested_top_k=requested_top_k,
                status_message_prefix="Nautilus verification",
                strategy_label=strategy_name,
                scoring_weights=result.get(INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY),
            )
            result["verification"] = verification.model_dump(mode="json")
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
        duration_ms = round((perf_counter() - started_at) * 1000, 2)
        metrics_recorder.record_job_duration("optimization", JobStatus.COMPLETED.value, duration_ms)
        logger.info(
            f"optimization worker completed job: {job_id}",
            event="job_lifecycle",
            jobType="optimization",
            jobId=job_id,
            status=JobStatus.COMPLETED.value,
            durationMs=duration_ms,
            leaseOwner=lease_owner,
            executionMode="external_worker",
        )
        return 0
    except Exception as exc:
        await cancel_verification_children(
            resolved_manager,
            job_id,
            reason="parent_job_failed",
        )
        duration_ms = round((perf_counter() - started_at) * 1000, 2)
        metrics_recorder.record_job_duration("optimization", JobStatus.FAILED.value, duration_ms)
        logger.exception(
            f"optimization worker failed: {job_id}",
            event="job_lifecycle",
            jobType="optimization",
            jobId=job_id,
            status=JobStatus.FAILED.value,
            durationMs=duration_ms,
            leaseOwner=lease_owner,
            executionMode="external_worker",
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
    parser.add_argument("--engine-policy-json")
    parser.add_argument("--timeout-seconds", type=int)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    engine_policy = EnginePolicy()
    if args.engine_policy_json:
        parsed = json.loads(args.engine_policy_json)
        if not isinstance(parsed, dict):
            raise ValueError("engine policy payload must be a JSON object")
        engine_policy = EnginePolicy.model_validate(parsed)
    return asyncio.run(
        run_optimization_worker(
            args.job_id,
            args.strategy_name,
            engine_policy=engine_policy,
            timeout_seconds=args.timeout_seconds,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
