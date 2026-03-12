"""Shared verification orchestration for optimize and lab jobs."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from pydantic import BaseModel, Field

from src.application.services.job_manager import JobInfo, JobManager
from src.application.services.run_contracts import build_config_override_run_spec
from src.application.workers.backtest_worker import run_backtest_worker
from src.domains.backtest.contracts import (
    CanonicalExecutionMetrics,
    EngineFamily,
    VerificationCandidateStatus,
    VerificationCandidateSummary,
    VerificationDelta,
    VerificationOverallStatus,
    VerificationSummary,
)
from src.domains.lab_agent.models import StrategyCandidate
from src.entrypoints.http.schemas.backtest import JobStatus

INTERNAL_VERIFICATION_CANDIDATES_KEY = "_verification_candidates"
INTERNAL_VERIFICATION_REQUESTED_TOP_K_KEY = "_verification_requested_top_k"
INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY = "_verification_scoring_weights"

_VERIFICATION_PROGRESS_OFFSET = 0.5
_VERIFICATION_PROGRESS_WEIGHT = 0.5


class VerificationCandidateSeed(BaseModel):
    """Internal durable payload needed to execute verification."""

    candidate_id: str = Field(description="Stable candidate identifier")
    fast_rank: int = Field(description="1-based fast rank")
    fast_score: float = Field(description="Fast-path score")
    fast_metrics: CanonicalExecutionMetrics | None = Field(default=None)
    strategy_name: str = Field(description="Base strategy used for child verification")
    config_override: dict[str, Any] | None = Field(default=None)
    verification_run_id: str | None = Field(default=None)
    strategy_candidate: StrategyCandidate | None = Field(default=None)


def build_canonical_metrics(payload: dict[str, Any] | None) -> CanonicalExecutionMetrics | None:
    """Build canonical metrics from a plain dictionary."""
    if not isinstance(payload, dict):
        return None

    values = {
        "total_return": payload.get("total_return"),
        "sharpe_ratio": payload.get("sharpe_ratio"),
        "sortino_ratio": payload.get("sortino_ratio"),
        "calmar_ratio": payload.get("calmar_ratio"),
        "max_drawdown": payload.get("max_drawdown"),
        "win_rate": payload.get("win_rate"),
        "trade_count": payload.get("trade_count"),
    }
    if not any(value is not None for value in values.values()):
        return None
    return CanonicalExecutionMetrics.model_validate(values)


def build_verification_seed(
    *,
    candidate_id: str,
    fast_rank: int,
    fast_score: float,
    fast_metrics: CanonicalExecutionMetrics | None,
    strategy_name: str,
    config_override: dict[str, Any] | None,
    strategy_candidate: StrategyCandidate | None = None,
) -> VerificationCandidateSeed:
    """Create a durable verification seed."""
    resolved_strategy_candidate: StrategyCandidate | None = None
    if isinstance(strategy_candidate, StrategyCandidate):
        resolved_strategy_candidate = strategy_candidate
    elif isinstance(strategy_candidate, dict):
        try:
            resolved_strategy_candidate = StrategyCandidate.model_validate(strategy_candidate)
        except Exception:
            resolved_strategy_candidate = None

    return VerificationCandidateSeed(
        candidate_id=candidate_id,
        fast_rank=fast_rank,
        fast_score=fast_score,
        fast_metrics=fast_metrics,
        strategy_name=strategy_name,
        config_override=deepcopy(config_override),
        strategy_candidate=deepcopy(resolved_strategy_candidate),
    )


def serialize_candidate_seeds(
    raw_result: dict[str, Any],
    seeds: list[VerificationCandidateSeed],
    *,
    requested_top_k: int,
    scoring_weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Attach internal verification metadata to a raw_result payload."""
    updated = dict(raw_result)
    updated[INTERNAL_VERIFICATION_CANDIDATES_KEY] = [
        seed.model_dump(mode="json")
        for seed in seeds
    ]
    updated[INTERNAL_VERIFICATION_REQUESTED_TOP_K_KEY] = requested_top_k
    if scoring_weights:
        updated[INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY] = deepcopy(scoring_weights)
    else:
        updated.pop(INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY, None)
    return updated


def extract_candidate_seeds(raw_result: dict[str, Any] | None) -> list[VerificationCandidateSeed]:
    """Restore durable verification seeds from raw_result."""
    if not isinstance(raw_result, dict):
        return []
    payload = raw_result.get(INTERNAL_VERIFICATION_CANDIDATES_KEY)
    if not isinstance(payload, list):
        return []

    seeds: list[VerificationCandidateSeed] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        seeds.append(VerificationCandidateSeed.model_validate(item))
    seeds.sort(key=lambda seed: seed.fast_rank)
    return seeds


def strip_verification_metadata(
    raw_result: dict[str, Any],
    *,
    drop_public_summary: bool = False,
) -> dict[str, Any]:
    """Remove internal verification payload from a public-facing raw result."""
    updated = dict(raw_result)
    updated.pop(INTERNAL_VERIFICATION_CANDIDATES_KEY, None)
    updated.pop(INTERNAL_VERIFICATION_REQUESTED_TOP_K_KEY, None)
    updated.pop(INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY, None)
    if drop_public_summary:
        updated.pop("verification", None)
    return updated


def requested_top_k_from_raw_result(raw_result: dict[str, Any] | None) -> int | None:
    if not isinstance(raw_result, dict):
        return None
    requested_top_k = raw_result.get(INTERNAL_VERIFICATION_REQUESTED_TOP_K_KEY)
    return int(requested_top_k) if isinstance(requested_top_k, int) else None


def find_candidate_seed(
    raw_result: dict[str, Any] | None,
    candidate_id: str | None,
) -> VerificationCandidateSeed | None:
    """Find a durable verification seed by candidate identifier."""
    if not candidate_id:
        return None
    for seed in extract_candidate_seeds(raw_result):
        if seed.candidate_id == candidate_id:
            return seed
    return None


def verification_requested(raw_result: dict[str, Any] | None) -> bool:
    return bool(extract_candidate_seeds(raw_result))


def _embedded_verification_summary(raw_result: dict[str, Any] | None) -> VerificationSummary | None:
    if not isinstance(raw_result, dict):
        return None
    verification_payload = raw_result.get("verification")
    if not isinstance(verification_payload, dict):
        return None
    return VerificationSummary.model_validate(verification_payload)


def _resolve_verified_metrics(job: JobInfo | None) -> CanonicalExecutionMetrics | None:
    if job is None:
        return None
    if job.canonical_result is not None and job.canonical_result.summary_metrics is not None:
        return job.canonical_result.summary_metrics
    if job.result is None:
        return None
    return CanonicalExecutionMetrics(
        total_return=job.result.total_return,
        sharpe_ratio=job.result.sharpe_ratio,
        sortino_ratio=job.result.sortino_ratio,
        calmar_ratio=job.result.calmar_ratio,
        max_drawdown=job.result.max_drawdown,
        win_rate=job.result.win_rate,
        trade_count=job.result.trade_count,
    )


def _build_delta(
    fast_metrics: CanonicalExecutionMetrics | None,
    verified_metrics: CanonicalExecutionMetrics | None,
) -> VerificationDelta | None:
    if fast_metrics is None or verified_metrics is None:
        return None
    return VerificationDelta(
        total_return_delta=(
            verified_metrics.total_return - fast_metrics.total_return
            if verified_metrics.total_return is not None
            and fast_metrics.total_return is not None
            else None
        ),
        sharpe_ratio_delta=(
            verified_metrics.sharpe_ratio - fast_metrics.sharpe_ratio
            if verified_metrics.sharpe_ratio is not None
            and fast_metrics.sharpe_ratio is not None
            else None
        ),
        max_drawdown_delta=(
            verified_metrics.max_drawdown - fast_metrics.max_drawdown
            if verified_metrics.max_drawdown is not None
            and fast_metrics.max_drawdown is not None
            else None
        ),
        trade_count_delta=(
            verified_metrics.trade_count - fast_metrics.trade_count
            if verified_metrics.trade_count is not None
            and fast_metrics.trade_count is not None
            else None
        ),
    )


def _build_mismatch_reasons(
    verification_status: VerificationCandidateStatus,
    delta: VerificationDelta | None,
) -> list[str]:
    if verification_status == VerificationCandidateStatus.FAILED:
        return ["verification_failed"]
    if delta is None:
        return ["verification_metrics_missing"]

    reasons: list[str] = []
    if delta.total_return_delta is not None and abs(delta.total_return_delta) >= 2.0:
        reasons.append("total_return_delta")
    if delta.sharpe_ratio_delta is not None and abs(delta.sharpe_ratio_delta) >= 0.20:
        reasons.append("sharpe_ratio_delta")
    if delta.max_drawdown_delta is not None and abs(delta.max_drawdown_delta) >= 1.0:
        reasons.append("max_drawdown_delta")
    if delta.trade_count_delta is not None and delta.trade_count_delta != 0:
        reasons.append("trade_count_delta")
    return reasons


def resolve_verification_summary(
    manager: JobManager,
    job: JobInfo,
) -> VerificationSummary | None:
    """Resolve a public verification summary from durable child jobs."""
    embedded_summary = _embedded_verification_summary(job.raw_result)
    seeds = extract_candidate_seeds(job.raw_result)
    if not seeds:
        return embedded_summary

    requested_top_k = requested_top_k_from_raw_result(job.raw_result)
    if requested_top_k is None and embedded_summary is None:
        return None
    requested_top_k = requested_top_k if requested_top_k is not None else len(seeds)
    candidate_summaries: list[VerificationCandidateSummary] = []
    completed_count = 0
    mismatch_count = 0
    all_terminal = True

    for seed in seeds:
        child_job = manager.get_job(seed.verification_run_id) if seed.verification_run_id else None
        verified_metrics: CanonicalExecutionMetrics | None = None
        if seed.verification_run_id is None or child_job is None:
            verification_status = VerificationCandidateStatus.QUEUED
            all_terminal = False
        elif child_job.status == JobStatus.PENDING:
            verification_status = VerificationCandidateStatus.QUEUED
            all_terminal = False
        elif child_job.status == JobStatus.RUNNING:
            verification_status = VerificationCandidateStatus.RUNNING
            all_terminal = False
        elif child_job.status == JobStatus.COMPLETED:
            verification_status = VerificationCandidateStatus.VERIFIED
            verified_metrics = _resolve_verified_metrics(child_job)
            completed_count += 1
        else:
            verification_status = VerificationCandidateStatus.FAILED
            completed_count += 1

        delta = _build_delta(seed.fast_metrics, verified_metrics)
        mismatch_reasons = _build_mismatch_reasons(verification_status, delta)
        if mismatch_reasons:
            mismatch_count += 1

        candidate_summaries.append(
            VerificationCandidateSummary(
                candidate_id=seed.candidate_id,
                fast_rank=seed.fast_rank,
                fast_score=seed.fast_score,
                fast_metrics=seed.fast_metrics,
                verification_run_id=seed.verification_run_id,
                verification_status=verification_status,
                verified_metrics=verified_metrics,
                delta=delta,
                mismatch_reasons=mismatch_reasons,
            )
        )

    authoritative_candidate_id: str | None = None
    winner_changed = False
    if all_terminal:
        for summary in candidate_summaries:
            if (
                summary.verification_status == VerificationCandidateStatus.VERIFIED
                and not summary.mismatch_reasons
            ):
                authoritative_candidate_id = summary.candidate_id
                break
        winner_changed = bool(
            authoritative_candidate_id
            and authoritative_candidate_id != candidate_summaries[0].candidate_id
        )

    if job.status == JobStatus.FAILED and not all_terminal:
        overall_status = VerificationOverallStatus.FAILED
    elif all_terminal:
        overall_status = (
            VerificationOverallStatus.COMPLETED
            if authoritative_candidate_id is not None
            else VerificationOverallStatus.COMPLETED_WITH_MISMATCH
        )
    elif completed_count > 0:
        overall_status = VerificationOverallStatus.RUNNING
    else:
        overall_status = VerificationOverallStatus.QUEUED

    return VerificationSummary(
        overall_status=overall_status,
        requested_top_k=requested_top_k,
        completed_count=completed_count,
        mismatch_count=mismatch_count,
        winner_changed=winner_changed,
        authoritative_candidate_id=authoritative_candidate_id,
        candidates=candidate_summaries,
    )


async def persist_verification_state(
    manager: JobManager,
    parent_job_id: str,
    raw_result: dict[str, Any],
    seeds: list[VerificationCandidateSeed],
    *,
    requested_top_k: int,
    scoring_weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Persist raw_result with internal verification metadata and public summary."""
    updated = serialize_candidate_seeds(
        raw_result,
        seeds,
        requested_top_k=requested_top_k,
        scoring_weights=scoring_weights,
    )
    parent_job = manager.get_job(parent_job_id)
    if parent_job is not None:
        summary = resolve_verification_summary(manager, parent_job)
        if summary is not None:
            updated["verification"] = summary.model_dump(mode="json")
    await manager.set_job_raw_result(parent_job_id, updated)
    return updated


async def run_verification_orchestrator(
    manager: JobManager,
    *,
    parent_job_id: str,
    raw_result: dict[str, Any],
    candidate_seeds: list[VerificationCandidateSeed],
    requested_top_k: int,
    status_message_prefix: str = "Nautilus verification",
    strategy_label: str | None = None,
    scoring_weights: dict[str, float] | None = None,
) -> tuple[dict[str, Any], VerificationSummary]:
    """Create child backtest jobs and execute them inline in fast-rank order."""
    seeds = [seed.model_copy(deep=True) for seed in candidate_seeds[:requested_top_k]]
    updated_raw_result = await persist_verification_state(
        manager,
        parent_job_id,
        raw_result,
        seeds,
        requested_top_k=requested_top_k,
        scoring_weights=scoring_weights,
    )
    total = len(seeds)
    if total == 0:
        summary = VerificationSummary(
            overall_status=VerificationOverallStatus.COMPLETED_WITH_MISMATCH,
            requested_top_k=requested_top_k,
            completed_count=0,
            mismatch_count=0,
            authoritative_candidate_id=None,
            candidates=[],
        )
        updated_raw_result["verification"] = summary.model_dump(mode="json")
        await manager.set_job_raw_result(parent_job_id, updated_raw_result)
        return updated_raw_result, summary

    for seed in seeds:
        if seed.verification_run_id is not None:
            continue
        run_spec = build_config_override_run_spec(
            "backtest",
            seed.strategy_name,
            config_override=seed.config_override,
            parameters={
                "verification_candidate_id": seed.candidate_id,
                "verification_fast_rank": seed.fast_rank,
            },
            engine_family=EngineFamily.NAUTILUS,
        )
        seed.verification_run_id = manager.create_job(
            strategy_name=seed.strategy_name,
            job_type="backtest",
            run_spec=run_spec,
            parent_run_id=parent_job_id,
        )

    updated_raw_result = await persist_verification_state(
        manager,
        parent_job_id,
        updated_raw_result,
        seeds,
        requested_top_k=requested_top_k,
        scoring_weights=scoring_weights,
    )

    for index, seed in enumerate(seeds, start=1):
        progress = _VERIFICATION_PROGRESS_OFFSET + (
            ((index - 1) / total) * _VERIFICATION_PROGRESS_WEIGHT
            if total > 0
            else 0.0
        )
        message = f"{status_message_prefix} {index}/{total}"
        if strategy_label:
            message = f"{message}: {strategy_label}"
        await manager.update_job_status(
            parent_job_id,
            JobStatus.RUNNING,
            message=message,
            progress=progress,
        )
        if seed.verification_run_id is None:
            raise RuntimeError(f"verification run id missing for candidate {seed.candidate_id}")
        await run_backtest_worker(
            seed.verification_run_id,
            seed.strategy_name,
            config_override=seed.config_override,
            manager=manager,
            heartbeat_seconds=60.0,
            timeout_seconds=None,
            exit_on_cancel=lambda _code: None,
        )
        updated_raw_result = await persist_verification_state(
            manager,
            parent_job_id,
            updated_raw_result,
            seeds,
            requested_top_k=requested_top_k,
            scoring_weights=scoring_weights,
        )

    parent_job = manager.get_job(parent_job_id)
    if parent_job is None:
        raise RuntimeError(f"parent job not found: {parent_job_id}")
    summary = resolve_verification_summary(manager, parent_job)
    if summary is None:
        raise RuntimeError("verification summary could not be resolved")
    updated_raw_result["verification"] = summary.model_dump(mode="json")
    await manager.set_job_raw_result(parent_job_id, updated_raw_result)
    return updated_raw_result, summary
