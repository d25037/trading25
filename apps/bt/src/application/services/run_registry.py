"""
Artifact-first run registry readers.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from loguru import logger

from src.application.services.backtest_result_summary import resolve_backtest_result_summary
from src.application.services.job_manager import JobInfo
from src.domains.backtest.contracts import ArtifactIndex, ArtifactKind
from src.entrypoints.http.schemas.backtest import BacktestResultSummary, SignalAttributionResult


def _find_artifact_path(
    artifact_index: ArtifactIndex | None,
    kind: ArtifactKind,
) -> str | None:
    if artifact_index is None:
        return None

    for artifact in artifact_index.artifacts:
        if artifact.kind != kind or not artifact.path:
            continue
        return artifact.path
    return None


def _canonical_summary_fallback(job: JobInfo) -> Mapping[str, Any] | None:
    summary_metrics = job.canonical_result.summary_metrics if job.canonical_result else None
    if summary_metrics is None:
        return None
    return summary_metrics.model_dump(exclude_none=True)


def resolve_job_backtest_summary(job: JobInfo) -> BacktestResultSummary | None:
    """Resolve a backtest summary from artifacts, then canonical result, then legacy fields."""
    artifact_html_path = _find_artifact_path(job.artifact_index, ArtifactKind.HTML)
    artifact_metrics_path = _find_artifact_path(job.artifact_index, ArtifactKind.METRICS_JSON)
    fallback = (
        _canonical_summary_fallback(job)
        or job.result
        or (job.raw_result if isinstance(job.raw_result, dict) else None)
    )
    return resolve_backtest_result_summary(
        html_path=artifact_html_path or job.html_path,
        fallback=fallback,
        metrics_path=artifact_metrics_path,
    )


def _load_attribution_artifact(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None

    artifact_path = Path(path)
    if not artifact_path.exists():
        return None

    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"寄与分析artifactの読み込みに失敗: {artifact_path}: {e}")
        return None

    if not isinstance(payload, dict):
        return None

    result = payload.get("result")
    return result if isinstance(result, dict) else None


def resolve_signal_attribution_result(job: JobInfo) -> SignalAttributionResult | None:
    """Resolve attribution result from artifacts, then canonical result, then legacy fields."""
    candidates: list[dict[str, Any]] = []
    artifact_result = _load_attribution_artifact(
        _find_artifact_path(job.artifact_index, ArtifactKind.ATTRIBUTION_JSON)
    )
    if artifact_result is not None:
        candidates.append(artifact_result)

    canonical_payload = job.canonical_result.payload if job.canonical_result else None
    if isinstance(canonical_payload, dict):
        candidates.append(canonical_payload)

    if isinstance(job.raw_result, dict):
        candidates.append(job.raw_result)

    for candidate in candidates:
        try:
            return SignalAttributionResult.model_validate(candidate)
        except Exception as e:
            logger.warning(f"寄与分析結果のパースに失敗: {e}")

    return None
