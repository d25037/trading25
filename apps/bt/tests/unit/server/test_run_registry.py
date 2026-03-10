"""Tests for artifact-first run registry readers."""

from __future__ import annotations

import json
from pathlib import Path

from src.application.services.job_manager import JobInfo
from src.application.services.run_registry import (
    resolve_job_backtest_summary,
    resolve_signal_attribution_result,
)
from src.domains.backtest.contracts import (
    ArtifactIndex,
    ArtifactKind,
    ArtifactRecord,
    ArtifactStorage,
    CanonicalExecutionMetrics,
    CanonicalExecutionResult,
    EngineFamily,
    RunType,
)
from src.entrypoints.http.schemas.backtest import BacktestResultSummary, JobStatus


def test_resolve_job_backtest_summary_uses_artifact_index_html_path(tmp_path: Path) -> None:
    html_path = tmp_path / "report.html"
    html_path.write_text("<html>ok</html>", encoding="utf-8")

    job = JobInfo("job-1", "demo-strategy", job_type="backtest")
    job.status = JobStatus.COMPLETED
    job.artifact_index = ArtifactIndex(
        artifacts=[
            ArtifactRecord(
                kind=ArtifactKind.HTML,
                storage=ArtifactStorage.FILESYSTEM,
                path=str(html_path),
            )
        ]
    )
    job.canonical_result = CanonicalExecutionResult(
        run_id="job-1",
        run_type=RunType.BACKTEST,
        strategy_name="demo-strategy",
        engine_family=EngineFamily.VECTORBT,
        status="completed",
        summary_metrics=CanonicalExecutionMetrics(
            total_return=7.0,
            sharpe_ratio=1.2,
            calmar_ratio=1.5,
            max_drawdown=-2.0,
            win_rate=60.0,
            trade_count=9,
        ),
    )

    summary = resolve_job_backtest_summary(job)

    assert summary is not None
    assert summary.total_return == 7.0
    assert summary.trade_count == 9
    assert summary.html_path == str(html_path)


def test_resolve_job_backtest_summary_prefers_canonical_result_before_legacy_summary() -> None:
    job = JobInfo("job-2", "demo-strategy", job_type="backtest")
    job.status = JobStatus.COMPLETED
    job.result = BacktestResultSummary(
        total_return=1.0,
        sharpe_ratio=0.5,
        sortino_ratio=None,
        calmar_ratio=0.4,
        max_drawdown=-3.0,
        win_rate=40.0,
        trade_count=3,
    )
    job.canonical_result = CanonicalExecutionResult(
        run_id="job-2",
        run_type=RunType.BACKTEST,
        strategy_name="demo-strategy",
        engine_family=EngineFamily.VECTORBT,
        status="completed",
        summary_metrics=CanonicalExecutionMetrics(
            total_return=5.0,
            sharpe_ratio=1.1,
            sortino_ratio=1.3,
            calmar_ratio=0.9,
            max_drawdown=-1.5,
            win_rate=70.0,
            trade_count=11,
        ),
    )

    summary = resolve_job_backtest_summary(job)

    assert summary is not None
    assert summary.total_return == 5.0
    assert summary.trade_count == 11
    assert summary.win_rate == 70.0


def test_resolve_job_backtest_summary_returns_none_when_no_matching_artifact_or_fallback() -> None:
    job = JobInfo("job-3", "demo-strategy", job_type="backtest")
    job.status = JobStatus.COMPLETED
    job.artifact_index = ArtifactIndex(
        artifacts=[
            ArtifactRecord(
                kind=ArtifactKind.HTML,
                storage=ArtifactStorage.FILESYSTEM,
                path="",
            )
        ]
    )
    job.canonical_result = CanonicalExecutionResult(
        run_id="job-3",
        run_type=RunType.BACKTEST,
        strategy_name="demo-strategy",
        engine_family=EngineFamily.VECTORBT,
        status="completed",
        summary_metrics=None,
    )

    assert resolve_job_backtest_summary(job) is None


def test_resolve_signal_attribution_result_uses_saved_artifact_when_raw_result_missing(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "attribution.json"
    artifact_path.write_text(
        json.dumps(
            {
                "saved_at": "2026-03-09T00:00:00+00:00",
                "result": {
                    "baseline_metrics": {"total_return": 12.0, "sharpe_ratio": 1.2},
                    "signals": [],
                    "top_n_selection": {
                        "top_n_requested": 5,
                        "top_n_effective": 0,
                        "selected_signal_ids": [],
                        "scores": [],
                    },
                    "timing": {
                        "total_seconds": 1.0,
                        "baseline_seconds": 0.1,
                        "loo_seconds": 0.5,
                        "shapley_seconds": 0.4,
                    },
                    "shapley": {
                        "method": "permutation",
                        "sample_size": 16,
                        "error": None,
                        "evaluations": 16,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    job = JobInfo("job-attr", "demo-strategy", job_type="backtest_attribution")
    job.status = JobStatus.COMPLETED
    job.artifact_index = ArtifactIndex(
        artifacts=[
            ArtifactRecord(
                kind=ArtifactKind.ATTRIBUTION_JSON,
                storage=ArtifactStorage.FILESYSTEM,
                path=str(artifact_path),
            )
        ]
    )

    result = resolve_signal_attribution_result(job)

    assert result is not None
    assert result.baseline_metrics.total_return == 12.0
    assert result.shapley.method == "permutation"


def test_resolve_signal_attribution_result_prefers_artifact_before_canonical_and_raw(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "attribution.json"
    artifact_path.write_text(
        json.dumps(
            {
                "saved_at": "2026-03-09T00:00:00+00:00",
                "result": {
                    "baseline_metrics": {"total_return": 12.0, "sharpe_ratio": 1.2},
                    "signals": [],
                    "top_n_selection": {
                        "top_n_requested": 5,
                        "top_n_effective": 0,
                        "selected_signal_ids": [],
                        "scores": [],
                    },
                    "timing": {
                        "total_seconds": 1.0,
                        "baseline_seconds": 0.1,
                        "loo_seconds": 0.5,
                        "shapley_seconds": 0.4,
                    },
                    "shapley": {
                        "method": "permutation",
                        "sample_size": 16,
                        "error": None,
                        "evaluations": 16,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    job = JobInfo("job-attr-2", "demo-strategy", job_type="backtest_attribution")
    job.status = JobStatus.COMPLETED
    job.raw_result = {
        "baseline_metrics": {"total_return": 3.0, "sharpe_ratio": 0.3},
        "signals": [],
        "top_n_selection": {
            "top_n_requested": 1,
            "top_n_effective": 0,
            "selected_signal_ids": [],
            "scores": [],
        },
        "timing": {
            "total_seconds": 9.0,
            "baseline_seconds": 1.0,
            "loo_seconds": 4.0,
            "shapley_seconds": 4.0,
        },
        "shapley": {"method": "raw", "sample_size": 1, "error": None, "evaluations": 1},
    }
    job.canonical_result = CanonicalExecutionResult(
        run_id="job-attr-2",
        run_type=RunType.ATTRIBUTION,
        strategy_name="demo-strategy",
        engine_family=EngineFamily.VECTORBT,
        status="completed",
        payload={
            "baseline_metrics": {"total_return": 7.0, "sharpe_ratio": 0.7},
            "signals": [],
            "top_n_selection": {
                "top_n_requested": 2,
                "top_n_effective": 0,
                "selected_signal_ids": [],
                "scores": [],
            },
            "timing": {
                "total_seconds": 2.0,
                "baseline_seconds": 0.2,
                "loo_seconds": 0.9,
                "shapley_seconds": 0.9,
            },
            "shapley": {
                "method": "canonical",
                "sample_size": 2,
                "error": None,
                "evaluations": 2,
            },
        },
    )
    job.artifact_index = ArtifactIndex(
        artifacts=[
            ArtifactRecord(
                kind=ArtifactKind.ATTRIBUTION_JSON,
                storage=ArtifactStorage.FILESYSTEM,
                path=str(artifact_path),
            )
        ]
    )

    result = resolve_signal_attribution_result(job)

    assert result is not None
    assert result.baseline_metrics.total_return == 12.0
    assert result.shapley.method == "permutation"


def test_resolve_signal_attribution_result_prefers_canonical_before_raw_result() -> None:
    job = JobInfo("job-attr-3", "demo-strategy", job_type="backtest_attribution")
    job.status = JobStatus.COMPLETED
    job.raw_result = {
        "baseline_metrics": {"total_return": 3.0, "sharpe_ratio": 0.3},
        "signals": [],
        "top_n_selection": {
            "top_n_requested": 1,
            "top_n_effective": 0,
            "selected_signal_ids": [],
            "scores": [],
        },
        "timing": {
            "total_seconds": 9.0,
            "baseline_seconds": 1.0,
            "loo_seconds": 4.0,
            "shapley_seconds": 4.0,
        },
        "shapley": {"method": "raw", "sample_size": 1, "error": None, "evaluations": 1},
    }
    job.canonical_result = CanonicalExecutionResult(
        run_id="job-attr-3",
        run_type=RunType.ATTRIBUTION,
        strategy_name="demo-strategy",
        engine_family=EngineFamily.VECTORBT,
        status="completed",
        payload={
            "baseline_metrics": {"total_return": 7.0, "sharpe_ratio": 0.7},
            "signals": [],
            "top_n_selection": {
                "top_n_requested": 2,
                "top_n_effective": 0,
                "selected_signal_ids": [],
                "scores": [],
            },
            "timing": {
                "total_seconds": 2.0,
                "baseline_seconds": 0.2,
                "loo_seconds": 0.9,
                "shapley_seconds": 0.9,
            },
            "shapley": {
                "method": "canonical",
                "sample_size": 2,
                "error": None,
                "evaluations": 2,
            },
        },
    )

    result = resolve_signal_attribution_result(job)

    assert result is not None
    assert result.baseline_metrics.total_return == 7.0
    assert result.shapley.method == "canonical"


def test_resolve_signal_attribution_result_returns_none_when_artifact_file_missing(
    tmp_path: Path,
) -> None:
    missing_path = tmp_path / "missing-attribution.json"

    job = JobInfo("job-attr-4", "demo-strategy", job_type="backtest_attribution")
    job.status = JobStatus.COMPLETED
    job.artifact_index = ArtifactIndex(
        artifacts=[
            ArtifactRecord(
                kind=ArtifactKind.ATTRIBUTION_JSON,
                storage=ArtifactStorage.FILESYSTEM,
                path=str(missing_path),
            )
        ]
    )

    assert resolve_signal_attribution_result(job) is None


def test_resolve_signal_attribution_result_returns_none_when_artifact_json_invalid(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "broken-attribution.json"
    artifact_path.write_text("{broken", encoding="utf-8")

    job = JobInfo("job-attr-5", "demo-strategy", job_type="backtest_attribution")
    job.status = JobStatus.COMPLETED
    job.artifact_index = ArtifactIndex(
        artifacts=[
            ArtifactRecord(
                kind=ArtifactKind.ATTRIBUTION_JSON,
                storage=ArtifactStorage.FILESYSTEM,
                path=str(artifact_path),
            )
        ]
    )
    job.raw_result = {"invalid": "shape"}

    assert resolve_signal_attribution_result(job) is None


def test_resolve_signal_attribution_result_returns_none_when_artifact_payload_not_object(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "list-attribution.json"
    artifact_path.write_text(json.dumps(["not", "an", "object"]), encoding="utf-8")

    job = JobInfo("job-attr-6", "demo-strategy", job_type="backtest_attribution")
    job.status = JobStatus.COMPLETED
    job.artifact_index = ArtifactIndex(
        artifacts=[
            ArtifactRecord(
                kind=ArtifactKind.ATTRIBUTION_JSON,
                storage=ArtifactStorage.FILESYSTEM,
                path=str(artifact_path),
            )
        ]
    )

    assert resolve_signal_attribution_result(job) is None


def test_resolve_signal_attribution_result_returns_none_when_all_candidates_invalid() -> None:
    job = JobInfo("job-attr-7", "demo-strategy", job_type="backtest_attribution")
    job.status = JobStatus.COMPLETED
    job.canonical_result = CanonicalExecutionResult(
        run_id="job-attr-7",
        run_type=RunType.ATTRIBUTION,
        strategy_name="demo-strategy",
        engine_family=EngineFamily.VECTORBT,
        status="completed",
        payload={"invalid": "canonical-shape"},
    )
    job.raw_result = {"invalid": "raw-shape"}

    assert resolve_signal_attribution_result(job) is None
