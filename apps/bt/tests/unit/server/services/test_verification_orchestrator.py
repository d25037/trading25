from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.application.services import verification_orchestrator as orchestrator
from src.application.services.job_manager import JobInfo, JobManager
from src.application.services.run_contracts import build_config_override_run_spec
from src.domains.backtest.contracts import (
    CanonicalExecutionMetrics,
    EngineFamily,
    VerificationCandidateStatus,
    VerificationDelta,
    VerificationOverallStatus,
    VerificationSummary,
)
from src.domains.lab_agent.models import StrategyCandidate
from src.entrypoints.http.schemas.backtest import BacktestResultSummary, JobStatus


def _make_candidate(strategy_id: str) -> StrategyCandidate:
    return StrategyCandidate(
        strategy_id=strategy_id,
        entry_filter_params={"signal_a": {"period": 20}},
        exit_trigger_params={"signal_b": {"period": 5}},
        shared_config={"dataset": "demo"},
    )


def _make_metrics(
    *,
    total_return: float,
    sharpe_ratio: float,
    max_drawdown: float,
    trade_count: int,
) -> CanonicalExecutionMetrics:
    return CanonicalExecutionMetrics(
        total_return=total_return,
        sharpe_ratio=sharpe_ratio,
        max_drawdown=max_drawdown,
        trade_count=trade_count,
    )


def _make_seed(
    candidate_id: str,
    *,
    fast_rank: int = 1,
    fast_score: float = 1.0,
    verification_run_id: str | None = None,
    strategy_candidate: StrategyCandidate | None = None,
) -> orchestrator.VerificationCandidateSeed:
    return orchestrator.build_verification_seed(
        candidate_id=candidate_id,
        fast_rank=fast_rank,
        fast_score=fast_score,
        fast_metrics=_make_metrics(
            total_return=10.0 + fast_rank,
            sharpe_ratio=1.0 + (fast_rank / 10),
            max_drawdown=-5.0,
            trade_count=5 + fast_rank,
        ),
        strategy_name="demo-strategy",
        config_override={"shared_config": {"dataset": "demo"}},
        strategy_candidate=strategy_candidate,
    ).model_copy(update={"verification_run_id": verification_run_id})


def _make_backtest_result(
    *,
    total_return: float,
    sharpe_ratio: float,
    max_drawdown: float,
    trade_count: int,
) -> BacktestResultSummary:
    return BacktestResultSummary(
        total_return=total_return,
        sharpe_ratio=sharpe_ratio,
        sortino_ratio=sharpe_ratio,
        calmar_ratio=sharpe_ratio,
        max_drawdown=max_drawdown,
        win_rate=55.0,
        trade_count=trade_count,
        html_path=None,
    )


def test_helper_round_trip_and_seed_lookup() -> None:
    strategy_candidate = _make_candidate("candidate-a")

    assert orchestrator.build_canonical_metrics(None) is None
    assert orchestrator.build_canonical_metrics({"foo": "bar"}) is None

    metrics = orchestrator.build_canonical_metrics(
        {
            "total_return": 12.5,
            "sharpe_ratio": 1.4,
            "sortino_ratio": 1.6,
            "calmar_ratio": 1.2,
            "max_drawdown": -4.2,
            "win_rate": 60.0,
            "trade_count": 8,
        }
    )
    assert metrics is not None
    assert metrics.trade_count == 8

    seed_a = orchestrator.build_verification_seed(
        candidate_id="candidate-a",
        fast_rank=2,
        fast_score=1.2,
        fast_metrics=metrics,
        strategy_name="demo-strategy",
        config_override={"shared_config": {"dataset": "demo"}},
        strategy_candidate=strategy_candidate.model_dump(mode="json"),
    )
    seed_b = orchestrator.build_verification_seed(
        candidate_id="candidate-b",
        fast_rank=1,
        fast_score=0.8,
        fast_metrics=metrics,
        strategy_name="demo-strategy",
        config_override={"shared_config": {"dataset": "demo"}},
        strategy_candidate={"strategy_id": "broken"},
    )

    serialized = orchestrator.serialize_candidate_seeds(
        {
            "lab_type": "generate",
            orchestrator.INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY: {"stale": 1.0},
        },
        [seed_a, seed_b],
        requested_top_k=2,
    )
    assert orchestrator.INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY not in serialized

    serialized = orchestrator.serialize_candidate_seeds(
        serialized,
        [seed_a, seed_b],
        requested_top_k=2,
        scoring_weights={"sharpe_ratio": 1.0},
    )
    noisy_payload = dict(serialized)
    noisy_payload[orchestrator.INTERNAL_VERIFICATION_CANDIDATES_KEY] = [
        "skip-me",
        *serialized[orchestrator.INTERNAL_VERIFICATION_CANDIDATES_KEY],
    ]

    restored = orchestrator.extract_candidate_seeds(noisy_payload)
    assert [seed.candidate_id for seed in restored] == ["candidate-b", "candidate-a"]
    assert restored[1].strategy_candidate is not None
    assert restored[0].strategy_candidate is None
    assert orchestrator.requested_top_k_from_raw_result(noisy_payload) == 2
    assert orchestrator.requested_top_k_from_raw_result({"bad": "value"}) is None
    assert orchestrator.find_candidate_seed(noisy_payload, "candidate-a") is not None
    assert orchestrator.find_candidate_seed(noisy_payload, None) is None
    assert orchestrator.verification_requested(noisy_payload) is True
    assert orchestrator.verification_requested({"lab_type": "generate"}) is False

    stripped = orchestrator.strip_verification_metadata(noisy_payload)
    assert orchestrator.INTERNAL_VERIFICATION_CANDIDATES_KEY not in stripped
    assert orchestrator.INTERNAL_VERIFICATION_REQUESTED_TOP_K_KEY not in stripped
    assert orchestrator.INTERNAL_VERIFICATION_SCORING_WEIGHTS_KEY not in stripped


def test_metric_resolution_and_mismatch_helpers() -> None:
    assert orchestrator._resolve_verified_metrics(None) is None  # noqa: SLF001

    job = JobInfo(job_id="job-1", strategy_name="demo")
    assert orchestrator._resolve_verified_metrics(job) is None  # noqa: SLF001

    canonical_metrics = _make_metrics(
        total_return=9.0,
        sharpe_ratio=1.2,
        max_drawdown=-3.5,
        trade_count=6,
    )
    job.canonical_result = SimpleNamespace(summary_metrics=canonical_metrics)  # type: ignore[assignment]
    assert orchestrator._resolve_verified_metrics(job) == canonical_metrics  # noqa: SLF001

    job.canonical_result = None
    job.result = _make_backtest_result(
        total_return=8.0,
        sharpe_ratio=1.1,
        max_drawdown=-4.0,
        trade_count=5,
    )
    resolved = orchestrator._resolve_verified_metrics(job)  # noqa: SLF001
    assert resolved is not None
    assert resolved.trade_count == 5

    fast_metrics = _make_metrics(
        total_return=10.0,
        sharpe_ratio=1.4,
        max_drawdown=-5.0,
        trade_count=7,
    )
    delta = orchestrator._build_delta(fast_metrics, resolved)  # noqa: SLF001
    assert delta is not None
    assert delta.total_return_delta == pytest.approx(-2.0)
    assert delta.sharpe_ratio_delta == pytest.approx(-0.3)
    assert delta.max_drawdown_delta == pytest.approx(1.0)
    assert delta.trade_count_delta == -2
    assert orchestrator._build_delta(None, resolved) is None  # noqa: SLF001
    assert orchestrator._build_mismatch_reasons(VerificationCandidateStatus.FAILED, None) == [  # noqa: SLF001
        "verification_failed"
    ]
    assert orchestrator._build_mismatch_reasons(VerificationCandidateStatus.VERIFIED, None) == [  # noqa: SLF001
        "verification_metrics_missing"
    ]
    assert orchestrator._build_mismatch_reasons(VerificationCandidateStatus.VERIFIED, delta) == [  # noqa: SLF001
        "total_return_delta",
        "sharpe_ratio_delta",
        "max_drawdown_delta",
        "trade_count_delta",
    ]


def test_resolve_verification_summary_falls_back_to_embedded_payload() -> None:
    manager = JobManager()
    parent_id = manager.create_job("demo", job_type="optimization")
    parent = manager.get_job(parent_id)
    assert parent is not None
    parent.raw_result = {
        "verification": VerificationSummary(
            overall_status=VerificationOverallStatus.QUEUED,
            requested_top_k=1,
            completed_count=0,
            mismatch_count=0,
            winner_changed=False,
            authoritative_candidate_id=None,
            candidates=[],
        ).model_dump(mode="json")
    }

    summary = orchestrator.resolve_verification_summary(manager, parent)
    assert summary is not None
    assert summary.overall_status == VerificationOverallStatus.QUEUED

    parent.raw_result = {
        orchestrator.INTERNAL_VERIFICATION_CANDIDATES_KEY: [
            _make_seed("queued-only").model_dump(mode="json")
        ]
    }
    assert orchestrator.resolve_verification_summary(manager, parent) is None

    parent.raw_result = {}
    assert orchestrator.resolve_verification_summary(manager, parent) is None


def test_resolve_verification_summary_handles_running_and_failed_states() -> None:
    manager = JobManager()
    parent_id = manager.create_job("demo", job_type="optimization")
    parent = manager.get_job(parent_id)
    assert parent is not None

    pending_id = manager.create_job("demo", job_type="backtest")
    running_id = manager.create_job("demo", job_type="backtest")
    completed_id = manager.create_job("demo", job_type="backtest")

    pending_job = manager.get_job(pending_id)
    running_job = manager.get_job(running_id)
    completed_job = manager.get_job(completed_id)
    assert pending_job is not None
    assert running_job is not None
    assert completed_job is not None

    pending_job.status = JobStatus.PENDING
    running_job.status = JobStatus.RUNNING
    completed_job.status = JobStatus.COMPLETED
    completed_job.result = _make_backtest_result(
        total_return=11.5,
        sharpe_ratio=1.31,
        max_drawdown=-5.0,
        trade_count=8,
    )

    parent.raw_result = orchestrator.serialize_candidate_seeds(
        {},
        [
            _make_seed("no-run-id", fast_rank=1),
            _make_seed("pending", fast_rank=2, verification_run_id=pending_id),
            _make_seed("running", fast_rank=3, verification_run_id=running_id),
            _make_seed("completed", fast_rank=4, verification_run_id=completed_id),
        ],
        requested_top_k=4,
    )

    summary = orchestrator.resolve_verification_summary(manager, parent)
    assert summary is not None
    assert summary.overall_status == VerificationOverallStatus.RUNNING
    assert [candidate.verification_status for candidate in summary.candidates or []] == [
        VerificationCandidateStatus.QUEUED,
        VerificationCandidateStatus.QUEUED,
        VerificationCandidateStatus.RUNNING,
        VerificationCandidateStatus.VERIFIED,
    ]
    assert summary.completed_count == 1

    failed_parent_id = manager.create_job("demo", job_type="optimization")
    failed_parent = manager.get_job(failed_parent_id)
    assert failed_parent is not None
    failed_parent.status = JobStatus.FAILED
    failed_parent.raw_result = orchestrator.serialize_candidate_seeds(
        {},
        [_make_seed("queued", fast_rank=1)],
        requested_top_k=1,
    )

    failed_summary = orchestrator.resolve_verification_summary(manager, failed_parent)
    assert failed_summary is not None
    assert failed_summary.overall_status == VerificationOverallStatus.FAILED


def test_resolve_verification_summary_picks_authoritative_verified_candidate() -> None:
    manager = JobManager()
    parent_id = manager.create_job("demo", job_type="optimization")
    parent = manager.get_job(parent_id)
    assert parent is not None
    parent.status = JobStatus.COMPLETED

    first_child_id = manager.create_job("demo", job_type="backtest")
    second_child_id = manager.create_job("demo", job_type="backtest")
    failed_child_id = manager.create_job("demo", job_type="backtest")

    first_child = manager.get_job(first_child_id)
    second_child = manager.get_job(second_child_id)
    failed_child = manager.get_job(failed_child_id)
    assert first_child is not None
    assert second_child is not None
    assert failed_child is not None

    first_child.status = JobStatus.COMPLETED
    first_child.result = _make_backtest_result(
        total_return=15.0,
        sharpe_ratio=2.0,
        max_drawdown=-2.0,
        trade_count=99,
    )
    second_child.status = JobStatus.COMPLETED
    second_child.result = _make_backtest_result(
        total_return=12.0,
        sharpe_ratio=1.2,
        max_drawdown=-5.0,
        trade_count=7,
    )
    failed_child.status = JobStatus.FAILED

    parent.raw_result = orchestrator.serialize_candidate_seeds(
        {},
        [
            _make_seed("fast-winner", fast_rank=1, verification_run_id=first_child_id),
            _make_seed("verified-winner", fast_rank=2, verification_run_id=second_child_id),
            _make_seed("failed-candidate", fast_rank=3, verification_run_id=failed_child_id),
        ],
        requested_top_k=3,
    )

    summary = orchestrator.resolve_verification_summary(manager, parent)
    assert summary is not None
    assert summary.overall_status == VerificationOverallStatus.COMPLETED
    assert summary.authoritative_candidate_id == "verified-winner"
    assert summary.winner_changed is True
    assert summary.mismatch_count == 2

    all_mismatch_parent_id = manager.create_job("demo", job_type="optimization")
    all_mismatch_parent = manager.get_job(all_mismatch_parent_id)
    assert all_mismatch_parent is not None
    all_mismatch_parent.status = JobStatus.COMPLETED
    all_mismatch_parent.raw_result = orchestrator.serialize_candidate_seeds(
        {},
        [_make_seed("failed-only", fast_rank=1, verification_run_id=failed_child_id)],
        requested_top_k=1,
    )

    mismatch_summary = orchestrator.resolve_verification_summary(manager, all_mismatch_parent)
    assert mismatch_summary is not None
    assert mismatch_summary.overall_status == VerificationOverallStatus.COMPLETED_WITH_MISMATCH
    assert mismatch_summary.authoritative_candidate_id is None


@pytest.mark.asyncio
async def test_persist_verification_state_attaches_resolved_summary() -> None:
    manager = JobManager()
    parent_id = manager.create_job("demo", job_type="optimization")
    child_id = manager.create_job("demo", job_type="backtest")

    parent = manager.get_job(parent_id)
    child = manager.get_job(child_id)
    assert parent is not None
    assert child is not None

    seed = _make_seed("candidate-1", verification_run_id=child_id, strategy_candidate=_make_candidate("candidate-1"))
    parent.raw_result = orchestrator.serialize_candidate_seeds({}, [seed], requested_top_k=1)
    child.status = JobStatus.COMPLETED
    child.result = _make_backtest_result(
        total_return=11.0,
        sharpe_ratio=1.1,
        max_drawdown=-5.0,
        trade_count=6,
    )

    updated = await orchestrator.persist_verification_state(
        manager,
        parent_id,
        {"lab_type": "generate"},
        [seed],
        requested_top_k=1,
        scoring_weights={"sharpe_ratio": 1.0},
    )

    assert updated["verification"]["overall_status"] == VerificationOverallStatus.COMPLETED.value
    persisted_parent = manager.get_job(parent_id)
    assert persisted_parent is not None
    assert persisted_parent.raw_result == updated


@pytest.mark.asyncio
async def test_run_verification_orchestrator_handles_empty_candidate_list() -> None:
    manager = JobManager()
    parent_id = manager.create_job("demo", job_type="optimization")

    updated, summary = await orchestrator.run_verification_orchestrator(
        manager,
        parent_job_id=parent_id,
        raw_result={"best_score": 1.0},
        candidate_seeds=[],
        requested_top_k=5,
    )

    assert summary.overall_status == VerificationOverallStatus.COMPLETED_WITH_MISMATCH
    assert updated["verification"]["requested_top_k"] == 5
    parent = manager.get_job(parent_id)
    assert parent is not None
    assert parent.raw_result == updated


@pytest.mark.asyncio
async def test_run_verification_orchestrator_creates_and_executes_child_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = JobManager()
    parent_id = manager.create_job("demo", job_type="optimization")
    parent = manager.get_job(parent_id)
    assert parent is not None
    parent.status = JobStatus.RUNNING

    precreated_child_run_spec = build_config_override_run_spec(
        "backtest",
        "demo-strategy",
        config_override={"shared_config": {"dataset": "demo"}},
        parameters={"verification_candidate_id": "existing"},
        engine_family=EngineFamily.NAUTILUS,
    )
    existing_child_id = manager.create_job(
        "demo-strategy",
        job_type="backtest",
        run_spec=precreated_child_run_spec,
        parent_run_id=parent_id,
    )

    calls: list[str] = []

    async def _run_backtest_worker(
        job_id: str,
        strategy_name: str,
        *,
        config_override: dict[str, object] | None,
        manager: JobManager,
        heartbeat_seconds: float,
        timeout_seconds: int | None,
        exit_on_cancel,
    ) -> None:
        _ = (heartbeat_seconds, timeout_seconds, exit_on_cancel)
        calls.append(job_id)
        child = manager.get_job(job_id)
        assert child is not None
        assert strategy_name == "demo-strategy"
        assert config_override == {"shared_config": {"dataset": "demo"}}
        assert child.run_spec is not None
        assert child.run_spec.parent_run_id == parent_id
        if job_id != existing_child_id:
            assert child.run_spec.engine_family == EngineFamily.NAUTILUS
        await manager.update_job_status(job_id, JobStatus.COMPLETED, progress=1.0)
        await manager.set_job_result(
            job_id,
            _make_backtest_result(
                total_return=11.0,
                sharpe_ratio=1.12,
                max_drawdown=-5.0,
                trade_count=6,
            ),
            raw_result={"engine": "nautilus"},
            html_path=None,
            dataset_name="demo",
            execution_time=0.01,
        )

    monkeypatch.setattr(orchestrator, "run_backtest_worker", _run_backtest_worker)

    seeds = [
        _make_seed("existing", fast_rank=1, verification_run_id=existing_child_id),
        _make_seed("new", fast_rank=2),
    ]
    updated, summary = await orchestrator.run_verification_orchestrator(
        manager,
        parent_job_id=parent_id,
        raw_result={"best_score": 1.0},
        candidate_seeds=seeds,
        requested_top_k=2,
        strategy_label="demo",
        scoring_weights={"sharpe_ratio": 1.0},
    )

    assert len(calls) == 2
    assert summary.overall_status == VerificationOverallStatus.COMPLETED
    assert summary.authoritative_candidate_id == "existing"
    assert updated["verification"]["authoritative_candidate_id"] == "existing"
    persisted_parent = manager.get_job(parent_id)
    assert persisted_parent is not None
    assert persisted_parent.raw_result == updated
