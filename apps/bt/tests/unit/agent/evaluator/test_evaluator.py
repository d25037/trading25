"""agent/evaluator/evaluator.py のテスト"""

from contextlib import contextmanager

from src.agent.evaluator import evaluator as evaluator_module
from src.agent.evaluator.evaluator import StrategyEvaluator
from src.agent.models import EvaluationResult, StrategyCandidate


def _candidate() -> StrategyCandidate:
    return StrategyCandidate(
        strategy_id="test",
        entry_filter_params={},
        exit_trigger_params={},
    )


def test_evaluate_single_forces_direct_mode(monkeypatch) -> None:
    captured_modes: list[str] = []
    expected = EvaluationResult(candidate=_candidate(), score=1.0)

    @contextmanager
    def _fake_mode_context(mode: str):
        captured_modes.append(mode)
        yield

    monkeypatch.setattr(
        "src.agent.evaluator.evaluator.data_access_mode_context",
        _fake_mode_context,
    )
    monkeypatch.setattr(
        "src.agent.evaluator.evaluator.evaluate_single_candidate",
        lambda *_args, **_kwargs: expected,
    )

    evaluator = StrategyEvaluator(shared_config_dict={"stock_codes": ["7203"]})
    result = evaluator.evaluate_single(_candidate())

    assert result == expected
    assert captured_modes == ["direct"]


def test_evaluate_batch_forces_direct_mode(monkeypatch) -> None:
    captured_modes: list[str] = []

    @contextmanager
    def _fake_mode_context(mode: str):
        captured_modes.append(mode)
        yield

    monkeypatch.setattr(
        "src.agent.evaluator.evaluator.data_access_mode_context",
        _fake_mode_context,
    )
    monkeypatch.setattr(
        StrategyEvaluator,
        "_evaluate_batch_internal",
        lambda self, candidates, top_k=None: [],
    )

    evaluator = StrategyEvaluator(shared_config_dict={"stock_codes": ["7203"]})
    result = evaluator.evaluate_batch([_candidate()], top_k=1, enable_cache=False)

    assert result == []
    assert captured_modes == ["direct"]


def test_evaluate_batch_empty_returns_immediately(monkeypatch) -> None:
    mode_calls: list[str] = []

    @contextmanager
    def _fake_mode_context(mode: str):
        mode_calls.append(mode)
        yield

    monkeypatch.setattr(
        "src.agent.evaluator.evaluator.data_access_mode_context",
        _fake_mode_context,
    )

    evaluator = StrategyEvaluator(shared_config_dict={"stock_codes": ["7203"]})
    assert evaluator.evaluate_batch([], enable_cache=True) == []
    assert mode_calls == []


def test_evaluate_batch_enables_and_disables_cache(monkeypatch) -> None:
    calls: list[str] = []

    class _Cache:
        def get_stats(self):
            return {"hits": 3, "misses": 1}

    monkeypatch.setattr(
        evaluator_module.DataCache,
        "enable",
        staticmethod(lambda: calls.append("enable") or _Cache()),
    )
    monkeypatch.setattr(
        evaluator_module.DataCache,
        "disable",
        staticmethod(lambda: calls.append("disable")),
    )
    monkeypatch.setattr(
        evaluator_module.DataCache,
        "get_instance",
        staticmethod(lambda: _Cache()),
    )
    monkeypatch.setattr(
        StrategyEvaluator,
        "_evaluate_batch_internal",
        lambda self, candidates, top_k=None: [],
    )

    evaluator = StrategyEvaluator(shared_config_dict={"stock_codes": ["7203"]})
    result = evaluator.evaluate_batch([_candidate()], enable_cache=True)

    assert result == []
    assert calls == ["enable", "disable"]


def test_evaluate_batch_internal_uses_prepare_and_execute(monkeypatch) -> None:
    prepared = object()
    expected = [EvaluationResult(candidate=_candidate(), score=0.5)]

    def _fake_execute_batch_evaluation(
        candidates,
        max_workers,
        prepared_data,
        shared_config_dict,
        scoring_weights,
        timeout_seconds,
    ):
        _ = (
            candidates,
            max_workers,
            prepared_data,
            shared_config_dict,
            scoring_weights,
            timeout_seconds,
        )
        return expected

    monkeypatch.setattr(evaluator_module, "get_max_workers", lambda n_jobs: 4)
    monkeypatch.setattr(
        evaluator_module,
        "prepare_batch_data",
        lambda shared_config_dict: prepared,
    )
    monkeypatch.setattr(
        evaluator_module,
        "execute_batch_evaluation",
        _fake_execute_batch_evaluation,
    )
    monkeypatch.setattr(
        StrategyEvaluator,
        "_finalize_batch_results",
        lambda self, results, top_k: results,
    )

    evaluator = StrategyEvaluator(shared_config_dict={"stock_codes": ["7203"]}, n_jobs=4)
    result = evaluator._evaluate_batch_internal([_candidate()], top_k=3)

    assert result == expected


def test_finalize_batch_results_sorts_and_applies_topk(monkeypatch) -> None:
    c1 = _candidate()
    c2 = StrategyCandidate(
        strategy_id="c2", entry_filter_params={}, exit_trigger_params={}
    )
    c3 = StrategyCandidate(
        strategy_id="c3", entry_filter_params={}, exit_trigger_params={}
    )
    success_low = EvaluationResult(candidate=c1, score=0.1, success=True)
    success_high = EvaluationResult(candidate=c2, score=0.9, success=True)
    failed = EvaluationResult(
        candidate=c3,
        score=-999.0,
        success=False,
        error_message="x",
    )

    monkeypatch.setattr(
        evaluator_module,
        "normalize_scores",
        lambda successful, scoring_weights: successful,
    )

    evaluator = StrategyEvaluator(shared_config_dict={"stock_codes": ["7203"]})
    result = evaluator._finalize_batch_results(
        [success_low, failed, success_high],
        top_k=2,
    )

    assert [r.candidate.strategy_id for r in result] == ["c2", "test"]
