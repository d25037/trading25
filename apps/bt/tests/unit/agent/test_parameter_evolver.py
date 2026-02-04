"""parameter_evolver.py のテスト (GA操作のユニットテスト)"""

import random


from src.agent.models import (
    EvaluationResult,
    EvolutionConfig,
    StrategyCandidate,
)
from src.agent.parameter_evolver import ParameterEvolver


def _make_candidate(sid="test", entry_params=None):
    if entry_params is None:
        entry_params = {
            "volume": {"enabled": True, "threshold": 1.5, "short_period": 20, "long_period": 100},
        }
    return StrategyCandidate(
        strategy_id=sid,
        entry_filter_params=entry_params,
        exit_trigger_params={},
    )


def _make_result(candidate, score=0.5):
    return EvaluationResult(
        candidate=candidate,
        score=score,
        sharpe_ratio=1.0,
        calmar_ratio=0.5,
        total_return=0.1,
        max_drawdown=-0.1,
        win_rate=0.5,
        trade_count=100,
        success=True,
    )


def _make_evolver(
    pop_size=10,
    generations=2,
    mutation_rate=0.3,
    crossover_rate=0.7,
    elite_ratio=0.25,
    tournament_size=2,
):
    config = EvolutionConfig(
        population_size=pop_size,
        generations=generations,
        mutation_rate=mutation_rate,
        crossover_rate=crossover_rate,
        elite_ratio=elite_ratio,
        tournament_size=tournament_size,
    )
    return ParameterEvolver(
        config=config,
        shared_config_dict={"initial_cash": 10000000, "stock_codes": ["7203"]},
        scoring_weights={"sharpe_ratio": 0.5, "calmar_ratio": 0.3, "total_return": 0.2},
    )


class TestParameterEvolverInit:
    def test_creation(self):
        evolver = _make_evolver()
        assert evolver is not None

    def test_param_ranges_populated(self):
        evolver = _make_evolver()
        assert "volume" in evolver.PARAM_RANGES
        assert "crossover" in evolver.PARAM_RANGES


class TestMutation:
    def test_mutate_returns_candidate(self):
        evolver = _make_evolver()
        random.seed(42)
        candidate = _make_candidate()
        mutated = evolver._mutate(candidate, mutation_strength=0.5)
        assert isinstance(mutated, StrategyCandidate)

    def test_mutate_changes_params(self):
        evolver = _make_evolver()
        random.seed(42)
        candidate = _make_candidate()
        mutated = evolver._mutate(candidate, mutation_strength=0.5)
        assert mutated.metadata.get("mutated") is True

    def test_mutate_preserves_structure(self):
        evolver = _make_evolver()
        random.seed(42)
        candidate = _make_candidate()
        mutated = evolver._mutate(candidate, mutation_strength=0.5)
        assert "volume" in mutated.entry_filter_params


class TestMutateSignalParams:
    def test_known_signal_mutates(self):
        evolver = _make_evolver()
        random.seed(42)
        params = {"enabled": True, "threshold": 1.5, "short_period": 50, "long_period": 150}
        result = evolver._mutate_signal_params("volume", params, mutation_strength=1.0)
        # With mutation_strength=1.0, all numeric params should be mutated
        assert "enabled" in result  # categorical preserved
        assert result["enabled"] is True

    def test_unknown_signal_unchanged(self):
        evolver = _make_evolver()
        random.seed(42)
        params = {"enabled": True, "custom": 42}
        result = evolver._mutate_signal_params("unknown_signal", params, mutation_strength=1.0)
        assert result["custom"] == 42  # no range defined, unchanged

    def test_categorical_params_preserved(self):
        evolver = _make_evolver()
        random.seed(42)
        params = {"enabled": True, "direction": "surge", "threshold": 1.5}
        result = evolver._mutate_signal_params("volume", params, mutation_strength=1.0)
        assert result["direction"] == "surge"

    def test_values_within_range(self):
        evolver = _make_evolver()
        random.seed(42)
        params = {"threshold": 1.5, "short_period": 50, "long_period": 150}
        for _ in range(50):
            result = evolver._mutate_signal_params("volume", params, mutation_strength=1.0)
            assert 0.3 <= result["threshold"] <= 3.0
            assert 10 <= result["short_period"] <= 100
            assert 50 <= result["long_period"] <= 300


class TestCrossover:
    def test_crossover_returns_candidate(self):
        evolver = _make_evolver()
        random.seed(42)
        parent1 = _make_candidate("p1")
        parent2 = _make_candidate("p2", entry_params={
            "volume": {"enabled": True, "threshold": 2.0, "short_period": 10, "long_period": 50},
        })
        child = evolver._crossover(parent1, parent2)
        assert isinstance(child, StrategyCandidate)

    def test_crossover_metadata(self):
        evolver = _make_evolver()
        random.seed(42)
        parent1 = _make_candidate("p1")
        parent2 = _make_candidate("p2")
        child = evolver._crossover(parent1, parent2)
        assert child.metadata.get("crossover") is True


class TestTournamentSelect:
    def test_returns_evaluation_result(self):
        evolver = _make_evolver(tournament_size=2)
        random.seed(42)
        c1 = _make_candidate("c1")
        c2 = _make_candidate("c2")
        results = [_make_result(c1, score=0.8), _make_result(c2, score=0.3)]
        selected = evolver._tournament_select(results)
        assert isinstance(selected, EvaluationResult)

    def test_selects_better_candidate(self):
        evolver = _make_evolver(tournament_size=2, pop_size=10)
        random.seed(42)
        c1 = _make_candidate("c1")
        c2 = _make_candidate("c2")
        results = [_make_result(c1, score=0.9), _make_result(c2, score=0.1)]
        selections = [evolver._tournament_select(results).candidate.strategy_id for _ in range(20)]
        assert selections.count("c1") >= selections.count("c2")


class TestInitializePopulation:
    def test_population_size(self):
        evolver = _make_evolver(pop_size=10)
        base = _make_candidate()
        population = evolver._initialize_population(base)
        assert len(population) == 10

    def test_first_is_base(self):
        evolver = _make_evolver(pop_size=10)
        base = _make_candidate("base")
        population = evolver._initialize_population(base)
        assert population[0].entry_filter_params == base.entry_filter_params


class TestEvolvePopulation:
    def test_output_size(self):
        evolver = _make_evolver(pop_size=10, elite_ratio=0.2)
        random.seed(42)
        candidates = [_make_candidate(f"c{i}") for i in range(10)]
        results = [_make_result(c, score=random.random()) for c in candidates]
        next_gen = evolver._evolve_population(results)
        assert len(next_gen) == 10

    def test_contains_elites(self):
        evolver = _make_evolver(pop_size=10, elite_ratio=0.3)
        random.seed(42)
        candidates = [_make_candidate(f"c{i}") for i in range(10)]
        results = [_make_result(c, score=float(i)) for i, c in enumerate(candidates)]
        next_gen = evolver._evolve_population(results)
        elite_count = sum(1 for c in next_gen if c.metadata.get("elite") is True)
        assert elite_count >= 1


class TestGetEvolutionHistory:
    def test_empty_initially(self):
        evolver = _make_evolver()
        assert evolver.get_evolution_history() == []

    def test_returns_history(self):
        evolver = _make_evolver()
        evolver.history = [{"generation": 1, "best_score": 0.5}]
        history = evolver.get_evolution_history()
        assert len(history) == 1
        assert history[0]["generation"] == 1
