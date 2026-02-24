"""parameter_evolver.py のテスト (GA操作のユニットテスト)"""

import random
from unittest.mock import patch

import pytest


from src.domains.lab_agent.models import (
    EvaluationResult,
    EvolutionConfig,
    StrategyCandidate,
)
from src.domains.lab_agent.parameter_evolver import ParameterEvolver


def _make_candidate(sid="test", entry_params=None, exit_params=None):
    if entry_params is None:
        entry_params = {
            "volume": {"enabled": True, "threshold": 1.5, "short_period": 20, "long_period": 100},
        }
    if exit_params is None:
        exit_params = {}
    return StrategyCandidate(
        strategy_id=sid,
        entry_filter_params=entry_params,
        exit_trigger_params=exit_params,
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

    def test_mutate_skips_non_dict_params(self):
        evolver = _make_evolver()
        candidate = _make_candidate(entry_params={"volume": "invalid"})
        mutated = evolver._mutate(candidate, mutation_strength=0.5)
        assert mutated.entry_filter_params["volume"] == "invalid"

    def test_mutate_includes_exit_params(self):
        evolver = _make_evolver()
        random.seed(42)
        candidate = _make_candidate(
            entry_params={},
            exit_params={
                "volume": {
                    "enabled": True,
                    "threshold": 1.5,
                    "short_period": 20,
                    "long_period": 100,
                },
            },
        )
        mutated = evolver._mutate(candidate, mutation_strength=1.0)
        assert "volume" in mutated.exit_trigger_params
        assert mutated.metadata.get("mutated") is True

    def test_is_signal_mutation_allowed_entry_filter_only(self):
        config = EvolutionConfig(entry_filter_only=True)
        evolver = ParameterEvolver(
            config=config,
            shared_config_dict={"initial_cash": 10000000, "stock_codes": ["7203"]},
        )
        assert evolver._is_signal_mutation_allowed("volume", usage_type="entry") is True
        assert evolver._is_signal_mutation_allowed("volume", usage_type="exit") is False

    def test_is_signal_mutation_allowed_exit_trigger_only(self):
        config = EvolutionConfig(target_scope="exit_trigger_only")
        evolver = ParameterEvolver(
            config=config,
            shared_config_dict={"initial_cash": 10000000, "stock_codes": ["7203"]},
        )
        assert evolver._is_signal_mutation_allowed("volume", usage_type="entry") is False
        assert evolver._is_signal_mutation_allowed("volume", usage_type="exit") is True

    def test_is_signal_mutation_allowed_allowed_categories(self):
        config = EvolutionConfig(allowed_categories=["fundamental"])
        evolver = ParameterEvolver(
            config=config,
            shared_config_dict={"initial_cash": 10000000, "stock_codes": ["7203"]},
        )
        assert evolver._is_signal_mutation_allowed("volume", usage_type="entry") is False
        assert evolver._is_signal_mutation_allowed(
            "fundamental", usage_type="entry"
        ) is True

    def test_random_add_mutation_passes_allowed_categories(self):
        config = EvolutionConfig(
            structure_mode="random_add",
            random_add_entry_signals=1,
            random_add_exit_signals=0,
            allowed_categories=["fundamental"],
        )
        evolver = ParameterEvolver(
            config=config,
            shared_config_dict={"initial_cash": 10000000, "stock_codes": ["7203"]},
        )
        evolver._base_entry_signals = {"volume"}
        evolver._base_exit_signals = set()
        candidate = _make_candidate()

        with patch(
            "src.domains.lab_agent.parameter_evolver.apply_random_add_structure",
            return_value=(candidate, {"entry": [], "exit": []}),
        ) as mock_random_add:
            evolver._mutate(candidate, mutation_strength=0.0)

        assert mock_random_add.call_args.kwargs["allowed_categories"] == {
            "fundamental"
        }

    def test_effective_random_add_counts_by_target_scope(self):
        entry_only = ParameterEvolver(
            config=EvolutionConfig(
                target_scope="entry_filter_only",
                random_add_entry_signals=3,
                random_add_exit_signals=4,
            ),
            shared_config_dict={"initial_cash": 10000000, "stock_codes": ["7203"]},
        )
        assert entry_only._effective_random_add_counts() == (3, 0)

        exit_only = ParameterEvolver(
            config=EvolutionConfig(
                target_scope="exit_trigger_only",
                random_add_entry_signals=3,
                random_add_exit_signals=4,
            ),
            shared_config_dict={"initial_cash": 10000000, "stock_codes": ["7203"]},
        )
        assert exit_only._effective_random_add_counts() == (0, 4)


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

    def test_crossover_handles_missing_entry_signal_in_selected_parent(self):
        evolver = _make_evolver()
        parent1 = _make_candidate("p1", entry_params={"volume": {"enabled": True}})
        parent2 = _make_candidate("p2", entry_params={})
        with patch("src.domains.lab_agent.parameter_evolver.random.random", return_value=0.8):
            child = evolver._crossover(parent1, parent2)
        assert child.entry_filter_params == {}

    def test_crossover_copies_exit_signal(self):
        evolver = _make_evolver()
        parent1 = _make_candidate(
            "p1",
            entry_params={},
            exit_params={"rsi_threshold": {"enabled": True, "period": 14}},
        )
        parent2 = _make_candidate("p2", entry_params={}, exit_params={})
        with patch("src.domains.lab_agent.parameter_evolver.random.random", return_value=0.2):
            child = evolver._crossover(parent1, parent2)
        assert "rsi_threshold" in child.exit_trigger_params


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


class TestEvolveFlow:
    def test_load_base_strategy_from_name(self):
        evolver = ParameterEvolver(config=EvolutionConfig(), shared_config_dict=None)
        with patch("src.domains.lab_agent.parameter_evolver.ConfigLoader") as MockLoader:
            MockLoader.return_value.load_strategy_config.return_value = {
                "entry_filter_params": {"volume": {"enabled": True}},
                "exit_trigger_params": {"rsi_threshold": {"enabled": True}},
                "shared_config": {"dataset": "demo"},
            }
            MockLoader.return_value.merge_shared_config.return_value = {"dataset": "demo"}

            candidate = evolver._load_base_strategy("demo_strategy")

        assert candidate.strategy_id == "base_demo_strategy"
        assert candidate.metadata["base_strategy"] == "demo_strategy"
        assert evolver.shared_config_dict == {"dataset": "demo"}
        assert evolver.evaluator.shared_config_dict == {"dataset": "demo"}

    def test_load_base_strategy_keeps_existing_shared_config(self):
        evolver = ParameterEvolver(
            config=EvolutionConfig(),
            shared_config_dict={"dataset": "preconfigured"},
        )
        with patch("src.domains.lab_agent.parameter_evolver.ConfigLoader") as MockLoader:
            MockLoader.return_value.load_strategy_config.return_value = {
                "entry_filter_params": {"volume": {"enabled": True}},
                "exit_trigger_params": {"rsi_threshold": {"enabled": True}},
                "shared_config": {"dataset": "demo"},
            }

            candidate = evolver._load_base_strategy("demo_strategy")

        MockLoader.return_value.merge_shared_config.assert_not_called()
        assert candidate.strategy_id == "base_demo_strategy"
        assert evolver.shared_config_dict == {"dataset": "preconfigured"}

    def test_evolve_success(self):
        evolver = _make_evolver(pop_size=10, generations=2)
        base = _make_candidate("base")
        population_g1 = [_make_candidate("g1_a"), _make_candidate("g1_b")]
        population_g2 = [_make_candidate("g2_a"), _make_candidate("g2_b")]

        results_g1 = [
            _make_result(population_g1[0], score=0.3),
            _make_result(population_g1[1], score=0.4),
        ]
        results_g2 = [
            _make_result(population_g2[0], score=0.8),
            _make_result(population_g2[1], score=0.2),
        ]

        with (
            patch.object(evolver, "_load_base_strategy", return_value=base),
            patch.object(evolver, "_initialize_population", return_value=population_g1),
            patch.object(
                evolver.evaluator,
                "evaluate_batch",
                side_effect=[results_g1, results_g2],
            ),
            patch.object(evolver, "_evolve_population", return_value=population_g2),
        ):
            best_candidate, all_results = evolver.evolve("demo_strategy")

        assert best_candidate.strategy_id == "g2_a"
        assert len(all_results) == 4
        assert len(evolver.history) == 2
        assert evolver.history[-1]["best_score"] == 0.8

    def test_evolve_keeps_best_when_later_generation_is_worse(self):
        evolver = _make_evolver(pop_size=10, generations=2)
        base = _make_candidate("base")
        population_g1 = [_make_candidate("g1_best"), _make_candidate("g1_other")]
        population_g2 = [_make_candidate("g2_worse"), _make_candidate("g2_other")]
        results_g1 = [
            _make_result(population_g1[0], score=0.9),
            _make_result(population_g1[1], score=0.4),
        ]
        results_g2 = [
            _make_result(population_g2[0], score=0.6),
            _make_result(population_g2[1], score=0.5),
        ]

        with (
            patch.object(evolver, "_load_base_strategy", return_value=base),
            patch.object(evolver, "_initialize_population", return_value=population_g1),
            patch.object(
                evolver.evaluator,
                "evaluate_batch",
                side_effect=[results_g1, results_g2],
            ),
            patch.object(evolver, "_evolve_population", return_value=population_g2),
        ):
            best_candidate, _ = evolver.evolve("demo_strategy")

        assert best_candidate.strategy_id == "g1_best"
        assert len(evolver.history) == 2

    def test_evolve_raises_when_no_successful_results(self):
        evolver = _make_evolver(pop_size=10, generations=2)
        base = _make_candidate("base")
        failed = _make_result(base, score=-999.0)
        failed.success = False

        with (
            patch.object(evolver, "_load_base_strategy", return_value=base),
            patch.object(evolver, "_initialize_population", return_value=[base]),
            patch.object(
                evolver.evaluator,
                "evaluate_batch",
                side_effect=[[failed], [failed]],
            ),
        ):
            with pytest.raises(RuntimeError, match="no successful evaluations"):
                evolver.evolve("demo_strategy")
