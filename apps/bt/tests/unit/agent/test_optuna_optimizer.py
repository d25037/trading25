"""optuna_optimizer.py のテスト"""

from unittest.mock import MagicMock, patch

import pytest

from src.agent.models import OptunaConfig, StrategyCandidate
from src.agent.optuna_optimizer import OptunaOptimizer, _extract_enabled_signal_names


def _make_candidate(sid: str = "test_strat"):
    return StrategyCandidate(
        strategy_id=sid,
        entry_filter_params={
            "volume": {"enabled": True, "threshold": 1.5, "short_period": 20, "long_period": 100},
        },
        exit_trigger_params={},
    )


def _make_optimizer(
    n_trials=10,
    timeout=60,
    sampler="tpe",
    entry_filter_only=False,
    target_scope="both",
    allowed_categories=None,
):
    config = OptunaConfig(
        n_trials=n_trials,
        timeout_seconds=timeout,
        sampler=sampler,
        entry_filter_only=entry_filter_only,
        target_scope=target_scope,
        allowed_categories=allowed_categories or [],
    )
    return OptunaOptimizer(
        config=config,
        shared_config_dict={"initial_cash": 10000000, "stock_codes": ["7203"]},
        scoring_weights={"sharpe_ratio": 0.5, "calmar_ratio": 0.3, "total_return": 0.2},
    )


class TestOptunaOptimizerInit:
    def test_creation(self):
        optimizer = _make_optimizer()
        assert optimizer is not None

    def test_param_ranges(self):
        optimizer = _make_optimizer()
        assert "volume" in optimizer.PARAM_RANGES

    def test_default_config(self):
        optimizer = OptunaOptimizer()
        assert optimizer.config.n_trials == 100
        assert optimizer.config.sampler == "tpe"

    def test_default_scoring_weights(self):
        optimizer = OptunaOptimizer()
        assert "sharpe_ratio" in optimizer.scoring_weights
        assert "calmar_ratio" in optimizer.scoring_weights

    def test_load_base_strategy_from_name(self):
        optimizer = _make_optimizer()
        optimizer.shared_config_dict = None
        with patch("src.agent.optuna_optimizer.ConfigLoader") as MockLoader:
            MockLoader.return_value.load_strategy_config.return_value = {
                "entry_filter_params": {"volume": {"enabled": True}},
                "exit_trigger_params": {"rsi_threshold": {"enabled": True}},
                "shared_config": {"dataset": "demo"},
            }
            MockLoader.return_value.merge_shared_config.return_value = {"dataset": "demo"}

            candidate = optimizer._load_base_strategy("demo_strategy")

        assert candidate.strategy_id == "base_demo_strategy"
        assert candidate.metadata["base_strategy"] == "demo_strategy"
        assert optimizer.shared_config_dict == {"dataset": "demo"}

    def test_creation_raises_when_optuna_unavailable(self):
        with patch("src.agent.optuna_optimizer.OPTUNA_AVAILABLE", False):
            with pytest.raises(ImportError, match="not installed"):
                OptunaOptimizer()


class TestCreateSampler:
    def test_tpe_sampler(self):
        optimizer = _make_optimizer(sampler="tpe")
        sampler = optimizer._create_sampler()
        assert sampler is not None

    def test_random_sampler(self):
        optimizer = _make_optimizer(sampler="random")
        sampler = optimizer._create_sampler()
        assert sampler is not None

    def test_cmaes_sampler(self):
        optimizer = _make_optimizer(sampler="cmaes")
        sampler = optimizer._create_sampler()
        assert sampler is not None

    def test_unknown_sampler_defaults_to_tpe(self):
        optimizer = _make_optimizer(sampler="unknown")
        sampler = optimizer._create_sampler()
        # Should default to TPESampler
        assert sampler is not None

    def test_raises_when_optuna_unavailable(self):
        optimizer = _make_optimizer()
        with patch("src.agent.optuna_optimizer.OPTUNA_AVAILABLE", False):
            with pytest.raises(ImportError, match="not available"):
                optimizer._create_sampler()


class TestOptimizeFlow:
    def test_optimize_calls_progress_callback(self):
        import optuna

        optimizer = _make_optimizer(n_trials=10)
        study = MagicMock()
        trial = MagicMock()
        trial.state = optuna.trial.TrialState.COMPLETE
        study.trials = [trial]
        study.best_trial = MagicMock()
        study.best_value = 1.23
        study.best_params = {"entry_volume_threshold": 1.8}

        def fake_optimize(_objective, n_trials, n_jobs, show_progress_bar, callbacks):
            assert n_trials == 10
            assert n_jobs == -1
            assert show_progress_bar is True
            for cb in callbacks:
                cb(study, trial)

        study.optimize.side_effect = fake_optimize
        progress_calls = []

        with (
            patch.object(optimizer, "_load_base_strategy", return_value=_make_candidate("base")),
            patch.object(optimizer, "_create_sampler", return_value=MagicMock()),
            patch.object(
                optimizer,
                "_build_candidate_from_params",
                return_value=_make_candidate("best"),
            ) as mock_build,
            patch("src.agent.optuna_optimizer.optuna_runtime.create_study", return_value=study),
        ):
            best_candidate, result_study = optimizer.optimize(
                "demo_strategy",
                progress_callback=lambda completed, total, best: progress_calls.append(
                    (completed, total, best)
                ),
            )

        assert best_candidate.strategy_id == "best"
        assert result_study is study
        assert progress_calls == [(1, 10, 1.23)]
        mock_build.assert_called_once_with({"entry_volume_threshold": 1.8})

    def test_optimize_without_progress_callback(self):
        optimizer = _make_optimizer(n_trials=10)
        study = MagicMock()
        study.trials = []
        study.best_trial = MagicMock()
        study.best_value = 0.42
        study.best_params = {}

        with (
            patch.object(optimizer, "_load_base_strategy", return_value=_make_candidate("base")),
            patch.object(optimizer, "_create_sampler", return_value=MagicMock()),
            patch.object(optimizer, "_build_candidate_from_params", return_value=_make_candidate()),
            patch("src.agent.optuna_optimizer.optuna_runtime.create_study", return_value=study),
        ):
            optimizer.optimize("demo_strategy", progress_callback=None)

    def test_optimize_applies_random_add_structure_when_enabled(self):
        optimizer = _make_optimizer(n_trials=10, target_scope="entry_filter_only")
        optimizer.config.structure_mode = "random_add"
        optimizer.config.random_add_entry_signals = 1
        optimizer.config.random_add_exit_signals = 0
        optimizer.config.seed = 7

        study = MagicMock()
        study.optimize.side_effect = lambda *args, **kwargs: None
        study.best_trial = MagicMock()
        study.best_value = 0.5
        study.best_params = {}

        base = _make_candidate("base")
        augmented = _make_candidate("base_augmented")
        augmented.entry_filter_params["period_breakout"] = {"enabled": True, "period": 20}

        with (
            patch.object(optimizer, "_load_base_strategy", return_value=base),
            patch.object(optimizer, "_create_sampler", return_value=MagicMock()),
            patch.object(optimizer, "_build_candidate_from_params", return_value=_make_candidate("best")),
            patch(
                "src.agent.optuna_optimizer.apply_random_add_structure",
                return_value=(augmented, {"entry": ["period_breakout"], "exit": []}),
            ) as mock_random_add,
            patch("src.agent.optuna_optimizer.optuna_runtime.create_study", return_value=study),
        ):
            optimizer.optimize("demo_strategy", progress_callback=None)

        mock_random_add.assert_called_once()
        assert "period_breakout" in optimizer.base_entry_params


class TestSampleParams:
    def test_samples_numeric_params(self):
        optimizer = _make_optimizer()
        optimizer.base_entry_params = {
            "volume": {"enabled": True, "threshold": 1.5, "short_period": 20, "long_period": 100, "direction": "surge"},
        }
        mock_trial = MagicMock()
        mock_trial.suggest_float.return_value = 2.0
        mock_trial.suggest_int.return_value = 50
        result = optimizer._sample_params(mock_trial, optimizer.base_entry_params, "entry")
        assert "volume" in result
        # categorical params should be preserved
        assert result["volume"]["enabled"] is True
        assert result["volume"]["direction"] == "surge"

    def test_skips_non_dict_params(self):
        optimizer = _make_optimizer()
        result = optimizer._sample_params(MagicMock(), {"volume": "not_a_dict"}, "entry")
        assert result == {}

    def test_preserves_unknown_params(self):
        optimizer = _make_optimizer()
        optimizer.base_entry_params = {
            "volume": {"enabled": True, "custom_param": 42},
        }
        mock_trial = MagicMock()
        result = optimizer._sample_params(mock_trial, optimizer.base_entry_params, "entry")
        # custom_param is not in PARAM_RANGES, should be preserved as-is
        assert result["volume"]["custom_param"] == 42

    def test_entry_filter_only_skips_exit_sampling(self):
        optimizer = _make_optimizer(entry_filter_only=True)
        base_exit = {
            "volume": {"enabled": True, "threshold": 1.5, "short_period": 20, "long_period": 100},
        }
        mock_trial = MagicMock()
        result = optimizer._sample_params(mock_trial, base_exit, "exit")
        assert result["volume"]["threshold"] == 1.5
        mock_trial.suggest_float.assert_not_called()
        mock_trial.suggest_int.assert_not_called()

    def test_exit_trigger_only_skips_entry_sampling(self):
        optimizer = _make_optimizer(target_scope="exit_trigger_only")
        base_entry = {
            "volume": {"enabled": True, "threshold": 1.5, "short_period": 20, "long_period": 100},
        }
        mock_trial = MagicMock()
        result = optimizer._sample_params(mock_trial, base_entry, "entry")
        assert result["volume"]["threshold"] == 1.5
        mock_trial.suggest_float.assert_not_called()
        mock_trial.suggest_int.assert_not_called()

    def test_effective_random_add_counts_by_target_scope(self):
        entry_only = _make_optimizer(
            target_scope="entry_filter_only",
        )
        entry_only.config.random_add_entry_signals = 2
        entry_only.config.random_add_exit_signals = 5
        assert entry_only._effective_random_add_counts() == (2, 0)

        exit_only = _make_optimizer(
            target_scope="exit_trigger_only",
        )
        exit_only.config.random_add_entry_signals = 2
        exit_only.config.random_add_exit_signals = 5
        assert exit_only._effective_random_add_counts() == (0, 5)

    def test_allowed_categories_skip_disallowed_signals(self):
        optimizer = _make_optimizer(allowed_categories=["fundamental"])
        base_entry = {
            "volume": {"enabled": True, "threshold": 1.5, "short_period": 20, "long_period": 100},
        }
        mock_trial = MagicMock()
        result = optimizer._sample_params(mock_trial, base_entry, "entry")
        assert result["volume"]["threshold"] == 1.5
        mock_trial.suggest_float.assert_not_called()
        mock_trial.suggest_int.assert_not_called()

    def test_samples_nested_fundamental_params(self):
        optimizer = _make_optimizer()
        base_entry = {
            "fundamental": {
                "enabled": True,
                "per": {"enabled": True, "threshold": 15.0},
                "use_adjusted": True,
            }
        }
        mock_trial = MagicMock()
        mock_trial.suggest_float.return_value = 12.3

        result = optimizer._sample_params(mock_trial, base_entry, "entry")

        assert result["fundamental"]["per"]["threshold"] == 12.3
        assert result["fundamental"]["per"]["enabled"] is True
        assert result["fundamental"]["use_adjusted"] is True


class TestBuildCandidateFromParams:
    def test_builds_candidate(self):
        optimizer = _make_optimizer()
        params = {
            "entry_volume_threshold": 2.0,
            "entry_volume_short_period": 15,
            "entry_volume_long_period": 80,
        }
        optimizer.base_entry_params = {"volume": {"enabled": True, "threshold": 1.5, "short_period": 20, "long_period": 100}}
        optimizer.base_exit_params = {}
        candidate = optimizer._build_candidate_from_params(params)
        assert isinstance(candidate, StrategyCandidate)
        assert candidate.entry_filter_params["volume"]["threshold"] == 2.0

    def test_preserves_base_params(self):
        optimizer = _make_optimizer()
        optimizer.base_entry_params = {"volume": {"enabled": True, "threshold": 1.5}}
        optimizer.base_exit_params = {"rsi_threshold": {"enabled": True, "period": 14}}
        candidate = optimizer._build_candidate_from_params({})
        assert candidate.entry_filter_params["volume"]["enabled"] is True
        assert candidate.exit_trigger_params["rsi_threshold"]["period"] == 14

    def test_metadata_set(self):
        optimizer = _make_optimizer()
        optimizer.base_entry_params = {}
        optimizer.base_exit_params = {}
        candidate = optimizer._build_candidate_from_params({})
        assert candidate.metadata["optimization_method"] == "optuna"

    def test_updates_exit_params_from_flat_keys(self):
        optimizer = _make_optimizer()
        optimizer.base_entry_params = {}
        optimizer.base_exit_params = {
            "trading_value_range": {"enabled": True, "period": 10},
        }
        candidate = optimizer._build_candidate_from_params(
            {"exit_trading_value_range_period": 25}
        )
        assert candidate.exit_trigger_params["trading_value_range"]["period"] == 25

    def test_build_candidate_updates_nested_path_and_ignores_malformed_key(self):
        optimizer = _make_optimizer()
        optimizer.base_entry_params = {
            "fundamental": {
                "enabled": True,
                "per": {"enabled": True, "threshold": 10.0},
            }
        }
        optimizer.base_exit_params = {}

        candidate = optimizer._build_candidate_from_params(
            {
                "entry_fundamental_per__threshold": 22.5,
                "malformed": 1.0,
            }
        )

        assert candidate.entry_filter_params["fundamental"]["per"]["threshold"] == 22.5

    def test_set_nested_param_creates_missing_child_dict(self):
        optimizer = _make_optimizer()
        params: dict[str, object] = {}

        optimizer._set_nested_param(params, "new.path.value", 3.14)

        assert params == {"new": {"path": {"value": 3.14}}}


class TestObjective:
    def test_objective_success(self):
        optimizer = _make_optimizer()
        optimizer.base_entry_params = {"volume": {"enabled": True}}
        optimizer.base_exit_params = {"rsi_threshold": {"enabled": True}}

        trial = MagicMock()
        trial.number = 1

        portfolio = MagicMock()
        portfolio.sharpe_ratio.return_value = 1.5
        portfolio.calmar_ratio.return_value = 0.8
        portfolio.total_return.return_value = 0.2

        strategy_instance = MagicMock()
        strategy_instance.run_optimized_backtest_kelly.return_value = (
            None,
            portfolio,
            None,
            None,
            None,
        )

        with (
            patch.object(
                optimizer,
                "_sample_params",
                side_effect=[
                    {"volume": {"enabled": True}},
                    {"rsi_threshold": {"enabled": True}},
                ],
            ),
            patch(
                "src.agent.optuna_optimizer.YamlConfigurableStrategy",
                return_value=strategy_instance,
            ),
        ):
            score = optimizer._objective(trial)

        assert score == 1.03
        trial.set_user_attr.assert_any_call("sharpe_ratio", 1.5)
        trial.set_user_attr.assert_any_call("calmar_ratio", 0.8)
        trial.set_user_attr.assert_any_call("total_return", 0.2)

    def test_objective_failure_returns_negative_score(self):
        optimizer = _make_optimizer()
        trial = MagicMock()
        trial.number = 99
        with patch.object(optimizer, "_sample_params", side_effect=RuntimeError("boom")):
            score = optimizer._objective(trial)
        assert score == -999.0

    def test_objective_uses_only_configured_scoring_weights(self):
        optimizer = _make_optimizer()
        optimizer.scoring_weights = {"sharpe_ratio": 1.0}
        optimizer.base_entry_params = {"volume": {"enabled": True}}
        optimizer.base_exit_params = {}

        trial = MagicMock()
        trial.number = 2

        portfolio = MagicMock()
        portfolio.sharpe_ratio.return_value = 1.2
        portfolio.calmar_ratio.return_value = 99.0
        portfolio.total_return.return_value = 99.0

        strategy_instance = MagicMock()
        strategy_instance.run_optimized_backtest_kelly.return_value = (
            None,
            portfolio,
            None,
            None,
            None,
        )

        with (
            patch.object(
                optimizer,
                "_sample_params",
                side_effect=[{"volume": {"enabled": True}}, {}],
            ),
            patch(
                "src.agent.optuna_optimizer.YamlConfigurableStrategy",
                return_value=strategy_instance,
            ),
        ):
            score = optimizer._objective(trial)

        assert score == 1.2


class TestGetOptimizationHistory:
    def test_empty_study(self):
        optimizer = _make_optimizer()
        mock_study = MagicMock()
        mock_study.trials = []
        history = optimizer.get_optimization_history(mock_study)
        assert history == []

    def test_with_trials(self):
        import optuna

        optimizer = _make_optimizer()
        mock_trial = MagicMock()
        mock_trial.number = 0
        mock_trial.value = 0.85
        mock_trial.state = optuna.trial.TrialState.COMPLETE
        mock_trial.params = {"entry_volume_threshold": 1.5}
        mock_trial.user_attrs = {
            "sharpe_ratio": 1.5,
            "calmar_ratio": 0.8,
            "total_return": 0.2,
        }
        mock_study = MagicMock()
        mock_study.trials = [mock_trial]
        history = optimizer.get_optimization_history(mock_study)
        assert len(history) == 1
        assert history[0]["trial"] == 0
        assert history[0]["score"] == 0.85

    def test_skips_failed_trials(self):
        import optuna

        optimizer = _make_optimizer()
        complete_trial = MagicMock()
        complete_trial.number = 0
        complete_trial.value = 0.5
        complete_trial.state = optuna.trial.TrialState.COMPLETE
        complete_trial.params = {}
        complete_trial.user_attrs = {}

        failed_trial = MagicMock()
        failed_trial.state = optuna.trial.TrialState.FAIL

        mock_study = MagicMock()
        mock_study.trials = [complete_trial, failed_trial]
        history = optimizer.get_optimization_history(mock_study)
        assert len(history) == 1


def test_extract_enabled_signal_names_handles_non_dict_and_disabled() -> None:
    params = {
        "buy_and_hold": True,
        "volume": {"enabled": True},
        "fundamental": {"enabled": False},
    }

    enabled = _extract_enabled_signal_names(params)

    assert enabled == {"buy_and_hold", "volume"}
