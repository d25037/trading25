"""optuna_optimizer.py のテスト"""

from unittest.mock import MagicMock

from src.agent.models import OptunaConfig, StrategyCandidate
from src.agent.optuna_optimizer import OptunaOptimizer


def _make_candidate():
    return StrategyCandidate(
        strategy_id="test_strat",
        entry_filter_params={
            "volume": {"enabled": True, "threshold": 1.5, "short_period": 20, "long_period": 100},
        },
        exit_trigger_params={},
    )


def _make_optimizer(n_trials=10, timeout=60, sampler="tpe"):
    config = OptunaConfig(
        n_trials=n_trials,
        timeout_seconds=timeout,
        sampler=sampler,
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
