"""config.py モデルのテスト"""

import pytest
from pydantic import ValidationError

from src.models.config import ParameterOptimizationConfig, SharedConfig, WalkForwardConfig


class TestParameterOptimizationConfig:
    def test_defaults(self):
        cfg = ParameterOptimizationConfig()
        assert cfg.enabled is False
        assert cfg.method == "grid_search"
        assert cfg.n_trials == 100
        assert cfg.n_jobs == -1
        assert "sharpe_ratio" in cfg.scoring_weights

    def test_valid_methods(self):
        for method in ["grid_search", "random_search"]:
            cfg = ParameterOptimizationConfig(method=method)
            assert cfg.method == method

    def test_invalid_method(self):
        with pytest.raises(ValidationError, match="method"):
            ParameterOptimizationConfig(method="bayesian")

    def test_custom_scoring_weights(self):
        weights = {"total_return": 1.0}
        cfg = ParameterOptimizationConfig(scoring_weights=weights)
        assert cfg.scoring_weights == weights


class TestWalkForwardConfig:
    def test_defaults(self):
        cfg = WalkForwardConfig()
        assert cfg.enabled is False
        assert cfg.train_window == 252
        assert cfg.test_window == 63
        assert cfg.step is None
        assert cfg.max_splits is None

    def test_custom_values(self):
        cfg = WalkForwardConfig(train_window=500, test_window=100, step=50, max_splits=10)
        assert cfg.train_window == 500
        assert cfg.step == 50
        assert cfg.max_splits == 10


class TestSharedConfig:
    def _make(self, **kwargs) -> SharedConfig:
        return SharedConfig.model_validate(
            kwargs, context={"resolve_stock_codes": False}
        )

    def test_defaults(self):
        cfg = self._make()
        assert cfg.initial_cash == 10000000
        assert cfg.fees == 0.001
        assert cfg.direction == "longonly"
        assert cfg.timeframe == "daily"
        assert cfg.kelly_fraction == 1.0

    def test_valid_directions(self):
        for d in ["longonly", "shortonly", "both"]:
            cfg = self._make(direction=d)
            assert cfg.direction == d

    def test_invalid_direction(self):
        with pytest.raises(ValidationError, match="direction"):
            self._make(direction="invalid")

    def test_initial_cash_zero(self):
        with pytest.raises(ValidationError, match="初期資金"):
            self._make(initial_cash=0)

    def test_initial_cash_negative(self):
        with pytest.raises(ValidationError, match="初期資金"):
            self._make(initial_cash=-1000)

    def test_fees_boundary_zero(self):
        cfg = self._make(fees=0.0)
        assert cfg.fees == 0.0

    def test_fees_boundary_just_below_one(self):
        cfg = self._make(fees=0.999)
        assert cfg.fees == 0.999

    def test_fees_negative(self):
        with pytest.raises(ValidationError, match="手数料"):
            self._make(fees=-0.01)

    def test_fees_one(self):
        with pytest.raises(ValidationError, match="手数料"):
            self._make(fees=1.0)

    def test_slippage_negative(self):
        with pytest.raises(ValidationError, match="コスト"):
            self._make(slippage=-0.01)

    def test_spread_one(self):
        with pytest.raises(ValidationError, match="コスト"):
            self._make(spread=1.0)

    def test_borrow_fee_negative(self):
        with pytest.raises(ValidationError, match="コスト"):
            self._make(borrow_fee=-0.1)

    def test_max_concurrent_positions_none(self):
        cfg = self._make(max_concurrent_positions=None)
        assert cfg.max_concurrent_positions is None

    def test_max_concurrent_positions_valid(self):
        cfg = self._make(max_concurrent_positions=10)
        assert cfg.max_concurrent_positions == 10

    def test_max_concurrent_positions_zero(self):
        with pytest.raises(ValidationError, match="max_concurrent_positions"):
            self._make(max_concurrent_positions=0)

    def test_max_concurrent_positions_negative(self):
        with pytest.raises(ValidationError, match="max_concurrent_positions"):
            self._make(max_concurrent_positions=-1)

    def test_max_exposure_none(self):
        cfg = self._make(max_exposure=None)
        assert cfg.max_exposure is None

    def test_max_exposure_valid(self):
        cfg = self._make(max_exposure=0.5)
        assert cfg.max_exposure == 0.5

    def test_max_exposure_boundary_one(self):
        cfg = self._make(max_exposure=1.0)
        assert cfg.max_exposure == 1.0

    def test_max_exposure_zero(self):
        with pytest.raises(ValidationError, match="max_exposure"):
            self._make(max_exposure=0.0)

    def test_max_exposure_above_one(self):
        with pytest.raises(ValidationError, match="max_exposure"):
            self._make(max_exposure=1.1)

    def test_timeframe_daily(self):
        cfg = self._make(timeframe="daily")
        assert cfg.timeframe == "daily"

    def test_timeframe_weekly(self):
        cfg = self._make(timeframe="weekly")
        assert cfg.timeframe == "weekly"

    def test_timeframe_invalid(self):
        with pytest.raises(ValidationError):
            self._make(timeframe="monthly")

    def test_nested_parameter_optimization(self):
        cfg = self._make()
        assert cfg.parameter_optimization is not None
        assert cfg.parameter_optimization.enabled is False

    def test_nested_walk_forward(self):
        cfg = self._make()
        assert cfg.walk_forward.enabled is False

    def test_resolve_stock_codes_skipped(self):
        cfg = self._make(stock_codes=["all"])
        assert cfg.stock_codes == ["all"]

    def test_specific_stock_codes(self):
        cfg = self._make(stock_codes=["7203", "6758"])
        assert cfg.stock_codes == ["7203", "6758"]
