"""Tests for SignalParams dynamic builder."""

from loguru import logger

from src.domains.optimization.param_builder import (
    _deep_merge,
    _unflatten_params,
    build_signal_params,
)
from src.shared.models.signals import (
    CrossoverSignalParams,
    FundamentalSignalParams,
    IndexOpenGapRegimeSignalParams,
    PeriodExtremaBreakSignalParams,
    SignalParams,
    VolumeRatioAboveSignalParams,
)


class TestParamBuilder:
    """SignalParams動的構築のテスト"""

    def test_build_signal_params_basic(self) -> None:
        base_params = SignalParams(
            period_extrema_break=PeriodExtremaBreakSignalParams(
                enabled=True,
                direction="high",
                lookback_days=10,
                period=100,
            )
        )

        grid_params = {
            "entry_filter_params.period_extrema_break.period": 200,
            "entry_filter_params.volume_ratio_above.ratio_threshold": 2.0,
        }

        result = build_signal_params(grid_params, "entry_filter_params", base_params)

        assert result.period_extrema_break is not None
        assert result.period_extrema_break.enabled is True
        assert result.period_extrema_break.direction == "high"
        assert result.period_extrema_break.period == 200

    def test_build_signal_params_preserve_base_settings(self) -> None:
        base_params = SignalParams(
            period_extrema_break=PeriodExtremaBreakSignalParams(
                enabled=True,
                direction="high",
                lookback_days=5,
                period=50,
            )
        )

        grid_params = {"entry_filter_params.period_extrema_break.period": 100}

        result = build_signal_params(grid_params, "entry_filter_params", base_params)

        assert result.period_extrema_break is not None
        assert result.period_extrema_break.enabled is True
        assert result.period_extrema_break.direction == "high"
        assert result.period_extrema_break.lookback_days == 5
        assert result.period_extrema_break.period == 100

    def test_build_signal_params_multiple_signals(self) -> None:
        base_params = SignalParams(
            period_extrema_break=PeriodExtremaBreakSignalParams(
                enabled=True,
                direction="high",
                lookback_days=10,
                period=100,
            ),
            volume_ratio_above=VolumeRatioAboveSignalParams(
                enabled=True,
                ratio_threshold=1.5,
                short_period=20,
                long_period=100,
                ma_type="ema",
            ),
        )

        grid_params = {
            "entry_filter_params.period_extrema_break.period": 200,
            "entry_filter_params.volume_ratio_above.ratio_threshold": 2.5,
        }

        result = build_signal_params(grid_params, "entry_filter_params", base_params)

        assert result.period_extrema_break is not None
        assert result.period_extrema_break.period == 200
        assert result.volume_ratio_above is not None
        assert result.volume_ratio_above.ratio_threshold == 2.5
        assert result.volume_ratio_above.ma_type == "ema"

    def test_build_signal_params_preserves_index_open_gap_regime(self) -> None:
        base_params = SignalParams(
            index_open_gap_regime=IndexOpenGapRegimeSignalParams(
                enabled=True,
                gap_threshold_1_pct=1.0,
                gap_threshold_2_pct=2.0,
                regime="down_medium",
            )
        )

        grid_params = {
            "entry_filter_params.index_open_gap_regime.gap_threshold_1_pct": 0.8,
            "entry_filter_params.index_open_gap_regime.regime": "down_large",
        }

        result = build_signal_params(grid_params, "entry_filter_params", base_params)

        assert result.index_open_gap_regime is not None
        assert result.index_open_gap_regime.enabled is True
        assert result.index_open_gap_regime.gap_threshold_1_pct == 0.8
        assert result.index_open_gap_regime.gap_threshold_2_pct == 2.0
        assert result.index_open_gap_regime.regime == "down_large"

    def test_build_signal_params_empty_grid(self) -> None:
        base_params = SignalParams(
            period_extrema_break=PeriodExtremaBreakSignalParams(
                enabled=True,
                direction="high",
                lookback_days=10,
                period=100,
            )
        )

        result = build_signal_params({}, "entry_filter_params", base_params)

        assert result.period_extrema_break is not None
        assert result.period_extrema_break.period == 100

    def test_build_signal_params_wrong_section(self) -> None:
        base_params = SignalParams(
            period_extrema_break=PeriodExtremaBreakSignalParams(
                enabled=True,
                direction="high",
                lookback_days=10,
                period=100,
            )
        )

        grid_params = {"exit_trigger_params.period_extrema_break.period": 200}

        result = build_signal_params(grid_params, "entry_filter_params", base_params)

        assert result.period_extrema_break is not None
        assert result.period_extrema_break.period == 100

    def test_validate_signal_names_mismatch_warning(self) -> None:
        base_params = SignalParams(
            crossover=CrossoverSignalParams(
                enabled=True,
                type="macd",
                direction="golden",
                fast_period=60,
                slow_period=130,
                signal_period=54,
            )
        )

        grid_params = {
            "entry_filter_params.macd_cross.fast_period": 36,
            "entry_filter_params.macd_cross.slow_period": 78,
        }

        log_messages: list[str] = []

        def log_sink(message: str) -> None:
            log_messages.append(message)

        handler_id = logger.add(log_sink, level="WARNING")

        try:
            result = build_signal_params(
                grid_params, "entry_filter_params", base_params
            )

            assert log_messages
            warning_text = "".join(log_messages)
            assert "macd_cross" in warning_text
            assert "ベースYAMLに存在しません" in warning_text
            assert result.crossover is not None
            assert result.crossover.fast_period == 60
            assert result.crossover.slow_period == 130
            assert result.crossover.signal_period == 54
        finally:
            logger.remove(handler_id)

    def test_validate_signal_names_match_no_warning(self) -> None:
        base_params = SignalParams(
            crossover=CrossoverSignalParams(
                enabled=True,
                type="macd",
                direction="golden",
                fast_period=60,
                slow_period=130,
                signal_period=54,
            )
        )

        grid_params = {
            "entry_filter_params.crossover.fast_period": 36,
            "entry_filter_params.crossover.slow_period": 78,
        }

        log_messages: list[str] = []

        def log_sink(message: str) -> None:
            log_messages.append(message)

        handler_id = logger.add(log_sink, level="WARNING")

        try:
            result = build_signal_params(
                grid_params, "entry_filter_params", base_params
            )

            assert log_messages == []
            assert result.crossover is not None
            assert result.crossover.fast_period == 36
            assert result.crossover.slow_period == 78
            assert result.crossover.signal_period == 54
            assert result.crossover.enabled is True
            assert result.crossover.type == "macd"
        finally:
            logger.remove(handler_id)

    def test_build_signal_params_nested_fundamental(self) -> None:
        base_params = SignalParams(
            fundamental=FundamentalSignalParams(
                enabled=True,
                per=FundamentalSignalParams.PERParams(
                    enabled=True,
                    threshold=15.0,
                ),
                pbr=FundamentalSignalParams.PBRParams(
                    enabled=True,
                    threshold=1.0,
                ),
                roe=FundamentalSignalParams.ROEParams(
                    enabled=True,
                    threshold=10.0,
                ),
            )
        )

        grid_params = {
            "entry_filter_params.fundamental.per.threshold": 20.0,
            "entry_filter_params.fundamental.pbr.threshold": 2.0,
        }

        result = build_signal_params(grid_params, "entry_filter_params", base_params)

        assert result.fundamental is not None
        assert result.fundamental.enabled is True
        assert result.fundamental.per.enabled is True
        assert result.fundamental.per.threshold == 20.0
        assert result.fundamental.pbr.enabled is True
        assert result.fundamental.pbr.threshold == 2.0
        assert result.fundamental.roe.threshold == 10.0

    def test_build_signal_params_nested_fundamental_with_periods(self) -> None:
        base_params = SignalParams(
            fundamental=FundamentalSignalParams(
                enabled=True,
                profit_growth=FundamentalSignalParams.ProfitGrowthParams(
                    enabled=True,
                    threshold=0.1,
                    periods=4,
                ),
            )
        )

        grid_params = {
            "entry_filter_params.fundamental.profit_growth.threshold": 0.2,
            "entry_filter_params.fundamental.profit_growth.periods": 8,
        }

        result = build_signal_params(grid_params, "entry_filter_params", base_params)

        assert result.fundamental is not None
        assert result.fundamental.profit_growth.enabled is True
        assert result.fundamental.profit_growth.threshold == 0.2
        assert result.fundamental.profit_growth.periods == 8


class TestHelperFunctions:
    """ヘルパー関数のテスト"""

    def test_unflatten_params_basic(self) -> None:
        flat_params = {"per.threshold": 15.0}
        result = _unflatten_params(flat_params)

        assert result == {"per": {"threshold": 15.0}}

    def test_unflatten_params_multiple(self) -> None:
        flat_params = {
            "per.threshold": 15.0,
            "pbr.threshold": 1.0,
        }
        result = _unflatten_params(flat_params)

        assert result == {
            "per": {"threshold": 15.0},
            "pbr": {"threshold": 1.0},
        }

    def test_unflatten_params_deep_nesting(self) -> None:
        flat_params = {"a.b.c.d": 123}
        result = _unflatten_params(flat_params)

        assert result == {"a": {"b": {"c": {"d": 123}}}}

    def test_deep_merge_basic(self) -> None:
        base = {"enabled": True, "threshold": 15.0}
        updates = {"threshold": 20.0}
        result = _deep_merge(base, updates)

        assert result == {"enabled": True, "threshold": 20.0}
        assert base == {"enabled": True, "threshold": 15.0}

    def test_deep_merge_nested(self) -> None:
        base = {
            "per": {"enabled": True, "threshold": 15.0},
            "pbr": {"enabled": False, "threshold": 1.0},
        }
        updates = {
            "per": {"threshold": 20.0},
        }
        result = _deep_merge(base, updates)

        assert result == {
            "per": {"enabled": True, "threshold": 20.0},
            "pbr": {"enabled": False, "threshold": 1.0},
        }

    def test_deep_merge_add_new_key(self) -> None:
        base = {"existing": 1}
        updates = {"new_key": 2}
        result = _deep_merge(base, updates)

        assert result == {"existing": 1, "new_key": 2}
