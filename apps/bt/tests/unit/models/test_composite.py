"""composite.py (SignalParams) のテスト"""

from src.shared.models.signals.composite import SignalParams


class TestSignalParams:
    def test_defaults_all_disabled(self):
        sp = SignalParams()
        assert not sp.has_any_enabled()
        assert not sp.has_any_entry_enabled()
        assert not sp.has_any_exit_enabled()

    def test_enable_volume_ratio_above(self):
        sp = SignalParams(volume_ratio_above={"enabled": True})
        assert sp.has_any_enabled()

    def test_enable_beta(self):
        sp = SignalParams(beta={"enabled": True})
        assert sp.has_any_enabled()

    def test_enable_volatility_percentile(self):
        sp = SignalParams(volatility_percentile={"enabled": True})
        assert sp.has_any_enabled()

    def test_enable_crossover(self):
        sp = SignalParams(crossover={"enabled": True})
        assert sp.has_any_enabled()

    def test_all_fields_exist(self):
        sp = SignalParams()
        expected_fields = [
            "volume_ratio_above", "volume_ratio_below", "trading_value", "trading_value_range",
            "trend", "fundamental", "volatility_percentile", "beta", "margin",
            "atr_support_position", "atr_support_cross",
            "retracement_position", "retracement_cross",
            "period_extrema_break", "period_extrema_position",
            "crossover", "bollinger_position", "bollinger_cross", "buy_and_hold",
            "baseline_deviation", "baseline_position", "rsi_threshold", "rsi_spread",
            "baseline_cross", "index_daily_change", "index_macd_histogram",
            "index_open_gap_regime",
            "risk_adjusted_return",
            "sector_strength_ranking", "sector_rotation_phase", "sector_volatility_regime",
        ]
        for field in expected_fields:
            assert hasattr(sp, field), f"Missing field: {field}"

    def test_has_any_entry_is_alias(self):
        sp = SignalParams()
        assert sp.has_any_entry_enabled() == sp.has_any_enabled()

    def test_has_any_exit_is_alias(self):
        sp = SignalParams()
        assert sp.has_any_exit_enabled() == sp.has_any_enabled()

    def test_fundamental_nested_enabled(self):
        sp = SignalParams(fundamental={"per": {"enabled": True}})
        assert sp.has_any_enabled()
