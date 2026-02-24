"""composite.py (SignalParams) のテスト"""

from src.shared.models.signals.composite import SignalParams


class TestSignalParams:
    def test_defaults_all_disabled(self):
        sp = SignalParams()
        assert not sp.has_any_enabled()
        assert not sp.has_any_entry_enabled()
        assert not sp.has_any_exit_enabled()

    def test_enable_volume(self):
        sp = SignalParams(volume={"enabled": True})
        assert sp.has_any_enabled()

    def test_enable_beta(self):
        sp = SignalParams(beta={"enabled": True})
        assert sp.has_any_enabled()

    def test_enable_crossover(self):
        sp = SignalParams(crossover={"enabled": True})
        assert sp.has_any_enabled()

    def test_all_fields_exist(self):
        sp = SignalParams()
        expected_fields = [
            "volume", "trading_value", "trading_value_range",
            "trend", "fundamental", "volatility", "beta", "margin",
            "atr_support_break", "retracement", "period_breakout",
            "crossover", "bollinger_bands", "buy_and_hold",
            "mean_reversion", "rsi_threshold", "rsi_spread",
            "ma_breakout", "index_daily_change", "index_macd_histogram",
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
