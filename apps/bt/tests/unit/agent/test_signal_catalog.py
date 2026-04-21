"""signal_catalog のユニットテスト。"""

from src.domains.lab_agent.signal_catalog import (
    PARAM_RANGES,
    SIGNAL_CATEGORY_MAP,
    SIGNAL_CONSTRAINTS_MAP,
)
from src.shared.models.signals import SignalParams
from src.domains.strategy.signals.registry import SIGNAL_REGISTRY


def test_registry_top_level_signals_are_covered_by_catalog() -> None:
    signal_fields = set(SignalParams.model_fields.keys())
    registry_top_level = {
        signal_def.param_key.split(".", 1)[0]
        for signal_def in SIGNAL_REGISTRY
        if signal_def.param_key.split(".", 1)[0] in signal_fields
    }
    assert registry_top_level.issubset(set(SIGNAL_CONSTRAINTS_MAP.keys()))


def test_new_registry_signals_are_available_in_lab_catalog() -> None:
    expected = {
        "atr_support_cross",
        "atr_support_position",
        "bollinger_cross",
        "bollinger_position",
        "period_extrema_break",
        "period_extrema_position",
        "risk_adjusted_return",
        "retracement_position",
        "retracement_cross",
        "volume_ratio_above",
        "volume_ratio_below",
        "trading_value_ema_ratio_below",
        "buy_and_hold",
        "sector_strength_ranking",
        "sector_rotation_phase",
        "sector_volatility_regime",
    }
    assert expected.issubset(set(SIGNAL_CONSTRAINTS_MAP.keys()))


def test_usage_and_category_mapping_are_applied() -> None:
    assert SIGNAL_CONSTRAINTS_MAP["fundamental"].usage == "entry"
    assert SIGNAL_CONSTRAINTS_MAP["buy_and_hold"].usage == "entry"
    assert SIGNAL_CATEGORY_MAP["baseline_cross"] == "trend"
    assert SIGNAL_CATEGORY_MAP["baseline_position"] == "trend"
    assert SIGNAL_CATEGORY_MAP["crossover"] == "trend"
    assert SIGNAL_CATEGORY_MAP["retracement_position"] == "trend"
    assert SIGNAL_CATEGORY_MAP["bollinger_cross"] == "volatility"
    assert SIGNAL_CATEGORY_MAP["atr_support_position"] == "breakout"


def test_param_ranges_generated_for_new_signals() -> None:
    assert "lookback_period" in PARAM_RANGES["risk_adjusted_return"]
    assert "retracement_level" in PARAM_RANGES["retracement_position"]
    assert "ratio_threshold" in PARAM_RANGES["volume_ratio_above"]
    assert "period" in PARAM_RANGES["period_extrema_break"]
    assert "momentum_period" in PARAM_RANGES["sector_strength_ranking"]


def test_param_ranges_respect_exclusive_minimums() -> None:
    lower, upper, param_type = PARAM_RANGES["fundamental"]["forward_eps_growth.threshold"]
    assert param_type == "float"
    assert lower > 0.0
    assert upper <= 2.0
