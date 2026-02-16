"""signal_catalog のユニットテスト。"""

from src.agent.signal_catalog import (
    PARAM_RANGES,
    SIGNAL_CATEGORY_MAP,
    SIGNAL_CONSTRAINTS_MAP,
)
from src.models.signals import SignalParams
from src.strategies.signals.registry import SIGNAL_REGISTRY


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
        "risk_adjusted_return",
        "retracement",
        "buy_and_hold",
        "sector_strength_ranking",
        "sector_rotation_phase",
        "sector_volatility_regime",
    }
    assert expected.issubset(set(SIGNAL_CONSTRAINTS_MAP.keys()))


def test_usage_and_category_overrides_are_applied() -> None:
    assert SIGNAL_CONSTRAINTS_MAP["fundamental"].usage == "entry"
    assert SIGNAL_CONSTRAINTS_MAP["buy_and_hold"].usage == "entry"
    assert SIGNAL_CATEGORY_MAP["ma_breakout"] == "trend"
    assert SIGNAL_CATEGORY_MAP["crossover"] == "trend"


def test_param_ranges_generated_for_new_signals() -> None:
    assert "lookback_period" in PARAM_RANGES["risk_adjusted_return"]
    assert "retracement_level" in PARAM_RANGES["retracement"]
    assert "momentum_period" in PARAM_RANGES["sector_strength_ranking"]

