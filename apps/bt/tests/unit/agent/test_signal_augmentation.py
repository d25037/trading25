"""signal_augmentation / signal_param_factory のテスト"""

from __future__ import annotations

import random

from src.domains.lab_agent.models import StrategyCandidate
from src.domains.lab_agent.signal_augmentation import (
    _apply_random_add_side,
    _is_enabled_signal_params,
    _is_fundamental_only,
    _is_mutually_exclusive,
    _is_usage_allowed,
    _list_addable_signals,
    _list_enabled_fundamental_children,
    _enable_fundamental_children,
    apply_random_add_structure,
)
from src.domains.lab_agent.signal_catalog import SIGNAL_CONSTRAINTS_MAP
from src.domains.lab_agent.signal_param_factory import build_signal_params
from src.domains.lab_agent.signal_search_space import PARAM_RANGES


class DeterministicRng:
    """テスト用: 常に最小値/先頭要素を選ぶ乱数"""

    def randint(self, a: int, b: int) -> int:  # noqa: ARG002
        return a

    def uniform(self, a: float, b: float) -> float:  # noqa: ARG002
        return (a + b) / 2.0

    def choice(self, seq):  # type: ignore[no-untyped-def]
        return seq[0]


class PreferDividendGrowthRng(DeterministicRng):
    """fundamental選択時に dividend_per_share_growth を優先する乱数"""

    def choice(self, seq):  # type: ignore[no-untyped-def]
        if "dividend_per_share_growth" in seq:
            return "dividend_per_share_growth"
        return seq[0]


def _make_candidate(entry: dict, exit: dict | None = None) -> StrategyCandidate:
    return StrategyCandidate(
        strategy_id="base",
        entry_filter_params=entry,
        exit_trigger_params=exit or {},
    )


def test_random_add_adds_expected_entry_signal() -> None:
    rng = DeterministicRng()
    base = _make_candidate(entry={"volume": {"enabled": True}})

    updated, added = apply_random_add_structure(
        base,
        rng=rng,
        add_entry_signals=1,
        add_exit_signals=0,
        base_entry_signals={"volume"},
        base_exit_signals=set(),
    )

    assert "volume" in updated.entry_filter_params
    assert len(updated.entry_filter_params) == 2
    assert added["entry"] == ["period_breakout"]  # AVAILABLE_SIGNALS の先頭
    assert updated.metadata["structure_mode"] == "random_add"
    assert updated.metadata["random_added_entry"] == ["period_breakout"]


def test_random_add_trims_extras_when_add_zero() -> None:
    rng = DeterministicRng()
    base = _make_candidate(
        entry={
            "volume": {"enabled": True},
            "period_breakout": {"enabled": True},
        }
    )

    updated, _ = apply_random_add_structure(
        base,
        rng=rng,
        add_entry_signals=0,
        add_exit_signals=0,
        base_entry_signals={"volume"},
        base_exit_signals=set(),
    )

    assert set(updated.entry_filter_params.keys()) == {"volume"}


def test_random_add_respects_mutual_exclusion() -> None:
    rng = DeterministicRng()
    base = _make_candidate(
        entry={
            "rsi_threshold": {"enabled": True, "period": 14, "threshold": 40.0, "condition": "above"},
        }
    )

    updated, _ = apply_random_add_structure(
        base,
        rng=rng,
        add_entry_signals=5,
        add_exit_signals=0,
        base_entry_signals={"rsi_threshold"},
        base_exit_signals=set(),
    )

    # rsi_spread は rsi_threshold と相互排他
    assert "rsi_spread" not in updated.entry_filter_params


def test_random_add_injects_missing_base_signal() -> None:
    rng = DeterministicRng()
    base = _make_candidate(entry={})

    updated, _ = apply_random_add_structure(
        base,
        rng=rng,
        add_entry_signals=0,
        add_exit_signals=0,
        base_entry_signals={"volume"},
        base_exit_signals=set(),
    )

    assert "volume" in updated.entry_filter_params
    assert updated.entry_filter_params["volume"]["enabled"] is True


def test_random_add_fundamental_only_enables_nested_children() -> None:
    rng = DeterministicRng()
    base = _make_candidate(
        entry={
            "fundamental": {
                "enabled": True,
                "per": {"enabled": True, "threshold": 15.0},
                "pbr": {"enabled": False, "threshold": 1.0},
                "forward_eps_growth": {"enabled": False, "threshold": 0.1},
            }
        }
    )

    updated, added = apply_random_add_structure(
        base,
        rng=rng,
        add_entry_signals=2,
        add_exit_signals=0,
        base_entry_signals={"fundamental"},
        base_exit_signals=set(),
        allowed_categories={"fundamental"},
    )

    fundamental = updated.entry_filter_params["fundamental"]
    enabled_children = {
        child_name
        for child_name, child_params in fundamental.items()
        if isinstance(child_params, dict) and child_params.get("enabled")
    }

    assert enabled_children == {"per", "pbr", "forward_eps_growth"}
    assert added["entry"] == [
        "fundamental.pbr",
        "fundamental.forward_eps_growth",
    ]


def test_random_add_fundamental_only_with_empty_base_adds_parent_and_child() -> None:
    rng = DeterministicRng()
    base = _make_candidate(entry={})

    updated, added = apply_random_add_structure(
        base,
        rng=rng,
        add_entry_signals=2,
        add_exit_signals=0,
        base_entry_signals=set(),
        base_exit_signals=set(),
        allowed_categories={"fundamental"},
    )

    assert "fundamental" in updated.entry_filter_params
    assert "fundamental" in added["entry"]
    assert any(item.startswith("fundamental.") for item in added["entry"])


def test_is_enabled_signal_params_variants() -> None:
    assert _is_enabled_signal_params(True) is True
    assert _is_enabled_signal_params({"enabled": False}) is False
    assert _is_enabled_signal_params({"threshold": 1.0}) is True


def test_is_fundamental_only_scope() -> None:
    assert _is_fundamental_only("entry", {"fundamental"}) is True
    assert _is_fundamental_only("exit", {"fundamental"}) is False
    assert _is_fundamental_only("entry", set()) is False


def test_is_mutually_exclusive_handles_symmetric_constraints() -> None:
    assert _is_mutually_exclusive("rsi_threshold", {"rsi_spread"}) is True
    assert _is_mutually_exclusive("volume", {"period_breakout"}) is False


def test_list_enabled_fundamental_children_skips_non_toggle_items() -> None:
    enabled = _list_enabled_fundamental_children(
        {
            "per": {"enabled": True},
            "pbr": {"enabled": False},
            "period_type": "FY",
            "metadata": {"threshold": 0.1},
        }
    )
    assert enabled == {"per"}


def test_enable_fundamental_children_zero_add_returns_empty() -> None:
    working = {"fundamental": {"enabled": True, "per": {"enabled": True}}}
    added = _enable_fundamental_children(working, rng=DeterministicRng(), add_signals=0)
    assert added == []


def test_enable_fundamental_children_merges_missing_defaults() -> None:
    working = {
        "fundamental": {
            "enabled": True,
            "per": {"enabled": True, "threshold": 12.0},
            "period_type": "FY",
        }
    }
    added = _enable_fundamental_children(working, rng=DeterministicRng(), add_signals=1)

    assert added and added[0].startswith("fundamental.")
    merged = working["fundamental"]
    assert "pbr" in merged
    assert "use_adjusted" in merged
    assert merged["per"]["enabled"] is True


def test_apply_random_add_side_enables_existing_and_injects_missing_base() -> None:
    updated, _ = _apply_random_add_side(
        params_dict={"volume": {"enabled": False}},
        usage_type="entry",
        rng=DeterministicRng(),
        base_signals={"volume", "period_breakout"},
        add_signals=0,
        allowed_categories=set(),
    )
    assert updated["volume"]["enabled"] is True
    assert "period_breakout" in updated


def test_apply_random_add_side_fundamental_only_adds_nested_child() -> None:
    updated, added = _apply_random_add_side(
        params_dict={
            "fundamental": {"enabled": True, "per": {"enabled": True}},
            "volume": {"enabled": True},
        },
        usage_type="entry",
        rng=DeterministicRng(),
        base_signals={"fundamental"},
        add_signals=1,
        allowed_categories={"fundamental"},
    )

    assert "volume" in updated
    assert "fundamental" not in added
    assert any(item.startswith("fundamental.") for item in added)


def test_list_addable_signals_filters_usage_and_constraints() -> None:
    enabled = {"rsi_threshold"}
    addable_entry = _list_addable_signals(
        usage_type="entry",
        enabled=enabled,
        allowed_categories={"oscillator"},
    )
    assert "rsi_spread" not in addable_entry
    assert "rsi_threshold" not in addable_entry

    addable_exit = _list_addable_signals(
        usage_type="exit",
        enabled=set(),
        allowed_categories={"fundamental"},
    )
    assert addable_exit == []


def test_is_usage_allowed_rules() -> None:
    both_signal = SIGNAL_CONSTRAINTS_MAP["volume"]
    entry_only_signal = SIGNAL_CONSTRAINTS_MAP["fundamental"]

    assert _is_usage_allowed(both_signal, "entry") is True
    assert _is_usage_allowed(entry_only_signal, "entry") is True
    assert _is_usage_allowed(entry_only_signal, "exit") is False


def test_build_signal_params_within_ranges() -> None:
    rng = random.Random(0)
    params = build_signal_params("period_breakout", "entry", rng)

    assert params["enabled"] is True
    assert "period" in params
    assert "lookback_days" in params
    min_period, max_period, _ = PARAM_RANGES["period_breakout"]["period"]
    assert min_period <= params["period"] <= max_period


def test_build_fundamental_params_has_child_enabled() -> None:
    rng = DeterministicRng()
    params = build_signal_params("fundamental", "entry", rng)

    assert params["enabled"] is True
    # DeterministicRng.choice() で "per" が選ばれる
    assert params["per"]["enabled"] is True


def test_build_fundamental_params_can_select_new_growth_signal() -> None:
    rng = PreferDividendGrowthRng()
    params = build_signal_params("fundamental", "entry", rng)

    assert params["enabled"] is True
    assert params["dividend_per_share_growth"]["enabled"] is True
    assert "periods" in params["dividend_per_share_growth"]
