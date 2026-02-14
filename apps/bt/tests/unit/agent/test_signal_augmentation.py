"""signal_augmentation / signal_param_factory のテスト"""

from __future__ import annotations

import random

from src.agent.models import StrategyCandidate
from src.agent.signal_augmentation import apply_random_add_structure
from src.agent.signal_param_factory import build_signal_params
from src.agent.signal_search_space import PARAM_RANGES


class DeterministicRng:
    """テスト用: 常に最小値/先頭要素を選ぶ乱数"""

    def randint(self, a: int, b: int) -> int:  # noqa: ARG002
        return a

    def uniform(self, a: float, b: float) -> float:  # noqa: ARG002
        return (a + b) / 2.0

    def choice(self, seq):  # type: ignore[no-untyped-def]
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

