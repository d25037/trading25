from __future__ import annotations

from typing import Any

from src.domains.strategy.runtime.compiler import compile_runtime_strategy
from src.domains.strategy.runtime.screening_mode import (
    load_strategy_screening_config,
    resolve_same_day_screening_mode,
    resolve_strategy_screening_mode,
)
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams


def _shared_config(**overrides: Any) -> SharedConfig:
    payload: dict[str, Any] = {"dataset": "primeExTopix500", "timeframe": "daily"}
    payload.update(overrides)
    return SharedConfig.model_validate(
        payload,
        context={"resolve_stock_codes": False},
    )


def test_resolve_strategy_screening_mode_standard() -> None:
    compiled_strategy = compile_runtime_strategy(
        strategy_name="demo",
        shared_config=_shared_config(),
    )

    assert resolve_same_day_screening_mode(compiled_strategy) is False
    assert resolve_strategy_screening_mode(compiled_strategy) == "standard"


def test_resolve_strategy_screening_mode_rejects_next_session_round_trip() -> None:
    compiled_strategy = compile_runtime_strategy(
        strategy_name="demo",
        shared_config=_shared_config(
            execution_policy={"mode": "next_session_round_trip"}
        ),
    )

    assert resolve_same_day_screening_mode(compiled_strategy) is False
    assert resolve_strategy_screening_mode(compiled_strategy) == "unsupported"


def test_resolve_strategy_screening_mode_accepts_same_day_signal_without_shared_flag() -> None:
    compiled_strategy = compile_runtime_strategy(
        strategy_name="demo",
        shared_config=_shared_config(),
        entry_signal_params=SignalParams.model_validate(
            {"index_open_gap_regime": {"enabled": True}}
        ),
    )

    assert resolve_same_day_screening_mode(compiled_strategy) is True
    assert resolve_strategy_screening_mode(compiled_strategy) == "same_day"


def test_resolve_strategy_screening_mode_accepts_current_session_round_trip() -> None:
    compiled_strategy = compile_runtime_strategy(
        strategy_name="demo",
        shared_config=_shared_config(
            execution_policy={"mode": "current_session_round_trip"}
        ),
        entry_signal_params=SignalParams.model_validate(
            {"volume_ratio_above": {"enabled": True}}
        ),
    )

    assert resolve_same_day_screening_mode(compiled_strategy) is True
    assert resolve_strategy_screening_mode(compiled_strategy) == "same_day"


def test_load_strategy_screening_config_uses_compiled_strategy_availability() -> None:
    class _ConfigLoader:
        def __init__(self, config: dict[str, Any]) -> None:
            self._config = config

        def load_strategy_config(self, strategy_name: str) -> dict[str, Any]:
            assert strategy_name == "production/demo"
            return self._config

        def merge_shared_config(self, config: dict[str, Any]) -> dict[str, Any]:
            return config.get("shared_config", {})

    config_loader = _ConfigLoader(
        {
            "shared_config": {"dataset": "primeExTopix500"},
            "entry_filter_params": {
                "index_open_gap_regime": {"enabled": True}
            },
            "exit_trigger_params": {},
        }
    )

    loaded = load_strategy_screening_config(config_loader, "production/demo")

    assert loaded.screening_mode == "same_day"
    assert loaded.entry_params.index_open_gap_regime.enabled is True
