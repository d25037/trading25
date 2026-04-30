from __future__ import annotations

from typing import Any, cast

from src.domains.strategy.runtime.compiler import compile_runtime_strategy
from src.domains.strategy.runtime.loader import ConfigLoader
from src.domains.strategy.runtime.screening_profile import (
    load_strategy_screening_config,
    resolve_entry_decidability,
    resolve_screening_profile,
)
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams


def _shared_config(**overrides: Any) -> SharedConfig:
    payload: dict[str, Any] = {
        "universe_preset": "primeExTopix500",
        "timeframe": "daily",
    }
    payload.update(overrides)
    return SharedConfig.model_validate(
        payload,
        context={"resolve_stock_codes": False},
    )


def test_resolve_screening_profile_standard_prior_close_strategy() -> None:
    compiled_strategy = compile_runtime_strategy(
        strategy_name="demo",
        shared_config=_shared_config(),
    )

    profile = resolve_screening_profile(compiled_strategy)

    assert profile.screening_support == "supported"
    assert profile.entry_decidability == "pre_open_decidable"


def test_resolve_screening_profile_rejects_next_session_round_trip() -> None:
    compiled_strategy = compile_runtime_strategy(
        strategy_name="demo",
        shared_config=_shared_config(
            execution_policy={"mode": "next_session_round_trip"}
        ),
    )

    profile = resolve_screening_profile(compiled_strategy)

    assert profile.screening_support == "unsupported"
    assert profile.entry_decidability is None


def test_resolve_entry_decidability_treats_standard_open_signal_as_pre_open_decidable() -> None:
    compiled_strategy = compile_runtime_strategy(
        strategy_name="demo",
        shared_config=_shared_config(),
        entry_signal_params=SignalParams.model_validate(
            {"index_open_gap_regime": {"enabled": True}}
        ),
    )

    assert resolve_entry_decidability(compiled_strategy) == "pre_open_decidable"
    profile = resolve_screening_profile(compiled_strategy)
    assert profile.screening_support == "supported"
    assert profile.entry_decidability == "pre_open_decidable"


def test_resolve_entry_decidability_treats_current_session_open_signal_as_in_session_required() -> None:
    compiled_strategy = compile_runtime_strategy(
        strategy_name="demo",
        shared_config=_shared_config(
            execution_policy={"mode": "current_session_round_trip"}
        ),
        entry_signal_params=SignalParams.model_validate(
            {"index_open_gap_regime": {"enabled": True}}
        ),
    )

    assert resolve_entry_decidability(compiled_strategy) == "requires_same_session_observation"


def test_resolve_entry_decidability_treats_current_session_prior_close_signal_as_pre_open_decidable() -> None:
    compiled_strategy = compile_runtime_strategy(
        strategy_name="demo",
        shared_config=_shared_config(
            execution_policy={"mode": "current_session_round_trip"}
        ),
        entry_signal_params=SignalParams.model_validate(
            {"volume_ratio_above": {"enabled": True}}
        ),
    )

    assert resolve_entry_decidability(compiled_strategy) == "pre_open_decidable"


def test_resolve_entry_decidability_treats_overnight_close_signal_as_in_session_required() -> None:
    compiled_strategy = compile_runtime_strategy(
        strategy_name="demo",
        shared_config=_shared_config(
            execution_policy={"mode": "overnight_round_trip"}
        ),
        entry_signal_params=SignalParams.model_validate(
            {"volume_ratio_above": {"enabled": True}}
        ),
    )

    assert resolve_entry_decidability(compiled_strategy) == "requires_same_session_observation"


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
            "shared_config": {"universe_preset": "primeExTopix500"},
            "entry_filter_params": {
                "index_open_gap_regime": {"enabled": True}
            },
            "exit_trigger_params": {},
        }
    )

    loaded = load_strategy_screening_config(
        cast(ConfigLoader, config_loader), "production/demo"
    )

    assert loaded.screening_support == "supported"
    assert loaded.entry_decidability == "pre_open_decidable"
    assert loaded.entry_params.index_open_gap_regime.enabled is True


def test_load_strategy_screening_config_ignores_legacy_dataset_field() -> None:
    class _ConfigLoader:
        def __init__(self, config: dict[str, Any]) -> None:
            self._config = config

        def load_strategy_config(self, strategy_name: str) -> dict[str, Any]:
            assert strategy_name == "production/demo"
            return self._config

        def merge_shared_config(self, config: dict[str, Any]) -> dict[str, Any]:
            return config.get("shared_config", {})

        def resolve_strategy_category(self, strategy_name: str) -> str:
            assert strategy_name == "production/demo"
            return "production"

    config_loader = _ConfigLoader(
        {
            "shared_config": {
                "dataset": "primeExTopix500_20260325",
                "universe_preset": "primeExTopix500",
            },
            "entry_filter_params": {
                "volume_ratio_above": {"enabled": True},
            },
            "exit_trigger_params": {},
        }
    )

    loaded = load_strategy_screening_config(
        cast(ConfigLoader, config_loader), "production/demo"
    )

    assert loaded.shared_config.universe_preset == "primeExTopix500"
    assert loaded.shared_config.dataset == "primeExTopix500"
    assert loaded.entry_params.volume_ratio_above.enabled is True
