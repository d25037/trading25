from __future__ import annotations

from src.domains.strategy.runtime.compiler import (
    CompiledAvailabilityPoint,
    CompiledExecutionSession,
    compile_strategy_config,
    compile_strategy_requirements,
)


class _StubConfigLoader:
    def merge_shared_config(self, strategy_config: dict[str, object]) -> dict[str, object]:
        merged: dict[str, object] = {
            "dataset": "primeExTopix500",
            "direction": "longonly",
            "timeframe": "daily",
        }
        shared_config = strategy_config.get("shared_config")
        if isinstance(shared_config, dict):
            merged.update(shared_config)
        return merged


def test_compile_strategy_config_collects_market_requirements() -> None:
    compiled = compile_strategy_config(
        "demo-strategy",
        {
            "shared_config": {"dataset": "primeExTopix500"},
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
        },
        config_loader=_StubConfigLoader(),
    )

    assert compiled.execution_semantics == "standard"
    assert compiled.dataset_name == "primeExTopix500"
    assert compiled.required_data_domains == ["market"]
    assert compiled.required_features == ["volume"]
    assert compiled.required_fundamental_fields == []
    assert compiled.signal_ids == ["entry.volume_ratio_above"]
    assert compiled.signals[0].availability.decision_cutoff == CompiledAvailabilityPoint.NEXT_SESSION_OPEN
    assert compiled.signals[0].availability.execution_session == CompiledExecutionSession.NEXT_SESSION


def test_compile_strategy_config_marks_oracle_allowlist_as_same_session() -> None:
    compiled = compile_strategy_config(
        "demo-strategy",
        {
            "shared_config": {"current_session_round_trip_oracle": True},
            "entry_filter_params": {
                "oracle_index_open_gap_regime": {"enabled": True},
                "volume_ratio_above": {"enabled": True},
            },
        },
        config_loader=_StubConfigLoader(),
    )

    signals = {signal.signal_id: signal for signal in compiled.signals}

    assert compiled.execution_semantics == "current_session_round_trip_oracle"
    assert signals["entry.oracle_index_open_gap_regime"].availability.observation_time == (
        CompiledAvailabilityPoint.CURRENT_SESSION_OPEN
    )
    assert signals["entry.oracle_index_open_gap_regime"].availability.execution_session == (
        CompiledExecutionSession.CURRENT_SESSION
    )
    assert signals["entry.volume_ratio_above"].availability.observation_time == (
        CompiledAvailabilityPoint.PRIOR_SESSION_CLOSE
    )
    assert signals["entry.volume_ratio_above"].availability.decision_cutoff == (
        CompiledAvailabilityPoint.CURRENT_SESSION_OPEN
    )


def test_compile_strategy_config_marks_oracle_allowlist_as_same_day_observation_in_standard_mode() -> None:
    compiled = compile_strategy_config(
        "demo-strategy",
        {
            "entry_filter_params": {
                "oracle_index_open_gap_regime": {"enabled": True},
            },
        },
        config_loader=_StubConfigLoader(),
    )

    signal = compiled.signals[0]

    assert compiled.execution_semantics == "standard"
    assert signal.availability.observation_time == (
        CompiledAvailabilityPoint.CURRENT_SESSION_OPEN
    )
    assert signal.availability.execution_session == (
        CompiledExecutionSession.NEXT_SESSION
    )


def test_compile_strategy_requirements_maps_from_compiled_strategy() -> None:
    compiled = compile_strategy_config(
        "demo-strategy",
        {
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
        },
        config_loader=_StubConfigLoader(),
    )

    requirements = compile_strategy_requirements(compiled)

    assert requirements.required_data_domains == ["market"]
    assert requirements.required_features == ["volume"]
    assert requirements.required_fundamental_fields == []
    assert requirements.signal_ids == ["entry.volume_ratio_above"]
