from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from src.domains.strategy.runtime.compiler import (
    CompiledAvailabilityPoint,
    CompiledExecutionSession,
    CompiledSignalAvailability,
    CompiledSignalIR,
    CompiledSignalScope,
    CompiledStrategyIR,
    _apply_config_override,
    _has_configured_exit_signal_params,
    _resolve_required_data_domains,
    _validate_round_trip_exit_rules,
    compile_strategy_config,
    compile_strategy_requirements,
    resolve_round_trip_execution_mode_name,
)
from src.domains.strategy.signals.registry import SignalDefinition
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams


class _StubConfigLoader:
    def merge_shared_config(self, strategy_config: dict[str, object]) -> dict[str, object]:
        merged: dict[str, object] = {
            "universe_preset": "primeExTopix500",
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
            "shared_config": {"universe_preset": "primeExTopix500"},
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


def test_compile_strategy_config_marks_same_day_signal_as_current_session() -> None:
    compiled = compile_strategy_config(
        "demo-strategy",
        {
            "shared_config": {
                "execution_policy": {"mode": "current_session_round_trip"}
            },
            "entry_filter_params": {
                "index_open_gap_regime": {"enabled": True},
                "volume_ratio_above": {"enabled": True},
            },
        },
        config_loader=_StubConfigLoader(),
    )

    signals = {signal.signal_id: signal for signal in compiled.signals}

    assert compiled.execution_semantics == "current_session_round_trip"
    assert signals["entry.index_open_gap_regime"].availability.observation_time == (
        CompiledAvailabilityPoint.CURRENT_SESSION_OPEN
    )
    assert signals["entry.index_open_gap_regime"].availability.execution_session == (
        CompiledExecutionSession.CURRENT_SESSION
    )
    assert signals["entry.volume_ratio_above"].availability.observation_time == (
        CompiledAvailabilityPoint.PRIOR_SESSION_CLOSE
    )
    assert signals["entry.volume_ratio_above"].availability.decision_cutoff == (
        CompiledAvailabilityPoint.CURRENT_SESSION_OPEN
    )


def test_compile_strategy_config_marks_same_day_signal_as_same_day_observation_in_standard_mode() -> None:
    compiled = compile_strategy_config(
        "demo-strategy",
        {
            "entry_filter_params": {
                "index_open_gap_regime": {"enabled": True},
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


def test_resolve_round_trip_execution_mode_name_handles_supported_and_standard_modes() -> None:
    standard = CompiledStrategyIR(
        strategy_name="standard",
        execution_semantics="standard",
        dataset_name="sample",
        timeframe="daily",
        signals=[],
        signal_ids=[],
        required_data_domains=[],
        required_features=[],
        required_fundamental_fields=[],
    )
    next_session = standard.model_copy(update={"execution_semantics": "next_session_round_trip"})
    overnight = standard.model_copy(update={"execution_semantics": "overnight_round_trip"})

    assert resolve_round_trip_execution_mode_name(standard) is None
    assert resolve_round_trip_execution_mode_name(next_session) == "next_session_round_trip"
    assert resolve_round_trip_execution_mode_name(overnight) == "overnight_round_trip"


def test_apply_config_override_merges_nested_dicts_and_replaces_missing_sections() -> None:
    base = {
        "shared_config": {"dataset": "sample", "timeframe": "daily"},
        "entry_filter_params": {"volume_ratio_above": {"enabled": True, "ratio_threshold": 1.2}},
    }
    override = {
        "shared_config": {"timeframe": "weekly"},
        "entry_filter_params": {"volume_ratio_above": {"ratio_threshold": 2.0}},
        "exit_trigger_params": {"volume_ratio_above": {"enabled": True}},
    }

    result = _apply_config_override(base, override)

    assert result["shared_config"] == {"dataset": "sample", "timeframe": "weekly"}
    assert result["entry_filter_params"]["volume_ratio_above"]["enabled"] is True
    assert result["entry_filter_params"]["volume_ratio_above"]["ratio_threshold"] == 2.0
    assert result["exit_trigger_params"] == {"volume_ratio_above": {"enabled": True}}


def test_apply_config_override_ignores_non_dict_override_values() -> None:
    base = {"shared_config": {"dataset": "sample"}}
    override = {"shared_config": "skip", "entry_filter_params": None}

    result = _apply_config_override(base, override)

    assert result["shared_config"] == {"dataset": "sample"}
    assert result["entry_filter_params"] == {}


def test_resolve_required_data_domains_includes_margin_and_statements() -> None:
    signals = [
        CompiledSignalIR(
            signal_id="entry.margin_balance_percentile",
            scope=CompiledSignalScope.ENTRY,
            param_key="margin_balance_percentile",
            signal_name="Margin",
            category="macro",
            description="",
            data_requirements=["margin", "statements:EPS"],
            availability=CompiledSignalAvailability(
                observation_time=CompiledAvailabilityPoint.CURRENT_SESSION_CLOSE,
                available_at=CompiledAvailabilityPoint.CURRENT_SESSION_CLOSE,
                decision_cutoff=CompiledAvailabilityPoint.NEXT_SESSION_OPEN,
                execution_session=CompiledExecutionSession.NEXT_SESSION,
            ),
        )
    ]

    assert _resolve_required_data_domains(signals) == ["margin", "statements"]


def test_has_configured_exit_signal_params_handles_none_and_missing_fields_set() -> None:
    assert _has_configured_exit_signal_params(None) is False

    class _NoFieldsSet:
        pass

    assert _has_configured_exit_signal_params(_NoFieldsSet()) is True


def test_validate_round_trip_exit_rules_raises_for_non_empty_exit_params() -> None:
    shared_config = SharedConfig.model_validate(
        {
            "execution_policy": {"mode": "next_session_round_trip"},
        },
        context={"resolve_stock_codes": False},
    )

    with patch.object(SignalParams, "model_fields_set", {"volume_ratio_above"}):
        with pytest.raises(ValueError, match="exit_trigger_params must be empty"):
            _validate_round_trip_exit_rules(
                shared_config=shared_config,
                exit_signal_params=SignalParams(),
            )


def test_compile_strategy_config_collects_exit_signal_when_enabled() -> None:
    compiled = compile_strategy_config(
        "demo-strategy",
        {
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
            "exit_trigger_params": {"volume_ratio_above": {"enabled": True}},
        },
        config_loader=_StubConfigLoader(),
    )

    assert "entry.volume_ratio_above" in compiled.signal_ids
    assert "exit.volume_ratio_above" in compiled.signal_ids


def test_compile_strategy_config_treats_open_plus_disclosure_signal_as_non_same_day() -> None:
    mixed_signal = SignalDefinition(
        name="Mixed Open + Disclosure",
        signal_func=lambda **_kwargs: pd.Series(dtype=bool),
        enabled_checker=lambda params: params.index_open_gap_regime.enabled,
        param_builder=lambda _params, _data: {},
        entry_purpose="entry",
        exit_purpose="exit",
        category="macro",
        description="",
        param_key="index_open_gap_regime",
        data_requirements=["benchmark_open_gap", "statements:EPS"],
    )

    with patch(
        "src.domains.strategy.runtime.compiler.SIGNAL_REGISTRY",
        [mixed_signal],
    ):
        compiled = compile_strategy_config(
            "demo-strategy",
            {
                "shared_config": {
                    "execution_policy": {"mode": "current_session_round_trip"}
                },
                "entry_filter_params": {
                    "index_open_gap_regime": {"enabled": True},
                },
            },
            config_loader=_StubConfigLoader(),
        )

    signal = compiled.signals[0]
    assert signal.availability.observation_time == CompiledAvailabilityPoint.PRIOR_SESSION_CLOSE
    assert signal.availability.available_at == CompiledAvailabilityPoint.PRIOR_SESSION_CLOSE
    assert signal.availability.execution_session == CompiledExecutionSession.CURRENT_SESSION


def test_compile_strategy_config_marks_overnight_round_trip_signals_for_current_close_entry() -> None:
    compiled = compile_strategy_config(
        "demo-strategy",
        {
            "shared_config": {
                "execution_policy": {"mode": "overnight_round_trip"}
            },
            "entry_filter_params": {
                "index_open_gap_regime": {"enabled": True},
                "volume_ratio_above": {"enabled": True},
            },
        },
        config_loader=_StubConfigLoader(),
    )

    signals = {signal.signal_id: signal for signal in compiled.signals}

    assert compiled.execution_semantics == "overnight_round_trip"
    assert signals["entry.index_open_gap_regime"].availability.observation_time == (
        CompiledAvailabilityPoint.CURRENT_SESSION_OPEN
    )
    assert signals["entry.index_open_gap_regime"].availability.decision_cutoff == (
        CompiledAvailabilityPoint.CURRENT_SESSION_CLOSE
    )
    assert signals["entry.volume_ratio_above"].availability.observation_time == (
        CompiledAvailabilityPoint.CURRENT_SESSION_CLOSE
    )
    assert signals["entry.volume_ratio_above"].availability.execution_session == (
        CompiledExecutionSession.CURRENT_SESSION
    )
