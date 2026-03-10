"""Compiled strategy IR and availability model helpers."""

from __future__ import annotations

from copy import deepcopy
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from src.domains.backtest.contracts import CompiledStrategyInputRequirements
from src.domains.strategy.runtime.loader import ConfigLoader
from src.domains.strategy.runtime.models import (
    StrategyConfig,
    resolve_execution_semantics,
    validate_strategy_config_dict,
)
from src.domains.strategy.runtime.parameter_extractor import _deep_merge_dict
from src.domains.strategy.signals.processor import SignalProcessor
from src.domains.strategy.signals.registry import SIGNAL_REGISTRY
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams


class CompiledExecutionSession(str, Enum):
    CURRENT_SESSION = "current_session"
    NEXT_SESSION = "next_session"


class CompiledAvailabilityPoint(str, Enum):
    PRIOR_SESSION_CLOSE = "prior_session_close"
    CURRENT_SESSION_OPEN = "current_session_open"
    CURRENT_SESSION_CLOSE = "current_session_close"
    NEXT_SESSION_OPEN = "next_session_open"


class CompiledSignalScope(str, Enum):
    ENTRY = "entry"
    EXIT = "exit"


class CompiledSignalAvailability(BaseModel):
    observation_time: CompiledAvailabilityPoint = Field(
        description="When the signal's source observation becomes observable",
    )
    available_at: CompiledAvailabilityPoint = Field(
        description="When the computed signal can be used by the engine",
    )
    decision_cutoff: CompiledAvailabilityPoint = Field(
        description="Latest decision point before execution",
    )
    execution_session: CompiledExecutionSession = Field(
        description="Which trading session executes the decision",
    )


class CompiledSignalIR(BaseModel):
    signal_id: str = Field(description="Stable scoped signal identifier")
    scope: CompiledSignalScope = Field(description="Signal scope")
    param_key: str = Field(description="Signal parameter key path")
    signal_name: str = Field(description="Human-readable signal name")
    category: str = Field(description="Signal category")
    description: str = Field(description="Signal description")
    data_requirements: list[str] = Field(
        default_factory=list,
        description="Declared data requirements from the signal registry",
    )
    availability: CompiledSignalAvailability = Field(
        description="Availability/no-lookahead metadata for this signal",
    )


class CompiledStrategyIR(BaseModel):
    schema_version: int = Field(default=1, description="Schema version")
    strategy_name: str = Field(description="Resolved strategy name")
    execution_semantics: str = Field(
        description="Compiled execution semantics label",
    )
    dataset_name: str | None = Field(default=None, description="Resolved dataset name")
    timeframe: str = Field(description="Compiled timeframe")
    signals: list[CompiledSignalIR] = Field(
        default_factory=list,
        description="Enabled signals after validation and config merge",
    )
    signal_ids: list[str] = Field(
        default_factory=list,
        description="Stable signal identifiers in compiled order",
    )
    required_data_domains: list[str] = Field(
        default_factory=list,
        description="Required high-level data domains",
    )
    required_features: list[str] = Field(
        default_factory=list,
        description="Required feature/data requirement identifiers",
    )
    required_fundamental_fields: list[str] = Field(
        default_factory=list,
        description="Required statements/fundamental fields",
    )


def uses_current_session_oracle_execution(
    compiled_strategy: CompiledStrategyIR,
) -> bool:
    if compiled_strategy.execution_semantics == "current_session_round_trip_oracle":
        return True

    return any(
        signal.scope == CompiledSignalScope.ENTRY
        and signal.availability.observation_time
        == CompiledAvailabilityPoint.CURRENT_SESSION_OPEN
        for signal in compiled_strategy.signals
    )


def resolve_round_trip_execution_mode_name(
    compiled_strategy: CompiledStrategyIR,
) -> str | None:
    if compiled_strategy.execution_semantics in (
        "next_session_round_trip",
        "current_session_round_trip_oracle",
    ):
        return compiled_strategy.execution_semantics
    return None


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def resolve_signal_availability(
    *,
    scope: CompiledSignalScope,
    param_key: str,
    shared_config: SharedConfig,
) -> CompiledSignalAvailability:
    same_day_oracle_signal = (
        scope == CompiledSignalScope.ENTRY
        and param_key in SignalProcessor._CURRENT_SESSION_ORACLE_SAME_DAY_ENTRY_ALLOWLIST
    )

    if shared_config.current_session_round_trip_oracle:
        if same_day_oracle_signal:
            return CompiledSignalAvailability(
                observation_time=CompiledAvailabilityPoint.CURRENT_SESSION_OPEN,
                available_at=CompiledAvailabilityPoint.CURRENT_SESSION_OPEN,
                decision_cutoff=CompiledAvailabilityPoint.CURRENT_SESSION_OPEN,
                execution_session=CompiledExecutionSession.CURRENT_SESSION,
            )
        return CompiledSignalAvailability(
            observation_time=CompiledAvailabilityPoint.PRIOR_SESSION_CLOSE,
            available_at=CompiledAvailabilityPoint.PRIOR_SESSION_CLOSE,
            decision_cutoff=CompiledAvailabilityPoint.CURRENT_SESSION_OPEN,
            execution_session=CompiledExecutionSession.CURRENT_SESSION,
        )

    if same_day_oracle_signal:
        return CompiledSignalAvailability(
            observation_time=CompiledAvailabilityPoint.CURRENT_SESSION_OPEN,
            available_at=CompiledAvailabilityPoint.CURRENT_SESSION_OPEN,
            decision_cutoff=CompiledAvailabilityPoint.NEXT_SESSION_OPEN,
            execution_session=CompiledExecutionSession.NEXT_SESSION,
        )

    return CompiledSignalAvailability(
        observation_time=CompiledAvailabilityPoint.CURRENT_SESSION_CLOSE,
        available_at=CompiledAvailabilityPoint.CURRENT_SESSION_CLOSE,
        decision_cutoff=CompiledAvailabilityPoint.NEXT_SESSION_OPEN,
        execution_session=CompiledExecutionSession.NEXT_SESSION,
    )


def _iter_enabled_signals(
    entry_signal_params: SignalParams,
    exit_signal_params: SignalParams,
    shared_config: SharedConfig,
) -> list[CompiledSignalIR]:
    compiled: list[CompiledSignalIR] = []

    for signal_def in SIGNAL_REGISTRY:
        if signal_def.enabled_checker(entry_signal_params):
            compiled.append(
                CompiledSignalIR(
                    signal_id=f"entry.{signal_def.param_key}",
                    scope=CompiledSignalScope.ENTRY,
                    param_key=signal_def.param_key,
                    signal_name=signal_def.name,
                    category=signal_def.category,
                    description=signal_def.description,
                    data_requirements=list(signal_def.data_requirements),
                    availability=resolve_signal_availability(
                        scope=CompiledSignalScope.ENTRY,
                        param_key=signal_def.param_key,
                        shared_config=shared_config,
                    ),
                )
            )

        if signal_def.exit_disabled or not signal_def.enabled_checker(exit_signal_params):
            continue

        compiled.append(
            CompiledSignalIR(
                signal_id=f"exit.{signal_def.param_key}",
                scope=CompiledSignalScope.EXIT,
                param_key=signal_def.param_key,
                signal_name=signal_def.name,
                category=signal_def.category,
                description=signal_def.description,
                data_requirements=list(signal_def.data_requirements),
                availability=resolve_signal_availability(
                    scope=CompiledSignalScope.EXIT,
                    param_key=signal_def.param_key,
                    shared_config=shared_config,
                ),
            )
        )

    return compiled


def _apply_config_override(
    strategy_config: dict[str, Any],
    config_override: dict[str, Any] | None,
) -> dict[str, Any]:
    if config_override is None:
        compiled_input = deepcopy(strategy_config)
        compiled_input.setdefault("entry_filter_params", {})
        return compiled_input

    compiled_input = deepcopy(strategy_config)
    for key in ("shared_config", "entry_filter_params", "exit_trigger_params"):
        override_value = config_override.get(key)
        if not isinstance(override_value, dict):
            continue
        existing = compiled_input.get(key)
        if isinstance(existing, dict):
            compiled_input[key] = _deep_merge_dict(existing, override_value)
        else:
            compiled_input[key] = deepcopy(override_value)
    compiled_input.setdefault("entry_filter_params", {})
    return compiled_input


def _resolve_required_data_domains(signals: list[CompiledSignalIR]) -> list[str]:
    domains: list[str] = []
    for signal in signals:
        for requirement in signal.data_requirements:
            base = requirement.partition(":")[0]
            if base in {"ohlc", "volume", "benchmark", "sector"}:
                domains.append("market")
            elif base == "statements":
                domains.append("statements")
            elif base == "margin":
                domains.append("margin")
    return _dedupe_preserve_order(domains)


def _resolve_required_fundamental_fields(signals: list[CompiledSignalIR]) -> list[str]:
    fields: list[str] = []
    for signal in signals:
        for requirement in signal.data_requirements:
            base, _, detail = requirement.partition(":")
            if base == "statements" and detail:
                fields.append(detail)
    return _dedupe_preserve_order(fields)


def _resolve_required_features(signals: list[CompiledSignalIR]) -> list[str]:
    return _dedupe_preserve_order(
        [requirement for signal in signals for requirement in signal.data_requirements]
    )


def compile_strategy_config(
    strategy_name: str,
    strategy_config: dict[str, Any],
    *,
    config_loader: ConfigLoader,
    config_override: dict[str, Any] | None = None,
) -> CompiledStrategyIR:
    compiled_input = _apply_config_override(strategy_config, config_override)
    validated: StrategyConfig = validate_strategy_config_dict(compiled_input)

    shared_config_dict = config_loader.merge_shared_config(compiled_input)
    shared_config = SharedConfig.model_validate(
        shared_config_dict,
        context={"resolve_stock_codes": False},
    )
    entry_signal_params = validated.entry_filter_params
    exit_signal_params = validated.exit_trigger_params or SignalParams()
    signals = _iter_enabled_signals(
        entry_signal_params=entry_signal_params,
        exit_signal_params=exit_signal_params,
        shared_config=shared_config,
    )

    return CompiledStrategyIR(
        strategy_name=strategy_name,
        execution_semantics=resolve_execution_semantics(shared_config),
        dataset_name=shared_config.dataset,
        timeframe=shared_config.timeframe,
        signals=signals,
        signal_ids=[signal.signal_id for signal in signals],
        required_data_domains=_resolve_required_data_domains(signals),
        required_features=_resolve_required_features(signals),
        required_fundamental_fields=_resolve_required_fundamental_fields(signals),
    )


def compile_strategy_requirements(
    compiled_strategy: CompiledStrategyIR,
) -> CompiledStrategyInputRequirements:
    return CompiledStrategyInputRequirements(
        required_data_domains=compiled_strategy.required_data_domains,
        required_features=compiled_strategy.required_features,
        required_fundamental_fields=compiled_strategy.required_fundamental_fields,
        signal_ids=compiled_strategy.signal_ids,
    )


def compile_runtime_strategy(
    *,
    strategy_name: str,
    shared_config: SharedConfig,
    entry_signal_params: SignalParams | None = None,
    exit_signal_params: SignalParams | None = None,
) -> CompiledStrategyIR:
    entry_params = entry_signal_params or SignalParams()
    exit_params = exit_signal_params or SignalParams()
    signals = _iter_enabled_signals(
        entry_signal_params=entry_params,
        exit_signal_params=exit_params,
        shared_config=shared_config,
    )
    return CompiledStrategyIR(
        strategy_name=strategy_name,
        execution_semantics=resolve_execution_semantics(shared_config),
        dataset_name=shared_config.dataset,
        timeframe=shared_config.timeframe,
        signals=signals,
        signal_ids=[signal.signal_id for signal in signals],
        required_data_domains=_resolve_required_data_domains(signals),
        required_features=_resolve_required_features(signals),
        required_fundamental_fields=_resolve_required_fundamental_fields(signals),
    )
