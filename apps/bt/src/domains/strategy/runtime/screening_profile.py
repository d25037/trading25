from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from src.domains.strategy.runtime.compiler import (
    CompiledAvailabilityPoint,
    CompiledExecutionSession,
    CompiledSignalScope,
    CompiledStrategyIR,
    compile_runtime_strategy,
)
from src.domains.strategy.runtime.loader import ConfigLoader
from src.domains.strategy.runtime.production_requirements import (
    validate_production_strategy_dataset_requirement,
)
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams

ScreeningSupport = Literal["supported", "unsupported"]
EntryDecidability = Literal[
    "pre_open_decidable",
    "requires_same_session_observation",
]

# Decidability is measured against the open of the session where the entry executes.
_ENTRY_SESSION_OPEN_POINT = {
    CompiledExecutionSession.CURRENT_SESSION: CompiledAvailabilityPoint.CURRENT_SESSION_OPEN,
    CompiledExecutionSession.NEXT_SESSION: CompiledAvailabilityPoint.NEXT_SESSION_OPEN,
}

_AVAILABILITY_ORDER = {
    CompiledAvailabilityPoint.PRIOR_SESSION_CLOSE: 0,
    CompiledAvailabilityPoint.CURRENT_SESSION_OPEN: 1,
    CompiledAvailabilityPoint.CURRENT_SESSION_CLOSE: 2,
    CompiledAvailabilityPoint.NEXT_SESSION_OPEN: 3,
}


@dataclass(frozen=True)
class ScreeningProfile:
    screening_support: ScreeningSupport
    entry_decidability: EntryDecidability | None


@dataclass(frozen=True)
class LoadedStrategyScreeningConfig:
    config: dict[str, Any]
    shared_config: SharedConfig
    entry_params: SignalParams
    exit_params: SignalParams
    compiled_strategy: CompiledStrategyIR
    screening_profile: ScreeningProfile
    screening_support: ScreeningSupport
    entry_decidability: EntryDecidability | None


def _project_screening_shared_config(payload: dict[str, Any]) -> dict[str, Any]:
    projected = dict(payload)
    projected.pop("dataset", None)
    return projected


def resolve_entry_decidability(
    compiled_strategy: CompiledStrategyIR,
) -> EntryDecidability:
    for signal in compiled_strategy.signals:
        if signal.scope != CompiledSignalScope.ENTRY:
            continue
        session_open = _ENTRY_SESSION_OPEN_POINT[signal.availability.execution_session]
        if _AVAILABILITY_ORDER[signal.availability.available_at] >= _AVAILABILITY_ORDER[session_open]:
            return "requires_same_session_observation"
    return "pre_open_decidable"


def resolve_screening_profile(
    compiled_strategy: CompiledStrategyIR,
) -> ScreeningProfile:
    if compiled_strategy.execution_semantics == "next_session_round_trip":
        return ScreeningProfile(
            screening_support="unsupported",
            entry_decidability=None,
        )
    return ScreeningProfile(
        screening_support="supported",
        entry_decidability=resolve_entry_decidability(compiled_strategy),
    )


def load_strategy_screening_config(
    config_loader: ConfigLoader,
    strategy_name: str,
) -> LoadedStrategyScreeningConfig:
    config = config_loader.load_strategy_config(strategy_name)
    category_resolver = getattr(config_loader, "resolve_strategy_category", None)
    resolved_category = category_resolver(strategy_name) if callable(category_resolver) else None
    validate_production_strategy_dataset_requirement(
        category=resolved_category if isinstance(resolved_category, str) else None,
        config=config,
        strategy_name=strategy_name,
    )
    shared_config_dict = _project_screening_shared_config(
        config_loader.merge_shared_config(config)
    )
    shared_config = SharedConfig.model_validate(
        shared_config_dict,
        context={"resolve_stock_codes": False},
    )
    entry_params = SignalParams(**config.get("entry_filter_params", {}))
    exit_params = SignalParams(**config.get("exit_trigger_params", {}))
    compiled_strategy = compile_runtime_strategy(
        strategy_name=strategy_name,
        shared_config=shared_config,
        entry_signal_params=entry_params,
        exit_signal_params=exit_params,
    )
    screening_profile = resolve_screening_profile(compiled_strategy)
    return LoadedStrategyScreeningConfig(
        config=config,
        shared_config=shared_config,
        entry_params=entry_params,
        exit_params=exit_params,
        compiled_strategy=compiled_strategy,
        screening_profile=screening_profile,
        screening_support=screening_profile.screening_support,
        entry_decidability=screening_profile.entry_decidability,
    )
