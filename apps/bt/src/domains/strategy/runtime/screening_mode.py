from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from src.domains.strategy.runtime.compiler import (
    CompiledStrategyIR,
    compile_runtime_strategy,
    uses_current_session_oracle_execution,
)
from src.domains.strategy.runtime.loader import ConfigLoader
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams

StrategyScreeningMode = Literal["standard", "oracle", "unsupported"]


@dataclass(frozen=True)
class LoadedStrategyScreeningConfig:
    config: dict[str, Any]
    shared_config: SharedConfig
    entry_params: SignalParams
    exit_params: SignalParams
    compiled_strategy: CompiledStrategyIR
    screening_mode: StrategyScreeningMode


def resolve_current_session_round_trip_oracle(
    compiled_strategy: CompiledStrategyIR,
) -> bool:
    return uses_current_session_oracle_execution(compiled_strategy)


def resolve_strategy_screening_mode(
    compiled_strategy: CompiledStrategyIR,
) -> StrategyScreeningMode:
    if compiled_strategy.execution_semantics == "next_session_round_trip":
        return "unsupported"
    if resolve_current_session_round_trip_oracle(compiled_strategy):
        return "oracle"
    return "standard"


def load_strategy_screening_config(
    config_loader: ConfigLoader,
    strategy_name: str,
) -> LoadedStrategyScreeningConfig:
    config = config_loader.load_strategy_config(strategy_name)
    shared_config_dict = config_loader.merge_shared_config(config)
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
    screening_mode = resolve_strategy_screening_mode(compiled_strategy)
    return LoadedStrategyScreeningConfig(
        config=config,
        shared_config=shared_config,
        entry_params=entry_params,
        exit_params=exit_params,
        compiled_strategy=compiled_strategy,
        screening_mode=screening_mode,
    )
