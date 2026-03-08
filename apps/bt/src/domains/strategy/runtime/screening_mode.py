from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

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
    screening_mode: StrategyScreeningMode
    current_session_round_trip_oracle: bool


def resolve_current_session_round_trip_oracle(
    shared_config: SharedConfig,
    entry_params: SignalParams,
) -> bool:
    return (
        shared_config.current_session_round_trip_oracle
        or entry_params.oracle_index_open_gap_regime.enabled
    )


def resolve_strategy_screening_mode(
    shared_config: SharedConfig,
    entry_params: SignalParams,
) -> StrategyScreeningMode:
    if shared_config.next_session_round_trip:
        return "unsupported"
    if resolve_current_session_round_trip_oracle(shared_config, entry_params):
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
    current_session_round_trip_oracle = resolve_current_session_round_trip_oracle(
        shared_config,
        entry_params,
    )
    screening_mode = resolve_strategy_screening_mode(shared_config, entry_params)
    return LoadedStrategyScreeningConfig(
        config=config,
        shared_config=shared_config,
        entry_params=entry_params,
        exit_params=exit_params,
        screening_mode=screening_mode,
        current_session_round_trip_oracle=current_session_round_trip_oracle,
    )
