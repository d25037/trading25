"""
Screening data requirement resolution.

Pure domain functions for screening signal requirements and statement period resolution.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol
from collections.abc import Callable, Sequence

from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams
from src.shared.models.types import StatementsPeriodType as APIPeriodType


class SignalDefinitionLike(Protocol):
    """Minimal contract required for requirement resolution."""

    data_requirements: list[str]
    enabled_checker: Callable[[SignalParams], bool]


@dataclass(frozen=True)
class MultiDataRequirementKey:
    stock_codes: tuple[str, ...]
    start_date: str | None
    end_date: str | None
    include_margin_data: bool
    include_statements_data: bool
    timeframe: Literal["daily", "weekly"]
    period_type: APIPeriodType
    include_forecast_revision: bool


@dataclass(frozen=True)
class TopixDataRequirementKey:
    start_date: str | None
    end_date: str | None


@dataclass(frozen=True)
class SectorDataRequirementKey:
    start_date: str | None
    end_date: str | None


@dataclass(frozen=True)
class StrategyDataRequirements:
    include_margin_data: bool
    include_statements_data: bool
    needs_benchmark: bool
    needs_sector: bool
    multi_data_key: MultiDataRequirementKey
    benchmark_data_key: TopixDataRequirementKey | None
    sector_data_key: SectorDataRequirementKey | None
    sector_mapping_key: str | None


def needs_data_requirement(
    entry_params: SignalParams,
    exit_params: SignalParams,
    requirement: str,
    signal_registry: Sequence[SignalDefinitionLike],
) -> bool:
    """Return True when any enabled signal needs the target requirement."""
    for signal_def in signal_registry:
        if not any(req == requirement or req.startswith(f"{requirement}:") for req in signal_def.data_requirements):
            continue

        if signal_def.enabled_checker(entry_params):
            return True
        if signal_def.enabled_checker(exit_params):
            return True

    return False


def resolve_period_type(
    entry_params: SignalParams,
    exit_params: SignalParams,
) -> APIPeriodType:
    """Resolve statements period type from fundamental settings."""
    for params in (entry_params, exit_params):
        period_type = getattr(params.fundamental, "period_type", None)
        if period_type == "all":
            return "all"
        if period_type == "FY":
            return "FY"
        if period_type == "1Q":
            return "1Q"
        if period_type == "2Q":
            return "2Q"
        if period_type == "3Q":
            return "3Q"

    return "FY"


def should_include_forecast_revision(
    entry_params: SignalParams,
    exit_params: SignalParams,
) -> bool:
    """Enable forecast revision fetch when forecast-oriented fundamental signals are on."""

    def _enabled(params: SignalParams) -> bool:
        fundamental = params.fundamental
        if not fundamental.enabled:
            return False
        return bool(
            fundamental.forward_eps_growth.enabled
            or fundamental.forecast_eps_above_recent_fy_actuals.enabled
            or fundamental.peg_ratio.enabled
            or fundamental.forward_dividend_growth.enabled
            or fundamental.forward_payout_ratio.enabled
        )

    return _enabled(entry_params) or _enabled(exit_params)


def build_strategy_data_requirements(
    *,
    shared_config: SharedConfig,
    entry_params: SignalParams,
    exit_params: SignalParams,
    stock_codes: tuple[str, ...],
    start_date: str | None,
    end_date: str | None,
    signal_registry: Sequence[SignalDefinitionLike],
) -> StrategyDataRequirements:
    """Build immutable requirement keys used by screening data loaders."""
    include_margin = shared_config.include_margin_data and needs_data_requirement(
        entry_params,
        exit_params,
        "margin",
        signal_registry,
    )
    include_statements = shared_config.include_statements_data and needs_data_requirement(
        entry_params,
        exit_params,
        "statements",
        signal_registry,
    )
    include_forecast_revision = should_include_forecast_revision(entry_params, exit_params)
    needs_benchmark = needs_data_requirement(entry_params, exit_params, "benchmark", signal_registry)
    needs_sector = needs_data_requirement(entry_params, exit_params, "sector", signal_registry)

    multi_data_key = MultiDataRequirementKey(
        stock_codes=stock_codes,
        start_date=start_date,
        end_date=end_date,
        include_margin_data=include_margin,
        include_statements_data=include_statements,
        timeframe=shared_config.timeframe,
        period_type=resolve_period_type(entry_params, exit_params),
        include_forecast_revision=include_forecast_revision,
    )

    benchmark_data_key = TopixDataRequirementKey(start_date=start_date, end_date=end_date) if needs_benchmark else None
    sector_data_key = SectorDataRequirementKey(start_date=start_date, end_date=end_date) if needs_sector else None

    return StrategyDataRequirements(
        include_margin_data=include_margin,
        include_statements_data=include_statements,
        needs_benchmark=needs_benchmark,
        needs_sector=needs_sector,
        multi_data_key=multi_data_key,
        benchmark_data_key=benchmark_data_key,
        sector_data_key=sector_data_key,
        sector_mapping_key="market_stocks" if needs_sector else None,
    )
