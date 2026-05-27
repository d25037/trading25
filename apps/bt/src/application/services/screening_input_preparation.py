"""Build per-strategy screening inputs from loaded market data."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd

from src.application.services.screening_execution import (
    RequestCacheStats,
    ScreeningRequestCache,
    StockUniverseItem,
    StrategyDataBundle,
    StrategyExecutionInput,
    StrategyRuntime,
)
from src.domains.analytics.screening_requirements import (
    MultiDataRequirementKey,
    SectorDataRequirementKey,
    StrategyDataRequirements,
    TopixDataRequirementKey,
)

BuildDataRequirementsFn = Callable[
    [StrategyRuntime, tuple[str, ...], str | None, int],
    StrategyDataRequirements,
]


def prepare_strategy_inputs(
    *,
    strategy_runtimes: list[StrategyRuntime],
    stock_universe: list[StockUniverseItem],
    reference_date: str | None,
    recent_days: int,
    filter_stock_universe_by_codes: Callable[
        [list[StockUniverseItem], frozenset[str] | None],
        list[StockUniverseItem],
    ],
    build_data_requirements: BuildDataRequirementsFn,
    load_multi_data: Callable[[MultiDataRequirementKey], dict[str, dict[str, Any]]],
    load_benchmark_data: Callable[[TopixDataRequirementKey], pd.DataFrame],
    load_sector_data: Callable[[SectorDataRequirementKey], dict[str, pd.DataFrame]],
    load_sector_mapping: Callable[[], dict[str, str]],
) -> tuple[list[StrategyExecutionInput], RequestCacheStats]:
    """戦略評価に必要なデータをロードし、戦略ごとの入力を構築する。"""
    if not strategy_runtimes:
        return [], RequestCacheStats(hits=0, misses=0)

    cache = ScreeningRequestCache()
    inputs: list[StrategyExecutionInput] = []

    for strategy in strategy_runtimes:
        strategy_stock_universe = (
            filter_stock_universe_by_codes(stock_universe, strategy.dataset_universe_codes)
            if strategy.dataset_universe_codes is not None
            else stock_universe
        )
        requirements = build_data_requirements(
            strategy,
            tuple(stock.code for stock in strategy_stock_universe),
            reference_date,
            recent_days,
        )
        warnings: list[str] = []

        try:
            multi_data = cache.get_multi_data(
                requirements.multi_data_key,
                loader=lambda key=requirements.multi_data_key: load_multi_data(key),
            )
        except Exception as exc:
            warnings.append(f"multi data load failed ({exc})")
            multi_data = {}

        benchmark_data = None
        if requirements.needs_benchmark and requirements.benchmark_data_key is not None:
            benchmark_result = cache.get_benchmark_data(
                requirements.benchmark_data_key,
                loader=lambda key=requirements.benchmark_data_key: load_benchmark_data(key),
            )
            benchmark_data = benchmark_result.data
            if benchmark_result.warning:
                warnings.append(f"benchmark load failed ({benchmark_result.warning})")

        sector_data = None
        stock_sector_mapping: dict[str, str] = {}
        if requirements.needs_sector and requirements.sector_data_key is not None:
            sector_result = cache.get_sector_data(
                requirements.sector_data_key,
                loader=lambda key=requirements.sector_data_key: load_sector_data(key),
            )
            sector_data = sector_result.data
            if sector_result.warning:
                warnings.append(f"sector data load failed ({sector_result.warning})")

            if requirements.sector_mapping_key is not None:
                mapping_result = cache.get_sector_mapping(
                    requirements.sector_mapping_key,
                    loader=load_sector_mapping,
                )
                if isinstance(mapping_result.data, dict):
                    stock_sector_mapping = mapping_result.data
                if mapping_result.warning:
                    warnings.append(f"sector mapping load failed ({mapping_result.warning})")

        inputs.append(
            StrategyExecutionInput(
                strategy=strategy,
                data_bundle=StrategyDataBundle(
                    multi_data=multi_data,
                    benchmark_data=benchmark_data,
                    sector_data=sector_data,
                    stock_sector_mapping=stock_sector_mapping,
                ),
                load_warnings=warnings,
            )
        )

    return inputs, cache.stats
