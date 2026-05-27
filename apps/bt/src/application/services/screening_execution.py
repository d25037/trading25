"""Execution data structures and request cache for market screening."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from src.domains.analytics.screening_requirements import (
    MultiDataRequirementKey,
    SectorDataRequirementKey,
    TopixDataRequirementKey,
)
from src.domains.strategy.runtime.compiler import CompiledStrategyIR
from src.entrypoints.http.schemas.screening import EntryDecidability
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams


@dataclass(frozen=True)
class StockUniverseItem:
    code: str
    company_name: str
    scale_category: str | None
    sector_33_name: str | None


@dataclass(frozen=True)
class StrategyRuntime:
    name: str
    response_name: str
    basename: str
    entry_params: SignalParams
    exit_params: SignalParams
    shared_config: SharedConfig
    compiled_strategy: CompiledStrategyIR
    entry_decidability: EntryDecidability
    dataset_universe_codes: frozenset[str] | None = None
    dataset_scope_label: str | None = None


@dataclass
class StrategyDataBundle:
    multi_data: dict[str, dict[str, Any]]
    benchmark_data: pd.DataFrame | None = None
    sector_data: dict[str, pd.DataFrame] | None = None
    stock_sector_mapping: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyExecutionInput:
    strategy: StrategyRuntime
    data_bundle: StrategyDataBundle
    load_warnings: list[str]


@dataclass(frozen=True)
class StrategyEvaluationResult:
    strategy: StrategyRuntime
    matched_rows: list[tuple[StockUniverseItem, str]]
    processed_codes: set[str]
    warnings: list[str]


@dataclass
class StrategyEvaluationAccumulator:
    strategy: StrategyRuntime
    matched_rows: list[tuple[StockUniverseItem, str]] = field(default_factory=list)
    processed_codes: set[str] = field(default_factory=set)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RequestCacheStats:
    hits: int
    misses: int


@dataclass(frozen=True)
class OptionalLoadResult:
    data: Any | None
    warning: str | None = None


class ScreeningRequestCache:
    """同一リクエスト内で data loader の結果を再利用するメモ化キャッシュ。"""

    def __init__(self) -> None:
        self._multi_data: dict[MultiDataRequirementKey, dict[str, dict[str, Any]]] = {}
        self._multi_data_errors: dict[MultiDataRequirementKey, str] = {}
        self._benchmark_data: dict[TopixDataRequirementKey, OptionalLoadResult] = {}
        self._sector_data: dict[SectorDataRequirementKey, OptionalLoadResult] = {}
        self._sector_mapping: dict[str, OptionalLoadResult] = {}
        self._hits = 0
        self._misses = 0

    @property
    def stats(self) -> RequestCacheStats:
        return RequestCacheStats(hits=self._hits, misses=self._misses)

    def _record_hit(self) -> None:
        self._hits += 1

    def _record_miss(self) -> None:
        self._misses += 1

    def get_multi_data(
        self,
        key: MultiDataRequirementKey,
        loader: Callable[[], dict[str, dict[str, Any]]],
    ) -> dict[str, dict[str, Any]]:
        if key in self._multi_data:
            self._record_hit()
            return self._multi_data[key]
        if key in self._multi_data_errors:
            self._record_hit()
            raise RuntimeError(self._multi_data_errors[key])

        self._record_miss()
        try:
            value = loader()
        except Exception as exc:
            self._multi_data_errors[key] = str(exc)
            raise

        self._multi_data[key] = value
        return value

    def get_benchmark_data(
        self,
        key: TopixDataRequirementKey,
        loader: Callable[[], pd.DataFrame],
    ) -> OptionalLoadResult:
        cached = self._benchmark_data.get(key)
        if cached is not None:
            self._record_hit()
            return cached

        self._record_miss()
        try:
            value = loader()
            result = OptionalLoadResult(data=value)
        except Exception as exc:
            result = OptionalLoadResult(data=None, warning=str(exc))
        self._benchmark_data[key] = result
        return result

    def get_sector_data(
        self,
        key: SectorDataRequirementKey,
        loader: Callable[[], dict[str, pd.DataFrame]],
    ) -> OptionalLoadResult:
        cached = self._sector_data.get(key)
        if cached is not None:
            self._record_hit()
            return cached

        self._record_miss()
        try:
            value = loader()
            result = OptionalLoadResult(data=value)
        except Exception as exc:
            result = OptionalLoadResult(data=None, warning=str(exc))
        self._sector_data[key] = result
        return result

    def get_sector_mapping(
        self,
        key: str,
        loader: Callable[[], dict[str, str]],
    ) -> OptionalLoadResult:
        cached = self._sector_mapping.get(key)
        if cached is not None:
            self._record_hit()
            return cached

        self._record_miss()
        try:
            value = loader()
            result = OptionalLoadResult(data=value)
        except Exception as exc:
            result = OptionalLoadResult(data=None, warning=str(exc))
        self._sector_mapping[key] = result
        return result
