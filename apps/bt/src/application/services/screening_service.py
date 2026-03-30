"""
Screening Service

production戦略YAML駆動の動的スクリーニングサービス。
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import perf_counter
from typing import Any, cast

import pandas as pd
from loguru import logger

from src.infrastructure.db.market.market_reader import MarketDbReadable
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.domains.analytics.screening_requirements import (
    APIPeriodType,
    MultiDataRequirementKey,
    SectorDataRequirementKey,
    StrategyDataRequirements,
    TopixDataRequirementKey,
    build_strategy_data_requirements,
    needs_data_requirement,
    resolve_period_type,
    should_include_forecast_revision,
)
from src.domains.analytics.screening_evaluator import (
    StockEvaluationOutcome,
    apply_stock_outcome as apply_screening_stock_outcome,
    build_per_stock_signal_cache_key as build_screening_per_stock_signal_cache_key,
    build_strategy_signal_cache_token as build_screening_strategy_signal_cache_token,
    evaluate_stock as evaluate_screening_stock,
    evaluate_strategy as evaluate_screening_strategy,
    evaluate_strategy_input as evaluate_screening_strategy_input,
)
from src.domains.analytics.screening_results import (
    build_result_item as build_screening_result_item,
    find_recent_match_date,
    pick_best_strategy,
    sort_results as sort_screening_results,
)
from src.domains.strategy.runtime.loader import ConfigLoader
from src.domains.strategy.runtime.compiler import CompiledStrategyIR
from src.domains.strategy.runtime.screening_profile import load_strategy_screening_config
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams, Signals
from src.shared.paths import get_backtest_results_dir
from src.entrypoints.http.schemas.screening import (
    EntryDecidability,
    MarketScreeningResponse,
    MatchedStrategyItem,
    ScreeningResultItem,
    ScreeningSortBy,
    ScreeningSummary,
    SortOrder,
)
from src.entrypoints.http.schemas.analytics_common import ResponseDiagnostics
from src.application.services.analytics_provenance import build_market_provenance
from src.application.services.dataset_presets import get_preset_label
from src.application.services.market_code_alias import resolve_market_codes
from src.application.services.screening_market_loader import (
    load_market_multi_data,
    load_market_sector_indices,
    load_market_stock_sector_mapping,
    load_market_topix_data,
)
from src.application.services.screening_strategy_selection import (
    build_strategy_response_names,
    build_strategy_selection_catalog,
    resolve_selected_strategy_names,
)
from src.application.services.strategy_dataset_metadata import (
    format_market_scope_label,
    resolve_dataset_metadata,
    resolve_dataset_stock_codes,
)
from src.domains.strategy.signals.processor import SignalProcessor
from src.domains.strategy.signals.registry import SIGNAL_REGISTRY


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _format_date(value: Any) -> str:
    """Datetime/文字列をYYYY-MM-DDへ正規化する。"""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    text = str(value)
    return text.split("T", 1)[0]


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


class ScreeningService:
    """戦略YAML駆動のスクリーニングサービス"""

    _WARNING_LIMIT = 50
    _DEFAULT_BACKTEST_METRIC = "sharpe_ratio"
    _DEFAULT_HISTORY_TRADING_DAYS = 520

    def __init__(self, reader: MarketDbReadable) -> None:
        self._reader = reader
        self._config_loader = ConfigLoader()
        self._signal_processor = SignalProcessor()

    def run_screening(
        self,
        entry_decidability: EntryDecidability = "pre_open_decidable",
        markets: str = "prime",
        strategies: str | None = None,
        recent_days: int = 10,
        reference_date: str | None = None,
        sort_by: ScreeningSortBy = "matchedDate",
        order: SortOrder = "desc",
        limit: int | None = None,
        scope_label: str | None = None,
        use_strategy_dataset_universe: bool = False,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> MarketScreeningResponse:
        """スクリーニングを実行"""
        run_started = perf_counter()
        requested_market_codes, query_market_codes = resolve_market_codes(markets)
        effective_reference_date = reference_date or self._get_latest_market_date()
        strategy_runtimes = self._resolve_strategies(
            strategies,
            entry_decidability=entry_decidability,
            use_strategy_dataset_universe=use_strategy_dataset_universe,
        )

        load_stage_started = perf_counter()
        stock_universe = self._load_stock_universe(query_market_codes)
        if use_strategy_dataset_universe:
            stock_universe = self._filter_stock_universe_by_codes(
                stock_universe,
                self._collect_dataset_universe_codes(strategy_runtimes),
            )
        strategy_scores, missing_metric_strategies, metric_warnings = self._load_strategy_scores(
            strategy_runtimes
        )
        strategy_inputs, cache_stats = self._prepare_strategy_inputs(
            strategy_runtimes=strategy_runtimes,
            stock_universe=stock_universe,
            reference_date=effective_reference_date,
            recent_days=recent_days,
        )
        self._log_stage_timing(
            stage="load",
            started_at=load_stage_started,
            stock_count=len(stock_universe),
            strategy_count=len(strategy_runtimes),
            cache_hits=cache_stats.hits,
            cache_misses=cache_stats.misses,
        )

        evaluate_stage_started = perf_counter()
        strategy_results, strategy_fail_warnings, worker_count = self._evaluate_strategies(
            strategy_inputs=strategy_inputs,
            stock_universe=stock_universe,
            recent_days=recent_days,
            progress_callback=progress_callback,
        )
        self._log_stage_timing(
            stage="evaluate",
            started_at=evaluate_stage_started,
            stock_count=len(stock_universe),
            strategy_count=len(strategy_runtimes),
            workers=worker_count,
        )

        aggregate_stage_started = perf_counter()
        warnings: list[str] = list(metric_warnings)
        warnings.extend(strategy_fail_warnings)

        by_strategy = {s.response_name: 0 for s in strategy_runtimes}
        processed_codes: set[str] = set()
        aggregated: dict[str, dict[str, Any]] = {}

        for strategy_result in strategy_results:
            strategy_name = strategy_result.strategy.response_name
            processed_codes |= strategy_result.processed_codes
            warnings.extend(strategy_result.warnings)

            strategy_score = strategy_scores.get(strategy_name)
            for stock, matched_date in strategy_result.matched_rows:
                by_strategy[strategy_name] += 1

                existing = aggregated.get(stock.code)
                if existing is None:
                    existing = {
                        "stock": stock,
                        "matchedDate": matched_date,
                        "matchedStrategies": [],
                    }
                    aggregated[stock.code] = existing
                elif matched_date > existing["matchedDate"]:
                    existing["matchedDate"] = matched_date

                existing["matchedStrategies"].append(
                    MatchedStrategyItem(
                        strategyName=strategy_name,
                        matchedDate=matched_date,
                        strategyScore=strategy_score,
                    )
                )

        all_results = [self._build_result_item(item) for item in aggregated.values()]
        self._log_stage_timing(
            stage="aggregate",
            started_at=aggregate_stage_started,
            match_count=len(all_results),
        )

        sort_stage_started = perf_counter()
        sorted_results = self._sort_results(all_results, sort_by=sort_by, order=order)
        match_count = len(sorted_results)
        if limit is not None and limit > 0:
            sorted_results = sorted_results[:limit]
        self._log_stage_timing(
            stage="sort",
            started_at=sort_stage_started,
            match_count=match_count,
            result_count=len(sorted_results),
        )

        summary = ScreeningSummary(
            totalStocksScreened=len(stock_universe),
            matchCount=match_count,
            skippedCount=max(0, len(stock_universe) - len(processed_codes)),
            byStrategy=by_strategy,
            strategiesEvaluated=[s.response_name for s in strategy_runtimes],
            strategiesWithoutBacktestMetrics=missing_metric_strategies,
            warnings=self._dedupe_warnings(warnings),
        )
        resolved_scope_label = scope_label or self._resolve_scope_label(
            requested_market_codes=requested_market_codes,
            strategy_runtimes=strategy_runtimes,
            use_strategy_dataset_universe=use_strategy_dataset_universe,
        )

        response = MarketScreeningResponse(
            results=sorted_results,
            summary=summary,
            entry_decidability=entry_decidability,
            markets=requested_market_codes,
            scopeLabel=resolved_scope_label,
            recentDays=recent_days,
            referenceDate=effective_reference_date,
            sortBy=sort_by,
            order=order,
            lastUpdated=_now_iso(),
            provenance=build_market_provenance(
                reference_date=effective_reference_date,
                loaded_domains=("stock_data", "stocks", "topix_data", "indices_data", "margin_data", "statements"),
                warnings=summary.warnings,
            ),
            diagnostics=ResponseDiagnostics(
                missing_required_data=[],
                used_fields=["stock_data", "stocks", "topix_data", "indices_data", "margin_data", "statements"],
                effective_period_type="multi",
                warnings=summary.warnings,
            ),
        )

        total_duration_ms = round((perf_counter() - run_started) * 1000, 2)
        logger.bind(
            event="screening_job_summary",
            duration_ms=total_duration_ms,
            stock_count=len(stock_universe),
            strategy_count=len(strategy_runtimes),
            worker_count=worker_count,
            cache_hit_count=cache_stats.hits,
            cache_miss_count=cache_stats.misses,
        ).info("screening execution summary")

        return response

    def _load_stock_universe(self, market_codes: list[str]) -> list[StockUniverseItem]:
        """市場フィルタ済み銘柄母集団を読み込む。"""
        if not market_codes:
            return []

        placeholders = ",".join("?" for _ in market_codes)
        rows = self._reader.query(
            f"""
            SELECT code, company_name, scale_category, sector_33_name
            FROM stocks
            WHERE market_code IN ({placeholders})
            ORDER BY code
            """,
            tuple(market_codes),
        )

        deduped: dict[str, StockUniverseItem] = {}
        for row in rows:
            code = normalize_stock_code(str(row["code"]))
            if code in deduped:
                continue
            deduped[code] = StockUniverseItem(
                code=code,
                company_name=row["company_name"],
                scale_category=row["scale_category"],
                sector_33_name=row["sector_33_name"],
            )

        return list(deduped.values())

    @staticmethod
    def _filter_stock_universe_by_codes(
        stock_universe: list[StockUniverseItem],
        allowed_codes: frozenset[str] | None,
    ) -> list[StockUniverseItem]:
        if not allowed_codes:
            return []
        return [stock for stock in stock_universe if stock.code in allowed_codes]

    @staticmethod
    def _collect_dataset_universe_codes(
        strategy_runtimes: list[StrategyRuntime],
    ) -> frozenset[str] | None:
        union_codes: set[str] = set()
        has_dataset_universe = False
        for strategy in strategy_runtimes:
            if strategy.dataset_universe_codes is None:
                continue
            has_dataset_universe = True
            union_codes.update(strategy.dataset_universe_codes)
        if not has_dataset_universe:
            return None
        return frozenset(union_codes)

    @staticmethod
    def _resolve_scope_label(
        *,
        requested_market_codes: list[str],
        strategy_runtimes: list[StrategyRuntime],
        use_strategy_dataset_universe: bool,
    ) -> str:
        if not use_strategy_dataset_universe:
            return format_market_scope_label(requested_market_codes)

        scope_labels: list[str] = []
        seen_labels: set[str] = set()
        for strategy in strategy_runtimes:
            scope_label = strategy.dataset_scope_label
            if scope_label is None:
                return format_market_scope_label(requested_market_codes)
            if scope_label in seen_labels:
                continue
            scope_labels.append(scope_label)
            seen_labels.add(scope_label)

        if scope_labels:
            return " + ".join(scope_labels)
        return format_market_scope_label(requested_market_codes)

    def _resolve_strategies(
        self,
        strategies: str | None,
        *,
        entry_decidability: EntryDecidability,
        use_strategy_dataset_universe: bool = False,
    ) -> list[StrategyRuntime]:
        """対象戦略をproductionカテゴリから解決する。"""
        metadata = [m for m in self._config_loader.get_strategy_metadata() if m.category == "production"]
        catalog = build_strategy_selection_catalog(
            metadata,
            load_strategy_config=lambda name: load_strategy_screening_config(
                self._config_loader,
                name,
            ),
            entry_decidability=entry_decidability,
        )
        selected_names = resolve_selected_strategy_names(
            strategies=strategies,
            catalog=catalog,
            entry_decidability=entry_decidability,
        )
        response_names = build_strategy_response_names(
            catalog.metadata_by_name,
            selected_names,
        )

        runtimes: list[StrategyRuntime] = []
        for name in selected_names:
            metadata_entry = catalog.metadata_by_name[name]
            runtime_payload = catalog.runtime_payloads[name]
            screening_support = runtime_payload.screening_support
            resolved_entry_decidability = runtime_payload.entry_decidability
            if screening_support != "supported" or resolved_entry_decidability is None:
                raise ValueError(f"Unsupported screening strategy selected: {name}")

            dataset_universe_codes: frozenset[str] | None = None
            dataset_scope_label: str | None = None
            if use_strategy_dataset_universe:
                try:
                    dataset_metadata = resolve_dataset_metadata(runtime_payload.shared_config.dataset)
                    dataset_universe_codes = frozenset(
                        resolve_dataset_stock_codes(runtime_payload.shared_config.dataset)
                    )
                    if dataset_metadata.dataset_preset is not None:
                        dataset_scope_label = get_preset_label(dataset_metadata.dataset_preset)
                except Exception as exc:
                    raise ValueError(
                        f"Invalid dataset universe for screening strategy {name}: {exc}"
                    ) from exc

            runtime = StrategyRuntime(
                name=name,
                response_name=response_names[name],
                basename=metadata_entry.path.stem,
                entry_params=runtime_payload.entry_params,
                exit_params=runtime_payload.exit_params,
                shared_config=runtime_payload.shared_config,
                compiled_strategy=runtime_payload.compiled_strategy,
                entry_decidability=cast(EntryDecidability, resolved_entry_decidability),
                dataset_universe_codes=dataset_universe_codes,
                dataset_scope_label=dataset_scope_label,
            )
            runtimes.append(runtime)

        return runtimes

    def _load_strategy_scores(
        self,
        strategies: list[StrategyRuntime],
    ) -> tuple[dict[str, float | None], list[str], list[str]]:
        """各戦略の最新バックテスト指標（固定: sharpe_ratio）を取得する。"""
        scores: dict[str, float | None] = {}
        missing: list[str] = []
        warnings: list[str] = []

        for strategy in strategies:
            score, warning = self._load_latest_metric(strategy.basename)
            scores[strategy.response_name] = score
            if score is None:
                missing.append(strategy.response_name)
            if warning:
                warnings.append(f"{strategy.response_name}: {warning}")

        return scores, missing, warnings

    def _load_latest_metric(
        self,
        strategy_basename: str,
    ) -> tuple[float | None, str | None]:
        """戦略ディレクトリ内の最新*.metrics.jsonから sharpe_ratio を取得する。"""
        strategy_dir = get_backtest_results_dir(strategy_basename)
        if not strategy_dir.exists():
            return None, None

        metric_files = sorted(
            strategy_dir.glob("*.metrics.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not metric_files:
            return None, None

        latest = metric_files[0]
        metric_name = self._DEFAULT_BACKTEST_METRIC

        try:
            payload = json.loads(latest.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return None, f"failed to read metrics ({latest.name}: {exc})"

        value = payload.get(metric_name)
        if value is None:
            return None, None

        if isinstance(value, (int, float)):
            return float(value), None

        try:
            return float(value), None
        except (TypeError, ValueError):
            return None, f"metric {metric_name} is not numeric in {latest.name}"

    def _prepare_strategy_inputs(
        self,
        strategy_runtimes: list[StrategyRuntime],
        stock_universe: list[StockUniverseItem],
        reference_date: str | None,
        recent_days: int,
    ) -> tuple[list[StrategyExecutionInput], RequestCacheStats]:
        """戦略評価に必要なデータをロードし、戦略ごとの入力を構築する。"""
        if not strategy_runtimes:
            return [], RequestCacheStats(hits=0, misses=0)

        cache = ScreeningRequestCache()
        inputs: list[StrategyExecutionInput] = []

        for strategy in strategy_runtimes:
            strategy_stock_universe = (
                self._filter_stock_universe_by_codes(stock_universe, strategy.dataset_universe_codes)
                if strategy.dataset_universe_codes is not None
                else stock_universe
            )
            requirements = self._build_data_requirements(
                strategy=strategy,
                stock_codes=tuple(stock.code for stock in strategy_stock_universe),
                reference_date=reference_date,
                recent_days=recent_days,
            )
            warnings: list[str] = []

            try:
                multi_data = cache.get_multi_data(
                    requirements.multi_data_key,
                    loader=lambda key=requirements.multi_data_key: self._load_multi_data(key),
                )
            except Exception as exc:
                warnings.append(f"multi data load failed ({exc})")
                multi_data = {}

            benchmark_data = None
            if requirements.needs_benchmark and requirements.benchmark_data_key is not None:
                benchmark_result = cache.get_benchmark_data(
                    requirements.benchmark_data_key,
                    loader=lambda key=requirements.benchmark_data_key: self._load_benchmark_data(key),
                )
                benchmark_data = benchmark_result.data
                if benchmark_result.warning:
                    warnings.append(f"benchmark load failed ({benchmark_result.warning})")

            sector_data = None
            stock_sector_mapping: dict[str, str] = {}
            if requirements.needs_sector and requirements.sector_data_key is not None:
                sector_result = cache.get_sector_data(
                    requirements.sector_data_key,
                    loader=lambda key=requirements.sector_data_key: self._load_sector_data(key),
                )
                sector_data = sector_result.data
                if sector_result.warning:
                    warnings.append(f"sector data load failed ({sector_result.warning})")

                if requirements.sector_mapping_key is not None:
                    mapping_result = cache.get_sector_mapping(
                        requirements.sector_mapping_key,
                        loader=self._load_sector_mapping,
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

    def _build_data_requirements(
        self,
        strategy: StrategyRuntime,
        stock_codes: tuple[str, ...],
        reference_date: str | None,
        recent_days: int,
    ) -> StrategyDataRequirements:
        start_date, end_date = self._resolve_date_range(
            shared_config=strategy.shared_config,
            reference_date=reference_date,
            recent_days=recent_days,
        )
        return build_strategy_data_requirements(
            shared_config=strategy.shared_config,
            entry_params=strategy.entry_params,
            exit_params=strategy.exit_params,
            stock_codes=stock_codes,
            start_date=start_date,
            end_date=end_date,
            signal_registry=SIGNAL_REGISTRY,
        )

    def _load_multi_data(
        self,
        key: MultiDataRequirementKey,
    ) -> dict[str, dict[str, Any]]:
        if key.timeframe != "daily":
            logger.warning(
                "screening market loader supports daily timeframe only; weekly data is unavailable",
            )
            return {}

        multi_data, warnings = load_market_multi_data(
            self._reader,
            list(key.stock_codes),
            start_date=key.start_date,
            end_date=key.end_date,
            include_margin_data=key.include_margin_data,
            include_statements_data=key.include_statements_data,
            period_type=key.period_type,
            include_forecast_revision=key.include_forecast_revision,
        )
        for warning in warnings:
            logger.warning("screening market loader warning: {}", warning)
        return multi_data

    def _load_benchmark_data(self, key: TopixDataRequirementKey) -> pd.DataFrame:
        return load_market_topix_data(
            self._reader,
            start_date=key.start_date,
            end_date=key.end_date,
        )

    def _load_sector_data(self, key: SectorDataRequirementKey) -> dict[str, pd.DataFrame]:
        return load_market_sector_indices(
            self._reader,
            start_date=key.start_date,
            end_date=key.end_date,
        )

    def _load_sector_mapping(self) -> dict[str, str]:
        return load_market_stock_sector_mapping(self._reader)

    def _evaluate_strategies(
        self,
        strategy_inputs: list[StrategyExecutionInput],
        stock_universe: list[StockUniverseItem],
        recent_days: int,
        progress_callback: Callable[[int, int], None] | None,
    ) -> tuple[list[StrategyEvaluationResult], list[str], int]:
        """銘柄主導(stock-major)で戦略評価を実行する。"""
        strategy_count = len(strategy_inputs)
        if strategy_count == 0:
            self._emit_progress(progress_callback, completed=0, total=0)
            return [], [], 1

        accumulators: dict[str, StrategyEvaluationAccumulator] = {}
        strategy_cache_tokens: dict[str, str] = {}
        for strategy_input in strategy_inputs:
            strategy_name = strategy_input.strategy.response_name
            accumulators[strategy_name] = StrategyEvaluationAccumulator(
                strategy=strategy_input.strategy,
                warnings=list(strategy_input.load_warnings),
            )
            strategy_cache_tokens[strategy_name] = self._build_strategy_signal_cache_token(
                strategy_input.strategy
            )

        total_stocks = len(stock_universe)
        if total_stocks == 0:
            self._emit_progress(progress_callback, completed=0, total=0)
            ordered_results = self._build_ordered_strategy_results(
                strategy_inputs=strategy_inputs,
                accumulators=accumulators,
            )
            return ordered_results, [], 1

        self._emit_progress(progress_callback, completed=0, total=total_stocks)

        worker_count = self._resolve_stock_workers(total_stocks)
        warnings: list[str] = []
        completed = 0

        if worker_count == 1:
            for stock in stock_universe:
                try:
                    outcome = self._evaluate_stock(
                        stock=stock,
                        strategy_inputs=strategy_inputs,
                        recent_days=recent_days,
                        strategy_cache_tokens=strategy_cache_tokens,
                    )
                except Exception as exc:
                    logger.exception(
                        "Stock screening failed",
                        stock_code=stock.code,
                    )
                    warnings.append(f"{stock.code}: evaluation failed ({exc})")
                else:
                    self._apply_stock_outcome(outcome, accumulators)
                finally:
                    completed += 1
                    self._emit_progress(progress_callback, completed=completed, total=total_stocks)
        else:
            outcomes_by_code: dict[str, StockEvaluationOutcome] = {}
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_to_stock = {
                    executor.submit(
                        self._evaluate_stock,
                        stock,
                        strategy_inputs,
                        recent_days,
                        strategy_cache_tokens,
                    ): stock
                    for stock in stock_universe
                }

                for future in as_completed(future_to_stock):
                    stock = future_to_stock[future]
                    try:
                        outcome = future.result()
                        outcomes_by_code[stock.code] = outcome
                    except Exception as exc:
                        logger.exception(
                            "Stock screening failed",
                            stock_code=stock.code,
                        )
                        warnings.append(f"{stock.code}: evaluation failed ({exc})")
                    finally:
                        completed += 1
                        self._emit_progress(progress_callback, completed=completed, total=total_stocks)

            for stock in stock_universe:
                outcome = outcomes_by_code.get(stock.code)
                if outcome is None:
                    continue
                self._apply_stock_outcome(outcome, accumulators)

        ordered_results = self._build_ordered_strategy_results(
            strategy_inputs=strategy_inputs,
            accumulators=accumulators,
        )

        return ordered_results, warnings, worker_count

    def _build_ordered_strategy_results(
        self,
        strategy_inputs: list[StrategyExecutionInput],
        accumulators: dict[str, StrategyEvaluationAccumulator],
    ) -> list[StrategyEvaluationResult]:
        ordered_results: list[StrategyEvaluationResult] = []
        for strategy_input in strategy_inputs:
            accumulator = accumulators[strategy_input.strategy.response_name]
            ordered_results.append(
                StrategyEvaluationResult(
                    strategy=accumulator.strategy,
                    matched_rows=accumulator.matched_rows,
                    processed_codes=accumulator.processed_codes,
                    warnings=accumulator.warnings,
                )
            )
        return ordered_results

    def _build_strategy_signal_cache_token(self, strategy: StrategyRuntime) -> str:
        return build_screening_strategy_signal_cache_token(strategy)

    def _build_per_stock_signal_cache_key(
        self,
        strategy_cache_token: str,
        daily: pd.DataFrame,
        margin_data: Any,
        statements_data: Any,
        data_bundle: StrategyDataBundle,
        stock_code: str,
        recent_days: int,
    ) -> tuple[Any, ...]:
        return build_screening_per_stock_signal_cache_key(
            strategy_cache_token,
            daily,
            margin_data,
            statements_data,
            data_bundle,
            stock_code,
            recent_days,
        )

    def _evaluate_stock(
        self,
        stock: StockUniverseItem,
        strategy_inputs: list[StrategyExecutionInput],
        recent_days: int,
        strategy_cache_tokens: dict[str, str],
    ) -> StockEvaluationOutcome:
        return evaluate_screening_stock(
            stock=stock,
            strategy_inputs=strategy_inputs,
            recent_days=recent_days,
            strategy_cache_tokens=strategy_cache_tokens,
            generate_signals=self._signal_processor.generate_signals,
            find_recent_match_date=self._find_recent_match_date,
            build_strategy_signal_cache_token_fn=self._build_strategy_signal_cache_token,
            build_per_stock_signal_cache_key_fn=self._build_per_stock_signal_cache_key,
        )

    def _apply_stock_outcome(
        self,
        outcome: StockEvaluationOutcome,
        accumulators: dict[str, StrategyEvaluationAccumulator],
    ) -> None:
        apply_screening_stock_outcome(outcome, accumulators)

    def _evaluate_strategy_input(
        self,
        strategy_input: StrategyExecutionInput,
        stock_universe: list[StockUniverseItem],
        recent_days: int,
    ) -> StrategyEvaluationResult:
        summary = evaluate_screening_strategy_input(
            strategy_input=strategy_input,
            stock_universe=stock_universe,
            recent_days=recent_days,
            generate_signals=self._signal_processor.generate_signals,
            find_recent_match_date=self._find_recent_match_date,
        )

        return StrategyEvaluationResult(
            strategy=strategy_input.strategy,
            matched_rows=cast(list[tuple[StockUniverseItem, str]], summary.matched_rows),
            processed_codes=summary.processed_codes,
            warnings=summary.warnings,
        )

    def _evaluate_strategy(
        self,
        strategy: StrategyRuntime,
        stock_universe: list[StockUniverseItem],
        recent_days: int,
        data_bundle: StrategyDataBundle,
    ) -> tuple[list[tuple[StockUniverseItem, str]], set[str], list[str]]:
        """1戦略分のスクリーニング評価を実行する。"""
        summary = evaluate_screening_strategy(
            strategy=strategy,
            stock_universe=stock_universe,
            recent_days=recent_days,
            data_bundle=data_bundle,
            generate_signals=self._signal_processor.generate_signals,
            find_recent_match_date=self._find_recent_match_date,
        )
        return (
            cast(list[tuple[StockUniverseItem, str]], summary.matched_rows),
            summary.processed_codes,
            summary.warnings,
        )

    def _resolve_strategy_workers(self, strategy_count: int) -> int:
        """戦略並列数を自動決定する。"""
        return self._resolve_parallel_workers(
            work_count=strategy_count,
            env_names=("BT_SCREENING_MAX_STRATEGY_WORKERS",),
        )

    def _resolve_stock_workers(self, stock_count: int) -> int:
        """銘柄並列数を自動決定する。"""
        return self._resolve_parallel_workers(
            work_count=stock_count,
            env_names=(
                "BT_SCREENING_MAX_STOCK_WORKERS",
                "BT_SCREENING_MAX_STRATEGY_WORKERS",
            ),
        )

    def _resolve_parallel_workers(
        self,
        work_count: int,
        env_names: tuple[str, ...],
    ) -> int:
        if work_count <= 1:
            return 1

        auto_workers = min(work_count, os.cpu_count() or 1)
        configured_name: str | None = None
        configured: str | None = None
        for env_name in env_names:
            raw = os.getenv(env_name)
            if raw is None:
                continue
            configured_name = env_name
            configured = raw
            break

        if configured is None:
            return max(1, auto_workers)

        try:
            configured_workers = int(configured)
            if configured_workers <= 0:
                raise ValueError("must be > 0")
            return max(1, min(auto_workers, configured_workers))
        except ValueError:
            logger.warning(
                f"Invalid {configured_name}. Fallback to auto workers.",
                value=configured,
            )
            return max(1, auto_workers)

    def _emit_progress(
        self,
        progress_callback: Callable[[int, int], None] | None,
        completed: int,
        total: int,
    ) -> None:
        if progress_callback is None:
            return
        try:
            progress_callback(completed, total)
        except Exception as exc:
            logger.warning(f"screening progress callback failed: {exc}")

    def _log_stage_timing(
        self,
        stage: str,
        started_at: float,
        **extra: Any,
    ) -> None:
        duration_ms = round((perf_counter() - started_at) * 1000, 2)
        logger.bind(
            event="screening_stage_timing",
            stage=stage,
            duration_ms=duration_ms,
            **extra,
        ).info("screening stage completed")

    def _resolve_date_range(
        self,
        shared_config: SharedConfig,
        reference_date: str | None,
        recent_days: int,
    ) -> tuple[str | None, str | None]:
        """shared_config とクエリ日付からロード対象期間を解決する。"""
        start_date = shared_config.start_date or None
        end_date = shared_config.end_date or None

        if reference_date:
            end_date = reference_date
        elif end_date is None:
            end_date = self._get_latest_market_date()

        if start_date is None and end_date is not None:
            history_days = self._resolve_history_trading_days(recent_days)
            if history_days > 1:
                start_date = self._get_trading_date_before(end_date, history_days - 1)

        return start_date, end_date

    def _resolve_history_trading_days(self, recent_days: int) -> int:
        """screening 読み込み対象の営業日本数を決定する。"""
        default_days = self._DEFAULT_HISTORY_TRADING_DAYS
        configured = os.getenv("BT_SCREENING_HISTORY_TRADING_DAYS")
        if configured is not None:
            try:
                value = int(configured)
                if value > 0:
                    default_days = value
                else:
                    raise ValueError("must be > 0")
            except ValueError:
                logger.warning(
                    "Invalid BT_SCREENING_HISTORY_TRADING_DAYS. Fallback to default.",
                    value=configured,
                )

        return max(recent_days, default_days)

    def _get_latest_market_date(self) -> str | None:
        try:
            row = self._reader.query_one("SELECT MAX(date) as max_date FROM stock_data")
        except Exception:
            return None
        if row is None:
            return None
        return row["max_date"]

    def _get_trading_date_before(self, date: str, offset: int) -> str | None:
        if offset < 0:
            return date
        try:
            row = self._reader.query_one(
                "SELECT DISTINCT date FROM stock_data WHERE date <= ? ORDER BY date DESC LIMIT 1 OFFSET ?",
                (date, offset),
            )
        except Exception:
            return None
        if row is None:
            try:
                oldest = self._reader.query_one(
                    "SELECT MIN(date) AS min_date FROM stock_data WHERE date <= ?",
                    (date,),
                )
            except Exception:
                return None
            if oldest is None:
                return None
            return oldest["min_date"]
        return row["date"]

    def _needs_data_requirement(
        self,
        entry_params: SignalParams,
        exit_params: SignalParams,
        requirement: str,
    ) -> bool:
        """指定データ要件に依存する有効シグナルがあるか判定する。"""
        return needs_data_requirement(
            entry_params,
            exit_params,
            requirement,
            signal_registry=SIGNAL_REGISTRY,
        )

    def _resolve_period_type(
        self,
        entry_params: SignalParams,
        exit_params: SignalParams,
    ) -> APIPeriodType:
        """fundamental設定から period_type を解決する。"""
        return resolve_period_type(entry_params, exit_params)

    def _should_include_forecast_revision(
        self,
        entry_params: SignalParams,
        exit_params: SignalParams,
    ) -> bool:
        """予想系シグナル有効時に四半期修正取得を有効化。"""
        return should_include_forecast_revision(entry_params, exit_params)

    def _find_recent_match_date(self, signals: Signals, recent_days: int) -> str | None:
        """entries=True かつ exits=False の直近一致日を返す。"""
        return find_recent_match_date(signals, recent_days, format_date=_format_date)

    def _build_result_item(self, aggregated_item: dict[str, Any]) -> ScreeningResultItem:
        """銘柄集約データをレスポンス項目へ変換する。"""
        stock: StockUniverseItem = aggregated_item["stock"]
        matched_date: str = aggregated_item["matchedDate"]
        matched_strategies: list[MatchedStrategyItem] = aggregated_item["matchedStrategies"]
        result_payload = build_screening_result_item(stock, matched_date, matched_strategies)
        return ScreeningResultItem.model_validate(result_payload)

    def _pick_best_strategy(
        self,
        matched_strategies: list[MatchedStrategyItem],
    ) -> MatchedStrategyItem:
        """最適戦略を決定する（score優先、nullは最後）。"""
        return cast(MatchedStrategyItem, pick_best_strategy(matched_strategies))

    def _sort_results(
        self,
        results: list[ScreeningResultItem],
        sort_by: ScreeningSortBy,
        order: SortOrder,
    ) -> list[ScreeningResultItem]:
        """結果ソート。bestStrategyScoreではnullを常に末尾へ配置。"""
        return sort_screening_results(results, sort_by, order)

    def _dedupe_warnings(self, warnings: list[str]) -> list[str]:
        """警告を順序保持で重複排除し、件数を制限する。"""
        deduped: list[str] = []
        seen: set[str] = set()

        for warning in warnings:
            if warning in seen:
                continue
            seen.add(warning)
            deduped.append(warning)
            if len(deduped) >= self._WARNING_LIMIT:
                break

        if len(warnings) > len(deduped):
            deduped.append("additional warnings were truncated")

        return deduped
