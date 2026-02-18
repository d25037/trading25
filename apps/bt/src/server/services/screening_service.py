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
from typing import Any, Literal

import pandas as pd
from loguru import logger

from src.api.dataset.statements_mixin import APIPeriodType
from src.lib.market_db.market_reader import MarketDbReader
from src.lib.market_db.query_helpers import normalize_stock_code
from src.lib.strategy_runtime.loader import ConfigLoader
from src.models.config import SharedConfig
from src.models.signals import SignalParams, Signals
from src.paths import get_backtest_results_dir
from src.server.schemas.screening import (
    MarketScreeningResponse,
    MatchedStrategyItem,
    ScreeningResultItem,
    ScreeningSortBy,
    ScreeningSummary,
    SortOrder,
)
from src.server.services.market_code_alias import resolve_market_codes
from src.server.services.screening_market_loader import (
    load_market_multi_data,
    load_market_sector_indices,
    load_market_stock_sector_mapping,
    load_market_topix_data,
)
from src.strategies.signals.processor import SignalProcessor
from src.strategies.signals.registry import SIGNAL_REGISTRY


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

    def __init__(self, reader: MarketDbReader) -> None:
        self._reader = reader
        self._config_loader = ConfigLoader()
        self._signal_processor = SignalProcessor()

    def run_screening(
        self,
        markets: str = "prime",
        strategies: str | None = None,
        recent_days: int = 10,
        reference_date: str | None = None,
        sort_by: ScreeningSortBy = "matchedDate",
        order: SortOrder = "desc",
        limit: int | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> MarketScreeningResponse:
        """スクリーニングを実行"""
        run_started = perf_counter()
        requested_market_codes, query_market_codes = resolve_market_codes(markets)
        effective_reference_date = reference_date or self._get_latest_market_date()

        load_stage_started = perf_counter()
        stock_universe = self._load_stock_universe(query_market_codes)
        strategy_runtimes = self._resolve_strategies(strategies)
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

        response = MarketScreeningResponse(
            results=sorted_results,
            summary=summary,
            markets=requested_market_codes,
            recentDays=recent_days,
            referenceDate=effective_reference_date,
            sortBy=sort_by,
            order=order,
            lastUpdated=_now_iso(),
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

    def _resolve_strategies(self, strategies: str | None) -> list[StrategyRuntime]:
        """対象戦略をproductionカテゴリから解決する。"""
        metadata = [m for m in self._config_loader.get_strategy_metadata() if m.category == "production"]
        if not metadata:
            raise ValueError("No production strategies found")

        metadata_by_name = {m.name: m for m in metadata}
        basename_map: dict[str, list[str]] = {}
        for m in metadata:
            basename_map.setdefault(m.path.stem, []).append(m.name)

        selected_names: list[str]
        if strategies is None or not strategies.strip():
            selected_names = sorted(metadata_by_name.keys())
        else:
            requested = [s.strip() for s in strategies.split(",") if s.strip()]
            selected_names = []
            invalid: list[str] = []

            for token in requested:
                resolved = self._resolve_strategy_token(token, metadata_by_name, basename_map)
                if resolved is None:
                    invalid.append(token)
                    continue
                if resolved not in selected_names:
                    selected_names.append(resolved)

            if invalid:
                raise ValueError(
                    "Invalid strategies (production only): " + ", ".join(sorted(set(invalid)))
                )

        if not selected_names:
            raise ValueError("No valid production strategies selected")

        selected_metadata = [metadata_by_name[name] for name in selected_names]

        # basename重複時はフルネームをレスポンス名に使用
        basename_counts: dict[str, int] = {}
        for m in selected_metadata:
            basename_counts[m.path.stem] = basename_counts.get(m.path.stem, 0) + 1

        runtimes: list[StrategyRuntime] = []
        for m in selected_metadata:
            config = self._config_loader.load_strategy_config(m.name)
            shared_config_dict = self._config_loader.merge_shared_config(config)

            response_name = m.path.stem
            if basename_counts[response_name] > 1:
                response_name = m.name

            runtimes.append(
                StrategyRuntime(
                    name=m.name,
                    response_name=response_name,
                    basename=m.path.stem,
                    entry_params=SignalParams(**config.get("entry_filter_params", {})),
                    exit_params=SignalParams(**config.get("exit_trigger_params", {})),
                    shared_config=SharedConfig.model_validate(
                        shared_config_dict,
                        context={"resolve_stock_codes": False},
                    ),
                )
            )

        return runtimes

    def _resolve_strategy_token(
        self,
        token: str,
        metadata_by_name: dict[str, Any],
        basename_map: dict[str, list[str]],
    ) -> str | None:
        """クエリ指定戦略名をproduction戦略へ解決する。"""
        if token in metadata_by_name:
            return token

        if token.startswith("production/"):
            return token if token in metadata_by_name else None

        production_prefixed = f"production/{token}"
        if production_prefixed in metadata_by_name:
            return production_prefixed

        candidates = basename_map.get(token, [])
        if len(candidates) == 1:
            return candidates[0]

        return None

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

        stock_codes = tuple(stock.code for stock in stock_universe)
        cache = ScreeningRequestCache()
        inputs: list[StrategyExecutionInput] = []

        for strategy in strategy_runtimes:
            requirements = self._build_data_requirements(
                strategy=strategy,
                stock_codes=stock_codes,
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

        include_margin = (
            strategy.shared_config.include_margin_data
            and self._needs_data_requirement(strategy.entry_params, strategy.exit_params, "margin")
        )
        include_statements = (
            strategy.shared_config.include_statements_data
            and self._needs_data_requirement(strategy.entry_params, strategy.exit_params, "statements")
        )
        include_forecast_revision = self._should_include_forecast_revision(
            strategy.entry_params,
            strategy.exit_params,
        )
        needs_benchmark = self._needs_data_requirement(
            strategy.entry_params,
            strategy.exit_params,
            "benchmark",
        )
        needs_sector = self._needs_data_requirement(
            strategy.entry_params,
            strategy.exit_params,
            "sector",
        )

        multi_data_key = MultiDataRequirementKey(
            stock_codes=stock_codes,
            start_date=start_date,
            end_date=end_date,
            include_margin_data=include_margin,
            include_statements_data=include_statements,
            timeframe=strategy.shared_config.timeframe,
            period_type=self._resolve_period_type(strategy.entry_params, strategy.exit_params),
            include_forecast_revision=include_forecast_revision,
        )

        benchmark_data_key = (
            TopixDataRequirementKey(
                start_date=start_date,
                end_date=end_date,
            )
            if needs_benchmark
            else None
        )

        sector_data_key = (
            SectorDataRequirementKey(
                start_date=start_date,
                end_date=end_date,
            )
            if needs_sector
            else None
        )

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

    def _load_multi_data(
        self,
        key: MultiDataRequirementKey,
    ) -> dict[str, dict[str, Any]]:
        if key.timeframe != "daily":
            logger.warning(
                "screening market loader supports daily timeframe only; weekly data is unavailable",
            )
            return {}

        if key.include_margin_data:
            logger.warning(
                "screening market loader does not provide margin data; margin signals may be skipped",
            )

        multi_data, warnings = load_market_multi_data(
            self._reader,
            list(key.stock_codes),
            start_date=key.start_date,
            end_date=key.end_date,
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
        """戦略評価を自動並列で実行。例外は戦略単位で吸収して継続する。"""
        total = len(strategy_inputs)
        if total == 0:
            self._emit_progress(progress_callback, completed=0, total=0)
            return [], [], 1

        self._emit_progress(progress_callback, completed=0, total=total)

        worker_count = self._resolve_strategy_workers(total)
        completed = 0
        warnings: list[str] = []
        result_by_strategy: dict[str, StrategyEvaluationResult] = {}

        if worker_count == 1:
            for strategy_input in strategy_inputs:
                try:
                    result = self._evaluate_strategy_input(
                        strategy_input,
                        stock_universe=stock_universe,
                        recent_days=recent_days,
                    )
                    result_by_strategy[strategy_input.strategy.response_name] = result
                except Exception as exc:
                    logger.exception(
                        "Strategy screening failed",
                        strategy=strategy_input.strategy.name,
                    )
                    warnings.append(
                        f"{strategy_input.strategy.response_name}: evaluation failed ({exc})"
                    )
                finally:
                    completed += 1
                    self._emit_progress(progress_callback, completed=completed, total=total)
        else:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_to_input = {
                    executor.submit(
                        self._evaluate_strategy_input,
                        strategy_input,
                        stock_universe,
                        recent_days,
                    ): strategy_input
                    for strategy_input in strategy_inputs
                }

                for future in as_completed(future_to_input):
                    strategy_input = future_to_input[future]
                    try:
                        result = future.result()
                        result_by_strategy[strategy_input.strategy.response_name] = result
                    except Exception as exc:
                        logger.exception(
                            "Strategy screening failed",
                            strategy=strategy_input.strategy.name,
                        )
                        warnings.append(
                            f"{strategy_input.strategy.response_name}: evaluation failed ({exc})"
                        )
                    finally:
                        completed += 1
                        self._emit_progress(progress_callback, completed=completed, total=total)

        ordered_results: list[StrategyEvaluationResult] = []
        for strategy_input in strategy_inputs:
            result = result_by_strategy.get(strategy_input.strategy.response_name)
            if result is not None:
                ordered_results.append(result)

        return ordered_results, warnings, worker_count

    def _evaluate_strategy_input(
        self,
        strategy_input: StrategyExecutionInput,
        stock_universe: list[StockUniverseItem],
        recent_days: int,
    ) -> StrategyEvaluationResult:
        matches, processed, eval_warnings = self._evaluate_strategy(
            strategy=strategy_input.strategy,
            stock_universe=stock_universe,
            recent_days=recent_days,
            data_bundle=strategy_input.data_bundle,
        )

        return StrategyEvaluationResult(
            strategy=strategy_input.strategy,
            matched_rows=matches,
            processed_codes=processed,
            warnings=[*strategy_input.load_warnings, *eval_warnings],
        )

    def _evaluate_strategy(
        self,
        strategy: StrategyRuntime,
        stock_universe: list[StockUniverseItem],
        recent_days: int,
        data_bundle: StrategyDataBundle,
    ) -> tuple[list[tuple[StockUniverseItem, str]], set[str], list[str]]:
        """1戦略分のスクリーニング評価を実行する。"""
        if not stock_universe:
            return [], set(), []

        matches: list[tuple[StockUniverseItem, str]] = []
        processed: set[str] = set()
        warnings: list[str] = []

        for stock in stock_universe:
            stock_data = data_bundle.multi_data.get(stock.code)
            if not stock_data:
                continue

            daily = stock_data.get("daily")
            if not isinstance(daily, pd.DataFrame) or daily.empty:
                continue

            processed.add(stock.code)

            margin_data = stock_data.get("margin_daily")
            statements_data = stock_data.get("statements_daily")

            try:
                signals = self._signal_processor.generate_signals(
                    strategy_entries=pd.Series(True, index=daily.index),
                    strategy_exits=pd.Series(False, index=daily.index),
                    ohlc_data=daily,
                    entry_signal_params=strategy.entry_params,
                    exit_signal_params=strategy.exit_params,
                    margin_data=margin_data,
                    statements_data=statements_data,
                    benchmark_data=data_bundle.benchmark_data,
                    sector_data=data_bundle.sector_data,
                    stock_sector_name=data_bundle.stock_sector_mapping.get(stock.code),
                )
            except Exception as exc:
                warnings.append(f"{stock.code} signal generation failed ({exc})")
                continue

            matched_date = self._find_recent_match_date(signals, recent_days)
            if matched_date is None:
                continue

            matches.append((stock, matched_date))

        return matches, processed, warnings

    def _resolve_strategy_workers(self, strategy_count: int) -> int:
        """戦略並列数を自動決定する。"""
        if strategy_count <= 1:
            return 1

        auto_workers = min(strategy_count, os.cpu_count() or 1)
        configured = os.getenv("BT_SCREENING_MAX_STRATEGY_WORKERS")
        if configured is None:
            return max(1, auto_workers)

        try:
            configured_workers = int(configured)
            if configured_workers <= 0:
                raise ValueError("must be > 0")
            return max(1, min(auto_workers, configured_workers))
        except ValueError:
            logger.warning(
                "Invalid BT_SCREENING_MAX_STRATEGY_WORKERS. Fallback to auto workers.",
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
        for signal_def in SIGNAL_REGISTRY:
            if not any(
                req == requirement or req.startswith(f"{requirement}:")
                for req in signal_def.data_requirements
            ):
                continue

            if signal_def.enabled_checker(entry_params):
                return True
            if signal_def.enabled_checker(exit_params):
                return True

        return False

    def _resolve_period_type(
        self,
        entry_params: SignalParams,
        exit_params: SignalParams,
    ) -> APIPeriodType:
        """fundamental設定から period_type を解決する。"""
        for params in (entry_params, exit_params):
            fundamental = params.fundamental
            period_type = getattr(fundamental, "period_type", None)
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

    def _should_include_forecast_revision(
        self,
        entry_params: SignalParams,
        exit_params: SignalParams,
    ) -> bool:
        """forward_eps_growth/peg_ratio 有効時に四半期修正取得を有効化。"""

        def _enabled(params: SignalParams) -> bool:
            fundamental = params.fundamental
            if not fundamental.enabled:
                return False
            return bool(
                fundamental.forward_eps_growth.enabled
                or fundamental.peg_ratio.enabled
            )

        return _enabled(entry_params) or _enabled(exit_params)

    def _find_recent_match_date(self, signals: Signals, recent_days: int) -> str | None:
        """entries=True かつ exits=False の直近一致日を返す。"""
        entries = signals.entries.fillna(False).astype(bool)
        exits = signals.exits.fillna(False).astype(bool)
        candidates = entries & (~exits)

        recent = candidates.tail(recent_days)
        if not recent.any():
            return None

        matched_index = recent[recent].index[-1]
        return _format_date(matched_index)

    def _build_result_item(self, aggregated_item: dict[str, Any]) -> ScreeningResultItem:
        """銘柄集約データをレスポンス項目へ変換する。"""
        stock: StockUniverseItem = aggregated_item["stock"]
        matched_date: str = aggregated_item["matchedDate"]
        matched_strategies: list[MatchedStrategyItem] = aggregated_item["matchedStrategies"]

        # 戦略一覧はスコア優先（nullは常に最後）で整列
        matched_strategies.sort(
            key=lambda s: (
                s.strategyScore is None,
                -(s.strategyScore or 0.0),
                s.strategyName,
            )
        )

        best = self._pick_best_strategy(matched_strategies)

        return ScreeningResultItem(
            stockCode=stock.code,
            companyName=stock.company_name,
            scaleCategory=stock.scale_category,
            sector33Name=stock.sector_33_name,
            matchedDate=matched_date,
            bestStrategyName=best.strategyName,
            bestStrategyScore=best.strategyScore,
            matchStrategyCount=len(matched_strategies),
            matchedStrategies=matched_strategies,
        )

    def _pick_best_strategy(
        self,
        matched_strategies: list[MatchedStrategyItem],
    ) -> MatchedStrategyItem:
        """最適戦略を決定する（score優先、nullは最後）。"""
        if not matched_strategies:
            raise ValueError("matched_strategies is empty")

        non_null = [s for s in matched_strategies if s.strategyScore is not None]
        if non_null:
            return max(
                non_null,
                key=lambda s: (
                    s.strategyScore if s.strategyScore is not None else float("-inf"),
                    s.strategyName,
                ),
            )

        # 全てnullの場合は最新一致日を優先
        return max(matched_strategies, key=lambda s: (s.matchedDate, s.strategyName))

    def _sort_results(
        self,
        results: list[ScreeningResultItem],
        sort_by: ScreeningSortBy,
        order: SortOrder,
    ) -> list[ScreeningResultItem]:
        """結果ソート。bestStrategyScoreではnullを常に末尾へ配置。"""
        if sort_by == "bestStrategyScore":
            if order == "asc":
                return sorted(
                    results,
                    key=lambda r: (
                        r.bestStrategyScore is None,
                        r.bestStrategyScore if r.bestStrategyScore is not None else float("inf"),
                        r.stockCode,
                    ),
                )

            return sorted(
                results,
                key=lambda r: (
                    r.bestStrategyScore is None,
                    -(r.bestStrategyScore or 0.0),
                    r.stockCode,
                ),
            )

        reverse = order == "desc"

        if sort_by == "matchedDate":
            return sorted(results, key=lambda r: (r.matchedDate, r.stockCode), reverse=reverse)

        if sort_by == "stockCode":
            return sorted(results, key=lambda r: r.stockCode, reverse=reverse)

        if sort_by == "matchStrategyCount":
            return sorted(
                results,
                key=lambda r: (r.matchStrategyCount, r.stockCode),
                reverse=reverse,
            )

        return results

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
