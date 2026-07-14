"""
Screening Service

production戦略YAML駆動の動的スクリーニングサービス。
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from time import perf_counter
from typing import Any, cast

import pandas as pd
from loguru import logger

from src.application.contracts.screening import (
    MarketScreeningResponse,
    MatchedStrategyItem,
    ScreeningResultItem,
    ScreeningSummary,
)
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
    ScreeningSortBy,
    SortOrder,
    build_result_item as build_screening_result_item,
    find_recent_match_date,
    pick_best_strategy,
    sort_results as sort_screening_results,
)
from src.application.services.screening_execution import (
    RequestCacheStats,
    ScreeningRequestCache,
    StockUniverseItem,
    StrategyDataBundle,
    StrategyEvaluationAccumulator,
    StrategyEvaluationResult,
    StrategyExecutionInput,
    StrategyRuntime,
)
from src.application.services.screening_market_loader import (
    load_market_multi_data,
    load_market_sector_indices,
    load_market_topix_data,
)
from src.application.services.screening_response_builder import (
    aggregate_screening_results,
    build_screening_response,
    dedupe_screening_warnings,
)
from src.application.services.screening_reference_dates import (
    get_latest_market_date as get_screening_latest_market_date,
    get_trading_date_before as get_screening_trading_date_before,
    resolve_date_range as resolve_screening_date_range,
    resolve_history_trading_days as resolve_screening_history_trading_days,
    stock_master_daily_has_date as screening_stock_master_daily_has_date,
    table_exists as screening_table_exists,
)
from src.application.services.screening_runtime_control import (
    emit_progress as emit_screening_progress,
    log_stage_timing as log_screening_stage_timing,
    resolve_parallel_workers as resolve_screening_parallel_workers,
    resolve_stock_workers as resolve_screening_stock_workers,
    resolve_strategy_workers as resolve_screening_strategy_workers,
)
from src.application.services.screening_input_preparation import (
    prepare_strategy_inputs as prepare_screening_strategy_inputs,
)
from src.application.services.screening_evaluation_runner import (
    build_ordered_strategy_results as build_screening_ordered_strategy_results,
    run_stock_major_evaluation,
)
from src.application.services.screening_strategy_runtime import resolve_screening_strategy_runtimes
from src.application.services.screening_strategy_metrics import (
    load_latest_metric as load_screening_latest_metric,
    load_strategy_scores as load_screening_strategy_scores,
)
from src.application.services.screening_universe import (
    collect_dataset_universe_codes,
    collect_dataset_universe_codes_as_of,
    filter_stock_universe_by_codes,
    load_stock_universe,
    resolve_scope_label,
    resolve_universe_codes_from_stock_master,
)
from src.domains.strategy.runtime.loader import ConfigLoader
from src.domains.strategy.runtime.screening_profile import EntryDecidability
from src.domains.strategy.signals.processor import SignalProcessor
from src.domains.strategy.signals.registry import SIGNAL_REGISTRY
from src.infrastructure.db.market.market_reader import MarketDbReadable
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams, Signals
from src.shared.paths import get_backtest_results_dir
from src.shared.utils.market_code_alias import resolve_market_codes

__all__ = [
    "MultiDataRequirementKey",
    "RequestCacheStats",
    "ScreeningRequestCache",
    "ScreeningService",
    "SectorDataRequirementKey",
    "StockEvaluationOutcome",
    "StockUniverseItem",
    "StrategyDataBundle",
    "StrategyEvaluationResult",
    "StrategyExecutionInput",
    "StrategyRuntime",
    "TopixDataRequirementKey",
    "_format_date",
]


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _format_date(value: Any) -> str:
    """Datetime/文字列をYYYY-MM-DDへ正規化する。"""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    text = str(value)
    return text.split("T", 1)[0]


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
        if effective_reference_date is None:
            raise ValueError("No market date available for screening")
        strategy_runtimes = self._resolve_strategies(
            strategies,
            entry_decidability=entry_decidability,
            use_strategy_dataset_universe=use_strategy_dataset_universe,
        )

        load_stage_started = perf_counter()
        stock_universe = self._load_stock_universe(query_market_codes, effective_reference_date)
        if use_strategy_dataset_universe:
            stock_universe = self._filter_stock_universe_by_codes(
                stock_universe,
                self._collect_dataset_universe_codes_as_of(
                    strategy_runtimes,
                    as_of_date=effective_reference_date,
                ),
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
        aggregation = aggregate_screening_results(
            strategy_runtimes=strategy_runtimes,
            strategy_results=strategy_results,
            strategy_scores=strategy_scores,
            build_result_item=self._build_result_item,
        )
        warnings.extend(aggregation.warnings)
        self._log_stage_timing(
            stage="aggregate",
            started_at=aggregate_stage_started,
            match_count=len(aggregation.results),
        )

        sort_stage_started = perf_counter()
        sorted_results = self._sort_results(aggregation.results, sort_by=sort_by, order=order)
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
            skippedCount=max(0, len(stock_universe) - len(aggregation.processed_codes)),
            byStrategy=aggregation.by_strategy,
            strategiesEvaluated=[s.response_name for s in strategy_runtimes],
            strategiesWithoutBacktestMetrics=missing_metric_strategies,
            warnings=self._dedupe_warnings(warnings),
        )
        resolved_scope_label = scope_label or self._resolve_scope_label(
            requested_market_codes=requested_market_codes,
            strategy_runtimes=strategy_runtimes,
            use_strategy_dataset_universe=use_strategy_dataset_universe,
        )

        response = build_screening_response(
            results=sorted_results,
            summary=summary,
            entry_decidability=entry_decidability,
            requested_market_codes=requested_market_codes,
            scope_label=resolved_scope_label,
            recent_days=recent_days,
            reference_date=effective_reference_date,
            sort_by=sort_by,
            order=order,
            last_updated=_now_iso(),
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

    def _load_stock_universe(
        self,
        market_codes: list[str],
        as_of_date: str,
    ) -> list[StockUniverseItem]:
        """市場フィルタ済み銘柄母集団を読み込む。"""
        return load_stock_universe(
            self._reader,
            market_codes,
            as_of_date=as_of_date,
            stock_master_daily_has_date=self._stock_master_daily_has_date,
        )

    @staticmethod
    def _filter_stock_universe_by_codes(
        stock_universe: list[StockUniverseItem],
        allowed_codes: frozenset[str] | None,
    ) -> list[StockUniverseItem]:
        return filter_stock_universe_by_codes(stock_universe, allowed_codes)

    @staticmethod
    def _collect_dataset_universe_codes(
        strategy_runtimes: list[StrategyRuntime],
    ) -> frozenset[str] | None:
        return collect_dataset_universe_codes(strategy_runtimes)

    def _collect_dataset_universe_codes_as_of(
        self,
        strategy_runtimes: list[StrategyRuntime],
        *,
        as_of_date: str,
    ) -> frozenset[str] | None:
        return collect_dataset_universe_codes_as_of(
            strategy_runtimes,
            as_of_date=as_of_date,
            has_stock_master_daily=lambda: self._table_exists("stock_master_daily"),
            resolve_universe_codes_from_stock_master=self._resolve_universe_codes_from_stock_master,
        )

    def _resolve_universe_codes_from_stock_master(self, *, preset: str, as_of_date: str) -> set[str]:
        return resolve_universe_codes_from_stock_master(
            self._reader,
            preset=preset,
            as_of_date=as_of_date,
        )

    @staticmethod
    def _resolve_scope_label(
        *,
        requested_market_codes: list[str],
        strategy_runtimes: list[StrategyRuntime],
        use_strategy_dataset_universe: bool,
    ) -> str:
        return resolve_scope_label(
            requested_market_codes=requested_market_codes,
            strategy_runtimes=strategy_runtimes,
            use_strategy_dataset_universe=use_strategy_dataset_universe,
        )

    def _resolve_strategies(
        self,
        strategies: str | None,
        *,
        entry_decidability: EntryDecidability,
        use_strategy_dataset_universe: bool = False,
    ) -> list[StrategyRuntime]:
        """対象戦略をproductionカテゴリから解決する。"""
        return resolve_screening_strategy_runtimes(
            self._config_loader,
            strategies,
            entry_decidability=entry_decidability,
            use_strategy_dataset_universe=use_strategy_dataset_universe,
        )

    def _load_strategy_scores(
        self,
        strategies: list[StrategyRuntime],
    ) -> tuple[dict[str, float | None], list[str], list[str]]:
        """各戦略の最新バックテスト指標（固定: sharpe_ratio）を取得する。"""
        return load_screening_strategy_scores(
            strategies,
            load_latest_metric_fn=self._load_latest_metric,
        )

    def _load_latest_metric(
        self,
        strategy_basename: str,
    ) -> tuple[float | None, str | None]:
        """戦略ディレクトリ内の最新*.metrics.jsonから sharpe_ratio を取得する。"""
        return load_screening_latest_metric(
            strategy_basename,
            metric_name=self._DEFAULT_BACKTEST_METRIC,
            get_backtest_results_dir=get_backtest_results_dir,
        )

    def _prepare_strategy_inputs(
        self,
        strategy_runtimes: list[StrategyRuntime],
        stock_universe: list[StockUniverseItem],
        reference_date: str | None,
        recent_days: int,
    ) -> tuple[list[StrategyExecutionInput], RequestCacheStats]:
        """戦略評価に必要なデータをロードし、戦略ごとの入力を構築する。"""
        return prepare_screening_strategy_inputs(
            strategy_runtimes=strategy_runtimes,
            stock_universe=stock_universe,
            reference_date=reference_date,
            recent_days=recent_days,
            filter_stock_universe_by_codes=self._filter_stock_universe_by_codes,
            build_data_requirements=lambda strategy, stock_codes, resolved_reference_date, resolved_recent_days: self._build_data_requirements(
                strategy=strategy,
                stock_codes=stock_codes,
                reference_date=resolved_reference_date,
                recent_days=resolved_recent_days,
            ),
            load_multi_data=self._load_multi_data,
            load_benchmark_data=self._load_benchmark_data,
            load_sector_data=self._load_sector_data,
        )

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

    def _evaluate_strategies(
        self,
        strategy_inputs: list[StrategyExecutionInput],
        stock_universe: list[StockUniverseItem],
        recent_days: int,
        progress_callback: Callable[[int, int], None] | None,
    ) -> tuple[list[StrategyEvaluationResult], list[str], int]:
        """銘柄主導(stock-major)で戦略評価を実行する。"""
        return run_stock_major_evaluation(
            strategy_inputs=strategy_inputs,
            stock_universe=stock_universe,
            recent_days=recent_days,
            progress_callback=progress_callback,
            build_strategy_signal_cache_token=self._build_strategy_signal_cache_token,
            evaluate_stock=self._evaluate_stock,
            apply_stock_outcome=self._apply_stock_outcome,
            resolve_stock_workers=self._resolve_stock_workers,
            emit_progress=lambda callback, completed, total: self._emit_progress(
                callback,
                completed=completed,
                total=total,
            ),
        )

    def _build_ordered_strategy_results(
        self,
        strategy_inputs: list[StrategyExecutionInput],
        accumulators: dict[str, StrategyEvaluationAccumulator],
    ) -> list[StrategyEvaluationResult]:
        return build_screening_ordered_strategy_results(
            strategy_inputs=strategy_inputs,
            accumulators=accumulators,
        )

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
        return resolve_screening_strategy_workers(strategy_count)

    def _resolve_stock_workers(self, stock_count: int) -> int:
        """銘柄並列数を自動決定する。"""
        return resolve_screening_stock_workers(stock_count)

    def _resolve_parallel_workers(
        self,
        work_count: int,
        env_names: tuple[str, ...],
    ) -> int:
        return resolve_screening_parallel_workers(
            work_count=work_count,
            env_names=env_names,
        )

    def _emit_progress(
        self,
        progress_callback: Callable[[int, int], None] | None,
        completed: int,
        total: int,
    ) -> None:
        emit_screening_progress(progress_callback, completed, total)

    def _log_stage_timing(
        self,
        stage: str,
        started_at: float,
        **extra: Any,
    ) -> None:
        log_screening_stage_timing(stage, started_at, **extra)

    def _resolve_date_range(
        self,
        shared_config: SharedConfig,
        reference_date: str | None,
        recent_days: int,
    ) -> tuple[str | None, str | None]:
        """shared_config とクエリ日付からロード対象期間を解決する。"""
        return resolve_screening_date_range(
            shared_config=shared_config,
            reference_date=reference_date,
            recent_days=recent_days,
            get_latest_market_date=self._get_latest_market_date,
            resolve_history_trading_days=self._resolve_history_trading_days,
            get_trading_date_before=self._get_trading_date_before,
        )

    def _resolve_history_trading_days(self, recent_days: int) -> int:
        """screening 読み込み対象の営業日本数を決定する。"""
        return resolve_screening_history_trading_days(
            recent_days,
            default_days=self._DEFAULT_HISTORY_TRADING_DAYS,
        )

    def _get_latest_market_date(self) -> str | None:
        return get_screening_latest_market_date(self._reader)

    def _table_exists(self, table_name: str) -> bool:
        return screening_table_exists(self._reader, table_name)

    def _stock_master_daily_has_date(self, as_of_date: str) -> bool:
        return screening_stock_master_daily_has_date(
            self._reader,
            as_of_date,
            table_exists=self._table_exists,
        )

    def _get_trading_date_before(self, date: str, offset: int) -> str | None:
        return get_screening_trading_date_before(self._reader, date, offset)

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
        return dedupe_screening_warnings(warnings, limit=self._WARNING_LIMIT)
