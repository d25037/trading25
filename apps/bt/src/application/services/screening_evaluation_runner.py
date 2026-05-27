"""Stock-major evaluation runner for market screening."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger

from src.application.services.screening_execution import (
    StockUniverseItem,
    StrategyEvaluationAccumulator,
    StrategyEvaluationResult,
    StrategyExecutionInput,
    StrategyRuntime,
)
from src.domains.analytics.screening_evaluator import StockEvaluationOutcome

EvaluateStockFn = Callable[
    [StockUniverseItem, list[StrategyExecutionInput], int, dict[str, str]],
    StockEvaluationOutcome,
]
BuildStrategyCacheTokenFn = Callable[[StrategyRuntime], str]
ApplyStockOutcomeFn = Callable[[StockEvaluationOutcome, dict[str, StrategyEvaluationAccumulator]], None]
ResolveStockWorkersFn = Callable[[int], int]
EmitProgressFn = Callable[[Callable[[int, int], None] | None, int, int], None]


def run_stock_major_evaluation(
    *,
    strategy_inputs: list[StrategyExecutionInput],
    stock_universe: list[StockUniverseItem],
    recent_days: int,
    progress_callback: Callable[[int, int], None] | None,
    build_strategy_signal_cache_token: BuildStrategyCacheTokenFn,
    evaluate_stock: EvaluateStockFn,
    apply_stock_outcome: ApplyStockOutcomeFn,
    resolve_stock_workers: ResolveStockWorkersFn,
    emit_progress: EmitProgressFn,
) -> tuple[list[StrategyEvaluationResult], list[str], int]:
    """銘柄主導(stock-major)で戦略評価を実行する。"""
    if not strategy_inputs:
        emit_progress(progress_callback, 0, 0)
        return [], [], 1

    accumulators: dict[str, StrategyEvaluationAccumulator] = {}
    strategy_cache_tokens: dict[str, str] = {}
    for strategy_input in strategy_inputs:
        strategy_name = strategy_input.strategy.response_name
        accumulators[strategy_name] = StrategyEvaluationAccumulator(
            strategy=strategy_input.strategy,
            warnings=list(strategy_input.load_warnings),
        )
        strategy_cache_tokens[strategy_name] = build_strategy_signal_cache_token(
            strategy_input.strategy
        )

    total_stocks = len(stock_universe)
    if total_stocks == 0:
        emit_progress(progress_callback, 0, 0)
        ordered_results = build_ordered_strategy_results(
            strategy_inputs=strategy_inputs,
            accumulators=accumulators,
        )
        return ordered_results, [], 1

    emit_progress(progress_callback, 0, total_stocks)

    worker_count = resolve_stock_workers(total_stocks)
    warnings: list[str] = []
    completed = 0

    if worker_count == 1:
        for stock in stock_universe:
            try:
                outcome = evaluate_stock(
                    stock,
                    strategy_inputs,
                    recent_days,
                    strategy_cache_tokens,
                )
            except Exception as exc:
                logger.exception(
                    "Stock screening failed",
                    stock_code=stock.code,
                )
                warnings.append(f"{stock.code}: evaluation failed ({exc})")
            else:
                apply_stock_outcome(outcome, accumulators)
            finally:
                completed += 1
                emit_progress(progress_callback, completed, total_stocks)
    else:
        outcomes_by_code: dict[str, StockEvaluationOutcome] = {}
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_stock = {
                executor.submit(
                    evaluate_stock,
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
                    emit_progress(progress_callback, completed, total_stocks)

        for stock in stock_universe:
            outcome = outcomes_by_code.get(stock.code)
            if outcome is None:
                continue
            apply_stock_outcome(outcome, accumulators)

    ordered_results = build_ordered_strategy_results(
        strategy_inputs=strategy_inputs,
        accumulators=accumulators,
    )

    return ordered_results, warnings, worker_count


def build_ordered_strategy_results(
    *,
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
