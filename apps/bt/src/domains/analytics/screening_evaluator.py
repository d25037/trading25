"""
Screening evaluator domain logic.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

import pandas as pd

from src.shared.models.signals import SignalParams, Signals

if TYPE_CHECKING:
    from src.domains.strategy.runtime.compiler import CompiledStrategyIR


class StockLike(Protocol):
    @property
    def code(self) -> str:
        ...


class StrategyLike(Protocol):
    @property
    def response_name(self) -> str:
        ...

    @property
    def entry_params(self) -> SignalParams:
        ...

    @property
    def exit_params(self) -> SignalParams:
        ...

    @property
    def entry_decidability(self) -> str:
        ...

    @property
    def compiled_strategy(self) -> "CompiledStrategyIR":
        ...


class StrategyDataBundleLike(Protocol):
    @property
    def multi_data(self) -> Mapping[str, Mapping[str, Any]]:
        ...

    @property
    def benchmark_data(self) -> pd.DataFrame | None:
        ...

    @property
    def sector_data(self) -> Mapping[str, pd.DataFrame] | None:
        ...

    @property
    def stock_sector_mapping(self) -> Mapping[str, str]:
        ...


class StrategyExecutionInputLike(Protocol):
    @property
    def strategy(self) -> StrategyLike:
        ...

    @property
    def data_bundle(self) -> StrategyDataBundleLike:
        ...

    @property
    def load_warnings(self) -> Sequence[str]:
        ...


class StrategyEvaluationAccumulatorLike(Protocol):
    @property
    def matched_rows(self) -> list[tuple[Any, str]]:
        ...

    @property
    def processed_codes(self) -> set[str]:
        ...

    @property
    def warnings(self) -> list[str]:
        ...


@dataclass(frozen=True)
class StockEvaluationOutcome:
    stock: StockLike
    matched_dates_by_strategy: dict[str, str]
    processed_strategy_names: set[str]
    warning_by_strategy: list[tuple[str, str]]


@dataclass(frozen=True)
class StrategyEvaluationSummary:
    matched_rows: list[tuple[Any, str]]
    processed_codes: set[str]
    warnings: list[str]


GenerateSignalsFn = Callable[..., Signals]
FindRecentMatchDateFn = Callable[[Signals, int], str | None]
BuildStrategySignalCacheTokenFn = Callable[[Any], str]
BuildPerStockSignalCacheKeyFn = Callable[
    [
        str,
        pd.DataFrame,
        Any,
        Any,
        Any,
        str,
        int,
    ],
    tuple[Any, ...],
]


def _resolve_daily_payload(
    stock_data: Mapping[str, Any],
) -> tuple[pd.DataFrame | None, Any, Any]:
    daily = stock_data.get("daily")
    if not isinstance(daily, pd.DataFrame) or daily.empty:
        return None, None, None
    return daily, stock_data.get("margin_daily"), stock_data.get("statements_daily")


def _run_generate_signals(
    *,
    generate_signals: GenerateSignalsFn,
    daily: pd.DataFrame,
    strategy: StrategyLike,
    data_bundle: StrategyDataBundleLike,
    stock_code: str,
    margin_data: Any,
    statements_data: Any,
    recent_days: int,
    ) -> Signals:
    return generate_signals(
        strategy_entries=pd.Series(True, index=daily.index),
        strategy_exits=pd.Series(False, index=daily.index),
        ohlc_data=daily,
        entry_signal_params=strategy.entry_params,
        exit_signal_params=strategy.exit_params,
        margin_data=margin_data,
        statements_data=statements_data,
        benchmark_data=data_bundle.benchmark_data,
        sector_data=data_bundle.sector_data,
        stock_sector_name=data_bundle.stock_sector_mapping.get(stock_code),
        stock_code=stock_code,
        universe_multi_data=data_bundle.multi_data,
        universe_member_codes=tuple(data_bundle.multi_data.keys()),
        screening_recent_days=recent_days,
        compiled_strategy=strategy.compiled_strategy,
        skip_exit_when_no_recent_entry=True,
    )


def build_strategy_signal_cache_token(strategy: StrategyLike) -> str:
    payload = {
        "entry": strategy.entry_params.model_dump(mode="json"),
        "exit": strategy.exit_params.model_dump(mode="json"),
        "entry_decidability": strategy.entry_decidability,
        "compiled_execution_semantics": strategy.compiled_strategy.execution_semantics,
        "compiled_signal_ids": strategy.compiled_strategy.signal_ids,
    }
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def build_per_stock_signal_cache_key(
    strategy_cache_token: str,
    daily: pd.DataFrame,
    margin_data: Any,
    statements_data: Any,
    data_bundle: StrategyDataBundleLike,
    stock_code: str,
    recent_days: int,
) -> tuple[Any, ...]:
    return (
        strategy_cache_token,
        id(daily),
        id(margin_data),
        id(statements_data),
        id(data_bundle.multi_data),
        id(data_bundle.benchmark_data),
        id(data_bundle.sector_data),
        data_bundle.stock_sector_mapping.get(stock_code),
        recent_days,
    )


def evaluate_stock(
    *,
    stock: StockLike,
    strategy_inputs: Sequence[StrategyExecutionInputLike],
    recent_days: int,
    strategy_cache_tokens: Mapping[str, str],
    generate_signals: GenerateSignalsFn,
    find_recent_match_date: FindRecentMatchDateFn,
    build_strategy_signal_cache_token_fn: BuildStrategySignalCacheTokenFn = build_strategy_signal_cache_token,
    build_per_stock_signal_cache_key_fn: BuildPerStockSignalCacheKeyFn = build_per_stock_signal_cache_key,
) -> StockEvaluationOutcome:
    matched_dates_by_strategy: dict[str, str] = {}
    processed_strategy_names: set[str] = set()
    warning_by_strategy: list[tuple[str, str]] = []
    signal_cache: dict[tuple[Any, ...], Signals] = {}

    for strategy_input in strategy_inputs:
        strategy = strategy_input.strategy
        strategy_name = strategy.response_name
        data_bundle = strategy_input.data_bundle
        try:
            stock_data = data_bundle.multi_data.get(stock.code)
            if not stock_data:
                continue

            daily, margin_data, statements_data = _resolve_daily_payload(stock_data)
            if daily is None:
                continue

            processed_strategy_names.add(strategy_name)

            cache_token = strategy_cache_tokens.get(strategy_name)
            if cache_token is None:
                cache_token = build_strategy_signal_cache_token_fn(strategy)

            cache_key = build_per_stock_signal_cache_key_fn(
                cache_token,
                daily,
                margin_data,
                statements_data,
                data_bundle,
                stock.code,
                recent_days,
            )

            signals = signal_cache.get(cache_key)
            if signals is None:
                try:
                    signals = _run_generate_signals(
                        generate_signals=generate_signals,
                        daily=daily,
                        strategy=strategy,
                        data_bundle=data_bundle,
                        stock_code=stock.code,
                        margin_data=margin_data,
                        statements_data=statements_data,
                        recent_days=recent_days,
                    )
                except Exception as exc:
                    warning_by_strategy.append(
                        (strategy_name, f"{stock.code} signal generation failed ({exc})")
                    )
                    continue
                signal_cache[cache_key] = signals

            matched_date = find_recent_match_date(signals, recent_days)
            if matched_date is not None:
                matched_dates_by_strategy[strategy_name] = matched_date
        except Exception as exc:
            warning_by_strategy.append((strategy_name, f"{stock.code} evaluation failed ({exc})"))
            continue

    return StockEvaluationOutcome(
        stock=stock,
        matched_dates_by_strategy=matched_dates_by_strategy,
        processed_strategy_names=processed_strategy_names,
        warning_by_strategy=warning_by_strategy,
    )


def apply_stock_outcome(
    outcome: StockEvaluationOutcome,
    accumulators: Mapping[str, StrategyEvaluationAccumulatorLike],
) -> None:
    stock_code = outcome.stock.code
    for strategy_name in outcome.processed_strategy_names:
        accumulator = accumulators.get(strategy_name)
        if accumulator is None:
            continue
        accumulator.processed_codes.add(stock_code)

    for strategy_name, matched_date in outcome.matched_dates_by_strategy.items():
        accumulator = accumulators.get(strategy_name)
        if accumulator is None:
            continue
        accumulator.matched_rows.append((outcome.stock, matched_date))

    for strategy_name, warning in outcome.warning_by_strategy:
        accumulator = accumulators.get(strategy_name)
        if accumulator is None:
            continue
        accumulator.warnings.append(warning)


def evaluate_strategy(
    *,
    strategy: StrategyLike,
    stock_universe: Sequence[StockLike],
    recent_days: int,
    data_bundle: StrategyDataBundleLike,
    generate_signals: GenerateSignalsFn,
    find_recent_match_date: FindRecentMatchDateFn,
) -> StrategyEvaluationSummary:
    if not stock_universe:
        return StrategyEvaluationSummary(
            matched_rows=[],
            processed_codes=set(),
            warnings=[],
        )

    matches: list[tuple[StockLike, str]] = []
    processed: set[str] = set()
    warnings: list[str] = []

    for stock in stock_universe:
        stock_data = data_bundle.multi_data.get(stock.code)
        if not stock_data:
            continue

        daily, margin_data, statements_data = _resolve_daily_payload(stock_data)
        if daily is None:
            continue

        processed.add(stock.code)

        try:
            signals = _run_generate_signals(
                generate_signals=generate_signals,
                daily=daily,
                strategy=strategy,
                data_bundle=data_bundle,
                stock_code=stock.code,
                margin_data=margin_data,
                statements_data=statements_data,
                recent_days=recent_days,
            )
        except Exception as exc:
            warnings.append(f"{stock.code} signal generation failed ({exc})")
            continue

        matched_date = find_recent_match_date(signals, recent_days)
        if matched_date is None:
            continue
        matches.append((stock, matched_date))

    return StrategyEvaluationSummary(
        matched_rows=matches,
        processed_codes=processed,
        warnings=warnings,
    )


def evaluate_strategy_input(
    *,
    strategy_input: StrategyExecutionInputLike,
    stock_universe: Sequence[StockLike],
    recent_days: int,
    generate_signals: GenerateSignalsFn,
    find_recent_match_date: FindRecentMatchDateFn,
) -> StrategyEvaluationSummary:
    summary = evaluate_strategy(
        strategy=strategy_input.strategy,
        stock_universe=stock_universe,
        recent_days=recent_days,
        data_bundle=strategy_input.data_bundle,
        generate_signals=generate_signals,
        find_recent_match_date=find_recent_match_date,
    )
    return StrategyEvaluationSummary(
        matched_rows=summary.matched_rows,
        processed_codes=summary.processed_codes,
        warnings=[*strategy_input.load_warnings, *summary.warnings],
    )
