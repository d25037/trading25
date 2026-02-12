"""
Signal attribution analysis for YAML-driven strategies.

This module computes per-signal contribution to total_return and sharpe_ratio
using:
- LOO (leave-one-out) ablation across all enabled signals
- Shapley values on top-N impactful signals
"""

from __future__ import annotations

import copy
import itertools
import math
import random
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

import pandas as pd
from loguru import logger

from src.lib.backtest_core.runner import BacktestRunner
from src.models.config import SharedConfig
from src.models.signals import SignalParams
from src.strategies.core.yaml_configurable_strategy import YamlConfigurableStrategy
from src.strategies.signals.registry import SIGNAL_REGISTRY, SignalDefinition

ProgressCallback = Callable[[str, float], None]

_EXACT_SHAPLEY_THRESHOLD = 8


class SignalAttributionCancelled(Exception):
    """Raised when signal attribution is cooperatively cancelled."""


@dataclass(frozen=True)
class AttributionMetrics:
    """Backtest metrics used by attribution logic."""

    total_return: float
    sharpe_ratio: float


@dataclass(frozen=True)
class SignalTarget:
    """A concrete signal instance with scope (entry / exit)."""

    signal_id: str
    scope: str
    param_key: str
    signal_name: str
    definition: SignalDefinition


@dataclass
class StrategyRuntimeCache:
    """Reusable loaded data from baseline run to avoid repeated IO/API calls."""

    multi_data_dict: dict[str, dict[str, pd.DataFrame]] | None = None
    benchmark_data: pd.DataFrame | None = None
    relative_data_dict: dict[str, dict[str, pd.DataFrame]] | None = None
    execution_data_dict: dict[str, dict[str, pd.DataFrame]] | None = None

    @classmethod
    def from_strategy(cls, strategy: YamlConfigurableStrategy) -> StrategyRuntimeCache:
        return cls(
            multi_data_dict=strategy.multi_data_dict,
            benchmark_data=strategy.benchmark_data,
            relative_data_dict=strategy.relative_data_dict,
            execution_data_dict=strategy.execution_data_dict,
        )

    def apply_to_strategy(self, strategy: YamlConfigurableStrategy) -> None:
        strategy.multi_data_dict = self.multi_data_dict
        strategy.benchmark_data = self.benchmark_data
        strategy.relative_data_dict = self.relative_data_dict
        strategy.execution_data_dict = self.execution_data_dict


def _safe_metric(value: Any) -> float:
    """Convert vectorbt metric outputs to finite float."""
    try:
        if hasattr(value, "mean"):
            value = value.mean()
        v = float(value)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return 0.0


def _clone_parameters(parameters: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(parameters)


def _disable_signal_in_parameters(
    parameters: dict[str, Any],
    scope: str,
    param_key: str,
) -> None:
    """Disable a concrete signal in parameter dict in-place."""
    section_name = (
        "entry_filter_params" if scope == "entry" else "exit_trigger_params"
    )
    section = parameters.setdefault(section_name, {})
    if not isinstance(section, dict):
        section = {}
        parameters[section_name] = section

    parts = param_key.split(".")
    if len(parts) == 1:
        key = parts[0]
        existing = section.get(key, {})
        if not isinstance(existing, dict):
            existing = {}
        existing["enabled"] = False
        section[key] = existing
        return

    root = parts[0]
    root_obj = section.get(root, {})
    if not isinstance(root_obj, dict):
        root_obj = {}
    cursor = root_obj
    for part in parts[1:-1]:
        nxt = cursor.get(part, {})
        if not isinstance(nxt, dict):
            nxt = {}
        cursor[part] = nxt
        cursor = nxt

    leaf_key = parts[-1]
    leaf_obj = cursor.get(leaf_key, {})
    if not isinstance(leaf_obj, dict):
        leaf_obj = {}
    leaf_obj["enabled"] = False
    cursor[leaf_key] = leaf_obj
    section[root] = root_obj


def _build_signal_params(parameters: dict[str, Any]) -> tuple[SignalParams, SignalParams]:
    entry = SignalParams(**parameters.get("entry_filter_params", {}))
    exit_ = SignalParams(**parameters.get("exit_trigger_params", {}))
    return entry, exit_


def _iter_enabled_signals(
    entry_signal_params: SignalParams,
    exit_signal_params: SignalParams,
) -> list[SignalTarget]:
    """List all enabled signals by scope based on current signal registry."""
    targets: list[SignalTarget] = []

    for signal_def in SIGNAL_REGISTRY:
        if signal_def.enabled_checker(entry_signal_params):
            targets.append(
                SignalTarget(
                    signal_id=f"entry.{signal_def.param_key}",
                    scope="entry",
                    param_key=signal_def.param_key,
                    signal_name=signal_def.name,
                    definition=signal_def,
                )
            )

        if (
            not signal_def.exit_disabled
            and signal_def.enabled_checker(exit_signal_params)
        ):
            targets.append(
                SignalTarget(
                    signal_id=f"exit.{signal_def.param_key}",
                    scope="exit",
                    param_key=signal_def.param_key,
                    signal_name=signal_def.name,
                    definition=signal_def,
                )
            )

    return targets


def _create_strategy_from_parameters(
    parameters: dict[str, Any],
) -> tuple[YamlConfigurableStrategy, SharedConfig]:
    shared_config = SharedConfig(**parameters.get("shared_config", {}))
    entry_signal_params, exit_signal_params = _build_signal_params(parameters)
    strategy = YamlConfigurableStrategy(
        shared_config=shared_config,
        entry_filter_params=entry_signal_params,
        exit_trigger_params=exit_signal_params,
    )
    return strategy, shared_config


def _evaluate_parameters(
    parameters: dict[str, Any],
    runtime_cache: StrategyRuntimeCache | None = None,
) -> tuple[AttributionMetrics, StrategyRuntimeCache]:
    strategy, shared_config = _create_strategy_from_parameters(parameters)
    if runtime_cache is not None:
        runtime_cache.apply_to_strategy(strategy)

    (
        _initial_portfolio,
        kelly_portfolio,
        _optimized_allocation,
        _stats,
        _all_entries,
    ) = strategy.run_optimized_backtest_kelly(
        kelly_fraction=shared_config.kelly_fraction,
        min_allocation=shared_config.min_allocation,
        max_allocation=shared_config.max_allocation,
    )
    portfolio_any: Any = kelly_portfolio
    metrics = AttributionMetrics(
        total_return=_safe_metric(portfolio_any.total_return()),
        sharpe_ratio=_safe_metric(portfolio_any.sharpe_ratio()),
    )
    return metrics, StrategyRuntimeCache.from_strategy(strategy)


def _loo_composite_score(
    delta_total_return: float,
    delta_sharpe_ratio: float,
    max_abs_return: float,
    max_abs_sharpe: float,
) -> float:
    ret_part = abs(delta_total_return) / max_abs_return if max_abs_return > 0 else 0.0
    sharpe_part = abs(delta_sharpe_ratio) / max_abs_sharpe if max_abs_sharpe > 0 else 0.0
    return 0.5 * ret_part + 0.5 * sharpe_part


def _subsets(players: list[str]) -> Iterable[frozenset[str]]:
    for r in range(len(players) + 1):
        for combo in itertools.combinations(players, r):
            yield frozenset(combo)


class SignalAttributionAnalyzer:
    """Compute LOO + Shapley top-N signal attribution for one strategy."""

    def __init__(
        self,
        strategy_name: str,
        config_override: dict[str, Any] | None = None,
        shapley_top_n: int = 5,
        shapley_permutations: int = 128,
        random_seed: int | None = None,
        *,
        parameters_hook: Callable[[], dict[str, Any]] | None = None,
        evaluate_hook: Callable[
            [dict[str, Any], StrategyRuntimeCache | None],
            tuple[AttributionMetrics, StrategyRuntimeCache],
        ]
        | None = None,
        cancel_check: Callable[[], bool] | None = None,
    ) -> None:
        self.strategy_name = strategy_name
        self.config_override = config_override
        self.shapley_top_n = max(1, shapley_top_n)
        self.shapley_permutations = max(1, shapley_permutations)
        self.random_seed = random_seed
        self._parameters_hook = parameters_hook
        self._evaluate_hook = evaluate_hook
        self._cancel_check = cancel_check
        self._runner = BacktestRunner()

    def _raise_if_cancelled(self) -> None:
        if self._cancel_check is not None and self._cancel_check():
            raise SignalAttributionCancelled("Signal attribution cancelled")

    def _load_parameters(self) -> dict[str, Any]:
        if self._parameters_hook is not None:
            return _clone_parameters(self._parameters_hook())

        parameters = self._runner.build_parameters_for_strategy(
            strategy=self.strategy_name,
            config_override=self.config_override,
        )
        return _clone_parameters(parameters)

    def _evaluate(
        self,
        parameters: dict[str, Any],
        runtime_cache: StrategyRuntimeCache | None = None,
    ) -> tuple[AttributionMetrics, StrategyRuntimeCache]:
        self._raise_if_cancelled()
        if self._evaluate_hook is not None:
            metrics, cache = self._evaluate_hook(parameters, runtime_cache)
        else:
            metrics, cache = _evaluate_parameters(parameters, runtime_cache)
        self._raise_if_cancelled()
        return metrics, cache

    def run(
        self,
        progress_callback: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        def notify(message: str, progress: float) -> None:
            if progress_callback is not None:
                bounded = max(0.0, min(1.0, progress))
                progress_callback(message, bounded)

        started = time.time()
        parameters = self._load_parameters()
        self._raise_if_cancelled()

        entry_params, exit_params = _build_signal_params(parameters)
        enabled_signals = _iter_enabled_signals(entry_params, exit_params)

        baseline_started = time.time()
        self._raise_if_cancelled()
        notify("Signal attribution: baseline 実行中...", 0.05)
        baseline_metrics, runtime_cache = self._evaluate(parameters, runtime_cache=None)
        baseline_seconds = time.time() - baseline_started
        notify("Signal attribution: baseline 完了", 0.2)

        loo_started = time.time()
        loo_records: dict[str, dict[str, Any]] = {}
        successful_loo: list[tuple[SignalTarget, float, float]] = []
        total_loo = len(enabled_signals)

        for index, signal in enumerate(enabled_signals, start=1):
            self._raise_if_cancelled()
            variant_parameters = _clone_parameters(parameters)
            _disable_signal_in_parameters(
                variant_parameters,
                scope=signal.scope,
                param_key=signal.param_key,
            )

            try:
                variant_metrics, _ = self._evaluate(variant_parameters, runtime_cache)
                delta_return = baseline_metrics.total_return - variant_metrics.total_return
                delta_sharpe = baseline_metrics.sharpe_ratio - variant_metrics.sharpe_ratio
                loo_records[signal.signal_id] = {
                    "status": "ok",
                    "variant_metrics": {
                        "total_return": variant_metrics.total_return,
                        "sharpe_ratio": variant_metrics.sharpe_ratio,
                    },
                    "delta_total_return": delta_return,
                    "delta_sharpe_ratio": delta_sharpe,
                    "error": None,
                }
                successful_loo.append((signal, delta_return, delta_sharpe))
            except SignalAttributionCancelled:
                raise
            except Exception as e:
                logger.warning(f"LOO evaluation failed for {signal.signal_id}: {e}")
                loo_records[signal.signal_id] = {
                    "status": "error",
                    "variant_metrics": None,
                    "delta_total_return": None,
                    "delta_sharpe_ratio": None,
                    "error": str(e),
                }

            loo_progress = 0.2 + (0.6 * index / total_loo) if total_loo > 0 else 0.8
            notify(
                f"Signal attribution: LOO {index}/{total_loo}",
                loo_progress,
            )

        loo_seconds = time.time() - loo_started

        max_abs_return = max((abs(x[1]) for x in successful_loo), default=0.0)
        max_abs_sharpe = max((abs(x[2]) for x in successful_loo), default=0.0)

        scored_signals: list[tuple[SignalTarget, float]] = []
        for signal, delta_return, delta_sharpe in successful_loo:
            score = _loo_composite_score(
                delta_return,
                delta_sharpe,
                max_abs_return,
                max_abs_sharpe,
            )
            scored_signals.append((signal, score))

        scored_signals.sort(key=lambda item: item[1], reverse=True)
        top_n_effective = min(self.shapley_top_n, len(scored_signals))
        selected_for_shapley = [item[0] for item in scored_signals[:top_n_effective]]
        selected_ids = [signal.signal_id for signal in selected_for_shapley]

        shapley_started = time.time()
        self._raise_if_cancelled()
        shapley_values: dict[str, dict[str, Any]] = {}
        shapley_meta: dict[str, Any] = {"method": None, "sample_size": None, "error": None}

        if selected_for_shapley:
            try:
                shapley_values, shapley_meta = self._compute_shapley(
                    baseline_parameters=parameters,
                    selected_signals=selected_for_shapley,
                    runtime_cache=runtime_cache,
                    progress_callback=notify,
                )
            except SignalAttributionCancelled:
                raise
            except Exception as e:
                logger.warning(f"Shapley evaluation failed: {e}")
                shapley_meta = {
                    "method": "error",
                    "sample_size": None,
                    "error": str(e),
                }
                for signal_id in selected_ids:
                    shapley_values[signal_id] = {
                        "status": "error",
                        "total_return": None,
                        "sharpe_ratio": None,
                        "method": "error",
                        "sample_size": None,
                        "error": str(e),
                    }

        shapley_seconds = time.time() - shapley_started
        self._raise_if_cancelled()
        notify("Signal attribution: 完了", 1.0)

        signal_results: list[dict[str, Any]] = []
        for signal in enabled_signals:
            loo_data = loo_records.get(signal.signal_id, {
                "status": "error",
                "variant_metrics": None,
                "delta_total_return": None,
                "delta_sharpe_ratio": None,
                "error": "LOO result not found",
            })
            shapley_data = shapley_values.get(signal.signal_id)
            signal_results.append(
                {
                    "signal_id": signal.signal_id,
                    "scope": signal.scope,
                    "param_key": signal.param_key,
                    "signal_name": signal.signal_name,
                    "loo": loo_data,
                    "shapley": shapley_data,
                }
            )

        total_seconds = time.time() - started

        return {
            "baseline_metrics": {
                "total_return": baseline_metrics.total_return,
                "sharpe_ratio": baseline_metrics.sharpe_ratio,
            },
            "signals": signal_results,
            "top_n_selection": {
                "top_n_requested": self.shapley_top_n,
                "top_n_effective": top_n_effective,
                "selected_signal_ids": selected_ids,
                "scores": [
                    {"signal_id": signal.signal_id, "score": score}
                    for signal, score in scored_signals[:top_n_effective]
                ],
            },
            "timing": {
                "total_seconds": total_seconds,
                "baseline_seconds": baseline_seconds,
                "loo_seconds": loo_seconds,
                "shapley_seconds": shapley_seconds,
            },
            "shapley": shapley_meta,
        }

    def _compute_shapley(
        self,
        baseline_parameters: dict[str, Any],
        selected_signals: list[SignalTarget],
        runtime_cache: StrategyRuntimeCache,
        progress_callback: ProgressCallback | None = None,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        self._raise_if_cancelled()
        signal_by_id = {signal.signal_id: signal for signal in selected_signals}
        players = list(signal_by_id.keys())
        n_players = len(players)
        values_cache: dict[frozenset[str], AttributionMetrics] = {}

        def notify(message: str, progress: float) -> None:
            if progress_callback is not None:
                progress_callback(message, progress)

        def evaluate_subset(enabled_player_ids: frozenset[str]) -> AttributionMetrics:
            self._raise_if_cancelled()
            if enabled_player_ids in values_cache:
                return values_cache[enabled_player_ids]

            subset_parameters = _clone_parameters(baseline_parameters)
            enabled_set = set(enabled_player_ids)
            for player in players:
                if player in enabled_set:
                    continue
                signal = signal_by_id[player]
                _disable_signal_in_parameters(
                    subset_parameters,
                    scope=signal.scope,
                    param_key=signal.param_key,
                )

            metrics, _ = self._evaluate(subset_parameters, runtime_cache=runtime_cache)
            self._raise_if_cancelled()
            values_cache[enabled_player_ids] = metrics
            return metrics

        if n_players <= _EXACT_SHAPLEY_THRESHOLD:
            method = "exact"
            sample_size = 2**n_players
            all_subsets = list(_subsets(players))
            for idx, subset in enumerate(all_subsets, start=1):
                self._raise_if_cancelled()
                evaluate_subset(subset)
                progress = 0.8 + (0.2 * idx / max(1, len(all_subsets)))
                notify(
                    f"Signal attribution: Shapley exact {idx}/{len(all_subsets)}",
                    progress,
                )

            factorial_n = math.factorial(n_players)
            totals: dict[str, AttributionMetrics] = {}
            for player in players:
                contribution_return = 0.0
                contribution_sharpe = 0.0
                others = [p for p in players if p != player]
                for subset in _subsets(others):
                    subset_with_player = frozenset(set(subset) | {player})
                    weight = (
                        math.factorial(len(subset))
                        * math.factorial(n_players - len(subset) - 1)
                        / factorial_n
                    )
                    v_subset = values_cache[subset]
                    v_with_player = values_cache[subset_with_player]
                    contribution_return += weight * (
                        v_with_player.total_return - v_subset.total_return
                    )
                    contribution_sharpe += weight * (
                        v_with_player.sharpe_ratio - v_subset.sharpe_ratio
                    )

                totals[player] = AttributionMetrics(
                    total_return=contribution_return,
                    sharpe_ratio=contribution_sharpe,
                )
        else:
            method = "permutation"
            sample_size = self.shapley_permutations
            rnd = random.Random(self.random_seed)
            totals = {
                player: AttributionMetrics(total_return=0.0, sharpe_ratio=0.0)
                for player in players
            }
            counts = {player: 0 for player in players}

            for idx in range(sample_size):
                self._raise_if_cancelled()
                perm = players[:]
                rnd.shuffle(perm)

                current_set = frozenset()
                current_metrics = evaluate_subset(current_set)
                for player in perm:
                    self._raise_if_cancelled()
                    next_set = frozenset(set(current_set) | {player})
                    next_metrics = evaluate_subset(next_set)
                    prev_total = totals[player]
                    totals[player] = AttributionMetrics(
                        total_return=prev_total.total_return
                        + (next_metrics.total_return - current_metrics.total_return),
                        sharpe_ratio=prev_total.sharpe_ratio
                        + (next_metrics.sharpe_ratio - current_metrics.sharpe_ratio),
                    )
                    counts[player] += 1
                    current_set = next_set
                    current_metrics = next_metrics

                progress = 0.8 + (0.2 * (idx + 1) / sample_size)
                notify(
                    f"Signal attribution: Shapley permutation {idx + 1}/{sample_size}",
                    progress,
                )

            for player in players:
                player_count = max(1, counts[player])
                totals[player] = AttributionMetrics(
                    total_return=totals[player].total_return / player_count,
                    sharpe_ratio=totals[player].sharpe_ratio / player_count,
                )

        result_map = {
            player: {
                "status": "ok",
                "total_return": totals[player].total_return,
                "sharpe_ratio": totals[player].sharpe_ratio,
                "method": method,
                "sample_size": sample_size,
                "error": None,
            }
            for player in players
        }
        meta = {
            "method": method,
            "sample_size": sample_size,
            "error": None,
            "evaluations": len(values_cache),
        }
        return result_map, meta


__all__ = [
    "AttributionMetrics",
    "SignalAttributionCancelled",
    "SignalAttributionAnalyzer",
    "SignalTarget",
    "StrategyRuntimeCache",
]
