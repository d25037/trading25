"""Pure score-ring membership and same-Close position-state semantics."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
import math
from pathlib import Path
from types import MappingProxyType
from typing import Any, TypedDict, cast

import numpy as np
import pandas as pd

from src.domains.analytics.daily_ranking_consumer_support import (
    compose_daily_ranking_signal_features,
)
from src.domains.analytics.daily_ranking_feature_builders import (
    AtrFeaturesRequest,
    LongLeadershipFeaturesRequest,
    LongScaffoldFeaturesRequest,
    SectorStrengthFeaturesRequest,
    ShortScaffoldFeaturesRequest,
    SmaFeaturesRequest,
    build_atr_features,
    build_long_leadership_features,
    build_long_scaffold_features,
    build_sector_strength_features,
    build_short_scaffold_features,
    build_sma_features,
)
from src.domains.analytics.daily_ranking_research_base import (
    DailyRankingPanelRequest,
    DailyRankingResearchRelations,
    build_daily_ranking_research_base,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    open_readonly_analysis_connection,
    require_market_v5_compatibility,
)
from src.domains.backtest.vectorbt_adapter import VectorbtAdapter
from src.shared.utils.pandas_type_guards import finite_float_or_none


SCORE_RING_THRESHOLDS: Mapping[str, float] = MappingProxyType(
    {
        "core_high_high": 0.80,
        "near_high_high_1": 0.70,
        "near_high_high_2": 0.60,
    }
)
ENTRY_RULE_IDS = (
    "E0_no_sma5_filter",
    "E1_close_above_sma5",
    "E2_count_ge_2",
    "E3_avoid_atr20_chase",
    "E4_count_ge_2_and_avoid_chase",
)
EXIT_RULE_IDS = (
    "X0_no_sma5_exit",
    "X1_close_below_sma5",
    "X2_count_le_1",
    "X3_below_streak_ge_3",
    "X4_atr20_below_le_neg1",
)
EXIT_PRECEDENCE = ("ring_exit", "sma5_exit", "time_exit", "terminal_exit")

_VALUE_SCORE_COLUMN = "value_composite_equal_score"
_LEADERSHIP_SCORE_COLUMN = "long_hybrid_leadership_score"
_REQUIRED_FEATURE_COLUMNS = frozenset(
    {
        "date",
        "code",
        "close",
        "topix_close",
        _VALUE_SCORE_COLUMN,
        _LEADERSHIP_SCORE_COLUMN,
    }
)
_REQUIRED_MARKET_TABLES = frozenset(
    {
        "stock_data_raw",
        "stock_data",
        "topix_data",
        "daily_valuation",
        "stock_master_daily",
        "indices_data",
        "index_master",
        "stock_provider_windows",
        "stock_adjustment_events",
        "current_basis_fundamentals_state",
        "current_basis_recompute_pending",
        "statements",
        "statement_metrics_adjusted",
    }
)
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000


@dataclass(frozen=True)
class PositionSignalFrames:
    close: pd.DataFrame
    entries: pd.DataFrame
    exits: pd.DataFrame
    held_intervals: pd.DataFrame
    state_events: pd.DataFrame


@dataclass(frozen=True)
class ResearchVariant:
    """One frozen score-ring and SMA5 position-state configuration."""

    ring_id: str
    entry_rule_id: str
    exit_rule_id: str
    max_holding_sessions: int
    name: str | None = None


@dataclass(frozen=True)
class BootstrapInterval:
    """Deterministic paired moving-block bootstrap summary."""

    estimate: float
    lower: float
    upper: float
    p_value: float
    observations: int
    block_length: int
    resamples: int
    seed: int


@dataclass(frozen=True)
class VariantExecution:
    """Compact VectorBT-derived execution evidence for one frozen variant."""

    variant: ResearchVariant
    fee_bps: float
    trade_records_df: pd.DataFrame
    daily_portfolio_returns: pd.Series
    benchmark_daily_returns: pd.Series
    state_events: pd.DataFrame

    @property
    def trade_records(self) -> pd.DataFrame:
        """Compatibility-friendly name for the normalized VectorBT ledger."""

        return self.trade_records_df

    @property
    def daily_returns(self) -> pd.Series:
        """The approved date-level equal-weight active-portfolio return series."""

        return self.daily_portfolio_returns


@dataclass(frozen=True)
class HardFilterPitLineage:
    """Experiment-local Market v5 provenance contract."""

    market_schema_version: int
    stock_price_adjustment_mode: str
    market_source: str
    source_mode: SourceMode


@dataclass(frozen=True)
class HardFilterEvidenceTables:
    rule_registry_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    trade_ledger_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    entry_rule_evidence_df: pd.DataFrame
    exit_rule_evidence_df: pd.DataFrame
    combined_rule_evidence_df: pd.DataFrame
    annual_stability_df: pd.DataFrame
    bootstrap_effect_ci_df: pd.DataFrame
    cost_sensitivity_df: pd.DataFrame


class _PeriodExecutionSlice(TypedDict):
    returns: pd.Series
    benchmark_returns: pd.Series
    trades: pd.DataFrame
    signal_dates: int
    turnover_events: int


class _PortfolioMetrics(TypedDict):
    annualized_ir: float | None
    max_drawdown: float | None
    expected_shortfall_5pct: float | None
    turnover: float | None


@dataclass(frozen=True)
class RankingSma5ScoreRingHardFilterResult:
    db_path: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    pit_lineage: HardFilterPitLineage
    rule_registry_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    trade_ledger_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    entry_rule_evidence_df: pd.DataFrame
    exit_rule_evidence_df: pd.DataFrame
    combined_rule_evidence_df: pd.DataFrame
    annual_stability_df: pd.DataFrame
    bootstrap_effect_ci_df: pd.DataFrame
    cost_sensitivity_df: pd.DataFrame
    decision_gate_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


@dataclass(frozen=True)
class RankingSma5ScoreRingHardFilterResearchResult:
    """Market v5 feature panel and provenance for score-ring execution research."""

    db_path: str
    source_mode: SourceMode
    source_detail: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    bootstrap_resamples: int
    min_trades: int
    min_signal_dates: int
    pit_lineage: HardFilterPitLineage
    feature_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def moving_block_bootstrap_delta_ci(
    candidate: pd.Series,
    baseline: pd.Series,
    *,
    block_length: int,
    resamples: int,
    seed: int,
) -> BootstrapInterval:
    """Bootstrap the paired candidate-minus-baseline mean on trading dates.

    The union index represents the comparison calendar.  A strategy without an
    active position on a date earns the cash return of zero.
    """

    if isinstance(block_length, bool) or not isinstance(block_length, int) or block_length <= 0:
        raise ValueError("block_length must be a positive integer")
    if isinstance(resamples, bool) or not isinstance(resamples, int) or resamples <= 0:
        raise ValueError("resamples must be a positive integer")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError("seed must be an integer")
    if not candidate.index.is_unique or not baseline.index.is_unique:
        raise ValueError("candidate and baseline indexes must be unique")

    union_index = candidate.index.union(baseline.index).sort_values()
    if union_index.empty:
        raise ValueError("candidate and baseline must contain at least one return")
    candidate_values = pd.to_numeric(
        candidate.reindex(union_index), errors="coerce"
    ).fillna(0.0)
    baseline_values = pd.to_numeric(
        baseline.reindex(union_index), errors="coerce"
    ).fillna(0.0)
    deltas = candidate_values.to_numpy(dtype=float) - baseline_values.to_numpy(dtype=float)
    if not np.isfinite(deltas).all():
        raise ValueError("candidate and baseline returns must be finite")

    observation_count = len(deltas)
    block_count = math.ceil(observation_count / block_length)
    rng = np.random.default_rng(seed)
    sampled_means = np.empty(resamples, dtype=float)
    offsets = np.arange(block_length)
    for sample_index in range(resamples):
        starts = rng.integers(0, observation_count, size=block_count)
        sampled_positions = (starts[:, None] + offsets) % observation_count
        sampled_means[sample_index] = float(
            deltas[sampled_positions.ravel()[:observation_count]].mean()
        )

    non_positive = int(np.count_nonzero(sampled_means <= 0.0))
    non_negative = int(np.count_nonzero(sampled_means >= 0.0))
    empirical_p = min(
        1.0,
        2.0 * (min(non_positive, non_negative) + 1.0) / (resamples + 1.0),
    )
    lower, upper = np.quantile(sampled_means, [0.025, 0.975])
    return BootstrapInterval(
        estimate=float(deltas.mean()),
        lower=float(lower),
        upper=float(upper),
        p_value=float(empirical_p),
        observations=observation_count,
        block_length=block_length,
        resamples=resamples,
        seed=seed,
    )


def holm_adjust(p_values: Sequence[float | None]) -> list[float | None]:
    """Return Holm step-down adjusted p-values in their original order."""

    indexed: list[tuple[int, float]] = []
    for index, value in enumerate(p_values):
        if value is None:
            continue
        numeric = float(value)
        if not math.isfinite(numeric) or numeric < 0.0 or numeric > 1.0:
            raise ValueError("p-values must be finite numbers between zero and one")
        indexed.append((index, numeric))

    adjusted: list[float | None] = [None] * len(p_values)
    running_max = 0.0
    family_size = len(indexed)
    for rank, (original_index, p_value) in enumerate(
        sorted(indexed, key=lambda item: item[1])
    ):
        running_max = max(running_max, (family_size - rank) * p_value)
        adjusted[original_index] = min(1.0, running_max)
    return adjusted


def build_evidence_tables(
    executions: Sequence[VariantExecution],
    *,
    block_length: int,
    resamples: int,
    seed: int,
) -> HardFilterEvidenceTables:
    """Aggregate frozen variant executions into the approved evidence tables."""

    execution_map: dict[tuple[str, str, int, str, float], VariantExecution] = {}
    for execution in executions:
        variant = execution.variant
        key = (
            variant.ring_id,
            variant.entry_rule_id,
            variant.max_holding_sessions,
            variant.exit_rule_id,
            float(execution.fee_bps),
        )
        if key in execution_map:
            raise ValueError(f"duplicate execution for variant/cost: {key}")
        execution_map[key] = execution
    if not execution_map:
        raise ValueError("executions must not be empty")

    variant_keys = sorted({key[:4] for key in execution_map})
    for variant_key in variant_keys:
        missing_costs = [
            cost_bps
            for cost_bps in (0.0, 10.0, 20.0)
            if (*variant_key, cost_bps) not in execution_map
        ]
        if missing_costs:
            raise ValueError(
                f"variant {variant_key} missing required cost executions: {missing_costs}"
            )

    registry_rows = [
        {
            "family": family,
            "variant_id": variant_id,
            "ring_id": ring_id,
            "entry_rule_id": entry_rule_id,
            "exit_rule_id": exit_rule_id,
            "max_holding_sessions": cap,
            "is_primary": ring_id == "core_high_high" and cap == 60,
        }
        for ring_id, entry_rule_id, cap, exit_rule_id in variant_keys
        if (
            family := _variant_family(entry_rule_id, exit_rule_id)
        )
        is not None
        for variant_id in [_variant_id(family, entry_rule_id, exit_rule_id)]
    ]
    rule_registry_df = pd.DataFrame(registry_rows)

    ledger_frames: list[pd.DataFrame] = []
    daily_frames: list[pd.DataFrame] = []
    for key, execution in sorted(execution_map.items()):
        ring_id, entry_rule_id, cap, exit_rule_id, cost_bps = key
        family = _variant_family(entry_rule_id, exit_rule_id) or "baseline"
        variant_id = _variant_id(family, entry_rule_id, exit_rule_id)
        ledger = execution.trade_records_df.copy()
        ledger.insert(0, "cost_bps", cost_bps)
        ledger.insert(0, "max_holding_sessions", cap)
        ledger.insert(0, "ring_id", ring_id)
        ledger.insert(0, "variant_id", variant_id)
        ledger.insert(0, "family", family)
        ledger_frames.append(ledger)
        daily_frames.append(
            pd.concat(
                [
                    execution.daily_portfolio_returns.rename("portfolio_return"),
                    execution.benchmark_daily_returns.rename("benchmark_return"),
                ],
                axis=1,
            )
            .assign(
                topix_excess_return=lambda frame: (
                    frame["portfolio_return"] - frame["benchmark_return"]
                )
            )
            .rename_axis("date")
            .reset_index()
            .assign(
                family=family,
                variant_id=variant_id,
                ring_id=ring_id,
                max_holding_sessions=cap,
                cost_bps=cost_bps,
            )
        )
    trade_ledger_df = pd.concat(ledger_frames, ignore_index=True)
    portfolio_daily_df = pd.concat(daily_frames, ignore_index=True)

    periods = (
        ("discovery", pd.Timestamp("2018-01-01"), pd.Timestamp("2021-12-31")),
        ("oos", pd.Timestamp("2022-01-01"), pd.Timestamp("2024-12-31")),
        ("holdout", pd.Timestamp("2025-01-01"), None),
    )
    evidence_rows: list[dict[str, object]] = []
    bootstrap_rows: list[dict[str, object]] = []
    cost_rows: list[dict[str, object]] = []
    annual_rows: list[dict[str, object]] = []
    for ring_id, entry_rule_id, cap, exit_rule_id in variant_keys:
        family = _variant_family(entry_rule_id, exit_rule_id)
        if family is None:
            continue
        variant_id = _variant_id(family, entry_rule_id, exit_rule_id)
        baseline_entry, baseline_exit = _baseline_rule_pair(family)
        baseline_key = (ring_id, baseline_entry, cap, baseline_exit)
        if not all((*baseline_key, cost) in execution_map for cost in (0.0, 10.0, 20.0)):
            raise ValueError(
                f"missing correct {family} baseline for {ring_id}/{cap}/{variant_id}"
            )

        candidate_by_cost = {
            cost: execution_map[(ring_id, entry_rule_id, cap, exit_rule_id, cost)]
            for cost in (0.0, 10.0, 20.0)
        }
        baseline_by_cost = {
            cost: execution_map[(*baseline_key, cost)]
            for cost in (0.0, 10.0, 20.0)
        }
        for period, start, end in periods:
            gross_candidate = _slice_execution(candidate_by_cost[0.0], start, end)
            net_candidate = _slice_execution(candidate_by_cost[10.0], start, end)
            net_baseline = _slice_execution(baseline_by_cost[10.0], start, end)
            candidate_metrics = _portfolio_metrics(
                net_candidate["returns"],
                benchmark_returns=net_candidate["benchmark_returns"],
                turnover_events=int(net_candidate["turnover_events"]),
            )
            baseline_metrics = _portfolio_metrics(
                net_baseline["returns"],
                benchmark_returns=net_baseline["benchmark_returns"],
                turnover_events=int(net_baseline["turnover_events"]),
            )
            gross_trade_returns = _trade_returns(gross_candidate["trades"])
            net_trade_returns = _trade_returns(net_candidate["trades"])
            interval = _optional_bootstrap_interval(
                net_candidate["returns"],
                net_baseline["returns"],
                block_length=block_length,
                resamples=resamples,
                seed=seed,
            )
            max_drawdown_improvement = _relative_loss_improvement(
                candidate_metrics["max_drawdown"],
                baseline_metrics["max_drawdown"],
            )
            expected_shortfall_improvement = _relative_loss_improvement(
                candidate_metrics["expected_shortfall_5pct"],
                baseline_metrics["expected_shortfall_5pct"],
            )
            tail_improvement_values = [
                value
                for value in (
                    max_drawdown_improvement,
                    expected_shortfall_improvement,
                )
                if value is not None
            ]
            turnover_ratio = _safe_ratio(
                candidate_metrics["turnover"],
                baseline_metrics["turnover"],
            )
            evidence_row = {
                "family": family,
                "variant_id": variant_id,
                "baseline_variant_id": _variant_id(
                    "baseline", baseline_entry, baseline_exit
                ),
                "ring_id": ring_id,
                "max_holding_sessions": cap,
                "period": period,
                "is_primary": ring_id == "core_high_high" and cap == 60,
                "trade_count": len(net_candidate["trades"]),
                "signal_date_count": int(net_candidate["signal_dates"]),
                "gross_mean_return": _series_stat(gross_trade_returns, "mean"),
                "gross_median_return": _series_stat(gross_trade_returns, "median"),
                "net_mean_return": _series_stat(net_trade_returns, "mean"),
                "net_median_return": _series_stat(net_trade_returns, "median"),
                "annualized_ir": candidate_metrics["annualized_ir"],
                "max_drawdown": candidate_metrics["max_drawdown"],
                "expected_shortfall_5pct": candidate_metrics[
                    "expected_shortfall_5pct"
                ],
                "turnover": candidate_metrics["turnover"],
                "net_mean_return_delta": _mean_delta(
                    net_candidate["returns"], net_baseline["returns"]
                ),
                "annualized_ir_delta": _difference(
                    candidate_metrics["annualized_ir"],
                    baseline_metrics["annualized_ir"],
                ),
                "max_drawdown_improvement_ratio": max_drawdown_improvement,
                "expected_shortfall_improvement_ratio": expected_shortfall_improvement,
                "tail_improvement_ratio": (
                    max(tail_improvement_values) if tail_improvement_values else None
                ),
                "turnover_ratio": turnover_ratio,
                "ci_lower": None if interval is None else interval.lower,
                "ci_upper": None if interval is None else interval.upper,
                "raw_p_value": None if interval is None else interval.p_value,
                "adjusted_p_value": None,
            }
            evidence_rows.append(evidence_row)
            bootstrap_rows.append(
                {
                    "family": family,
                    "variant_id": variant_id,
                    "ring_id": ring_id,
                    "max_holding_sessions": cap,
                    "period": period,
                    "estimate": None if interval is None else interval.estimate,
                    "ci_lower": None if interval is None else interval.lower,
                    "ci_upper": None if interval is None else interval.upper,
                    "raw_p_value": None if interval is None else interval.p_value,
                    "observations": 0 if interval is None else interval.observations,
                    "block_length": block_length,
                    "resamples": resamples,
                    "seed": seed,
                }
            )
            for cost_bps in (10.0, 20.0):
                candidate_cost_returns = _slice_returns(
                    candidate_by_cost[cost_bps].daily_portfolio_returns, start, end
                )
                baseline_cost_returns = _slice_returns(
                    baseline_by_cost[cost_bps].daily_portfolio_returns, start, end
                )
                cost_rows.append(
                    {
                        "family": family,
                        "variant_id": variant_id,
                        "ring_id": ring_id,
                        "max_holding_sessions": cap,
                        "period": period,
                        "cost_bps": cost_bps,
                        "net_mean_return_delta": _mean_delta(
                            candidate_cost_returns, baseline_cost_returns
                        ),
                    }
                )

        if ring_id == "core_high_high" and cap == 60:
            candidate_oos = _slice_returns(
                candidate_by_cost[10.0].daily_portfolio_returns,
                pd.Timestamp("2022-01-01"),
                pd.Timestamp("2024-12-31"),
            )
            baseline_oos = _slice_returns(
                baseline_by_cost[10.0].daily_portfolio_returns,
                pd.Timestamp("2022-01-01"),
                pd.Timestamp("2024-12-31"),
            )
            paired = _align_cash_returns(candidate_oos, baseline_oos)
            for year, year_rows in paired.groupby(paired.index.year):
                annual_rows.append(
                    {
                        "family": family,
                        "variant_id": variant_id,
                        "ring_id": ring_id,
                        "max_holding_sessions": cap,
                        "period": "oos",
                        "year": int(year),
                        "net_mean_return_delta": float(
                            (year_rows["candidate"] - year_rows["baseline"]).mean()
                        ),
                    }
                )

    evidence_df = pd.DataFrame(evidence_rows)
    if not evidence_df.empty:
        for _, indexes in evidence_df.groupby(
            ["family", "ring_id", "max_holding_sessions", "period"],
            sort=False,
        ).groups.items():
            index_list = list(indexes)
            adjusted = holm_adjust(
                [
                    _finite_number(value)
                    for value in evidence_df.loc[index_list, "raw_p_value"]
                ]
            )
            evidence_df.loc[index_list, "adjusted_p_value"] = adjusted
    evidence_columns = list(evidence_df.columns)

    coverage_diagnostics_df = pd.DataFrame(
        [
            {
                "execution_count": len(execution_map),
                "variant_count": len(variant_keys),
                "first_date": (
                    portfolio_daily_df["date"].min()
                    if not portfolio_daily_df.empty
                    else None
                ),
                "last_date": (
                    portfolio_daily_df["date"].max()
                    if not portfolio_daily_df.empty
                    else None
                ),
            }
        ]
    )
    return HardFilterEvidenceTables(
        rule_registry_df=rule_registry_df,
        coverage_diagnostics_df=coverage_diagnostics_df,
        trade_ledger_df=trade_ledger_df,
        portfolio_daily_df=portfolio_daily_df,
        entry_rule_evidence_df=evidence_df.loc[
            evidence_df["family"].eq("entry"), evidence_columns
        ].reset_index(drop=True),
        exit_rule_evidence_df=evidence_df.loc[
            evidence_df["family"].eq("exit"), evidence_columns
        ].reset_index(drop=True),
        combined_rule_evidence_df=evidence_df.loc[
            evidence_df["family"].eq("combined"), evidence_columns
        ].reset_index(drop=True),
        annual_stability_df=pd.DataFrame(annual_rows),
        bootstrap_effect_ci_df=pd.DataFrame(bootstrap_rows),
        cost_sensitivity_df=pd.DataFrame(cost_rows),
    )


def _variant_family(entry_rule_id: str, exit_rule_id: str) -> str | None:
    entry_is_baseline = entry_rule_id == "E0_no_sma5_filter"
    exit_is_baseline = exit_rule_id == "X0_no_sma5_exit"
    if entry_is_baseline and exit_is_baseline:
        return None
    if not entry_is_baseline and exit_is_baseline:
        return "entry"
    if entry_is_baseline and not exit_is_baseline:
        return "exit"
    return "combined"


def _variant_id(family: str, entry_rule_id: str, exit_rule_id: str) -> str:
    if family == "entry":
        return entry_rule_id
    if family == "exit":
        return exit_rule_id
    if family == "baseline":
        return "E0_no_sma5_filter"
    return f"{entry_rule_id}__{exit_rule_id}"


def _baseline_rule_pair(family: str) -> tuple[str, str]:
    if family in {"entry", "exit", "combined"}:
        return "E0_no_sma5_filter", "X0_no_sma5_exit"
    raise ValueError(f"unknown evidence family: {family}")


def _slice_execution(
    execution: VariantExecution,
    start: pd.Timestamp,
    end: pd.Timestamp | None,
) -> _PeriodExecutionSlice:
    returns = _slice_returns(execution.daily_portfolio_returns, start, end)
    trades = execution.trade_records_df
    entry_dates = pd.to_datetime(trades["Entry Timestamp"], errors="coerce")
    exit_dates = pd.to_datetime(trades["Exit Timestamp"], errors="coerce")
    # Period trade statistics use only closed trades fully contained in the slice.
    trade_mask = entry_dates.ge(start) & exit_dates.ge(start)
    if end is not None:
        trade_mask &= entry_dates.le(end) & exit_dates.le(end)
    period_trades = trades.loc[trade_mask].copy()
    events = execution.state_events
    event_dates = pd.to_datetime(events["date"], errors="coerce")
    signal_mask = events["event_type"].eq("entry") & event_dates.ge(start)
    if end is not None:
        signal_mask &= event_dates.le(end)
    signal_dates = event_dates.loc[signal_mask].nunique()
    turnover_mask = events["event_type"].isin(["entry", "exit"]) & event_dates.ge(start)
    if end is not None:
        turnover_mask &= event_dates.le(end)
    return {
        "returns": returns,
        "benchmark_returns": _slice_returns(
            execution.benchmark_daily_returns, start, end
        ),
        "trades": period_trades,
        "signal_dates": signal_dates,
        "turnover_events": int(turnover_mask.sum()),
    }


def _slice_returns(
    returns: pd.Series,
    start: pd.Timestamp,
    end: pd.Timestamp | None,
) -> pd.Series:
    normalized = pd.Series(returns, dtype=float).copy()
    normalized.index = pd.to_datetime(normalized.index, errors="raise")
    mask = normalized.index >= start
    if end is not None:
        mask &= normalized.index <= end
    return normalized.loc[mask]


def _align_cash_returns(candidate: pd.Series, baseline: pd.Series) -> pd.DataFrame:
    union_index = candidate.index.union(baseline.index).sort_values()
    return pd.DataFrame(
        {
            "candidate": pd.to_numeric(
                candidate.reindex(union_index), errors="coerce"
            ).fillna(0.0),
            "baseline": pd.to_numeric(
                baseline.reindex(union_index), errors="coerce"
            ).fillna(0.0),
        },
        index=union_index,
    )


def _optional_bootstrap_interval(
    candidate: pd.Series,
    baseline: pd.Series,
    *,
    block_length: int,
    resamples: int,
    seed: int,
) -> BootstrapInterval | None:
    if candidate.empty and baseline.empty:
        return None
    return moving_block_bootstrap_delta_ci(
        candidate,
        baseline,
        block_length=block_length,
        resamples=resamples,
        seed=seed,
    )


def _portfolio_metrics(
    returns: pd.Series,
    *,
    benchmark_returns: pd.Series,
    turnover_events: int,
) -> _PortfolioMetrics:
    values = pd.to_numeric(returns, errors="coerce").dropna()
    if values.empty:
        return {
            "annualized_ir": None,
            "max_drawdown": None,
            "expected_shortfall_5pct": None,
            "turnover": None,
        }
    benchmark = pd.to_numeric(
        benchmark_returns.reindex(values.index), errors="coerce"
    )
    if benchmark.isna().any():
        raise ValueError("benchmark returns must cover every portfolio return date")
    excess_returns = values - benchmark
    standard_deviation = float(excess_returns.std(ddof=1))
    annualized_ir = (
        float(excess_returns.mean() / standard_deviation * math.sqrt(252.0))
        if standard_deviation > 0.0
        else 0.0
    )
    nav = (1.0 + values).cumprod()
    max_drawdown = float((nav / nav.cummax() - 1.0).min())
    threshold = float(values.quantile(0.05))
    expected_shortfall = float(values.loc[values <= threshold].mean())
    turnover = float(turnover_events / values.index.size)
    return {
        "annualized_ir": annualized_ir,
        "max_drawdown": max_drawdown,
        "expected_shortfall_5pct": expected_shortfall,
        "turnover": turnover,
    }


def _trade_returns(trades: pd.DataFrame) -> pd.Series:
    if "Return" not in trades.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(trades["Return"], errors="coerce").dropna()


def _series_stat(values: pd.Series, operation: str) -> float | None:
    if values.empty:
        return None
    if operation == "mean":
        return float(values.mean())
    if operation == "median":
        return float(values.median())
    raise ValueError(f"unknown series statistic: {operation}")


def _mean_delta(candidate: pd.Series, baseline: pd.Series) -> float | None:
    paired = _align_cash_returns(candidate, baseline)
    if paired.empty:
        return None
    return float((paired["candidate"] - paired["baseline"]).mean())


def _difference(candidate: float | None, baseline: float | None) -> float | None:
    if candidate is None or baseline is None:
        return None
    return candidate - baseline


def _relative_loss_improvement(
    candidate: float | None,
    baseline: float | None,
) -> float | None:
    if candidate is None or baseline is None or abs(baseline) == 0.0:
        return None
    return (abs(baseline) - abs(candidate)) / abs(baseline)


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None:
        return None
    if denominator == 0.0:
        return 0.0 if numerator == 0.0 else None
    return numerator / denominator


def build_decision_gate_df(
    evidence_df: pd.DataFrame,
    annual_stability_df: pd.DataFrame,
    cost_sensitivity_df: pd.DataFrame,
) -> pd.DataFrame:
    """Apply the frozen primary, robustness, and operational adoption gates."""

    evidence_columns = {
        "family",
        "variant_id",
        "ring_id",
        "max_holding_sessions",
        "period",
        "is_primary",
        "ci_lower",
        "ci_upper",
        "adjusted_p_value",
        "trade_count",
        "signal_date_count",
        "annualized_ir_delta",
        "tail_improvement_ratio",
        "turnover_ratio",
        "net_mean_return_delta",
    }
    annual_columns = {
        "family",
        "variant_id",
        "ring_id",
        "max_holding_sessions",
        "period",
        "year",
        "net_mean_return_delta",
    }
    cost_columns = {
        "family",
        "variant_id",
        "ring_id",
        "max_holding_sessions",
        "period",
        "cost_bps",
        "net_mean_return_delta",
    }
    _require_columns(evidence_df, evidence_columns, frame_name="evidence_df")
    _require_columns(
        annual_stability_df,
        annual_columns,
        frame_name="annual_stability_df",
    )
    _require_columns(
        cost_sensitivity_df,
        cost_columns,
        frame_name="cost_sensitivity_df",
    )

    variant_rows: list[dict[str, object]] = []
    primary = evidence_df.loc[
        evidence_df["ring_id"].eq("core_high_high")
        & pd.to_numeric(
            evidence_df["max_holding_sessions"], errors="coerce"
        ).eq(60)
        & evidence_df["period"].eq("oos")
        & evidence_df["is_primary"].eq(True)
        & evidence_df["family"].isin(["entry", "exit", "combined"])
    ]
    for (family, variant_id), rows in primary.groupby(
        ["family", "variant_id"], sort=True, dropna=False
    ):
        primary_valid = len(rows) == 1
        row = rows.iloc[0]
        family_text = str(family)
        variant_text = str(variant_id)

        ci_lower = _finite_number(row["ci_lower"])
        ci_upper = _finite_number(row["ci_upper"])
        adjusted_p = _finite_number(row["adjusted_p_value"])
        trade_count = _finite_number(row["trade_count"])
        signal_date_count = _finite_number(row["signal_date_count"])
        ir_delta = _finite_number(row["annualized_ir_delta"])
        tail_improvement = _finite_number(row["tail_improvement_ratio"])
        turnover_ratio = _finite_number(row["turnover_ratio"])
        net_delta = _finite_number(row["net_mean_return_delta"])

        passes_bootstrap_ci = bool(
            primary_valid
            and ci_lower is not None
            and ci_upper is not None
            and ci_lower > 0.0
            and ci_upper > 0.0
        )
        passes_adjusted_p = bool(
            primary_valid and adjusted_p is not None and adjusted_p < 0.05
        )
        passes_trade_count = bool(
            primary_valid and trade_count is not None and trade_count >= 200.0
        )
        passes_signal_date_count = bool(
            primary_valid
            and signal_date_count is not None
            and signal_date_count >= 100.0
        )
        passes_ir_lift = bool(
            primary_valid and ir_delta is not None and ir_delta >= 0.15
        )
        passes_tail_improvement = bool(
            primary_valid
            and tail_improvement is not None
            and tail_improvement >= 0.10
        )
        passes_turnover = bool(
            primary_valid and turnover_ratio is not None and turnover_ratio <= 1.5
        )
        passes_base_cost = bool(
            primary_valid and net_delta is not None and net_delta > 0.0
        )

        holdout = evidence_df.loc[
            evidence_df["family"].eq(family_text)
            & evidence_df["variant_id"].eq(variant_text)
            & evidence_df["ring_id"].eq("core_high_high")
            & pd.to_numeric(
                evidence_df["max_holding_sessions"], errors="coerce"
            ).eq(60)
            & evidence_df["period"].eq("holdout")
        ]
        holdout_delta = (
            _finite_number(holdout.iloc[0]["net_mean_return_delta"])
            if len(holdout) == 1
            else None
        )
        passes_holdout_direction = bool(
            holdout_delta is not None and holdout_delta > 0.0
        )

        robustness = evidence_df.loc[
            evidence_df["family"].eq(family_text)
            & evidence_df["variant_id"].eq(variant_text)
            & evidence_df["ring_id"].isin(
                ["near_high_high_1", "near_high_high_2"]
            )
            & pd.to_numeric(
                evidence_df["max_holding_sessions"], errors="coerce"
            ).eq(20)
            & evidence_df["period"].eq("oos")
        ]
        robustness_by_ring = {
            ring_id: group
            for ring_id, group in robustness.groupby("ring_id", sort=False)
        }
        passes_robustness_sign = all(
            ring_id in robustness_by_ring
            and len(robustness_by_ring[ring_id]) == 1
            and (
                robustness_delta := _finite_number(
                    robustness_by_ring[ring_id].iloc[0]["net_mean_return_delta"]
                )
            )
            is not None
            and robustness_delta > 0.0
            for ring_id in ("near_high_high_1", "near_high_high_2")
        )

        annual = annual_stability_df.loc[
            annual_stability_df["family"].eq(family_text)
            & annual_stability_df["variant_id"].eq(variant_text)
            & annual_stability_df["ring_id"].eq("core_high_high")
            & pd.to_numeric(
                annual_stability_df["max_holding_sessions"], errors="coerce"
            ).eq(60)
            & annual_stability_df["period"].eq("oos")
        ]
        annual_by_year = (
            annual.assign(
                year_numeric=pd.to_numeric(annual["year"], errors="coerce"),
                delta_numeric=pd.to_numeric(
                    annual["net_mean_return_delta"], errors="coerce"
                ),
            )
            .dropna(subset=["year_numeric", "delta_numeric"])
            .groupby("year_numeric", sort=True)["delta_numeric"]
            .mean()
        )
        annual_by_year = annual_by_year.loc[np.isfinite(annual_by_year)]
        total_years = int(len(annual_by_year))
        positive_years = int(annual_by_year.gt(0.0).sum())
        passes_positive_year_majority = bool(
            total_years >= 2 and positive_years > total_years / 2.0
        )
        passes_not_single_year_dependent = bool(
            total_years >= 2
            and float(annual_by_year.sum() - annual_by_year.max()) > 0.0
        )
        passes_annual_stability = bool(
            passes_positive_year_majority
            and passes_not_single_year_dependent
        )

        costs = cost_sensitivity_df.loc[
            cost_sensitivity_df["family"].eq(family_text)
            & cost_sensitivity_df["variant_id"].eq(variant_text)
            & cost_sensitivity_df["ring_id"].eq("core_high_high")
            & pd.to_numeric(
                cost_sensitivity_df["max_holding_sessions"], errors="coerce"
            ).eq(60)
            & cost_sensitivity_df["period"].eq("oos")
        ]
        cost_delta_by_level: dict[float, float | None] = {}
        for cost_bps in (10.0, 20.0):
            cost_rows = costs.loc[
                pd.to_numeric(costs["cost_bps"], errors="coerce").eq(cost_bps)
            ]
            cost_delta_by_level[cost_bps] = (
                _finite_number(cost_rows.iloc[0]["net_mean_return_delta"])
                if len(cost_rows) == 1
                else None
            )
        base_cost_delta = cost_delta_by_level[10.0]
        stress_cost_delta = cost_delta_by_level[20.0]
        passes_cost_sensitivity = bool(
            base_cost_delta is not None
            and base_cost_delta > 0.0
            and stress_cost_delta is not None
            and stress_cost_delta > 0.0
        )

        pre_holdout_values = (
            passes_bootstrap_ci,
            passes_adjusted_p,
            passes_trade_count,
            passes_signal_date_count,
            passes_ir_lift,
            passes_tail_improvement,
            passes_turnover,
            passes_base_cost,
            passes_cost_sensitivity,
            passes_annual_stability,
            passes_robustness_sign,
        )
        passes_pre_holdout = all(pre_holdout_values)
        all_required_gates = bool(
            passes_pre_holdout and passes_holdout_direction
        )
        variant_rows.append(
            {
                "row_type": "variant",
                "family": family_text,
                "variant_id": variant_text,
                "passes_bootstrap_ci": passes_bootstrap_ci,
                "passes_adjusted_p": passes_adjusted_p,
                "passes_trade_count": passes_trade_count,
                "passes_signal_date_count": passes_signal_date_count,
                "passes_ir_lift": passes_ir_lift,
                "passes_tail_improvement": passes_tail_improvement,
                "passes_turnover": passes_turnover,
                "passes_base_cost": passes_base_cost,
                "passes_cost_sensitivity": passes_cost_sensitivity,
                "distinct_annual_year_count": total_years,
                "positive_annual_year_count": positive_years,
                "passes_positive_year_majority": passes_positive_year_majority,
                "passes_not_single_year_dependent": (
                    passes_not_single_year_dependent
                ),
                "passes_annual_stability": passes_annual_stability,
                "passes_robustness_sign": passes_robustness_sign,
                "passes_holdout_direction": passes_holdout_direction,
                "passes_pre_holdout": passes_pre_holdout,
                "all_required_gates": all_required_gates,
                "decision": (
                    "production_candidate"
                    if all_required_gates
                    else "insufficient_evidence"
                ),
            }
        )

    raw_pre_holdout = {
        (str(row["family"]), str(row["variant_id"])): bool(
            row["passes_pre_holdout"]
        )
        for row in variant_rows
    }
    for row in variant_rows:
        prerequisite = True
        if (
            row["family"] == "entry"
            and row["variant_id"] == "E4_count_ge_2_and_avoid_chase"
        ):
            prerequisite = bool(
                raw_pre_holdout.get(("entry", "E2_count_ge_2"), False)
                and raw_pre_holdout.get(("entry", "E3_avoid_atr20_chase"), False)
            )
        row["passes_confirmatory_prerequisite"] = prerequisite
        if not prerequisite:
            row["all_required_gates"] = False
            row["decision"] = "not_evaluated"

    effective_pre_holdout = {
        (str(row["family"]), str(row["variant_id"])): bool(
            row["passes_pre_holdout"]
            and row["passes_confirmatory_prerequisite"]
        )
        for row in variant_rows
        if row["family"] != "combined"
    }
    for row in variant_rows:
        if row["family"] != "combined":
            continue
        components = str(row["variant_id"]).split("__", maxsplit=1)
        prerequisite = bool(
            len(components) == 2
            and effective_pre_holdout.get(("entry", components[0]), False)
            and effective_pre_holdout.get(("exit", components[1]), False)
        )
        row["passes_confirmatory_prerequisite"] = prerequisite
        if not prerequisite:
            row["all_required_gates"] = False
            row["decision"] = "not_evaluated"

    family_rows: list[dict[str, object]] = []
    for family in ("entry", "exit", "combined"):
        rows = [row for row in variant_rows if row["family"] == family]
        eligible_rows = [
            row for row in rows if bool(row["passes_confirmatory_prerequisite"])
        ]
        if family == "combined" and not eligible_rows:
            family_decision = "not_evaluated"
        elif any(
            row["decision"] == "production_candidate" for row in eligible_rows
        ):
            family_decision = "production_candidate"
        else:
            family_decision = "insufficient_evidence"
        family_rows.append(
            {
                "row_type": "family",
                "family": family,
                "variant_id": None,
                "passes_confirmatory_prerequisite": bool(eligible_rows),
                "passes_pre_holdout": any(
                    bool(
                        row["passes_pre_holdout"]
                        and row["passes_confirmatory_prerequisite"]
                    )
                    for row in rows
                ),
                "all_required_gates": any(
                    bool(row["all_required_gates"]) for row in rows
                ),
                "decision": family_decision,
            }
        )
    return pd.DataFrame([*variant_rows, *family_rows])


def _require_columns(
    frame: pd.DataFrame,
    required: set[str],
    *,
    frame_name: str,
) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} missing required columns: {missing}")


def _finite_number(value: object) -> float | None:
    return _safe_finite_float_or_none(value)


def classify_score_ring(value_score: object, leadership_score: object) -> str:
    """Return the most selective score-ring label satisfied by two scores."""
    value = _safe_finite_float_or_none(value_score)
    leadership = _safe_finite_float_or_none(leadership_score)
    if value is None or leadership is None:
        return "missing"
    for ring_id, threshold in SCORE_RING_THRESHOLDS.items():
        if value >= threshold and leadership >= threshold:
            return ring_id
    return "outside"


def entry_rule_matches(row: Mapping[str, object], rule_id: str) -> bool:
    """Evaluate a frozen entry rule, failing closed for missing numeric inputs."""
    if rule_id not in ENTRY_RULE_IDS:
        raise ValueError(f"unknown entry rule: {rule_id}")
    if rule_id == "E0_no_sma5_filter":
        return True
    if rule_id == "E1_close_above_sma5":
        close = _numeric_value(row, "close")
        sma5 = _numeric_value(row, "sma5")
        return close is not None and sma5 is not None and close >= sma5
    if rule_id == "E2_count_ge_2":
        count = _numeric_value(row, "sma5_above_count_5d")
        return count is not None and count >= 2.0
    if rule_id == "E3_avoid_atr20_chase":
        deviation = _numeric_value(row, "sma5_atr20_deviation")
        return deviation is not None and deviation < 1.0
    count = _numeric_value(row, "sma5_above_count_5d")
    deviation = _numeric_value(row, "sma5_atr20_deviation")
    return count is not None and count >= 2.0 and deviation is not None and deviation < 1.0


def exit_rule_matches(row: Mapping[str, object], rule_id: str) -> bool:
    """Evaluate a frozen exit rule, failing closed for missing numeric inputs."""
    if rule_id not in EXIT_RULE_IDS:
        raise ValueError(f"unknown exit rule: {rule_id}")
    if rule_id == "X0_no_sma5_exit":
        return False
    if rule_id == "X1_close_below_sma5":
        close = _numeric_value(row, "close")
        sma5 = _numeric_value(row, "sma5")
        return close is not None and sma5 is not None and close < sma5
    if rule_id == "X2_count_le_1":
        count = _numeric_value(row, "sma5_above_count_5d")
        return count is not None and count <= 1.0
    if rule_id == "X3_below_streak_ge_3":
        streak = _numeric_value(row, "sma5_below_streak")
        return streak is not None and streak >= 3.0
    deviation = _numeric_value(row, "sma5_atr20_deviation")
    return deviation is not None and deviation <= -1.0


def build_position_signal_frames(
    feature_df: pd.DataFrame,
    *,
    ring_id: str,
    entry_rule_id: str,
    exit_rule_id: str,
    max_holding_sessions: int,
) -> PositionSignalFrames:
    """Build aligned price, signal, exposure, and event frames for one variant.

    Membership is threshold based, so a wider ring contains every qualifying row,
    including those classified into a more selective label.
    """
    _validate_arguments(
        feature_df,
        ring_id=ring_id,
        entry_rule_id=entry_rule_id,
        exit_rule_id=exit_rule_id,
        max_holding_sessions=max_holding_sessions,
    )
    prepared = feature_df.copy()
    prepared["date"] = pd.to_datetime(prepared["date"], errors="raise")
    prepared["code"] = prepared["code"].astype(str)
    if prepared.duplicated(["date", "code"]).any():
        raise ValueError("feature_df must contain at most one row per date and code")
    prepared = prepared.sort_values(["code", "date"], kind="stable")

    dates = pd.DatetimeIndex(sorted(prepared["date"].unique()), name="date")
    codes = pd.Index(sorted(prepared["code"].unique()), name="code")
    close = (
        prepared.assign(close=pd.to_numeric(prepared["close"], errors="coerce"))
        .pivot(index="date", columns="code", values="close")
        .reindex(index=dates, columns=codes)
    )
    entries = pd.DataFrame(False, index=dates, columns=codes, dtype=bool)
    exits = pd.DataFrame(False, index=dates, columns=codes, dtype=bool)
    held_intervals = pd.DataFrame(False, index=dates, columns=codes, dtype=bool)
    events: list[dict[str, object]] = []

    for code, code_frame in prepared.groupby("code", sort=False):
        _build_code_position_state(
            code_frame,
            code=str(code),
            ring_id=ring_id,
            entry_rule_id=entry_rule_id,
            exit_rule_id=exit_rule_id,
            max_holding_sessions=max_holding_sessions,
            entries=entries,
            exits=exits,
            held_intervals=held_intervals,
            events=events,
        )

    state_events = pd.DataFrame(
        events,
        columns=["date", "code", "event_type", "exit_reason"],
    )
    if not state_events.empty:
        event_order = {"exit": 0, "entry": 1}
        state_events["_event_order"] = state_events["event_type"].map(event_order)
        state_events = (
            state_events.sort_values(["date", "code", "_event_order"], kind="stable")
            .drop(columns="_event_order")
            .reset_index(drop=True)
        )
    return PositionSignalFrames(
        close=close,
        entries=entries,
        exits=exits,
        held_intervals=held_intervals,
        state_events=state_events,
    )


def _build_code_position_state(
    code_frame: pd.DataFrame,
    *,
    code: str,
    ring_id: str,
    entry_rule_id: str,
    exit_rule_id: str,
    max_holding_sessions: int,
    entries: pd.DataFrame,
    exits: pd.DataFrame,
    held_intervals: pd.DataFrame,
    events: list[dict[str, object]],
) -> None:
    rows = cast(list[dict[str, Any]], code_frame.to_dict(orient="records"))
    finite_close_dates = [
        pd.Timestamp(row["date"])
        for row in rows
        if _numeric_value(row, "close") is not None
    ]
    if not finite_close_dates:
        return
    last_finite_close_date = finite_close_dates[-1]
    active = False
    held_sessions = 0
    previous_entry_eligibility = False

    for row in rows:
        date = pd.Timestamp(row["date"])
        has_close = _numeric_value(row, "close") is not None
        ring_member = _row_is_in_ring(row, ring_id)
        entry_eligible = (
            has_close and ring_member and entry_rule_matches(row, entry_rule_id)
        )

        if active:
            exit_reason = _exit_reason(
                row,
                ring_member=ring_member,
                exit_rule_id=exit_rule_id,
                held_sessions=held_sessions,
                max_holding_sessions=max_holding_sessions,
            )
            if has_close and exit_reason is not None:
                _emit_exit(
                    date,
                    code,
                    exit_reason,
                    exits=exits,
                    held_intervals=held_intervals,
                    events=events,
                )
                active = False
                held_sessions = 0
            elif has_close:
                held_intervals.loc[date, code] = True
                held_sessions += 1
        elif entry_eligible and not previous_entry_eligibility:
            if date != last_finite_close_date:
                entries.loc[date, code] = True
                events.append(
                    {
                        "date": date,
                        "code": code,
                        "event_type": "entry",
                        "exit_reason": None,
                    }
                )
                active = True
                held_sessions = 0

        previous_entry_eligibility = entry_eligible

    if active:
        _emit_exit(
            last_finite_close_date,
            code,
            "terminal_exit",
            exits=exits,
            held_intervals=held_intervals,
            events=events,
        )


def _exit_reason(
    row: Mapping[str, object],
    *,
    ring_member: bool,
    exit_rule_id: str,
    held_sessions: int,
    max_holding_sessions: int,
) -> str | None:
    if not ring_member:
        return "ring_exit"
    if exit_rule_matches(row, exit_rule_id):
        return "sma5_exit"
    if held_sessions >= max_holding_sessions - 1:
        return "time_exit"
    return None


def _emit_exit(
    date: pd.Timestamp,
    code: str,
    exit_reason: str,
    *,
    exits: pd.DataFrame,
    held_intervals: pd.DataFrame,
    events: list[dict[str, object]],
) -> None:
    exits.loc[date, code] = True
    held_intervals.loc[date, code] = True
    events.append(
        {
            "date": date,
            "code": code,
            "event_type": "exit",
            "exit_reason": exit_reason,
        }
    )


def _row_is_in_ring(row: Mapping[str, object], ring_id: str) -> bool:
    threshold = SCORE_RING_THRESHOLDS[ring_id]
    value = _numeric_value(row, _VALUE_SCORE_COLUMN)
    leadership = _numeric_value(row, _LEADERSHIP_SCORE_COLUMN)
    return value is not None and leadership is not None and value >= threshold and leadership >= threshold


def _numeric_value(row: Mapping[str, object], column: str) -> float | None:
    return _safe_finite_float_or_none(row.get(column))


def _safe_finite_float_or_none(value: object) -> float | None:
    try:
        return finite_float_or_none(value)
    except (TypeError, ValueError):
        return None


def _validate_arguments(
    feature_df: pd.DataFrame,
    *,
    ring_id: str,
    entry_rule_id: str,
    exit_rule_id: str,
    max_holding_sessions: int,
) -> None:
    if ring_id not in SCORE_RING_THRESHOLDS:
        raise ValueError(f"unknown score ring: {ring_id}")
    if entry_rule_id not in ENTRY_RULE_IDS:
        raise ValueError(f"unknown entry rule: {entry_rule_id}")
    if exit_rule_id not in EXIT_RULE_IDS:
        raise ValueError(f"unknown exit rule: {exit_rule_id}")
    if (
        isinstance(max_holding_sessions, bool)
        or not isinstance(max_holding_sessions, int)
        or max_holding_sessions <= 0
    ):
        raise ValueError("max_holding_sessions must be a positive integer")
    missing_columns = sorted(_REQUIRED_FEATURE_COLUMNS.difference(feature_df.columns))
    if missing_columns:
        raise ValueError(f"feature_df missing required columns: {', '.join(missing_columns)}")


def build_score_ring_feature_panel(
    conn: Any,
    relations: DailyRankingResearchRelations,
) -> pd.DataFrame:
    """Build the frozen score/SMA panel from canonical Daily Ranking builders.

    Scores are produced exclusively by the shared scaffold builders.  The two
    execution aliases below only express the SMA exit primitives in the units
    consumed by the frozen Task 1 state machine.
    """

    signal_source = relations.ranked_signals
    atr_features = build_atr_features(
        conn,
        AtrFeaturesRequest(source=signal_source, namespace="hard_filter_atr"),
    )
    short_features = build_short_scaffold_features(
        conn,
        ShortScaffoldFeaturesRequest(
            source=signal_source,
            atr_features=atr_features,
            namespace="hard_filter_short",
        ),
    )
    sector_features = build_sector_strength_features(
        conn,
        SectorStrengthFeaturesRequest(
            source=signal_source,
            population_source=signal_source,
            namespace="hard_filter_sector",
        ),
    )
    leadership_features = build_long_leadership_features(
        conn,
        LongLeadershipFeaturesRequest(
            source=signal_source,
            sector_features=sector_features,
            namespace="hard_filter_leadership",
            leadership_windows=_LEADERSHIP_WINDOWS,
        ),
    )
    sma_features = build_sma_features(
        conn,
        SmaFeaturesRequest(
            source=signal_source,
            price_history=relations.price_history,
            namespace="hard_filter_sma",
        ),
    )
    long_scaffold = build_long_scaffold_features(
        conn,
        LongScaffoldFeaturesRequest(
            source=signal_source,
            leadership_features=leadership_features,
            short_scaffold_features=short_features,
            namespace="hard_filter_long",
        ),
    )
    composed = compose_daily_ranking_signal_features(
        conn,
        source=signal_source,
        features=(long_scaffold, sma_features),
        namespace="sma5_score_ring_hard_filter",
    )
    panel_name = "ranking_sma5_score_ring_hard_filter_feature_panel"
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {panel_name} AS
        SELECT
            composed.*,
            CAST(
                CASE
                    WHEN composed.below_sma5_streak_ge3_flag THEN 3
                    WHEN composed.close_below_sma5_flag = 1 THEN 1
                    ELSE 0
                END AS INTEGER
            ) AS sma5_below_streak,
            CAST(
                CASE
                    WHEN composed.sma5 IS NOT NULL AND composed.atr20 > 0.0
                    THEN (composed.close - composed.sma5) / composed.atr20
                END AS DOUBLE
            ) AS sma5_atr20_deviation
        FROM {composed.name} AS composed
        """
    )
    panel = conn.execute(
        f"SELECT * FROM {panel_name} ORDER BY date, code"
    ).fetchdf()
    if "date" in panel.columns:
        panel["date"] = pd.to_datetime(panel["date"], errors="raise")
    return panel


def run_ranking_sma5_score_ring_hard_filter_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    bootstrap_resamples: int = 2_000,
    min_trades: int = 10,
    min_signal_dates: int = 10,
    observation_sample_limit: int = _DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingSma5ScoreRingHardFilterResearchResult:
    """Materialize the Market v5 score-ring feature panel with strict lineage."""

    _validate_research_parameters(
        bootstrap_resamples=bootstrap_resamples,
        min_trades=min_trades,
        min_signal_dates=min_signal_dates,
        observation_sample_limit=observation_sample_limit,
    )
    analysis_start = None if start_date is None else date.fromisoformat(start_date)
    analysis_end = None if end_date is None else date.fromisoformat(end_date)
    if analysis_start is not None and analysis_end is not None and analysis_start > analysis_end:
        raise ValueError("start_date must be on or before end_date")

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-sma5-score-ring-hard-filter-",
    ) as ctx:
        schema_version = require_market_v5_compatibility(
            ctx.connection,
            required_tables=_REQUIRED_MARKET_TABLES,
        )
        _assert_unambiguous_provider_adjusted_provenance(ctx.connection)
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="sma5_score_ring_hard_filter",
                analysis_start_date=analysis_start,
                analysis_end_date=analysis_end,
                horizons=(1,),
                market_scopes=("prime",),
                include_liquidity=True,
                percentile_features=(),
            ),
        )
        price_lineage = relations.lineage.price
        if (
            relations.lineage.verification_status != "verified"
            or price_lineage.verification_status != "verified"
            or not price_lineage.no_stock_data_fallback
        ):
            raise RuntimeError(
                "Market v5 price provenance is not verified; no stock_data fallback is allowed"
            )
        feature_df = build_score_ring_feature_panel(ctx.connection, relations)
        pit_lineage = HardFilterPitLineage(
            market_schema_version=schema_version,
            stock_price_adjustment_mode="provider_adjusted_v1",
            market_source=ctx.source_detail,
            source_mode=ctx.source_mode,
        )
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    effective_start_date, effective_end_date = _resolve_effective_analysis_dates(
        feature_df,
        requested_start=analysis_start,
        requested_end=analysis_end,
    )
    return RankingSma5ScoreRingHardFilterResearchResult(
        db_path=str(db_path_obj),
        source_mode=source_mode,
        source_detail=source_detail,
        analysis_start_date=effective_start_date,
        analysis_end_date=effective_end_date,
        bootstrap_resamples=int(bootstrap_resamples),
        min_trades=int(min_trades),
        min_signal_dates=int(min_signal_dates),
        pit_lineage=pit_lineage,
        feature_df=feature_df,
        observation_sample_df=feature_df.head(int(observation_sample_limit)).copy(),
    )


def execute_variant(
    feature_df: pd.DataFrame,
    variant: ResearchVariant,
    *,
    fee_bps: float,
    signal_frames: PositionSignalFrames | None = None,
) -> VariantExecution:
    """Execute one state-machine variant using VectorBT's authoritative ledger.

    Callers running multiple cost levels may supply the prebuilt state frames
    for this variant.  The returned evidence deliberately retains only compact
    tabular/series outputs after VectorBT accounting is extracted.
    """

    fee_bps_value = _safe_finite_float_or_none(fee_bps)
    if fee_bps_value is None or fee_bps_value < 0.0:
        raise ValueError("fee_bps must be a finite non-negative number")
    frames = signal_frames or build_position_signal_frames(
        feature_df,
        ring_id=variant.ring_id,
        entry_rule_id=variant.entry_rule_id,
        exit_rule_id=variant.exit_rule_id,
        max_holding_sessions=variant.max_holding_sessions,
    )
    portfolio = VectorbtAdapter(engine="numba").create_signal_portfolio(
        close=frames.close,
        entries=frames.entries,
        exits=frames.exits,
        direction="longonly",
        init_cash=1_000_000.0,
        fees=fee_bps_value / 20_000.0,
        slippage=0.0,
        cash_sharing=False,
        group_by=False,
        accumulate=False,
        size=1.0,
        size_type="percent",
        freq="D",
    )
    trade_records = _normalize_and_reconcile_trade_records(
        portfolio,
        frames.state_events,
    )
    daily_returns = _build_active_portfolio_returns(portfolio, frames)
    benchmark_returns = _build_benchmark_returns(feature_df, frames.close.index)
    return VariantExecution(
        variant=variant,
        fee_bps=fee_bps_value,
        trade_records_df=trade_records,
        daily_portfolio_returns=daily_returns,
        benchmark_daily_returns=benchmark_returns,
        state_events=frames.state_events.copy(),
    )


def _resolve_effective_analysis_dates(
    feature_df: pd.DataFrame,
    *,
    requested_start: date | None,
    requested_end: date | None,
) -> tuple[str, str]:
    if "date" not in feature_df.columns:
        raise RuntimeError("score-ring feature panel is missing date coverage")
    dates = pd.to_datetime(feature_df["date"], errors="coerce").dropna()
    if dates.empty:
        raise RuntimeError("score-ring feature panel has no effective date coverage")
    effective_start = dates.min().date()
    effective_end = dates.max().date()
    if requested_start is not None:
        effective_start = max(effective_start, requested_start)
    if requested_end is not None:
        effective_end = min(effective_end, requested_end)
    if effective_start > effective_end:
        raise RuntimeError("score-ring feature panel has no coverage within requested dates")
    return effective_start.isoformat(), effective_end.isoformat()


def _build_benchmark_returns(
    feature_df: pd.DataFrame,
    comparison_dates: pd.Index,
) -> pd.Series:
    normalized_dates = pd.DatetimeIndex(comparison_dates)
    benchmark = feature_df.loc[:, ["date", "topix_close"]].copy()
    benchmark["date"] = pd.to_datetime(benchmark["date"], errors="raise")
    benchmark["topix_close"] = pd.to_numeric(
        benchmark["topix_close"], errors="coerce"
    )
    invalid_close = (
        benchmark["topix_close"].isna()
        | ~np.isfinite(benchmark["topix_close"])
        | benchmark["topix_close"].le(0.0)
    )
    if invalid_close.any():
        raise ValueError("topix_close must be finite and strictly positive")
    if benchmark.groupby("date")["topix_close"].nunique(dropna=False).gt(1).any():
        raise ValueError("feature_df must contain one TOPIX close per date")
    topix_close = (
        benchmark.drop_duplicates("date")
        .set_index("date")["topix_close"]
        .reindex(normalized_dates)
    )
    if topix_close.isna().any():
        raise ValueError("feature_df must contain a finite TOPIX close for every date")
    returns = topix_close.pct_change(fill_method=None).fillna(0.0).astype(float)
    returns.name = "benchmark_return"
    return returns


def _validate_research_parameters(
    *,
    bootstrap_resamples: int,
    min_trades: int,
    min_signal_dates: int,
    observation_sample_limit: int,
) -> None:
    for name, value in (
        ("bootstrap_resamples", bootstrap_resamples),
        ("min_trades", min_trades),
        ("min_signal_dates", min_signal_dates),
        ("observation_sample_limit", observation_sample_limit),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")


def _assert_unambiguous_provider_adjusted_provenance(conn: Any) -> None:
    rows = conn.execute(
        "SELECT DISTINCT value FROM sync_metadata "
        "WHERE key = 'stock_price_adjustment_mode'"
    ).fetchall()
    modes = {str(row[0]) for row in rows if row and row[0] is not None}
    if modes != {"provider_adjusted_v1"}:
        observed = ", ".join(sorted(modes)) if modes else "missing"
        raise RuntimeError(
            "Incompatible market.duckdb metadata: required "
            "stock_price_adjustment_mode=provider_adjusted_v1; observed "
            f"{observed}"
        )


def _normalize_and_reconcile_trade_records(
    portfolio: Any,
    state_events: pd.DataFrame,
) -> pd.DataFrame:
    records_readable = getattr(portfolio.trades, "records_readable", None)
    if records_readable is None:
        raise RuntimeError("VectorBT trade records_readable is unavailable")
    records = pd.DataFrame(records_readable).copy().reset_index(drop=True)
    required_columns = {"Column", "Entry Timestamp", "Exit Timestamp"}
    missing = sorted(required_columns.difference(records.columns))
    if missing:
        raise RuntimeError(
            "VectorBT trade ledger is missing required columns: " + ", ".join(missing)
        )
    if "Status" in records.columns and not records["Status"].eq("Closed").all():
        raise RuntimeError("VectorBT trade ledger contains an unclosed state-machine trade")

    entries = state_events.loc[state_events["event_type"].eq("entry")]
    exits = state_events.loc[state_events["event_type"].eq("exit")]
    if len(entries) != len(exits) or len(entries) != len(records):
        raise RuntimeError(
            "VectorBT trade ledger does not reconcile to the state-event pair count"
        )
    expected_entries = {
        (str(row.code), pd.Timestamp(str(row.date)))
        for row in entries.itertuples(index=False)
    }
    expected_exits = {
        (str(row.code), pd.Timestamp(str(row.date)))
        for row in exits.itertuples(index=False)
    }
    observed_entries = {
        (str(row["Column"]), pd.Timestamp(str(row["Entry Timestamp"])))
        for _, row in records.iterrows()
    }
    observed_exits = {
        (str(row["Column"]), pd.Timestamp(str(row["Exit Timestamp"])))
        for _, row in records.iterrows()
    }
    if observed_entries != expected_entries or observed_exits != expected_exits:
        raise RuntimeError(
            "VectorBT fills do not reconcile to the independently generated state events"
        )
    records["Column"] = records["Column"].astype(str)
    records["Entry Timestamp"] = pd.to_datetime(records["Entry Timestamp"], errors="raise")
    records["Exit Timestamp"] = pd.to_datetime(records["Exit Timestamp"], errors="raise")
    records["code"] = records["Column"]
    records["entry_date"] = records["Entry Timestamp"]
    records["exit_date"] = records["Exit Timestamp"]
    return records


def _build_active_portfolio_returns(
    portfolio: Any,
    frames: PositionSignalFrames,
) -> pd.Series:
    """Average only held returns, plus entry-fill fees booked by VectorBT."""

    raw_returns = pd.DataFrame(portfolio.returns()).reindex(
        index=frames.close.index,
        columns=frames.close.columns,
    )
    held_returns = raw_returns.where(frames.held_intervals)
    held_count = frames.held_intervals.sum(axis=1).astype(float)
    held_mean = held_returns.sum(axis=1, min_count=1).div(held_count.where(held_count > 0))

    entry_fee_returns = raw_returns.where(frames.entries)
    entry_count = frames.entries.sum(axis=1).astype(float)
    entry_fee_adjustment = entry_fee_returns.sum(axis=1, min_count=1).fillna(0.0).div(
        (held_count + entry_count).where((held_count + entry_count) > 0)
    ).fillna(0.0)
    daily_returns = held_mean.fillna(0.0) + entry_fee_adjustment
    daily_returns.name = "portfolio_return"
    return daily_returns.astype(float)
