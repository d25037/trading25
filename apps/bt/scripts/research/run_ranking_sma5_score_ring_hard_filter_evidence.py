#!/usr/bin/env python3
"""Runner-first entrypoint for SMA5 score-ring hard-filter evidence."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable
from pathlib import Path

import pandas as pd


def _ensure_bt_root_on_path() -> Path:
    bt_root = Path(__file__).resolve().parents[2]
    bt_root_str = str(bt_root)
    if bt_root_str not in sys.path:
        sys.path.insert(0, bt_root_str)
    return bt_root


_BT_ROOT = _ensure_bt_root_on_path()

from scripts.research.common import (  # noqa: E402
    add_bundle_output_arguments,
    emit_bundle_payload,
    ensure_bt_workdir,
)
from src.domains.analytics.ranking_sma5_score_ring_hard_filter_evidence import (  # noqa: E402
    ENTRY_RULE_IDS,
    EXIT_RULE_IDS,
    SCORE_RING_THRESHOLDS,
    RankingSma5ScoreRingHardFilterResearchResult,
    ResearchVariant,
    VariantExecution,
    build_decision_gate_df,
    build_evidence_tables,
    execute_variant,
    run_ranking_sma5_score_ring_hard_filter_research,
)
from src.domains.analytics.research_bundle import (  # noqa: E402
    ResearchBundleInfo,
    write_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


EXPERIMENT_ID = "market-behavior/ranking-sma5-score-ring-hard-filter-evidence"
DEFAULT_START_DATE = "2018-01-01"
DEFAULT_BOOTSTRAP_BLOCK_LENGTH = 20
DEFAULT_BOOTSTRAP_RESAMPLES = 2_000
DEFAULT_BOOTSTRAP_SEED = 20260724
DEFAULT_MIN_TRADES = 200
DEFAULT_MIN_SIGNAL_DATES = 100
DEFAULT_COST_LEVELS = (0.0, 10.0, 20.0)
PRIMARY_HOLDING_CAP = 60
ROBUSTNESS_HOLDING_CAP = 20

_RESULT_TABLE_NAMES = (
    "rule_registry_df",
    "coverage_diagnostics_df",
    "trade_ledger_df",
    "portfolio_daily_df",
    "entry_rule_evidence_df",
    "exit_rule_evidence_df",
    "combined_rule_evidence_df",
    "annual_stability_df",
    "bootstrap_effect_ci_df",
    "cost_sensitivity_df",
    "decision_gate_df",
    "observation_sample_df",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Daily Ranking score-ring SMA5 entry/exit hard-filter evidence "
            "on Market v5 provider-adjusted prices."
        )
    )
    parser.add_argument(
        "--db-path",
        default=get_settings().market_db_path,
        help="Path to market.duckdb. Defaults to the active app setting.",
    )
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help="Analysis start date (YYYY-MM-DD).",
    )
    parser.add_argument("--end-date", default=None, help="Analysis end date (YYYY-MM-DD).")
    parser.add_argument(
        "--bootstrap-block-length",
        type=int,
        default=DEFAULT_BOOTSTRAP_BLOCK_LENGTH,
        help="Circular moving-block bootstrap length in trading sessions.",
    )
    parser.add_argument(
        "--bootstrap-resamples",
        type=int,
        default=DEFAULT_BOOTSTRAP_RESAMPLES,
        help="Number of paired moving-block bootstrap resamples.",
    )
    parser.add_argument(
        "--bootstrap-seed",
        type=int,
        default=DEFAULT_BOOTSTRAP_SEED,
        help="Fixed seed for paired moving-block bootstrap resampling.",
    )
    parser.add_argument(
        "--min-trades",
        type=int,
        default=DEFAULT_MIN_TRADES,
        help="Minimum OOS closed-trade coverage recorded for this run.",
    )
    parser.add_argument(
        "--min-signal-dates",
        type=int,
        default=DEFAULT_MIN_SIGNAL_DATES,
        help="Minimum OOS signal-date coverage recorded for this run.",
    )
    parser.add_argument(
        "--cost-levels",
        default="0,10,20",
        help="Required comma-separated round-trip cost levels in bps: 0,10,20.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def write_ranking_sma5_score_ring_hard_filter_bundle(
    result: RankingSma5ScoreRingHardFilterResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
    block_length: int = DEFAULT_BOOTSTRAP_BLOCK_LENGTH,
    resamples: int | None = None,
    seed: int = DEFAULT_BOOTSTRAP_SEED,
    cost_levels: Iterable[float] = DEFAULT_COST_LEVELS,
) -> ResearchBundleInfo:
    """Execute frozen variants and write the canonical twelve-table bundle."""

    resolved_cost_levels = _require_approved_cost_levels(cost_levels)
    resolved_resamples = result.bootstrap_resamples if resamples is None else resamples
    executions = _execute_frozen_variants(
        result.feature_df,
        cost_levels=resolved_cost_levels,
    )
    evidence = build_evidence_tables(
        executions,
        block_length=block_length,
        resamples=resolved_resamples,
        seed=seed,
    )
    evidence_frames = (
        evidence.entry_rule_evidence_df,
        evidence.exit_rule_evidence_df,
        evidence.combined_rule_evidence_df,
    )
    decision_gate_df = build_decision_gate_df(
        _concat_evidence_frames(evidence_frames),
        evidence.annual_stability_df,
        evidence.cost_sensitivity_df,
    )
    result_tables = {
        "rule_registry_df": evidence.rule_registry_df,
        "coverage_diagnostics_df": evidence.coverage_diagnostics_df,
        "trade_ledger_df": evidence.trade_ledger_df,
        "portfolio_daily_df": evidence.portfolio_daily_df,
        "entry_rule_evidence_df": evidence.entry_rule_evidence_df,
        "exit_rule_evidence_df": evidence.exit_rule_evidence_df,
        "combined_rule_evidence_df": evidence.combined_rule_evidence_df,
        "annual_stability_df": evidence.annual_stability_df,
        "bootstrap_effect_ci_df": evidence.bootstrap_effect_ci_df,
        "cost_sensitivity_df": evidence.cost_sensitivity_df,
        "decision_gate_df": decision_gate_df,
        "observation_sample_df": result.observation_sample_df,
    }
    if tuple(result_tables) != _RESULT_TABLE_NAMES:
        raise RuntimeError("hard-filter bundle table contract drifted")
    return write_research_bundle(
        experiment_id=EXPERIMENT_ID,
        module="scripts.research.run_ranking_sma5_score_ring_hard_filter_evidence",
        function="run_ranking_sma5_score_ring_hard_filter_research",
        params={
            "bootstrap_block_length": block_length,
            "bootstrap_resamples": resolved_resamples,
            "bootstrap_seed": seed,
            "min_trades": result.min_trades,
            "min_signal_dates": result.min_signal_dates,
            "cost_levels_bps": list(resolved_cost_levels),
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "execution_policy": "close_proxy_same_session",
            "execution_is_optimistic": True,
            "stock_price_adjustment_mode": "provider_adjusted_v1",
            "primary_ring": "core_high_high",
            "primary_holding_cap": PRIMARY_HOLDING_CAP,
            "robustness_holding_cap": ROBUSTNESS_HOLDING_CAP,
            "discovery_period": ["2018-01-01", "2021-12-31"],
            "validation_period": ["2022-01-01", "2024-12-31"],
            "holdout_period": ["2025-01-01", result.analysis_end_date],
        },
        result_tables=result_tables,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    cost_levels = _parse_cost_levels(args.cost_levels)
    result = run_ranking_sma5_score_ring_hard_filter_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        bootstrap_resamples=args.bootstrap_resamples,
        min_trades=args.min_trades,
        min_signal_dates=args.min_signal_dates,
    )
    bundle = write_ranking_sma5_score_ring_hard_filter_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
        block_length=args.bootstrap_block_length,
        resamples=args.bootstrap_resamples,
        seed=args.bootstrap_seed,
        cost_levels=cost_levels,
    )
    emit_bundle_payload(bundle)
    return 0


def _execute_frozen_variants(
    feature_df: pd.DataFrame,
    *,
    cost_levels: tuple[float, ...],
) -> list[VariantExecution]:
    variants = _frozen_variants()
    return [
        execute_variant(feature_df, variant, fee_bps=fee_bps)
        for fee_bps in cost_levels
        for variant in variants
    ]


def _frozen_variants() -> tuple[ResearchVariant, ...]:
    variants: list[ResearchVariant] = []
    for ring_id in SCORE_RING_THRESHOLDS:
        for cap in (PRIMARY_HOLDING_CAP, ROBUSTNESS_HOLDING_CAP):
            variants.append(
                ResearchVariant(
                    ring_id=ring_id,
                    entry_rule_id=ENTRY_RULE_IDS[0],
                    exit_rule_id=EXIT_RULE_IDS[0],
                    max_holding_sessions=cap,
                )
            )
            variants.extend(
                ResearchVariant(
                    ring_id=ring_id,
                    entry_rule_id=entry_rule_id,
                    exit_rule_id=EXIT_RULE_IDS[0],
                    max_holding_sessions=cap,
                )
                for entry_rule_id in ENTRY_RULE_IDS[1:]
            )
            variants.extend(
                ResearchVariant(
                    ring_id=ring_id,
                    entry_rule_id=ENTRY_RULE_IDS[0],
                    exit_rule_id=exit_rule_id,
                    max_holding_sessions=cap,
                )
                for exit_rule_id in EXIT_RULE_IDS[1:]
            )
            variants.extend(
                ResearchVariant(
                    ring_id=ring_id,
                    entry_rule_id=entry_rule_id,
                    exit_rule_id=exit_rule_id,
                    max_holding_sessions=cap,
                )
                for entry_rule_id in ENTRY_RULE_IDS[1:]
                for exit_rule_id in EXIT_RULE_IDS[1:]
            )
    return tuple(variants)


def _parse_cost_levels(value: str) -> tuple[float, ...]:
    try:
        levels = tuple(float(part.strip()) for part in value.split(",") if part.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError("cost-levels must be numeric") from exc
    return _require_approved_cost_levels(levels)


def _require_approved_cost_levels(cost_levels: Iterable[float]) -> tuple[float, ...]:
    levels = tuple(float(cost) for cost in cost_levels)
    if set(levels) != set(DEFAULT_COST_LEVELS) or len(levels) != len(DEFAULT_COST_LEVELS):
        raise ValueError("cost-levels must contain exactly 0,10,20 bps")
    return tuple(cost for cost in DEFAULT_COST_LEVELS)


def _concat_evidence_frames(frames: tuple[pd.DataFrame, ...]) -> pd.DataFrame:
    return pd.concat(frames, ignore_index=True)


def _build_summary_markdown(
    result: RankingSma5ScoreRingHardFilterResearchResult,
) -> str:
    return "\n".join(
        [
            "# Ranking SMA5 Score-Ring Hard-Filter Evidence",
            "",
            "## Metadata",
            "",
            f"- db_path: `{result.db_path}`",
            f"- analysis_start_date: `{result.analysis_start_date}`",
            f"- analysis_end_date: `{result.analysis_end_date}`",
            "- execution_policy: `close_proxy_same_session`",
            "- execution_is_optimistic: `true`",
            "- stock_price_adjustment_mode: `provider_adjusted_v1`",
            "",
            "## Caveat",
            "",
            "Signal and fill both use the same Close; this is an optimistic execution proxy.",
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
