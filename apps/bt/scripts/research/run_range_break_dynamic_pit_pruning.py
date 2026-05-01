#!/usr/bin/env python3
"""Runner-first entrypoint for dynamic-PIT range-break pruning research."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


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
from src.domains.analytics.range_break_dynamic_pit_pruning import (  # noqa: E402
    DEFAULT_ALLOCATION_PCT,
    DEFAULT_COMPARISON_RESULT_STEM,
    DEFAULT_DISCOVERY_END_DATE,
    DEFAULT_RESULT_STEM,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    DEFAULT_STRATEGY_NAME,
    DEFAULT_TARGET_MAX_TRADES,
    DEFAULT_TARGET_MIN_TRADES,
    run_range_break_dynamic_pit_pruning,
    write_range_break_dynamic_pit_pruning_bundle,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze the dynamic-PIT range_break_v15 result and rank PIT-safe "
            "trade-pruning candidates around a target trade-count band."
        )
    )
    parser.add_argument(
        "--strategy",
        default=DEFAULT_STRATEGY_NAME,
        help=f"Backtest result strategy directory. Defaults to {DEFAULT_STRATEGY_NAME}.",
    )
    parser.add_argument(
        "--result-stem",
        default=DEFAULT_RESULT_STEM,
        help=f"Source backtest result stem. Defaults to {DEFAULT_RESULT_STEM}.",
    )
    parser.add_argument(
        "--comparison-result-stem",
        default=DEFAULT_COMPARISON_RESULT_STEM,
        help=(
            "Static-universe comparison result stem used to label dynamic-new trades. "
            f"Defaults to {DEFAULT_COMPARISON_RESULT_STEM}."
        ),
    )
    parser.add_argument(
        "--target-min-trades",
        type=int,
        default=DEFAULT_TARGET_MIN_TRADES,
        help="Lower bound for the desired full-period trade count.",
    )
    parser.add_argument(
        "--target-max-trades",
        type=int,
        default=DEFAULT_TARGET_MAX_TRADES,
        help="Upper bound for the desired full-period trade count.",
    )
    parser.add_argument(
        "--discovery-end-date",
        default=DEFAULT_DISCOVERY_END_DATE,
        help="Last date used to learn pruning thresholds.",
    )
    parser.add_argument(
        "--severe-loss-threshold-pct",
        type=float,
        default=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
        help="Trade-return threshold used to mark severe losses.",
    )
    parser.add_argument(
        "--allocation-pct",
        type=float,
        default=DEFAULT_ALLOCATION_PCT,
        help="Per-trade allocation used for approximate gross exposure diagnostics.",
    )
    parser.add_argument(
        "--skip-enrichment",
        action="store_true",
        help="Skip market/fundamental feature enrichment and only summarize report trades.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_range_break_dynamic_pit_pruning(
        strategy_name=args.strategy,
        result_stem=args.result_stem,
        comparison_result_stem=args.comparison_result_stem,
        target_min_trades=args.target_min_trades,
        target_max_trades=args.target_max_trades,
        discovery_end_date=args.discovery_end_date,
        severe_loss_threshold_pct=args.severe_loss_threshold_pct,
        allocation_pct=args.allocation_pct,
        enrich_features=not args.skip_enrichment,
    )
    bundle = write_range_break_dynamic_pit_pruning_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
