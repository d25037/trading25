#!/usr/bin/env python3
"""Runner-first entrypoint for range-break trade-archetype decomposition."""

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
from src.domains.analytics.range_break_trade_archetype_decomposition import (  # noqa: E402
    DEFAULT_DATASET_NAME,
    DEFAULT_HOLDOUT_MONTHS,
    DEFAULT_QUANTILE_BUCKET_COUNT,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    DEFAULT_STRATEGY_NAME,
    run_range_break_trade_archetype_decomposition,
    write_range_break_trade_archetype_decomposition_bundle,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Decompose range_break production trades into entry-time breakout, "
            "overheat, liquidity, beta, market-regime, and value-factor buckets."
        )
    )
    parser.add_argument(
        "--strategy",
        default=DEFAULT_STRATEGY_NAME,
        help=f"Strategy name to analyze. Defaults to {DEFAULT_STRATEGY_NAME}.",
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET_NAME,
        help=f"Dataset or market universe preset to analyze. Defaults to {DEFAULT_DATASET_NAME}.",
    )
    parser.add_argument(
        "--holdout-months",
        type=int,
        default=DEFAULT_HOLDOUT_MONTHS,
        help="Recent holdout window size in calendar months.",
    )
    parser.add_argument(
        "--severe-loss-threshold-pct",
        type=float,
        default=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
        help="Trade-return threshold used to mark severe losses.",
    )
    parser.add_argument(
        "--quantile-buckets",
        type=int,
        default=DEFAULT_QUANTILE_BUCKET_COUNT,
        help="Number of quantile buckets used in feature summaries.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_range_break_trade_archetype_decomposition(
        strategy_name=args.strategy,
        dataset_name=args.dataset,
        holdout_months=args.holdout_months,
        severe_loss_threshold_pct=args.severe_loss_threshold_pct,
        quantile_bucket_count=args.quantile_buckets,
    )
    bundle = write_range_break_trade_archetype_decomposition_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
