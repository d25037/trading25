#!/usr/bin/env python3
"""Runner-first entrypoint for annual value technical feature importance research."""

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
from src.domains.analytics.annual_value_technical_feature_importance import (  # noqa: E402
    DEFAULT_BUCKET_COUNT,
    DEFAULT_FOCUS_LIQUIDITY_SCENARIO,
    DEFAULT_FOCUS_MARKET_SCOPE,
    DEFAULT_FOCUS_SELECTION_FRACTION,
    DEFAULT_FOCUS_SCORE_METHODS,
    run_annual_value_technical_feature_importance,
    write_annual_value_technical_feature_importance_bundle,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Rank entry previous-session technical features for annual value-composite "
            "stock selection, using bucket, residual-correlation, and walk-forward overlays."
        )
    )
    parser.add_argument(
        "--input-bundle",
        default=None,
        help=(
            "Annual value-composite selection bundle path. Defaults to the latest "
            "annual-value-composite-selection bundle under the output root."
        ),
    )
    parser.add_argument(
        "--bucket-count",
        type=int,
        default=DEFAULT_BUCKET_COUNT,
        help=f"Cross-sectional buckets per year for feature ranking. Default: {DEFAULT_BUCKET_COUNT}.",
    )
    parser.add_argument(
        "--focus-market-scope",
        default=DEFAULT_FOCUS_MARKET_SCOPE,
        help=f"Value selection market scope to analyze. Default: {DEFAULT_FOCUS_MARKET_SCOPE}.",
    )
    parser.add_argument(
        "--focus-selection-fraction",
        type=float,
        default=DEFAULT_FOCUS_SELECTION_FRACTION,
        help=f"Value selection top fraction to analyze. Default: {DEFAULT_FOCUS_SELECTION_FRACTION}.",
    )
    parser.add_argument(
        "--focus-liquidity-scenario",
        default=DEFAULT_FOCUS_LIQUIDITY_SCENARIO,
        help=f"Value selection liquidity scenario to analyze. Default: {DEFAULT_FOCUS_LIQUIDITY_SCENARIO}.",
    )
    parser.add_argument(
        "--focus-score-method",
        action="append",
        dest="focus_score_methods",
        default=None,
        help=(
            "Score method to include. Can be passed multiple times. "
            f"Default: {', '.join(DEFAULT_FOCUS_SCORE_METHODS)}. fixed_55_25_20 is ignored if passed."
        ),
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_annual_value_technical_feature_importance(
        args.input_bundle,
        output_root=args.output_root,
        bucket_count=args.bucket_count,
        focus_market_scope=args.focus_market_scope,
        focus_selection_fraction=args.focus_selection_fraction,
        focus_liquidity_scenario=args.focus_liquidity_scenario,
        focus_score_methods=args.focus_score_methods or DEFAULT_FOCUS_SCORE_METHODS,
    )
    bundle = write_annual_value_technical_feature_importance_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
