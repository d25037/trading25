#!/usr/bin/env python3
"""Runner-first entrypoint for annual value-composite technical filter research."""

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
from src.domains.analytics.annual_value_composite_technical_filter import (  # noqa: E402
    DEFAULT_NEAR_SMA_THRESHOLD,
    DEFAULT_SLOPE_WINDOW,
    DEFAULT_SMA_WINDOW,
    run_annual_value_composite_technical_filter,
    write_annual_value_composite_technical_filter_bundle,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate entry previous-session SMA250/TOPIX trend filters on annual "
            "value-composite selection portfolios."
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
        "--sma-window",
        type=int,
        default=DEFAULT_SMA_WINDOW,
        help=f"SMA lookback session count. Default: {DEFAULT_SMA_WINDOW}.",
    )
    parser.add_argument(
        "--slope-window",
        type=int,
        default=DEFAULT_SLOPE_WINDOW,
        help=f"SMA slope lookback session count. Default: {DEFAULT_SLOPE_WINDOW}.",
    )
    parser.add_argument(
        "--near-sma-threshold",
        type=float,
        default=DEFAULT_NEAR_SMA_THRESHOLD,
        help=(
            "Price/SMA threshold for the near-SMA recovery branch. "
            f"Default: {DEFAULT_NEAR_SMA_THRESHOLD}."
        ),
    )
    parser.add_argument(
        "--focus-standard-top10-no-liquidity",
        action="store_true",
        help=(
            "Limit input rows to standard / no-liquidity / top-10%% / main score methods. "
            "Use this for fast practical hypothesis checks."
        ),
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_annual_value_composite_technical_filter(
        args.input_bundle,
        output_root=args.output_root,
        sma_window=args.sma_window,
        slope_window=args.slope_window,
        near_sma_threshold=args.near_sma_threshold,
        focus_standard_top10_no_liquidity=args.focus_standard_top10_no_liquidity,
    )
    bundle = write_annual_value_composite_technical_filter_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
