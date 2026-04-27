#!/usr/bin/env python3
"""Runner-first entrypoint for falling-knife non-rebound fundamentals."""

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
from src.domains.analytics.falling_knife_fundamental_quality_pruning import (  # noqa: E402
    DEFAULT_HORIZON_DAYS,
    DEFAULT_MIN_QUALITY_SCORE,
    DEFAULT_SEVERE_LOSS_THRESHOLD,
)
from src.domains.analytics.falling_knife_non_rebound_fundamental_profile import (  # noqa: E402
    DEFAULT_REBOUND_THRESHOLD,
    run_falling_knife_non_rebound_fundamental_profile,
    write_falling_knife_non_rebound_fundamental_profile_bundle,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Profile PIT-safe fundamental features of falling-knife events that "
            "do not rebound."
        )
    )
    parser.add_argument(
        "--input-bundle",
        default=None,
        help=(
            "Path to a falling-knife reversal study bundle. Defaults to the latest "
            "bundle under the output root."
        ),
    )
    parser.add_argument(
        "--horizon-days",
        type=int,
        default=DEFAULT_HORIZON_DAYS,
        help=f"Catch-return horizon to evaluate. Default: {DEFAULT_HORIZON_DAYS}.",
    )
    parser.add_argument(
        "--rebound-threshold",
        type=float,
        default=DEFAULT_REBOUND_THRESHOLD,
        help=(
            "Return threshold. Events with catch return <= this value are "
            f"non-rebound. Default: {DEFAULT_REBOUND_THRESHOLD}."
        ),
    )
    parser.add_argument(
        "--severe-loss-threshold",
        type=float,
        default=DEFAULT_SEVERE_LOSS_THRESHOLD,
        help=(
            "Trade return threshold retained as a secondary tail diagnostic. "
            f"Default: {DEFAULT_SEVERE_LOSS_THRESHOLD}."
        ),
    )
    parser.add_argument(
        "--min-quality-score",
        type=int,
        default=DEFAULT_MIN_QUALITY_SCORE,
        help=(
            "Minimum score across forecast/profit/CFO/FCF/equity-ratio flags "
            f"for high_quality. Default: {DEFAULT_MIN_QUALITY_SCORE}."
        ),
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_falling_knife_non_rebound_fundamental_profile(
        args.input_bundle,
        output_root=args.output_root,
        horizon_days=args.horizon_days,
        rebound_threshold=args.rebound_threshold,
        severe_loss_threshold=args.severe_loss_threshold,
        min_quality_score=args.min_quality_score,
    )
    bundle = write_falling_knife_non_rebound_fundamental_profile_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
