#!/usr/bin/env python3
"""Runner-first entrypoint for falling-knife bad-tail pruning research."""

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
from src.domains.analytics.falling_knife_bad_tail_pruning import (  # noqa: E402
    DEFAULT_HORIZON_DAYS,
    DEFAULT_SEVERE_LOSS_THRESHOLD,
    run_falling_knife_bad_tail_pruning,
    write_falling_knife_bad_tail_pruning_bundle,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate falling-knife exclusion rules that try to preserve rebound "
            "returns while reducing severe-loss frequency."
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
        "--severe-loss-threshold",
        type=float,
        default=DEFAULT_SEVERE_LOSS_THRESHOLD,
        help=(
            "Trade return threshold treated as severe loss. "
            f"Default: {DEFAULT_SEVERE_LOSS_THRESHOLD}."
        ),
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_falling_knife_bad_tail_pruning(
        args.input_bundle,
        output_root=args.output_root,
        horizon_days=args.horizon_days,
        severe_loss_threshold=args.severe_loss_threshold,
    )
    bundle = write_falling_knife_bad_tail_pruning_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
