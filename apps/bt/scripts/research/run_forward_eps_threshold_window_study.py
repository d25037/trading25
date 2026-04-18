#!/usr/bin/env python3
"""Runner-first entrypoint for the forward EPS threshold window study."""

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
from src.domains.analytics.forward_eps_threshold_window_study import (  # noqa: E402
    DEFAULT_BASELINE_STRATEGY_NAME,
    DEFAULT_DATASET_NAME,
    DEFAULT_STRATEGY_NAMES,
    run_forward_eps_threshold_window_study,
    write_forward_eps_threshold_window_study_bundle,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a rolling-window threshold sweep for forward_eps_driven "
            "experimental clones."
        )
    )
    parser.add_argument(
        "--strategy",
        action="append",
        dest="strategies",
        default=None,
        help=(
            "Strategy name to study. Repeat to override defaults "
            f"({', '.join(DEFAULT_STRATEGY_NAMES)})."
        ),
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET_NAME,
        help=f"Dataset name to study. Defaults to {DEFAULT_DATASET_NAME}.",
    )
    parser.add_argument(
        "--baseline-strategy",
        default=DEFAULT_BASELINE_STRATEGY_NAME,
        help=(
            "Baseline strategy name used for return and drawdown deltas. "
            f"Defaults to {DEFAULT_BASELINE_STRATEGY_NAME}."
        ),
    )
    parser.add_argument(
        "--rolling-months",
        type=int,
        default=6,
        help="Rolling window size in calendar months.",
    )
    parser.add_argument(
        "--step-months",
        type=int,
        default=1,
        help="Rolling window step size in calendar months.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_forward_eps_threshold_window_study(
        strategy_names=tuple(args.strategies or DEFAULT_STRATEGY_NAMES),
        dataset_name=args.dataset,
        baseline_strategy_name=args.baseline_strategy,
        rolling_months=args.rolling_months,
        rolling_step_months=args.step_months,
    )
    bundle = write_forward_eps_threshold_window_study_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
