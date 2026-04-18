#!/usr/bin/env python3
"""Runner-first entrypoint for the production strategy robustness audit."""

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
from src.domains.analytics.production_strategy_robustness_audit import (  # noqa: E402
    DEFAULT_DATASET_NAMES,
    DEFAULT_STRATEGY_NAMES,
    run_production_strategy_robustness_audit,
    write_production_strategy_robustness_audit_bundle,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a reproducible robustness audit for production strategies across "
            "full-history and recent holdout windows."
        )
    )
    parser.add_argument(
        "--strategy",
        action="append",
        dest="strategies",
        default=None,
        help=(
            "Strategy name to audit. Repeat to override defaults "
            f"({', '.join(DEFAULT_STRATEGY_NAMES)})."
        ),
    )
    parser.add_argument(
        "--dataset",
        action="append",
        dest="datasets",
        default=None,
        help=(
            "Dataset name to audit. Repeat to override defaults "
            f"({', '.join(DEFAULT_DATASET_NAMES)})."
        ),
    )
    parser.add_argument(
        "--holdout-months",
        type=int,
        default=6,
        help="Recent holdout window size in calendar months.",
    )
    parser.add_argument(
        "--no-reference-buy-and-hold",
        action="store_true",
        help="Skip the same-universe buy-and-hold reference run.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_production_strategy_robustness_audit(
        strategy_names=tuple(args.strategies or DEFAULT_STRATEGY_NAMES),
        dataset_names=tuple(args.datasets or DEFAULT_DATASET_NAMES),
        holdout_months=args.holdout_months,
        include_reference_buy_and_hold=not args.no_reference_buy_and_hold,
    )
    bundle = write_production_strategy_robustness_audit_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
