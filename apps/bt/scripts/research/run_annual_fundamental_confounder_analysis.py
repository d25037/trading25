#!/usr/bin/env python3
"""Runner-first entrypoint for annual fundamental confounder analysis."""

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
from src.domains.analytics.annual_fundamental_confounder_analysis import (  # noqa: E402
    DEFAULT_MIN_OBSERVATIONS,
    DEFAULT_WINSOR_LOWER,
    DEFAULT_WINSOR_UPPER,
    POSITIVE_RATIO_ONLY_COLUMNS,
    run_annual_fundamental_confounder_analysis,
    write_annual_fundamental_confounder_analysis_bundle,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze confounding and independent factor effects in the annual "
            "first-open last-close fundamental panel."
        )
    )
    parser.add_argument(
        "--input-bundle",
        default=None,
        help=(
            "Annual fundamental panel bundle path. Defaults to the latest "
            "annual-first-open-last-close-fundamental-panel bundle under the output root."
        ),
    )
    parser.add_argument(
        "--winsor-lower",
        type=float,
        default=DEFAULT_WINSOR_LOWER,
        help=f"Lower winsorization quantile for event returns. Default: {DEFAULT_WINSOR_LOWER}.",
    )
    parser.add_argument(
        "--winsor-upper",
        type=float,
        default=DEFAULT_WINSOR_UPPER,
        help=f"Upper winsorization quantile for event returns. Default: {DEFAULT_WINSOR_UPPER}.",
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=DEFAULT_MIN_OBSERVATIONS,
        help=f"Minimum observations for regression/spread cells. Default: {DEFAULT_MIN_OBSERVATIONS}.",
    )
    parser.add_argument(
        "--require-positive-pbr-and-forward-per",
        action="store_true",
        help=(
            "Filter realized events to rows where both PBR and forward PER are "
            f"strictly positive ({', '.join(POSITIVE_RATIO_ONLY_COLUMNS)})."
        ),
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_annual_fundamental_confounder_analysis(
        args.input_bundle,
        output_root=args.output_root,
        winsor_lower=args.winsor_lower,
        winsor_upper=args.winsor_upper,
        min_observations=args.min_observations,
        required_positive_columns=(
            POSITIVE_RATIO_ONLY_COLUMNS if args.require_positive_pbr_and_forward_per else ()
        ),
    )
    bundle = write_annual_fundamental_confounder_analysis_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
