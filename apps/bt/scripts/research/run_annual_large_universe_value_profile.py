#!/usr/bin/env python3
"""Runner-first entrypoint for annual large-universe value profile research."""

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
    DEFAULT_WINSOR_LOWER,
    DEFAULT_WINSOR_UPPER,
    POSITIVE_RATIO_ONLY_COLUMNS,
)
from src.domains.analytics.annual_large_universe_value_profile import (  # noqa: E402
    DEFAULT_MIN_OBSERVATIONS,
    DEFAULT_SELECTION_FRACTIONS,
    run_annual_large_universe_value_profile,
    write_annual_large_universe_value_profile_bundle,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Restrict the annual value panel to TOPIX100/TOPIX500 scale-category "
            "universes and compare low-PBR, small-cap, and low-forward-PER balance."
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
        "--db-path",
        default=None,
        help=(
            "Optional market.duckdb path for daily portfolio curves. Defaults to "
            "the db_path recorded in the input annual panel bundle."
        ),
    )
    parser.add_argument(
        "--selection-fraction",
        action="append",
        type=float,
        dest="selection_fractions",
        default=None,
        help=(
            "Top fraction to select within each year x large universe. Repeat to override "
            f"defaults ({', '.join(str(value) for value in DEFAULT_SELECTION_FRACTIONS)})."
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
        "--include-non-positive-ratios",
        action="store_true",
        help=(
            "Do not require PBR and forward PER to be strictly positive. The default "
            f"matches practical annual value reruns: {', '.join(POSITIVE_RATIO_ONLY_COLUMNS)} > 0."
        ),
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=DEFAULT_MIN_OBSERVATIONS,
        help=(
            "Minimum observations for regression and bucket readouts. "
            f"Default: {DEFAULT_MIN_OBSERVATIONS}."
        ),
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_annual_large_universe_value_profile(
        args.input_bundle,
        db_path=args.db_path,
        output_root=args.output_root,
        selection_fractions=tuple(args.selection_fractions or DEFAULT_SELECTION_FRACTIONS),
        winsor_lower=args.winsor_lower,
        winsor_upper=args.winsor_upper,
        required_positive_columns=(
            () if args.include_non_positive_ratios else POSITIVE_RATIO_ONLY_COLUMNS
        ),
        min_observations=args.min_observations,
    )
    bundle = write_annual_large_universe_value_profile_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
