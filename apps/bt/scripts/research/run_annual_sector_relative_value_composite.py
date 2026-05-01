#!/usr/bin/env python3
"""Runner-first entrypoint for annual sector-relative value composite research."""

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
)
from src.domains.analytics.annual_sector_relative_value_composite import (  # noqa: E402
    DEFAULT_MIN_SECTOR_OBSERVATIONS,
    DEFAULT_SELECTION_FRACTIONS,
    VALUE_COMPOSITE_REQUIRED_POSITIVE_COLUMNS,
    run_annual_sector_relative_value_composite,
    write_annual_sector_relative_value_composite_bundle,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare raw and sector-relative PBR / forward-PER valuation scores "
            "inside annual value-composite selection portfolios."
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
            "Top fraction to select within each year x market scope. Repeat to override "
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
        "--min-sector-observations",
        type=int,
        default=DEFAULT_MIN_SECTOR_OBSERVATIONS,
        help=(
            "Minimum finite names inside each year x market x sector group before "
            "sector-relative valuation scores are eligible. "
            f"Default: {DEFAULT_MIN_SECTOR_OBSERVATIONS}."
        ),
    )
    parser.add_argument(
        "--allow-non-positive-pbr-or-forward-per",
        action="store_true",
        help=(
            "Do not enforce the default positive PBR and positive forward PER filter "
            f"({', '.join(VALUE_COMPOSITE_REQUIRED_POSITIVE_COLUMNS)})."
        ),
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_annual_sector_relative_value_composite(
        args.input_bundle,
        db_path=args.db_path,
        output_root=args.output_root,
        selection_fractions=tuple(args.selection_fractions or DEFAULT_SELECTION_FRACTIONS),
        winsor_lower=args.winsor_lower,
        winsor_upper=args.winsor_upper,
        min_sector_observations=args.min_sector_observations,
        required_positive_columns=(
            () if args.allow_non_positive_pbr_or_forward_per else VALUE_COMPOSITE_REQUIRED_POSITIVE_COLUMNS
        ),
    )
    bundle = write_annual_sector_relative_value_composite_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
