#!/usr/bin/env python3
"""Runner-first entrypoint for annual forward PER regime decomposition research."""

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
from src.domains.analytics.annual_forward_per_regime_decomposition import (  # noqa: E402
    DEFAULT_MIN_TRAIN_OBSERVATIONS,
    DEFAULT_SELECTION_FRACTIONS,
    run_annual_forward_per_regime_decomposition,
    write_annual_forward_per_regime_decomposition_bundle,
)
from src.domains.analytics.annual_fundamental_confounder_analysis import (  # noqa: E402
    DEFAULT_WINSOR_LOWER,
    DEFAULT_WINSOR_UPPER,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Decompose low forward PER into positive-low and non-positive regimes "
            "and compare event-level plus portfolio-level behavior."
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
        "--min-train-observations",
        type=int,
        default=DEFAULT_MIN_TRAIN_OBSERVATIONS,
        help=(
            "Minimum prior observations for walk-forward regression weights. "
            f"Default: {DEFAULT_MIN_TRAIN_OBSERVATIONS}."
        ),
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_annual_forward_per_regime_decomposition(
        args.input_bundle,
        db_path=args.db_path,
        output_root=args.output_root,
        selection_fractions=tuple(args.selection_fractions or DEFAULT_SELECTION_FRACTIONS),
        winsor_lower=args.winsor_lower,
        winsor_upper=args.winsor_upper,
        min_train_observations=args.min_train_observations,
    )
    bundle = write_annual_forward_per_regime_decomposition_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
