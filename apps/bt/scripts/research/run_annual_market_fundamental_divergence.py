#!/usr/bin/env python3
"""Runner-first entrypoint for annual market fundamental divergence research."""

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
from src.domains.analytics.annual_market_fundamental_divergence import (  # noqa: E402
    DEFAULT_MIN_OBSERVATIONS,
    run_annual_market_fundamental_divergence,
    write_annual_market_fundamental_divergence_bundle,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare fundamental feature distributions across prime, standard, "
            "and growth using the annual first-open last-close panel."
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
        help=f"Minimum observations per market side for pairwise divergence. Default: {DEFAULT_MIN_OBSERVATIONS}.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_annual_market_fundamental_divergence(
        args.input_bundle,
        output_root=args.output_root,
        winsor_lower=args.winsor_lower,
        winsor_upper=args.winsor_upper,
        min_observations=args.min_observations,
    )
    bundle = write_annual_market_fundamental_divergence_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
