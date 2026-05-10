#!/usr/bin/env python3
"""Runner-first entrypoint for Prime liquidity x momentum interaction research."""

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
from src.domains.analytics.free_float_liquidity_prime_momentum_interaction import (  # noqa: E402
    DEFAULT_HORIZONS,
    DEFAULT_MIN_OBSERVATIONS,
    run_free_float_liquidity_prime_momentum_interaction,
    write_free_float_liquidity_prime_momentum_interaction_bundle,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Test whether Prime free-float liquidity residual adds information "
            "to 20d/60d price momentum."
        )
    )
    parser.add_argument(
        "--input-bundle",
        default=None,
        help=(
            "Free-float liquidity regime decomposition bundle path. Defaults to "
            "the latest market-behavior/free-float-liquidity-regime-decomposition "
            "bundle under output-root."
        ),
    )
    parser.add_argument(
        "--horizons",
        default=",".join(str(horizon) for horizon in DEFAULT_HORIZONS),
        help="Comma-separated forward horizons in sessions.",
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=DEFAULT_MIN_OBSERVATIONS,
        help="Minimum observations required for each regression model.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_free_float_liquidity_prime_momentum_interaction(
        args.input_bundle,
        output_root=args.output_root,
        horizons=_parse_positive_ints(args.horizons, name="horizons"),
        min_observations=args.min_observations,
    )
    bundle = write_free_float_liquidity_prime_momentum_interaction_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


def _parse_positive_ints(value: str, *, name: str) -> tuple[int, ...]:
    values = tuple(
        sorted({int(part.strip()) for part in value.split(",") if part.strip()})
    )
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError(f"{name} must contain positive integers")
    return values


if __name__ == "__main__":
    raise SystemExit(main())
