#!/usr/bin/env python3
"""Runner-first entrypoint for free-float liquidity regime decomposition."""

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
from src.domains.analytics.free_float_liquidity_regime_decomposition import (  # noqa: E402
    DEFAULT_HIGH_RESIDUAL_Z,
    DEFAULT_LOW_RESIDUAL_Z,
    DEFAULT_RECENT_RETURN_WINDOWS,
    DEFAULT_RECOVERY_CHANGE_THRESHOLD,
    run_free_float_liquidity_regime_decomposition,
    write_free_float_liquidity_regime_decomposition_bundle,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Split free-float liquidity residual observations into rerating, "
            "distribution stress, stale-liquidity, and recovery regimes."
        )
    )
    parser.add_argument(
        "--input-bundle",
        default=None,
        help=(
            "Free-float liquidity gap bundle path. Defaults to the latest "
            "market-behavior/free-float-liquidity-gap bundle under output-root."
        ),
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Optional market.duckdb override. Defaults to the input bundle db_path.",
    )
    parser.add_argument(
        "--recent-return-windows",
        default=",".join(str(window) for window in DEFAULT_RECENT_RETURN_WINDOWS),
        help="Comma-separated lookback return windows in sessions.",
    )
    parser.add_argument(
        "--high-residual-z",
        type=float,
        default=DEFAULT_HIGH_RESIDUAL_Z,
        help="Residual z threshold for high-participation regimes.",
    )
    parser.add_argument(
        "--low-residual-z",
        type=float,
        default=DEFAULT_LOW_RESIDUAL_Z,
        help="Residual z threshold for stale-liquidity regimes.",
    )
    parser.add_argument(
        "--recovery-change-threshold",
        type=float,
        default=DEFAULT_RECOVERY_CHANGE_THRESHOLD,
        help="Residual-change threshold for liquidity_recovery inside low residual observations.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_free_float_liquidity_regime_decomposition(
        args.input_bundle,
        output_root=args.output_root,
        db_path=args.db_path,
        recent_return_windows=_parse_positive_ints(
            args.recent_return_windows,
            name="recent-return-windows",
        ),
        high_residual_z=args.high_residual_z,
        low_residual_z=args.low_residual_z,
        recovery_change_threshold=args.recovery_change_threshold,
    )
    bundle = write_free_float_liquidity_regime_decomposition_bundle(
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
