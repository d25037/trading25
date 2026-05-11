#!/usr/bin/env python3
"""Runner-first entrypoint for liquidity-implied price convergence research."""

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
from src.domains.analytics.free_float_liquidity_implied_price_convergence import (  # noqa: E402
    DEFAULT_ADV_WINDOWS,
    DEFAULT_HORIZONS,
    DEFAULT_MIN_DAILY_REGRESSION_OBSERVATIONS,
    DEFAULT_OBSERVATION_STRIDE_SESSIONS,
    DEFAULT_RECENT_RETURN_WINDOWS,
    run_free_float_liquidity_implied_price_convergence,
    write_free_float_liquidity_implied_price_convergence_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "PIT-safe Prime study of whether liquidity-implied price gaps close "
            "via realized price moves or implied-price decay."
        )
    )
    parser.add_argument(
        "--db-path",
        default=get_settings().market_db_path,
        help="Path to market.duckdb. Defaults to the active app setting.",
    )
    parser.add_argument("--start-date", default=None, help="Observation start date.")
    parser.add_argument("--end-date", default=None, help="Observation end date.")
    parser.add_argument(
        "--adv-windows",
        default=",".join(str(window) for window in DEFAULT_ADV_WINDOWS),
        help="Comma-separated ADV windows in sessions.",
    )
    parser.add_argument(
        "--horizons",
        default=",".join(str(horizon) for horizon in DEFAULT_HORIZONS),
        help="Comma-separated forward close-to-close horizons in sessions.",
    )
    parser.add_argument(
        "--recent-return-windows",
        default=",".join(str(window) for window in DEFAULT_RECENT_RETURN_WINDOWS),
        help="Comma-separated backward-looking return windows for regime labels.",
    )
    parser.add_argument(
        "--observation-stride-sessions",
        type=int,
        default=DEFAULT_OBSERVATION_STRIDE_SESSIONS,
        help="Sample every N sessions per code to keep bundle size manageable.",
    )
    parser.add_argument(
        "--min-daily-regression-observations",
        type=int,
        default=DEFAULT_MIN_DAILY_REGRESSION_OBSERVATIONS,
        help="Minimum same-date Prime observations required for daily regression.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_free_float_liquidity_implied_price_convergence(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        adv_windows=_parse_positive_ints(args.adv_windows, name="adv-windows"),
        horizons=_parse_positive_ints(args.horizons, name="horizons"),
        recent_return_windows=_parse_positive_ints(
            args.recent_return_windows,
            name="recent-return-windows",
        ),
        observation_stride_sessions=args.observation_stride_sessions,
        min_daily_regression_observations=args.min_daily_regression_observations,
    )
    bundle = write_free_float_liquidity_implied_price_convergence_bundle(
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
