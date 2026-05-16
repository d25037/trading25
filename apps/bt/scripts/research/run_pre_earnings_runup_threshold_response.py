#!/usr/bin/env python3
"""Runner-first entrypoint for pre-earnings runup threshold response research."""

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
from src.domains.analytics.pre_earnings_runup_threshold_response import (  # noqa: E402
    DEFAULT_20D_RUNUP_THRESHOLDS,
    DEFAULT_60D_RUNUP_THRESHOLDS,
    DEFAULT_HORIZONS,
    DEFAULT_LIQUIDITY_WINDOW,
    DEFAULT_MIN_EVENTS,
    DEFAULT_PRE_WINDOWS,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    run_pre_earnings_runup_threshold_response_research,
    write_pre_earnings_runup_threshold_response_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run PIT-safe pre-earnings 20d/60d runup threshold response research "
            "across EPS 1.2x positive, hold-through, post-entry, and no-fill outcomes."
        )
    )
    parser.add_argument(
        "--db-path",
        default=get_settings().market_db_path,
        help="Path to market.duckdb. Defaults to the active app setting.",
    )
    parser.add_argument("--start-date", default=None, help="Disclosure start date.")
    parser.add_argument("--end-date", default=None, help="Disclosure end date.")
    parser.add_argument(
        "--pre-windows",
        default=",".join(str(window) for window in DEFAULT_PRE_WINDOWS),
        help="Comma-separated pre-disclosure return windows in trading sessions.",
    )
    parser.add_argument(
        "--horizons",
        default=",".join(str(horizon) for horizon in DEFAULT_HORIZONS),
        help="Comma-separated forward return horizons in trading sessions.",
    )
    parser.add_argument(
        "--liquidity-window",
        type=int,
        default=DEFAULT_LIQUIDITY_WINDOW,
        help="Trading-session window for median ADV liquidity diagnostics.",
    )
    parser.add_argument(
        "--severe-loss-threshold-pct",
        type=float,
        default=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
        help="Forward excess return threshold used for left-tail diagnostics.",
    )
    parser.add_argument(
        "--thresholds-20d",
        default=",".join(str(value) for value in DEFAULT_20D_RUNUP_THRESHOLDS),
        help="Comma-separated absolute 20d return thresholds in percent.",
    )
    parser.add_argument(
        "--thresholds-60d",
        default=",".join(str(value) for value in DEFAULT_60D_RUNUP_THRESHOLDS),
        help="Comma-separated absolute 60d return thresholds in percent.",
    )
    parser.add_argument(
        "--min-events",
        type=int,
        default=DEFAULT_MIN_EVENTS,
        help="Minimum events required for a summarized row.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_pre_earnings_runup_threshold_response_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        pre_windows=_parse_positive_ints(args.pre_windows, name="pre-windows"),
        horizons=_parse_positive_ints(args.horizons, name="horizons"),
        liquidity_window=args.liquidity_window,
        severe_loss_threshold_pct=args.severe_loss_threshold_pct,
        thresholds_20d=_parse_non_negative_floats(args.thresholds_20d, name="thresholds-20d"),
        thresholds_60d=_parse_non_negative_floats(args.thresholds_60d, name="thresholds-60d"),
        min_events=args.min_events,
    )
    bundle = write_pre_earnings_runup_threshold_response_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


def _parse_positive_ints(value: str, *, name: str) -> tuple[int, ...]:
    values = tuple(sorted({int(part.strip()) for part in value.split(",") if part.strip()}))
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError(f"{name} must contain positive integers")
    return values


def _parse_non_negative_floats(value: str, *, name: str) -> tuple[float, ...]:
    values = tuple(sorted({float(part.strip()) for part in value.split(",") if part.strip()}))
    if not values or any(item < 0 for item in values):
        raise argparse.ArgumentTypeError(f"{name} must contain non-negative numbers")
    return values


if __name__ == "__main__":
    raise SystemExit(main())
