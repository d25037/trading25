#!/usr/bin/env python3
"""Runner-first entrypoint for pre-disclosure flow/volatility research."""

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
from src.domains.analytics.pre_disclosure_flow_volatility import (  # noqa: E402
    DEFAULT_ATR_PERIOD,
    DEFAULT_BASELINE_WINDOW,
    DEFAULT_BUCKET_COUNT,
    DEFAULT_HORIZONS,
    DEFAULT_PRE_WINDOWS,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    run_pre_disclosure_flow_volatility_research,
    write_pre_disclosure_flow_volatility_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run PIT-safe pre-disclosure flow/volatility research over statements "
            "events, stock OHLCV, TOPIX excess returns, and market splits."
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
        help="Comma-separated pre-disclosure windows in trading sessions.",
    )
    parser.add_argument(
        "--horizons",
        default=",".join(str(horizon) for horizon in DEFAULT_HORIZONS),
        help="Comma-separated forward horizons in trading sessions.",
    )
    parser.add_argument(
        "--atr-period",
        type=int,
        default=DEFAULT_ATR_PERIOD,
        help="ATR period used for pre-disclosure volatility features.",
    )
    parser.add_argument(
        "--baseline-window",
        type=int,
        default=DEFAULT_BASELINE_WINDOW,
        help="Prior-session baseline window for ATR and volume z-scores.",
    )
    parser.add_argument(
        "--bucket-count",
        type=int,
        default=DEFAULT_BUCKET_COUNT,
        help="Market-scope bucket count for informed-flow score.",
    )
    parser.add_argument(
        "--severe-loss-threshold-pct",
        type=float,
        default=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
        help="Forward excess return threshold used for left-tail diagnostics.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_pre_disclosure_flow_volatility_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        pre_windows=_parse_positive_ints(args.pre_windows, name="pre-windows"),
        horizons=_parse_positive_ints(args.horizons, name="horizons"),
        atr_period=args.atr_period,
        baseline_window=args.baseline_window,
        bucket_count=args.bucket_count,
        severe_loss_threshold_pct=args.severe_loss_threshold_pct,
    )
    bundle = write_pre_disclosure_flow_volatility_bundle(
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


if __name__ == "__main__":
    raise SystemExit(main())
