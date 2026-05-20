#!/usr/bin/env python3
"""Runner-first entrypoint for daily move asymmetry research."""

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
from src.domains.analytics.daily_move_asymmetry import (  # noqa: E402
    DEFAULT_HORIZONS,
    DEFAULT_MIN_OBSERVATIONS,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_ROLLING_VOL_WINDOW,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    run_daily_move_asymmetry_research,
    write_daily_move_asymmetry_bundle,
)
from src.shared.paths.resolver import get_data_dir  # noqa: E402


def _default_market_db_path() -> str:
    return str((get_data_dir() / "market-timeseries" / "market.duckdb").resolve())


def _parse_int_csv(raw: str) -> tuple[int, ...]:
    values = tuple(int(part.strip()) for part in raw.split(",") if part.strip())
    if not values:
        raise argparse.ArgumentTypeError("value must contain at least one integer")
    return values


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Measure daily up/down asymmetry for TOPIX and Prime stocks using "
            "volatility-normalized close-to-close moves."
        )
    )
    parser.add_argument(
        "--db-path",
        default=_default_market_db_path(),
        help="market.duckdb path. Defaults to the active XDG market snapshot.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional first event date, YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional last event date, YYYY-MM-DD.",
    )
    parser.add_argument(
        "--horizons",
        type=_parse_int_csv,
        default=DEFAULT_HORIZONS,
        help=f"Comma-separated forward horizons. Default: {','.join(str(v) for v in DEFAULT_HORIZONS)}.",
    )
    parser.add_argument(
        "--rolling-vol-window",
        type=int,
        default=DEFAULT_ROLLING_VOL_WINDOW,
        help=f"Rolling volatility window in sessions. Default: {DEFAULT_ROLLING_VOL_WINDOW}.",
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=DEFAULT_MIN_OBSERVATIONS,
        help=f"Minimum observations for summary rows. Default: {DEFAULT_MIN_OBSERVATIONS}.",
    )
    parser.add_argument(
        "--severe-loss-threshold-pct",
        type=float,
        default=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
        help=(
            "Forward-return threshold counted as severe loss. "
            f"Default: {DEFAULT_SEVERE_LOSS_THRESHOLD_PCT}."
        ),
    )
    parser.add_argument(
        "--observation-sample-limit",
        type=int,
        default=DEFAULT_OBSERVATION_SAMPLE_LIMIT,
        help=f"Rows stored in observation_sample_df. Default: {DEFAULT_OBSERVATION_SAMPLE_LIMIT}.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_daily_move_asymmetry_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        horizons=args.horizons,
        rolling_vol_window=args.rolling_vol_window,
        min_observations=args.min_observations,
        severe_loss_threshold_pct=args.severe_loss_threshold_pct,
        observation_sample_limit=args.observation_sample_limit,
    )
    bundle = write_daily_move_asymmetry_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
