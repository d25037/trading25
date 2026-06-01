#!/usr/bin/env python3
"""Runner-first entrypoint for rerating x bubble regime forward response."""

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
from src.domains.analytics.market_bubble_footprint import (  # noqa: E402
    DEFAULT_FOOTPRINT_HORIZONS,
    DEFAULT_MARKET_SCOPES,
    DEFAULT_MIN_OBSERVATIONS,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_RERATING_SIGNAL_HORIZONS,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    run_rerating_bubble_regime_forward_response_research,
    write_rerating_bubble_regime_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Join the market bubble footprint monitor to rerating liquidity buckets "
            "and summarize forward TOPIX-excess returns."
        )
    )
    parser.add_argument(
        "--db-path",
        default=get_settings().market_db_path,
        help="Path to market.duckdb. Defaults to the active app setting.",
    )
    parser.add_argument("--start-date", default="2018-01-01", help="Anchor start date.")
    parser.add_argument("--end-date", default=None, help="Anchor end date.")
    parser.add_argument(
        "--signal-horizons",
        type=_parse_positive_ints,
        default=DEFAULT_RERATING_SIGNAL_HORIZONS,
        help="Comma-separated forward return horizons in trading sessions.",
    )
    parser.add_argument(
        "--footprint-horizons",
        type=_parse_positive_ints,
        default=DEFAULT_FOOTPRINT_HORIZONS,
        help="Comma-separated bubble footprint lookback horizons in trading sessions.",
    )
    parser.add_argument(
        "--markets",
        type=_parse_strings,
        default=DEFAULT_MARKET_SCOPES,
        help="Comma-separated market scopes: prime, standard, growth, unknown, or all.",
    )
    parser.add_argument(
        "--frequency",
        choices=("monthly", "weekly"),
        default="monthly",
        help="Snapshot frequency.",
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=DEFAULT_MIN_OBSERVATIONS,
        help="Minimum observations required for a summarized row.",
    )
    parser.add_argument(
        "--severe-loss-threshold-pct",
        type=float,
        default=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
        help="Forward excess return threshold used for left-tail diagnostics.",
    )
    parser.add_argument(
        "--observation-sample-limit",
        type=int,
        default=DEFAULT_OBSERVATION_SAMPLE_LIMIT,
        help="Number of observation rows to persist as a sample table.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_rerating_bubble_regime_forward_response_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        signal_horizons=args.signal_horizons,
        footprint_horizons=args.footprint_horizons,
        market_scopes=args.markets,
        frequency=args.frequency,
        min_observations=args.min_observations,
        severe_loss_threshold_pct=args.severe_loss_threshold_pct,
        observation_sample_limit=args.observation_sample_limit,
    )
    bundle = write_rerating_bubble_regime_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


def _parse_positive_ints(value: str | tuple[int, ...]) -> tuple[int, ...]:
    if isinstance(value, tuple):
        return value
    values = tuple(sorted({int(part.strip()) for part in value.split(",") if part.strip()}))
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError("value must contain positive integers")
    return values


def _parse_strings(value: str | tuple[str, ...]) -> tuple[str, ...]:
    if isinstance(value, tuple):
        return value
    values = tuple(part.strip() for part in value.split(",") if part.strip())
    if not values:
        raise argparse.ArgumentTypeError("value must contain at least one item")
    return values


if __name__ == "__main__":
    raise SystemExit(main())
