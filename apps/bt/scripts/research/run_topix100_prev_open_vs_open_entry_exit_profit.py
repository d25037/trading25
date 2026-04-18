#!/usr/bin/env python3
"""Runner-first entrypoint for TOPIX100 prev-open-vs-open entry/exit profit research."""

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

from scripts.research.common import add_bundle_output_arguments, emit_bundle_payload  # noqa: E402
from src.domains.analytics.topix100_prev_open_vs_open_entry_exit_profit import (  # noqa: E402
    DEFAULT_BUCKET_COUNT,
    DEFAULT_PERIOD_MONTHS,
    DEFAULT_ROUND_TRIP_COST_BPS,
    run_topix100_prev_open_vs_open_entry_exit_profit_research,
    write_topix100_prev_open_vs_open_entry_exit_profit_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def _parse_interval_minutes(value: str) -> tuple[int, ...]:
    return tuple(int(item.strip()) for item in value.split(",") if item.strip())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run TOPIX100 prev-open-vs-open entry/exit profit research and "
            "persist a reproducible artifact bundle under "
            "~/.local/share/trading25/research/."
        )
    )
    parser.add_argument(
        "--db-path",
        default=get_settings().market_db_path,
        help="Path to market.duckdb. Defaults to the active app setting.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional end date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--intervals",
        default="5,15,30",
        help="Comma-separated intraday opening-bucket intervals in minutes.",
    )
    parser.add_argument(
        "--bucket-count",
        type=int,
        default=DEFAULT_BUCKET_COUNT,
        help="How many volume-ratio groups to build within each period.",
    )
    parser.add_argument(
        "--period-months",
        type=int,
        default=DEFAULT_PERIOD_MONTHS,
        help="Months per analysis period. Defaults to 6.",
    )
    parser.add_argument(
        "--round-trip-cost-bps",
        type=float,
        default=DEFAULT_ROUND_TRIP_COST_BPS,
        help="Round-trip transaction cost in basis points. Defaults to 0.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_topix100_prev_open_vs_open_entry_exit_profit_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        interval_minutes_list=_parse_interval_minutes(args.intervals),
        bucket_count=args.bucket_count,
        period_months=args.period_months,
        round_trip_cost_bps=args.round_trip_cost_bps,
    )
    bundle = write_topix100_prev_open_vs_open_entry_exit_profit_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
