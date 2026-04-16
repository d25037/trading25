#!/usr/bin/env python3
"""Runner-first entrypoint for TOPIX100 13:30 -> next 10:45 conditioning research."""

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
from src.domains.analytics.topix100_1330_entry_next_1045_exit_conditioning import (  # noqa: E402
    run_topix100_1330_entry_next_1045_exit_conditioning_research,
    write_topix100_1330_entry_next_1045_exit_conditioning_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run TOPIX100 13:30 to next-session 10:45 conditioning research and "
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
        "--interval-minutes",
        type=int,
        default=5,
        help="Intraday interval in minutes. Defaults to 5.",
    )
    parser.add_argument(
        "--entry-time",
        default="13:30",
        help="Entry time on day D. Defaults to 13:30.",
    )
    parser.add_argument(
        "--exit-time",
        default="10:45",
        help="Exit time on day D+1. Defaults to 10:45.",
    )
    parser.add_argument(
        "--tail-fraction",
        type=float,
        default=0.10,
        help="Tail fraction per side for top/bottom entry-strength groups. Defaults to 0.10.",
    )
    parser.add_argument(
        "--prev-day-peak-time",
        default="10:45",
        help="Previous-day peak anchor used for winner/loser conditioning. Defaults to 10:45.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_topix100_1330_entry_next_1045_exit_conditioning_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        interval_minutes=args.interval_minutes,
        entry_time=args.entry_time,
        exit_time=args.exit_time,
        tail_fraction=args.tail_fraction,
        prev_day_peak_time=args.prev_day_peak_time,
    )
    bundle = write_topix100_1330_entry_next_1045_exit_conditioning_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
