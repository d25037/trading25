#!/usr/bin/env python3
"""Runner-first entrypoint for TOPIX100 peak winner/loser intraday path research."""

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
from src.domains.analytics.topix100_peak_winner_loser_intraday_path import (  # noqa: E402
    run_topix100_peak_winner_loser_intraday_path_research,
    write_topix100_peak_winner_loser_intraday_path_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def _parse_anchor_candidate_times(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run TOPIX100 peak winner/loser intraday path research and persist a "
            "reproducible artifact bundle under ~/.local/share/trading25/research/."
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
        "--anchor-candidate-times",
        default="10:30,10:45",
        help="Comma-separated candidate anchor times (HH:MM).",
    )
    parser.add_argument(
        "--midday-reference-time",
        default="13:30",
        help="Reference intraday time used for post-anchor window summaries.",
    )
    parser.add_argument(
        "--tail-fraction",
        type=float,
        default=0.10,
        help="Tail fraction per side for top/bottom cross-sectional groups. Defaults to 0.10.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_topix100_peak_winner_loser_intraday_path_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        interval_minutes=args.interval_minutes,
        anchor_candidate_times=_parse_anchor_candidate_times(args.anchor_candidate_times),
        midday_reference_time=args.midday_reference_time,
        tail_fraction=args.tail_fraction,
    )
    bundle = write_topix100_peak_winner_loser_intraday_path_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
