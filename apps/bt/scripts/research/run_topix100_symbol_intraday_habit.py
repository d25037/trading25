#!/usr/bin/env python3
"""Runner-first entrypoint for TOPIX100 symbol intraday habit research."""

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
from src.domains.analytics.topix100_symbol_intraday_habit import (  # noqa: E402
    run_topix100_symbol_intraday_habit_research,
    write_topix100_symbol_intraday_habit_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run sampled TOPIX100 symbol intraday habit research and persist a "
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
        default=30,
        help="Intraday interval in minutes. Defaults to 30.",
    )
    parser.add_argument(
        "--sample-seed",
        type=int,
        default=42,
        help="Deterministic seed for the four random TOPIX100 symbols.",
    )
    parser.add_argument(
        "--random-sample-size",
        type=int,
        default=4,
        help="How many non-anchor TOPIX100 symbols to sample.",
    )
    parser.add_argument(
        "--anchor-code",
        default="6857",
        help="Fixed anchor symbol code. Defaults to Advantest (6857).",
    )
    parser.add_argument(
        "--analysis-period-months",
        type=int,
        default=6,
        help="Months per analysis period. Defaults to 6.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_topix100_symbol_intraday_habit_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        interval_minutes=args.interval_minutes,
        sample_seed=args.sample_seed,
        random_sample_size=args.random_sample_size,
        anchor_code=args.anchor_code,
        analysis_period_months=args.analysis_period_months,
    )
    bundle = write_topix100_symbol_intraday_habit_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
