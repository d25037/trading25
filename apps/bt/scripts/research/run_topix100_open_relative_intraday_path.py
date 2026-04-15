#!/usr/bin/env python3
"""Runner-first entrypoint for TOPIX100 open-relative intraday path research."""

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
from src.domains.analytics.topix100_open_relative_intraday_path import (  # noqa: E402
    run_topix100_open_relative_intraday_path_research,
    write_topix100_open_relative_intraday_path_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def _parse_interval_minutes(value: str) -> list[int]:
    items = [item.strip() for item in value.split(",")]
    return [int(item) for item in items if item]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run TOPIX100 open-relative intraday path research and persist a "
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
        "--intervals",
        default="5,15,30",
        help="Comma-separated intraday intervals in minutes.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_topix100_open_relative_intraday_path_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        interval_minutes_list=_parse_interval_minutes(args.intervals),
    )
    bundle = write_topix100_open_relative_intraday_path_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
