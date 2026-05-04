#!/usr/bin/env python3
"""Runner-first entrypoint for high-zone bearish price-action research."""

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
from src.domains.analytics.high_zone_bearish_price_action import (  # noqa: E402
    DEFAULT_HORIZONS,
    DEFAULT_LOOKBACK_YEARS,
    DEFAULT_MIN_EVENTS_FOR_SELECTION,
    DEFAULT_SAMPLE_EVENT_SIZE,
    run_high_zone_bearish_price_action_research,
    write_high_zone_bearish_price_action_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def _parse_int_csv(value: str) -> tuple[int, ...]:
    return tuple(int(part.strip()) for part in value.split(",") if part.strip())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Prime/Standard high-zone bearish price-action research and "
            "persist a reproducible artifact bundle under ~/.local/share/trading25/research/."
        )
    )
    parser.add_argument(
        "--db-path",
        default=get_settings().market_db_path,
        help="Path to market.duckdb. Defaults to the active app setting.",
    )
    parser.add_argument("--start-date", default=None, help="Analysis start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", default=None, help="Analysis end date (YYYY-MM-DD).")
    parser.add_argument(
        "--lookback-years",
        type=int,
        default=DEFAULT_LOOKBACK_YEARS,
        help="Default lookback years when --start-date is omitted.",
    )
    parser.add_argument(
        "--horizons",
        type=_parse_int_csv,
        default=DEFAULT_HORIZONS,
        help="Comma-separated forward horizons in trading days.",
    )
    parser.add_argument(
        "--sample-event-size",
        type=int,
        default=DEFAULT_SAMPLE_EVENT_SIZE,
        help="Deterministic event sample size per market/pattern.",
    )
    parser.add_argument(
        "--min-events-for-selection",
        type=int,
        default=DEFAULT_MIN_EVENTS_FOR_SELECTION,
        help="Minimum events required before a pattern can appear in top-negative ranking.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_high_zone_bearish_price_action_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_years=args.lookback_years,
        horizons=args.horizons,
        sample_event_size=args.sample_event_size,
        min_events_for_selection=args.min_events_for_selection,
    )
    bundle = write_high_zone_bearish_price_action_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
