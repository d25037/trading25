#!/usr/bin/env python3
"""Runner-first entrypoint for TOPIX100 price-vs-SMA Q10 bounce research."""

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
from src.domains.analytics.topix100_price_vs_sma_q10_bounce import (  # noqa: E402
    run_topix100_price_vs_sma_q10_bounce_research,
    write_topix100_price_vs_sma_q10_bounce_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def _parse_csv_list(value: str | None) -> list[str] | None:
    if value is None:
        return None
    items = [item.strip() for item in value.split(",")]
    return [item for item in items if item]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run TOPIX100 price-vs-SMA Q10 bounce research and persist a "
            "reproducible artifact bundle under ~/.local/share/trading25/research/."
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
        default=10,
        help="Default lookback years when --start-date is omitted.",
    )
    parser.add_argument(
        "--min-constituents-per-day",
        type=int,
        default=80,
        help="Minimum TOPIX100 constituents required per day after warmup.",
    )
    parser.add_argument(
        "--price-features",
        default=None,
        help="Optional comma-separated price feature keys.",
    )
    parser.add_argument(
        "--volume-features",
        default=None,
        help="Optional comma-separated volume feature keys.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_topix100_price_vs_sma_q10_bounce_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_years=args.lookback_years,
        min_constituents_per_day=args.min_constituents_per_day,
        price_features=_parse_csv_list(args.price_features),
        volume_features=_parse_csv_list(args.volume_features),
    )
    bundle = write_topix100_price_vs_sma_q10_bounce_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
