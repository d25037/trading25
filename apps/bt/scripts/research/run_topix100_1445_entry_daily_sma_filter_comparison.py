#!/usr/bin/env python3
"""Runner-first entrypoint for TOPIX100 14:45 daily-SMA filter comparison."""

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
from src.domains.analytics.topix100_1445_entry_daily_sma_filter_comparison import (  # noqa: E402
    DEFAULT_EXIT_LABEL,
    DEFAULT_INTERVAL_MINUTES,
    DEFAULT_SIGNAL_FAMILY,
    run_topix100_1445_entry_daily_sma_filter_comparison_research,
    write_topix100_1445_entry_daily_sma_filter_comparison_research_bundle,
)
from src.domains.analytics.topix100_1445_entry_signal_regime_comparison import (  # noqa: E402
    DEFAULT_BUCKET_COUNT,
    DEFAULT_ENTRY_TIME,
    DEFAULT_NEXT_SESSION_EXIT_TIME,
    DEFAULT_PERIOD_MONTHS,
    DEFAULT_TAIL_FRACTION,
)
from src.shared.config.settings import get_settings  # noqa: E402


def _parse_sma_windows(value: str) -> tuple[int, ...]:
    return tuple(int(item.strip()) for item in value.split(",") if item.strip())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run TOPIX100 14:45 daily-SMA filter comparison research and persist "
            "a reproducible artifact bundle under ~/.local/share/trading25/research/."
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
        default=DEFAULT_INTERVAL_MINUTES,
        help="Opening-bucket interval to analyze. Defaults to 15.",
    )
    parser.add_argument(
        "--signal-family",
        default=DEFAULT_SIGNAL_FAMILY,
        help="Signal family from the 14:45 signal/regime study.",
    )
    parser.add_argument(
        "--exit-label",
        default=DEFAULT_EXIT_LABEL,
        help="Trade exit label from the 14:45 signal/regime study.",
    )
    parser.add_argument(
        "--daily-sma-windows",
        default="20,50,100",
        help="Comma-separated prior-close daily SMA windows.",
    )
    parser.add_argument(
        "--bucket-count",
        type=int,
        default=DEFAULT_BUCKET_COUNT,
        help="How many signal-ratio groups to build within each half-year period.",
    )
    parser.add_argument(
        "--period-months",
        type=int,
        default=DEFAULT_PERIOD_MONTHS,
        help="Months per analysis period. Defaults to 6.",
    )
    parser.add_argument(
        "--entry-time",
        default=DEFAULT_ENTRY_TIME,
        help="Same-day entry time. Defaults to 14:45.",
    )
    parser.add_argument(
        "--next-session-exit-time",
        default=DEFAULT_NEXT_SESSION_EXIT_TIME,
        help="Next-session timed exit used by the base research.",
    )
    parser.add_argument(
        "--tail-fraction",
        type=float,
        default=DEFAULT_TAIL_FRACTION,
        help="Cross-sectional tail fraction per side for winners/losers labels.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_topix100_1445_entry_daily_sma_filter_comparison_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        interval_minutes=args.interval_minutes,
        signal_family=args.signal_family,
        exit_label=args.exit_label,
        daily_sma_windows=_parse_sma_windows(args.daily_sma_windows),
        bucket_count=args.bucket_count,
        period_months=args.period_months,
        entry_time=args.entry_time,
        next_session_exit_time=args.next_session_exit_time,
        tail_fraction=args.tail_fraction,
    )
    bundle = write_topix100_1445_entry_daily_sma_filter_comparison_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
