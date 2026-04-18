#!/usr/bin/env python3
"""Runner-first entrypoint for TOPIX100 14:45 daily-SMA branch portfolio research."""

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
from src.domains.analytics.topix100_1445_entry_daily_sma_filter_portfolio import (  # noqa: E402
    DEFAULT_MARKET_REGIME_BUCKET_KEY,
    DEFAULT_SMA_FILTER_STATE,
    DEFAULT_SMA_WINDOW,
    DEFAULT_SUBGROUP_KEY,
    run_topix100_1445_entry_daily_sma_filter_portfolio_research,
    write_topix100_1445_entry_daily_sma_filter_portfolio_research_bundle,
)
from src.domains.analytics.topix100_1445_entry_signal_regime_comparison import (  # noqa: E402
    DEFAULT_BUCKET_COUNT,
    DEFAULT_ENTRY_TIME,
    DEFAULT_NEXT_SESSION_EXIT_TIME,
    DEFAULT_PERIOD_MONTHS,
    DEFAULT_TAIL_FRACTION,
)
from src.domains.analytics.topix100_1445_entry_daily_sma_filter_comparison import (  # noqa: E402
    DEFAULT_EXIT_LABEL,
    DEFAULT_INTERVAL_MINUTES,
    DEFAULT_SIGNAL_FAMILY,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run TOPIX100 14:45 daily-SMA branch portfolio research and persist "
            "a reproducible artifact bundle."
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
        "--market-regime-bucket-key",
        default=DEFAULT_MARKET_REGIME_BUCKET_KEY,
        help="Market regime bucket key to evaluate.",
    )
    parser.add_argument(
        "--subgroup-key",
        default=DEFAULT_SUBGROUP_KEY,
        help="TOPIX100 cross-sectional subgroup key to evaluate.",
    )
    parser.add_argument(
        "--sma-window",
        type=int,
        default=DEFAULT_SMA_WINDOW,
        help="Daily SMA window used for the branch filter.",
    )
    parser.add_argument(
        "--sma-filter-state",
        default=DEFAULT_SMA_FILTER_STATE,
        help="One of all / above / at_or_below.",
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
    result = run_topix100_1445_entry_daily_sma_filter_portfolio_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        interval_minutes=args.interval_minutes,
        signal_family=args.signal_family,
        exit_label=args.exit_label,
        market_regime_bucket_key=args.market_regime_bucket_key,
        subgroup_key=args.subgroup_key,
        sma_window=args.sma_window,
        sma_filter_state=args.sma_filter_state,
        bucket_count=args.bucket_count,
        period_months=args.period_months,
        entry_time=args.entry_time,
        next_session_exit_time=args.next_session_exit_time,
        tail_fraction=args.tail_fraction,
    )
    bundle = write_topix100_1445_entry_daily_sma_filter_portfolio_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
