#!/usr/bin/env python3
"""Runner-first entrypoint for tradeable prime pullback-entry research."""

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
from src.domains.analytics.speculative_volume_surge_prime_pullback_tradeable import (  # noqa: E402
    DEFAULT_ADV_WINDOW,
    DEFAULT_COOLDOWN_SESSIONS,
    DEFAULT_ENTRY_BUCKETS,
    DEFAULT_HOLDING_PERIOD_SESSIONS,
    DEFAULT_INITIAL_PEAK_WINDOW,
    DEFAULT_LOOKBACK_YEARS,
    DEFAULT_PRICE_JUMP_THRESHOLD,
    DEFAULT_PULLBACK_SEARCH_WINDOW,
    DEFAULT_SAMPLE_SIZE,
    DEFAULT_VOLUME_RATIO_THRESHOLD,
    DEFAULT_VOLUME_WINDOW,
    run_speculative_volume_surge_prime_pullback_tradeable_research,
    write_speculative_volume_surge_prime_pullback_tradeable_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def _parse_entry_bucket_csv(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in value.split(",") if part.strip())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run tradeable prime-only speculative pullback-entry research and "
            "persist a reproducible bundle under ~/.local/share/trading25/research/."
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
        help="Analysis start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Analysis end date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--lookback-years",
        type=int,
        default=DEFAULT_LOOKBACK_YEARS,
        help="Default lookback years when --start-date is omitted.",
    )
    parser.add_argument(
        "--price-jump-threshold",
        type=float,
        default=DEFAULT_PRICE_JUMP_THRESHOLD,
        help="Primary event close-return threshold, e.g. 0.10 for +10%%.",
    )
    parser.add_argument(
        "--volume-ratio-threshold",
        type=float,
        default=DEFAULT_VOLUME_RATIO_THRESHOLD,
        help="Primary event volume / trailing-average threshold.",
    )
    parser.add_argument(
        "--volume-window",
        type=int,
        default=DEFAULT_VOLUME_WINDOW,
        help="Trailing volume window used for the primary event ratio.",
    )
    parser.add_argument(
        "--adv-window",
        type=int,
        default=DEFAULT_ADV_WINDOW,
        help="Trailing ADV window used for liquidity bucketing.",
    )
    parser.add_argument(
        "--cooldown-sessions",
        type=int,
        default=DEFAULT_COOLDOWN_SESSIONS,
        help="Same-code cooldown used to merge nearby trigger days into one episode.",
    )
    parser.add_argument(
        "--initial-peak-window",
        type=int,
        default=DEFAULT_INITIAL_PEAK_WINDOW,
        help="Session window used to define the initial peak after the surge trigger.",
    )
    parser.add_argument(
        "--pullback-search-window",
        type=int,
        default=DEFAULT_PULLBACK_SEARCH_WINDOW,
        help="Maximum session offset used to monitor entry signals and deepest pullbacks.",
    )
    parser.add_argument(
        "--entry-buckets",
        type=_parse_entry_bucket_csv,
        default=DEFAULT_ENTRY_BUCKETS,
        help="Comma-separated pullback buckets used as entry triggers.",
    )
    parser.add_argument(
        "--holding-period-sessions",
        type=int,
        default=DEFAULT_HOLDING_PERIOD_SESSIONS,
        help="Maximum holding window measured from the entry session.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help="Positive/negative example count retained in the bundle.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_speculative_volume_surge_prime_pullback_tradeable_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_years=args.lookback_years,
        price_jump_threshold=args.price_jump_threshold,
        volume_ratio_threshold=args.volume_ratio_threshold,
        volume_window=args.volume_window,
        adv_window=args.adv_window,
        cooldown_sessions=args.cooldown_sessions,
        initial_peak_window=args.initial_peak_window,
        pullback_search_window=args.pullback_search_window,
        entry_buckets=args.entry_buckets,
        holding_period_sessions=args.holding_period_sessions,
        sample_size=args.sample_size,
    )
    bundle = write_speculative_volume_surge_prime_pullback_tradeable_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
