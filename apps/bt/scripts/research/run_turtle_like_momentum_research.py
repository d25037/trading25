#!/usr/bin/env python3
"""Runner-first entrypoint for Turtle-like momentum research."""

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
from src.domains.analytics.turtle_like_momentum_research import (  # noqa: E402
    DEFAULT_ATR_SESSIONS,
    DEFAULT_CHANNEL_SPECS,
    DEFAULT_ENTRY_MODES,
    DEFAULT_LOOKBACK_YEARS,
    DEFAULT_MIN_AVG_TRADING_VALUE_MIL_JPY,
    DEFAULT_SIZING_METHODS,
    run_turtle_like_momentum_research,
    write_turtle_like_momentum_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def _parse_csv(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in value.split(",") if part.strip())


def _parse_channel_specs(value: str) -> tuple[tuple[int, int], ...]:
    specs: list[tuple[int, int]] = []
    for part in value.split(","):
        item = part.strip()
        if not item:
            continue
        entry_raw, exit_raw = item.split(":", 1)
        specs.append((int(entry_raw), int(exit_raw)))
    return tuple(specs)


def _format_channel_specs_default() -> str:
    return ",".join(f"{entry}:{exit}" for entry, exit in DEFAULT_CHANNEL_SPECS)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Turtle-like Donchian channel momentum research and persist a "
            "reproducible artifact bundle."
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
        "--channel-specs",
        type=_parse_channel_specs,
        default=DEFAULT_CHANNEL_SPECS,
        help=(
            "Comma-separated entry:exit Donchian channel specs in sessions "
            f"(default: {_format_channel_specs_default()})."
        ),
    )
    parser.add_argument(
        "--entry-modes",
        type=_parse_csv,
        default=DEFAULT_ENTRY_MODES,
        help="Comma-separated entry modes: close_confirmed,high_touch_next_open.",
    )
    parser.add_argument(
        "--sizing-methods",
        type=_parse_csv,
        default=DEFAULT_SIZING_METHODS,
        help="Comma-separated sizing methods: equal_weight,inverse_atr.",
    )
    parser.add_argument(
        "--atr-sessions",
        type=int,
        default=DEFAULT_ATR_SESSIONS,
        help=f"ATR sessions for inverse-ATR sizing. Default: {DEFAULT_ATR_SESSIONS}.",
    )
    parser.add_argument(
        "--min-avg-trading-value-mil-jpy",
        type=float,
        default=DEFAULT_MIN_AVG_TRADING_VALUE_MIL_JPY,
        help="Minimum 60-session average trading value in million JPY.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_turtle_like_momentum_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_years=args.lookback_years,
        channel_specs=args.channel_specs,
        entry_modes=args.entry_modes,
        sizing_methods=args.sizing_methods,
        atr_sessions=args.atr_sessions,
        min_avg_trading_value_mil_jpy=args.min_avg_trading_value_mil_jpy,
    )
    bundle = write_turtle_like_momentum_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
