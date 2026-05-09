#!/usr/bin/env python3
"""Runner-first entrypoint for classical momentum research."""

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
from src.domains.analytics.classical_momentum_research import (  # noqa: E402
    DEFAULT_HOLD_SESSIONS,
    DEFAULT_LOOKBACK_SPECS,
    DEFAULT_LOOKBACK_YEARS,
    DEFAULT_MIN_AVG_TRADING_VALUE_MIL_JPY,
    DEFAULT_REBALANCE_INTERVAL_SESSIONS,
    DEFAULT_SELECTION_FRACTIONS,
    run_classical_momentum_research,
    write_classical_momentum_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def _parse_int_csv(value: str) -> tuple[int, ...]:
    return tuple(int(part.strip()) for part in value.split(",") if part.strip())


def _parse_float_csv(value: str) -> tuple[float, ...]:
    return tuple(float(part.strip()) for part in value.split(",") if part.strip())


def _parse_lookback_specs(value: str) -> tuple[tuple[int, int], ...]:
    specs: list[tuple[int, int]] = []
    for part in value.split(","):
        item = part.strip()
        if not item:
            continue
        lookback_raw, skip_raw = item.split(":", 1)
        specs.append((int(lookback_raw), int(skip_raw)))
    return tuple(specs)


def _format_lookback_specs_default() -> str:
    return ",".join(f"{lookback}:{skip}" for lookback, skip in DEFAULT_LOOKBACK_SPECS)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run classical cross-sectional momentum research and persist a "
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
        default=DEFAULT_LOOKBACK_YEARS,
        help="Default lookback years when --start-date is omitted.",
    )
    parser.add_argument(
        "--lookback-specs",
        type=_parse_lookback_specs,
        default=DEFAULT_LOOKBACK_SPECS,
        help=(
            "Comma-separated lookback:skip specs in sessions "
            f"(default: {_format_lookback_specs_default()})."
        ),
    )
    parser.add_argument(
        "--hold-sessions",
        type=_parse_int_csv,
        default=DEFAULT_HOLD_SESSIONS,
        help="Comma-separated holding periods in sessions.",
    )
    parser.add_argument(
        "--selection-fractions",
        type=_parse_float_csv,
        default=DEFAULT_SELECTION_FRACTIONS,
        help="Comma-separated top cross-section fractions, e.g. 0.05,0.10.",
    )
    parser.add_argument(
        "--rebalance-interval-sessions",
        type=int,
        default=DEFAULT_REBALANCE_INTERVAL_SESSIONS,
        help="Rebalance every N sessions.",
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
    result = run_classical_momentum_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_years=args.lookback_years,
        lookback_specs=args.lookback_specs,
        hold_sessions=args.hold_sessions,
        selection_fractions=args.selection_fractions,
        rebalance_interval_sessions=args.rebalance_interval_sessions,
        min_avg_trading_value_mil_jpy=args.min_avg_trading_value_mil_jpy,
    )
    bundle = write_classical_momentum_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
