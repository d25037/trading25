#!/usr/bin/env python3
"""Runner-first entrypoint for index market-strength research."""

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
from src.domains.analytics.index_market_strength_research import (  # noqa: E402
    DEFAULT_HORIZON_SESSIONS,
    DEFAULT_LOOKBACK_WINDOWS,
    run_index_market_strength_research,
    write_index_market_strength_research_bundle,
)
from src.shared.paths.resolver import get_data_dir  # noqa: E402


def _default_market_db_path() -> str:
    return str((get_data_dir() / "market-timeseries" / "market.duckdb").resolve())


def _parse_lookbacks(raw: str) -> tuple[int, ...]:
    values = tuple(int(part.strip()) for part in raw.split(",") if part.strip())
    if not values:
        raise argparse.ArgumentTypeError("lookbacks must contain at least one integer")
    return values


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Classify sector33 index market states from N-day rebound, N-day return, "
            "and N-day price position, then measure forward returns."
        )
    )
    parser.add_argument(
        "--db-path",
        default=_default_market_db_path(),
        help="market.duckdb path. Defaults to the active XDG market snapshot.",
    )
    parser.add_argument(
        "--lookbacks",
        type=_parse_lookbacks,
        default=DEFAULT_LOOKBACK_WINDOWS,
        help=(
            "Comma-separated lookback session counts. "
            f"Default: {','.join(str(v) for v in DEFAULT_LOOKBACK_WINDOWS)}."
        ),
    )
    parser.add_argument(
        "--horizon-sessions",
        type=int,
        default=DEFAULT_HORIZON_SESSIONS,
        help=f"Forward return horizon in trading sessions. Default: {DEFAULT_HORIZON_SESSIONS}.",
    )
    parser.add_argument(
        "--target-category",
        default="sector33",
        help="index_master category to analyze. Default: sector33.",
    )
    parser.add_argument(
        "--min-summary-observations",
        type=int,
        default=20,
        help="Minimum observations required for a summary bucket. Default: 20.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_index_market_strength_research(
        args.db_path,
        lookback_windows=args.lookbacks,
        horizon_sessions=args.horizon_sessions,
        target_category_prefix=args.target_category,
        min_summary_observations=args.min_summary_observations,
    )
    bundle = write_index_market_strength_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
