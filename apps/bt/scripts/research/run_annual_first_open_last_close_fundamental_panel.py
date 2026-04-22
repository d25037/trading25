#!/usr/bin/env python3
"""Runner-first entrypoint for annual first-open last-close fundamental research."""

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
from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (  # noqa: E402
    DEFAULT_ADV_WINDOW,
    DEFAULT_BUCKET_COUNT,
    DEFAULT_MARKETS,
    run_annual_first_open_last_close_fundamental_panel,
    write_annual_first_open_last_close_fundamental_panel_bundle,
)
from src.shared.paths.resolver import get_data_dir  # noqa: E402


def _default_db_path() -> str:
    return str((get_data_dir() / "market-timeseries" / "market.duckdb").resolve())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build an annual panel that buys each stock at the first trading day's "
            "open and exits at the last trading day's close, joined to PIT-safe "
            "FY fundamentals as of entry."
        )
    )
    parser.add_argument(
        "--db-path",
        default=_default_db_path(),
        help="market.duckdb path. Defaults to the active XDG market snapshot.",
    )
    parser.add_argument(
        "--market",
        action="append",
        dest="markets",
        default=None,
        help=(
            "Market to include. Repeat to override defaults "
            f"({', '.join(DEFAULT_MARKETS)}). Accepts prime, standard, growth "
            "or current market codes 0111/0112/0113."
        ),
    )
    parser.add_argument("--start-year", type=int, default=None, help="Optional first calendar year.")
    parser.add_argument("--end-year", type=int, default=None, help="Optional last calendar year.")
    parser.add_argument(
        "--bucket-count",
        type=int,
        default=DEFAULT_BUCKET_COUNT,
        help=f"Quantile bucket count for factor summaries. Default: {DEFAULT_BUCKET_COUNT}.",
    )
    parser.add_argument(
        "--adv-window",
        type=int,
        default=DEFAULT_ADV_WINDOW,
        help=f"Prior-session window for average trading value. Default: {DEFAULT_ADV_WINDOW}.",
    )
    parser.add_argument(
        "--include-incomplete-last-year",
        action="store_true",
        help="Include a final calendar year even when its last trading day is not year-end-like.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_annual_first_open_last_close_fundamental_panel(
        args.db_path,
        markets=tuple(args.markets or DEFAULT_MARKETS),
        start_year=args.start_year,
        end_year=args.end_year,
        bucket_count=args.bucket_count,
        adv_window=args.adv_window,
        include_incomplete_last_year=bool(args.include_incomplete_last_year),
    )
    bundle = write_annual_first_open_last_close_fundamental_panel_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
