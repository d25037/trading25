#!/usr/bin/env python3
"""Runner-first entrypoint for pre-earnings EPS 1.2x proxy research."""

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
from src.domains.analytics.pre_earnings_eps120_proxy import (  # noqa: E402
    DEFAULT_MIN_EVENTS,
    run_pre_earnings_eps120_proxy_research,
    write_pre_earnings_eps120_proxy_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run PIT-safe pre-earnings valuation proxy research for EPS 1.2x "
            "positive FY disclosures."
        )
    )
    parser.add_argument(
        "--db-path",
        default=get_settings().market_db_path,
        help="Path to market.duckdb. Defaults to the active app setting.",
    )
    parser.add_argument("--start-date", default=None, help="Disclosure start date.")
    parser.add_argument("--end-date", default=None, help="Disclosure end date.")
    parser.add_argument(
        "--min-events",
        type=int,
        default=DEFAULT_MIN_EVENTS,
        help="Minimum event count for reported buckets and grid rows.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_pre_earnings_eps120_proxy_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        min_events=args.min_events,
    )
    bundle = write_pre_earnings_eps120_proxy_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
