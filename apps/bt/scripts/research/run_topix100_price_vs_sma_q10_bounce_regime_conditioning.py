#!/usr/bin/env python3
"""Runner-first entrypoint for TOPIX100 Q10 bounce regime conditioning research."""

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
from src.domains.analytics.topix100_price_vs_sma_q10_bounce_regime_conditioning import (  # noqa: E402
    DEFAULT_PRICE_FEATURE,
    DEFAULT_VOLUME_FEATURE,
    run_topix100_price_vs_sma_q10_bounce_regime_conditioning_research,
    write_topix100_price_vs_sma_q10_bounce_regime_conditioning_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run TOPIX100 Q10 bounce regime conditioning research and persist a "
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
        "--price-feature",
        default=DEFAULT_PRICE_FEATURE,
        help="Price feature key to analyze.",
    )
    parser.add_argument(
        "--volume-feature",
        default=DEFAULT_VOLUME_FEATURE,
        help="Volume feature key to analyze.",
    )
    parser.add_argument(
        "--sigma-threshold-1",
        type=float,
        default=1.0,
        help="First sigma threshold used for regime grouping.",
    )
    parser.add_argument(
        "--sigma-threshold-2",
        type=float,
        default=2.0,
        help="Second sigma threshold used for regime grouping.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_topix100_price_vs_sma_q10_bounce_regime_conditioning_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_years=args.lookback_years,
        min_constituents_per_day=args.min_constituents_per_day,
        price_feature=args.price_feature,
        volume_feature=args.volume_feature,
        sigma_threshold_1=args.sigma_threshold_1,
        sigma_threshold_2=args.sigma_threshold_2,
    )
    bundle = write_topix100_price_vs_sma_q10_bounce_regime_conditioning_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
