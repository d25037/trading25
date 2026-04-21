#!/usr/bin/env python3
"""Runner-first entrypoint for accumulation-flow follow-through research."""

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
from src.domains.analytics.accumulation_flow_followthrough import (  # noqa: E402
    DEFAULT_CHAIKIN_FAST_PERIOD,
    DEFAULT_CHAIKIN_OSCILLATOR_THRESHOLD,
    DEFAULT_CHAIKIN_SLOW_PERIOD,
    DEFAULT_CMF_PERIOD,
    DEFAULT_CMF_THRESHOLD,
    DEFAULT_CONCENTRATION_CAPS,
    DEFAULT_OBV_LOOKBACK_PERIOD,
    DEFAULT_HORIZONS,
    DEFAULT_LOOKBACK_YEARS,
    DEFAULT_LOWER_WICK_THRESHOLD,
    DEFAULT_MAX_CLOSE_TO_HIGH,
    DEFAULT_MAX_CLOSE_TO_SMA,
    DEFAULT_MIN_VOTES,
    DEFAULT_OBV_SCORE_THRESHOLD,
    DEFAULT_PRICE_HIGH_LOOKBACK_PERIOD,
    DEFAULT_PRICE_SMA_PERIOD,
    run_accumulation_flow_followthrough_research,
    write_accumulation_flow_followthrough_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def _parse_int_csv(value: str) -> list[int]:
    return [int(part.strip()) for part in value.split(",") if part.strip()]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Study next-open follow-through after CMF/Chaikin/OBV accumulation-pressure events."
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
        help="Optional inclusive signal start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional inclusive signal end date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--lookback-years",
        type=int,
        default=DEFAULT_LOOKBACK_YEARS,
        help="Default lookback window when --start-date is omitted.",
    )
    parser.add_argument(
        "--horizons",
        type=_parse_int_csv,
        default=list(DEFAULT_HORIZONS),
        help="Comma-separated forward close horizons in sessions.",
    )
    parser.add_argument("--cmf-period", type=int, default=DEFAULT_CMF_PERIOD)
    parser.add_argument(
        "--chaikin-fast-period",
        type=int,
        default=DEFAULT_CHAIKIN_FAST_PERIOD,
    )
    parser.add_argument(
        "--chaikin-slow-period",
        type=int,
        default=DEFAULT_CHAIKIN_SLOW_PERIOD,
    )
    parser.add_argument(
        "--obv-lookback-period",
        type=int,
        default=DEFAULT_OBV_LOOKBACK_PERIOD,
    )
    parser.add_argument("--cmf-threshold", type=float, default=DEFAULT_CMF_THRESHOLD)
    parser.add_argument(
        "--chaikin-oscillator-threshold",
        type=float,
        default=DEFAULT_CHAIKIN_OSCILLATOR_THRESHOLD,
    )
    parser.add_argument(
        "--obv-score-threshold",
        type=float,
        default=DEFAULT_OBV_SCORE_THRESHOLD,
    )
    parser.add_argument("--min-votes", type=int, default=DEFAULT_MIN_VOTES)
    parser.add_argument("--price-sma-period", type=int, default=DEFAULT_PRICE_SMA_PERIOD)
    parser.add_argument(
        "--price-high-lookback-period",
        type=int,
        default=DEFAULT_PRICE_HIGH_LOOKBACK_PERIOD,
    )
    parser.add_argument("--max-close-to-sma", type=float, default=DEFAULT_MAX_CLOSE_TO_SMA)
    parser.add_argument(
        "--max-close-to-high",
        type=float,
        default=DEFAULT_MAX_CLOSE_TO_HIGH,
    )
    parser.add_argument(
        "--lower-wick-threshold",
        type=float,
        default=DEFAULT_LOWER_WICK_THRESHOLD,
    )
    parser.add_argument(
        "--concentration-caps",
        type=_parse_int_csv,
        default=list(DEFAULT_CONCENTRATION_CAPS),
        help="Comma-separated max names per entry date for capped portfolio lenses.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_accumulation_flow_followthrough_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_years=args.lookback_years,
        horizons=args.horizons,
        cmf_period=args.cmf_period,
        chaikin_fast_period=args.chaikin_fast_period,
        chaikin_slow_period=args.chaikin_slow_period,
        obv_lookback_period=args.obv_lookback_period,
        cmf_threshold=args.cmf_threshold,
        chaikin_oscillator_threshold=args.chaikin_oscillator_threshold,
        obv_score_threshold=args.obv_score_threshold,
        min_votes=args.min_votes,
        price_sma_period=args.price_sma_period,
        price_high_lookback_period=args.price_high_lookback_period,
        max_close_to_sma=args.max_close_to_sma,
        max_close_to_high=args.max_close_to_high,
        lower_wick_threshold=args.lower_wick_threshold,
        concentration_caps=args.concentration_caps,
    )
    bundle = write_accumulation_flow_followthrough_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
