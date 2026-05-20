#!/usr/bin/env python3
"""Runner-first entrypoint for short-term shock forward-response research."""

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
from src.domains.analytics.short_term_shock_forward_response import (  # noqa: E402
    DEFAULT_CASE_STUDY_DATES,
    DEFAULT_CASE_STUDY_WINDOW_SESSIONS,
    DEFAULT_HORIZONS,
    DEFAULT_MARKET_SCOPES,
    DEFAULT_MARKET_SHOCK_THRESHOLDS,
    DEFAULT_MIN_OBSERVATIONS,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_PULLBACK_THRESHOLDS_20D,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    DEFAULT_UPTREND_THRESHOLDS_60D,
    run_short_term_shock_forward_response_research,
    write_short_term_shock_forward_response_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run PIT-safe short-term pullback and market-shock forward-response "
            "research over daily stock anchors."
        )
    )
    parser.add_argument(
        "--db-path",
        default=get_settings().market_db_path,
        help="Path to market.duckdb. Defaults to the active app setting.",
    )
    parser.add_argument("--start-date", default=None, help="Anchor start date.")
    parser.add_argument("--end-date", default=None, help="Anchor end date.")
    parser.add_argument(
        "--horizons",
        default=",".join(str(horizon) for horizon in DEFAULT_HORIZONS),
        help="Comma-separated forward return horizons in trading sessions.",
    )
    parser.add_argument(
        "--pullback-thresholds-20d",
        default=",".join(str(value) for value in DEFAULT_PULLBACK_THRESHOLDS_20D),
        help="Comma-separated non-negative 20d drawdown thresholds in percent.",
    )
    parser.add_argument(
        "--uptrend-thresholds-60d",
        default=",".join(str(value) for value in DEFAULT_UPTREND_THRESHOLDS_60D),
        help="Comma-separated non-negative 60d return thresholds in percent.",
    )
    parser.add_argument(
        "--market-shock-thresholds",
        default=",".join(str(value) for value in DEFAULT_MARKET_SHOCK_THRESHOLDS),
        help="Comma-separated negative TOPIX 1d shock thresholds in percent.",
    )
    parser.add_argument(
        "--sample-scopes",
        default="daily,weekly,monthly",
        help="Comma-separated sample scopes for general/pullback tables: daily, weekly, monthly.",
    )
    parser.add_argument(
        "--case-study-dates",
        default=",".join(DEFAULT_CASE_STUDY_DATES),
        help="Comma-separated TOPIX shock dates to break out as case studies.",
    )
    parser.add_argument(
        "--case-study-window-sessions",
        type=int,
        default=DEFAULT_CASE_STUDY_WINDOW_SESSIONS,
        help="Trading sessions before/after each case-study date.",
    )
    parser.add_argument(
        "--markets",
        default=",".join(DEFAULT_MARKET_SCOPES),
        help="Comma-separated market scopes to include: prime, standard, growth, unknown, or all.",
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=DEFAULT_MIN_OBSERVATIONS,
        help="Minimum observations required for a summarized row.",
    )
    parser.add_argument(
        "--severe-loss-threshold-pct",
        type=float,
        default=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
        help="Forward return threshold used for left-tail diagnostics.",
    )
    parser.add_argument(
        "--observation-sample-limit",
        type=int,
        default=DEFAULT_OBSERVATION_SAMPLE_LIMIT,
        help="Number of observation rows to persist as a sample table.",
    )
    parser.add_argument(
        "--core-only",
        action="store_true",
        help="Skip heavier stock-market and valuation interaction tables.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_short_term_shock_forward_response_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        horizons=_parse_positive_ints(args.horizons, name="horizons"),
        pullback_thresholds_20d=_parse_non_negative_floats(
            args.pullback_thresholds_20d,
            name="pullback-thresholds-20d",
        ),
        uptrend_thresholds_60d=_parse_non_negative_floats(
            args.uptrend_thresholds_60d,
            name="uptrend-thresholds-60d",
        ),
        market_shock_thresholds=_parse_negative_floats(
            args.market_shock_thresholds,
            name="market-shock-thresholds",
        ),
        sample_scopes=_parse_strings(args.sample_scopes),
        case_study_dates=_parse_strings(args.case_study_dates),
        case_study_window_sessions=args.case_study_window_sessions,
        market_scopes=_parse_strings(args.markets),
        min_observations=args.min_observations,
        severe_loss_threshold_pct=args.severe_loss_threshold_pct,
        observation_sample_limit=args.observation_sample_limit,
        core_only=args.core_only,
    )
    bundle = write_short_term_shock_forward_response_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


def _parse_positive_ints(value: str, *, name: str) -> tuple[int, ...]:
    values = tuple(sorted({int(part.strip()) for part in value.split(",") if part.strip()}))
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError(f"{name} must contain positive integers")
    return values


def _parse_non_negative_floats(value: str, *, name: str) -> tuple[float, ...]:
    values = tuple(sorted({float(part.strip()) for part in value.split(",") if part.strip()}))
    if not values or any(item < 0 for item in values):
        raise argparse.ArgumentTypeError(f"{name} must contain non-negative numbers")
    return values


def _parse_negative_floats(value: str, *, name: str) -> tuple[float, ...]:
    values = tuple(sorted({float(part.strip()) for part in value.split(",") if part.strip()}))
    if not values or any(item >= 0 for item in values):
        raise argparse.ArgumentTypeError(f"{name} must contain negative numbers")
    return values


def _parse_strings(value: str) -> tuple[str, ...]:
    values = tuple(part.strip() for part in value.split(",") if part.strip())
    if not values:
        raise argparse.ArgumentTypeError("value must contain at least one item")
    return values


if __name__ == "__main__":
    raise SystemExit(main())
