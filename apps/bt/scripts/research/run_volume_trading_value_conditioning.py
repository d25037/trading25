#!/usr/bin/env python3
"""Runner-first entrypoint for volume-vs-trading-value conditioning research."""

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
from src.domains.analytics.volume_trading_value_conditioning import (  # noqa: E402
    DEFAULT_HORIZONS,
    DEFAULT_LOOKBACK_YEARS,
    DEFAULT_LONG_WINDOWS,
    DEFAULT_MIN_SIGNAL_EVENTS,
    DEFAULT_MIN_UNIQUE_CODES,
    DEFAULT_SAMPLE_SEED,
    DEFAULT_SAMPLE_SIZE_PER_UNIVERSE,
    DEFAULT_SHORT_WINDOWS,
    DEFAULT_THRESHOLDS,
    DEFAULT_TOP_K,
    DEFAULT_VALIDATION_RATIO,
    run_volume_trading_value_conditioning_research,
    write_volume_trading_value_conditioning_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def _parse_int_csv(value: str) -> tuple[int, ...]:
    return tuple(int(part.strip()) for part in value.split(",") if part.strip())


def _parse_float_csv(value: str) -> tuple[float, ...]:
    return tuple(float(part.strip()) for part in value.split(",") if part.strip())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run conditioned raw-volume vs trading-value surge research and persist "
            "a reproducible artifact bundle under ~/.local/share/trading25/research/."
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
        "--validation-ratio",
        type=float,
        default=DEFAULT_VALIDATION_RATIO,
        help="Fraction of analysis dates reserved for validation.",
    )
    parser.add_argument(
        "--analysis-use-sampled-codes",
        action="store_true",
        help="Restrict the analysis panel to the deterministic sampled codes per universe.",
    )
    parser.add_argument(
        "--short-windows",
        type=_parse_int_csv,
        default=DEFAULT_SHORT_WINDOWS,
        help="Comma-separated short MA windows for surge ratios.",
    )
    parser.add_argument(
        "--long-windows",
        type=_parse_int_csv,
        default=DEFAULT_LONG_WINDOWS,
        help="Comma-separated long MA windows for surge ratios.",
    )
    parser.add_argument(
        "--thresholds",
        type=_parse_float_csv,
        default=DEFAULT_THRESHOLDS,
        help="Comma-separated surge thresholds.",
    )
    parser.add_argument(
        "--horizons",
        type=_parse_int_csv,
        default=DEFAULT_HORIZONS,
        help="Comma-separated future-close horizons in trading days.",
    )
    parser.add_argument(
        "--sample-seed",
        type=int,
        default=DEFAULT_SAMPLE_SEED,
        help="Seed token for deterministic code samples.",
    )
    parser.add_argument(
        "--sample-size-per-universe",
        type=int,
        default=DEFAULT_SAMPLE_SIZE_PER_UNIVERSE,
        help="Deterministic sample size per universe when sampled analysis is enabled.",
    )
    parser.add_argument(
        "--min-signal-events",
        type=int,
        default=DEFAULT_MIN_SIGNAL_EVENTS,
        help="Minimum signal events required for conditioned-bucket ranking.",
    )
    parser.add_argument(
        "--min-unique-codes",
        type=int,
        default=DEFAULT_MIN_UNIQUE_CODES,
        help="Minimum unique codes required for conditioned-bucket ranking.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=DEFAULT_TOP_K,
        help="Top conditioned buckets to retain per universe / signal family / split / horizon.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_volume_trading_value_conditioning_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_years=args.lookback_years,
        validation_ratio=args.validation_ratio,
        analysis_use_sampled_codes=args.analysis_use_sampled_codes,
        short_windows=args.short_windows,
        long_windows=args.long_windows,
        threshold_values=args.thresholds,
        horizons=args.horizons,
        sample_seed=args.sample_seed,
        sample_size_per_universe=args.sample_size_per_universe,
        min_signal_events=args.min_signal_events,
        min_unique_codes=args.min_unique_codes,
        top_k=args.top_k,
    )
    bundle = write_volume_trading_value_conditioning_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
