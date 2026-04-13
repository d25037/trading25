"""Run TOPIX downside return-standard-deviation x trend x breadth overlay research."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

_BT_ROOT = Path(__file__).resolve().parents[2]
_BT_ROOT_STR = str(_BT_ROOT)
if _BT_ROOT_STR not in sys.path:
    sys.path.insert(0, _BT_ROOT_STR)

from scripts.research.common import add_bundle_output_arguments, emit_bundle_payload  # noqa: E402
from src.domains.analytics.topix_downside_return_standard_deviation_trend_breadth_overlay import (  # noqa: E402
    DEFAULT_BREADTH_RULES,
    DEFAULT_DISCOVERY_WINDOW_DAYS,
    DEFAULT_DOWNSIDE_RETURN_STANDARD_DEVIATION_MEAN_WINDOW_DAYS,
    DEFAULT_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_DAYS,
    DEFAULT_HIGH_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS,
    DEFAULT_LOW_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS,
    DEFAULT_MIN_CONSTITUENTS_PER_DAY,
    DEFAULT_RANK_TOP_KS,
    DEFAULT_REDUCED_EXPOSURE_RATIOS,
    DEFAULT_STEP_WINDOW_DAYS,
    DEFAULT_TREND_RULES,
    DEFAULT_VALIDATION_RATIO,
    DEFAULT_VALIDATION_WINDOW_DAYS,
    run_topix_downside_return_standard_deviation_trend_breadth_overlay_research,
    write_topix_downside_return_standard_deviation_trend_breadth_overlay_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402

DEFAULT_STD_WINDOW_SPEC = ",".join(
    str(value) for value in DEFAULT_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_DAYS
)
DEFAULT_MEAN_WINDOW_SPEC = ",".join(
    str(value) for value in DEFAULT_DOWNSIDE_RETURN_STANDARD_DEVIATION_MEAN_WINDOW_DAYS
)
DEFAULT_HIGH_THRESHOLD_SPEC = ",".join(
    f"{value:.2f}"
    for value in DEFAULT_HIGH_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS
)
DEFAULT_LOW_THRESHOLD_SPEC = ",".join(
    f"{value:.2f}"
    for value in DEFAULT_LOW_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS
)
DEFAULT_REDUCED_EXPOSURE_SPEC = ",".join(
    f"{value:.2f}" for value in DEFAULT_REDUCED_EXPOSURE_RATIOS
)
DEFAULT_TREND_RULE_SPEC = ",".join(DEFAULT_TREND_RULES)
DEFAULT_BREADTH_RULE_SPEC = ",".join(DEFAULT_BREADTH_RULES)
DEFAULT_RANK_TOP_K_SPEC = ",".join(str(value) for value in DEFAULT_RANK_TOP_KS)


def _parse_positive_int_sequence(raw: str) -> tuple[int, ...]:
    values: list[int] = []
    for token in raw.split(","):
        stripped = token.strip()
        if not stripped:
            continue
        try:
            value = int(stripped)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"Invalid integer token: {stripped!r}"
            ) from exc
        if value <= 0:
            raise argparse.ArgumentTypeError(
                f"Values must be positive integers: {stripped!r}"
            )
        values.append(value)
    if not values:
        raise argparse.ArgumentTypeError("Provide at least one positive integer")
    return tuple(sorted(set(values)))


def _parse_non_negative_float_sequence(raw: str) -> tuple[float, ...]:
    values: list[float] = []
    for token in raw.split(","):
        stripped = token.strip()
        if not stripped:
            continue
        try:
            value = float(stripped)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"Invalid numeric token: {stripped!r}"
            ) from exc
        if value < 0.0:
            raise argparse.ArgumentTypeError(
                f"Values must be non-negative: {stripped!r}"
            )
        values.append(value)
    if not values:
        raise argparse.ArgumentTypeError("Provide at least one non-negative number")
    return tuple(sorted(set(values)))


def _parse_string_sequence(raw: str) -> tuple[str, ...]:
    values = tuple(token.strip() for token in raw.split(",") if token.strip())
    if not values:
        raise argparse.ArgumentTypeError("Provide at least one non-empty value")
    return tuple(dict.fromkeys(values))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run TOPIX downside return-standard-deviation x trend x breadth "
            "overlay research and persist a reproducible artifact bundle under "
            "~/.local/share/trading25/research/."
        )
    )
    parser.add_argument("--db-path", default=str(get_settings().market_db_path))
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument(
        "--downside-return-standard-deviation-window-days",
        default=DEFAULT_STD_WINDOW_SPEC,
    )
    parser.add_argument(
        "--downside-return-standard-deviation-mean-window-days",
        default=DEFAULT_MEAN_WINDOW_SPEC,
    )
    parser.add_argument(
        "--high-annualized-downside-return-standard-deviation-thresholds",
        default=DEFAULT_HIGH_THRESHOLD_SPEC,
    )
    parser.add_argument(
        "--low-annualized-downside-return-standard-deviation-thresholds",
        default=DEFAULT_LOW_THRESHOLD_SPEC,
    )
    parser.add_argument(
        "--reduced-exposure-ratios",
        default=DEFAULT_REDUCED_EXPOSURE_SPEC,
    )
    parser.add_argument("--trend-rules", default=DEFAULT_TREND_RULE_SPEC)
    parser.add_argument("--breadth-rules", default=DEFAULT_BREADTH_RULE_SPEC)
    parser.add_argument(
        "--min-constituents-per-day",
        type=int,
        default=DEFAULT_MIN_CONSTITUENTS_PER_DAY,
    )
    parser.add_argument(
        "--validation-ratio",
        type=float,
        default=DEFAULT_VALIDATION_RATIO,
    )
    parser.add_argument("--rank-top-ks", default=DEFAULT_RANK_TOP_K_SPEC)
    parser.add_argument(
        "--discovery-window-days",
        type=int,
        default=DEFAULT_DISCOVERY_WINDOW_DAYS,
    )
    parser.add_argument(
        "--validation-window-days",
        type=int,
        default=DEFAULT_VALIDATION_WINDOW_DAYS,
    )
    parser.add_argument(
        "--step-window-days",
        type=int,
        default=DEFAULT_STEP_WINDOW_DAYS,
    )
    add_bundle_output_arguments(parser)
    args = parser.parse_args(argv)
    args.downside_return_standard_deviation_window_days = _parse_positive_int_sequence(
        args.downside_return_standard_deviation_window_days
    )
    args.downside_return_standard_deviation_mean_window_days = _parse_positive_int_sequence(
        args.downside_return_standard_deviation_mean_window_days
    )
    args.high_annualized_downside_return_standard_deviation_thresholds = (
        _parse_non_negative_float_sequence(
            args.high_annualized_downside_return_standard_deviation_thresholds
        )
    )
    args.low_annualized_downside_return_standard_deviation_thresholds = (
        _parse_non_negative_float_sequence(
            args.low_annualized_downside_return_standard_deviation_thresholds
        )
    )
    args.reduced_exposure_ratios = _parse_non_negative_float_sequence(
        args.reduced_exposure_ratios
    )
    args.trend_rules = _parse_string_sequence(args.trend_rules)
    args.breadth_rules = _parse_string_sequence(args.breadth_rules)
    args.rank_top_ks = _parse_positive_int_sequence(args.rank_top_ks)
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_topix_downside_return_standard_deviation_trend_breadth_overlay_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        downside_return_standard_deviation_window_days=(
            args.downside_return_standard_deviation_window_days
        ),
        downside_return_standard_deviation_mean_window_days=(
            args.downside_return_standard_deviation_mean_window_days
        ),
        high_annualized_downside_return_standard_deviation_thresholds=(
            args.high_annualized_downside_return_standard_deviation_thresholds
        ),
        low_annualized_downside_return_standard_deviation_thresholds=(
            args.low_annualized_downside_return_standard_deviation_thresholds
        ),
        reduced_exposure_ratios=args.reduced_exposure_ratios,
        trend_rules=args.trend_rules,
        breadth_rules=args.breadth_rules,
        min_constituents_per_day=args.min_constituents_per_day,
        validation_ratio=args.validation_ratio,
        rank_top_ks=args.rank_top_ks,
        discovery_window_days=args.discovery_window_days,
        validation_window_days=args.validation_window_days,
        step_window_days=args.step_window_days,
    )
    bundle = write_topix_downside_return_standard_deviation_trend_breadth_overlay_research_bundle(
        result,
        output_root=Path(args.output_root) if args.output_root else None,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
