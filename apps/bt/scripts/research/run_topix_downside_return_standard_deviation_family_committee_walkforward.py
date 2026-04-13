"""Run the TOPIX downside return-standard-deviation family committee walk-forward study."""

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
from src.domains.analytics.topix_downside_return_standard_deviation_family_committee_walkforward import (  # noqa: E402
    DEFAULT_COMMITTEE_SIZES,
    DEFAULT_DISCOVERY_WINDOW_DAYS,
    DEFAULT_FAMILY_DOWNSIDE_RETURN_STANDARD_DEVIATION_MEAN_WINDOW_DAYS,
    DEFAULT_FAMILY_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_DAYS,
    DEFAULT_FAMILY_HIGH_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS,
    DEFAULT_FAMILY_LOW_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS,
    DEFAULT_FAMILY_REDUCED_EXPOSURE_RATIOS,
    DEFAULT_RANK_TOP_KS,
    DEFAULT_STEP_WINDOW_DAYS,
    DEFAULT_VALIDATION_RATIO,
    DEFAULT_VALIDATION_WINDOW_DAYS,
    run_topix_downside_return_standard_deviation_family_committee_walkforward_research,
    write_topix_downside_return_standard_deviation_family_committee_walkforward_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402

DEFAULT_FAMILY_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_SPEC = ",".join(
    str(value) for value in DEFAULT_FAMILY_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_DAYS
)
DEFAULT_FAMILY_DOWNSIDE_RETURN_STANDARD_DEVIATION_MEAN_WINDOW_SPEC = ",".join(
    str(value) for value in DEFAULT_FAMILY_DOWNSIDE_RETURN_STANDARD_DEVIATION_MEAN_WINDOW_DAYS
)
DEFAULT_HIGH_THRESHOLD_SPEC = ",".join(
    f"{value:.2f}"
    for value in DEFAULT_FAMILY_HIGH_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS
)
DEFAULT_LOW_THRESHOLD_SPEC = ",".join(
    f"{value:.2f}"
    for value in DEFAULT_FAMILY_LOW_ANNUALIZED_DOWNSIDE_RETURN_STANDARD_DEVIATION_THRESHOLDS
)
DEFAULT_REDUCED_EXPOSURE_SPEC = ",".join(
    f"{value:.2f}" for value in DEFAULT_FAMILY_REDUCED_EXPOSURE_RATIOS
)
DEFAULT_COMMITTEE_SIZE_SPEC = ",".join(str(value) for value in DEFAULT_COMMITTEE_SIZES)
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


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the TOPIX downside return-standard-deviation family committee "
            "walk-forward study and persist a reproducible artifact bundle under "
            "~/.local/share/trading25/research/."
        )
    )
    parser.add_argument("--db-path", default=str(get_settings().market_db_path))
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument(
        "--fixed-split-validation-ratio",
        type=float,
        default=DEFAULT_VALIDATION_RATIO,
    )
    parser.add_argument(
        "--family-downside-return-standard-deviation-window-days",
        default=DEFAULT_FAMILY_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_SPEC,
    )
    parser.add_argument(
        "--family-downside-return-standard-deviation-mean-window-days",
        default=DEFAULT_FAMILY_DOWNSIDE_RETURN_STANDARD_DEVIATION_MEAN_WINDOW_SPEC,
    )
    parser.add_argument(
        "--family-high-annualized-downside-return-standard-deviation-thresholds",
        default=DEFAULT_HIGH_THRESHOLD_SPEC,
    )
    parser.add_argument(
        "--family-low-annualized-downside-return-standard-deviation-thresholds",
        default=DEFAULT_LOW_THRESHOLD_SPEC,
    )
    parser.add_argument(
        "--family-reduced-exposure-ratios",
        default=DEFAULT_REDUCED_EXPOSURE_SPEC,
    )
    parser.add_argument(
        "--committee-sizes",
        default=DEFAULT_COMMITTEE_SIZE_SPEC,
    )
    parser.add_argument(
        "--rank-top-ks",
        default=DEFAULT_RANK_TOP_K_SPEC,
    )
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
    args.family_downside_return_standard_deviation_window_days = _parse_positive_int_sequence(
        args.family_downside_return_standard_deviation_window_days
    )
    args.family_downside_return_standard_deviation_mean_window_days = (
        _parse_positive_int_sequence(
            args.family_downside_return_standard_deviation_mean_window_days
        )
    )
    args.family_high_annualized_downside_return_standard_deviation_thresholds = (
        _parse_non_negative_float_sequence(
            args.family_high_annualized_downside_return_standard_deviation_thresholds
        )
    )
    args.family_low_annualized_downside_return_standard_deviation_thresholds = (
        _parse_non_negative_float_sequence(
            args.family_low_annualized_downside_return_standard_deviation_thresholds
        )
    )
    args.family_reduced_exposure_ratios = _parse_non_negative_float_sequence(
        args.family_reduced_exposure_ratios
    )
    args.committee_sizes = _parse_positive_int_sequence(args.committee_sizes)
    args.rank_top_ks = _parse_positive_int_sequence(args.rank_top_ks)
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_topix_downside_return_standard_deviation_family_committee_walkforward_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        fixed_split_validation_ratio=args.fixed_split_validation_ratio,
        family_downside_return_standard_deviation_window_days=(
            args.family_downside_return_standard_deviation_window_days
        ),
        family_downside_return_standard_deviation_mean_window_days=(
            args.family_downside_return_standard_deviation_mean_window_days
        ),
        family_high_annualized_downside_return_standard_deviation_thresholds=(
            args.family_high_annualized_downside_return_standard_deviation_thresholds
        ),
        family_low_annualized_downside_return_standard_deviation_thresholds=(
            args.family_low_annualized_downside_return_standard_deviation_thresholds
        ),
        family_reduced_exposure_ratios=args.family_reduced_exposure_ratios,
        committee_sizes=args.committee_sizes,
        rank_top_ks=args.rank_top_ks,
        discovery_window_days=args.discovery_window_days,
        validation_window_days=args.validation_window_days,
        step_window_days=args.step_window_days,
    )
    bundle = (
        write_topix_downside_return_standard_deviation_family_committee_walkforward_research_bundle(
            result,
            output_root=Path(args.output_root) if args.output_root else None,
            run_id=args.run_id,
            notes=args.notes,
        )
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
