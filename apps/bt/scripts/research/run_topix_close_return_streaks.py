"""Run TOPIX close return streak research and persist a research bundle."""

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
from src.domains.analytics.topix_close_return_streaks import (  # noqa: E402
    DEFAULT_FUTURE_HORIZONS,
    DEFAULT_MAX_SEGMENT_LENGTH_BUCKET,
    DEFAULT_MAX_STREAK_DAY_BUCKET,
    DEFAULT_VALIDATION_RATIO,
    run_topix_close_return_streaks_research,
    write_topix_close_return_streaks_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402

DEFAULT_FUTURE_HORIZON_SPEC = ",".join(str(value) for value in DEFAULT_FUTURE_HORIZONS)


def _parse_positive_int_sequence(raw: str) -> tuple[int, ...]:
    values: set[int] = set()
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
        values.add(value)

    if not values:
        raise argparse.ArgumentTypeError("Provide at least one positive integer")
    return tuple(sorted(values))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run TOPIX close return streak research and persist a reproducible "
            "artifact bundle under ~/.local/share/trading25/research/."
        )
    )
    parser.add_argument("--db-path", default=str(get_settings().market_db_path))
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument(
        "--future-horizons",
        default=DEFAULT_FUTURE_HORIZON_SPEC,
        help="Comma-separated future horizons in trading days. Example: 1,5,10,20.",
    )
    parser.add_argument(
        "--validation-ratio",
        type=float,
        default=DEFAULT_VALIDATION_RATIO,
    )
    parser.add_argument(
        "--max-streak-day-bucket",
        type=int,
        default=DEFAULT_MAX_STREAK_DAY_BUCKET,
    )
    parser.add_argument(
        "--max-segment-length-bucket",
        type=int,
        default=DEFAULT_MAX_SEGMENT_LENGTH_BUCKET,
    )
    add_bundle_output_arguments(parser)
    args = parser.parse_args(argv)
    args.future_horizons = _parse_positive_int_sequence(args.future_horizons)
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_topix_close_return_streaks_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        future_horizons=args.future_horizons,
        validation_ratio=args.validation_ratio,
        max_streak_day_bucket=args.max_streak_day_bucket,
        max_segment_length_bucket=args.max_segment_length_bucket,
    )
    bundle = write_topix_close_return_streaks_research_bundle(
        result,
        output_root=Path(args.output_root) if args.output_root else None,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
