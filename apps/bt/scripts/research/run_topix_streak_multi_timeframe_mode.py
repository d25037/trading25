"""Run TOPIX streak multi-timeframe mode research and persist a research bundle."""

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
from src.domains.analytics.topix_streak_extreme_mode import (  # noqa: E402
    DEFAULT_CANDIDATE_WINDOWS,
    DEFAULT_FUTURE_HORIZONS,
    DEFAULT_MIN_MODE_CANDLES,
    DEFAULT_VALIDATION_RATIO,
)
from src.domains.analytics.topix_streak_multi_timeframe_mode import (  # noqa: E402
    DEFAULT_MIN_STATE_OBSERVATIONS,
    DEFAULT_PAIR_STABILITY_HORIZONS,
    run_topix_streak_multi_timeframe_mode_research,
    write_topix_streak_multi_timeframe_mode_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402

DEFAULT_CANDIDATE_WINDOW_SPEC = (
    f"{DEFAULT_CANDIDATE_WINDOWS[0]}:{DEFAULT_CANDIDATE_WINDOWS[-1]}"
)
DEFAULT_FUTURE_HORIZON_SPEC = ",".join(
    str(value) for value in DEFAULT_FUTURE_HORIZONS
)
DEFAULT_STABILITY_HORIZON_SPEC = ",".join(
    str(value) for value in DEFAULT_PAIR_STABILITY_HORIZONS
)


def _parse_positive_int_sequence(raw: str) -> tuple[int, ...]:
    values: set[int] = set()
    for token in raw.split(","):
        stripped = token.strip()
        if not stripped:
            continue
        if ":" in stripped:
            parts = [part.strip() for part in stripped.split(":")]
            if len(parts) not in (2, 3):
                raise argparse.ArgumentTypeError(
                    f"Invalid range token: {stripped!r}. Expected start:end or start:end:step."
                )
            try:
                start = int(parts[0])
                end = int(parts[1])
                step = int(parts[2]) if len(parts) == 3 else 1
            except ValueError as exc:
                raise argparse.ArgumentTypeError(
                    f"Invalid integer in range token: {stripped!r}"
                ) from exc
            if start <= 0 or end <= 0 or step <= 0 or end < start:
                raise argparse.ArgumentTypeError(
                    f"Invalid positive range token: {stripped!r}"
                )
            values.update(range(start, end + 1, step))
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
            "Run TOPIX streak multi-timeframe mode research and persist a "
            "reproducible artifact bundle under ~/.local/share/trading25/research/."
        )
    )
    parser.add_argument("--db-path", default=str(get_settings().market_db_path))
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument(
        "--candidate-windows",
        default=DEFAULT_CANDIDATE_WINDOW_SPEC,
        help=(
            "Comma-separated integers and/or inclusive ranges over streak candles. "
            "Examples: 2,3,5 or 2:60 or 2:20:2."
        ),
    )
    parser.add_argument(
        "--future-horizons",
        default=DEFAULT_FUTURE_HORIZON_SPEC,
        help="Comma-separated future horizons in trading days from streak end. Example: 1,5,10,20.",
    )
    parser.add_argument(
        "--stability-horizons",
        default=DEFAULT_STABILITY_HORIZON_SPEC,
        help="Comma-separated subset of future horizons used for short/long pair ranking.",
    )
    parser.add_argument(
        "--validation-ratio",
        type=float,
        default=DEFAULT_VALIDATION_RATIO,
    )
    parser.add_argument(
        "--min-mode-candles",
        type=int,
        default=DEFAULT_MIN_MODE_CANDLES,
    )
    parser.add_argument(
        "--min-state-observations",
        type=int,
        default=DEFAULT_MIN_STATE_OBSERVATIONS,
    )
    add_bundle_output_arguments(parser)
    args = parser.parse_args(argv)
    args.candidate_windows = _parse_positive_int_sequence(args.candidate_windows)
    args.future_horizons = _parse_positive_int_sequence(args.future_horizons)
    args.stability_horizons = _parse_positive_int_sequence(args.stability_horizons)
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_topix_streak_multi_timeframe_mode_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        candidate_windows=args.candidate_windows,
        future_horizons=args.future_horizons,
        stability_horizons=args.stability_horizons,
        validation_ratio=args.validation_ratio,
        min_mode_candles=args.min_mode_candles,
        min_state_observations=args.min_state_observations,
    )
    bundle = write_topix_streak_multi_timeframe_mode_research_bundle(
        result,
        output_root=Path(args.output_root) if args.output_root else None,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
