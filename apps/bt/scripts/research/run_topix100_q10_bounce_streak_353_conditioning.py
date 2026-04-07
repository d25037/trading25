"""Run TOPIX100 Q10 bounce x streak 3/53 conditioning and persist a bundle."""

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
from src.domains.analytics.topix100_price_vs_sma_q10_bounce_regime_conditioning import (  # noqa: E402
    DEFAULT_PRICE_FEATURE,
    DEFAULT_VOLUME_FEATURE,
)
from src.domains.analytics.topix100_q10_bounce_streak_353_conditioning import (  # noqa: E402
    DEFAULT_MIN_CONSTITUENTS_PER_BUCKET_STATE_DATE,
    run_topix100_q10_bounce_streak_353_conditioning_research,
    write_topix100_q10_bounce_streak_353_conditioning_research_bundle,
)
from src.domains.analytics.topix100_streak_353_transfer import (  # noqa: E402
    DEFAULT_LONG_WINDOW_STREAKS,
    DEFAULT_SHORT_WINDOW_STREAKS,
)
from src.domains.analytics.topix_close_return_streaks import DEFAULT_VALIDATION_RATIO  # noqa: E402
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the TOPIX100 Q10 bounce x streak 3/53 conditioning study and "
            "persist a reproducible bundle under ~/.local/share/trading25/research/."
        )
    )
    parser.add_argument("--db-path", default=str(get_settings().market_db_path))
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--price-feature", default=DEFAULT_PRICE_FEATURE)
    parser.add_argument("--volume-feature", default=DEFAULT_VOLUME_FEATURE)
    parser.add_argument(
        "--short-window-streaks",
        type=int,
        default=DEFAULT_SHORT_WINDOW_STREAKS,
    )
    parser.add_argument(
        "--long-window-streaks",
        type=int,
        default=DEFAULT_LONG_WINDOW_STREAKS,
    )
    parser.add_argument(
        "--validation-ratio",
        type=float,
        default=DEFAULT_VALIDATION_RATIO,
    )
    parser.add_argument(
        "--min-constituents-per-bucket-state-date",
        type=int,
        default=DEFAULT_MIN_CONSTITUENTS_PER_BUCKET_STATE_DATE,
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_topix100_q10_bounce_streak_353_conditioning_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        price_feature=args.price_feature,
        volume_feature=args.volume_feature,
        short_window_streaks=args.short_window_streaks,
        long_window_streaks=args.long_window_streaks,
        validation_ratio=args.validation_ratio,
        min_constituents_per_bucket_state_date=args.min_constituents_per_bucket_state_date,
    )
    bundle = write_topix100_q10_bounce_streak_353_conditioning_research_bundle(
        result,
        output_root=Path(args.output_root) if args.output_root else None,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
