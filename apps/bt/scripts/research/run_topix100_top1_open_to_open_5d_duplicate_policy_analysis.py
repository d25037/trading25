"""Run the TOPIX100 Top1 duplicate-policy comparison study."""

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
from src.domains.analytics.topix100_top1_open_to_open_5d_duplicate_policy_analysis import (  # noqa: E402
    DEFAULT_DUPLICATE_POLICIES,
    run_topix100_top1_open_to_open_5d_duplicate_policy_analysis,
    write_topix100_top1_open_to_open_5d_duplicate_policy_analysis_bundle,
)
from src.domains.analytics.topix100_top1_open_to_open_5d_fixed_committee_overlay import (  # noqa: E402
    DEFAULT_FIXED_COMMITTEE_LOW_THRESHOLD,
    DEFAULT_HOLDING_SESSION_COUNT,
    DEFAULT_SLEEVE_COUNT,
    DEFAULT_TOP1_MODEL_NAME,
)
from src.domains.analytics.topix_downside_return_standard_deviation_shock_confirmation_committee_overlay import (  # noqa: E402
    DEFAULT_COMMITTEE_HIGH_THRESHOLDS,
    DEFAULT_COMMITTEE_MEAN_WINDOW_DAYS,
    DEFAULT_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_DAYS,
    DEFAULT_FIXED_BREADTH_VOTE_THRESHOLD,
    DEFAULT_FIXED_CONFIRMATION_MODE,
    DEFAULT_FIXED_REDUCED_EXPOSURE_RATIO,
)
from src.domains.analytics.topix_close_return_streaks import DEFAULT_VALIDATION_RATIO  # noqa: E402
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare duplicate-handling policies for TOPIX100 Top1 open-to-open 5D "
            "under the same fixed TOPIX committee overlay."
        )
    )
    parser.add_argument("--db-path", default=str(get_settings().market_db_path))
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--top1-bundle-path")
    parser.add_argument("--committee-bundle-path")
    parser.add_argument("--model-name", default=DEFAULT_TOP1_MODEL_NAME)
    parser.add_argument("--fallback-candidate-top-k", type=int, default=5)
    parser.add_argument("--sleeve-count", type=int, default=DEFAULT_SLEEVE_COUNT)
    parser.add_argument(
        "--holding-session-count",
        type=int,
        default=DEFAULT_HOLDING_SESSION_COUNT,
    )
    parser.add_argument(
        "--duplicate-policies",
        nargs="+",
        default=list(DEFAULT_DUPLICATE_POLICIES),
    )
    parser.add_argument(
        "--downside-return-standard-deviation-window-days",
        type=int,
        default=DEFAULT_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_DAYS,
    )
    parser.add_argument(
        "--committee-mean-window-days",
        type=int,
        nargs="+",
        default=list(DEFAULT_COMMITTEE_MEAN_WINDOW_DAYS),
    )
    parser.add_argument(
        "--committee-high-thresholds",
        type=float,
        nargs="+",
        default=list(DEFAULT_COMMITTEE_HIGH_THRESHOLDS),
    )
    parser.add_argument(
        "--committee-low-threshold",
        type=float,
        default=DEFAULT_FIXED_COMMITTEE_LOW_THRESHOLD,
    )
    parser.add_argument("--committee-trend-vote-threshold", type=int, default=1)
    parser.add_argument(
        "--committee-breadth-vote-threshold",
        type=int,
        default=DEFAULT_FIXED_BREADTH_VOTE_THRESHOLD,
    )
    parser.add_argument(
        "--committee-confirmation-mode",
        default=DEFAULT_FIXED_CONFIRMATION_MODE,
    )
    parser.add_argument(
        "--committee-reduced-exposure-ratio",
        type=float,
        default=DEFAULT_FIXED_REDUCED_EXPOSURE_RATIO,
    )
    parser.add_argument("--min-constituents-per-day", type=int, default=50)
    parser.add_argument("--validation-ratio", type=float, default=DEFAULT_VALIDATION_RATIO)
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_topix100_top1_open_to_open_5d_duplicate_policy_analysis(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        top1_bundle_path=args.top1_bundle_path,
        committee_bundle_path=args.committee_bundle_path,
        model_name=args.model_name,
        fallback_candidate_top_k=args.fallback_candidate_top_k,
        sleeve_count=args.sleeve_count,
        holding_session_count=args.holding_session_count,
        duplicate_policies=args.duplicate_policies,
        downside_return_standard_deviation_window_days=(
            args.downside_return_standard_deviation_window_days
        ),
        committee_mean_window_days=args.committee_mean_window_days,
        committee_high_thresholds=args.committee_high_thresholds,
        committee_low_threshold=args.committee_low_threshold,
        committee_trend_vote_threshold=args.committee_trend_vote_threshold,
        committee_breadth_vote_threshold=args.committee_breadth_vote_threshold,
        committee_confirmation_mode=args.committee_confirmation_mode,
        committee_reduced_exposure_ratio=args.committee_reduced_exposure_ratio,
        min_constituents_per_day=args.min_constituents_per_day,
        validation_ratio=args.validation_ratio,
    )
    bundle = write_topix100_top1_open_to_open_5d_duplicate_policy_analysis_bundle(
        result,
        output_root=Path(args.output_root) if args.output_root else None,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
