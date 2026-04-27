#!/usr/bin/env python3
"""Runner-first entrypoint for the falling-knife reversal study."""

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
from src.domains.analytics.falling_knife_reversal_study import (  # noqa: E402
    DEFAULT_FIVE_DAY_DROP_THRESHOLD,
    DEFAULT_FORWARD_HORIZONS,
    DEFAULT_MARKET_CODES,
    DEFAULT_MAX_WAIT_DAYS,
    DEFAULT_MIN_CONDITION_COUNT,
    DEFAULT_RISK_ADJUSTED_LOOKBACK,
    DEFAULT_RISK_ADJUSTED_THRESHOLD,
    DEFAULT_SEVERE_LOSS_THRESHOLD,
    DEFAULT_SIGNAL_COOLDOWN_DAYS,
    DEFAULT_SIXTY_DAY_DRAWDOWN_THRESHOLD,
    DEFAULT_TWENTY_DAY_DROP_THRESHOLD,
    run_falling_knife_reversal_study,
    write_falling_knife_reversal_study_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Test the 'do not catch a falling knife' proverb using daily OHLC, "
            "Daily Risk Adjusted Return, and catch-vs-wait trade comparisons."
        )
    )
    parser.add_argument(
        "--db-path",
        default=get_settings().market_db_path,
        help="Path to market.duckdb. Defaults to the active app setting.",
    )
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument(
        "--market-code",
        action="append",
        dest="market_codes",
        default=None,
        help=(
            "Market code to include. Repeat to override the default current listed "
            f"markets ({', '.join(DEFAULT_MARKET_CODES)})."
        ),
    )
    parser.add_argument(
        "--forward-horizon",
        action="append",
        type=int,
        dest="forward_horizons",
        default=None,
        help=(
            "Forward close horizon in sessions. Repeat to override defaults "
            f"({', '.join(str(value) for value in DEFAULT_FORWARD_HORIZONS)})."
        ),
    )
    parser.add_argument(
        "--risk-adjusted-lookback",
        type=int,
        default=DEFAULT_RISK_ADJUSTED_LOOKBACK,
    )
    parser.add_argument(
        "--condition-ratio-type",
        choices=("sharpe", "sortino"),
        default="sortino",
    )
    parser.add_argument(
        "--five-day-drop-threshold",
        type=float,
        default=DEFAULT_FIVE_DAY_DROP_THRESHOLD,
    )
    parser.add_argument(
        "--twenty-day-drop-threshold",
        type=float,
        default=DEFAULT_TWENTY_DAY_DROP_THRESHOLD,
    )
    parser.add_argument(
        "--sixty-day-drawdown-threshold",
        type=float,
        default=DEFAULT_SIXTY_DAY_DRAWDOWN_THRESHOLD,
    )
    parser.add_argument(
        "--risk-adjusted-threshold",
        type=float,
        default=DEFAULT_RISK_ADJUSTED_THRESHOLD,
    )
    parser.add_argument(
        "--min-condition-count",
        type=int,
        default=DEFAULT_MIN_CONDITION_COUNT,
    )
    parser.add_argument("--max-wait-days", type=int, default=DEFAULT_MAX_WAIT_DAYS)
    parser.add_argument(
        "--signal-cooldown-days",
        type=int,
        default=DEFAULT_SIGNAL_COOLDOWN_DAYS,
        help=(
            "Minimum same-code cooldown after a falling-knife signal. "
            "Prevents counting every day in the same selloff as a separate episode."
        ),
    )
    parser.add_argument(
        "--severe-loss-threshold",
        type=float,
        default=DEFAULT_SEVERE_LOSS_THRESHOLD,
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_falling_knife_reversal_study(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        market_codes=tuple(args.market_codes or DEFAULT_MARKET_CODES),
        forward_horizons=tuple(args.forward_horizons or DEFAULT_FORWARD_HORIZONS),
        risk_adjusted_lookback=args.risk_adjusted_lookback,
        condition_ratio_type=args.condition_ratio_type,
        five_day_drop_threshold=args.five_day_drop_threshold,
        twenty_day_drop_threshold=args.twenty_day_drop_threshold,
        sixty_day_drawdown_threshold=args.sixty_day_drawdown_threshold,
        risk_adjusted_threshold=args.risk_adjusted_threshold,
        min_condition_count=args.min_condition_count,
        max_wait_days=args.max_wait_days,
        signal_cooldown_days=args.signal_cooldown_days,
        severe_loss_threshold=args.severe_loss_threshold,
    )
    bundle = write_falling_knife_reversal_study_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
