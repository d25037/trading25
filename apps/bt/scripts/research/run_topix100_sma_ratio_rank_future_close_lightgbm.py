#!/usr/bin/env python3
"""Runner-first entrypoint for TOPIX100 SMA-ratio LightGBM research."""

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
from src.domains.analytics.topix100_sma_ratio_rank_future_close import (  # noqa: E402
    run_topix100_sma_ratio_rank_future_close_research,
)
from src.domains.analytics.topix100_sma_ratio_rank_future_close_lightgbm import (  # noqa: E402
    DEFAULT_WALKFORWARD_STEP,
    DEFAULT_WALKFORWARD_TEST_WINDOW,
    DEFAULT_WALKFORWARD_TRAIN_WINDOW,
    run_topix100_sma_ratio_rank_future_close_lightgbm_research,
    write_topix100_sma_ratio_rank_future_close_lightgbm_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run TOPIX100 SMA-ratio LightGBM research and persist a reproducible "
            "artifact bundle under ~/.local/share/trading25/research/."
        )
    )
    parser.add_argument("--db-path", default=get_settings().market_db_path)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--lookback-years", type=int, default=10)
    parser.add_argument("--min-constituents-per-day", type=int, default=80)
    parser.add_argument(
        "--train-window",
        type=int,
        default=DEFAULT_WALKFORWARD_TRAIN_WINDOW,
    )
    parser.add_argument(
        "--test-window",
        type=int,
        default=DEFAULT_WALKFORWARD_TEST_WINDOW,
    )
    parser.add_argument(
        "--step",
        type=int,
        default=DEFAULT_WALKFORWARD_STEP,
    )
    parser.add_argument(
        "--skip-diagnostic",
        action="store_true",
        help="Skip the fixed-split diagnostic and persist walk-forward outputs only.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    base_result = run_topix100_sma_ratio_rank_future_close_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_years=args.lookback_years,
        min_constituents_per_day=args.min_constituents_per_day,
    )
    result = run_topix100_sma_ratio_rank_future_close_lightgbm_research(
        base_result,
        train_window=args.train_window,
        test_window=args.test_window,
        step=args.step,
        include_diagnostic=not args.skip_diagnostic,
    )
    bundle = write_topix100_sma_ratio_rank_future_close_lightgbm_research_bundle(
        result,
        base_result=base_result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
