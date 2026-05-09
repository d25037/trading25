#!/usr/bin/env python3
"""Runner-first entrypoint for value+breakout overheat-filter research."""

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
from src.domains.analytics.value_breakout_overheat_filter import (  # noqa: E402
    DEFAULT_BREAKOUT_LOOKBACK_SESSIONS,
    DEFAULT_BREAKOUT_POLICY,
    DEFAULT_BREAKOUT_WINDOW,
    DEFAULT_HOLDOUT_MONTHS,
    DEFAULT_LIQUIDITY_SCENARIO,
    DEFAULT_MARKET_SCOPE,
    DEFAULT_REBALANCE_MONTHS,
    DEFAULT_RISK_RATIO_TYPE,
    DEFAULT_SCORE_METHOD,
    DEFAULT_SELECTION_COUNT,
    DEFAULT_SIZE_HAIRCUT,
    DEFAULT_THRESHOLD_QUANTILE,
    run_value_breakout_overheat_filter,
    write_value_breakout_overheat_filter_bundle,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Overlay forward-EPS-style short-term overheat filters on a selected "
            "annual value + breakout portfolio bundle."
        )
    )
    parser.add_argument(
        "--input-bundle",
        default=None,
        help=(
            "Annual value+breakout periodic rebalance bundle path. Defaults to the latest "
            "bundle under the output root."
        ),
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Optional market.duckdb path. Defaults to the db_path recorded in the input bundle.",
    )
    parser.add_argument("--market-scope", default=DEFAULT_MARKET_SCOPE)
    parser.add_argument("--score-method", default=DEFAULT_SCORE_METHOD)
    parser.add_argument("--liquidity-scenario", default=DEFAULT_LIQUIDITY_SCENARIO)
    parser.add_argument("--breakout-policy", default=DEFAULT_BREAKOUT_POLICY)
    parser.add_argument("--breakout-window", type=int, default=DEFAULT_BREAKOUT_WINDOW)
    parser.add_argument(
        "--breakout-lookback-sessions",
        type=int,
        default=DEFAULT_BREAKOUT_LOOKBACK_SESSIONS,
    )
    parser.add_argument("--rebalance-months", type=int, default=DEFAULT_REBALANCE_MONTHS)
    parser.add_argument("--selection-count", type=int, default=DEFAULT_SELECTION_COUNT)
    parser.add_argument("--holdout-months", type=int, default=DEFAULT_HOLDOUT_MONTHS)
    parser.add_argument("--threshold-quantile", type=float, default=DEFAULT_THRESHOLD_QUANTILE)
    parser.add_argument("--size-haircut", type=float, default=DEFAULT_SIZE_HAIRCUT)
    parser.add_argument(
        "--risk-ratio-type",
        choices=("sharpe", "sortino"),
        default=DEFAULT_RISK_RATIO_TYPE,
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_value_breakout_overheat_filter(
        args.input_bundle,
        db_path=args.db_path,
        output_root=args.output_root,
        market_scope=args.market_scope,
        score_method=args.score_method,
        liquidity_scenario=args.liquidity_scenario,
        breakout_policy=args.breakout_policy,
        breakout_window=args.breakout_window,
        breakout_lookback_sessions=args.breakout_lookback_sessions,
        rebalance_months=args.rebalance_months,
        selection_count=args.selection_count,
        holdout_months=args.holdout_months,
        threshold_quantile=args.threshold_quantile,
        size_haircut=args.size_haircut,
        risk_ratio_type=args.risk_ratio_type,
    )
    bundle = write_value_breakout_overheat_filter_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
