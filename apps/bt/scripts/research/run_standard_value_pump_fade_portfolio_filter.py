#!/usr/bin/env python3
"""Runner-first entrypoint for Standard value pump/fade portfolio filters."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


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
from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (  # noqa: E402
    _open_analysis_connection,
    _query_price_rows,
)
from src.domains.analytics.annual_fundamental_confounder_analysis import (  # noqa: E402
    POSITIVE_RATIO_ONLY_COLUMNS,
)
from src.domains.analytics.annual_value_breakout_periodic_rebalance import (  # noqa: E402
    run_annual_value_breakout_periodic_rebalance,
)
from src.domains.analytics.standard_value_pump_fade_portfolio_filter import (  # noqa: E402
    DEFAULT_FILTER_POLICIES,
    run_standard_value_pump_fade_portfolio_filter_from_frames,
    write_standard_value_pump_fade_portfolio_filter_bundle,
)
from src.shared.paths.resolver import get_data_dir  # noqa: E402


def _default_db_path() -> str:
    return str((get_data_dir() / "market-timeseries" / "market.duckdb").resolve())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Recompute Standard value+breakout daily portfolio curves after pump/fade "
            "hard filters, using the same portfolio metric definitions as annual value research."
        )
    )
    parser.add_argument(
        "--db-path",
        default=_default_db_path(),
        help="market.duckdb path. Defaults to the active XDG market snapshot.",
    )
    parser.add_argument("--market", default="standard")
    parser.add_argument("--score-method", default="prime_size_tilt")
    parser.add_argument("--liquidity-scenario", default="adv10m")
    parser.add_argument("--breakout-window", type=int, default=120)
    parser.add_argument("--breakout-lookback-sessions", type=int, default=20)
    parser.add_argument("--rebalance-months", type=int, default=3)
    parser.add_argument("--base-selection-count", type=int, default=10)
    parser.add_argument("--refill-pool-selection-count", type=int, default=100)
    parser.add_argument(
        "--filter-policy",
        action="append",
        default=None,
        help=(
            "Filter policy to evaluate. Repeat to override defaults "
            f"({', '.join(DEFAULT_FILTER_POLICIES)})."
        ),
    )
    parser.add_argument(
        "--diagnostic-lookback-days",
        type=int,
        default=900,
        help="Price-history lookback before first signal date for pump/fade diagnostics.",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="Optional first calendar year. Default matches annual value runner.",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Optional last calendar year.",
    )
    parser.add_argument(
        "--include-incomplete-last-period",
        action="store_true",
        help="Include final incomplete period. Default matches the prior portfolio bundle.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def _load_price_history(
    db_path: str,
    selected_event_df: pd.DataFrame,
    *,
    diagnostic_lookback_days: int,
) -> pd.DataFrame:
    if selected_event_df.empty:
        return pd.DataFrame(columns=["code", "date", "open", "high", "low", "close", "volume"])
    codes = sorted(selected_event_df["code"].astype(str).unique().tolist())
    signal_dates = pd.to_datetime(selected_event_df["signal_date"], errors="coerce").dropna()
    exit_dates = pd.to_datetime(selected_event_df["exit_date"], errors="coerce").dropna()
    if signal_dates.empty or exit_dates.empty:
        return pd.DataFrame(columns=["code", "date", "open", "high", "low", "close", "volume"])
    start_date = (
        signal_dates.min() - pd.Timedelta(days=int(diagnostic_lookback_days))
    ).strftime("%Y-%m-%d")
    end_date = exit_dates.max().strftime("%Y-%m-%d")
    with _open_analysis_connection(db_path) as ctx:
        return _query_price_rows(
            ctx.connection,
            codes=codes,
            start_date=start_date,
            end_date=end_date,
        )


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    annual_result = run_annual_value_breakout_periodic_rebalance(
        args.db_path,
        markets=(args.market,),
        rebalance_months=(args.rebalance_months,),
        selection_counts=(args.base_selection_count, args.refill_pool_selection_count),
        score_methods=(args.score_method,),
        liquidity_scenarios=(args.liquidity_scenario,),
        breakout_policies=("breakout_additive",),
        breakout_windows=(args.breakout_window,),
        breakout_lookback_sessions=(args.breakout_lookback_sessions,),
        start_year=args.start_year,
        end_year=args.end_year,
        required_positive_columns=POSITIVE_RATIO_ONLY_COLUMNS,
        include_incomplete_last_period=bool(args.include_incomplete_last_period),
        skip_portfolio_curves=True,
    )
    price_history_df = _load_price_history(
        args.db_path,
        annual_result.selected_event_df,
        diagnostic_lookback_days=args.diagnostic_lookback_days,
    )
    result = run_standard_value_pump_fade_portfolio_filter_from_frames(
        db_path=args.db_path,
        selected_event_df=annual_result.selected_event_df,
        price_history_df=price_history_df,
        base_selection_count=args.base_selection_count,
        refill_pool_selection_count=args.refill_pool_selection_count,
        filter_policies=tuple(args.filter_policy or DEFAULT_FILTER_POLICIES),
    )
    bundle = write_standard_value_pump_fade_portfolio_filter_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
