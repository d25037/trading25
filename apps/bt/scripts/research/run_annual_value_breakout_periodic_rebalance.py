#!/usr/bin/env python3
"""Runner-first entrypoint for value rebalance x breakout momentum research."""

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
from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (  # noqa: E402
    DEFAULT_ADV_WINDOW,
    DEFAULT_MARKETS,
)
from src.domains.analytics.annual_fundamental_confounder_analysis import (  # noqa: E402
    DEFAULT_WINSOR_LOWER,
    DEFAULT_WINSOR_UPPER,
    POSITIVE_RATIO_ONLY_COLUMNS,
)
from src.domains.analytics.annual_value_breakout_periodic_rebalance import (  # noqa: E402
    DEFAULT_BREAKOUT_LOOKBACK_SESSIONS,
    DEFAULT_BREAKOUT_SCORE_BOOST,
    DEFAULT_BREAKOUT_WINDOWS,
    DEFAULT_REBALANCE_MONTHS,
    DEFAULT_SELECTION_COUNTS,
    run_annual_value_breakout_periodic_rebalance,
    write_annual_value_breakout_periodic_rebalance_bundle,
)
from src.shared.paths.resolver import get_data_dir  # noqa: E402


def _default_db_path() -> str:
    return str((get_data_dir() / "market-timeseries" / "market.duckdb").resolve())


def _parse_factor_weight(value: str) -> tuple[float, float, float]:
    parts = [part.strip() for part in value.split(",")]
    if len(parts) != 3:
        raise argparse.ArgumentTypeError("factor weight must be pbr,size,forward_per")
    try:
        weights = tuple(float(part) for part in parts)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("factor weights must be numeric") from exc
    return weights  # type: ignore[return-value]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate N-month value-composite rebalance portfolios with prior-session "
            "N-day breakout momentum gates and additive score overlays."
        )
    )
    parser.add_argument(
        "--db-path",
        default=_default_db_path(),
        help="market.duckdb path. Defaults to the active XDG market snapshot.",
    )
    parser.add_argument(
        "--market",
        action="append",
        dest="markets",
        default=None,
        help=(
            "Market to include. Repeat to override defaults "
            f"({', '.join(DEFAULT_MARKETS)}). Accepts prime, standard, growth "
            "or current market codes 0111/0112/0113."
        ),
    )
    parser.add_argument(
        "--rebalance-months",
        action="append",
        type=int,
        default=None,
        help=(
            "N-month holding cadence to evaluate. Repeat to override defaults "
            f"({', '.join(str(value) for value in DEFAULT_REBALANCE_MONTHS)})."
        ),
    )
    parser.add_argument(
        "--selection-count",
        action="append",
        type=int,
        default=None,
        help=(
            "Top-N selected names per rebalance period and market scope. Repeat "
            f"to override defaults ({', '.join(str(value) for value in DEFAULT_SELECTION_COUNTS)})."
        ),
    )
    parser.add_argument(
        "--score-method",
        action="append",
        default=None,
        help=(
            "Score method to evaluate. Repeat to restrict the scan. Examples: "
            "prime_size_tilt, equal_weight, standard_pbr_tilt, walkforward_regression_weight."
        ),
    )
    parser.add_argument(
        "--liquidity-scenario",
        action="append",
        default=None,
        help="Liquidity scenario to evaluate. Repeat to restrict the scan. Examples: none, adv10m.",
    )
    parser.add_argument(
        "--breakout-policy",
        action="append",
        default=None,
        help=(
            "Breakout policy to evaluate. Repeat to restrict the scan. Examples: "
            "value_only, breakout_signal, breakout_recent, breakout_additive."
        ),
    )
    parser.add_argument(
        "--breakout-window",
        action="append",
        type=int,
        default=None,
        help=(
            "N-day breakout window. Repeat to override defaults "
            f"({', '.join(str(value) for value in DEFAULT_BREAKOUT_WINDOWS)})."
        ),
    )
    parser.add_argument(
        "--breakout-lookback-sessions",
        action="append",
        type=int,
        default=None,
        help=(
            "Recent-breakout lookback in sessions. Repeat to override defaults "
            f"({', '.join(str(value) for value in DEFAULT_BREAKOUT_LOOKBACK_SESSIONS)}). "
            "Use 0 for signal-date-only policy."
        ),
    )
    parser.add_argument(
        "--breakout-score-boost",
        type=float,
        default=DEFAULT_BREAKOUT_SCORE_BOOST,
        help=f"Additive score boost for recent breakout recency. Default: {DEFAULT_BREAKOUT_SCORE_BOOST}.",
    )
    parser.add_argument(
        "--factor-weight-step",
        type=float,
        default=None,
        help=(
            "Generate a simplex grid over PBR/size/forward-PER factor weights. "
            "Example: 0.25 creates 15 factor-weight score methods."
        ),
    )
    parser.add_argument(
        "--factor-weight",
        action="append",
        type=_parse_factor_weight,
        default=None,
        help=(
            "Explicit factor weights as pbr,size,forward_per. Repeat to add "
            "custom optimization candidates. Values must sum to 1.0."
        ),
    )
    parser.add_argument(
        "--max-portfolio-configs",
        type=int,
        default=None,
        help=(
            "Build daily portfolio curves only for the top N selection-summary "
            "configs plus all value-only baselines. Keeps broad scans tractable."
        ),
    )
    parser.add_argument(
        "--skip-portfolio-curves",
        action="store_true",
        help=(
            "Skip daily portfolio curve construction. Use for broad parameter scans; "
            "selection_summary_df still contains fast event-level proxy metrics."
        ),
    )
    parser.add_argument("--start-year", type=int, default=None, help="Optional first calendar year.")
    parser.add_argument("--end-year", type=int, default=None, help="Optional last calendar year.")
    parser.add_argument(
        "--winsor-lower",
        type=float,
        default=DEFAULT_WINSOR_LOWER,
        help=f"Lower winsorization quantile for event returns. Default: {DEFAULT_WINSOR_LOWER}.",
    )
    parser.add_argument(
        "--winsor-upper",
        type=float,
        default=DEFAULT_WINSOR_UPPER,
        help=f"Upper winsorization quantile for event returns. Default: {DEFAULT_WINSOR_UPPER}.",
    )
    parser.add_argument(
        "--min-train-observations",
        type=int,
        default=80,
        help="Minimum prior observations for walk-forward regression weights. Default: 80.",
    )
    parser.add_argument(
        "--adv-window",
        type=int,
        default=DEFAULT_ADV_WINDOW,
        help=f"Prior-session window for average trading value. Default: {DEFAULT_ADV_WINDOW}.",
    )
    parser.add_argument(
        "--require-positive-pbr-and-forward-per",
        action="store_true",
        help=(
            "Filter realized events to rows where both PBR and forward PER are "
            f"strictly positive ({', '.join(POSITIVE_RATIO_ONLY_COLUMNS)})."
        ),
    )
    parser.add_argument(
        "--include-incomplete-last-period",
        action="store_true",
        help="Include the final period even if its calendar-year close is incomplete.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_annual_value_breakout_periodic_rebalance(
        args.db_path,
        markets=tuple(args.markets or DEFAULT_MARKETS),
        rebalance_months=tuple(args.rebalance_months or DEFAULT_REBALANCE_MONTHS),
        selection_counts=tuple(args.selection_count or DEFAULT_SELECTION_COUNTS),
        score_methods=tuple(args.score_method) if args.score_method else None,
        liquidity_scenarios=tuple(args.liquidity_scenario) if args.liquidity_scenario else None,
        breakout_policies=tuple(args.breakout_policy) if args.breakout_policy else None,
        factor_weight_step=args.factor_weight_step,
        factor_weights=tuple(args.factor_weight or ()),
        max_portfolio_configs=args.max_portfolio_configs,
        skip_portfolio_curves=bool(args.skip_portfolio_curves),
        breakout_windows=tuple(args.breakout_window or DEFAULT_BREAKOUT_WINDOWS),
        breakout_lookback_sessions=tuple(
            args.breakout_lookback_sessions or DEFAULT_BREAKOUT_LOOKBACK_SESSIONS
        ),
        breakout_score_boost=args.breakout_score_boost,
        start_year=args.start_year,
        end_year=args.end_year,
        winsor_lower=args.winsor_lower,
        winsor_upper=args.winsor_upper,
        min_train_observations=args.min_train_observations,
        adv_window=args.adv_window,
        required_positive_columns=(
            POSITIVE_RATIO_ONLY_COLUMNS if args.require_positive_pbr_and_forward_per else ()
        ),
        include_incomplete_last_period=bool(args.include_incomplete_last_period),
    )
    bundle = write_annual_value_breakout_periodic_rebalance_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
