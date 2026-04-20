#!/usr/bin/env python3
"""Runner-first entrypoint for TOPIX500 EPS>0 / forecast missing / CFO>0 deep dive."""

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
from src.domains.analytics.topix500_positive_eps_missing_forecast_cfo_positive_deep_dive import (  # noqa: E402
    DEFAULT_HORIZONS,
    DEFAULT_PRIOR_SESSIONS,
    DEFAULT_RECENT_YEAR_WINDOW,
    run_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive,
    write_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive_bundle,
)
from src.shared.paths.resolver import get_data_dir  # noqa: E402


def _default_db_path() -> str:
    return str((get_data_dir() / "market-timeseries" / "market.duckdb").resolve())


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def _parse_horizons(value: str) -> tuple[int, ...]:
    parsed = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    if not parsed:
        raise argparse.ArgumentTypeError("horizons must contain at least one positive integer")
    if any(item <= 0 for item in parsed):
        raise argparse.ArgumentTypeError("horizons must be positive integers")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Deep-dive TOPIX500 FY actual EPS>0 events where forecast is missing "
            "and OperatingCashFlow is positive."
        )
    )
    parser.add_argument(
        "--db-path",
        default=_default_db_path(),
        help="market.duckdb path. Defaults to the active XDG market snapshot.",
    )
    parser.add_argument(
        "--adv-window",
        type=_positive_int,
        default=20,
        help="Trailing session count used for the entry ADV calculation.",
    )
    parser.add_argument(
        "--prior-sessions",
        type=int,
        default=DEFAULT_PRIOR_SESSIONS,
        help="Trading-session lookback used for the pre-entry drawdown calculation.",
    )
    parser.add_argument(
        "--horizons",
        type=_parse_horizons,
        default=DEFAULT_HORIZONS,
        help="Comma-separated trading-session horizons, e.g. 21,63,126,252.",
    )
    parser.add_argument(
        "--recent-year-window",
        type=int,
        default=DEFAULT_RECENT_YEAR_WINDOW,
        help="Trailing full-calendar-year window used for the yearly name-count summary.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive(
        args.db_path,
        adv_window=args.adv_window,
        prior_sessions=args.prior_sessions,
        horizons=args.horizons,
        recent_year_window=args.recent_year_window,
    )
    bundle = write_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
