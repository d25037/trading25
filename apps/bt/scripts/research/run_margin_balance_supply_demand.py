#!/usr/bin/env python3
"""Runner-first entrypoint for PIT-safe margin balance supply/demand research."""

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
from src.domains.analytics.margin_balance_supply_demand import (  # noqa: E402
    DEFAULT_ADV_WINDOW,
    DEFAULT_BUCKET_COUNT,
    DEFAULT_DISCOVERY_END_DATE,
    DEFAULT_EFFECTIVE_LAG_SESSIONS,
    DEFAULT_HORIZONS,
    DEFAULT_MIN_DAILY_OBSERVATIONS,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    run_margin_balance_supply_demand_research,
    write_margin_balance_supply_demand_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Phase 1 margin-balance supply/demand research using only local "
            "market.duckdb margin_data. Institutional short-selling reports are "
            "not included."
        )
    )
    parser.add_argument(
        "--db-path",
        default=get_settings().market_db_path,
        help="Path to market.duckdb. Defaults to the active app setting.",
    )
    parser.add_argument("--start-date", default=None, help="Analysis start date.")
    parser.add_argument("--end-date", default=None, help="Analysis end date.")
    parser.add_argument(
        "--horizons",
        default=",".join(str(horizon) for horizon in DEFAULT_HORIZONS),
        help="Comma-separated open-to-future-close horizons in trading sessions.",
    )
    parser.add_argument(
        "--adv-window",
        type=int,
        default=DEFAULT_ADV_WINDOW,
        help="Prior-session ADV window used to normalize margin balances.",
    )
    parser.add_argument(
        "--effective-lag-sessions",
        type=int,
        default=DEFAULT_EFFECTIVE_LAG_SESSIONS,
        help=(
            "Trading-session lag from margin record date to first tradable entry date. "
            "Default 3 models Friday balance -> Tuesday evening publication -> "
            "Wednesday open."
        ),
    )
    parser.add_argument(
        "--bucket-count",
        type=int,
        default=DEFAULT_BUCKET_COUNT,
        help="Cross-sectional bucket count per effective date.",
    )
    parser.add_argument(
        "--min-daily-observations",
        type=int,
        default=DEFAULT_MIN_DAILY_OBSERVATIONS,
        help="Minimum cross-section size before assigning feature buckets.",
    )
    parser.add_argument(
        "--discovery-end-date",
        default=DEFAULT_DISCOVERY_END_DATE,
        help="Last effective date treated as discovery for pruning diagnostics.",
    )
    parser.add_argument(
        "--severe-loss-threshold-pct",
        type=float,
        default=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
        help="Return threshold used for severe-loss diagnostics.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_margin_balance_supply_demand_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        horizons=_parse_horizons(args.horizons),
        adv_window=args.adv_window,
        effective_lag_sessions=args.effective_lag_sessions,
        bucket_count=args.bucket_count,
        min_daily_observations=args.min_daily_observations,
        discovery_end_date=args.discovery_end_date,
        severe_loss_threshold_pct=args.severe_loss_threshold_pct,
    )
    bundle = write_margin_balance_supply_demand_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


def _parse_horizons(value: str) -> tuple[int, ...]:
    horizons = tuple(sorted({int(part.strip()) for part in value.split(",") if part.strip()}))
    if not horizons:
        raise argparse.ArgumentTypeError("at least one horizon is required")
    return horizons


if __name__ == "__main__":
    raise SystemExit(main())
