#!/usr/bin/env python3
"""Runner-first entrypoint for 1357 x NT ratio / TOPIX hedge research."""

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
from src.domains.analytics.hedge_1357_nt_ratio_topix import (  # noqa: E402
    run_1357_nt_ratio_topix_hedge_research,
    write_1357_nt_ratio_topix_hedge_research_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def _parse_csv_list(value: str | None) -> list[str] | None:
    if value is None:
        return None
    items = [item.strip() for item in value.split(",")]
    return [item for item in items if item]


def _parse_csv_floats(value: str) -> list[float]:
    items = [item.strip() for item in value.split(",")]
    return [float(item) for item in items if item]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run 1357 x NT ratio / TOPIX hedge research and persist a "
            "reproducible artifact bundle under ~/.local/share/trading25/research/."
        )
    )
    parser.add_argument(
        "--db-path",
        default=get_settings().market_db_path,
        help="Path to market.duckdb. Defaults to the active app setting.",
    )
    parser.add_argument("--start-date", default=None, help="Event start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", default=None, help="Event end date (YYYY-MM-DD).")
    parser.add_argument(
        "--sigma-threshold-1",
        type=float,
        default=1.0,
        help="Shared sigma threshold 1 for TOPIX close and NT ratio.",
    )
    parser.add_argument(
        "--sigma-threshold-2",
        type=float,
        default=2.0,
        help="Shared sigma threshold 2 for TOPIX close and NT ratio.",
    )
    parser.add_argument(
        "--selected-groups",
        default=None,
        help="Optional comma-separated stock group keys.",
    )
    parser.add_argument(
        "--fixed-weights",
        default="0.1,0.2,0.3,0.4,0.5",
        help="Comma-separated fixed hedge weights.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_1357_nt_ratio_topix_hedge_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        sigma_threshold_1=args.sigma_threshold_1,
        sigma_threshold_2=args.sigma_threshold_2,
        selected_groups=_parse_csv_list(args.selected_groups),
        fixed_weights=_parse_csv_floats(args.fixed_weights),
    )
    bundle = write_1357_nt_ratio_topix_hedge_research_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
