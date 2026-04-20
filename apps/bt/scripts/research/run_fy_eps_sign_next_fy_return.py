#!/usr/bin/env python3
"""Runner-first entrypoint for FY EPS sign to next-FY return research."""

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
from src.domains.analytics.fy_eps_sign_next_fy_return import (  # noqa: E402
    DEFAULT_MARKETS,
    DEFAULT_FORECAST_RATIO_THRESHOLDS,
    run_fy_eps_sign_next_fy_return,
    write_fy_eps_sign_next_fy_return_bundle,
)
from src.shared.paths.resolver import get_data_dir  # noqa: E402


def _default_db_path() -> str:
    return str((get_data_dir() / "market-timeseries" / "market.duckdb").resolve())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Group FY disclosures by actual EPS sign and study next-session-open "
            "to next-FY-window-close returns."
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
            "Market or scope to include. Repeat to override defaults "
            f"({', '.join(DEFAULT_MARKETS)}). "
            "Also accepts topix500 / primeExTopix500 as the latest scale-category proxy scopes."
        ),
    )
    parser.add_argument(
        "--forecast-ratio-threshold",
        action="append",
        dest="forecast_ratio_thresholds",
        type=float,
        default=None,
        help=(
            "Repeat to override forecast/actual overlay thresholds. "
            f"Defaults to {', '.join(f'{value:.1f}' for value in DEFAULT_FORECAST_RATIO_THRESHOLDS)}."
        ),
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_fy_eps_sign_next_fy_return(
        args.db_path,
        markets=tuple(args.markets or DEFAULT_MARKETS),
        forecast_ratio_thresholds=tuple(
            args.forecast_ratio_thresholds or DEFAULT_FORECAST_RATIO_THRESHOLDS
        ),
    )
    bundle = write_fy_eps_sign_next_fy_return_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
