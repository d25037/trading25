#!/usr/bin/env python3
"""Runner-first entrypoint for market negative EPS right-tail decomposition."""

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
from src.domains.analytics.standard_negative_eps_right_tail_decomposition import (  # noqa: E402
    DEFAULT_ADV_WINDOW,
    DEFAULT_MARKET,
    run_standard_negative_eps_right_tail_decomposition,
    write_standard_negative_eps_right_tail_bundle,
)
from src.shared.paths.resolver import get_data_dir  # noqa: E402


def _default_db_path() -> str:
    return str((get_data_dir() / "market-timeseries" / "market.duckdb").resolve())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Decompose market-scoped FY actual EPS<0 events by forecast sign, "
            "CFO sign, and trailing liquidity."
        )
    )
    parser.add_argument(
        "--db-path",
        default=_default_db_path(),
        help="market.duckdb path. Defaults to the active XDG market snapshot.",
    )
    parser.add_argument(
        "--market",
        choices=["prime", "standard"],
        default=DEFAULT_MARKET,
        help="Market scope classified by the current stock-master snapshot.",
    )
    parser.add_argument(
        "--adv-window",
        type=int,
        default=DEFAULT_ADV_WINDOW,
        help="Trailing session count used for the pre-entry average trading-value overlay.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_standard_negative_eps_right_tail_decomposition(
        args.db_path,
        market=args.market,
        adv_window=args.adv_window,
    )
    bundle = write_standard_negative_eps_right_tail_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
