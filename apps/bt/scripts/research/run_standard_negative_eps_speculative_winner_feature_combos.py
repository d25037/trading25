#!/usr/bin/env python3
"""Runner-first entrypoint for standard EPS<0 speculative winner feature-combo mining."""

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
from src.domains.analytics.standard_negative_eps_speculative_winner_feature_combos import (  # noqa: E402
    DEFAULT_MIN_EVENT_COUNT,
    DEFAULT_MIN_WINNER_COUNT,
    DEFAULT_TOP_EXAMPLES_LIMIT,
    DEFAULT_WINNER_QUANTILE,
    DEFAULT_SPARSE_SECTOR_MIN_EVENT_COUNT,
    DEFAULT_ADV_WINDOW,
    run_standard_negative_eps_speculative_winner_feature_combos,
    write_standard_negative_eps_speculative_winner_feature_combos_bundle,
)
from src.shared.paths.resolver import get_data_dir  # noqa: E402


def _default_db_path() -> str:
    return str((get_data_dir() / "market-timeseries" / "market.duckdb").resolve())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Mine pre-entry feature combinations associated with top-decile winners "
            "inside standard FY actual EPS<0 speculative cohorts."
        )
    )
    parser.add_argument(
        "--db-path",
        default=_default_db_path(),
        help="market.duckdb path. Defaults to the active XDG market snapshot.",
    )
    parser.add_argument(
        "--adv-window",
        type=int,
        default=DEFAULT_ADV_WINDOW,
        help="Trailing session count used for the pre-entry ADV feature.",
    )
    parser.add_argument(
        "--winner-quantile",
        type=float,
        default=DEFAULT_WINNER_QUANTILE,
        help="Winner threshold quantile inside each realized cohort, e.g. 0.9 for top decile.",
    )
    parser.add_argument(
        "--min-event-count",
        type=int,
        default=DEFAULT_MIN_EVENT_COUNT,
        help="Minimum realized event count required for pair and triplet combo rows.",
    )
    parser.add_argument(
        "--min-winner-count",
        type=int,
        default=DEFAULT_MIN_WINNER_COUNT,
        help="Minimum winner count required for a pair cell to expand into triplets.",
    )
    parser.add_argument(
        "--top-examples-limit",
        type=int,
        default=DEFAULT_TOP_EXAMPLES_LIMIT,
        help="Total representative winner examples retained across the two cohorts.",
    )
    parser.add_argument(
        "--sparse-sector-min-event-count",
        type=int,
        default=DEFAULT_SPARSE_SECTOR_MIN_EVENT_COUNT,
        help="Minimum realized-event count required to keep a sector bucket by name.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_standard_negative_eps_speculative_winner_feature_combos(
        args.db_path,
        adv_window=args.adv_window,
        winner_quantile=args.winner_quantile,
        min_event_count=args.min_event_count,
        min_winner_count=args.min_winner_count,
        top_examples_limit=args.top_examples_limit,
        sparse_sector_min_event_count=args.sparse_sector_min_event_count,
    )
    bundle = write_standard_negative_eps_speculative_winner_feature_combos_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
