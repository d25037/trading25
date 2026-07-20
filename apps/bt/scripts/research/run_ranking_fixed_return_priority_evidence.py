#!/usr/bin/env python3
"""Run the PIT Prime-only fixed 20D/60D Ranking priority experiment."""

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
from src.domains.analytics.ranking_fixed_return_priority_evidence import (  # noqa: E402
    DEFAULT_BOOTSTRAP_RESAMPLES,
    DEFAULT_BOOTSTRAP_SEED,
    DEFAULT_HORIZONS,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    run_ranking_fixed_return_priority_evidence_research,
    write_ranking_fixed_return_priority_evidence_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Test fixed 20D/60D priority inside fixed-return-free Prime long "
            "scaffolds using exact-date PIT membership."
        )
    )
    parser.add_argument(
        "--db-path",
        default=get_settings().market_db_path,
        help="Path to market.duckdb. Defaults to the active app setting.",
    )
    parser.add_argument(
        "--start-date",
        default="2017-01-01",
        help="Inclusive signal-date start (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Inclusive signal-date end (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--horizons",
        default=",".join(str(item) for item in DEFAULT_HORIZONS),
        help="Comma-separated forward horizons in trading sessions.",
    )
    parser.add_argument(
        "--bootstrap-resamples",
        type=int,
        default=DEFAULT_BOOTSTRAP_RESAMPLES,
        help="Moving-block bootstrap resample count.",
    )
    parser.add_argument(
        "--bootstrap-seed",
        type=int,
        default=DEFAULT_BOOTSTRAP_SEED,
        help="Fixed bootstrap seed.",
    )
    parser.add_argument(
        "--observation-sample-limit",
        type=int,
        default=DEFAULT_OBSERVATION_SAMPLE_LIMIT,
        help="Maximum observation rows stored in the bundle sample.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_ranking_fixed_return_priority_evidence_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        horizons=_parse_positive_ints(args.horizons),
        bootstrap_resamples=args.bootstrap_resamples,
        bootstrap_seed=args.bootstrap_seed,
        observation_sample_limit=args.observation_sample_limit,
    )
    bundle = write_ranking_fixed_return_priority_evidence_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


def _parse_positive_ints(value: str) -> tuple[int, ...]:
    items = tuple(sorted({int(part.strip()) for part in value.split(",") if part.strip()}))
    if not items or any(item <= 0 for item in items):
        raise argparse.ArgumentTypeError("horizons must contain positive integers")
    return items


if __name__ == "__main__":
    raise SystemExit(main())
