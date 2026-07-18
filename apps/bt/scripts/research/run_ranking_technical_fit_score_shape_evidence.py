#!/usr/bin/env python3
"""Runner-first entrypoint for Ranking Technical Fit Score shape evidence."""

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
from src.domains.analytics.ranking_technical_fit_score_shape_evidence import (  # noqa: E402
    DEFAULT_BOOTSTRAP_RESAMPLES,
    DEFAULT_HORIZONS,
    DEFAULT_MIN_TRAINING_DATES,
    DEFAULT_MIN_TRAINING_OBSERVATIONS,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    run_ranking_technical_fit_score_shape_evidence_research,
    write_ranking_technical_fit_score_shape_evidence_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Learn prior-only nonlinear Technical Fit Score mappings inside "
            "fixed-return-free Prime Value/Long-Hybrid candidate rings."
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
        help="Signal-date analysis start (default: 2017-01-01).",
    )
    parser.add_argument("--end-date", default=None, help="Signal-date analysis end.")
    parser.add_argument(
        "--horizons",
        default=",".join(str(horizon) for horizon in DEFAULT_HORIZONS),
        help="Comma-separated forward horizons (default: 5,20,60).",
    )
    parser.add_argument(
        "--min-training-observations",
        type=int,
        default=DEFAULT_MIN_TRAINING_OBSERVATIONS,
        help="Required prior-only observations in every raw bin (default: 200).",
    )
    parser.add_argument(
        "--min-training-dates",
        type=int,
        default=DEFAULT_MIN_TRAINING_DATES,
        help="Required distinct prior-only signal dates in every raw bin (default: 50).",
    )
    parser.add_argument(
        "--bootstrap-resamples",
        type=int,
        default=DEFAULT_BOOTSTRAP_RESAMPLES,
        help="Moving-block bootstrap resamples (default: 2000).",
    )
    parser.add_argument(
        "--observation-sample-limit",
        type=int,
        default=DEFAULT_OBSERVATION_SAMPLE_LIMIT,
        help="Maximum observation rows persisted in the sample table.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def _parse_positive_ints(value: str, *, name: str) -> tuple[int, ...]:
    try:
        values = tuple(
            sorted({int(part.strip()) for part in value.split(",") if part.strip()})
        )
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"{name} must contain comma-separated positive integers"
        ) from exc
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError(f"{name} must contain positive integers")
    return values


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_ranking_technical_fit_score_shape_evidence_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        horizons=_parse_positive_ints(args.horizons, name="horizons"),
        min_training_observations=args.min_training_observations,
        min_training_dates=args.min_training_dates,
        bootstrap_resamples=args.bootstrap_resamples,
        observation_sample_limit=args.observation_sample_limit,
    )
    bundle = write_ranking_technical_fit_score_shape_evidence_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
