#!/usr/bin/env python3
"""Runner-first entrypoint for Ranking short/red evidence research."""

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
from src.domains.analytics.ranking_short_red_evidence import (  # noqa: E402
    DEFAULT_HORIZONS,
    DEFAULT_MARKET_SCOPES,
    DEFAULT_MIN_OBSERVATIONS,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_TAIL_RETURN_THRESHOLD_PCT,
    run_ranking_short_red_evidence_research,
    write_ranking_short_red_evidence_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run PIT-safe Daily Ranking short/red evidence over liquidity regimes, "
            "relative valuation, 20D/60D technical state, and ATR20/ATR60 features."
        )
    )
    parser.add_argument(
        "--db-path",
        default=get_settings().market_db_path,
        help="Path to market.duckdb. Defaults to the active app setting.",
    )
    parser.add_argument("--start-date", default=None, help="Anchor start date.")
    parser.add_argument("--end-date", default=None, help="Anchor end date.")
    parser.add_argument(
        "--horizons",
        default=",".join(str(horizon) for horizon in DEFAULT_HORIZONS),
        help="Comma-separated forward return horizons in trading sessions.",
    )
    parser.add_argument(
        "--markets",
        default=",".join(DEFAULT_MARKET_SCOPES),
        help="Comma-separated market scopes to include: prime, standard, growth, unknown, or all.",
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=DEFAULT_MIN_OBSERVATIONS,
        help="Minimum observations required for a summarized row.",
    )
    parser.add_argument(
        "--tail-return-threshold-pct",
        type=float,
        default=DEFAULT_TAIL_RETURN_THRESHOLD_PCT,
        help=(
            "Negative return threshold used for downside tail diagnostics; "
            "the absolute value is used for upside tail diagnostics."
        ),
    )
    parser.add_argument(
        "--observation-sample-limit",
        type=int,
        default=DEFAULT_OBSERVATION_SAMPLE_LIMIT,
        help="Number of replay/sample rows to persist.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_ranking_short_red_evidence_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        horizons=_parse_positive_ints(args.horizons, name="horizons"),
        market_scopes=_parse_strings(args.markets),
        min_observations=args.min_observations,
        tail_return_threshold_pct=args.tail_return_threshold_pct,
        observation_sample_limit=args.observation_sample_limit,
    )
    bundle = write_ranking_short_red_evidence_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


def _parse_positive_ints(value: str, *, name: str) -> tuple[int, ...]:
    values = tuple(sorted({int(part.strip()) for part in value.split(",") if part.strip()}))
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError(f"{name} must contain positive integers")
    return values


def _parse_strings(value: str) -> tuple[str, ...]:
    values = tuple(part.strip() for part in value.split(",") if part.strip())
    if not values:
        raise argparse.ArgumentTypeError("value must contain at least one item")
    return values


if __name__ == "__main__":
    raise SystemExit(main())
