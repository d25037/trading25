#!/usr/bin/env python3
"""Runner-first entrypoint for Daily Ranking triage lens research."""

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
from src.domains.analytics.ranking_daily_triage_lens import (  # noqa: E402
    DEFAULT_START_DATE,
    DEFAULT_STRONG_GAIN_THRESHOLD_PCT,
    DEFAULT_TOP_KS,
    run_ranking_daily_triage_lens_research,
    write_ranking_daily_triage_lens_bundle,
)
from src.domains.analytics.ranking_color_evidence import (  # noqa: E402
    DEFAULT_MARKET_SCOPES,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
)
from src.domains.analytics.ranking_sector_strength_evidence import (  # noqa: E402
    DEFAULT_HORIZONS,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Daily Ranking triage-lens research. The primary outcome is "
            "attention efficiency for inspect/watch/ignore/kill candidate buckets."
        )
    )
    parser.add_argument(
        "--db-path",
        default=get_settings().market_db_path,
        help="Path to market.duckdb. Defaults to the active app setting.",
    )
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="Anchor start date.")
    parser.add_argument("--end-date", default=None, help="Anchor end date.")
    parser.add_argument(
        "--horizons",
        default=",".join(str(horizon) for horizon in DEFAULT_HORIZONS),
        help="Comma-separated forward return horizons in trading sessions.",
    )
    parser.add_argument(
        "--markets",
        default=",".join(DEFAULT_MARKET_SCOPES),
        help="Comma-separated market scopes to include: prime, standard, growth, or all.",
    )
    parser.add_argument(
        "--top-ks",
        default=",".join(str(top_k) for top_k in DEFAULT_TOP_KS),
        help="Comma-separated per-date shortlist sizes for attention-efficiency metrics.",
    )
    parser.add_argument(
        "--severe-loss-threshold-pct",
        type=float,
        default=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
        help="Forward excess return threshold used for left-tail diagnostics.",
    )
    parser.add_argument(
        "--strong-gain-threshold-pct",
        type=float,
        default=DEFAULT_STRONG_GAIN_THRESHOLD_PCT,
        help="Forward excess return threshold used for right-tail capture.",
    )
    parser.add_argument(
        "--observation-sample-limit",
        type=int,
        default=DEFAULT_OBSERVATION_SAMPLE_LIMIT,
        help="Maximum candidate rows persisted to the bundle candidate/sample tables.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_ranking_daily_triage_lens_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        horizons=_parse_positive_ints(args.horizons, name="horizons"),
        market_scopes=_parse_strings(args.markets, name="markets"),
        top_ks=_parse_positive_ints(args.top_ks, name="top_ks"),
        severe_loss_threshold_pct=args.severe_loss_threshold_pct,
        strong_gain_threshold_pct=args.strong_gain_threshold_pct,
        observation_sample_limit=args.observation_sample_limit,
    )
    bundle = write_ranking_daily_triage_lens_bundle(
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


def _parse_strings(value: str, *, name: str) -> tuple[str, ...]:
    values = tuple(part.strip() for part in value.split(",") if part.strip())
    if not values:
        raise argparse.ArgumentTypeError(f"{name} must contain at least one item")
    return values


if __name__ == "__main__":
    raise SystemExit(main())
