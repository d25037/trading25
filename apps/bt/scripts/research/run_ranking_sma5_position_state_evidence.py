#!/usr/bin/env python3
"""Runner-first entrypoint for SMA5 position-state evidence research."""

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
from src.domains.analytics.ranking_sma5_position_state_evidence import (  # noqa: E402
    DEFAULT_MARKET_SCOPES,
    DEFAULT_MIN_POSITION_DAYS,
    DEFAULT_MIN_TRADES,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    _DEFAULT_POSITION_LONG_SCAFFOLDS,
    _ENTRY_RULES,
    _EXIT_RULES,
    run_ranking_sma5_position_state_evidence_research,
    write_ranking_sma5_position_state_evidence_bundle,
)
from src.shared.config.settings import get_settings  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Daily Ranking position-state evidence using SMA5 count, "
            "below-SMA5 streak, and ATR20-normalized SMA5 deviation exits."
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
        "--markets",
        default=",".join(DEFAULT_MARKET_SCOPES),
        help="Comma-separated market scopes to include: prime, standard, growth, or all.",
    )
    parser.add_argument(
        "--long-scaffolds",
        default=",".join(_DEFAULT_POSITION_LONG_SCAFFOLDS),
        help="Comma-separated Daily Ranking long scaffold ids.",
    )
    parser.add_argument(
        "--entry-rules",
        default=",".join(_ENTRY_RULES),
        help="Comma-separated entry rule ids.",
    )
    parser.add_argument(
        "--exit-rules",
        default=",".join(_EXIT_RULES),
        help="Comma-separated exit rule ids.",
    )
    parser.add_argument(
        "--min-position-days",
        type=int,
        default=DEFAULT_MIN_POSITION_DAYS,
        help="Minimum held stock-days required for a daily evidence row.",
    )
    parser.add_argument(
        "--min-trades",
        type=int,
        default=DEFAULT_MIN_TRADES,
        help="Minimum trades required for a trade or exit evidence row.",
    )
    parser.add_argument(
        "--severe-loss-threshold-pct",
        type=float,
        default=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
        help="Daily/trade excess return threshold used for left-tail diagnostics.",
    )
    parser.add_argument(
        "--observation-sample-limit",
        type=int,
        default=DEFAULT_OBSERVATION_SAMPLE_LIMIT,
        help="Number of observation rows to persist as a sample table.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_ranking_sma5_position_state_evidence_research(
        args.db_path,
        start_date=args.start_date,
        end_date=args.end_date,
        market_scopes=_parse_strings(args.markets, name="markets"),
        long_scaffolds=_parse_strings(args.long_scaffolds, name="long-scaffolds"),
        entry_rules=_parse_strings(args.entry_rules, name="entry-rules"),
        exit_rules=_parse_strings(args.exit_rules, name="exit-rules"),
        min_position_days=args.min_position_days,
        min_trades=args.min_trades,
        severe_loss_threshold_pct=args.severe_loss_threshold_pct,
        observation_sample_limit=args.observation_sample_limit,
    )
    bundle = write_ranking_sma5_position_state_evidence_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


def _parse_strings(value: str, *, name: str) -> tuple[str, ...]:
    values = tuple(part.strip() for part in value.split(",") if part.strip())
    if not values:
        raise argparse.ArgumentTypeError(f"{name} must contain at least one item")
    return values


if __name__ == "__main__":
    raise SystemExit(main())
