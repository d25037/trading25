#!/usr/bin/env python3
"""Runner-first entrypoint for Prime value volatility-participation research."""

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
from src.domains.analytics.annual_prime_value_technical_risk_decomposition import (  # noqa: E402
    DEFAULT_MARKET_SCOPE,
    DEFAULT_SELECTION_FRACTIONS,
)
from src.domains.analytics.annual_prime_value_volume_volatility_participation import (  # noqa: E402
    run_annual_prime_value_volume_volatility_participation,
    write_annual_prime_value_volume_volatility_participation_bundle,
)
from src.domains.analytics.annual_value_technical_feature_importance import (  # noqa: E402
    DEFAULT_BUCKET_COUNT,
    DEFAULT_FOCUS_LIQUIDITY_SCENARIO,
    DEFAULT_FOCUS_SCORE_METHODS,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Test whether Prime annual value high-volatility effects are accompanied "
            "by volume/trading-value participation changes."
        )
    )
    parser.add_argument(
        "--input-bundle",
        default=None,
        help=(
            "Annual value-composite selection bundle path. Defaults to the latest "
            "annual-value-composite-selection bundle under the output root."
        ),
    )
    parser.add_argument(
        "--selection-fraction",
        type=float,
        action="append",
        dest="selection_fractions",
        default=None,
        help=f"Selection fraction to include. Can be repeated. Default: {DEFAULT_SELECTION_FRACTIONS}.",
    )
    parser.add_argument(
        "--market-scope",
        default=DEFAULT_MARKET_SCOPE,
        help=f"Market scope to analyze. Default: {DEFAULT_MARKET_SCOPE}.",
    )
    parser.add_argument(
        "--liquidity-scenario",
        default=DEFAULT_FOCUS_LIQUIDITY_SCENARIO,
        help=f"Liquidity scenario to analyze. Default: {DEFAULT_FOCUS_LIQUIDITY_SCENARIO}.",
    )
    parser.add_argument(
        "--score-method",
        action="append",
        dest="score_methods",
        default=None,
        help=(
            "Score method to include. Can be repeated. "
            f"Default: {', '.join(DEFAULT_FOCUS_SCORE_METHODS)}. fixed_55_25_20 is ignored if passed."
        ),
    )
    parser.add_argument(
        "--bucket-count",
        type=int,
        default=DEFAULT_BUCKET_COUNT,
        help=f"Volatility buckets per year. Default: {DEFAULT_BUCKET_COUNT}.",
    )
    parser.add_argument(
        "--participation-bucket-count",
        type=int,
        default=3,
        help="Participation buckets inside high-volatility events. Default: 3.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_annual_prime_value_volume_volatility_participation(
        args.input_bundle,
        output_root=args.output_root,
        selection_fractions=args.selection_fractions or DEFAULT_SELECTION_FRACTIONS,
        market_scope=args.market_scope,
        liquidity_scenario=args.liquidity_scenario,
        score_methods=args.score_methods or DEFAULT_FOCUS_SCORE_METHODS,
        bucket_count=args.bucket_count,
        participation_bucket_count=args.participation_bucket_count,
    )
    bundle = write_annual_prime_value_volume_volatility_participation_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
