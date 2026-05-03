#!/usr/bin/env python3
"""Runner-first entrypoint for Prime top-slice PBR absorption research."""

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
from src.domains.analytics.annual_prime_value_pbr_absorption import (  # noqa: E402
    DEFAULT_PBR_WEIGHTS,
    DEFAULT_SELECTION_FRACTION,
    run_annual_prime_value_pbr_absorption,
    write_annual_prime_value_pbr_absorption_bundle,
)
from src.domains.analytics.annual_prime_value_technical_risk_decomposition import (  # noqa: E402
    DEFAULT_MARKET_SCOPE,
)
from src.domains.analytics.annual_value_technical_feature_importance import (  # noqa: E402
    DEFAULT_FOCUS_LIQUIDITY_SCENARIO,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Test whether PBR's Prime-wide value effect is absorbed by small-cap and "
            "low-forward-PER scores in the annual Prime top slice."
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
        "--market-scope",
        default=DEFAULT_MARKET_SCOPE,
        help=f"Market scope to analyze. Default: {DEFAULT_MARKET_SCOPE}.",
    )
    parser.add_argument(
        "--selection-fraction",
        type=float,
        default=DEFAULT_SELECTION_FRACTION,
        help=f"Top selection fraction. Default: {DEFAULT_SELECTION_FRACTION}.",
    )
    parser.add_argument(
        "--liquidity-scenario",
        default=DEFAULT_FOCUS_LIQUIDITY_SCENARIO,
        help=f"Liquidity scenario to analyze. Default: {DEFAULT_FOCUS_LIQUIDITY_SCENARIO}.",
    )
    parser.add_argument(
        "--pbr-weight",
        type=float,
        action="append",
        dest="pbr_weights",
        default=None,
        help=(
            "PBR score weight to test. Can be repeated. The remaining weight is split "
            f"between size and forward PER by the prime_size_tilt ratio. Default: {DEFAULT_PBR_WEIGHTS}."
        ),
    )
    parser.add_argument(
        "--baseline-pbr-weight",
        type=float,
        default=0.05,
        help="PBR weight used as the overlap/swap baseline. Default: 0.05.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_annual_prime_value_pbr_absorption(
        args.input_bundle,
        output_root=args.output_root,
        market_scope=args.market_scope,
        selection_fraction=args.selection_fraction,
        liquidity_scenario=args.liquidity_scenario,
        pbr_weights=args.pbr_weights or DEFAULT_PBR_WEIGHTS,
        baseline_pbr_weight=args.baseline_pbr_weight,
    )
    bundle = write_annual_prime_value_pbr_absorption_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
