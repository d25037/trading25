#!/usr/bin/env python3
"""Runner-first entrypoint for forward EPS component decomposition."""

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
from src.domains.analytics.forward_eps_component_decomposition import (  # noqa: E402
    DEFAULT_QUANTILE_BUCKET_COUNT,
    run_forward_eps_component_decomposition,
    write_forward_eps_component_decomposition_bundle,
)
from src.domains.analytics.forward_eps_trade_archetype_decomposition import (  # noqa: E402
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Decompose realized forward_eps_driven trades into value, earnings "
            "expectation, volume-attention, and price-momentum components."
        )
    )
    parser.add_argument(
        "--input-bundle",
        default=None,
        help=(
            "Forward EPS trade-archetype decomposition bundle path. Defaults to "
            "the latest bundle under the output root."
        ),
    )
    parser.add_argument(
        "--quantile-buckets",
        type=int,
        default=DEFAULT_QUANTILE_BUCKET_COUNT,
        help=f"Number of component score buckets. Default: {DEFAULT_QUANTILE_BUCKET_COUNT}.",
    )
    parser.add_argument(
        "--severe-loss-threshold-pct",
        type=float,
        default=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
        help="Trade-return threshold used to mark severe losses.",
    )
    add_bundle_output_arguments(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_bt_workdir(_BT_ROOT)
    args = parse_args(argv)
    result = run_forward_eps_component_decomposition(
        args.input_bundle,
        output_root=args.output_root,
        quantile_bucket_count=args.quantile_buckets,
        severe_loss_threshold_pct=args.severe_loss_threshold_pct,
    )
    bundle = write_forward_eps_component_decomposition_bundle(
        result,
        output_root=args.output_root,
        run_id=args.run_id,
        notes=args.notes,
    )
    emit_bundle_payload(bundle)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
