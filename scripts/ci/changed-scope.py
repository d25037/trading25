#!/usr/bin/env python3
"""Classify changed files into CI tiers."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from test_taxonomy import (  # noqa: E402
    is_contract_path,
    is_docs_path,
    is_product_path,
    is_research_path,
    is_security_path,
    is_shared_path,
    normalize_path,
)


@dataclass(frozen=True)
class CiScope:
    product_ci: bool
    research_ci: bool
    contracts_ci: bool
    security_ci: bool
    docs_only: bool


def classify_changed_paths(paths: list[str]) -> CiScope:
    normalized = [normalize_path(path) for path in paths if path.strip()]
    if not normalized:
        return CiScope(
            product_ci=True,
            research_ci=True,
            contracts_ci=True,
            security_ci=True,
            docs_only=False,
        )

    docs_only = all(is_docs_path(path) for path in normalized)
    product_ci = False
    research_ci = False
    contracts_ci = False
    security_ci = False

    for path in normalized:
        is_docs = is_docs_path(path)
        is_shared = is_shared_path(path)
        is_product = is_product_path(path)
        is_research = is_research_path(path)
        is_contract = is_contract_path(path)
        is_security = is_security_path(path)

        if is_shared:
            product_ci = True
            research_ci = True
        if is_product:
            product_ci = True
        if is_research:
            research_ci = True
        if is_contract:
            contracts_ci = True
            product_ci = True
        if is_security:
            security_ci = True
            product_ci = True
        if not (
            is_docs
            or is_shared
            or is_product
            or is_research
            or is_contract
            or is_security
        ):
            product_ci = True

    return CiScope(
        product_ci=product_ci,
        research_ci=research_ci,
        contracts_ci=contracts_ci,
        security_ci=security_ci,
        docs_only=docs_only and not product_ci and not research_ci and not contracts_ci,
    )


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", help="Changed paths. Reads stdin when empty.")
    parser.add_argument(
        "--github-output",
        action="store_true",
        help="Write key=value lines suitable for $GITHUB_OUTPUT.",
    )
    return parser.parse_args(argv)


def _format_bool(value: bool) -> str:
    return "true" if value else "false"


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    paths = args.paths or [line.strip() for line in sys.stdin if line.strip()]
    scope = classify_changed_paths(paths)
    lines = [
        f"product_ci={_format_bool(scope.product_ci)}",
        f"research_ci={_format_bool(scope.research_ci)}",
        f"contracts_ci={_format_bool(scope.contracts_ci)}",
        f"security_ci={_format_bool(scope.security_ci)}",
        f"docs_only={_format_bool(scope.docs_only)}",
    ]
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
