#!/usr/bin/env python3
"""Classify changed files into CI tiers."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import sys


@dataclass(frozen=True)
class CiScope:
    product_ci: bool
    research_ci: bool
    contracts_ci: bool
    security_ci: bool
    docs_only: bool


RESEARCH_PREFIXES = (
    "apps/bt/scripts/research/",
    "apps/bt/docs/experiments/",
)
RESEARCH_TEST_PREFIXES = (
    "apps/bt/tests/unit/scripts/test_run_",
    "apps/bt/tests/unit/domains/analytics/test_topix",
)
RESEARCH_MODULE_MARKERS = (
    "/topix",
    "_research.py",
    "research_bundle.py",
)
PRODUCTION_ANALYTICS_MODULES = {
    "cost_structure",
    "fundamental_ranking",
    "margin_metrics",
    "regression_core",
    "screening_evaluator",
    "screening_requirements",
    "screening_results",
    "value_composite_scoring",
}
PRODUCT_PREFIXES = (
    "apps/bt/src/entrypoints/",
    "apps/bt/src/application/",
    "apps/bt/src/infrastructure/",
    "apps/bt/src/domains/backtest/",
    "apps/bt/src/domains/strategy/",
    "apps/bt/src/domains/optimization/",
    "apps/bt/src/domains/agent/",
    "apps/bt/src/strategy_config/",
    "apps/bt/tests/api/",
    "apps/bt/tests/integration/",
    "apps/bt/tests/paths/",
    "apps/bt/tests/security/",
    "apps/bt/tests/server/",
    "apps/ts/",
)
SHARED_PREFIXES = (
    "apps/bt/src/shared/",
    "apps/bt/src/data/",
    "apps/bt/src/config/",
)
CONTRACT_PREFIXES = (
    "contracts/",
    "apps/ts/packages/contracts/",
    "apps/bt/src/entrypoints/http/routes/",
    "apps/bt/src/entrypoints/http/schemas/",
)
SECURITY_PREFIXES = (
    ".github/dependabot.yml",
    "apps/bt/uv.lock",
    "apps/bt/pyproject.toml",
    "apps/ts/bun.lock",
    "apps/ts/package.json",
)
ALWAYS_PRODUCT_PREFIXES = (
    ".github/workflows/",
    "scripts/ci/",
    "scripts/prepush-ci.sh",
    "scripts/test-apps.sh",
    "scripts/test-packages.sh",
    "scripts/typecheck.sh",
    "scripts/lint.sh",
)


def _is_docs_path(path: str) -> bool:
    return path.endswith((".md", ".mdx", ".txt")) or path.startswith("docs/")


def _is_research_path(path: str) -> bool:
    if path.startswith(RESEARCH_PREFIXES) or path.startswith(RESEARCH_TEST_PREFIXES):
        return True
    if path.startswith("scripts/check-research-guardrails.py"):
        return True
    if path.startswith("apps/bt/src/domains/analytics/"):
        module_name = _analytics_module_name(path)
        if module_name in PRODUCTION_ANALYTICS_MODULES:
            return False
        if module_name is not None:
            return True
        return any(marker in path for marker in RESEARCH_MODULE_MARKERS)
    return False


def _analytics_module_name(path: str) -> str | None:
    prefix = "apps/bt/src/domains/analytics/"
    if not path.startswith(prefix) or not path.endswith(".py"):
        return None
    return path.removeprefix(prefix).removesuffix(".py")


def _is_product_path(path: str) -> bool:
    module_name = _analytics_module_name(path)
    return (
        path.startswith(PRODUCT_PREFIXES)
        or path.startswith(ALWAYS_PRODUCT_PREFIXES)
        or module_name in PRODUCTION_ANALYTICS_MODULES
    )


def _is_shared_path(path: str) -> bool:
    return path.startswith(SHARED_PREFIXES)


def _is_contract_path(path: str) -> bool:
    return path.startswith(CONTRACT_PREFIXES) or path == "scripts/check-contract-sync.sh"


def _is_security_path(path: str) -> bool:
    return path.startswith(SECURITY_PREFIXES) or path.startswith("scripts/check-privacy-leaks.py")


def classify_changed_paths(paths: list[str]) -> CiScope:
    normalized = [path.strip().lstrip("./") for path in paths if path.strip()]
    if not normalized:
        return CiScope(
            product_ci=True,
            research_ci=True,
            contracts_ci=True,
            security_ci=True,
            docs_only=False,
        )

    docs_only = all(_is_docs_path(path) for path in normalized)
    product_ci = False
    research_ci = False
    contracts_ci = False
    security_ci = False

    for path in normalized:
        is_docs = _is_docs_path(path)
        is_shared = _is_shared_path(path)
        is_product = _is_product_path(path)
        is_research = _is_research_path(path)
        is_contract = _is_contract_path(path)
        is_security = _is_security_path(path)

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
