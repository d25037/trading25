#!/usr/bin/env python3
"""Shared test and CI path taxonomy."""

from __future__ import annotations


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
    "fundamental_ranking",
    "margin_metrics",
    "regression_core",
    "screening_evaluator",
    "screening_requirements",
    "screening_results",
    "market_bubble_footprint_monitor",
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
    "apps/bt/src/domains/lab_agent/",
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
    "apps/bt/src/application/contracts/",
    "apps/bt/src/entrypoints/http/routes/",
    "apps/bt/src/entrypoints/http/schemas/",
    "apps/bt/src/shared/models/",
)
CONTRACT_FILES = {
    "apps/bt/pyproject.toml",
    "apps/bt/src/entrypoints/http/app.py",
    "apps/bt/src/entrypoints/http/openapi_config.py",
    "scripts/check-contract-sync.sh",
    "scripts/openapi_compat.py",
}
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
GOVERNANCE_PREFIXES = (
    ".codex/skills/",
    "scripts/skills/",
    "apps/bt/tests/unit/scripts/test_audit_skills.py",
)


def normalize_path(path: str) -> str:
    normalized = path.strip()
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def analytics_module_name(path: str) -> str | None:
    prefix = "apps/bt/src/domains/analytics/"
    if not path.startswith(prefix) or not path.endswith(".py"):
        return None
    return path.removeprefix(prefix).removesuffix(".py")


def is_docs_path(path: str) -> bool:
    return path.endswith((".md", ".mdx", ".txt")) or path.startswith("docs/")


def is_governance_path(path: str) -> bool:
    return path == "AGENTS.md" or path.endswith("/AGENTS.md") or path.startswith(
        GOVERNANCE_PREFIXES
    )


def is_research_path(path: str) -> bool:
    if path.startswith(RESEARCH_PREFIXES) or path.startswith(RESEARCH_TEST_PREFIXES):
        return True
    if path.startswith("scripts/check-research-guardrails.py"):
        return True
    if path.startswith("apps/bt/src/domains/analytics/"):
        module_name = analytics_module_name(path)
        if module_name in PRODUCTION_ANALYTICS_MODULES:
            return False
        if module_name is not None:
            return True
        return any(marker in path for marker in RESEARCH_MODULE_MARKERS)
    return False


def is_product_path(path: str) -> bool:
    module_name = analytics_module_name(path)
    return (
        path.startswith(PRODUCT_PREFIXES)
        or path.startswith(ALWAYS_PRODUCT_PREFIXES)
        or module_name in PRODUCTION_ANALYTICS_MODULES
    )


def is_shared_path(path: str) -> bool:
    return path.startswith(SHARED_PREFIXES)


def is_contract_path(path: str) -> bool:
    production_domain = path.startswith("apps/bt/src/domains/") and is_product_path(path)
    return path.startswith(CONTRACT_PREFIXES) or path in CONTRACT_FILES or production_domain


def is_security_path(path: str) -> bool:
    return path.startswith(SECURITY_PREFIXES) or path.startswith("scripts/check-privacy-leaks.py")
