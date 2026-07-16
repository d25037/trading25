"""Tests for scripts/ci/research-test-targets.py."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "scripts" / "ci" / "research-test-targets.py"
    spec = importlib.util.spec_from_file_location("research_test_targets", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load research_test_targets module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["research_test_targets"] = module
    spec.loader.exec_module(module)
    return module


def test_runner_change_maps_to_matching_runner_test() -> None:
    module = _load_module()

    targets = module.pytest_targets_for_research_changes(
        ["apps/bt/scripts/research/run_volume_ratio_future_return_regime.py"]
    )

    assert targets == (
        "tests/unit/scripts/test_run_volume_ratio_future_return_regime.py",
    )


def test_research_module_change_maps_to_matching_domain_test() -> None:
    module = _load_module()

    targets = module.pytest_targets_for_research_changes(
        ["apps/bt/src/domains/analytics/volume_ratio_future_return_regime.py"]
    )

    assert targets == (
        "tests/unit/domains/analytics/test_volume_ratio_future_return_regime.py",
    )


def test_deleted_research_python_file_is_not_a_lint_target() -> None:
    module = _load_module()

    targets = module.research_python_files(
        ["apps/bt/src/domains/analytics/topix_streak_state.py"]
    )

    assert targets == ()


def test_runner_without_matching_test_falls_back_to_script_tests() -> None:
    module = _load_module()
    targets = module.pytest_targets_for_research_changes(
        ["apps/bt/scripts/research/run_uncovered_research.py"]
    )
    assert targets == ("tests/unit/scripts",)


def test_domain_without_matching_test_falls_back_to_analytics_tests() -> None:
    module = _load_module()
    targets = module.pytest_targets_for_research_changes(
        ["apps/bt/src/domains/analytics/uncovered_research.py"]
    )
    assert targets == ("tests/unit/domains/analytics",)


def test_research_bundle_change_keeps_infra_tests() -> None:
    module = _load_module()

    targets = module.pytest_targets_for_research_changes(
        ["apps/bt/src/domains/analytics/research_bundle.py"]
    )

    assert targets == (
        "tests/unit/scripts/test_check_research_guardrails.py",
        "tests/unit/domains/analytics/test_research_bundle.py",
    )


def test_guardrail_script_change_maps_to_guardrail_test() -> None:
    module = _load_module()

    targets = module.pytest_targets_for_research_changes(
        ["scripts/check-research-guardrails.py"]
    )

    assert targets == ("tests/unit/scripts/test_check_research_guardrails.py",)


def test_fast_research_targets_are_curated_surface_tests() -> None:
    module = _load_module()

    assert module.fast_research_pytest_targets() == (
        "tests/unit/scripts/test_check_research_guardrails.py",
        "tests/unit/domains/analytics/test_research_bundle.py",
        "tests/unit/domains/analytics/test_research_core.py",
    )


def test_docs_change_has_no_pytest_target() -> None:
    module = _load_module()

    assert (
        module.pytest_targets_for_research_changes(
            ["apps/bt/docs/experiments/market-behavior/foo/README.md"]
        )
        == ()
    )
