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


def test_changed_research_test_maps_to_itself() -> None:
    module = _load_module()

    targets = module.pytest_targets_for_research_changes(
        [
            "apps/bt/tests/unit/domains/analytics/"
            "test_ranking_research_selection_contract.py"
        ]
    )

    assert targets == (
        "tests/unit/domains/analytics/"
        "test_ranking_research_selection_contract.py",
    )


def test_deleted_research_test_does_not_map_to_a_missing_pytest_target() -> None:
    module = _load_module()

    targets = module.pytest_targets_for_research_changes(
        [
            "apps/bt/tests/unit/domains/analytics/"
            "test_removed_research_experiment.py"
        ]
    )

    assert targets == ()


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


def test_runner_without_runner_test_maps_to_matching_domain_test() -> None:
    module = _load_module()

    targets = module.pytest_targets_for_research_changes(
        ["apps/bt/scripts/research/run_ranking_fixed_return_priority_evidence.py"]
    )

    assert targets == (
        "tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py",
    )


def test_domain_without_matching_test_falls_back_to_analytics_tests() -> None:
    module = _load_module()
    targets = module.pytest_targets_for_research_changes(
        ["apps/bt/src/domains/analytics/uncovered_research.py"]
    )
    assert targets == ("tests/unit/domains/analytics",)


def test_shared_daily_ranking_helpers_map_to_consumer_tests() -> None:
    module = _load_module()

    expected_consumers = {
        "tests/unit/domains/analytics/test_daily_ranking_research_base.py",
        "tests/unit/domains/analytics/test_daily_ranking_consumer_support.py",
        "tests/unit/domains/analytics/test_market_bubble_footprint.py",
        "tests/unit/domains/analytics/test_ranking_color_evidence.py",
        "tests/unit/domains/analytics/"
        "test_ranking_fixed_return_priority_evidence.py",
        "tests/unit/domains/analytics/"
        "test_ranking_n225_crowded_rerating_benchmark.py",
        "tests/unit/domains/analytics/"
        "test_ranking_n225_neutral_rerating_benchmark.py",
        "tests/unit/domains/analytics/"
        "test_ranking_technical_fit_score_shape_evidence.py",
        "tests/unit/domains/analytics/"
        "test_ranking_trend_acceleration_conditional_lift.py",
    }

    for module_name in (
        "daily_ranking_research_base",
        "daily_ranking_consumer_support",
    ):
        targets = module.pytest_targets_for_research_changes(
            [f"apps/bt/src/domains/analytics/{module_name}.py"]
        )

        assert expected_consumers <= set(targets)
        assert all(module._exists(target) for target in targets)


def test_shared_daily_ranking_market_fixture_maps_to_exact_consumers() -> None:
    module = _load_module()

    targets = module.pytest_targets_for_research_changes(
        [
            "apps/bt/tests/unit/domains/analytics/"
            "daily_ranking_market_v4_fixture.py"
        ]
    )

    assert targets == (
        "tests/unit/domains/analytics/test_atr_expansion_forward_response.py",
        "tests/unit/domains/analytics/test_market_bubble_footprint.py",
        "tests/unit/domains/analytics/test_ranking_sector_strength_evidence.py",
        "tests/unit/domains/analytics/test_ranking_short_red_evidence.py",
        (
            "tests/unit/domains/analytics/"
            "test_ranking_short_sector_strength_evidence.py"
        ),
    )
    assert all(module._exists(target) for target in targets)


def test_deleted_daily_ranking_adapter_falls_back_without_missing_target() -> None:
    module = _load_module()

    targets = module.pytest_targets_for_research_changes(
        [
            "apps/bt/src/domains/analytics/"
            "ranking_technical_fit_price_projection.py"
        ]
    )

    assert targets == ("tests/unit/domains/analytics",)
    assert not any("technical_fit_price_projection" in target for target in targets)


def test_named_shared_experiment_support_maps_to_its_consumers() -> None:
    module = _load_module()
    expected_by_module = {
        "market_bubble_footprint_support": {
            "tests/unit/domains/analytics/test_market_bubble_footprint.py",
            "tests/unit/domains/analytics/test_market_bubble_footprint_monitor.py",
        },
        "ranking_n225_rerating_benchmark_support": {
            "tests/unit/domains/analytics/"
            "test_ranking_n225_crowded_rerating_benchmark.py",
            "tests/unit/domains/analytics/"
            "test_ranking_n225_neutral_rerating_benchmark.py",
        },
    }

    for module_name, expected in expected_by_module.items():
        targets = module.pytest_targets_for_research_changes(
            [f"apps/bt/src/domains/analytics/{module_name}.py"]
        )

        assert set(targets) == expected
        assert all(module._exists(target) for target in targets)


def test_published_technical_fit_digest_maps_to_its_consumer_test() -> None:
    module = _load_module()

    targets = module.pytest_targets_for_research_changes(
        [
            "apps/bt/tests/fixtures/research/"
            "ranking_technical_fit_score_shape_evidence_published_digest.json"
        ]
    )

    assert targets == (
        "tests/unit/domains/analytics/"
        "test_ranking_technical_fit_score_shape_evidence.py",
    )


def test_versioned_ranking_digests_map_to_registry_and_experiment_tests() -> None:
    module = _load_module()
    expected = {
        "ranking_trend_acceleration_conditional_lift_v8_published_digest.json": (
            "tests/unit/domains/analytics/test_ranking_publication_registry.py",
            "tests/unit/domains/analytics/"
            "test_ranking_trend_acceleration_conditional_lift.py",
        ),
        "ranking_fixed_return_priority_evidence_v11_published_digest.json": (
            "tests/unit/domains/analytics/test_ranking_publication_registry.py",
            "tests/unit/domains/analytics/"
            "test_ranking_fixed_return_priority_evidence.py",
        ),
        "ranking_technical_fit_score_shape_evidence_v12_published_digest.json": (
            "tests/unit/domains/analytics/test_ranking_publication_registry.py",
            "tests/unit/domains/analytics/"
            "test_ranking_technical_fit_score_shape_evidence.py",
        ),
        "ranking_trend_acceleration_conditional_lift_v9_published_digest.json": (
            "tests/unit/domains/analytics/test_ranking_publication_registry.py",
            "tests/unit/domains/analytics/"
            "test_ranking_trend_acceleration_conditional_lift.py",
        ),
        "ranking_fixed_return_priority_evidence_v12_published_digest.json": (
            "tests/unit/domains/analytics/test_ranking_publication_registry.py",
            "tests/unit/domains/analytics/"
            "test_ranking_fixed_return_priority_evidence.py",
        ),
        "ranking_technical_fit_score_shape_evidence_v13_published_digest.json": (
            "tests/unit/domains/analytics/test_ranking_publication_registry.py",
            "tests/unit/domains/analytics/"
            "test_ranking_technical_fit_score_shape_evidence.py",
        ),
    }
    for filename, targets in expected.items():
        assert module.pytest_targets_for_research_changes(
            [f"apps/bt/tests/fixtures/research/{filename}"]
        ) == targets


def test_ranking_publication_registry_fixture_maps_to_consistency_test() -> None:
    module = _load_module()

    targets = module.pytest_targets_for_research_changes(
        [
            "apps/bt/tests/fixtures/research/"
            "ranking_publication_registry.json"
        ]
    )

    assert targets == (
        "tests/unit/domains/analytics/test_ranking_publication_registry.py",
    )


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
        "tests/unit/domains/analytics/"
        "test_ranking_research_selection_contract.py",
        "tests/unit/domains/analytics/test_ranking_publication_registry.py",
    )


def test_research_targets_are_partitioned_across_shards_exactly_once() -> None:
    module = _load_module()
    targets = tuple(f"tests/research/test_{index}.py" for index in range(17))

    shards = tuple(
        module.shard_targets(targets, shard_index=index, shard_count=6)
        for index in range(6)
    )

    assert shards[0] == (
        "tests/research/test_0.py",
        "tests/research/test_6.py",
        "tests/research/test_12.py",
    )
    assert sorted(target for shard in shards for target in shard) == sorted(targets)
    assert len({target for shard in shards for target in shard}) == len(targets)


def test_docs_change_has_no_pytest_target() -> None:
    module = _load_module()

    assert (
        module.pytest_targets_for_research_changes(
            ["apps/bt/docs/experiments/market-behavior/foo/README.md"]
        )
        == ()
    )
