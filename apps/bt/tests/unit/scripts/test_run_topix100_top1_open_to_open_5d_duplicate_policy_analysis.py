"""Tests for the TOPIX100 duplicate-policy analysis runner."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module(
        "run_topix100_top1_open_to_open_5d_duplicate_policy_analysis.py"
    )


def test_parse_args_accepts_duplicate_policy_options() -> None:
    module = _load_module()
    args = module.parse_args(
        [
            "--fallback-candidate-top-k",
            "5",
            "--duplicate-policies",
            "allow_stack",
            "skip_if_held",
            "next_unique_within_top5",
            "--run-id",
            "20260413_170000_testabcd",
        ]
    )

    assert args.fallback_candidate_top_k == 5
    assert args.duplicate_policies == [
        "allow_stack",
        "skip_if_held",
        "next_unique_within_top5",
    ]
    assert args.run_id == "20260413_170000_testabcd"


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix100-top1-open-to-open-5d-duplicate-policy-analysis",
        run_id="20260413_170000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix100_top1_open_to_open_5d_duplicate_policy_analysis",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_top1_open_to_open_5d_duplicate_policy_analysis_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(["--run-id", "20260413_170000_testabcd"])

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260413_170000_testabcd")
