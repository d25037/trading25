"""Tests for the falling-knife bad-tail pruning runner script."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module("run_falling_knife_bad_tail_pruning.py")


def test_parse_args_accepts_bad_tail_pruning_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--input-bundle",
            "/tmp/input",
            "--horizon-days",
            "60",
            "--severe-loss-threshold",
            "-0.15",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260427_120500_testabcd",
        ]
    )

    assert args.input_bundle == "/tmp/input"
    assert args.horizon_days == 60
    assert args.severe_loss_threshold == -0.15
    assert args.run_id == "20260427_120500_testabcd"


def test_main_runs_bad_tail_pruning_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/falling-knife-bad-tail-pruning",
        run_id="20260427_120500_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_falling_knife_bad_tail_pruning",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_falling_knife_bad_tail_pruning_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--input-bundle",
            "/tmp/input",
            "--run-id",
            "20260427_120500_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260427_120500_testabcd")
