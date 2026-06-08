"""Tests for the forward EPS component decomposition runner."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module("run_forward_eps_component_decomposition.py")


def test_parse_args_accepts_component_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--input-bundle",
            "/tmp/forward-eps/run",
            "--quantile-buckets",
            "4",
            "--severe-loss-threshold-pct",
            "-12",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260509_test",
        ]
    )

    assert args.input_bundle == "/tmp/forward-eps/run"
    assert args.quantile_buckets == 4
    assert args.severe_loss_threshold_pct == -12.0
    assert args.run_id == "20260509_test"


def test_main_runs_component_decomposition_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="strategy-audit/forward-eps-component-decomposition",
        run_id="20260509_test",
    )

    monkeypatch.setattr(
        module,
        "run_forward_eps_component_decomposition",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_forward_eps_component_decomposition_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--input-bundle",
            "/tmp/forward-eps/run",
            "--run-id",
            "20260509_test",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260509_test")
