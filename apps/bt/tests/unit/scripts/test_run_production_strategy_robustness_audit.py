"""Tests for the production strategy robustness audit runner script."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module("run_production_strategy_robustness_audit.py")


def test_parse_args_accepts_strategy_dataset_and_holdout_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--strategy",
            "production/forward_eps_driven",
            "--strategy",
            "production/range_break_v15",
            "--dataset",
            "primeExTopix500_20260325",
            "--dataset",
            "topix500_20260325",
            "--holdout-months",
            "9",
            "--run-id",
            "20260417_120000_testabcd",
        ]
    )

    assert args.strategies == [
        "production/forward_eps_driven",
        "production/range_break_v15",
    ]
    assert args.datasets == [
        "primeExTopix500_20260325",
        "topix500_20260325",
    ]
    assert args.holdout_months == 9
    assert args.run_id == "20260417_120000_testabcd"


def test_main_runs_audit_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="strategy-audit/production-strategy-robustness",
        run_id="20260417_120000_testabcd",
    )
    recorded: dict[str, object] = {}

    monkeypatch.setattr(
        module,
        "run_production_strategy_robustness_audit",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_production_strategy_robustness_audit_bundle",
        lambda result, **kwargs: fake_bundle,
    )
    monkeypatch.setattr(
        module,
        "ensure_bt_workdir",
        lambda bt_root: recorded.setdefault("bt_root", bt_root),
    )

    exit_code = module.main(
        [
            "--run-id",
            "20260417_120000_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert recorded["bt_root"] == module._BT_ROOT
    assert_standard_bundle_payload(payload, run_id="20260417_120000_testabcd")
