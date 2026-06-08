"""Tests for the forward EPS trade-archetype runner script."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module(
        "run_forward_eps_trade_archetype_decomposition.py"
    )


def test_parse_args_accepts_custom_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--strategy",
            "production/forward_eps_driven",
            "--dataset",
            "primeExTopix500_20260325",
            "--holdout-months",
            "9",
            "--severe-loss-threshold-pct",
            "-12.5",
            "--quantile-buckets",
            "6",
        ]
    )

    assert args.strategy == "production/forward_eps_driven"
    assert args.dataset == "primeExTopix500_20260325"
    assert args.holdout_months == 9
    assert args.severe_loss_threshold_pct == -12.5
    assert args.quantile_buckets == 6


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="strategy-audit/forward-eps-trade-archetype-decomposition",
        run_id="20260418_120000_testabcd",
    )
    recorded: dict[str, object] = {}

    monkeypatch.setattr(
        module,
        "run_forward_eps_trade_archetype_decomposition",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_forward_eps_trade_archetype_decomposition_bundle",
        lambda result, **kwargs: fake_bundle,
    )
    monkeypatch.setattr(
        module,
        "ensure_bt_workdir",
        lambda bt_root: recorded.setdefault("bt_root", bt_root),
    )

    exit_code = module.main(["--run-id", "20260418_120000_testabcd"])

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert recorded["bt_root"] == module._BT_ROOT
    assert_standard_bundle_payload(payload, run_id="20260418_120000_testabcd")
