"""Tests for the range-break trade-archetype runner script."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from tests.unit.scripts.research_runner_test_helpers import load_research_runner_module


def _load_runner_module():
    return load_research_runner_module(
        "run_range_break_trade_archetype_decomposition.py"
    )


def test_parse_args_accepts_range_break_options() -> None:
    module = _load_runner_module()

    args = module.parse_args(
        [
            "--strategy",
            "production/range_break_v15",
            "--dataset",
            "primeExTopix500",
            "--holdout-months",
            "9",
            "--severe-loss-threshold-pct",
            "-12",
            "--quantile-buckets",
            "4",
        ]
    )

    assert args.strategy == "production/range_break_v15"
    assert args.dataset == "primeExTopix500"
    assert args.holdout_months == 9
    assert args.severe_loss_threshold_pct == -12
    assert args.quantile_buckets == 4


def test_main_runs_range_break_decomposition_and_emits_bundle(monkeypatch) -> None:
    module = _load_runner_module()
    calls: dict[str, object] = {}
    fake_result = SimpleNamespace()
    fake_bundle = SimpleNamespace(
        experiment_id="strategy-audit/range-break-trade-archetype-decomposition",
        bundle_dir=Path("/tmp/range-break-bundle"),
    )

    monkeypatch.setattr(
        module, "ensure_bt_workdir", lambda root: calls.setdefault("root", root)
    )

    def fake_run(**kwargs):
        calls["run_kwargs"] = kwargs
        return fake_result

    def fake_write(result, **kwargs):
        calls["write_result"] = result
        calls["write_kwargs"] = kwargs
        return fake_bundle

    monkeypatch.setattr(
        module, "run_range_break_trade_archetype_decomposition", fake_run
    )
    monkeypatch.setattr(
        module, "write_range_break_trade_archetype_decomposition_bundle", fake_write
    )
    monkeypatch.setattr(
        module, "emit_bundle_payload", lambda bundle: calls.setdefault("bundle", bundle)
    )

    exit_code = module.main(["--dataset", "primeExTopix500", "--run-id", "test-run"])

    assert exit_code == 0
    assert calls["run_kwargs"] == {
        "strategy_name": "production/range_break_v15",
        "dataset_name": "primeExTopix500",
        "holdout_months": 6,
        "severe_loss_threshold_pct": -10.0,
        "quantile_bucket_count": 5,
    }
    assert calls["write_result"] is fake_result
    assert calls["write_kwargs"]["run_id"] == "test-run"
    assert calls["bundle"] is fake_bundle
