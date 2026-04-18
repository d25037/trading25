"""Tests for run_forward_eps_threshold_window_study script."""

from __future__ import annotations

from pathlib import Path

from scripts.research import run_forward_eps_threshold_window_study as script_mod


def test_parse_args_accepts_strategy_dataset_and_window_options() -> None:
    args = script_mod.parse_args(
        [
            "--strategy",
            "experimental/robustness/forward_eps_driven",
            "--dataset",
            "primeExTopix500_20260325",
            "--baseline-strategy",
            "experimental/robustness/forward_eps_driven",
            "--rolling-months",
            "12",
            "--step-months",
            "3",
        ]
    )

    assert args.strategies == ["experimental/robustness/forward_eps_driven"]
    assert args.dataset == "primeExTopix500_20260325"
    assert args.baseline_strategy == "experimental/robustness/forward_eps_driven"
    assert args.rolling_months == 12
    assert args.step_months == 3


def test_main_runs_study_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    recorded: dict[str, object] = {}

    def _fake_run(**kwargs):
        recorded["run_kwargs"] = kwargs
        return object()

    class _FakeBundle:
        experiment_id = "strategy-audit/forward-eps-threshold-window-study"
        run_id = "fake-run"
        bundle_dir = Path("/tmp/fake-bundle")
        manifest_path = Path("/tmp/fake-bundle/manifest.json")
        results_db_path = Path("/tmp/fake-bundle/results.duckdb")
        summary_path = Path("/tmp/fake-bundle/summary.md")

    def _fake_write(result, **kwargs):
        recorded["bundle_kwargs"] = kwargs
        assert result is not None
        return _FakeBundle()

    monkeypatch.setattr(script_mod, "run_forward_eps_threshold_window_study", _fake_run)
    monkeypatch.setattr(
        script_mod,
        "write_forward_eps_threshold_window_study_bundle",
        _fake_write,
    )
    monkeypatch.setattr(
        script_mod,
        "ensure_bt_workdir",
        lambda bt_root: recorded.setdefault("bt_root", bt_root),
    )

    exit_code = script_mod.main(
        [
            "--strategy",
            "experimental/robustness/forward_eps_driven_forward_eps_0_4",
            "--dataset",
            "primeExTopix500_20260325",
            "--rolling-months",
            "6",
            "--step-months",
            "1",
        ]
    )

    assert exit_code == 0
    assert recorded["bt_root"] == script_mod._BT_ROOT
    assert recorded["run_kwargs"] == {
        "strategy_names": (
            "experimental/robustness/forward_eps_driven_forward_eps_0_4",
        ),
        "dataset_name": "primeExTopix500_20260325",
        "baseline_strategy_name": "experimental/robustness/forward_eps_driven",
        "rolling_months": 6,
        "rolling_step_months": 1,
    }
    assert recorded["bundle_kwargs"] == {
        "output_root": None,
        "run_id": None,
        "notes": None,
    }
    assert "bundlePath" in capsys.readouterr().out
