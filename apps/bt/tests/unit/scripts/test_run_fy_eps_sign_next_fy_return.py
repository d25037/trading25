"""Tests for run_fy_eps_sign_next_fy_return script."""

from __future__ import annotations

from pathlib import Path

from scripts.research import run_fy_eps_sign_next_fy_return as script_mod


def test_parse_args_accepts_db_path_markets_and_thresholds() -> None:
    args = script_mod.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--market",
            "standard",
            "--market",
            "growth",
            "--forecast-ratio-threshold",
            "1.2",
            "--forecast-ratio-threshold",
            "1.4",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.markets == ["standard", "growth"]
    assert args.forecast_ratio_thresholds == [1.2, 1.4]


def test_parse_args_accepts_prime_ex_topix500_scope() -> None:
    args = script_mod.parse_args(
        [
            "--market",
            "primeExTopix500",
        ]
    )

    assert args.markets == ["primeExTopix500"]


def test_main_runs_study_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    recorded: dict[str, object] = {}

    def _fake_run(
        db_path: str,
        *,
        markets: tuple[str, ...],
        forecast_ratio_thresholds: tuple[float, ...],
    ):
        recorded["run_args"] = {
            "db_path": db_path,
            "markets": markets,
            "forecast_ratio_thresholds": forecast_ratio_thresholds,
        }
        return object()

    class _FakeBundle:
        experiment_id = "market-behavior/fy-eps-sign-next-fy-return"
        run_id = "fake-run"
        bundle_dir = Path("/tmp/fake-bundle")
        manifest_path = Path("/tmp/fake-bundle/manifest.json")
        results_db_path = Path("/tmp/fake-bundle/results.duckdb")
        summary_path = Path("/tmp/fake-bundle/summary.md")

    def _fake_write(result, **kwargs):
        recorded["bundle_kwargs"] = kwargs
        assert result is not None
        return _FakeBundle()

    monkeypatch.setattr(script_mod, "run_fy_eps_sign_next_fy_return", _fake_run)
    monkeypatch.setattr(script_mod, "write_fy_eps_sign_next_fy_return_bundle", _fake_write)
    monkeypatch.setattr(
        script_mod,
        "ensure_bt_workdir",
        lambda bt_root: recorded.setdefault("bt_root", bt_root),
    )

    exit_code = script_mod.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--market",
            "growth",
        ]
    )

    assert exit_code == 0
    assert recorded["bt_root"] == script_mod._BT_ROOT
    assert recorded["run_args"] == {
        "db_path": "/tmp/market.duckdb",
        "markets": ("growth",),
        "forecast_ratio_thresholds": (1.2, 1.4),
    }
    assert recorded["bundle_kwargs"] == {
        "output_root": None,
        "run_id": None,
        "notes": None,
    }
    assert "bundlePath" in capsys.readouterr().out
