"""Tests for run_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive script."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.research import (
    run_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive as script_mod,
)


def test_parse_args_accepts_prior_sessions_horizons_and_recent_year_window() -> None:
    args = script_mod.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--adv-window",
            "5",
            "--prior-sessions",
            "42",
            "--horizons",
            "5,20,60",
            "--recent-year-window",
            "8",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.adv_window == 5
    assert args.prior_sessions == 42
    assert args.horizons == (5, 20, 60)
    assert args.recent_year_window == 8


def test_parse_args_rejects_non_positive_adv_window() -> None:
    with pytest.raises(SystemExit):
        script_mod.parse_args(
            [
                "--adv-window",
                "0",
            ]
        )


def test_main_runs_study_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    recorded: dict[str, object] = {}

    def _fake_run(
        db_path: str,
        *,
        adv_window: int,
        prior_sessions: int,
        horizons: tuple[int, ...],
        recent_year_window: int,
    ):
        recorded["run_args"] = {
            "db_path": db_path,
            "adv_window": adv_window,
            "prior_sessions": prior_sessions,
            "horizons": horizons,
            "recent_year_window": recent_year_window,
        }
        return object()

    class _FakeBundle:
        experiment_id = (
            "market-behavior/topix500-positive-eps-missing-forecast-cfo-positive-deep-dive"
        )
        run_id = "fake-run"
        bundle_dir = Path("/tmp/fake-bundle")
        manifest_path = Path("/tmp/fake-bundle/manifest.json")
        results_db_path = Path("/tmp/fake-bundle/results.duckdb")
        summary_path = Path("/tmp/fake-bundle/summary.md")

    def _fake_write(result, **kwargs):
        recorded["bundle_kwargs"] = kwargs
        assert result is not None
        return _FakeBundle()

    monkeypatch.setattr(
        script_mod,
        "run_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive",
        _fake_run,
    )
    monkeypatch.setattr(
        script_mod,
        "write_topix500_positive_eps_missing_forecast_cfo_positive_deep_dive_bundle",
        _fake_write,
    )
    monkeypatch.setattr(
        script_mod,
        "ensure_bt_workdir",
        lambda bt_root: recorded.setdefault("bt_root", bt_root),
    )

    exit_code = script_mod.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--adv-window",
            "7",
            "--prior-sessions",
            "30",
            "--horizons",
            "10,30",
            "--recent-year-window",
            "9",
        ]
    )

    assert exit_code == 0
    assert recorded["bt_root"] == script_mod._BT_ROOT
    assert recorded["run_args"] == {
        "db_path": "/tmp/market.duckdb",
        "adv_window": 7,
        "prior_sessions": 30,
        "horizons": (10, 30),
        "recent_year_window": 9,
    }
    assert recorded["bundle_kwargs"] == {
        "output_root": None,
        "run_id": None,
        "notes": None,
    }
    assert "bundlePath" in capsys.readouterr().out
