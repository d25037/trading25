"""Tests for run_standard_missing_forecast_cfo_non_positive_deep_dive script."""

from __future__ import annotations

from pathlib import Path

from scripts.research import run_standard_missing_forecast_cfo_non_positive_deep_dive as script_mod


def test_parse_args_accepts_market_prior_sessions_and_horizons() -> None:
    args = script_mod.parse_args(
        [
            "--market",
            "primeExTopix500",
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

    assert args.market == "primeExTopix500"
    assert args.db_path == "/tmp/market.duckdb"
    assert args.adv_window == 5
    assert args.prior_sessions == 42
    assert args.horizons == (5, 20, 60)
    assert args.recent_year_window == 8


def test_main_runs_study_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    recorded: dict[str, object] = {}

    def _fake_run(
        db_path: str,
        *,
        market: str,
        adv_window: int,
        prior_sessions: int,
        horizons: tuple[int, ...],
        recent_year_window: int,
    ):
        recorded["run_args"] = {
            "db_path": db_path,
            "market": market,
            "adv_window": adv_window,
            "prior_sessions": prior_sessions,
            "horizons": horizons,
            "recent_year_window": recent_year_window,
        }
        return object()

    class _FakeBundle:
        experiment_id = "market-behavior/standard-missing-forecast-cfo-non-positive-deep-dive"
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
        "run_standard_missing_forecast_cfo_non_positive_deep_dive",
        _fake_run,
    )
    monkeypatch.setattr(
        script_mod,
        "write_standard_missing_forecast_cfo_non_positive_deep_dive_bundle",
        _fake_write,
    )
    monkeypatch.setattr(
        script_mod,
        "ensure_bt_workdir",
        lambda bt_root: recorded.setdefault("bt_root", bt_root),
    )

    exit_code = script_mod.main(
        [
            "--market",
            "primeExTopix500",
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
        "market": "primeExTopix500",
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
