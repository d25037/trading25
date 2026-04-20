"""Tests for run_standard_negative_eps_right_tail_decomposition script."""

from __future__ import annotations

from pathlib import Path

from scripts.research import run_standard_negative_eps_right_tail_decomposition as script_mod


def test_parse_args_accepts_market_db_path_and_adv_window() -> None:
    args = script_mod.parse_args(
        [
            "--market",
            "prime",
            "--db-path",
            "/tmp/market.duckdb",
            "--adv-window",
            "5",
        ]
    )

    assert args.market == "prime"
    assert args.db_path == "/tmp/market.duckdb"
    assert args.adv_window == 5


def test_main_runs_study_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    recorded: dict[str, object] = {}

    def _fake_run(db_path: str, *, market: str, adv_window: int):
        recorded["run_args"] = {
            "db_path": db_path,
            "market": market,
            "adv_window": adv_window,
        }
        return object()

    class _FakeBundle:
        experiment_id = "market-behavior/standard-negative-eps-right-tail-decomposition"
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
        "run_standard_negative_eps_right_tail_decomposition",
        _fake_run,
    )
    monkeypatch.setattr(
        script_mod,
        "write_standard_negative_eps_right_tail_bundle",
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
            "prime",
            "--db-path",
            "/tmp/market.duckdb",
            "--adv-window",
            "7",
        ]
    )

    assert exit_code == 0
    assert recorded["bt_root"] == script_mod._BT_ROOT
    assert recorded["run_args"] == {
        "db_path": "/tmp/market.duckdb",
        "market": "prime",
        "adv_window": 7,
    }
    assert recorded["bundle_kwargs"] == {
        "output_root": None,
        "run_id": None,
        "notes": None,
    }
    assert "bundlePath" in capsys.readouterr().out
