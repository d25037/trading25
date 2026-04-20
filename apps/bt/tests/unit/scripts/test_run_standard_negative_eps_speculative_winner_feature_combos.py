"""Tests for run_standard_negative_eps_speculative_winner_feature_combos script."""

from __future__ import annotations

import sys
from pathlib import Path

from scripts.research import run_standard_negative_eps_speculative_winner_feature_combos as script_mod


def test_ensure_bt_root_on_path_inserts_missing_root(monkeypatch) -> None:
    bt_root = Path(script_mod.__file__).resolve().parents[2]
    monkeypatch.setattr(sys, "path", [path for path in sys.path if path != str(bt_root)])

    resolved = script_mod._ensure_bt_root_on_path()

    assert resolved == bt_root
    assert sys.path[0] == str(bt_root)


def test_parse_args_accepts_combo_mining_parameters() -> None:
    args = script_mod.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--adv-window",
            "5",
            "--winner-quantile",
            "0.85",
            "--min-event-count",
            "12",
            "--min-winner-count",
            "4",
            "--top-examples-limit",
            "6",
            "--sparse-sector-min-event-count",
            "9",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.adv_window == 5
    assert args.winner_quantile == 0.85
    assert args.min_event_count == 12
    assert args.min_winner_count == 4
    assert args.top_examples_limit == 6
    assert args.sparse_sector_min_event_count == 9


def test_main_runs_study_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    recorded: dict[str, object] = {}

    def _fake_run(
        db_path: str,
        *,
        adv_window: int,
        winner_quantile: float,
        min_event_count: int,
        min_winner_count: int,
        top_examples_limit: int,
        sparse_sector_min_event_count: int,
    ):
        recorded["run_args"] = {
            "db_path": db_path,
            "adv_window": adv_window,
            "winner_quantile": winner_quantile,
            "min_event_count": min_event_count,
            "min_winner_count": min_winner_count,
            "top_examples_limit": top_examples_limit,
            "sparse_sector_min_event_count": sparse_sector_min_event_count,
        }
        return object()

    class _FakeBundle:
        experiment_id = "market-behavior/standard-negative-eps-speculative-winner-feature-combos"
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
        "run_standard_negative_eps_speculative_winner_feature_combos",
        _fake_run,
    )
    monkeypatch.setattr(
        script_mod,
        "write_standard_negative_eps_speculative_winner_feature_combos_bundle",
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
            "--winner-quantile",
            "0.8",
            "--min-event-count",
            "10",
            "--min-winner-count",
            "2",
            "--top-examples-limit",
            "8",
            "--sparse-sector-min-event-count",
            "5",
        ]
    )

    assert exit_code == 0
    assert recorded["bt_root"] == script_mod._BT_ROOT
    assert recorded["run_args"] == {
        "db_path": "/tmp/market.duckdb",
        "adv_window": 7,
        "winner_quantile": 0.8,
        "min_event_count": 10,
        "min_winner_count": 2,
        "top_examples_limit": 8,
        "sparse_sector_min_event_count": 5,
    }
    assert recorded["bundle_kwargs"] == {
        "output_root": None,
        "run_id": None,
        "notes": None,
    }
    assert "bundlePath" in capsys.readouterr().out
