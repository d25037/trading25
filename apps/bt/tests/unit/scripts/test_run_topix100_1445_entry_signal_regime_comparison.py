"""Tests for the TOPIX100 14:45 signal/regime comparison runner."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module(
        "run_topix100_1445_entry_signal_regime_comparison.py"
    )


def test_parse_args_accepts_runner_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-04-18",
            "--end-date",
            "2026-04-17",
            "--intervals",
            "5,15,30",
            "--bucket-count",
            "4",
            "--period-months",
            "6",
            "--entry-time",
            "14:45",
            "--next-session-exit-time",
            "10:30",
            "--tail-fraction",
            "0.15",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260418_174500_testabcd",
            "--notes",
            "14:45 signal regime bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.intervals == "5,15,30"
    assert args.bucket_count == 4
    assert args.period_months == 6
    assert args.entry_time == "14:45"
    assert args.next_session_exit_time == "10:30"
    assert args.tail_fraction == 0.15
    assert args.run_id == "20260418_174500_testabcd"


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix100-1445-entry-signal-regime-comparison",
        run_id="20260418_174500_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix100_1445_entry_signal_regime_comparison_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_1445_entry_signal_regime_comparison_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--intervals",
            "5,15,30",
            "--bucket-count",
            "4",
            "--period-months",
            "6",
            "--entry-time",
            "14:45",
            "--next-session-exit-time",
            "10:30",
            "--tail-fraction",
            "0.10",
            "--run-id",
            "20260418_174500_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260418_174500_testabcd")
