"""Tests for the stock intraday / overnight share runner script."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module("run_stock_intraday_overnight_share.py")


def test_parse_args_accepts_stock_intraday_runner_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-12-31",
            "--selected-groups",
            "TOPIX100,TOPIX500",
            "--min-session-count",
            "15",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260331_182000_testabcd",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.selected_groups == "TOPIX100,TOPIX500"
    assert args.min_session_count == 15
    assert args.run_id == "20260331_182000_testabcd"


def test_main_runs_stock_intraday_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/stock-intraday-overnight-share",
        run_id="20260331_182000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_stock_intraday_overnight_share_analysis",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_stock_intraday_overnight_share_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260331_182000_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260331_182000_testabcd")
