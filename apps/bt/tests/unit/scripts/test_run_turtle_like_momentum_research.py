"""Tests for the Turtle-like momentum research runner script."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module("run_turtle_like_momentum_research.py")


def test_parse_args_accepts_turtle_like_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-12-31",
            "--channel-specs",
            "20:10,55:20",
            "--entry-modes",
            "close_confirmed,high_touch_next_open",
            "--sizing-methods",
            "equal_weight,inverse_atr",
            "--min-avg-trading-value-mil-jpy",
            "10",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260509_test",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.channel_specs == ((20, 10), (55, 20))
    assert args.entry_modes == ("close_confirmed", "high_touch_next_open")
    assert args.sizing_methods == ("equal_weight", "inverse_atr")
    assert args.run_id == "20260509_test"


def test_main_runs_turtle_like_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/turtle-like-momentum-research",
        run_id="20260509_test",
    )

    monkeypatch.setattr(
        module,
        "run_turtle_like_momentum_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_turtle_like_momentum_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260509_test",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260509_test")
