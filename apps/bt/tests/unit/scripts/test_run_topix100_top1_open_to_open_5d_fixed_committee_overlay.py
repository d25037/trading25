"""Tests for the TOPIX100 Top1 fixed committee overlay runner."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module(
        "run_topix100_top1_open_to_open_5d_fixed_committee_overlay.py"
    )


def test_parse_args_accepts_overlay_options() -> None:
    module = _load_module()
    args = module.parse_args(
        [
            "--top1-bundle-path",
            "/tmp/top1-source",
            "--committee-bundle-path",
            "/tmp/committee-source",
            "--top-k",
            "1",
            "--sleeve-count",
            "5",
            "--holding-session-count",
            "5",
            "--committee-mean-window-days",
            "1",
            "2",
            "--committee-high-thresholds",
            "0.24",
            "0.25",
            "--committee-low-threshold",
            "0.22",
            "--run-id",
            "20260413_160000_testabcd",
        ]
    )

    assert args.top1_bundle_path == "/tmp/top1-source"
    assert args.committee_bundle_path == "/tmp/committee-source"
    assert args.top_k == 1
    assert args.sleeve_count == 5
    assert args.holding_session_count == 5
    assert args.committee_mean_window_days == [1, 2]
    assert args.committee_high_thresholds == [0.24, 0.25]
    assert args.committee_low_threshold == 0.22
    assert args.run_id == "20260413_160000_testabcd"


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix100-top1-open-to-open-5d-fixed-committee-overlay",
        run_id="20260413_160000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix100_top1_open_to_open_5d_fixed_committee_overlay_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_top1_open_to_open_5d_fixed_committee_overlay_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(["--run-id", "20260413_160000_testabcd"])

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260413_160000_testabcd")
