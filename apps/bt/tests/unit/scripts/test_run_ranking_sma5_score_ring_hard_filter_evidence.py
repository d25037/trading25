from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from scripts.research import run_ranking_sma5_score_ring_hard_filter_evidence as runner
from src.domains.analytics.ranking_sma5_score_ring_hard_filter_evidence import (
    HardFilterPitLineage,
    RankingSma5ScoreRingHardFilterResearchResult,
)


EXPECTED_TABLES = {
    "rule_registry_df",
    "coverage_diagnostics_df",
    "trade_ledger_df",
    "portfolio_daily_df",
    "entry_rule_evidence_df",
    "exit_rule_evidence_df",
    "combined_rule_evidence_df",
    "annual_stability_df",
    "bootstrap_effect_ci_df",
    "cost_sensitivity_df",
    "decision_gate_df",
    "observation_sample_df",
}


def test_parse_args_uses_frozen_research_defaults() -> None:
    args = runner.parse_args([])

    assert args.start_date == "2018-01-01"
    assert args.end_date is None
    assert args.bootstrap_block_length == 20
    assert args.bootstrap_resamples == 2_000
    assert args.bootstrap_seed == 20260724
    assert args.min_trades == 200
    assert args.min_signal_dates == 100
    assert args.cost_levels == "0,10,20"
    assert args.output_root is None
    assert args.run_id is None
    assert args.notes is None


def test_parse_args_accepts_explicit_runner_and_bundle_overrides() -> None:
    args = runner.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2020-01-01",
            "--end-date",
            "2025-06-30",
            "--bootstrap-block-length",
            "7",
            "--bootstrap-resamples",
            "100",
            "--bootstrap-seed",
            "11",
            "--min-trades",
            "12",
            "--min-signal-dates",
            "9",
            "--cost-levels",
            "0,10,20",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "smoke",
            "--notes",
            "bounded smoke",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.start_date == "2020-01-01"
    assert args.end_date == "2025-06-30"
    assert args.bootstrap_block_length == 7
    assert args.bootstrap_resamples == 100
    assert args.bootstrap_seed == 11
    assert args.min_trades == 12
    assert args.min_signal_dates == 9
    assert args.cost_levels == "0,10,20"
    assert args.output_root == "/tmp/research"
    assert args.run_id == "smoke"
    assert args.notes == "bounded smoke"


def test_bundle_writes_exact_tables_and_canonical_market_v5_metadata(tmp_path: Path) -> None:
    result = _domain_result(tmp_path / "market.duckdb")

    bundle = runner.write_ranking_sma5_score_ring_hard_filter_bundle(
        result,
        output_root=tmp_path / "bundles",
        run_id="unit",
        notes="focused runner test",
        block_length=2,
        resamples=10,
        seed=7,
        cost_levels=(0.0, 10.0, 20.0),
    )

    conn = duckdb.connect(str(bundle.results_db_path), read_only=True)
    try:
        observed_tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
        observed_costs = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT cost_bps FROM trade_ledger_df"
            ).fetchall()
        }
    finally:
        conn.close()
    manifest = json.loads(bundle.manifest_path.read_text(encoding="utf-8"))

    assert observed_tables == EXPECTED_TABLES
    assert observed_costs == {0.0, 10.0, 20.0}
    assert set(manifest["output_tables"]) == EXPECTED_TABLES
    assert manifest["result_metadata"] == {
        "execution_policy": "close_proxy_same_session",
        "execution_is_optimistic": True,
        "market_schema_version": 5,
        "stock_price_adjustment_mode": "provider_adjusted_v1",
        "market_source": "unit fixture",
        "source_mode": "live",
        "primary_ring": "core_high_high",
        "primary_holding_cap": 60,
        "robustness_holding_cap": 20,
        "discovery_period": ["2018-01-01", "2021-12-31"],
        "validation_period": ["2022-01-01", "2024-12-31"],
        "holdout_period": ["2025-01-01", "2025-01-08"],
    }
    assert not (bundle.bundle_dir / "summary.json").exists()
    assert "published_summary" not in bundle.manifest_path.read_text(encoding="utf-8")


def test_frozen_variant_execution_builds_signal_frames_once_per_variant(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _domain_result(tmp_path / "market.duckdb")
    build_frames = runner.build_position_signal_frames
    calls = 0

    def counting_build_frames(*args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        return build_frames(*args, **kwargs)

    monkeypatch.setattr(runner, "build_position_signal_frames", counting_build_frames)

    executions = runner._execute_frozen_variants(
        result.feature_df,
        cost_levels=(0.0, 10.0, 20.0),
    )

    assert calls == len(runner._frozen_variants())
    assert len(executions) == len(runner._frozen_variants()) * 3
    assert all(not hasattr(execution, "portfolio") for execution in executions)
    assert all(not hasattr(execution, "signal_frames") for execution in executions)


@pytest.mark.parametrize(
    ("lineage", "message"),
    [
        (
            HardFilterPitLineage(4, "provider_adjusted_v1", "unit fixture", "live"),
            "market_schema_version=5",
        ),
        (
            HardFilterPitLineage(5, "legacy_adjusted_v1", "unit fixture", "live"),
            "stock_price_adjustment_mode=provider_adjusted_v1",
        ),
        (
            HardFilterPitLineage(5, "provider_adjusted_v1", "", "live"),
            "market_source",
        ),
    ],
)
def test_bundle_rejects_incompatible_or_incomplete_lineage(
    tmp_path: Path,
    lineage: HardFilterPitLineage,
    message: str,
) -> None:
    result = replace(_domain_result(tmp_path / "market.duckdb"), pit_lineage=lineage)

    with pytest.raises(ValueError, match=message):
        runner.write_ranking_sma5_score_ring_hard_filter_bundle(
            result,
            output_root=tmp_path / "bundles",
            run_id="bad-lineage",
            resamples=10,
        )


def _domain_result(db_path: Path) -> RankingSma5ScoreRingHardFilterResearchResult:
    db_path.touch()
    dates = pd.DatetimeIndex(
        [
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2025-01-07",
            "2025-01-08",
        ]
    )
    feature_df = pd.DataFrame(
        {
            "date": dates,
            "code": ["1001"] * len(dates),
            "close": [100.0, 101.0, 102.0, 101.0, 103.0],
            "topix_close": [2000.0, 2001.0, 2002.0, 2003.0, 2004.0],
            "value_composite_equal_score": [0.5, 0.8, 0.8, 0.8, 0.5],
            "long_hybrid_leadership_score": [0.5, 0.8, 0.8, 0.8, 0.5],
            "sma5": [100.0, 100.0, 100.0, 102.0, 102.0],
            "sma5_above_count_5d": [0, 2, 3, 1, 0],
            "sma5_below_streak": [0, 0, 0, 1, 0],
            "sma5_atr20_deviation": [0.0, 0.2, 0.5, -1.1, 0.0],
        }
    )
    return RankingSma5ScoreRingHardFilterResearchResult(
        db_path=str(db_path),
        source_mode="live",
        source_detail="unit fixture",
        analysis_start_date="2024-01-02",
        analysis_end_date="2025-01-08",
        bootstrap_resamples=10,
        min_trades=1,
        min_signal_dates=1,
        pit_lineage=HardFilterPitLineage(
            market_schema_version=5,
            stock_price_adjustment_mode="provider_adjusted_v1",
            market_source="unit fixture",
            source_mode="live",
        ),
        feature_df=feature_df,
        observation_sample_df=feature_df.copy(),
    )
