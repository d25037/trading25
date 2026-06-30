from __future__ import annotations

from pathlib import Path

from src.domains.analytics.ranking_sma5_atr_deviation_evidence import (
    RankingSma5AtrDeviationEvidenceResult,
    build_summary_markdown,
    run_ranking_sma5_atr_deviation_evidence_research,
    write_ranking_sma5_atr_deviation_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_sma5_count_long_evidence import (
    _build_sma5_count_long_db,
)
from tests.unit.domains.analytics.test_ranking_sma5_deviation_evidence import (
    _add_statements_fixture,
)


def test_sma5_atr_deviation_evidence_builds_direction_threshold_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_sma5_count_long_db(tmp_path / "market.duckdb")
    _add_statements_fixture(db_path)

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.atr_windows == (5, 20)
    assert not result.coverage_diagnostics_df.empty
    assert not result.sma5_atr_deviation_bucket_evidence_df.empty
    assert not result.long_scaffold_sma5_atr_threshold_evidence_df.empty
    assert not result.short_overlay_sma5_atr_threshold_evidence_df.empty
    assert {
        "sma5_atr5_deviation",
        "sma5_atr20_deviation",
        "sma5_atr5_deviation_bucket",
        "sma5_atr20_deviation_bucket",
        "atr5",
        "atr20",
        "forward_close_excess_return_5d_pct",
        "forward_close_excess_return_20d_pct",
    }.issubset(result.observation_sample_df.columns)
    assert {5, 20}.issubset(
        set(result.sma5_atr_deviation_bucket_evidence_df["atr_window"].astype(int))
    )
    assert {"above", "below"}.issubset(
        set(result.long_scaffold_sma5_atr_threshold_evidence_df["direction"].astype(str))
    )
    assert {0.05, 0.1}.issubset(
        set(
            result.long_scaffold_sma5_atr_threshold_evidence_df[
                "threshold_abs_atr"
            ].astype(float)
        )
    )


def test_sma5_atr_deviation_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_sma5_count_long_db(tmp_path / "market.duckdb")
    _add_statements_fixture(db_path)
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking SMA5 ATR Deviation Evidence" in summary
    assert "SMA5 ATR Deviation Bucket Evidence" in summary
    assert "Long Scaffold x SMA5 ATR Threshold Evidence" in summary

    bundle = write_ranking_sma5_atr_deviation_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(db_path: Path) -> RankingSma5AtrDeviationEvidenceResult:
    return run_ranking_sma5_atr_deviation_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(5, 20),
        threshold_abs_atr=(0.05, 0.1),
        market_scopes=("prime",),
        liquidity_bands=("high", "mid", "low"),
        min_observations=1,
        observation_sample_limit=100,
    )
