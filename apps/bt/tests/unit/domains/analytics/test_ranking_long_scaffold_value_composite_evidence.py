from __future__ import annotations

from pathlib import Path

import duckdb

from src.domains.analytics.ranking_long_scaffold_value_composite_evidence import (
    RankingLongScaffoldValueCompositeEvidenceResult,
    _LONG_SCAFFOLDS,
    _VALUE_COMPOSITE_BUCKETS,
    build_summary_markdown,
    run_ranking_long_scaffold_value_composite_evidence_research,
    write_ranking_long_scaffold_value_composite_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_sma5_count_long_evidence import (
    _build_sma5_count_long_db,
)


def test_long_scaffold_value_composite_evidence_builds_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_value_composite_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.horizons == (5, 20)
    assert not result.coverage_diagnostics_df.empty
    assert not result.long_scaffold_evidence_df.empty
    assert not result.value_composite_bucket_evidence_df.empty
    assert not result.long_scaffold_value_composite_bucket_evidence_df.empty
    assert not result.value_composite_bucket_correlation_df.empty
    assert not result.date_basket_evidence_df.empty
    assert {
        "forward_per_percentile",
        "low_forward_per_score",
        "pbr_percentile",
        "low_pbr_score",
        "value_composite_equal_score",
        "long_hybrid_leadership_score",
        "atr20_acceleration_ex_overheat_flag",
        "forward_close_excess_return_20d_pct",
    }.issubset(result.observation_sample_df.columns)
    assert "score_ge_0_90" in set(
        result.value_composite_bucket_evidence_df["value_bucket"].astype(str)
    )
    assert "deep_value" in set(
        result.long_scaffold_value_composite_bucket_evidence_df[
            "long_scaffold"
        ].astype(str)
    )


def test_value_composite_config_defines_pit_score_buckets() -> None:
    buckets = {name: condition for name, condition in _VALUE_COMPOSITE_BUCKETS}
    scaffolds = {name: condition for name, condition in _LONG_SCAFFOLDS}

    assert buckets["score_ge_0_90"] == "value_composite_equal_score >= 0.90"
    assert (
        buckets["score_0_80_to_0_90"]
        == "value_composite_equal_score >= 0.80 AND value_composite_equal_score < 0.90"
    )
    assert buckets["missing"] == "value_composite_equal_score IS NULL"
    assert "deep_value_long_hybrid_atr20_accel" in scaffolds
    assert "value_composite_long_hybrid_atr20_accel" in scaffolds
    assert "value_composite_equal_score >= 0.8" in scaffolds[
        "value_composite_long_hybrid_atr20_accel"
    ]


def test_long_scaffold_value_composite_evidence_writes_bundle(
    tmp_path: Path,
) -> None:
    db_path = _build_value_composite_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Long Scaffold Value Composite Evidence" in summary
    assert "Long Scaffold Evidence" in summary
    assert "Value Composite Bucket Evidence" in summary
    assert "Long Scaffold x Value Composite Bucket Evidence" in summary
    assert "Value Composite Bucket Correlation" in summary
    assert "Date Basket Evidence" in summary

    bundle = write_ranking_long_scaffold_value_composite_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(db_path: Path) -> RankingLongScaffoldValueCompositeEvidenceResult:
    return run_ranking_long_scaffold_value_composite_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(5, 20),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_value_composite_db(db_path: Path) -> Path:
    db_path = _build_sma5_count_long_db(db_path)
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        UPDATE daily_valuation
        SET
            forward_per = CASE
                WHEN code = '1111' THEN 5.0
                WHEN code = '2222' THEN 8.0
                ELSE forward_per
            END,
            pbr = CASE
                WHEN code = '1111' THEN 0.5
                WHEN code = '2222' THEN 0.8
                ELSE pbr
            END
        """
    )
    conn.close()
    return db_path
