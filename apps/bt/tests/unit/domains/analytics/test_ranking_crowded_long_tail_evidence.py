from __future__ import annotations

from pathlib import Path

import duckdb

from src.domains.analytics.ranking_crowded_long_tail_evidence import (
    RankingCrowdedLongTailEvidenceResult,
    build_summary_markdown,
    run_ranking_crowded_long_tail_evidence_research,
    write_ranking_crowded_long_tail_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_forecast_operating_profit_growth_evidence import (
    _build_forecast_op_growth_db,
)


def test_ranking_crowded_long_tail_evidence_builds_tables(tmp_path: Path) -> None:
    db_path = _build_crowded_long_tail_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert not result.coverage_diagnostics_df.empty
    assert not result.valuation_overlap_tail_df.empty
    assert not result.atr_overheat_tail_df.empty
    assert not result.sector_bucket_tail_df.empty
    assert not result.horizon_path_tail_df.empty
    assert {
        "pbr_percentile",
        "psr_percentile",
        "forward_psr_percentile",
        "atr20_acceleration_ex_overheat_flag",
        "sector_strength_bucket",
    }.issubset(result.observation_sample_df.columns)
    assert {"valuation_overlap", "horizon_path"}.issubset(
        set(result.horizon_path_tail_df["dimension"].astype(str))
        | set(result.valuation_overlap_tail_df["dimension"].astype(str))
    )
    assert "low10_pbr" in set(result.valuation_overlap_tail_df["bucket"].astype(str))
    assert "atr_overheat" in set(result.atr_overheat_tail_df["dimension"].astype(str))
    assert "sector_bucket" in set(result.sector_bucket_tail_df["dimension"].astype(str))


def test_ranking_crowded_long_tail_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_crowded_long_tail_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Crowded Long Tail Evidence" in summary
    assert "Valuation Low10 Overlap Tail" in summary
    assert "ATR / Overheat Tail" in summary
    assert "Sector Bucket Tail" in summary

    bundle = write_ranking_crowded_long_tail_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(db_path: Path) -> RankingCrowdedLongTailEvidenceResult:
    return run_ranking_crowded_long_tail_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-06-28",
        horizons=(5,),
        market_scopes=("prime",),
        min_observations=1,
        long_hybrid_threshold=0.6,
        observation_sample_limit=100,
    )


def _build_crowded_long_tail_db(db_path: Path) -> Path:
    _build_forecast_op_growth_db(db_path)
    conn = duckdb.connect(str(db_path))
    conn.execute("ALTER TABLE daily_valuation ADD COLUMN psr DOUBLE")
    conn.execute("ALTER TABLE daily_valuation ADD COLUMN forward_psr DOUBLE")
    conn.execute(
        """
        UPDATE daily_valuation
        SET
            psr = CASE
                WHEN code = '1111' THEN 0.5
                WHEN code = '2222' THEN 0.6
                WHEN code = '3333' THEN 3.0
                ELSE 1.0 + (CAST(code AS INTEGER) % 20) * 0.05
            END,
            forward_psr = CASE
                WHEN code = '1111' THEN 0.4
                WHEN code = '2222' THEN 0.7
                WHEN code = '3333' THEN 2.8
                ELSE 1.1 + (CAST(code AS INTEGER) % 20) * 0.04
            END
        """
    )
    conn.close()
    return db_path
