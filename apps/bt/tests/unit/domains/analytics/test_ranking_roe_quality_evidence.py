from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.domains.analytics.ranking_roe_quality_evidence import (
    RankingRoeQualityEvidenceResult,
    build_summary_markdown,
    run_ranking_roe_quality_evidence_research,
    write_ranking_roe_quality_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_forecast_operating_profit_growth_evidence import (
    _build_forecast_op_growth_db,
)


def test_ranking_roe_quality_evidence_builds_tables(tmp_path: Path) -> None:
    db_path = _build_roe_quality_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert not result.coverage_diagnostics_df.empty
    assert not result.roe_bucket_evidence_df.empty
    assert not result.forward_roe_bucket_evidence_df.empty
    assert not result.decision_scope_quality_evidence_df.empty
    assert not result.long_deep_dive_quality_evidence_df.empty
    assert not result.short_deep_dive_quality_evidence_df.empty
    assert {
        "roe",
        "roe_percentile",
        "roe_signal",
        "forward_roe",
        "forward_roe_percentile",
        "forward_roe_signal",
        "valuation_signal",
    }.issubset(result.observation_sample_df.columns)
    assert {
        "low_roe_20pct",
        "middle_roe_60pct",
        "high_roe_20pct",
    }.issubset(set(result.roe_bucket_evidence_df["roe_bucket"].astype(str)))
    assert {
        "low_forward_roe_20pct",
        "middle_forward_roe_60pct",
        "high_forward_roe_20pct",
    }.issubset(
        set(result.forward_roe_bucket_evidence_df["forward_roe_bucket"].astype(str))
    )
    assert {
        "roe_high",
        "forward_roe_high",
        "roe_and_forward_roe_high",
    }.issubset(
        set(result.decision_scope_quality_evidence_df["quality_condition"].astype(str))
    )
    assert {
        "high_roe",
        "high_forward_roe",
    }.issubset(set(result.long_deep_dive_quality_evidence_df["deep_scope"].astype(str)))
    assert {
        "low_roe_sector_weak",
        "low_forward_roe_sector_weak",
    }.issubset(set(result.short_deep_dive_quality_evidence_df["deep_scope"].astype(str)))

    sample = result.observation_sample_df
    alpha = sample.loc[sample["code"] == "1111"].iloc[0]
    beta = sample.loc[sample["code"] == "2222"].iloc[0]
    zeta = sample.loc[sample["code"] == "6666"].iloc[0]
    assert alpha["roe"] == pytest.approx(25.0)
    assert alpha["forward_roe"] == pytest.approx(30.0)
    assert beta["roe"] == pytest.approx(2.0)
    assert beta["forward_roe"] == pytest.approx(3.0)
    assert alpha["roe_signal"] in {"roe_high", "roe_very_high"}
    assert beta["roe_signal"] == "roe_low"
    assert zeta["forward_roe_signal"] == "missing_forward_roe"


def test_ranking_roe_quality_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_roe_quality_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking ROE Quality Evidence" in summary
    assert "ROE Bucket Evidence" in summary
    assert "FwdROE Bucket Evidence" in summary
    assert "Daily Ranking Decision Scope x ROE/FwdROE Quality Evidence" in summary
    assert "Explicit Long Deep Dive" in summary
    assert "Explicit Short Deep Dive" in summary

    bundle = write_ranking_roe_quality_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(db_path: Path) -> RankingRoeQualityEvidenceResult:
    return run_ranking_roe_quality_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_roe_quality_db(db_path: Path) -> Path:
    _build_forecast_op_growth_db(db_path)
    conn = duckdb.connect(str(db_path))
    conn.execute("ALTER TABLE statement_metrics_adjusted ADD COLUMN adjusted_eps DOUBLE")
    conn.execute("ALTER TABLE statement_metrics_adjusted ADD COLUMN adjusted_bps DOUBLE")
    conn.execute(
        "ALTER TABLE statement_metrics_adjusted "
        "ADD COLUMN adjusted_forecast_eps DOUBLE"
    )
    quality_rows = [
        ("1111", "2023-05-15", "2023-03-31", "FY", 25.0, 100.0, 30.0, "unit"),
        ("2222", "2023-05-15", "2023-03-31", "FY", 2.0, 100.0, 3.0, "unit"),
        ("3333", "2023-05-15", "2023-03-31", "FY", 10.0, 100.0, 8.0, "unit"),
        ("4444", "2023-05-15", "2023-03-31", "FY", 18.0, 100.0, 20.0, "unit"),
        ("5555", "2023-05-15", "2023-03-31", "FY", -3.0, 100.0, 4.0, "unit"),
        ("6666", "2023-05-15", "2023-03-31", "FY", 4.0, 100.0, None, "unit"),
    ]
    quality_rows.extend(
        (
            str(7000 + extra_index),
            "2023-05-15",
            "2023-03-31",
            "FY",
            5.0 + extra_index * 0.25,
            100.0,
            6.0 + extra_index * 0.2,
            "unit",
        )
        for extra_index in range(60)
    )
    conn.execute(
        """
        CREATE TEMP TABLE quality_fixture (
            code TEXT, disclosed_date DATE, period_end DATE, period_type TEXT,
            adjusted_eps DOUBLE, adjusted_bps DOUBLE,
            adjusted_forecast_eps DOUBLE, source_label TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO quality_fixture VALUES (?, ?, ?, ?, ?, ?, ?, ?)", quality_rows
    )
    conn.execute(
        """
        INSERT INTO statements
        SELECT quality.code, 'quality-' || quality.code, quality.disclosed_date,
               CAST(quality.disclosed_date AS VARCHAR) || 'T15:00:00+09:00',
               quality.period_end, quality.period_type
        FROM quality_fixture AS quality
        """
    )
    conn.execute(
        """
        INSERT INTO statement_metrics_adjusted
        SELECT quality.code, 'quality-' || quality.code, quality.disclosed_date,
               CAST(quality.disclosed_date AS VARCHAR) || 'T15:00:00+09:00',
               quality.period_end, quality.period_type,
               state.fundamentals_adjustment_basis_date,
               state.source_fingerprint,
               quality.adjusted_eps, quality.adjusted_bps,
               quality.adjusted_forecast_eps
        FROM quality_fixture AS quality
        JOIN current_basis_fundamentals_state AS state USING (code)
        """
    )
    conn.execute(
        """
        UPDATE current_basis_fundamentals_state
        SET statement_count = 1
        WHERE code IN (SELECT code FROM quality_fixture)
        """
    )
    conn.close()
    return db_path
