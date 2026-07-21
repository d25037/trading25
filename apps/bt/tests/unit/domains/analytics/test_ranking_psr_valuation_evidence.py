from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.domains.analytics.ranking_psr_valuation_evidence import (
    RankingPsrValuationEvidenceResult,
    build_summary_markdown,
    run_ranking_psr_valuation_evidence_research,
    write_ranking_psr_valuation_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_forecast_operating_profit_growth_evidence import (
    _build_forecast_op_growth_db,
)


def test_ranking_psr_valuation_evidence_builds_tables(tmp_path: Path) -> None:
    db_path = _build_psr_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert not result.coverage_diagnostics_df.empty
    assert not result.psr_bucket_evidence_df.empty
    assert not result.decision_scope_psr_evidence_df.empty
    assert not result.long_deep_dive_psr_evidence_df.empty
    assert not result.short_deep_dive_psr_evidence_df.empty
    assert {
        "actual_sales",
        "actual_sales_disclosed_date",
        "psr",
        "psr_percentile",
        "psr_signal",
        "valuation_signal",
    }.issubset(result.observation_sample_df.columns)
    assert {
        "low_psr_20pct",
        "high_psr_20pct",
        "middle_psr_60pct",
    }.issubset(set(result.psr_bucket_evidence_df["psr_bucket"].astype(str)))
    assert {
        "psr_undervalued",
        "psr_overvalued",
        "deep_value_or_psr_undervalued",
    }.issubset(set(result.decision_scope_psr_evidence_df["decision_scope"].astype(str)))
    assert {
        "psr_undervalued",
        "deep_value",
    }.issubset(set(result.long_deep_dive_psr_evidence_df["deep_scope"].astype(str)))
    assert {
        "psr_overvalued_sector_weak",
        "overvalued_sector_weak",
    }.issubset(set(result.short_deep_dive_psr_evidence_df["deep_scope"].astype(str)))

    sample = result.observation_sample_df
    alpha = sample.loc[sample["code"] == "1111"].iloc[0]
    beta = sample.loc[sample["code"] == "2222"].iloc[0]
    assert alpha["psr"] == pytest.approx(110_000_000.0 / 300_000_000.0)
    assert beta["psr"] == pytest.approx(2.2)
    assert alpha["psr_signal"] == "psr_undervalued"
    assert beta["psr_signal"] in {"psr_overvalued", "psr_very_overvalued"}


def test_ranking_psr_valuation_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_psr_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking PSR Valuation Evidence" in summary
    assert "PSR Bucket Evidence" in summary
    assert "Daily Ranking Decision Scope x PSR Evidence" in summary
    assert "Explicit Long Deep Dive" in summary
    assert "Explicit Short Deep Dive" in summary

    bundle = write_ranking_psr_valuation_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(db_path: Path) -> RankingPsrValuationEvidenceResult:
    return run_ranking_psr_valuation_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_psr_db(db_path: Path) -> Path:
    _build_forecast_op_growth_db(db_path)
    conn = duckdb.connect(str(db_path))
    conn.execute("ALTER TABLE statements ADD COLUMN sales DOUBLE")
    conn.execute("ALTER TABLE statements ADD COLUMN type_of_document TEXT")
    statement_rows = [
        ("1111", "2023-05-15", 300_000_000.0, "FY", "FinancialStatements"),
        ("1111", "2024-05-15", 500_000_000.0, "FY", "FinancialStatements"),
        ("2222", "2023-05-15", 100_000_000.0, "FY", "FinancialStatements"),
        ("3333", "2023-05-15", 100_000_000.0, "FY", "FinancialStatements"),
        ("4444", "2023-05-15", 120_000_000.0, "FY", "FinancialStatements"),
        ("5555", "2023-05-15", 150_000_000.0, "FY", "FinancialStatements"),
        ("6666", "2023-05-15", 0.0, "FY", "FinancialStatements"),
    ]
    statement_rows.extend(
        (
            str(7000 + extra_index),
            "2023-05-15",
            90_000_000.0 + extra_index * 1_000_000.0,
            "FY",
            "FinancialStatements",
        )
        for extra_index in range(60)
    )
    conn.executemany(
        "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                code,
                f"statement-{code}-{disclosed_date}",
                disclosed_date,
                f"{disclosed_date}T15:00:00+09:00",
                "2023-03-31",
                period_type,
                sales,
                document_type,
            )
            for code, disclosed_date, sales, period_type, document_type in statement_rows
        ],
    )
    conn.execute(
        """
        INSERT INTO statement_metrics_adjusted
        SELECT statement.code, statement.statement_id, statement.disclosed_date,
               statement.disclosed_at, statement.period_end,
               statement.type_of_current_period,
               state.fundamentals_adjustment_basis_date,
               state.source_fingerprint
        FROM statements AS statement
        JOIN current_basis_fundamentals_state AS state USING (code)
        """
    )
    conn.execute(
        """
        UPDATE current_basis_fundamentals_state
        SET statement_count = statement_counts.statement_count
        FROM (
            SELECT code, count(*) AS statement_count
            FROM statements
            GROUP BY code
        ) AS statement_counts
        WHERE current_basis_fundamentals_state.code = statement_counts.code
        """
    )
    conn.close()
    return db_path
