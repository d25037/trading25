from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest

from src.domains.analytics.ranking_short_value_composite_evidence import (
    RankingShortValueCompositeEvidenceResult,
    build_summary_markdown,
    run_ranking_short_value_composite_evidence_research,
    write_ranking_short_value_composite_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_short_sector_strength_evidence import (
    _build_short_sector_db,
)


def test_ranking_short_value_composite_evidence_builds_condition_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_short_value_composite_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert not result.coverage_diagnostics_df.empty
    assert not result.valuation_axis_evidence_df.empty
    assert not result.short_search_condition_evidence_df.empty
    assert {
        "high_fwd_per_pbr_composite_score",
        "psr_percentile",
        "sector_strength_score",
        "sma5_above_count_5d",
        "atr20_to_atr60_overheat",
    }.issubset(result.observation_sample_df.columns)
    assert {
        "high_fwd_per_pbr_composite_80",
        "high_psr_80",
        "overvalued_or_high_psr",
    }.issubset(set(result.valuation_axis_evidence_df["condition_name"].astype(str)))
    assert {
        "overvalued_breakdown_core",
        "overvalued_breakdown_without_psr",
        "high_fpbr_breakdown",
        "high_psr_breakdown",
    }.issubset(
        set(result.short_search_condition_evidence_df["condition_name"].astype(str))
    )

    sample = result.observation_sample_df
    assert sample["high_fwd_per_pbr_composite_score"].dropna().between(0, 1).all()
    assert sample["psr_percentile"].dropna().between(0, 1).all()


def test_ranking_short_value_composite_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_short_value_composite_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Short Value Composite Evidence" in summary
    assert "Valuation Axis Evidence" in summary
    assert "Short Search Condition Evidence" in summary

    bundle = write_ranking_short_value_composite_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"horizons": (0,)}, "horizons must contain positive integers"),
        ({"min_observations": 0}, "min_observations must be positive"),
        (
            {"tail_return_threshold_pct": 0.0},
            "tail_return_threshold_pct must be positive",
        ),
    ],
)
def test_ranking_short_value_composite_evidence_rejects_invalid_params(
    tmp_path: Path,
    kwargs: dict[str, Any],
    message: str,
) -> None:
    db_path = _build_short_value_composite_db(tmp_path / "market.duckdb")

    with pytest.raises(ValueError, match=message):
        run_ranking_short_value_composite_evidence_research(db_path, **kwargs)


def _run_test_research(db_path: Path) -> RankingShortValueCompositeEvidenceResult:
    return run_ranking_short_value_composite_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-05-31",
        horizons=(20,),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_short_value_composite_db(db_path: Path) -> Path:
    _build_short_sector_db(db_path)
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE statements (
            code TEXT,
            disclosed_date TEXT,
            sales DOUBLE,
            type_of_current_period TEXT,
            type_of_document TEXT
        )
        """
    )
    code_rows = conn.execute("SELECT DISTINCT code FROM stock_data ORDER BY code").fetchall()
    statement_rows = []
    for index, (code,) in enumerate(code_rows):
        sales = 60_000_000.0 + index * 2_000_000.0
        if code.endswith("100") or code.endswith("101"):
            sales = 15_000_000.0
        statement_rows.append(
            (str(code), "2023-05-15", sales, "FY", "FinancialStatements")
        )
    conn.executemany("INSERT INTO statements VALUES (?, ?, ?, ?, ?)", statement_rows)
    conn.close()
    return db_path
