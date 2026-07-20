from __future__ import annotations

from pathlib import Path

import duckdb

from src.domains.analytics.ranking_sma5_deviation_evidence import (
    RankingSma5DeviationEvidenceResult,
    build_summary_markdown,
    run_ranking_sma5_deviation_evidence_research,
    write_ranking_sma5_deviation_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_sma5_count_long_evidence import (
    _build_sma5_count_long_db,
)


def test_sma5_deviation_evidence_builds_bucket_and_overlay_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_sma5_count_long_db(tmp_path / "market.duckdb")
    _add_statements_fixture(db_path)

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.horizons == (5, 20)
    assert not result.coverage_diagnostics_df.empty
    assert not result.sma5_deviation_bucket_evidence_df.empty
    assert not result.long_scaffold_sma5_deviation_evidence_df.empty
    assert not result.short_overlay_sma5_deviation_evidence_df.empty
    assert {
        "sma5",
        "sma5_deviation_pct",
        "sma5_deviation_bucket",
        "long_hybrid_leadership_score",
        "atr20_acceleration_ex_overheat_flag",
        "forward_close_excess_return_5d_pct",
        "forward_close_excess_return_20d_pct",
    }.issubset(result.observation_sample_df.columns)
    assert set(
        result.sma5_deviation_bucket_evidence_df["sma5_deviation_bucket"]
    ).issubset(
        {
            "below_sma5_le_neg2",
            "below_sma5_neg2_to_0",
            "above_sma5_0_to_2",
            "above_sma5_2_to_5",
            "above_sma5_gt_5",
        }
    )
    assert {
        "all_market",
        "deep_value",
        "neutral_long_hybrid_atr20_accel",
        "crowded_long_hybrid",
    }.intersection(
        set(result.long_scaffold_sma5_deviation_evidence_df["long_scaffold"])
    )
    assert {
        "all_high_liquidity",
        "high_psr",
        "sector_weak",
    }.intersection(
        set(result.short_overlay_sma5_deviation_evidence_df["short_overlay"])
    )


def test_sma5_deviation_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_sma5_count_long_db(tmp_path / "market.duckdb")
    _add_statements_fixture(db_path)
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking SMA5 Deviation Evidence" in summary
    assert "SMA5 Deviation Bucket Evidence" in summary
    assert "Long Scaffold x SMA5 Deviation Evidence" in summary

    bundle = write_ranking_sma5_deviation_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(db_path: Path) -> RankingSma5DeviationEvidenceResult:
    return run_ranking_sma5_deviation_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(5, 20),
        market_scopes=("prime",),
        liquidity_bands=("high", "mid", "low"),
        min_observations=1,
        observation_sample_limit=100,
    )


def _add_statements_fixture(db_path: Path) -> None:
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
    codes = [
        str(row[0])
        for row in conn.execute("SELECT DISTINCT code FROM stock_data_raw").fetchall()
    ]
    conn.executemany(
        "INSERT INTO statements VALUES (?, ?, ?, ?, ?)",
        [
            (code, "2023-12-31", 1000000000.0 + index * 10000000.0, "FY", "")
            for index, code in enumerate(codes)
        ],
    )
    conn.execute(
        "UPDATE stock_data SET open = 1, high = 1, low = 1, close = 1, volume = 0"
    )
    conn.close()
