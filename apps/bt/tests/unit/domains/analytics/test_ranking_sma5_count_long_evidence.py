from __future__ import annotations

from pathlib import Path

import duckdb

from src.domains.analytics.ranking_sma5_count_long_evidence import (
    RankingSma5CountLongEvidenceResult,
    build_summary_markdown,
    run_ranking_sma5_count_long_evidence_research,
    write_ranking_sma5_count_long_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_color_evidence import (
    _build_ranking_color_db,
)


def test_sma5_count_long_evidence_builds_long_scaffold_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_sma5_count_long_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.horizons == (5, 20)
    assert not result.coverage_diagnostics_df.empty
    assert not result.long_scaffold_evidence_df.empty
    assert not result.sma5_count_group_evidence_df.empty
    assert not result.long_scaffold_sma5_count_group_evidence_df.empty
    assert not result.same_day_sma5_group_spread_df.empty
    assert not result.long_scaffold_same_day_sma5_group_spread_df.empty
    assert {
        "sma5_above_count_5d",
        "sma5_count_group",
        "long_hybrid_leadership_score",
        "atr20_acceleration_ex_overheat_flag",
        "forward_close_excess_return_5d_pct",
        "forward_close_excess_return_20d_pct",
    }.issubset(result.observation_sample_df.columns)
    assert set(result.observation_sample_df["sma5_above_count_5d"].dropna()).issubset(
        {0, 1, 2, 3, 4, 5}
    )
    assert set(result.sma5_count_group_evidence_df["sma5_count_group"].astype(str)).issubset(
        {
            "sma5_above_count_0_1",
            "sma5_above_count_2_3",
            "sma5_above_count_4_5",
        }
    )
    assert {
        "all_market",
        "deep_value",
        "neutral_long_hybrid_atr20_accel",
        "crowded_long_hybrid",
    }.intersection(set(result.long_scaffold_evidence_df["long_scaffold"].astype(str)))
    assert {
        "base_sma5_count_group",
        "comparison_sma5_count_group",
        "matched_date_count",
        "median_daily_median_excess_spread_pct",
        "comparison_outperform_date_rate_pct",
    }.issubset(result.same_day_sma5_group_spread_df.columns)
    assert (
        result.same_day_sma5_group_spread_df["matched_date_count"].astype(int) > 0
    ).all()


def test_sma5_count_long_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_sma5_count_long_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking SMA5 Count Long Evidence" in summary
    assert "Long Scaffold Evidence" in summary
    assert "SMA5 Count Group Evidence" in summary
    assert "Long Scaffold x SMA5 Count Group Evidence" in summary
    assert "Same-Day SMA5 Count Group Spread" in summary
    assert "Long Scaffold Same-Day SMA5 Count Group Spread" in summary

    bundle = write_ranking_sma5_count_long_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(db_path: Path) -> RankingSma5CountLongEvidenceResult:
    return run_ranking_sma5_count_long_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(5, 20),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_sma5_count_long_db(db_path: Path) -> Path:
    _build_ranking_color_db(db_path)
    conn = duckdb.connect(str(db_path))
    conn.execute("ALTER TABLE stock_master_daily ADD COLUMN sector_33_code TEXT")
    conn.execute("ALTER TABLE stock_master_daily ADD COLUMN sector_33_name TEXT")
    conn.execute(
        """
        UPDATE stock_master_daily
        SET
            sector_33_code = CASE
                WHEN code IN ('1111', '4444') THEN '3600'
                WHEN code IN ('2222', '5555') THEN '3200'
                ELSE '6100'
            END,
            sector_33_name = CASE
                WHEN code IN ('1111', '4444') THEN 'Machinery'
                WHEN code IN ('2222', '5555') THEN 'Chemicals'
                ELSE 'Retail'
            END
        """
    )
    conn.execute(
        """
        CREATE TABLE index_master (
            code TEXT,
            name TEXT,
            name_english TEXT,
            category TEXT,
            data_start_date TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    dates = [
        str(row[0])
        for row in conn.execute(
            "SELECT DISTINCT date FROM topix_data ORDER BY date"
        ).fetchall()
    ]
    index_rows: list[tuple[str, str, float, float, float, float, str]] = []
    for date_index, date in enumerate(dates):
        for code, sector_name, base, slope in (
            ("004E", "Machinery", 1000.0, 0.8),
            ("0046", "Chemicals", 900.0, -0.4),
            ("005A", "Retail", 800.0, 0.2),
        ):
            close = base + date_index * slope
            index_rows.append(
                (
                    code,
                    date,
                    close * 0.998,
                    close * 1.002,
                    close * 0.996,
                    close,
                    sector_name,
                )
            )
    conn.executemany(
        "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?)",
        index_rows,
    )
    conn.executemany(
        "INSERT INTO index_master VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("004E", "Machinery", None, "sector33", None, None, None),
            ("0046", "Chemicals", None, "sector33", None, None, None),
            ("005A", "Retail", None, "sector33", None, None, None),
        ],
    )
    conn.close()
    return db_path
