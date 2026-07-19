from __future__ import annotations

from pathlib import Path

import duckdb

from src.domains.analytics.ranking_core_sector_relative_value_evidence import (
    RankingCoreSectorRelativeValueEvidenceResult,
    _create_core_sector_relative_tables,
    build_summary_markdown,
    run_ranking_core_sector_relative_value_evidence_research,
    write_ranking_core_sector_relative_value_evidence_bundle,
)

from test_ranking_sector_strength_evidence import _build_sector_strength_db

from daily_ranking_market_v4_fixture import (
    upgrade_daily_ranking_fixture_to_market_v4,
)


def test_ranking_core_sector_relative_value_evidence_classifies_core_rules() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TEMP TABLE ranking_sector_master (
            market_scope TEXT,
            date TEXT,
            code TEXT,
            sector_33_code TEXT,
            sector_33_name TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE ranking_color_ranked (
            market_scope TEXT,
            date TEXT,
            code TEXT,
            pbr DOUBLE,
            forward_per DOUBLE,
            pbr_percentile DOUBLE,
            forward_per_percentile DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE ranking_sector_signal_panel (
            market_scope TEXT,
            date TEXT,
            code TEXT,
            company_name TEXT,
            sector_33_name TEXT,
            sector_strength_bucket TEXT,
            sector_strength_score DOUBLE,
            sector_20d_topix_excess_pct DOUBLE,
            sector_60d_topix_excess_pct DOUBLE,
            liquidity_scope TEXT,
            ui_color TEXT,
            value_condition TEXT,
            pbr_percentile DOUBLE,
            forward_per_percentile DOUBLE
        )
        """
    )
    rows = [
        ("prime", "2024-04-01", "1001", "01", "Core Sector"),
        ("prime", "2024-04-01", "1002", "01", "Core Sector"),
        ("prime", "2024-04-01", "1003", "01", "Core Sector"),
        ("prime", "2024-04-01", "1004", "01", "Core Sector"),
        ("prime", "2024-04-01", "1005", "01", "Core Sector"),
    ]
    conn.executemany("INSERT INTO ranking_sector_master VALUES (?, ?, ?, ?, ?)", rows)
    conn.executemany(
        "INSERT INTO ranking_color_ranked VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("prime", "2024-04-01", "1001", 0.5, 6.0, 0.05, 0.05),
            ("prime", "2024-04-01", "1002", 0.6, 7.0, 0.10, 0.10),
            ("prime", "2024-04-01", "1003", 1.5, 20.0, 0.70, 0.70),
            ("prime", "2024-04-01", "1004", 1.6, 22.0, 0.80, 0.80),
            ("prime", "2024-04-01", "1005", 1.7, 24.0, 0.90, 0.90),
        ],
    )
    conn.executemany(
        "INSERT INTO ranking_sector_signal_panel VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "prime",
                "2024-04-01",
                "1001",
                "Raw and sector relative",
                "Core Sector",
                "sector_strong",
                0.9,
                3.0,
                4.0,
                "neutral_rerating",
                "blue",
                "low_pbr20_low_fwd_per20",
                0.05,
                0.05,
            ),
            (
                "prime",
                "2024-04-01",
                "1003",
                "Sector relative only",
                "Core Sector",
                "sector_strong",
                0.9,
                3.0,
                4.0,
                "neutral_rerating",
                "blue",
                "no_value_confirmation",
                0.70,
                0.70,
            ),
        ],
    )

    _create_core_sector_relative_tables(conn, min_sector_observations=2)

    rules = {
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT core_rule FROM ranking_core_rule_observations"
        ).fetchall()
    }
    assert {
        "raw_core",
        "sector_relative_core",
        "raw_and_sector_relative_core",
        "hybrid_core",
    }.issubset(rules)


def test_ranking_core_sector_relative_value_evidence_builds_daily_matrix(
    tmp_path: Path,
) -> None:
    db_path = _build_core_value_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.market_source == "stock_master_daily_exact_date"
    assert not result.raw_sector_relative_matrix_df.empty
    assert "core_rule" in result.core_rule_summary_df.columns
    assert "sector_33_name" in result.sector_concentration_df.columns
    assert {
        "raw_pbr_bucket",
        "raw_forward_per_bucket",
        "sector_relative_pbr_bucket",
        "sector_relative_forward_per_bucket",
        "median_forward_topix_excess_return_pct",
    }.issubset(result.raw_sector_relative_matrix_df.columns)
    assert {
        "sector_pbr_percentile",
        "sector_forward_per_percentile",
        "hybrid_value_percentile",
        "core_rule",
        "forward_sector_excess_return_20d_pct",
    }.issubset(result.observation_sample_df.columns)


def test_ranking_core_sector_relative_value_evidence_writes_bundle(
    tmp_path: Path,
) -> None:
    db_path = _build_core_value_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Core Sector-Relative Value Evidence" in summary
    assert "Core Rule Summary" in summary
    assert "Raw x Sector-Relative Matrix" in summary

    bundle = write_ranking_core_sector_relative_value_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(db_path: Path) -> RankingCoreSectorRelativeValueEvidenceResult:
    return run_ranking_core_sector_relative_value_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_core_value_db(db_path: Path) -> Path:
    db_path = _build_sector_strength_db(db_path)
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        UPDATE daily_valuation
        SET per = 100.0
        WHERE code IN ('1103', '1104', '1105', '1106', '1107', '1108', '1109', '1110')
        """
    )
    conn.execute(
        """
        INSERT INTO indices_data
        SELECT
            'N225_UNDERPX' AS code,
            date,
            close * 14.0 * 0.998 AS open,
            close * 14.0 * 1.002 AS high,
            close * 14.0 * 0.996 AS low,
            close * 14.0 AS close,
            0 AS volume
        FROM topix_data
        """
    )
    upgrade_daily_ranking_fixture_to_market_v4(conn)
    conn.close()
    return db_path
