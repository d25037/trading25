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
    _extend_long_history(conn)
    conn.close()
    return db_path


def _extend_long_history(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        INSERT INTO stock_data_raw
        WITH dates AS (
            SELECT CAST(day AS DATE) AS date
            FROM generate_series(
                DATE '2022-01-03', DATE '2023-06-30', INTERVAL 1 DAY
            ) calendar(day)
            WHERE dayofweek(day) BETWEEN 1 AND 5
        ),
        seed AS (
            SELECT * EXCLUDE (row_number)
            FROM (
                SELECT *, row_number() OVER (PARTITION BY code ORDER BY date) AS row_number
                FROM stock_data_raw
            )
            WHERE row_number = 1
        )
        SELECT seed.code, dates.date, seed.open, seed.high, seed.low, seed.close,
               seed.volume, seed.adjustment_factor
        FROM seed CROSS JOIN dates
        """
    )
    conn.execute(
        """
        INSERT INTO daily_valuation
        WITH dates AS (
            SELECT CAST(day AS DATE) AS date
            FROM generate_series(
                DATE '2022-01-03', DATE '2023-06-30', INTERVAL 1 DAY
            ) calendar(day)
            WHERE dayofweek(day) BETWEEN 1 AND 5
        ),
        seed AS (
            SELECT * EXCLUDE (row_number)
            FROM (
                SELECT *, row_number() OVER (PARTITION BY code ORDER BY date) AS row_number
                FROM daily_valuation
            )
            WHERE row_number = 1
        )
        SELECT seed.code, CAST(dates.date AS VARCHAR), CAST(dates.date AS VARCHAR),
               seed.per, seed.forward_per, seed.pbr, seed.p_op, seed.forward_p_op,
               seed.market_cap, seed.free_float_market_cap, seed.basis_version,
               seed.psr, seed.forward_psr
        FROM seed CROSS JOIN dates
        """
    )
    conn.execute(
        """
        INSERT INTO topix_data
        WITH dates AS (
            SELECT CAST(day AS DATE) AS date
            FROM generate_series(
                DATE '2022-01-03', DATE '2023-06-30', INTERVAL 1 DAY
            ) calendar(day)
            WHERE dayofweek(day) BETWEEN 1 AND 5
        ),
        seed AS (
            SELECT * FROM topix_data ORDER BY CAST(date AS DATE) LIMIT 1
        )
        SELECT dates.date, seed.open, seed.high, seed.low, seed.close
        FROM seed CROSS JOIN dates
        """
    )
    conn.execute(
        """
        INSERT INTO indices_data
        WITH dates AS (
            SELECT CAST(day AS DATE) AS date
            FROM generate_series(
                DATE '2022-01-03', DATE '2023-06-30', INTERVAL 1 DAY
            ) calendar(day)
            WHERE dayofweek(day) BETWEEN 1 AND 5
        ),
        seed AS (
            SELECT * EXCLUDE (row_number)
            FROM (
                SELECT *, row_number() OVER (PARTITION BY code ORDER BY date) AS row_number
                FROM indices_data
            )
            WHERE row_number = 1
        )
        SELECT seed.code, dates.date, seed.open, seed.high, seed.low, seed.close,
               seed.sector_name
        FROM seed CROSS JOIN dates
        """
    )
    conn.execute(
        """
        UPDATE daily_valuation
        SET basis_version = 'event-pit-v1:' || code || ':2022-01-03'
        """
    )
    conn.execute(
        """
        UPDATE stock_adjustment_basis_segments
        SET basis_id = 'event-pit-v1:' || code || ':2022-01-03',
            source_date_from = DATE '2022-01-03'
        """
    )
    conn.execute(
        """
        UPDATE stock_adjustment_bases
        SET basis_id = 'event-pit-v1:' || code || ':2022-01-03',
            valid_from = DATE '2022-01-03',
            adjustment_through_date = DATE '2022-01-03'
        """
    )
