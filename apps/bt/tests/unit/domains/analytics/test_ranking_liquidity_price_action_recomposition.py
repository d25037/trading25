from __future__ import annotations

from pathlib import Path

import duckdb

from src.domains.analytics.ranking_liquidity_price_action_recomposition import (
    RankingLiquidityPriceActionRecompositionResult,
    build_summary_markdown,
    run_ranking_liquidity_price_action_recomposition_research,
    write_ranking_liquidity_price_action_recomposition_bundle,
)
from tests.unit.domains.analytics.daily_ranking_market_v5_fixture import (
    refresh_daily_ranking_provider_window,
)
from tests.unit.domains.analytics.test_ranking_color_evidence import (
    _build_ranking_color_db,
)


def test_liquidity_price_action_recomposition_builds_mixed_buckets(
    tmp_path: Path,
) -> None:
    db_path = _build_recomposition_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert not result.coverage_diagnostics_df.empty
    assert not result.price_action_bucket_evidence_df.empty
    assert not result.short_overlay_evidence_df.empty
    assert {
        "price_action_bucket",
        "liquidity_band",
        "liquidity_regime",
        "liquidity_residual_z",
        "recent_return_20d_pct",
        "recent_return_60d_pct",
        "psr_percentile",
        "sector_strength_bucket",
    }.issubset(result.observation_sample_df.columns)
    assert {
        "dual_positive_crowded",
        "recent20_positive_60d_negative",
        "recent20_negative_60d_positive",
        "dual_negative_stress",
    }.issubset(
        set(result.price_action_bucket_evidence_df["price_action_bucket"].astype(str))
    )
    assert {
        "high_psr",
        "sector_weak",
        "high_psr_sector_weak",
    }.issubset(set(result.short_overlay_evidence_df["short_overlay"].astype(str)))


def test_liquidity_price_action_recomposition_can_compare_all_liquidity_bands(
    tmp_path: Path,
) -> None:
    db_path = _build_recomposition_db(tmp_path / "market.duckdb")

    result = run_ranking_liquidity_price_action_recomposition_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        liquidity_bands=("high", "mid", "low"),
        min_observations=1,
        observation_sample_limit=100,
    )

    assert result.liquidity_bands == ("high", "mid", "low")
    assert {
        "high_liquidity_z_ge_1",
        "mid_liquidity_z_minus1_to_1",
        "low_liquidity_z_lt_minus1",
    }.issubset(
        set(result.price_action_bucket_evidence_df["liquidity_band"].astype(str))
    )
    assert {
        "high_liquidity_z_ge_1",
        "mid_liquidity_z_minus1_to_1",
        "low_liquidity_z_lt_minus1",
    }.issubset(set(result.short_overlay_evidence_df["liquidity_band"].astype(str)))


def test_liquidity_price_action_recomposition_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_recomposition_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Liquidity Price Action Recomposition" in summary
    assert "Price Action Bucket Evidence" in summary
    assert "Short Overlay Evidence" in summary

    bundle = write_ranking_liquidity_price_action_recomposition_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(
    db_path: Path,
) -> RankingLiquidityPriceActionRecompositionResult:
    return run_ranking_liquidity_price_action_recomposition_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_recomposition_db(db_path: Path) -> Path:
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
    conn.executemany(
        "INSERT INTO index_master VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("004E", "Machinery", None, "sector33", None, None, None),
            ("0046", "Chemicals", None, "sector33", None, None, None),
            ("005A", "Retail", None, "sector33", None, None, None),
        ],
    )
    dates = [
        str(row[0])
        for row in conn.execute(
            "SELECT DISTINCT date FROM topix_data ORDER BY date"
        ).fetchall()
    ]
    sector_rows: list[tuple[str, str, float, float, float, float, str]] = []
    for date_index, date in enumerate(dates):
        for code, base, slope, sector_name in (
            ("004E", 1000.0, 0.8, "Machinery"),
            ("0046", 900.0, -0.4, "Chemicals"),
            ("005A", 800.0, 0.2, "Retail"),
        ):
            close = base + date_index * slope
            sector_rows.append(
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
        sector_rows,
    )
    conn.execute("ALTER TABLE statements ADD COLUMN sales DOUBLE")
    conn.execute("ALTER TABLE statements ADD COLUMN type_of_document TEXT")
    conn.executemany(
        "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                code,
                f"statement-{code}",
                "2023-05-15",
                "2023-05-15T15:00:00+09:00",
                "2023-03-31",
                "FY",
                sales,
                "FinancialStatements",
            )
            for code, sales in (
                ("1111", 300_000_000.0),
                ("2222", 100_000_000.0),
                ("3333", 100_000_000.0),
                ("4444", 120_000_000.0),
                ("5555", 150_000_000.0),
                ("6666", 80_000_000.0),
            )
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
        SET statement_count = 1
        WHERE code IN ('1111', '2222', '3333', '4444', '5555', '6666')
        """
    )
    conn.execute(
        """
        WITH numbered AS (
            SELECT
                code,
                date,
                row_number() OVER (PARTITION BY code ORDER BY date) - 1 AS rn
            FROM stock_data_raw
            WHERE code IN ('1111', '2222', '4444', '5555')
        ),
        shaped AS (
            SELECT
                code,
                date,
                CASE
                    WHEN code = '1111' THEN 100.0 + rn * 0.45
                    WHEN code = '2222' THEN 220.0 - rn * 0.35
                    WHEN code = '4444' THEN 250.0 - rn * 0.8
                        + CASE WHEN rn > 190 THEN (rn - 190) * 1.5 ELSE 0 END
                    WHEN code = '5555' THEN 60.0 + rn * 0.8
                        - CASE WHEN rn > 190 THEN (rn - 190) * 1.5 ELSE 0 END
                END AS close
            FROM numbered
        )
        UPDATE stock_data_raw AS sd
        SET
            close = shaped.close,
            open = shaped.close * 0.995,
            high = shaped.close * 1.01,
            low = shaped.close * 0.99,
            volume = 50000000 + CAST(strftime(sd.date::DATE, '%j') AS BIGINT),
            adjusted_close = shaped.close,
            adjusted_open = shaped.close * 0.995,
            adjusted_high = shaped.close * 1.01,
            adjusted_low = shaped.close * 0.99,
            adjusted_volume = 50000000
                + CAST(strftime(sd.date::DATE, '%j') AS BIGINT)
        FROM shaped
        WHERE sd.code = shaped.code
          AND sd.date = shaped.date
        """
    )
    conn.execute(
        "UPDATE stock_data_raw SET volume = 1, adjusted_volume = 1 "
        "WHERE code = '3333'"
    )
    conn.execute(
        """
        UPDATE stock_data AS consumer
        SET open = raw.adjusted_open, high = raw.adjusted_high,
            low = raw.adjusted_low, close = raw.adjusted_close,
            volume = raw.adjusted_volume
        FROM stock_data_raw AS raw
        WHERE consumer.code = raw.code
          AND CAST(consumer.date AS DATE) = raw.date
        """
    )
    for (code,) in conn.execute(
        "SELECT DISTINCT code FROM stock_data_raw ORDER BY code"
    ).fetchall():
        refresh_daily_ranking_provider_window(conn, code=str(code))
    conn.close()
    return db_path
