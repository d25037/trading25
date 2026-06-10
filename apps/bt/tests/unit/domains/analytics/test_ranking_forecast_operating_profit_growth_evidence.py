from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.domains.analytics.ranking_forecast_operating_profit_growth_evidence import (
    RankingForecastOperatingProfitGrowthEvidenceResult,
    build_summary_markdown,
    run_ranking_forecast_operating_profit_growth_evidence_research,
    write_ranking_forecast_operating_profit_growth_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_color_evidence import (
    _build_ranking_color_db,
)


def test_ranking_forecast_op_growth_evidence_builds_tables(tmp_path: Path) -> None:
    db_path = _build_forecast_op_growth_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert not result.coverage_diagnostics_df.empty
    assert not result.growth_bucket_evidence_df.empty
    assert not result.valuation_growth_ratio_evidence_df.empty
    assert not result.decision_scope_growth_evidence_df.empty
    assert not result.long_deep_dive_growth_evidence_df.empty
    assert not result.short_deep_dive_growth_evidence_df.empty
    assert {
        "forecast_operating_profit_growth_ratio",
        "per_to_fop_growth_ratio",
        "forward_per_to_fop_growth_ratio",
        "valuation_signal",
        "strong_value_confirmation",
        "overvalued_warning",
        "no_value_confirmation",
    }.issubset(result.observation_sample_df.columns)
    assert {
        "contraction_lt_1_0",
        "high_growth_1_5_to_2_0",
        "exceptional_growth_ge_2_0",
    }.issubset(set(result.growth_bucket_evidence_df["growth_bucket"].astype(str)))
    assert {
        "per_to_fop_growth_ratio",
        "forward_per_to_fop_growth_ratio",
    }.issubset(set(result.valuation_growth_ratio_evidence_df["ratio_feature"].astype(str)))
    assert {
        "high_per20",
        "low_forward_per20",
        "rally_overvalued",
    }.issubset(set(result.decision_scope_growth_evidence_df["decision_scope"].astype(str)))
    assert {
        "long_hybrid_leadership_strong_atr20_accel",
        "current_sector_strong",
        "deep_value",
    }.intersection(set(result.long_deep_dive_growth_evidence_df["deep_scope"].astype(str)))
    assert {
        "sector_weak",
        "overvalued",
        "crowded_no_value",
        "stale_overvalued",
    }.issubset(set(result.short_deep_dive_growth_evidence_df["deep_scope"].astype(str)))
    assert {
        "high_growth_ge_1_5",
        "low_or_missing_growth",
    }.issubset(
        set(result.decision_scope_growth_evidence_df["growth_condition"].astype(str))
    )

    sample = result.observation_sample_df
    high_growth = sample.loc[sample["code"] == "1111"].iloc[0]
    high_growth_from_forward_p_op = sample.loc[sample["code"] == "4444"].iloc[0]
    non_positive_forecast = sample.loc[sample["code"] == "6666"].iloc[0]
    assert high_growth["forecast_operating_profit_growth_ratio"] == pytest.approx(1.8)
    assert high_growth_from_forward_p_op[
        "forecast_operating_profit_growth_ratio"
    ] == pytest.approx(
        1.5,
    )
    assert high_growth["p_op"] == pytest.approx(18.0)
    assert high_growth["forward_p_op"] == pytest.approx(10.0)
    assert non_positive_forecast["forecast_operating_profit_growth_ratio"] != (
        non_positive_forecast["forecast_operating_profit_growth_ratio"]
    )


def test_ranking_forecast_op_growth_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_forecast_op_growth_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Forecast OP Growth Bucket Evidence" in summary
    assert "PER/Fwd PER to Forecast OP Growth Ratio Evidence" in summary
    assert "Daily Ranking Decision Scope x Forecast OP Growth Evidence" in summary
    assert "Explicit Long Deep Dive" in summary
    assert "Explicit Short Deep Dive" in summary

    bundle = write_ranking_forecast_operating_profit_growth_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(
    db_path: Path,
) -> RankingForecastOperatingProfitGrowthEvidenceResult:
    return run_ranking_forecast_operating_profit_growth_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_forecast_op_growth_db(db_path: Path) -> Path:
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
        CREATE TABLE indices_data (
            code TEXT,
            date TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT
        )
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
        for row in conn.execute("SELECT DISTINCT date FROM topix_data ORDER BY date").fetchall()
    ]
    index_rows: list[tuple[str, str, float, float, float, float, int]] = []
    for date_index, date in enumerate(dates):
        for code, base, slope in (
            ("004E", 1000.0, 0.8),
            ("0046", 900.0, -0.4),
            ("005A", 800.0, 0.2),
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
                    0,
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
    conn.execute(
        """
        UPDATE daily_valuation
        SET p_op = 18.0, forward_p_op = 10.0
        WHERE code = '1111'
        """
    )
    conn.execute(
        """
        UPDATE daily_valuation
        SET p_op = 8.0, forward_p_op = 10.0
        WHERE code = '2222'
        """
    )
    conn.execute(
        """
        UPDATE daily_valuation
        SET p_op = 22.0, forward_p_op = 10.0
        WHERE code = '3333'
        """
    )
    conn.execute(
        """
        UPDATE daily_valuation
        SET p_op = 15.0, forward_p_op = 10.0
        WHERE code = '4444'
        """
    )
    conn.execute(
        """
        UPDATE daily_valuation
        SET p_op = 14.0, forward_p_op = NULL
        WHERE code = '5555'
        """
    )
    conn.execute(
        """
        UPDATE daily_valuation
        SET p_op = 20.0, forward_p_op = 0.0
        WHERE code = '6666'
        """
    )
    conn.close()
    return db_path
