from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.domains.analytics.ranking_sector_strength_evidence import (
    RankingSectorStrengthEvidenceResult,
    build_summary_markdown,
    run_ranking_sector_strength_evidence_research,
    write_ranking_sector_strength_evidence_bundle,
)


def test_ranking_sector_strength_evidence_builds_pit_sector_interactions(
    tmp_path: Path,
) -> None:
    db_path = _build_sector_strength_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert result.market_source == "stock_master_daily_exact_date"
    assert not result.sector_daily_state_df.empty
    assert not result.color_sector_interaction_df.empty
    assert not result.sector_excess_interaction_df.empty
    assert {
        "sector_33_name",
        "sector_strength_score",
        "sector_strength_bucket",
        "sector_consistency_bucket",
    }.issubset(result.sector_daily_state_df.columns)
    assert {"sector_strong", "sector_neutral", "sector_weak"}.issubset(
        set(result.sector_daily_state_df["sector_strength_bucket"].astype(str))
    )
    assert {"green", "blue"}.issubset(
        set(result.color_sector_interaction_df["ui_color"].astype(str))
    )
    assert {"crowded_rerating", "neutral_rerating"}.issubset(
        set(result.color_sector_interaction_df["liquidity_regime"].astype(str))
    )
    assert {
        "mean_forward_topix_excess_return_pct",
        "median_forward_topix_excess_return_pct",
        "severe_loss_rate_pct",
        "median_sector_strength_score",
        "value_condition",
        "value_confirmation_tier",
    }.issubset(result.color_sector_interaction_df.columns)
    assert {
        "mean_forward_sector_excess_return_pct",
        "median_forward_sector_excess_return_pct",
    }.issubset(result.sector_excess_interaction_df.columns)
    assert {
        "sector_33_name",
        "sector_strength_bucket",
        "ui_color",
        "value_condition",
        "value_confirmation_tier",
        "forward_close_return_20d_pct",
        "forward_sector_excess_return_20d_pct",
    }.issubset(result.observation_sample_df.columns)
    assert {
        "low_per20_fwdper_per_lte_0_8",
        "no_value_confirmation",
    }.issubset(set(result.observation_sample_df["value_condition"].astype(str)))
    assert {
        "strong_value_confirmation",
        "no_value_confirmation",
    }.issubset(set(result.observation_sample_df["value_confirmation_tier"].astype(str)))

    strong_green = result.color_sector_interaction_df[
        (result.color_sector_interaction_df["ui_color"].astype(str) == "green")
        & (
            result.color_sector_interaction_df["sector_strength_bucket"].astype(str)
            == "sector_strong"
        )
    ]
    assert not strong_green.empty
    assert strong_green["value_condition"].notna().all()


def test_sector_strength_uses_official_sector_index_price_action(tmp_path: Path) -> None:
    db_path = _build_sector_strength_db(tmp_path / "market.duckdb")

    result = run_ranking_sector_strength_evidence_research(
        db_path,
        start_date="2024-04-30",
        end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=20,
    )

    service_state = result.sector_daily_state_df[
        result.sector_daily_state_df["sector_33_code"].astype(str).eq("9050")
    ]

    assert len(service_state) == 1
    row = service_state.iloc[0]
    assert row["sector_33_name"] == "Service"
    assert row["sector_index_code"] == "0060"
    assert row["sector_index_20d_topix_excess_pct"] > 10.0
    assert row["sector_index_strength_score"] >= 0.8
    assert row["sector_constituent_strength_score"] < 0.4
    assert abs(
        row["sector_strength_score"]
        - (
            row["sector_index_strength_score"]
            + row["sector_constituent_strength_score"]
        )
        / 2.0
    ) < 1e-12
    assert row["sector_strength_bucket"] == "sector_neutral"


def test_ranking_sector_strength_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_sector_strength_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Sector Strength Evidence" in summary
    assert "Color x Sector Strength" in summary
    assert "Color x Sector Excess" in summary

    bundle = write_ranking_sector_strength_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def _run_test_research(db_path: Path) -> RankingSectorStrengthEvidenceResult:
    return run_ranking_sector_strength_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_sector_strength_db(db_path: Path) -> Path:
    dates = pd.bdate_range("2023-07-03", "2024-06-28").strftime("%Y-%m-%d").tolist()
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE stock_data (
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
        CREATE TABLE topix_data (
            date TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_master_daily (
            date TEXT,
            code TEXT,
            company_name TEXT,
            market_code TEXT,
            market_name TEXT,
            sector_17_code TEXT,
            sector_17_name TEXT,
            sector_33_code TEXT,
            sector_33_name TEXT,
            scale_category TEXT,
            listed_date TEXT,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE daily_valuation (
            code TEXT,
            date TEXT,
            price_basis_date TEXT,
            per DOUBLE,
            forward_per DOUBLE,
            pbr DOUBLE,
            p_op DOUBLE,
            forward_p_op DOUBLE,
            market_cap DOUBLE,
            free_float_market_cap DOUBLE,
            basis_version TEXT
        )
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

    sectors = [
        ("3600", "Strong Machinery", 0.18),
        ("6100", "Neutral Retail", 0.04),
        ("3200", "Weak Chemicals", -0.10),
        ("9050", "Service", -0.06),
        ("0050", "Fishery", -0.08),
        ("1050", "Mining", -0.09),
        ("2050", "Construction", -0.07),
        ("3050", "Foods", -0.06),
    ]
    stock_specs: list[tuple[str, str, str, str, float, float]] = []
    for sector_index, (sector_code, sector_name, sector_slope) in enumerate(sectors):
        for rank in range(24):
            code = f"{sector_index + 1}{rank + 100:03d}"
            stock_specs.append(
                (
                    code,
                    f"{sector_name} {rank}",
                    sector_code,
                    sector_name,
                    80.0 + sector_index * 30.0 + rank,
                    sector_slope + rank * 0.002,
                )
            )

    stock_rows: list[tuple[str, str, float, float, float, float, int]] = []
    master_rows: list[
        tuple[str, str, str, str, str, str, str, str, str, str, str, str]
    ] = []
    valuation_rows: list[
        tuple[str, str, str, float, float, float, float, float, float, float, str]
    ] = []
    for date_index, date in enumerate(dates):
        for spec_index, (code, name, sector_code, sector_name, base, slope) in enumerate(
            stock_specs
        ):
            close = base + date_index * slope
            volume = 10_000 + date_index * 20 + spec_index * 10
            if spec_index in {0, 1, 2}:
                volume *= 120
            stock_rows.append(
                (
                    code,
                    date,
                    close * 0.995,
                    close * 1.01,
                    close * 0.99,
                    close,
                    volume,
                )
            )
            master_rows.append(
                (
                    date,
                    code,
                    name,
                    "0111",
                    "Prime",
                    sector_code,
                    "Sector17",
                    sector_code,
                    sector_name,
                    "-",
                    "2000-01-01",
                    None,
                )
            )
            value_rank = spec_index % 24
            valuation_rows.append(
                (
                    code,
                    date,
                    date,
                    8.0 + value_rank * 0.5,
                    6.0 + value_rank * 0.4,
                    0.4 + value_rank * 0.06,
                    7.0 + value_rank * 0.4,
                    5.0 + value_rank * 0.4,
                    80_000_000.0 + spec_index * 5_000_000.0,
                    60_000_000.0 + spec_index * 4_000_000.0,
                    "unit",
                )
            )
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        master_rows,
    )
    conn.executemany(
        "INSERT INTO daily_valuation VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        valuation_rows,
    )
    topix_rows: list[tuple[str, float, float, float, float]] = []
    for date_index, date in enumerate(dates):
        close = 1000.0 + date_index * 0.06
        topix_rows.append((date, close * 0.998, close * 1.002, close * 0.996, close))
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)", topix_rows)
    index_master_rows = [
        ("004E", "東証業種別 Strong Machinery", None, "sector33", None, None, None),
        ("005A", "東証業種別 Neutral Retail", None, "sector33", None, None, None),
        ("0046", "東証業種別 Weak Chemicals", None, "sector33", None, None, None),
        ("0060", "東証業種別 Service", None, "sector33", None, None, None),
        ("0040", "東証業種別 Fishery", None, "sector33", None, None, None),
        ("0041", "東証業種別 Mining", None, "sector33", None, None, None),
        ("0042", "東証業種別 Construction", None, "sector33", None, None, None),
        ("0043", "東証業種別 Foods", None, "sector33", None, None, None),
    ]
    conn.executemany(
        "INSERT INTO index_master VALUES (?, ?, ?, ?, ?, ?, ?)",
        index_master_rows,
    )
    index_rows: list[tuple[str, str, float, float, float, float, int]] = []
    index_specs = [
        ("004E", 1000.0, 0.20),
        ("005A", 900.0, 0.05),
        ("0046", 800.0, -0.12),
        ("0060", 700.0, 24.00),
        ("0040", 700.0, -0.10),
        ("0041", 680.0, -0.11),
        ("0042", 660.0, -0.09),
        ("0043", 640.0, -0.08),
    ]
    for date_index, date in enumerate(dates):
        for code, base, slope in index_specs:
            close = base * (1.011**date_index) if code == "0060" else base + date_index * slope
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
    conn.close()
    return db_path
