from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.ranking_short_sector_strength_evidence import (
    RankingShortSectorStrengthEvidenceResult,
    build_summary_markdown,
    run_ranking_short_sector_strength_evidence_research,
    write_ranking_short_sector_strength_evidence_bundle,
)


def test_ranking_short_sector_strength_evidence_builds_interactions(
    tmp_path: Path,
) -> None:
    db_path = _build_short_sector_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert not result.coverage_diagnostics_df.empty
    assert not result.short_candidate_sector_interaction_df.empty
    assert not result.short_value_sector_interaction_df.empty
    assert not result.stale_rally_sector_interaction_df.empty
    assert not result.technical_sector_short_interaction_df.empty
    assert not result.priority_short_sector_readout_df.empty
    assert {
        "candidate_bucket",
        "sector_strength_bucket",
        "median_sector_strength_score",
        "mean_forward_raw_return_pct",
        "mean_topix_return_pct",
        "mean_forward_excess_return_pct",
        "negative_excess_return_rate_pct",
        "downside_excess_tail_rate_pct",
        "upside_excess_tail_rate_pct",
        "strong_value_confirmation_rate_pct",
        "no_value_confirmation_rate_pct",
    }.issubset(result.short_candidate_sector_interaction_df.columns)
    assert {"sector_weak", "sector_strong"}.issubset(
        set(result.short_candidate_sector_interaction_df["sector_strength_bucket"].astype(str))
    )
    assert {
        "valuation_state",
        "liquidity_regime",
        "sector_strength_bucket",
    }.issubset(result.short_value_sector_interaction_df.columns)
    assert {
        "recent_20d_and_60d_positive",
        "all_stale_high_valuation",
    }.issubset(set(result.stale_rally_sector_interaction_df["trend_split"].astype(str)))
    assert {
        "stale_high_valuation_sector_weak",
        "distribution_stress_high_valuation_sector_weak",
        "strong_low_value_sector_strong_short_prohibit",
    }.issubset(set(result.priority_short_sector_readout_df["priority_condition"].astype(str)))
    assert {
        "sector_33_name",
        "sector_strength_score",
        "sector_strength_bucket",
        "forward_close_excess_return_20d_pct",
    }.issubset(result.observation_sample_df.columns)


def test_ranking_short_sector_strength_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_short_sector_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Short Sector Strength Evidence" in summary
    assert "Short Candidate x Sector Strength" in summary
    assert "Priority Short Sector Readout" in summary

    bundle = write_ranking_short_sector_strength_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"horizons": (0,)}, "horizons must be positive"),
        ({"min_observations": 0}, "min_observations must be positive"),
        (
            {"tail_return_threshold_pct": 0.0},
            "tail_return_threshold_pct must be negative",
        ),
    ],
)
def test_ranking_short_sector_strength_evidence_rejects_invalid_params(
    tmp_path: Path,
    kwargs: dict[str, Any],
    message: str,
) -> None:
    db_path = _build_short_sector_db(tmp_path / "market.duckdb")

    with pytest.raises(ValueError, match=message):
        run_ranking_short_sector_strength_evidence_research(db_path, **kwargs)


def _run_test_research(db_path: Path) -> RankingShortSectorStrengthEvidenceResult:
    return run_ranking_short_sector_strength_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-05-31",
        horizons=(20,),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_short_sector_db(db_path: Path) -> Path:
    dates = pd.bdate_range("2023-07-03", "2024-07-31").strftime("%Y-%m-%d").tolist()
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
            market_cap DOUBLE,
            free_float_market_cap DOUBLE,
            p_op DOUBLE,
            forward_p_op DOUBLE,
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
        ("3600", "Strong Machinery", 0.16),
        ("6100", "Neutral Retail", 0.03),
        ("3200", "Weak Chemicals", -0.08),
    ]
    stock_specs: list[tuple[str, str, str, str, float, float, int, float, float, float]] = []
    for sector_index, (sector_code, sector_name, sector_slope) in enumerate(sectors):
        for rank in range(25):
            code = f"{sector_index + 1}{rank + 100:03d}"
            base = 80.0 + sector_index * 30.0 + rank
            slope = sector_slope + rank * 0.002
            volume = 1_000_000 + rank * 30_000
            per = 8.0 + rank * 0.5
            forward_per = 7.0 + rank * 0.4
            pbr = 0.6 + rank * 0.04
            if rank == 0:
                volume = 45_000_000
                per = 55.0
                forward_per = 50.0
                pbr = 4.5
            elif rank == 1:
                volume = 35_000_000
                per = 45.0
                forward_per = 42.0
                pbr = 3.8
            elif rank == 2:
                volume = 80_000
                per = 50.0
                forward_per = 46.0
                pbr = 4.0
            elif rank == 3:
                volume = 15_000_000
                per = 5.0
                forward_per = 3.5
                pbr = 0.45
            stock_specs.append(
                (
                    code,
                    f"{sector_name} {rank}",
                    sector_code,
                    sector_name,
                    base,
                    slope,
                    volume,
                    per,
                    forward_per,
                    pbr,
                )
            )

    stock_rows: list[tuple[str, str, float, float, float, float, int]] = []
    master_rows: list[
        tuple[str, str, str, str, str, str, str, str, str, str, str, str | None]
    ] = []
    valuation_rows: list[
        tuple[
            str,
            str,
            str,
            float,
            float,
            float,
            float,
            float,
            float,
            float,
            str,
        ]
    ] = []
    for date_index, date in enumerate(dates):
        topix_close = 1000.0 + date_index * 0.05
        conn.execute(
            "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)",
            [
                date,
                topix_close * 0.998,
                topix_close * 1.002,
                topix_close * 0.996,
                topix_close,
            ],
        )
        for spec_index, (
            code,
            name,
            sector_code,
            sector_name,
            base,
            slope,
            base_volume,
            per,
            forward_per,
            pbr,
        ) in enumerate(stock_specs):
            close = base + date_index * slope
            open_price = close * 0.998
            intraday_range = close * 0.010 * (1.0 + max(0, date_index - 100) / 100.0)
            stock_rows.append(
                (
                    code,
                    date,
                    open_price,
                    max(open_price, close) + intraday_range,
                    min(open_price, close) - intraday_range,
                    close,
                    int(base_volume + date_index * 100 + spec_index * 20),
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
                    "TOPIX Small 1",
                    "2000-01-01",
                    None,
                )
            )
            market_cap = close * 10_000_000
            valuation_rows.append(
                (
                    code,
                    date,
                    date,
                    per,
                    forward_per,
                    pbr,
                    market_cap,
                    market_cap * 0.65,
                    per * 1.1,
                    forward_per * 1.1,
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
    conn.executemany(
        "INSERT INTO index_master VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("004E", "東証業種別 Strong Machinery", None, "sector33", None, None, None),
            ("005A", "東証業種別 Neutral Retail", None, "sector33", None, None, None),
            ("0046", "東証業種別 Weak Chemicals", None, "sector33", None, None, None),
        ],
    )
    index_rows: list[tuple[str, str, float, float, float, float, int]] = []
    for date_index, date in enumerate(dates):
        for code, base, slope in (
            ("004E", 1000.0, 0.22),
            ("005A", 900.0, 0.03),
            ("0046", 800.0, -0.12),
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
    conn.close()
    return db_path
