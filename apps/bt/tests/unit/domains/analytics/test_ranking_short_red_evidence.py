from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.ranking_short_red_evidence import (
    RankingShortRedEvidenceResult,
    build_summary_markdown,
    run_ranking_short_red_evidence_research,
    write_ranking_short_red_evidence_bundle,
)


def test_ranking_short_red_evidence_emits_independent_tables(tmp_path: Path) -> None:
    db_path = _build_short_red_db(tmp_path / "market.duckdb")

    result = _run_test_research(db_path)

    assert result.observation_count > 0
    assert not result.coverage_diagnostics_df.empty
    assert not result.short_red_candidate_df.empty
    assert not result.regime_valuation_interaction_df.empty
    assert not result.technical_atr_short_interaction_df.empty
    assert not result.stale_liquidity_short_diagnostics_df.empty
    assert not result.stale_high_valuation_trend_split_df.empty
    assert not result.live_ranking_replay_df.empty
    assert {
        "candidate_bucket",
        "horizon",
        "mean_forward_raw_return_pct",
        "median_forward_raw_return_pct",
        "mean_topix_return_pct",
        "median_forward_excess_return_pct",
        "negative_excess_return_rate_pct",
        "downside_excess_tail_rate_pct",
        "upside_excess_tail_rate_pct",
        "median_pbr_percentile",
        "median_atr20_to_atr60",
    }.issubset(result.short_red_candidate_df.columns)
    assert {
        "crowded_high_valuation",
        "crowded_no_value",
        "distribution_stress_weak_trend",
        "stale_high_valuation_weak_trend",
    }.issubset(set(result.short_red_candidate_df["candidate_bucket"].astype(str)))
    assert {
        "technical_state",
        "atr20_acceleration_rate_pct",
        "atr20_to_atr60_overheat_rate_pct",
    }.issubset(result.technical_atr_short_interaction_df.columns)
    assert {
        "stale_condition",
        "median_liquidity_residual_z",
    }.issubset(result.stale_liquidity_short_diagnostics_df.columns)
    assert {
        "trend_split",
        "negative_excess_return_rate_pct",
        "upside_excess_tail_rate_pct",
    }.issubset(result.stale_high_valuation_trend_split_df.columns)
    assert {
        "date",
        "code",
        "candidate_bucket",
        "liquidity_regime",
        "pbr_percentile",
        "forward_close_return_20d_pct",
        "topix_close_return_20d_pct",
        "forward_close_excess_return_20d_pct",
    }.issubset(result.live_ranking_replay_df.columns)


def test_ranking_short_red_evidence_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_short_red_db(tmp_path / "market.duckdb")
    result = _run_test_research(db_path)

    summary = build_summary_markdown(result)
    assert "Ranking Short Red Evidence" in summary
    assert "Short Red Candidates" in summary
    assert "Regime x Valuation Interaction" in summary
    assert "Technical ATR Short Interaction" in summary
    assert "Stale Liquidity Short Diagnostics" in summary
    assert "Stale High Valuation Trend Split" in summary
    assert "Live Ranking Replay" in summary

    bundle = write_ranking_short_red_evidence_bundle(
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
def test_ranking_short_red_evidence_rejects_invalid_params(
    tmp_path: Path,
    kwargs: dict[str, Any],
    message: str,
) -> None:
    db_path = _build_short_red_db(tmp_path / "market.duckdb")

    with pytest.raises(ValueError, match=message):
        run_ranking_short_red_evidence_research(db_path, **kwargs)


def test_ranking_short_red_evidence_requires_existing_db(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        run_ranking_short_red_evidence_research(tmp_path / "missing.duckdb")


def _run_test_research(db_path: Path) -> RankingShortRedEvidenceResult:
    return run_ranking_short_red_evidence_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-05-31",
        horizons=(5, 20),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=100,
    )


def _build_short_red_db(db_path: Path) -> Path:
    dates = pd.bdate_range("2023-08-01", "2024-07-31").strftime("%Y-%m-%d").tolist()
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
            scale_category TEXT
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

    code_profiles = [
        ("1001", "Crowded Expensive", 120.0, 0.0015, 40_000_000, 45.0, 42.0, 4.2),
        ("1002", "Crowded Missing", 110.0, 0.0012, 35_000_000, None, None, 3.8),
        ("1003", "Distribution Weak", 130.0, -0.0012, 30_000_000, 28.0, 27.0, 2.6),
        ("1004", "Stale Expensive", 100.0, -0.0006, 100_000, 36.0, 35.0, 3.2),
        ("1005", "Neutral Cheap", 95.0, 0.0008, 8_000_000, 5.0, 3.5, 0.6),
    ]
    code_profiles.extend(
        (
            str(2000 + code_idx),
            f"Filler {code_idx}",
            70.0 + code_idx,
            -0.0003 + (code_idx % 9) * 0.00008,
            1_000_000 + code_idx * 50_000,
            8.0 + code_idx * 0.35,
            7.5 + code_idx * 0.35,
            0.8 + code_idx * 0.03,
        )
        for code_idx in range(55)
    )

    stock_rows: list[tuple[str, str, float, float, float, float, int]] = []
    master_rows: list[tuple[str, str, str, str, str, str | None]] = []
    valuation_rows: list[
        tuple[
            str,
            str,
            str,
            float | None,
            float | None,
            float,
            float,
            float,
            float | None,
            float | None,
            str,
        ]
    ] = []
    for index, date in enumerate(dates):
        topix_close = 1900.0 + index * 0.35
        conn.execute(
            "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)",
            [
                date,
                topix_close * 0.999,
                topix_close * 1.003,
                topix_close * 0.997,
                topix_close,
            ],
        )
        for code, name, base, drift, base_volume, per, forward_per, pbr in code_profiles:
            volatility_wave = 1.0 + max(0, index - 90) / 120.0
            close = base * (1.0 + drift * index) * (1.0 + 0.01 * ((index % 13) - 6) / 6)
            open_price = close * 0.998
            intraday_range = close * 0.010 * volatility_wave
            volume = int(base_volume + index * 100)
            stock_rows.append(
                (
                    code,
                    date,
                    open_price,
                    max(open_price, close) + intraday_range,
                    min(open_price, close) - intraday_range,
                    close,
                    volume,
                )
            )
            master_rows.append((date, code, name, "0111", "Prime", "TOPIX Small 1"))
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
                    None if per is None else per * 1.1,
                    None if forward_per is None else forward_per * 1.1,
                    "unit",
                )
            )

    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?)", master_rows
    )
    conn.executemany(
        "INSERT INTO daily_valuation VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        valuation_rows,
    )
    conn.close()
    return db_path
