from __future__ import annotations

from pathlib import Path

import duckdb

from src.domains.analytics.ranking_n225_neutral_rerating_benchmark import (
    run_ranking_n225_neutral_rerating_benchmark_research,
    write_ranking_n225_neutral_rerating_benchmark_bundle,
)

from test_ranking_sector_strength_evidence import _build_sector_strength_db


def test_n225_neutral_rerating_benchmark_builds_signal_summary(tmp_path: Path) -> None:
    db_path = _build_n225_benchmark_db(tmp_path / "market.duckdb")

    result = run_ranking_n225_neutral_rerating_benchmark_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=50,
    )

    assert result.observation_count > 0
    assert not result.coverage_diagnostics_df.empty
    assert not result.signal_summary_df.empty
    assert not result.signal_benchmark_comparison_df.empty
    assert "neutral_all" in set(result.signal_summary_df["signal"].astype(str))
    assert {
        "mean_n225_excess_return_pct",
        "median_n225_excess_return_pct",
        "median_topix_excess_return_pct",
        "median_n225_minus_topix_excess_pct",
    }.issubset(result.signal_summary_df.columns)
    assert {"n225", "topix", "raw"}.issubset(
        set(result.signal_benchmark_comparison_df["benchmark"].astype(str))
    )
    assert {
        "deep_value_flag",
        "sector_strong_flag",
        "atr20_acceleration_ex_overheat_flag",
        "momentum_20_60_top20_flag",
    }.issubset(result.observation_sample_df.columns)


def test_n225_neutral_rerating_benchmark_writes_bundle(tmp_path: Path) -> None:
    db_path = _build_n225_benchmark_db(tmp_path / "market.duckdb")
    result = run_ranking_n225_neutral_rerating_benchmark_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=50,
    )

    bundle = write_ranking_n225_neutral_rerating_benchmark_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )

    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()
    assert bundle.summary_path.exists()


def _build_n225_benchmark_db(db_path: Path) -> Path:
    db_path = _build_sector_strength_db(db_path)
    conn = duckdb.connect(str(db_path))
    dates = [
        str(row[0])
        for row in conn.execute("SELECT DISTINCT date FROM topix_data ORDER BY date").fetchall()
    ]
    rows: list[tuple[str, str, float, float, float, float, int]] = []
    for index, date in enumerate(dates):
        close = 30000.0 + index * 16.0
        rows.append(
            (
                "N225_UNDERPX",
                date,
                close * 0.998,
                close * 1.002,
                close * 0.996,
                close,
                0,
            )
        )
    conn.executemany("INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    conn.close()
    return db_path
