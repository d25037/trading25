from __future__ import annotations

from pathlib import Path

from src.domains.analytics.ranking_n225_crowded_rerating_benchmark import (
    run_ranking_n225_crowded_rerating_benchmark_research,
    write_ranking_n225_crowded_rerating_benchmark_bundle,
)

from test_ranking_n225_neutral_rerating_benchmark import _build_n225_benchmark_db


def test_n225_crowded_rerating_benchmark_builds_bundle(tmp_path: Path) -> None:
    db_path = _build_n225_benchmark_db(tmp_path / "market.duckdb")

    result = run_ranking_n225_crowded_rerating_benchmark_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-04-30",
        horizons=(20,),
        market_scopes=("prime",),
        min_observations=1,
        observation_sample_limit=50,
    )

    assert result.observation_count > 0
    assert set(result.liquidity_regimes) == {"crowded_rerating"}
    assert not result.signal_summary_df.empty
    assert "crowded_all" in set(result.signal_summary_df["signal"].astype(str))
    assert "neutral_all" not in set(result.signal_summary_df["signal"].astype(str))

    bundle = write_ranking_n225_crowded_rerating_benchmark_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()
    assert bundle.summary_path.exists()
