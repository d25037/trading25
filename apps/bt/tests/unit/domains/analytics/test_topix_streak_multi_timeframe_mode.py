from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics.topix_streak_multi_timeframe_mode import (
    TOPIX_STREAK_MULTI_TIMEFRAME_MODE_RESEARCH_EXPERIMENT_ID,
    TopixStreakMultiTimeframeModeResearchResult,
    _build_multi_timeframe_state_streak_df,
    _build_pair_horizon_rank_df,
    _build_published_summary_payload,
    _build_research_bundle_summary_markdown,
    _compute_edge_state_consistency,
    _compute_locked_best_worst_ratio,
    _compute_ranking_consistency,
    _resolve_stability_horizons,
    _select_best_pair,
    _select_state_row,
    get_topix_streak_multi_timeframe_mode_bundle_path_for_run_id,
    get_topix_streak_multi_timeframe_mode_latest_bundle_path,
    load_topix_streak_multi_timeframe_mode_research_bundle,
    run_topix_streak_multi_timeframe_mode_research,
    write_topix_streak_multi_timeframe_mode_research_bundle,
)
from src.domains.analytics.topix_streak_extreme_mode import (
    run_topix_streak_extreme_mode_research,
)


def _build_topix_only_db(db_path: Path, returns: list[float]) -> str:
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE topix_data (
            date TEXT PRIMARY KEY,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            created_at TEXT
        )
        """
    )

    dates = pd.bdate_range("2024-01-01", periods=len(returns) + 1)
    rows: list[tuple[str, float, float, float, float, None]] = []
    close = 100.0
    rows.append((dates[0].strftime("%Y-%m-%d"), 100.0, 101.0, 99.0, 100.0, None))
    for date, close_return in zip(dates[1:], returns, strict=True):
        prev_close = close
        close = prev_close * (1.0 + close_return)
        rows.append(
            (
                date.strftime("%Y-%m-%d"),
                prev_close,
                max(prev_close, close) * 1.002,
                min(prev_close, close) * 0.998,
                close,
                None,
            )
        )
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)", rows)
    conn.close()
    return str(db_path)


def _build_multi_timeframe_returns() -> list[float]:
    pattern = [
        0.03,
        0.07,
        0.025,
        0.06,
        0.04,
        0.08,
        0.015,
        0.055,
        0.045,
        0.065,
        0.02,
        0.09,
        0.035,
        0.05,
        0.028,
        0.075,
        0.018,
        0.058,
        0.038,
        0.085,
    ]
    returns: list[float] = []
    for phase_index in range(8):
        for magnitude_index, magnitude in enumerate(pattern):
            sign = 1 if (phase_index + magnitude_index) % 2 == 0 else -1
            returns.append(sign * magnitude)
    return returns


def _build_stub_result(
    *,
    pair_score_df: pd.DataFrame | None = None,
    selected_pair_state_summary_df: pd.DataFrame | None = None,
    selected_pair_state_segment_summary_df: pd.DataFrame | None = None,
) -> TopixStreakMultiTimeframeModeResearchResult:
    return TopixStreakMultiTimeframeModeResearchResult(
        db_path="/tmp/unused.duckdb",
        source_mode="snapshot",
        source_detail="test",
        available_start_date="2024-01-01",
        available_end_date="2024-12-31",
        analysis_start_date="2024-02-01",
        analysis_end_date="2024-12-01",
        candidate_windows=(2, 3, 20, 21),
        future_horizons=(5, 10),
        stability_horizons=(5, 10),
        validation_ratio=0.3,
        min_mode_candles=3,
        min_state_observations=1,
        selected_base_window_streaks=3,
        selected_short_window_streaks=3,
        selected_long_window_streaks=53,
        selection_metric="test",
        streak_candle_df=pd.DataFrame(),
        single_window_score_df=pd.DataFrame(),
        pair_score_df=pair_score_df if pair_score_df is not None else pd.DataFrame(),
        selected_pair_state_streak_df=pd.DataFrame(),
        selected_pair_state_segment_df=pd.DataFrame(),
        selected_pair_state_summary_df=(
            selected_pair_state_summary_df
            if selected_pair_state_summary_df is not None
            else pd.DataFrame(
                columns=[
                    "sample_split",
                    "horizon_days",
                    "state_key",
                    "state_label",
                    "mean_future_return",
                ]
            )
        ),
        selected_pair_state_segment_summary_df=(
            selected_pair_state_segment_summary_df
            if selected_pair_state_segment_summary_df is not None
            else pd.DataFrame(columns=["sample_split", "state_key", "state_label"])
        ),
        selected_pair_horizon_rank_df=pd.DataFrame(),
    )


def test_streak_multi_timeframe_research_selects_short_and_long_pair(
    tmp_path: Path,
) -> None:
    db_path = _build_topix_only_db(
        tmp_path / "market-streak-multi-timeframe.duckdb",
        returns=_build_multi_timeframe_returns(),
    )

    result = run_topix_streak_multi_timeframe_mode_research(
        db_path,
        candidate_windows=(2, 3, 4, 5, 20, 21, 22, 23),
        future_horizons=(1, 3),
        stability_horizons=(1, 3),
        validation_ratio=0.25,
        min_mode_candles=3,
        min_state_observations=1,
    )

    assert result.selected_short_window_streaks <= 10
    assert result.selected_long_window_streaks >= 20
    assert result.selected_short_window_streaks < result.selected_long_window_streaks
    assert not result.pair_score_df.empty
    assert not result.selected_pair_state_summary_df.empty
    validation_states = result.selected_pair_state_summary_df[
        result.selected_pair_state_summary_df["sample_split"] == "validation"
    ]
    assert validation_states["state_key"].nunique() == 4


def test_bundle_roundtrip_preserves_streak_multi_timeframe_result(
    tmp_path: Path,
) -> None:
    db_path = _build_topix_only_db(
        tmp_path / "market-streak-multi-timeframe-roundtrip.duckdb",
        returns=_build_multi_timeframe_returns(),
    )
    result = run_topix_streak_multi_timeframe_mode_research(
        db_path,
        candidate_windows=(2, 3, 4, 5, 20, 21, 22, 23),
        future_horizons=(1, 3),
        stability_horizons=(1, 3),
        validation_ratio=0.25,
        min_mode_candles=3,
        min_state_observations=1,
    )

    bundle = write_topix_streak_multi_timeframe_mode_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260406_110000_testabcd",
    )
    reloaded = load_topix_streak_multi_timeframe_mode_research_bundle(bundle.bundle_dir)

    assert (
        bundle.experiment_id
        == TOPIX_STREAK_MULTI_TIMEFRAME_MODE_RESEARCH_EXPERIMENT_ID
    )
    assert bundle.summary_path.exists()
    assert (
        get_topix_streak_multi_timeframe_mode_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix_streak_multi_timeframe_mode_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    assert reloaded.selected_short_window_streaks == result.selected_short_window_streaks
    assert reloaded.selected_long_window_streaks == result.selected_long_window_streaks
    reloaded_pair_score_df = reloaded.pair_score_df.copy()
    result_pair_score_df = result.pair_score_df.copy()
    reloaded_pair_score_df["selection_rank"] = pd.to_numeric(
        reloaded_pair_score_df["selection_rank"],
        errors="coerce",
    )
    result_pair_score_df["selection_rank"] = pd.to_numeric(
        result_pair_score_df["selection_rank"],
        errors="coerce",
    )
    pdt.assert_frame_equal(
        reloaded_pair_score_df,
        result_pair_score_df,
        check_dtype=False,
    )
    pdt.assert_frame_equal(
        reloaded.selected_pair_horizon_rank_df,
        result.selected_pair_horizon_rank_df,
        check_dtype=False,
    )


def test_helper_guards_cover_error_paths() -> None:
    assert _resolve_stability_horizons(None, future_horizons=(1, 3)) == (1, 3)

    with pytest.raises(ValueError, match="min_state_observations must be positive"):
        run_topix_streak_multi_timeframe_mode_research(
            "/tmp/unused.duckdb",
            min_state_observations=0,
        )

    with pytest.raises(
        ValueError,
        match="stability_horizons must contain at least one positive integer",
    ):
        _resolve_stability_horizons((0,), future_horizons=(1, 3))

    with pytest.raises(
        ValueError,
        match="stability_horizons must be a subset of future_horizons",
    ):
        _resolve_stability_horizons((5,), future_horizons=(1, 3))


def test_ranking_helpers_cover_single_and_empty_cases() -> None:
    single_horizon_rows = [
        {"horizon_days": 5, "rank_position": 1, "state_key": "alpha"},
        {"horizon_days": 5, "rank_position": 2, "state_key": "beta"},
    ]

    assert _compute_ranking_consistency(single_horizon_rows) == pytest.approx(1.0)
    assert _compute_edge_state_consistency([]) == 0.0
    assert _compute_locked_best_worst_ratio([]) == 0.0

    empty_pair_score_df = pd.DataFrame(
        columns=[
            "selection_eligible",
            "selection_score",
            "short_window_streaks",
            "long_window_streaks",
            "locked_best_worst_ratio",
            "ranking_consistency",
            "mean_spread",
            "disagreement_ratio",
        ]
    )
    with pytest.raises(ValueError, match="No eligible streak multi-timeframe pairs"):
        _select_best_pair(empty_pair_score_df)

    empty_state_summary_df = pd.DataFrame(
        columns=["sample_split", "horizon_days", "state_key", "state_label", "mean_future_return"]
    )
    assert _select_state_row(
        empty_state_summary_df,
        largest=True,
        horizons=(5, 10),
    ) is None


def test_state_builder_and_ranker_cover_missing_pair_paths(tmp_path: Path) -> None:
    db_path = _build_topix_only_db(
        tmp_path / "market-streak-multi-timeframe-helper.duckdb",
        returns=_build_multi_timeframe_returns(),
    )
    base_result = run_topix_streak_extreme_mode_research(
        db_path,
        candidate_windows=(2, 3, 4, 5, 20, 21, 22, 23),
        future_horizons=(1, 3),
        validation_ratio=0.25,
        min_mode_candles=3,
    )

    with pytest.raises(ValueError, match="Short/long streak rows were not found"):
        _build_multi_timeframe_state_streak_df(
            base_result.mode_assignments_df,
            short_window_streaks=3,
            long_window_streaks=99,
            future_horizons=(1, 3),
        )

    no_rank_df = pd.DataFrame(
        [
            {
                "sample_split": "validation",
                "horizon_days": 1,
                "state_key": "long_bullish__short_bullish",
                "state_label": "Long Bullish / Short Bullish",
                "long_mode": "bullish",
                "short_mode": "bullish",
                "state_candle_count": 5,
                "mean_future_return": 0.01,
            }
        ]
    )
    with pytest.raises(
        ValueError,
        match="No validation state summary rows matched the stability horizons",
    ):
        _build_pair_horizon_rank_df(no_rank_df, stability_horizons=(5,))


def test_summary_builders_cover_empty_and_partial_sections() -> None:
    empty_result = _build_stub_result()
    payload = _build_published_summary_payload(empty_result)
    assert payload["title"] == "TOPIX Streak Multi-Timeframe Mode"
    assert payload["resultBullets"] == []

    partial_result = _build_stub_result(
        selected_pair_state_summary_df=pd.DataFrame(
            [
                {
                    "sample_split": "validation",
                    "horizon_days": 5,
                    "state_key": "long_bearish__short_bearish",
                    "state_label": "Long Bearish / Short Bearish",
                    "mean_future_return": 0.02,
                }
            ]
        )
    )
    summary_markdown = _build_research_bundle_summary_markdown(partial_result)
    assert "Validation Forward Ordering" in summary_markdown
    assert "10d" not in summary_markdown
