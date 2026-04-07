from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics.topix_extreme_close_to_close_mode import (
    TOPIX_EXTREME_CLOSE_TO_CLOSE_MODE_RESEARCH_EXPERIMENT_ID,
    get_topix_extreme_close_to_close_mode_bundle_path_for_run_id,
    get_topix_extreme_close_to_close_mode_latest_bundle_path,
    load_topix_extreme_close_to_close_mode_research_bundle,
    run_topix_extreme_close_to_close_mode_research,
    write_topix_extreme_close_to_close_mode_research_bundle,
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


def test_mode_assignment_uses_sign_of_largest_absolute_close_to_close_return(
    tmp_path: Path,
) -> None:
    db_path = _build_topix_only_db(
        tmp_path / "market-dominant-window.duckdb",
        returns=[0.12, -0.01, 0.01, -0.01, -0.01, 0.01],
    )

    result = run_topix_extreme_close_to_close_mode_research(
        db_path,
        candidate_windows=(3,),
        future_horizons=(1,),
        validation_ratio=0.0,
        min_mode_days=1,
    )

    first_row = result.mode_assignments_df.iloc[0]
    assert first_row["date"] == "2024-01-04"
    assert first_row["mode"] == "bullish"
    assert first_row["dominant_event_date"] == "2024-01-02"
    assert first_row["dominant_close_return"] == pytest.approx(0.12)


def test_research_prefers_short_window_when_large_shock_turns_into_small_opposite_drift(
    tmp_path: Path,
) -> None:
    repeating_pattern = [
        0.12,
        -0.01,
        -0.01,
        -0.01,
        -0.01,
        -0.01,
        -0.06,
        0.01,
        0.01,
        0.01,
        0.01,
        0.01,
    ]
    db_path = _build_topix_only_db(
        tmp_path / "market-window-selection.duckdb",
        returns=repeating_pattern * 20,
    )

    result = run_topix_extreme_close_to_close_mode_research(
        db_path,
        candidate_windows=(2, 3, 5, 10),
        future_horizons=(1, 3),
        validation_ratio=0.25,
        min_mode_days=10,
    )

    assert result.selected_window_days == 2
    assert result.selected_short_window_days == 2
    assert result.selected_long_window_days == 2

    discovery_scores = result.window_score_df[
        result.window_score_df["sample_split"] == "discovery"
    ].copy()
    top_row = discovery_scores.sort_values(
        ["selection_score", "window_days"],
        ascending=[False, True],
        kind="stable",
    ).iloc[0]
    assert int(top_row["window_days"]) == 2

    validation_rows = result.selected_window_comparison_df[
        result.selected_window_comparison_df["sample_split"] == "validation"
    ]
    assert not validation_rows.empty
    assert (validation_rows["mean_return_separation"] > 0).all()

    state_summary_df = result.multi_timeframe_state_summary_df[
        result.multi_timeframe_state_summary_df["sample_split"] == "validation"
    ]
    assert not state_summary_df.empty
    assert set(state_summary_df["state_key"]).issubset(
        {
            "long_bullish__short_bullish",
            "long_bullish__short_bearish",
            "long_bearish__short_bullish",
            "long_bearish__short_bearish",
        }
    )
    state_segment_summary_df = result.multi_timeframe_state_segment_summary_df[
        result.multi_timeframe_state_segment_summary_df["sample_split"] == "validation"
    ]
    assert not state_segment_summary_df.empty
    assert (state_segment_summary_df["mean_segment_day_count"] > 0).all()


def test_bundle_roundtrip_preserves_topix_mode_research_result(tmp_path: Path) -> None:
    db_path = _build_topix_only_db(
        tmp_path / "market-roundtrip.duckdb",
        returns=[0.03, 0.01, -0.02, -0.01, 0.02, 0.01, -0.03, -0.01] * 8,
    )
    result = run_topix_extreme_close_to_close_mode_research(
        db_path,
        candidate_windows=(2, 3, 4),
        future_horizons=(1, 3),
        validation_ratio=0.2,
        min_mode_days=3,
    )

    bundle = write_topix_extreme_close_to_close_mode_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260404_120000_testabcd",
    )
    reloaded = load_topix_extreme_close_to_close_mode_research_bundle(bundle.bundle_dir)

    assert (
        bundle.experiment_id
        == TOPIX_EXTREME_CLOSE_TO_CLOSE_MODE_RESEARCH_EXPERIMENT_ID
    )
    assert bundle.summary_path.exists()
    assert (
        get_topix_extreme_close_to_close_mode_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix_extreme_close_to_close_mode_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    assert reloaded.selected_window_days == result.selected_window_days
    assert reloaded.selected_short_window_days == result.selected_short_window_days
    assert reloaded.selected_long_window_days == result.selected_long_window_days
    assert reloaded.candidate_windows == result.candidate_windows
    assert reloaded.future_horizons == result.future_horizons
    pdt.assert_frame_equal(
        reloaded.selected_window_daily_df,
        result.selected_window_daily_df,
        check_dtype=False,
    )
    pdt.assert_frame_equal(
        reloaded.multi_timeframe_state_segment_summary_df,
        result.multi_timeframe_state_segment_summary_df,
        check_dtype=False,
    )
