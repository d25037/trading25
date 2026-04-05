from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics.topix_streak_extreme_mode import (
    TOPIX_STREAK_EXTREME_MODE_RESEARCH_EXPERIMENT_ID,
    get_topix_streak_extreme_mode_bundle_path_for_run_id,
    get_topix_streak_extreme_mode_latest_bundle_path,
    load_topix_streak_extreme_mode_research_bundle,
    run_topix_streak_extreme_mode_research,
    write_topix_streak_extreme_mode_research_bundle,
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


def test_mode_assignment_uses_sign_of_largest_absolute_streak_candle(
    tmp_path: Path,
) -> None:
    db_path = _build_topix_only_db(
        tmp_path / "market-streak-extreme.duckdb",
        returns=[0.03, 0.02, -0.01, -0.01, 0.04, -0.06, 0.01],
    )

    result = run_topix_streak_extreme_mode_research(
        db_path,
        candidate_windows=(2,),
        future_horizons=(1,),
        validation_ratio=0.0,
        min_mode_candles=1,
    )

    first_row = result.mode_assignments_df.iloc[0]
    assert first_row["segment_start_date"] == "2024-01-04"
    assert first_row["segment_end_date"] == "2024-01-05"
    assert first_row["mode"] == "bullish"
    assert first_row["dominant_segment_start_date"] == "2024-01-02"
    assert first_row["dominant_segment_end_date"] == "2024-01-03"
    assert first_row["dominant_segment_return"] == pytest.approx(0.0506, rel=1e-6)

    last_row = result.mode_assignments_df.iloc[-1]
    assert last_row["mode"] == "bearish"
    assert last_row["dominant_segment_return"] == pytest.approx(-0.06)


def test_bundle_roundtrip_preserves_topix_streak_extreme_mode_result(
    tmp_path: Path,
) -> None:
    db_path = _build_topix_only_db(
        tmp_path / "market-streak-extreme-roundtrip.duckdb",
        returns=[0.03, 0.01, -0.02, -0.01, 0.02, 0.01, -0.03, -0.01] * 8,
    )
    result = run_topix_streak_extreme_mode_research(
        db_path,
        candidate_windows=(2, 3, 4),
        future_horizons=(1, 3),
        validation_ratio=0.2,
        min_mode_candles=3,
    )

    bundle = write_topix_streak_extreme_mode_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260405_010000_testabcd",
    )
    reloaded = load_topix_streak_extreme_mode_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == TOPIX_STREAK_EXTREME_MODE_RESEARCH_EXPERIMENT_ID
    assert bundle.summary_path.exists()
    assert (
        get_topix_streak_extreme_mode_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix_streak_extreme_mode_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    assert reloaded.selected_window_streaks == result.selected_window_streaks
    assert reloaded.candidate_windows == result.candidate_windows
    assert reloaded.future_horizons == result.future_horizons
    reloaded_window_score_df = reloaded.window_score_df.copy()
    result_window_score_df = result.window_score_df.copy()
    reloaded_window_score_df["selection_rank"] = pd.to_numeric(
        reloaded_window_score_df["selection_rank"],
        errors="coerce",
    )
    result_window_score_df["selection_rank"] = pd.to_numeric(
        result_window_score_df["selection_rank"],
        errors="coerce",
    )
    pdt.assert_frame_equal(
        reloaded.selected_window_streak_df,
        result.selected_window_streak_df,
        check_dtype=False,
    )
    pdt.assert_frame_equal(
        reloaded_window_score_df,
        result_window_score_df,
        check_dtype=False,
    )
