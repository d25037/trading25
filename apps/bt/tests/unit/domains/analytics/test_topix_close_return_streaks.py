from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics.topix_close_return_streaks import (
    TOPIX_CLOSE_RETURN_STREAKS_RESEARCH_EXPERIMENT_ID,
    get_topix_close_return_streaks_bundle_path_for_run_id,
    get_topix_close_return_streaks_latest_bundle_path,
    load_topix_close_return_streaks_research_bundle,
    run_topix_close_return_streaks_research,
    write_topix_close_return_streaks_research_bundle,
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


def test_streak_research_groups_consecutive_close_returns_into_segments(
    tmp_path: Path,
) -> None:
    db_path = _build_topix_only_db(
        tmp_path / "market-streaks.duckdb",
        returns=[0.02, 0.01, -0.03, -0.01, 0.04, 0.02, 0.01, -0.02],
    )

    result = run_topix_close_return_streaks_research(
        db_path,
        future_horizons=(1, 3),
        validation_ratio=0.0,
        max_streak_day_bucket=4,
        max_segment_length_bucket=4,
    )

    first_segment = result.streak_segment_df.iloc[0]
    assert first_segment["mode"] == "bullish"
    assert first_segment["start_date"] == "2024-01-02"
    assert first_segment["end_date"] == "2024-01-03"
    assert int(first_segment["segment_day_count"]) == 2
    assert first_segment["synthetic_open"] == pytest.approx(100.0)
    assert first_segment["synthetic_close"] == pytest.approx(103.02)
    assert first_segment["segment_return"] == pytest.approx(0.0302)
    assert bool(first_segment["is_complete"]) is True

    first_daily_row = result.streak_daily_df.iloc[0]
    assert first_daily_row["mode"] == "bullish"
    assert int(first_daily_row["streak_day"]) == 1
    assert int(first_daily_row["remaining_segment_days"]) == 1
    assert first_daily_row["segment_start_date"] == "2024-01-02"
    assert first_daily_row["segment_end_date"] == "2024-01-03"

    streak_state_rows = result.streak_state_summary_df[
        (result.streak_state_summary_df["sample_split"] == "full")
        & (result.streak_state_summary_df["mode"] == "bullish")
        & (result.streak_state_summary_df["streak_day_label"] == "1")
        & (result.streak_state_summary_df["horizon_days"] == 1)
    ]
    assert not streak_state_rows.empty
    assert float(streak_state_rows.iloc[0]["continuation_rate_1d"]) > 0.0

    segment_end_rows = result.segment_end_summary_df[
        (result.segment_end_summary_df["sample_split"] == "full")
        & (result.segment_end_summary_df["mode"] == "bullish")
        & (result.segment_end_summary_df["segment_length_label"] == "2")
        & (result.segment_end_summary_df["horizon_days"] == 3)
    ]
    assert not segment_end_rows.empty


def test_bundle_roundtrip_preserves_topix_close_return_streaks_result(
    tmp_path: Path,
) -> None:
    db_path = _build_topix_only_db(
        tmp_path / "market-streak-roundtrip.duckdb",
        returns=[0.03, 0.01, -0.02, -0.01, 0.02, 0.01, -0.03, -0.01] * 8,
    )
    result = run_topix_close_return_streaks_research(
        db_path,
        future_horizons=(1, 3),
        validation_ratio=0.2,
        max_streak_day_bucket=5,
        max_segment_length_bucket=5,
    )

    bundle = write_topix_close_return_streaks_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260405_000000_testabcd",
    )
    reloaded = load_topix_close_return_streaks_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == TOPIX_CLOSE_RETURN_STREAKS_RESEARCH_EXPERIMENT_ID
    assert bundle.summary_path.exists()
    assert (
        get_topix_close_return_streaks_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix_close_return_streaks_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    assert reloaded.future_horizons == result.future_horizons
    assert reloaded.max_streak_day_bucket == result.max_streak_day_bucket
    assert reloaded.max_segment_length_bucket == result.max_segment_length_bucket
    pdt.assert_frame_equal(
        reloaded.streak_state_summary_df,
        result.streak_state_summary_df,
        check_dtype=False,
    )
    pdt.assert_frame_equal(
        reloaded.segment_end_summary_df,
        result.segment_end_summary_df,
        check_dtype=False,
    )
