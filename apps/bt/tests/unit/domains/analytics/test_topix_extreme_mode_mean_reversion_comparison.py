from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics.topix_extreme_mode_mean_reversion_comparison import (
    TOPIX_EXTREME_MODE_MEAN_REVERSION_COMPARISON_EXPERIMENT_ID,
    _build_trade_rows_for_signal_sequence,
    get_topix_extreme_mode_mean_reversion_comparison_bundle_path_for_run_id,
    get_topix_extreme_mode_mean_reversion_comparison_latest_bundle_path,
    load_topix_extreme_mode_mean_reversion_comparison_bundle,
    run_topix_extreme_mode_mean_reversion_comparison_research,
    write_topix_extreme_mode_mean_reversion_comparison_bundle,
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


def test_trade_builder_skips_overlapping_signals_until_exit() -> None:
    price_df = pd.DataFrame(
        [
            {"date": "2024-01-01", "open": 100.0, "close": 100.0},
            {"date": "2024-01-02", "open": 100.0, "close": 101.0},
            {"date": "2024-01-03", "open": 101.0, "close": 103.0},
            {"date": "2024-01-04", "open": 103.0, "close": 104.0},
        ]
    )
    signal_df = pd.DataFrame(
        [
            {"signal_date": "2024-01-01", "mode": "bearish"},
            {"signal_date": "2024-01-02", "mode": "bearish"},
        ]
    )

    trades_df = _build_trade_rows_for_signal_sequence(
        price_df,
        signal_df,
        model="streak",
        sample_split="validation",
        strategy="long_on_bearish",
        hold_days=2,
        side_resolver=lambda row: "long",
    )

    assert len(trades_df) == 1
    trade = trades_df.iloc[0]
    assert trade["entry_date"] == "2024-01-02"
    assert trade["exit_date"] == "2024-01-03"
    assert trade["trade_return"] == pytest.approx(0.03)


def test_bundle_roundtrip_preserves_mean_reversion_comparison_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_topix_only_db(
        tmp_path / "market-extreme-mode-mean-reversion.duckdb",
        returns=[0.02, -0.03, 0.025, -0.02, 0.03, -0.025] * 24,
    )
    result = run_topix_extreme_mode_mean_reversion_comparison_research(
        db_path,
        normal_candidate_windows=(2, 3),
        streak_candidate_windows=(2, 3),
        future_horizons=(1, 3),
        hold_days=(1, 3),
        validation_ratio=0.2,
        min_normal_mode_days=4,
        min_streak_mode_candles=4,
    )

    assert result.selected_normal_window_days in {2, 3}
    assert result.selected_streak_window_streaks in {2, 3}
    assert set(result.model_overview_df["model"]) == {"normal", "streak"}
    assert set(result.backtest_summary_df["strategy"]) == {
        "long_on_bearish",
        "short_on_bullish",
        "long_bear_short_bull",
    }
    assert not result.validation_leaderboard_df.empty

    bundle = write_topix_extreme_mode_mean_reversion_comparison_bundle(
        result,
        output_root=tmp_path,
        run_id="20260405_020000_testabcd",
    )
    reloaded = load_topix_extreme_mode_mean_reversion_comparison_bundle(bundle.bundle_dir)

    assert (
        bundle.experiment_id
        == TOPIX_EXTREME_MODE_MEAN_REVERSION_COMPARISON_EXPERIMENT_ID
    )
    assert bundle.summary_path.exists()
    assert (
        get_topix_extreme_mode_mean_reversion_comparison_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix_extreme_mode_mean_reversion_comparison_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.model_overview_df,
        result.model_overview_df,
        check_dtype=False,
    )
    pdt.assert_frame_equal(
        reloaded.backtest_summary_df,
        result.backtest_summary_df,
        check_dtype=False,
    )
