from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics.topix_return_standard_deviation_exposure_timing import (
    TOPIX_RETURN_STANDARD_DEVIATION_EXPOSURE_TIMING_EXPERIMENT_ID,
    _simulate_candidate_daily_df,
    get_topix_return_standard_deviation_exposure_timing_bundle_path_for_run_id,
    get_topix_return_standard_deviation_exposure_timing_latest_bundle_path,
    load_topix_return_standard_deviation_exposure_timing_research_bundle,
    run_topix_return_standard_deviation_exposure_timing_research,
    write_topix_return_standard_deviation_exposure_timing_research_bundle,
)


def _build_topix_only_db(
    db_path: Path,
    *,
    overnight_returns: list[float],
    intraday_returns: list[float],
) -> str:
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

    dates = pd.bdate_range("2024-01-01", periods=len(overnight_returns) + 1)
    rows: list[tuple[str, float, float, float, float, None]] = []
    previous_close = 100.0
    rows.append((dates[0].strftime("%Y-%m-%d"), 100.0, 101.0, 99.0, 100.0, None))
    for date, overnight_return, intraday_return in zip(
        dates[1:],
        overnight_returns,
        intraday_returns,
        strict=True,
    ):
        open_price = previous_close * (1.0 + overnight_return)
        close_price = open_price * (1.0 + intraday_return)
        rows.append(
            (
                date.strftime("%Y-%m-%d"),
                open_price,
                max(open_price, close_price) * 1.002,
                min(open_price, close_price) * 0.998,
                close_price,
                None,
            )
        )
        previous_close = close_price
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)", rows)
    conn.close()
    return str(db_path)


def test_simulation_uses_previous_exposure_for_overnight_and_new_exposure_for_intraday() -> None:
    signal_df = pd.DataFrame(
        {
            "signal_date": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "realized_date": ["2024-01-03", "2024-01-04", "2024-01-05"],
            "sample_split": ["discovery", "discovery", "validation"],
            "return_standard_deviation": [0.010, 0.011, 0.009],
            "annualized_return_standard_deviation": [0.20, 0.21, 0.19],
            "annualized_return_standard_deviation_mean": [0.30, 0.22, 0.18],
            "realized_close_return": [0.0302, 0.0200, 0.0201],
            "realized_overnight_return": [0.01, 0.00, 0.01],
            "realized_intraday_return": [0.02, 0.02, 0.01],
        }
    )

    result_df = _simulate_candidate_daily_df(
        candidate_id="test_candidate",
        candidate_signal_df=signal_df,
        high_annualized_return_standard_deviation_threshold=0.25,
        low_annualized_return_standard_deviation_threshold=0.20,
        reduced_exposure_ratio=0.50,
    )

    assert result_df["exposure_ratio_before_rebalance"].tolist() == pytest.approx([1.0, 0.5, 0.5])
    assert result_df["target_exposure_ratio"].tolist() == pytest.approx([0.5, 0.5, 1.0])
    assert result_df["rebalanced"].tolist() == [True, False, True]
    assert result_df["signal_state"].tolist() == [
        "above_high_threshold",
        "inside_band",
        "below_low_threshold",
    ]
    assert result_df["strategy_return"].tolist() == pytest.approx(
        [
            (1.0 + 1.0 * 0.01) * (1.0 + 0.5 * 0.02) - 1.0,
            (1.0 + 0.5 * 0.00) * (1.0 + 0.5 * 0.02) - 1.0,
            (1.0 + 0.5 * 0.01) * (1.0 + 1.0 * 0.01) - 1.0,
        ]
    )


def test_research_bundle_roundtrip_preserves_topix_return_standard_deviation_result(
    tmp_path: Path,
) -> None:
    overnight_returns = [0.0 if index % 2 == 0 else 0.002 for index in range(80)]
    intraday_returns = [
        [0.010, -0.015, 0.012, -0.020, 0.018, -0.010][index % 6]
        for index in range(80)
    ]
    db_path = _build_topix_only_db(
        tmp_path / "market-topix-return-standard-deviation.duckdb",
        overnight_returns=overnight_returns,
        intraday_returns=intraday_returns,
    )

    result = run_topix_return_standard_deviation_exposure_timing_research(
        db_path,
        return_standard_deviation_window_days=(3,),
        return_standard_deviation_mean_window_days=(2,),
        high_annualized_return_standard_deviation_thresholds=(0.25,),
        low_annualized_return_standard_deviation_thresholds=(0.15,),
        reduced_exposure_ratios=(0.50,),
        validation_ratio=0.25,
    )

    assert result.candidate_count == 1
    assert not result.baseline_metrics_df.empty
    assert not result.candidate_metrics_df.empty
    assert not result.candidate_comparison_df.empty
    assert not result.selection_summary_df.empty
    assert not result.best_sharpe_daily_df.empty

    bundle = write_topix_return_standard_deviation_exposure_timing_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260413_120000_testabcd",
    )
    reloaded = load_topix_return_standard_deviation_exposure_timing_research_bundle(
        bundle.bundle_dir
    )

    assert bundle.experiment_id == TOPIX_RETURN_STANDARD_DEVIATION_EXPOSURE_TIMING_EXPERIMENT_ID
    assert bundle.summary_path.exists()
    assert (
        get_topix_return_standard_deviation_exposure_timing_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix_return_standard_deviation_exposure_timing_latest_bundle_path(
            output_root=tmp_path
        )
        == bundle.bundle_dir
    )
    assert reloaded.return_standard_deviation_window_days == (3,)
    assert reloaded.return_standard_deviation_mean_window_days == (2,)
    assert reloaded.candidate_count == 1
    pdt.assert_frame_equal(
        reloaded.candidate_comparison_df,
        result.candidate_comparison_df,
        check_dtype=False,
    )
    pdt.assert_frame_equal(
        reloaded.best_sharpe_daily_df,
        result.best_sharpe_daily_df,
        check_dtype=False,
    )
