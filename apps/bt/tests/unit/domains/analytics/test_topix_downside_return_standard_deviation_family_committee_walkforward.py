from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt

from src.domains.analytics.topix_downside_return_standard_deviation_family_committee_walkforward import (
    TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_FAMILY_COMMITTEE_WALKFORWARD_EXPERIMENT_ID,
    _build_committee_summary_df,
    get_topix_downside_return_standard_deviation_family_committee_walkforward_bundle_path_for_run_id,
    get_topix_downside_return_standard_deviation_family_committee_walkforward_latest_bundle_path,
    load_topix_downside_return_standard_deviation_family_committee_walkforward_research_bundle,
    run_topix_downside_return_standard_deviation_family_committee_walkforward_research,
    write_topix_downside_return_standard_deviation_family_committee_walkforward_research_bundle,
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


def test_build_committee_summary_df_aggregates_win_rate() -> None:
    walkforward_fold_committee_df = pd.DataFrame(
        {
            "committee_size": [1, 1, 3, 3],
            "validation_sharpe_win": [True, False, True, True],
            "validation_cagr_win": [False, False, True, True],
            "validation_sharpe_ratio_excess": [0.10, -0.20, 0.30, 0.40],
            "validation_cagr_excess": [0.01, -0.02, 0.03, 0.04],
            "validation_max_drawdown_improvement": [0.05, -0.01, 0.07, 0.08],
            "committee_validation_sharpe_ratio": [1.2, 0.8, 1.5, 1.6],
            "committee_validation_cagr": [0.10, 0.05, 0.12, 0.13],
            "baseline_validation_sharpe_ratio": [1.0, 1.0, 1.0, 1.0],
            "baseline_validation_cagr": [0.09, 0.09, 0.09, 0.09],
        }
    )

    summary_df = _build_committee_summary_df(walkforward_fold_committee_df)

    top_row = summary_df.iloc[0]
    assert int(top_row["committee_size"]) == 3
    assert float(top_row["validation_sharpe_win_rate"]) == 1.0
    assert float(top_row["avg_validation_sharpe_ratio_excess"]) == 0.35


def test_research_bundle_roundtrip_preserves_walkforward_result(tmp_path: Path) -> None:
    overnight_returns = [0.001 if index % 2 == 0 else -0.001 for index in range(60)]
    intraday_returns = [
        [0.012, -0.018, 0.009, -0.010, 0.015, -0.012, 0.006, -0.004][index % 8]
        for index in range(60)
    ]
    db_path = _build_topix_only_db(
        tmp_path / "market-topix-downside-family-committee.duckdb",
        overnight_returns=overnight_returns,
        intraday_returns=intraday_returns,
    )

    result = run_topix_downside_return_standard_deviation_family_committee_walkforward_research(
        db_path,
        fixed_split_validation_ratio=0.25,
        family_downside_return_standard_deviation_window_days=(3,),
        family_downside_return_standard_deviation_mean_window_days=(1,),
        family_high_annualized_downside_return_standard_deviation_thresholds=(0.10, 0.20),
        family_low_annualized_downside_return_standard_deviation_thresholds=(0.05, 0.10),
        family_reduced_exposure_ratios=(0.0, 0.1),
        committee_sizes=(1, 2),
        rank_top_ks=(1, 2),
        discovery_window_days=8,
        validation_window_days=4,
        step_window_days=2,
    )

    assert result.candidate_count == 8
    assert result.fold_count > 0
    assert not result.rank_stability_df.empty
    assert not result.walkforward_fold_candidate_rank_df.empty
    assert not result.walkforward_fold_committee_df.empty
    assert set(result.committee_summary_df["committee_size"].astype(int)) == {1, 2}

    bundle = write_topix_downside_return_standard_deviation_family_committee_walkforward_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260413_120000_testabcd",
    )
    reloaded = (
        load_topix_downside_return_standard_deviation_family_committee_walkforward_research_bundle(
            bundle.bundle_dir
        )
    )

    assert (
        bundle.experiment_id
        == TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_FAMILY_COMMITTEE_WALKFORWARD_EXPERIMENT_ID
    )
    assert (
        get_topix_downside_return_standard_deviation_family_committee_walkforward_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix_downside_return_standard_deviation_family_committee_walkforward_latest_bundle_path(
            output_root=tmp_path
        )
        == bundle.bundle_dir
    )
    assert reloaded.committee_sizes == (1, 2)
    assert reloaded.fold_count == result.fold_count
    pdt.assert_frame_equal(
        reloaded.committee_summary_df,
        result.committee_summary_df,
        check_dtype=False,
    )
