from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt

from src.domains.analytics.topix_downside_return_standard_deviation_shock_confirmation_committee_overlay import (
    TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_SHOCK_CONFIRMATION_COMMITTEE_OVERLAY_EXPERIMENT_ID,
    _build_committee_daily_df,
    get_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_bundle_path_for_run_id,
    get_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_latest_bundle_path,
    load_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research_bundle,
    run_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research,
    write_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research_bundle,
)


def _build_topix_and_topix100_db(db_path: Path, *, periods: int = 120) -> str:
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE topix_data (
            date TEXT PRIMARY KEY,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stocks (
            code TEXT PRIMARY KEY,
            company_name TEXT,
            company_name_english TEXT,
            market_code TEXT,
            market_name TEXT,
            sector_17_code TEXT,
            sector_17_name TEXT,
            sector_33_code TEXT,
            sector_33_name TEXT,
            scale_category TEXT,
            listed_date TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_data (
            code TEXT,
            date TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            created_at TEXT
        )
        """
    )

    dates = pd.bdate_range("2024-01-01", periods=periods)
    topix_rows: list[tuple[str, float, float, float, float, float, None]] = []
    prev_close = 100.0
    for index, date in enumerate(dates):
        if index == 0:
            open_price = prev_close
        else:
            open_price = prev_close * (1.0 + (-0.002 if index % 9 == 0 else 0.001))
        intraday = -0.009 if index % 13 == 0 else 0.003
        close_price = open_price * (1.0 + intraday)
        topix_rows.append(
            (
                date.strftime("%Y-%m-%d"),
                open_price,
                max(open_price, close_price) * 1.002,
                min(open_price, close_price) * 0.998,
                close_price,
                1_000_000.0,
                None,
            )
        )
        prev_close = close_price
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?, ?)", topix_rows)

    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "1301",
                "Alpha",
                None,
                "0111",
                "Prime",
                None,
                None,
                None,
                None,
                "TOPIX Core30",
                "2020-01-01",
                None,
                None,
            ),
            (
                "1302",
                "Beta",
                None,
                "0111",
                "Prime",
                None,
                None,
                None,
                None,
                "TOPIX Large70",
                "2020-01-01",
                None,
                None,
            ),
        ],
    )

    stock_rows: list[tuple[str, str, float, float, float, float, float, None]] = []
    for code, start_price, drift in (("1301", 100.0, 0.006), ("1302", 120.0, -0.004)):
        close_price = start_price
        for index, date in enumerate(dates):
            open_price = close_price * (1.0 + (0.001 if index % 5 else -0.001))
            close_price = open_price * (1.0 + drift)
            stock_rows.append(
                (
                    code,
                    date.strftime("%Y-%m-%d"),
                    open_price,
                    max(open_price, close_price) * 1.002,
                    min(open_price, close_price) * 0.998,
                    close_price,
                    100_000.0 + index,
                    None,
                )
            )
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.close()
    return str(db_path)


def test_build_committee_daily_df_averages_member_returns_and_exposure() -> None:
    member_a = pd.DataFrame(
        {
            "candidate_id": ["a", "a"],
            "signal_date": ["2024-01-01", "2024-01-02"],
            "realized_date": ["2024-01-02", "2024-01-03"],
            "sample_split": ["discovery", "validation"],
            "realized_overnight_return": [0.0, 0.0],
            "realized_intraday_return": [0.02, -0.01],
            "baseline_return": [0.02, -0.01],
            "strategy_return": [0.02, 0.0],
            "exposure_ratio_before_rebalance": [1.0, 0.0],
            "target_exposure_ratio": [1.0, 0.0],
            "exposure_change": [0.0, 0.0],
            "rebalanced": [False, False],
        }
    )
    member_b = pd.DataFrame(
        {
            "candidate_id": ["b", "b"],
            "signal_date": ["2024-01-01", "2024-01-02"],
            "realized_date": ["2024-01-02", "2024-01-03"],
            "sample_split": ["discovery", "validation"],
            "realized_overnight_return": [0.0, 0.0],
            "realized_intraday_return": [0.02, -0.01],
            "baseline_return": [0.02, -0.01],
            "strategy_return": [0.0, -0.01],
            "exposure_ratio_before_rebalance": [1.0, 1.0],
            "target_exposure_ratio": [0.0, 1.0],
            "exposure_change": [-1.0, 0.0],
            "rebalanced": [True, False],
        }
    )

    committee_df = _build_committee_daily_df(
        committee_id="committee",
        member_daily_dfs=[member_a, member_b],
    )

    assert committee_df["strategy_return"].tolist() == [0.01, -0.005]
    assert committee_df["target_exposure_ratio"].tolist() == [0.5, 0.5]
    assert committee_df["member_reduced_count"].tolist() == [1, 1]
    assert committee_df["rebalanced"].tolist() == [True, False]


def test_research_bundle_roundtrip_preserves_committee_overlay_result(
    tmp_path: Path,
) -> None:
    db_path = _build_topix_and_topix100_db(
        tmp_path / "market-topix-shock-confirmation-committee.duckdb",
    )
    result = run_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research(
        db_path,
        downside_return_standard_deviation_window_days=5,
        committee_mean_window_days=(1, 2),
        committee_high_thresholds=(0.20, 0.24),
        low_thresholds=(0.10, 0.12),
        trend_vote_thresholds=(1,),
        fixed_breadth_vote_threshold=1,
        min_constituents_per_day=2,
        validation_ratio=0.25,
        rank_top_ks=(1,),
        discovery_window_days=20,
        validation_window_days=10,
        step_window_days=5,
    )

    assert result.single_candidate_count == 8
    assert result.committee_candidate_count == 2
    assert not result.walkforward_committee_top1_df.empty

    bundle = write_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260413_120000_testabcd",
    )
    reloaded = load_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_SHOCK_CONFIRMATION_COMMITTEE_OVERLAY_EXPERIMENT_ID
    )
    assert (
        get_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_latest_bundle_path(
            output_root=tmp_path
        )
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.space_comparison_summary_df,
        result.space_comparison_summary_df,
        check_dtype=False,
    )
