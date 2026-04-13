from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt

from src.domains.analytics.topix_downside_return_standard_deviation_shock_confirmation_vote_overlay import (
    TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_SHOCK_CONFIRMATION_VOTE_OVERLAY_EXPERIMENT_ID,
    _simulate_candidate_daily_df_with_family_votes,
    get_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_bundle_path_for_run_id,
    get_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_latest_bundle_path,
    load_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research_bundle,
    run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research,
    write_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research_bundle,
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


def test_simulate_candidate_daily_df_with_family_votes_distinguishes_modes() -> None:
    candidate_signal_df = pd.DataFrame(
        {
            "signal_date": ["2024-01-01", "2024-01-02"],
            "realized_date": ["2024-01-02", "2024-01-03"],
            "sample_split": ["discovery", "validation"],
            "downside_return_standard_deviation": [0.01, 0.01],
            "annualized_downside_return_standard_deviation": [0.15, 0.04],
            "annualized_downside_return_standard_deviation_mean": [0.15, 0.04],
            "realized_close_return": [0.01, 0.03],
            "realized_overnight_return": [0.0, 0.0],
            "realized_intraday_return": [0.01, 0.03],
            "topix_close_below_sma20": [True, False],
            "topix_sma20_below_sma60": [False, False],
            "topix_drawdown_63d": [-0.06, -0.02],
            "topix_return_10d": [-0.04, 0.0],
            "breadth_above_sma20_ratio": [0.30, 0.60],
            "breadth_positive_5d_ratio": [0.30, 0.60],
            "breadth_at_20d_low_ratio": [0.10, 0.05],
            "stock_count": [80, 80],
            "equal_weight_return_5d": [-0.02, 0.01],
        }
    )

    strict_df = _simulate_candidate_daily_df_with_family_votes(
        candidate_id="strict",
        candidate_signal_df=candidate_signal_df,
        high_annualized_downside_return_standard_deviation_threshold=0.20,
        low_annualized_downside_return_standard_deviation_threshold=0.05,
        reduced_exposure_ratio=0.0,
        trend_family_rules=("close_below_sma20", "drawdown_63d_le_neg0p05"),
        breadth_family_rules=("topix100_above_sma20_le_0p40",),
        trend_vote_threshold=2,
        breadth_vote_threshold=1,
        confirmation_mode="stress_and_trend_and_breadth",
    )
    vote_df = _simulate_candidate_daily_df_with_family_votes(
        candidate_id="vote",
        candidate_signal_df=candidate_signal_df,
        high_annualized_downside_return_standard_deviation_threshold=0.20,
        low_annualized_downside_return_standard_deviation_threshold=0.05,
        reduced_exposure_ratio=0.0,
        trend_family_rules=("close_below_sma20", "drawdown_63d_le_neg0p05"),
        breadth_family_rules=("topix100_above_sma20_le_0p40",),
        trend_vote_threshold=2,
        breadth_vote_threshold=1,
        confirmation_mode="two_of_three_vote",
    )

    assert strict_df["signal_state"].tolist() == [
        "non_stress_confirmation_only",
        "below_low_threshold",
    ]
    assert vote_df["signal_state"].tolist() == [
        "risk_off_confirmed",
        "below_low_threshold",
    ]
    assert strict_df["target_exposure_ratio"].tolist() == [1.0, 1.0]
    assert vote_df["target_exposure_ratio"].tolist() == [0.0, 1.0]
    assert strict_df["trend_vote_count"].tolist() == [2, 0]
    assert vote_df["breadth_vote_count"].tolist() == [1, 0]


def test_research_bundle_roundtrip_preserves_vote_overlay_result(tmp_path: Path) -> None:
    db_path = _build_topix_and_topix100_db(
        tmp_path / "market-topix-shock-confirmation-vote.duckdb",
    )
    result = run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research(
        db_path,
        downside_return_standard_deviation_window_days=(5,),
        downside_return_standard_deviation_mean_window_days=(1,),
        high_annualized_downside_return_standard_deviation_thresholds=(0.20,),
        low_annualized_downside_return_standard_deviation_thresholds=(0.10,),
        reduced_exposure_ratios=(0.0,),
        trend_family_rules=("close_below_sma20", "drawdown_63d_le_neg0p05"),
        breadth_family_rules=("topix100_above_sma20_le_0p40",),
        trend_vote_thresholds=(1, 2),
        breadth_vote_thresholds=(1,),
        confirmation_modes=("stress_and_trend_or_breadth", "two_of_three_vote"),
        min_constituents_per_day=2,
        validation_ratio=0.25,
        rank_top_ks=(1,),
        discovery_window_days=20,
        validation_window_days=10,
        step_window_days=5,
    )

    assert result.candidate_count == 4
    assert result.fold_count > 0
    assert not result.walkforward_top1_df.empty

    bundle = write_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260413_120000_testabcd",
    )
    reloaded = load_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_SHOCK_CONFIRMATION_VOTE_OVERLAY_EXPERIMENT_ID
    )
    assert (
        get_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_latest_bundle_path(
            output_root=tmp_path
        )
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.walkforward_top1_summary_df,
        result.walkforward_top1_summary_df,
        check_dtype=False,
    )
