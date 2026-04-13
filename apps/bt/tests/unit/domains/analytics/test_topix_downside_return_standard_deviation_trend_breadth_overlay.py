from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt

from src.domains.analytics.topix_downside_return_standard_deviation_trend_breadth_overlay import (
    TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_TREND_BREADTH_OVERLAY_EXPERIMENT_ID,
    _build_topix100_breadth_daily_df,
    _simulate_candidate_daily_df_with_regime_filters,
    get_topix_downside_return_standard_deviation_trend_breadth_overlay_bundle_path_for_run_id,
    get_topix_downside_return_standard_deviation_trend_breadth_overlay_latest_bundle_path,
    load_topix_downside_return_standard_deviation_trend_breadth_overlay_research_bundle,
    run_topix_downside_return_standard_deviation_trend_breadth_overlay_research,
    write_topix_downside_return_standard_deviation_trend_breadth_overlay_research_bundle,
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
            overnight = -0.002 if index % 7 == 0 else 0.001
            open_price = prev_close * (1.0 + overnight)
        intraday = -0.008 if index % 11 == 0 else 0.003
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

    stock_master_rows = [
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
    ]
    conn.executemany("INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", stock_master_rows)

    stock_rows: list[tuple[str, str, float, float, float, float, float, None]] = []
    for code, start_price, daily_drift in (("1301", 100.0, 0.006), ("1302", 120.0, -0.004)):
        close_price = start_price
        for index, date in enumerate(dates):
            open_price = close_price * (1.0 + (0.001 if index % 5 else -0.001))
            close_price = open_price * (1.0 + daily_drift)
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


def test_build_topix100_breadth_daily_df_computes_expected_ratios() -> None:
    dates = pd.bdate_range("2024-01-01", periods=25)
    history_df = pd.DataFrame(
        {
            "code": ["1301"] * 25 + ["1302"] * 25,
            "date": [*map(lambda value: value.strftime("%Y-%m-%d"), dates)] * 2,
            "close": list(range(1, 26)) + list(range(25, 0, -1)),
        }
    )

    breadth_daily_df = _build_topix100_breadth_daily_df(
        history_df,
        min_constituents_per_day=2,
    )

    last_row = breadth_daily_df.iloc[-1]
    assert int(last_row["stock_count"]) == 2
    assert float(last_row["breadth_above_sma20_ratio"]) == 0.5
    assert float(last_row["breadth_positive_5d_ratio"]) == 0.5
    assert float(last_row["breadth_at_20d_low_ratio"]) == 0.5


def test_simulate_candidate_daily_df_with_regime_filters_requires_confirmation_and_low_exit() -> None:
    candidate_signal_df = pd.DataFrame(
        {
            "signal_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "realized_date": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "sample_split": ["discovery", "discovery", "validation"],
            "downside_return_standard_deviation": [0.01, 0.02, 0.01],
            "annualized_downside_return_standard_deviation": [0.16, 0.32, 0.10],
            "annualized_downside_return_standard_deviation_mean": [0.26, 0.26, 0.04],
            "realized_close_return": [0.01, -0.02, 0.03],
            "realized_overnight_return": [0.0, 0.0, 0.0],
            "realized_intraday_return": [0.01, -0.02, 0.03],
            "topix_close_below_sma20": [False, True, True],
            "topix_sma20_below_sma60": [False, True, True],
            "topix_drawdown_63d": [-0.01, -0.06, -0.02],
            "topix_return_10d": [0.01, -0.04, -0.01],
            "breadth_above_sma20_ratio": [0.60, 0.30, 0.45],
            "breadth_positive_5d_ratio": [0.70, 0.20, 0.50],
            "breadth_at_20d_low_ratio": [0.05, 0.25, 0.10],
            "stock_count": [80, 80, 80],
            "equal_weight_return_5d": [0.01, -0.03, 0.0],
        }
    )

    daily_df = _simulate_candidate_daily_df_with_regime_filters(
        candidate_id="candidate",
        candidate_signal_df=candidate_signal_df,
        high_annualized_downside_return_standard_deviation_threshold=0.20,
        low_annualized_downside_return_standard_deviation_threshold=0.05,
        reduced_exposure_ratio=0.0,
        trend_rule="close_below_sma20",
        breadth_rule="topix100_above_sma20_le_0p40",
    )

    assert daily_df["signal_state"].tolist() == [
        "high_threshold_unconfirmed",
        "risk_off_confirmed",
        "below_low_threshold",
    ]
    assert daily_df["target_exposure_ratio"].tolist() == [1.0, 0.0, 1.0]


def test_research_bundle_roundtrip_preserves_trend_breadth_overlay_result(tmp_path: Path) -> None:
    db_path = _build_topix_and_topix100_db(
        tmp_path / "market-topix-downside-trend-breadth.duckdb",
    )

    result = run_topix_downside_return_standard_deviation_trend_breadth_overlay_research(
        db_path,
        downside_return_standard_deviation_window_days=(3,),
        downside_return_standard_deviation_mean_window_days=(1,),
        high_annualized_downside_return_standard_deviation_thresholds=(0.10,),
        low_annualized_downside_return_standard_deviation_thresholds=(0.05,),
        reduced_exposure_ratios=(0.0,),
        trend_rules=("close_below_sma20",),
        breadth_rules=("topix100_above_sma20_le_0p40",),
        min_constituents_per_day=2,
        validation_ratio=0.25,
        rank_top_ks=(1,),
        discovery_window_days=20,
        validation_window_days=10,
        step_window_days=5,
    )

    assert result.candidate_count == 1
    assert result.fold_count > 0
    assert not result.breadth_daily_df.empty
    assert not result.walkforward_top1_df.empty

    bundle = write_topix_downside_return_standard_deviation_trend_breadth_overlay_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260413_120000_testabcd",
    )
    reloaded = load_topix_downside_return_standard_deviation_trend_breadth_overlay_research_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == TOPIX_DOWNSIDE_RETURN_STANDARD_DEVIATION_TREND_BREADTH_OVERLAY_EXPERIMENT_ID
    )
    assert (
        get_topix_downside_return_standard_deviation_trend_breadth_overlay_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix_downside_return_standard_deviation_trend_breadth_overlay_latest_bundle_path(
            output_root=tmp_path
        )
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.walkforward_top1_summary_df,
        result.walkforward_top1_summary_df,
        check_dtype=False,
    )
