from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.accumulation_flow_followthrough import (
    _attach_benchmark_returns,
    _build_cohort_portfolio_summary_df,
    _build_capped_entry_cohort_df,
    _build_entry_cohort_df,
    _build_event_df,
    _build_event_summary_df,
    _build_oos_portfolio_summary_df,
    _prepare_accumulation_panel,
    _prepare_topix_return_panel,
    _query_universe_stock_history,
    get_accumulation_flow_followthrough_bundle_path_for_run_id,
    get_accumulation_flow_followthrough_latest_bundle_path,
    load_accumulation_flow_followthrough_research_bundle,
    run_accumulation_flow_followthrough_research,
    write_accumulation_flow_followthrough_research_bundle,
)


def _build_raw_panel() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    dates = pd.date_range("2026-01-01", periods=8, freq="D")
    for code, offset in [("1111", 0.0), ("2222", 2.0)]:
        for index, date in enumerate(dates):
            close = 100.0 + offset + index
            rows.append(
                {
                    "universe_key": "standard",
                    "code": code,
                    "company_name": f"{code} Corp",
                    "date": date.strftime("%Y-%m-%d"),
                    "open": close - 0.5,
                    "high": close + 1.0,
                    "low": close - 4.0,
                    "close": close,
                    "volume": 1000.0,
                }
            )
    return pd.DataFrame(rows)


def _prepare_test_panel(raw_df: pd.DataFrame) -> pd.DataFrame:
    return _prepare_accumulation_panel(
        raw_df,
        analysis_start_date="2026-01-01",
        analysis_end_date="2026-01-08",
        horizons=(2,),
        cmf_period=2,
        chaikin_fast_period=2,
        chaikin_slow_period=3,
        obv_lookback_period=2,
        cmf_threshold=0.1,
        chaikin_oscillator_threshold=0.1,
        obv_score_threshold=0.5,
        min_votes=2,
        price_sma_period=3,
        price_high_lookback_period=3,
        max_close_to_sma=0.05,
        max_close_to_high=0.0,
        lower_wick_threshold=0.5,
    )


def _create_research_duckdb(db_path: str) -> None:
    conn = duckdb.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE stocks (
                code VARCHAR,
                company_name VARCHAR,
                market_code VARCHAR,
                scale_category VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE stock_data (
                date DATE,
                code VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE topix_data (
                date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE
            )
            """
        )
        conn.executemany(
            "INSERT INTO stocks VALUES (?, ?, ?, ?)",
            [
                ("1111", "Topix Member", "0111", "TOPIX Mid400"),
                ("2222", "Prime Non Topix", "0111", None),
                ("3333", "Standard Name", "0112", None),
                ("4444", "Growth Name", "0113", None),
            ],
        )
        stock_rows: list[tuple[str, str, float, float, float, float, float]] = []
        topix_rows: list[tuple[str, float, float, float, float, float]] = []
        dates = pd.date_range("2026-01-01", periods=80, freq="D")
        for day_index, date in enumerate(dates):
            date_text = date.strftime("%Y-%m-%d")
            topix_close = 1000.0 + day_index
            topix_rows.append(
                (
                    date_text,
                    topix_close - 0.5,
                    topix_close + 1.0,
                    topix_close - 1.0,
                    topix_close,
                    1_000_000.0,
                )
            )
            for code_index, code in enumerate(["1111", "2222", "3333", "4444"]):
                close = 100.0 + code_index * 5.0 + day_index
                stock_rows.append(
                    (
                        date_text,
                        code,
                        close - 0.5,
                        close + 1.0,
                        close - 4.0,
                        close,
                        1000.0 + code_index * 100.0,
                    )
                )
        conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)", topix_rows)
        conn.executemany(
            "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)",
            stock_rows,
        )
    finally:
        conn.close()


def test_query_universe_stock_history_applies_end_date_and_universe_params() -> None:
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(
            """
            CREATE TABLE stocks (
                code VARCHAR,
                company_name VARCHAR,
                market_code VARCHAR,
                scale_category VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE stock_data (
                date DATE,
                code VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO stocks VALUES
                ('1111', 'Standard One', '0112', NULL),
                ('2222', 'Prime One', '0111', NULL)
            """
        )
        conn.execute(
            """
            INSERT INTO stock_data VALUES
                ('2026-01-01', '1111', 10, 11, 9, 10, 100),
                ('2026-01-02', '1111', 10, 12, 9, 11, 100),
                ('2026-01-03', '1111', 11, 13, 10, 12, 100),
                ('2026-01-02', '2222', 20, 21, 19, 20, 100)
            """
        )

        result = _query_universe_stock_history(
            conn,
            universe_key="standard",
            end_date="2026-01-02",
        )
    finally:
        conn.close()

    assert result["code"].tolist() == ["1111", "1111"]
    assert result["date"].astype(str).tolist() == ["2026-01-01", "2026-01-02"]


def test_prepare_accumulation_panel_and_event_branches() -> None:
    panel_df = _prepare_test_panel(_build_raw_panel())

    assert panel_df["accumulation_pressure"].sum() > 0
    assert panel_df["price_not_extended"].sum() > 0
    assert panel_df["lower_wick_absorption"].sum() > 0

    event_df = _build_event_df(panel_df, horizons=(2,))

    assert {
        "accumulation",
        "not_extended",
        "not_extended_lower_wick",
    }.issubset(set(event_df["filter_key"]))
    assert event_df["next_open_to_close_2d_return"].notna().sum() > 0


def test_event_and_cohort_summaries_include_portfolio_lens() -> None:
    panel_df = _prepare_test_panel(_build_raw_panel())
    event_df = _build_event_df(panel_df, horizons=(2,))
    topix_df = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=8, freq="D"),
            "open": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0],
            "close": [100.5, 101.5, 102.5, 103.5, 104.5, 105.5, 106.5, 107.5],
        }
    )
    topix_returns = _prepare_topix_return_panel(topix_df, horizons=(2,))
    event_df = _attach_benchmark_returns(event_df, topix_returns, horizons=(2,))

    event_summary_df = _build_event_summary_df(event_df, horizons=(2,))
    focused_event_summary = event_summary_df[
        (event_summary_df["universe_key"] == "standard")
        & (event_summary_df["filter_key"] == "not_extended_lower_wick")
        & (event_summary_df["horizon_days"] == 2)
    ].iloc[0]
    assert int(focused_event_summary["event_count"]) > 0
    assert float(focused_event_summary["mean_return"]) > 0
    assert "mean_excess_return" in focused_event_summary
    assert float(focused_event_summary["win_rate"]) == pytest.approx(1.0)

    entry_cohort_df = _build_entry_cohort_df(event_df, horizons=(2,))
    two_name_cohort = entry_cohort_df[
        (entry_cohort_df["filter_key"] == "not_extended_lower_wick")
        & (entry_cohort_df["cohort_event_count"] == 2)
    ]
    assert not two_name_cohort.empty

    portfolio_summary_df = _build_cohort_portfolio_summary_df(entry_cohort_df)
    focused_portfolio_summary = portfolio_summary_df[
        (portfolio_summary_df["universe_key"] == "standard")
        & (portfolio_summary_df["filter_key"] == "not_extended_lower_wick")
        & (portfolio_summary_df["horizon_days"] == 2)
    ].iloc[0]
    assert int(focused_portfolio_summary["total_signal_count"]) >= 2
    assert float(focused_portfolio_summary["mean_cohort_return"]) > 0
    assert "mean_excess_return" in focused_portfolio_summary


def test_capped_portfolio_limits_names_per_entry_date() -> None:
    event_df = pd.DataFrame(
        {
            "date": ["2026-01-01"] * 3,
            "entry_date": ["2026-01-02"] * 3,
            "calendar_year": [2026] * 3,
            "universe_key": ["standard"] * 3,
            "universe_label": ["Standard"] * 3,
            "filter_key": ["not_extended"] * 3,
            "filter_label": ["Accumulation + not extended"] * 3,
            "code": ["1111", "2222", "3333"],
            "cmf": [0.20, 0.80, 0.30],
            "obv_flow_score": [0.10, 0.20, 0.30],
            "chaikin_oscillator": [1.0, 2.0, 3.0],
            "lower_wick_ratio": [0.1, 0.2, 0.3],
            "accumulation_vote_count": [2, 3, 2],
            "next_open_to_close_2d_return": [0.01, 0.05, -0.02],
            "topix_next_open_to_close_2d_return": [0.02, 0.02, 0.02],
            "excess_next_open_to_close_2d_return": [-0.01, 0.03, -0.04],
        }
    )

    capped_df = _build_capped_entry_cohort_df(
        event_df,
        horizons=(2,),
        concentration_caps=(1, 2),
    )

    cap_one = capped_df.loc[capped_df["max_names_per_date"] == 1].iloc[0]
    assert int(cap_one["cohort_event_count"]) == 1
    assert float(cap_one["equal_weight_return"]) == pytest.approx(0.05)
    assert float(cap_one["excess_equal_weight_return"]) == pytest.approx(0.03)


def test_oos_portfolio_summary_splits_fixed_periods() -> None:
    entry_cohort_df = pd.DataFrame(
        {
            "entry_date": ["2020-01-02", "2022-01-04", "2024-01-04"],
            "calendar_year": [2020, 2022, 2024],
            "universe_key": ["standard"] * 3,
            "universe_label": ["Standard"] * 3,
            "filter_key": ["not_extended"] * 3,
            "filter_label": ["Accumulation + not extended"] * 3,
            "horizon_days": [20, 20, 20],
            "cohort_event_count": [3, 3, 3],
            "cohort_unique_code_count": [3, 3, 3],
            "equal_weight_return": [0.02, 0.01, 0.03],
            "topix_return": [0.01, 0.02, 0.01],
            "excess_equal_weight_return": [0.01, -0.01, 0.02],
        }
    )

    summary_df = _build_oos_portfolio_summary_df(
        entry_cohort_df,
        pd.DataFrame(),
    )

    assert set(summary_df["sample_key"]) == {
        "discovery_2016_2020",
        "validation_2021_2023",
        "oos_2024_forward",
    }
    oos_row = summary_df.loc[summary_df["sample_key"] == "oos_2024_forward"].iloc[0]
    assert float(oos_row["mean_excess_return"]) == pytest.approx(0.02)


def test_accumulation_features_are_pit_stable_against_future_price_edits() -> None:
    raw_df = _build_raw_panel()
    baseline_df = _prepare_test_panel(raw_df)

    revised_raw_df = raw_df.copy()
    future_mask = pd.to_datetime(revised_raw_df["date"]) >= pd.Timestamp("2026-01-07")
    revised_raw_df.loc[future_mask, "close"] = (
        pd.to_numeric(revised_raw_df.loc[future_mask, "close"]) * 3.0
    )
    revised_raw_df.loc[future_mask, "open"] = (
        pd.to_numeric(revised_raw_df.loc[future_mask, "open"]) * 3.0
    )
    revised_df = _prepare_test_panel(revised_raw_df)

    feature_columns = [
        "date",
        "code",
        "cmf",
        "chaikin_oscillator",
        "obv_flow_score",
        "accumulation_vote_count",
        "price_not_extended",
        "lower_wick_ratio",
    ]
    baseline_features = baseline_df.loc[
        baseline_df["date"] <= pd.Timestamp("2026-01-06"),
        feature_columns,
    ].reset_index(drop=True)
    revised_features = revised_df.loc[
        revised_df["date"] <= pd.Timestamp("2026-01-06"),
        feature_columns,
    ].reset_index(drop=True)

    pd.testing.assert_frame_equal(
        baseline_features,
        revised_features,
        check_exact=False,
        atol=1e-12,
    )


def test_run_write_and_load_research_bundle(tmp_path) -> None:
    db_path = tmp_path / "market.duckdb"
    _create_research_duckdb(str(db_path))

    result = run_accumulation_flow_followthrough_research(
        str(db_path),
        start_date="2026-01-10",
        end_date="2026-03-10",
        horizons=(2, 5),
        cmf_period=2,
        chaikin_fast_period=2,
        chaikin_slow_period=3,
        obv_lookback_period=2,
        min_votes=2,
        price_sma_period=3,
        price_high_lookback_period=3,
        max_close_to_high=0.0,
        concentration_caps=(1, 2),
    )

    assert result.event_df["universe_key"].nunique() == 4
    assert not result.event_summary_df.empty
    assert not result.capped_cohort_portfolio_summary_df.empty
    assert not result.oos_portfolio_summary_df.empty

    output_root = tmp_path / "research"
    bundle = write_accumulation_flow_followthrough_research_bundle(
        result,
        output_root=output_root,
        run_id="unit-run",
        notes="unit coverage",
    )
    loaded = load_accumulation_flow_followthrough_research_bundle(bundle.bundle_dir)
    latest_path = get_accumulation_flow_followthrough_latest_bundle_path(
        output_root=output_root,
    )
    run_path = get_accumulation_flow_followthrough_bundle_path_for_run_id(
        "unit-run",
        output_root=output_root,
    )

    assert loaded.analysis_start_date == "2026-01-10"
    assert latest_path == bundle.bundle_dir
    assert run_path == bundle.bundle_dir
    assert "OOS 2024-Forward Portfolio Lens" in bundle.summary_path.read_text()
