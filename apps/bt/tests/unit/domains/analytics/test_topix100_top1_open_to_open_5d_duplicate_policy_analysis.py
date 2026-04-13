from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import duckdb
import pandas as pd
import pandas.testing as pdt

from src.domains.analytics.topix100_top1_open_to_open_5d_duplicate_policy_analysis import (
    TOPIX100_TOP1_OPEN_TO_OPEN_5D_DUPLICATE_POLICY_ANALYSIS_EXPERIMENT_ID,
    _build_selected_trade_df_for_policy,
    _build_committee_id,
    get_topix100_top1_open_to_open_5d_duplicate_policy_analysis_bundle_path_for_run_id,
    get_topix100_top1_open_to_open_5d_duplicate_policy_analysis_latest_bundle_path,
    load_topix100_top1_open_to_open_5d_duplicate_policy_analysis_bundle,
    run_topix100_top1_open_to_open_5d_duplicate_policy_analysis,
    write_topix100_top1_open_to_open_5d_duplicate_policy_analysis_bundle,
)


def _build_candidate_pick_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "model_name": ["lightgbm"] * 6,
            "top_k": [5] * 6,
            "signal_date": [
                "2024-01-01",
                "2024-01-01",
                "2024-01-02",
                "2024-01-02",
                "2024-01-03",
                "2024-01-03",
            ],
            "date": [
                "2024-01-01",
                "2024-01-01",
                "2024-01-02",
                "2024-01-02",
                "2024-01-03",
                "2024-01-03",
            ],
            "entry_date": [
                "2024-01-02",
                "2024-01-02",
                "2024-01-03",
                "2024-01-03",
                "2024-01-04",
                "2024-01-04",
            ],
            "exit_date": [
                "2024-01-09",
                "2024-01-09",
                "2024-01-10",
                "2024-01-10",
                "2024-01-11",
                "2024-01-11",
            ],
            "swing_entry_date": [
                "2024-01-02",
                "2024-01-02",
                "2024-01-03",
                "2024-01-03",
                "2024-01-04",
                "2024-01-04",
            ],
            "swing_exit_date": [
                "2024-01-09",
                "2024-01-09",
                "2024-01-10",
                "2024-01-10",
                "2024-01-11",
                "2024-01-11",
            ],
            "selection_rank": [1, 2, 1, 2, 1, 2],
            "code": ["1301", "1302", "1301", "1303", "1304", "1301"],
            "company_name": ["A", "B", "A", "C", "D", "A"],
            "score": [0.9, 0.8, 0.95, 0.7, 0.85, 0.6],
            "realized_return": [0.10, 0.08, 0.12, 0.05, 0.06, 0.03],
            "source_realized_return": [0.10, 0.08, 0.12, 0.05, 0.06, 0.03],
        }
    )


def _build_market_db(db_path: Path, *, periods: int = 40) -> str:
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
    date_strings = [date.strftime("%Y-%m-%d") for date in dates]
    topix_rows: list[tuple[str, float, float, float, float, float, None]] = []
    topix_open = 100.0
    for index, date_string in enumerate(date_strings):
        if index > 0:
            topix_open *= 1.0 + 0.002
        topix_close = topix_open * 1.001
        topix_rows.append(
            (
                date_string,
                topix_open,
                topix_close * 1.002,
                topix_open * 0.998,
                topix_close,
                1_000_000.0,
                None,
            )
        )
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?, ?)", topix_rows)
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (code, code, None, "0111", "Prime", None, None, None, None, "TOPIX Core30", "2020-01-01", None, None)
            for code in ("1301", "1302", "1303", "1304")
        ],
    )
    stock_rows: list[tuple[str, str, float, float, float, float, float, None]] = []
    for code, start_open, ret in (
        ("1301", 100.0, 0.015),
        ("1302", 110.0, 0.010),
        ("1303", 120.0, 0.008),
        ("1304", 130.0, 0.006),
    ):
        open_price = start_open
        for index, date_string in enumerate(date_strings):
            if index > 0:
                open_price *= 1.0 + ret
            close_price = open_price * 1.001
            stock_rows.append(
                (
                    code,
                    date_string,
                    open_price,
                    close_price * 1.002,
                    open_price * 0.998,
                    close_price,
                    100_000.0 + index,
                    None,
                )
            )
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.close()
    return str(db_path)


def test_build_selected_trade_df_for_policy_distinguishes_stack_skip_and_alternative() -> None:
    candidate_pick_df = _build_candidate_pick_df()

    allow_events, allow_trades = _build_selected_trade_df_for_policy(
        candidate_pick_df,
        policy="allow_stack",
    )
    skip_events, skip_trades = _build_selected_trade_df_for_policy(
        candidate_pick_df,
        policy="skip_if_held",
    )
    unique_events, unique_trades = _build_selected_trade_df_for_policy(
        candidate_pick_df,
        policy="next_unique_within_top5",
    )

    assert allow_trades["code"].tolist() == ["1301", "1301", "1304"]
    assert allow_events["action"].tolist() == [
        "selected_top1",
        "selected_top1_duplicate",
        "selected_top1",
    ]
    assert skip_trades["code"].tolist() == ["1301", "1304"]
    assert skip_events["action"].tolist() == [
        "selected_top1",
        "skipped_duplicate_top1",
        "selected_top1",
    ]
    assert unique_trades["code"].tolist() == ["1301", "1303", "1304"]
    assert unique_events["action"].tolist() == [
        "selected_top1",
        "selected_alternative",
        "selected_top1",
    ]


def test_duplicate_policy_analysis_bundle_roundtrip(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _build_market_db(tmp_path / "market-top1-duplicate-analysis.duckdb")
    candidate_pick_df = _build_candidate_pick_df()
    candidate_id = _build_committee_id(
        low_threshold=0.22,
        trend_vote_threshold=1,
        breadth_vote_threshold=3,
        confirmation_mode="stress_and_trend_and_breadth",
        reduced_exposure_ratio=0.0,
        mean_window_days=(1, 2),
        high_thresholds=(0.24, 0.25),
    )
    committee_daily_df = pd.DataFrame(
        {
            "candidate_id": [candidate_id] * 30,
            "signal_date": pd.bdate_range("2024-01-01", periods=30).strftime("%Y-%m-%d"),
            "realized_date": pd.bdate_range("2024-01-02", periods=30).strftime("%Y-%m-%d"),
            "sample_split": ["validation"] * 30,
            "realized_overnight_return": [0.0] * 30,
            "realized_intraday_return": [0.0] * 30,
            "baseline_return": [0.0] * 30,
            "member_average_exposure_before_rebalance": [1.0] * 30,
            "member_average_target_exposure_ratio": [1.0] * 30,
            "member_reduced_count": [0] * 30,
            "member_reduced_rate": [0.0] * 30,
            "exposure_ratio_before_rebalance": [1.0] * 30,
            "target_exposure_ratio": [1.0] * 30,
            "exposure_change": [0.0] * 30,
            "rebalanced": [False] * 30,
            "strategy_return": [0.0] * 30,
            "signal_state": ["all_full"] * 30,
        }
    )

    monkeypatch.setattr(
        "src.domains.analytics.topix100_top1_open_to_open_5d_duplicate_policy_analysis.load_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward_research_bundle",
        lambda _path: SimpleNamespace(walkforward_topk_pick_df=candidate_pick_df),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_top1_open_to_open_5d_duplicate_policy_analysis.load_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research_bundle",
        lambda _path: SimpleNamespace(
            committee_candidate_metrics_df=pd.DataFrame({"candidate_id": [candidate_id]})
        ),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_top1_open_to_open_5d_duplicate_policy_analysis._build_fixed_committee_daily_df",
        lambda **kwargs: committee_daily_df,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_top1_open_to_open_5d_duplicate_policy_analysis.load_research_bundle_info",
        lambda path: SimpleNamespace(
            run_id=(
                "top1-source-run"
                if "top1" in str(path)
                else "committee-source-run"
            )
        ),
    )

    result = run_topix100_top1_open_to_open_5d_duplicate_policy_analysis(
        db_path,
        top1_bundle_path=tmp_path / "top1-source",
        committee_bundle_path=tmp_path / "committee-source",
    )

    assert set(result.duplicate_policies) == {
        "allow_stack",
        "skip_if_held",
        "next_unique_within_top5",
    }
    assert not result.policy_portfolio_stats_df.empty
    assert not result.policy_concentration_summary_df.empty

    bundle = write_topix100_top1_open_to_open_5d_duplicate_policy_analysis_bundle(
        result,
        output_root=tmp_path,
        run_id="20260413_170000_testabcd",
    )
    reloaded = load_topix100_top1_open_to_open_5d_duplicate_policy_analysis_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == TOPIX100_TOP1_OPEN_TO_OPEN_5D_DUPLICATE_POLICY_ANALYSIS_EXPERIMENT_ID
    )
    assert (
        get_topix100_top1_open_to_open_5d_duplicate_policy_analysis_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_top1_open_to_open_5d_duplicate_policy_analysis_latest_bundle_path(
            output_root=tmp_path
        )
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.policy_concentration_summary_df,
        result.policy_concentration_summary_df,
        check_dtype=False,
    )
