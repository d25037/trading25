from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics.topix100_top1_open_to_open_5d_fixed_committee_overlay import (
    DEFAULT_FIXED_COMMITTEE_LOW_THRESHOLD,
    TOPIX100_TOP1_OPEN_TO_OPEN_5D_FIXED_COMMITTEE_OVERLAY_EXPERIMENT_ID,
    _assign_trade_sleeves,
    _build_committee_id,
    _build_portfolio_daily_df,
    get_topix100_top1_open_to_open_5d_fixed_committee_overlay_bundle_path_for_run_id,
    get_topix100_top1_open_to_open_5d_fixed_committee_overlay_latest_bundle_path,
    load_topix100_top1_open_to_open_5d_fixed_committee_overlay_research_bundle,
    run_topix100_top1_open_to_open_5d_fixed_committee_overlay_research,
    write_topix100_top1_open_to_open_5d_fixed_committee_overlay_research_bundle,
)


def _build_topix_and_topix100_db(db_path: Path, *, periods: int = 260) -> tuple[str, list[str]]:
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
            topix_open *= 1.0 + (0.012 if index % 11 == 0 else 0.003)
        topix_close = topix_open * (0.985 if index % 13 == 0 else 1.004)
        topix_rows.append(
            (
                date_string,
                topix_open,
                max(topix_open, topix_close) * 1.002,
                min(topix_open, topix_close) * 0.998,
                topix_close,
                1_000_000.0,
                None,
            )
        )
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
    for code, start_open, open_to_open_return in (
        ("1301", 100.0, 0.020),
        ("1302", 120.0, 0.010),
    ):
        open_price = start_open
        for index, date_string in enumerate(date_strings):
            if index > 0:
                open_price *= 1.0 + open_to_open_return
            close_price = open_price * (1.002 if index % 7 else 0.998)
            stock_rows.append(
                (
                    code,
                    date_string,
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
    return str(db_path), date_strings


def test_assign_trade_sleeves_reuses_bucket_on_exit_open_date() -> None:
    source_df = pd.DataFrame(
        {
            "signal_date": ["2024-01-01", "2024-01-02", "2024-01-08"],
            "entry_date": ["2024-01-02", "2024-01-03", "2024-01-09"],
            "exit_date": ["2024-01-09", "2024-01-10", "2024-01-16"],
            "code": ["1301", "1302", "1301"],
            "company_name": ["Alpha", "Beta", "Alpha"],
            "score": [0.8, 0.7, 0.9],
            "source_realized_return": [0.10, 0.05, 0.12],
        }
    )

    schedule_df = _assign_trade_sleeves(source_df, sleeve_count=2)

    assert schedule_df["sleeve_id"].tolist() == [1, 2, 1]


def test_build_portfolio_daily_df_scales_open_to_open_returns_by_committee_exposure() -> None:
    trade_interval_df = pd.DataFrame(
        {
            "trade_id": [1, 2, 2],
            "sleeve_id": [1, 2, 2],
            "open_date": ["2024-01-02", "2024-01-02", "2024-01-03"],
            "next_open_date": ["2024-01-03", "2024-01-03", "2024-01-04"],
            "raw_open_to_open_return": [0.10, 0.00, 0.20],
        }
    )
    analysis_interval_df = pd.DataFrame(
        {
            "open_date": ["2024-01-02", "2024-01-03"],
            "next_open_date": ["2024-01-03", "2024-01-04"],
            "topix_open_to_open_return": [0.02, -0.01],
        }
    )
    committee_daily_df = pd.DataFrame(
        {
            "realized_date": ["2024-01-02", "2024-01-03"],
            "sample_split": ["discovery", "validation"],
            "target_exposure_ratio": [0.50, 1.00],
            "member_reduced_count": [2, 0],
            "member_reduced_rate": [0.50, 0.0],
            "signal_state": ["committee_reduced", "all_full"],
        }
    )

    portfolio_df = _build_portfolio_daily_df(
        trade_interval_df=trade_interval_df,
        analysis_interval_df=analysis_interval_df,
        committee_daily_df=committee_daily_df,
        sleeve_count=2,
    )

    assert portfolio_df["raw_portfolio_return"].tolist() == pytest.approx(
        [0.05, 0.09523809523809534]
    )
    assert portfolio_df["overlay_portfolio_return"].tolist() == pytest.approx(
        [0.025, 0.09756097560975618]
    )
    assert portfolio_df["overlay_deployed_capital_ratio"].tolist() == pytest.approx(
        [0.5, 0.48780487804878053]
    )


def test_research_bundle_roundtrip_preserves_fixed_committee_overlay_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path, date_strings = _build_topix_and_topix100_db(
        tmp_path / "market-topix100-top1-fixed-overlay.duckdb",
    )
    signal_index = 220
    pick_df = pd.DataFrame(
        {
            "model_name": ["lightgbm", "lightgbm"],
            "date": [date_strings[signal_index], date_strings[signal_index + 1]],
            "code": ["1301", "1302"],
            "company_name": ["Alpha", "Beta"],
            "score": [0.9, 0.8],
            "realized_return": [(1.02**5) - 1.0, (1.01**5) - 1.0],
            "swing_entry_date": [date_strings[signal_index + 1], date_strings[signal_index + 2]],
            "swing_exit_date": [date_strings[signal_index + 6], date_strings[signal_index + 7]],
            "selection_rank": [1, 1],
            "top_k": [1, 1],
            "split_index": [0, 0],
            "train_start": [date_strings[0], date_strings[0]],
            "train_end": [date_strings[signal_index - 1], date_strings[signal_index - 1]],
            "test_start": [date_strings[signal_index], date_strings[signal_index]],
            "test_end": [date_strings[signal_index + 10], date_strings[signal_index + 10]],
        }
    )
    candidate_id = _build_committee_id(
        low_threshold=DEFAULT_FIXED_COMMITTEE_LOW_THRESHOLD,
        trend_vote_threshold=1,
        breadth_vote_threshold=3,
        confirmation_mode="stress_and_trend_and_breadth",
        reduced_exposure_ratio=0.0,
        mean_window_days=(1, 2),
        high_thresholds=(0.24, 0.25),
    )
    committee_metrics_df = pd.DataFrame(
        {
            "candidate_id": [candidate_id],
            "sample_split": ["full"],
            "cagr": [0.10],
            "sharpe_ratio": [0.90],
            "sortino_ratio": [1.30],
            "max_drawdown": [-0.20],
        }
    )

    monkeypatch.setattr(
        "src.domains.analytics.topix100_top1_open_to_open_5d_fixed_committee_overlay.load_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward_research_bundle",
        lambda _path: SimpleNamespace(walkforward_topk_pick_df=pick_df),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_top1_open_to_open_5d_fixed_committee_overlay.load_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research_bundle",
        lambda _path: SimpleNamespace(committee_candidate_metrics_df=committee_metrics_df),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_top1_open_to_open_5d_fixed_committee_overlay.load_research_bundle_info",
        lambda path: SimpleNamespace(
            run_id=(
                "top1-source-run"
                if "top1" in str(path)
                else "committee-source-run"
            )
        ),
    )

    result = run_topix100_top1_open_to_open_5d_fixed_committee_overlay_research(
        db_path,
        top1_bundle_path=tmp_path / "top1-source",
        committee_bundle_path=tmp_path / "committee-source",
        sleeve_count=5,
        min_constituents_per_day=2,
    )

    assert result.top1_bundle_run_id == "top1-source-run"
    assert result.committee_bundle_run_id == "committee-source-run"
    assert result.committee_candidate_id == candidate_id
    assert not result.trade_integrity_df.empty
    assert result.trade_integrity_df["complete"].all()
    assert not result.portfolio_daily_df.empty
    assert set(result.portfolio_stats_df["series_name"]) == {
        "top1_raw",
        "top1_fixed_committee_overlay",
        "topix_open_to_open_hold",
    }

    bundle = write_topix100_top1_open_to_open_5d_fixed_committee_overlay_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260413_160000_testabcd",
    )
    reloaded = load_topix100_top1_open_to_open_5d_fixed_committee_overlay_research_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == TOPIX100_TOP1_OPEN_TO_OPEN_5D_FIXED_COMMITTEE_OVERLAY_EXPERIMENT_ID
    )
    assert bundle.summary_path.exists()
    assert (
        get_topix100_top1_open_to_open_5d_fixed_committee_overlay_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_top1_open_to_open_5d_fixed_committee_overlay_latest_bundle_path(
            output_root=tmp_path
        )
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.portfolio_stats_df,
        result.portfolio_stats_df,
        check_dtype=False,
    )
