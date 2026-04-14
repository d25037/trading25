from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics.topix100_streak_353_transfer import (
    TOPIX100_STREAK_353_TRANSFER_RESEARCH_EXPERIMENT_ID,
    _build_published_summary_payload,
    build_topix100_streak_daily_state_panel_df,
    build_topix100_streak_state_snapshot_df,
    get_topix100_streak_353_transfer_bundle_path_for_run_id,
    get_topix100_streak_353_transfer_latest_bundle_path,
    load_topix100_streak_353_transfer_research_bundle,
    run_topix100_streak_353_transfer_research,
    write_topix100_streak_353_transfer_research_bundle,
)


def _build_multi_timeframe_returns() -> list[float]:
    pattern = [
        0.03,
        0.07,
        0.025,
        0.06,
        0.04,
        0.08,
        0.015,
        0.055,
        0.045,
        0.065,
        0.02,
        0.09,
        0.035,
        0.05,
        0.028,
        0.075,
        0.018,
        0.058,
        0.038,
        0.085,
    ]
    returns: list[float] = []
    for phase_index in range(8):
        for magnitude_index, magnitude in enumerate(pattern):
            sign = 1 if (phase_index + magnitude_index) % 2 == 0 else -1
            returns.append(sign * magnitude)
    return returns


def _build_topix100_stock_transfer_db(db_path: Path) -> str:
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE stocks (
            code TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            company_name_english TEXT,
            market_code TEXT NOT NULL,
            market_name TEXT NOT NULL,
            sector_17_code TEXT NOT NULL,
            sector_17_name TEXT NOT NULL,
            sector_33_code TEXT NOT NULL,
            sector_33_name TEXT NOT NULL,
            scale_category TEXT,
            listed_date TEXT NOT NULL,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_data (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            adjustment_factor DOUBLE,
            created_at TEXT
        )
        """
    )

    stocks = [
        (
            "1111",
            "Alpha",
            "ALPHA",
            "0111",
            "プライム",
            "1",
            "A",
            "1",
            "A",
            "TOPIX Core30",
            "2000-01-01",
            None,
            None,
        ),
        (
            "2222",
            "Beta",
            "BETA",
            "0111",
            "プライム",
            "1",
            "A",
            "1",
            "A",
            "TOPIX Core30",
            "2000-01-01",
            None,
            None,
        ),
        (
            "3333",
            "Gamma",
            "GAMMA",
            "0111",
            "プライム",
            "1",
            "A",
            "1",
            "A",
            "TOPIX Large70",
            "2000-01-01",
            None,
            None,
        ),
        (
            "4444",
            "Delta",
            "DELTA",
            "0111",
            "プライム",
            "1",
            "A",
            "1",
            "A",
            "TOPIX Large70",
            "2000-01-01",
            None,
            None,
        ),
    ]
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stocks,
    )

    returns = _build_multi_timeframe_returns()
    dates = pd.bdate_range("2024-01-01", periods=len(returns) + 1)
    rows: list[tuple[str, str, float, float, float, float, int, float, None]] = []
    stock_specs = [
        ("1111", 100.0, 1.00),
        ("2222", 115.0, 0.97),
        ("3333", 130.0, 1.03),
        ("4444", 145.0, 0.94),
    ]
    for stock_index, (code, base_close, scale) in enumerate(stock_specs):
        close = base_close
        rows.append(
            (
                code,
                dates[0].strftime("%Y-%m-%d"),
                close,
                close * 1.01,
                close * 0.99,
                close,
                10_000 + (stock_index * 200),
                1.0,
                None,
            )
        )
        for day_index, (date, close_return) in enumerate(
            zip(dates[1:], returns, strict=True),
            start=1,
        ):
            adjusted_return = close_return * scale
            prev_close = close
            close = prev_close * (1.0 + adjusted_return)
            rows.append(
                (
                    code,
                    date.strftime("%Y-%m-%d"),
                    prev_close,
                    max(prev_close, close) * 1.01,
                    min(prev_close, close) * 0.99,
                    close,
                    10_000 + (stock_index * 200) + day_index,
                    1.0,
                    None,
                )
            )

    conn.executemany(
        "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.close()
    return str(db_path)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return _build_topix100_stock_transfer_db(tmp_path / "market.duckdb")


def test_topix100_streak_353_transfer_returns_state_summaries(
    analytics_db_path: str,
) -> None:
    result = run_topix100_streak_353_transfer_research(
        analytics_db_path,
        future_horizons=(1, 3),
        validation_ratio=0.25,
        min_stock_events_per_state=1,
        min_constituents_per_date_state=1,
    )

    assert result.short_window_streaks == 3
    assert result.long_window_streaks == 53
    assert result.covered_constituent_count == 4
    assert not result.state_event_df.empty
    assert not result.state_date_summary_df.empty
    assert not result.state_stock_consistency_df.empty
    full_states = result.state_event_df["state_key"]
    validation_states = result.state_date_summary_df[
        (result.state_date_summary_df["sample_split"] == "validation")
        & (result.state_date_summary_df["horizon_days"] == 1)
    ]
    assert full_states.nunique() >= 2
    assert validation_states["state_key"].nunique() >= 2


def test_topix100_streak_state_snapshot_returns_latest_state_per_stock(
    analytics_db_path: str,
) -> None:
    conn = duckdb.connect(analytics_db_path, read_only=True)
    history_df = conn.execute(
        """
        SELECT
            s.code,
            s.company_name,
            sd.date,
            sd.close
        FROM stock_data sd
        JOIN stocks s ON s.code = sd.code
        ORDER BY s.code, sd.date
        """
    ).fetchdf()
    conn.close()

    snapshot_df = build_topix100_streak_state_snapshot_df(history_df)

    assert not snapshot_df.empty
    assert "short_mode" not in snapshot_df.columns
    assert "long_mode" not in snapshot_df.columns
    assert "state_key" not in snapshot_df.columns
    assert "state_label" not in snapshot_df.columns


def test_topix100_streak_daily_state_panel_is_point_in_time_stable() -> None:
    target_date = "2026-01-06"
    base_history_df = pd.DataFrame.from_records(
        [
            {
                "code": "1111",
                "company_name": "Alpha",
                "date": "2026-01-01",
                "close": 100.0,
            },
            {
                "code": "1111",
                "company_name": "Alpha",
                "date": "2026-01-02",
                "close": 105.0,
            },
            {
                "code": "1111",
                "company_name": "Alpha",
                "date": "2026-01-05",
                "close": 100.0,
            },
            {
                "code": "1111",
                "company_name": "Alpha",
                "date": target_date,
                "close": 98.0,
            },
        ]
    )
    extended_history_df = pd.concat(
        [
            base_history_df,
            pd.DataFrame.from_records(
                [
                    {
                        "code": "1111",
                        "company_name": "Alpha",
                        "date": "2026-01-07",
                        "close": 97.0,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    base_panel_df = build_topix100_streak_daily_state_panel_df(
        base_history_df,
        analysis_end_date=target_date,
        validation_ratio=None,
        short_window_streaks=1,
        long_window_streaks=2,
    )
    extended_panel_df = build_topix100_streak_daily_state_panel_df(
        extended_history_df,
        analysis_end_date="2026-01-07",
        validation_ratio=None,
        short_window_streaks=1,
        long_window_streaks=2,
    )

    base_row = (
        base_panel_df[base_panel_df["date"] == target_date]
        .reset_index(drop=True)
        .iloc[0]
    )
    extended_row = (
        extended_panel_df[extended_panel_df["date"] == target_date]
        .reset_index(drop=True)
        .iloc[0]
    )

    assert base_row["segment_return"] == pytest.approx(98.0 / 105.0 - 1.0)
    assert extended_row["segment_return"] == pytest.approx(float(base_row["segment_return"]))
    assert extended_row["current_streak_mode"] == base_row["current_streak_mode"]
    assert extended_row["current_streak_day_count"] == base_row["current_streak_day_count"]


def test_topix100_streak_daily_state_panel_preserves_full_daily_universe(
    analytics_db_path: str,
) -> None:
    conn = duckdb.connect(analytics_db_path, read_only=True)
    history_df = conn.execute(
        """
        SELECT
            s.code,
            s.company_name,
            sd.date,
            sd.close
        FROM stock_data sd
        JOIN stocks s ON s.code = sd.code
        ORDER BY s.code, sd.date
        """
    ).fetchdf()
    conn.close()

    state_panel_df = build_topix100_streak_daily_state_panel_df(history_df)

    assert not state_panel_df.empty
    latest_date = str(state_panel_df["date"].max())
    latest_codes = set(
        state_panel_df[state_panel_df["date"] == latest_date]["code"].astype(str)
    )
    assert latest_codes == {"1111", "2222", "3333", "4444"}


def test_topix100_streak_353_transfer_bundle_roundtrip(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_topix100_streak_353_transfer_research(
        analytics_db_path,
        future_horizons=(1, 3),
        validation_ratio=0.25,
        min_stock_events_per_state=1,
        min_constituents_per_date_state=1,
    )

    bundle = write_topix100_streak_353_transfer_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260406_130000_testabcd",
    )
    reloaded = load_topix100_streak_353_transfer_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == TOPIX100_STREAK_353_TRANSFER_RESEARCH_EXPERIMENT_ID
    assert (
        get_topix100_streak_353_transfer_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_streak_353_transfer_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.state_date_summary_df,
        result.state_date_summary_df,
        check_dtype=False,
    )
    pdt.assert_frame_equal(
        reloaded.state_stock_consistency_df,
        result.state_stock_consistency_df,
        check_dtype=False,
    )


def test_published_summary_mentions_fixed_transfer_logic(
    analytics_db_path: str,
) -> None:
    result = run_topix100_streak_353_transfer_research(
        analytics_db_path,
        future_horizons=(1, 3),
        validation_ratio=0.25,
        min_stock_events_per_state=1,
        min_constituents_per_date_state=1,
    )

    summary = _build_published_summary_payload(result)

    assert summary["title"] == "TOPIX100 Streak 3/53 Transfer Study"
    assert "transfer test" in summary["resultBullets"][0].lower()
    assert summary["selectedParameters"][0]["value"] == "3 streaks"
