from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.topix100_open_close_volume_ratio_conditioning import (
    TOPIX100_OPEN_CLOSE_VOLUME_RATIO_CONDITIONING_EXPERIMENT_ID,
    get_topix100_open_close_volume_ratio_conditioning_bundle_path_for_run_id,
    get_topix100_open_close_volume_ratio_conditioning_latest_bundle_path,
    load_topix100_open_close_volume_ratio_conditioning_research_bundle,
    run_topix100_open_close_volume_ratio_conditioning_research,
    write_topix100_open_close_volume_ratio_conditioning_research_bundle,
)


def _create_tables(conn: duckdb.DuckDBPyConnection) -> None:
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
        CREATE TABLE stock_data_minute_raw (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            turnover_value DOUBLE,
            created_at TEXT,
            PRIMARY KEY (code, date, time)
        )
        """
    )


def _insert_stocks(conn: duckdb.DuckDBPyConnection) -> None:
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "1111",
                "Low Ratio",
                "LOW RATIO",
                "0111",
                "Prime",
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
                "Mid Ratio 1",
                "MID RATIO 1",
                "0111",
                "Prime",
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
                "Mid Ratio 2",
                "MID RATIO 2",
                "0111",
                "Prime",
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
                "High Ratio",
                "HIGH RATIO",
                "0111",
                "Prime",
                "1",
                "A",
                "1",
                "A",
                "TOPIX Large70",
                "2000-01-01",
                None,
                None,
            ),
        ],
    )


def _build_session_rows(
    *,
    code: str,
    date: str,
    day_open: float,
    day_close: float,
    opening_volume: int,
    closing_volume: int,
) -> list[tuple[str, str, str, float, float, float, float, int, float, None]]:
    return [
        (
            code,
            date,
            "09:00",
            day_open,
            day_open,
            day_open,
            day_open,
            opening_volume,
            float(opening_volume) * day_open,
            None,
        ),
        (
            code,
            date,
            "15:30",
            day_close,
            day_close,
            day_close,
            day_close,
            closing_volume,
            float(closing_volume) * day_close,
            None,
        ),
    ]


def _build_market_db(db_path: Path) -> str:
    conn = duckdb.connect(str(db_path))
    _create_tables(conn)
    _insert_stocks(conn)

    profile_map = {
        "1111": {
            "dates": {
                "2024-01-05": (100.0, 98.0, 1000, 200),
                "2024-01-08": (97.0, 96.0, 800, 180),
                "2024-07-05": (100.0, 98.5, 900, 190),
                "2024-07-08": (97.5, 96.5, 700, 170),
            },
        },
        "2222": {
            "dates": {
                "2024-01-05": (100.0, 99.5, 1000, 600),
                "2024-01-08": (99.7, 100.2, 1100, 650),
                "2024-07-05": (100.0, 99.8, 1000, 620),
                "2024-07-08": (100.1, 100.4, 1050, 660),
            },
        },
        "3333": {
            "dates": {
                "2024-01-05": (100.0, 100.5, 1000, 1400),
                "2024-01-08": (100.8, 101.2, 1300, 1600),
                "2024-07-05": (100.0, 100.7, 1100, 1500),
                "2024-07-08": (100.9, 101.4, 1400, 1700),
            },
        },
        "4444": {
            "dates": {
                "2024-01-05": (100.0, 102.0, 1000, 2200),
                "2024-01-08": (103.0, 104.0, 1800, 2600),
                "2024-07-05": (100.0, 102.2, 1200, 2400),
                "2024-07-08": (103.2, 104.4, 2000, 2800),
            },
        },
    }

    rows: list[tuple[str, str, str, float, float, float, float, int, float, None]] = []
    for code, profile in profile_map.items():
        for date, (day_open, day_close, opening_volume, closing_volume) in profile["dates"].items():
            rows.extend(
                _build_session_rows(
                    code=code,
                    date=date,
                    day_open=day_open,
                    day_close=day_close,
                    opening_volume=int(opening_volume),
                    closing_volume=int(closing_volume),
                )
            )

    conn.executemany(
        "INSERT INTO stock_data_minute_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.close()
    return str(db_path)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return _build_market_db(tmp_path / "market.duckdb")


def test_run_research_builds_half_year_periods_and_positive_high_minus_low_spreads(
    analytics_db_path: str,
) -> None:
    result = run_topix100_open_close_volume_ratio_conditioning_research(
        analytics_db_path,
        interval_minutes_list=[5, 15, 30],
        bucket_count=4,
        period_months=6,
    )

    assert result.analysis_start_date == "2024-01-05"
    assert result.analysis_end_date == "2024-07-08"
    assert result.ratio_mode == "same_session_close_vs_open"
    assert result.topix100_constituent_count == 4
    assert result.total_session_count == 16
    assert len(result.periods_df) == 2
    assert result.periods_df["period_start_date"].tolist() == ["2024-01-05", "2024-07-05"]
    assert set(result.session_level_df["interval_minutes"].astype(int)) == {5, 15, 30}
    assert set(result.period_bucket_summary_df["ratio_bucket_label"]) == {"Q1", "Q2", "Q3", "Q4"}

    for row in result.period_interval_summary_df.itertuples(index=False):
        assert row.same_day_intraday_mean_spread_high_minus_low > 0
        assert row.next_session_intraday_mean_spread_high_minus_low > 0
        assert row.close_to_next_close_mean_spread_high_minus_low is not None

    interval_row = result.interval_summary_df.loc[
        result.interval_summary_df["interval_minutes"] == 5
    ].iloc[0]
    assert interval_row["high_ratio_bucket_label"] == "Q4"
    assert interval_row["low_ratio_bucket_label"] == "Q1"
    assert interval_row["same_day_intraday_mean_spread_high_minus_low"] > 0
    assert interval_row["next_session_intraday_mean_spread_high_minus_low"] > 0
    assert interval_row["overnight_mean_spread_high_minus_low"] is not None


def test_run_research_supports_previous_close_vs_open_mode(
    analytics_db_path: str,
) -> None:
    result = run_topix100_open_close_volume_ratio_conditioning_research(
        analytics_db_path,
        interval_minutes_list=[5],
        ratio_mode="previous_close_vs_open",
        bucket_count=4,
        period_months=6,
    )

    assert result.ratio_mode == "previous_close_vs_open"
    assert result.total_session_count == 12
    assert set(result.session_level_df["reference_date"].astype(str)) >= {"2024-01-05"}
    assert set(result.session_level_df["comparison_bucket_time"].astype(str)) == {"09:00"}

    interval_row = result.interval_summary_df.iloc[0]
    assert interval_row["high_ratio_bucket_label"] == "Q4"
    assert interval_row["low_ratio_bucket_label"] == "Q1"
    assert interval_row["ratio_mean_spread_high_minus_low"] > 0
    assert interval_row["same_day_intraday_mean_spread_high_minus_low"] is not None


def test_run_research_supports_previous_open_vs_open_mode(
    analytics_db_path: str,
) -> None:
    result = run_topix100_open_close_volume_ratio_conditioning_research(
        analytics_db_path,
        interval_minutes_list=[5],
        ratio_mode="previous_open_vs_open",
        bucket_count=4,
        period_months=6,
    )

    assert result.ratio_mode == "previous_open_vs_open"
    assert result.total_session_count == 12
    assert set(result.session_level_df["reference_bucket_time"].astype(str)) == {"09:00"}
    assert set(result.session_level_df["comparison_bucket_time"].astype(str)) == {"09:00"}

    interval_row = result.interval_summary_df.iloc[0]
    assert interval_row["high_ratio_bucket_label"] == "Q4"
    assert interval_row["low_ratio_bucket_label"] == "Q1"
    assert interval_row["ratio_mean_spread_high_minus_low"] > 0
    assert interval_row["next_session_intraday_mean_spread_high_minus_low"] is not None


def test_run_research_supports_previous_close_vs_close_mode_and_disables_same_day_metrics(
    analytics_db_path: str,
) -> None:
    result = run_topix100_open_close_volume_ratio_conditioning_research(
        analytics_db_path,
        interval_minutes_list=[5],
        ratio_mode="previous_close_vs_close",
        bucket_count=4,
        period_months=6,
    )

    assert result.ratio_mode == "previous_close_vs_close"
    assert result.total_session_count == 12
    assert set(result.session_level_df["reference_bucket_time"].astype(str)) == {"15:30"}
    assert set(result.session_level_df["comparison_bucket_time"].astype(str)) == {"15:30"}
    assert result.session_level_df["same_day_intraday_return"].isna().all()
    assert result.session_level_df["close_to_next_close_return"].isna().all()

    interval_row = result.interval_summary_df.iloc[0]
    assert interval_row["high_ratio_bucket_label"] == "Q4"
    assert interval_row["low_ratio_bucket_label"] == "Q1"
    assert pd.isna(interval_row["same_day_intraday_mean_spread_high_minus_low"])
    assert pd.isna(interval_row["close_to_next_close_mean_spread_high_minus_low"])
    assert interval_row["overnight_mean_spread_high_minus_low"] is not None
    assert interval_row["next_session_intraday_mean_spread_high_minus_low"] is not None


def test_research_bundle_roundtrip(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_topix100_open_close_volume_ratio_conditioning_research(
        analytics_db_path,
        interval_minutes_list=[5],
        bucket_count=4,
        period_months=6,
    )

    bundle = write_topix100_open_close_volume_ratio_conditioning_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260418_120000_testabcd",
    )
    reloaded = load_topix100_open_close_volume_ratio_conditioning_research_bundle(
        bundle.bundle_dir
    )

    assert bundle.experiment_id == TOPIX100_OPEN_CLOSE_VOLUME_RATIO_CONDITIONING_EXPERIMENT_ID
    assert bundle.run_id == "20260418_120000_testabcd"
    assert reloaded.ratio_mode == "same_session_close_vs_open"
    pd.testing.assert_frame_equal(reloaded.periods_df, result.periods_df)
    pd.testing.assert_frame_equal(reloaded.session_level_df, result.session_level_df)
    pd.testing.assert_frame_equal(
        reloaded.period_interval_summary_df,
        result.period_interval_summary_df,
    )
    assert (
        get_topix100_open_close_volume_ratio_conditioning_bundle_path_for_run_id(
            "20260418_120000_testabcd",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_open_close_volume_ratio_conditioning_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
