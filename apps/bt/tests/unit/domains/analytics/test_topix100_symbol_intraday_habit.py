from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.topix100_symbol_intraday_habit import (
    TOPIX100_SYMBOL_INTRADAY_HABIT_EXPERIMENT_ID,
    TOPIX100_SYMBOL_INTRADAY_HABIT_OVERLAY_PLOT_FILENAME,
    TOPIX100_SYMBOL_INTRADAY_HABIT_OVERVIEW_PLOT_FILENAME,
    get_topix100_symbol_intraday_habit_bundle_path_for_run_id,
    get_topix100_symbol_intraday_habit_latest_bundle_path,
    load_topix100_symbol_intraday_habit_research_bundle,
    run_topix100_symbol_intraday_habit_research,
    write_topix100_symbol_intraday_habit_research_bundle,
)


def _build_market_db(db_path: Path) -> str:
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
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "68570",
                "Advantest",
                "ADVANTEST",
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
                "72030",
                "Toyota",
                "TOYOTA",
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
                "67580",
                "Sony",
                "SONY",
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
                "80350",
                "Tokyo Electron",
                "TOKYO ELECTRON",
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
                "99830",
                "Fast Retailing",
                "FAST RETAILING",
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
                "11110",
                "Non Topix100",
                "NON TOPIX100",
                "0111",
                "Prime",
                "1",
                "A",
                "1",
                "A",
                "TOPIX Mid400",
                "2000-01-01",
                None,
                None,
            ),
        ],
    )

    period_dates = (
        "2024-04-15",
        "2024-10-15",
        "2025-04-15",
        "2025-10-15",
    )
    symbol_profiles = {
        "68570": (100.0, (1.00, 1.015, 1.025, 0.990, 0.995)),
        "72030": (200.0, (1.00, 1.008, 1.010, 0.998, 1.002)),
        "67580": (300.0, (1.00, 0.998, 1.004, 0.996, 1.001)),
        "80350": (400.0, (1.00, 1.012, 1.018, 1.005, 1.010)),
        "99830": (500.0, (1.00, 0.995, 1.002, 0.992, 0.999)),
    }
    minute_points = ("09:00", "09:30", "10:30", "13:30", "15:00")
    rows: list[tuple[object, ...]] = []
    for date_index, date_value in enumerate(period_dates):
        for code, (base_price, multipliers) in symbol_profiles.items():
            price_shift = 1.0 + date_index * 0.001
            previous_close = None
            for time_value, multiplier in zip(minute_points, multipliers, strict=True):
                close_price = base_price * price_shift * multiplier
                open_price = close_price if previous_close is None else previous_close
                high_price = max(open_price, close_price) * 1.001
                low_price = min(open_price, close_price) * 0.999
                rows.append(
                    (
                        code,
                        date_value,
                        time_value,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        1000 + date_index * 10,
                        close_price * (1000 + date_index * 10),
                        None,
                    )
                )
                previous_close = close_price

    rows.append(
        (
            "11110",
            "2024-04-15",
            "09:00",
            100.0,
            101.0,
            99.0,
            100.0,
            1000,
            100000.0,
            None,
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


def test_run_research_samples_advantest_and_four_half_year_periods(
    analytics_db_path: str,
) -> None:
    result = run_topix100_symbol_intraday_habit_research(
        analytics_db_path,
        interval_minutes=30,
        sample_seed=42,
    )

    assert result.analysis_start_date == "2024-04-15"
    assert result.analysis_end_date == "2025-10-15"
    assert result.interval_minutes == 30
    assert result.topix100_constituent_count == 5
    assert result.total_session_count == 20
    assert tuple(result.periods_df["period_index"]) == (1, 2, 3, 4)
    assert result.sampled_symbols_df.iloc[0]["code"] == "6857"
    assert set(result.sampled_symbols_df["code"]) == {
        "6857",
        "6758",
        "7203",
        "8035",
        "9983",
    }

    advantest_habit = result.habit_summary_df[
        (result.habit_summary_df["code"] == "6857")
        & (result.habit_summary_df["bucket_time"] == "10:30")
    ].iloc[0]
    assert advantest_habit["dominant_direction"] == "positive"
    assert advantest_habit["sign_consistency"] == pytest.approx(1.0)
    assert bool(advantest_habit["is_persistent_sign"]) is True


def test_research_bundle_roundtrip(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_topix100_symbol_intraday_habit_research(
        analytics_db_path,
        interval_minutes=30,
        sample_seed=42,
    )

    bundle = write_topix100_symbol_intraday_habit_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260415_150000_testabcd",
    )
    reloaded = load_topix100_symbol_intraday_habit_research_bundle(
        bundle.bundle_dir
    )

    assert bundle.experiment_id == TOPIX100_SYMBOL_INTRADAY_HABIT_EXPERIMENT_ID
    assert (
        get_topix100_symbol_intraday_habit_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_symbol_intraday_habit_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        bundle.bundle_dir / TOPIX100_SYMBOL_INTRADAY_HABIT_OVERVIEW_PLOT_FILENAME
    ).exists()
    assert (
        bundle.bundle_dir / TOPIX100_SYMBOL_INTRADAY_HABIT_OVERLAY_PLOT_FILENAME
    ).exists()
    assert reloaded.sample_seed == result.sample_seed
    assert reloaded.interval_minutes == result.interval_minutes
    pd.testing.assert_frame_equal(
        reloaded.habit_summary_df,
        result.habit_summary_df,
        check_dtype=False,
    )
