from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics import topix_close_stock_overnight_distribution as analysis_module
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    format_close_bucket_label,
    get_topix_available_date_range,
    get_topix_close_return_stats,
    run_topix_close_stock_overnight_distribution,
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
        CREATE TABLE stock_data (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            adjustment_factor DOUBLE,
            created_at TEXT,
            PRIMARY KEY (code, date)
        )
        """
    )
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

    stocks = [
        ("72030", "Topix100 Prime", "TOPIX100 PRIME", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("11110", "Prime Ex", "PRIME EX", "0111", "プライム", "1", "A", "1", "A", "-", "2000-01-01", None, None),
        ("22220", "Topix500 Mid400", "TOPIX500 MID400", "0111", "プライム", "1", "A", "1", "A", "TOPIX Mid400", "2000-01-01", None, None),
        ("33330", "Growth", "GROWTH", "0113", "グロース", "1", "A", "1", "A", "-", "2000-01-01", None, None),
    ]
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stocks,
    )

    topix_rows = [
        ("2024-01-01", 100.0, 100.5, 99.5, 100.0, None),
        ("2024-01-02", 101.0, 103.5, 100.5, 103.0, None),
        ("2024-01-03", 100.0, 100.5, 97.5, 98.0, None),
        ("2024-01-04", 97.5, 97.5, 96.0, 96.8, None),
        ("2024-01-05", 96.8, 97.5, 96.6, 97.2, None),
        ("2024-01-08", 97.8, 98.8, 97.5, 98.5, None),
        ("2024-01-09", 99.5, 101.8, 99.0, 101.5, None),
    ]
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)", topix_rows)

    stock_rows = [
        ("7203", "2024-01-02", 10.0, 10.4, 9.9, 10.0, 1000, 1.0, None),
        ("72030", "2024-01-02", 10.0, 10.5, 9.8, 9.0, 1000, 1.0, None),
        ("11110", "2024-01-02", 20.0, 20.5, 19.5, 20.0, 1000, 1.0, None),
        ("22220", "2024-01-02", 30.0, 30.5, 29.5, 30.0, 1000, 1.0, None),
        ("33330", "2024-01-02", 40.0, 40.5, 39.5, 40.0, 1000, 1.0, None),
        ("7203", "2024-01-03", 12.0, 12.6, 11.8, 12.0, 1000, 1.0, None),
        ("11110", "2024-01-03", 18.0, 18.2, 17.5, 18.0, 1000, 1.0, None),
        ("22220", "2024-01-03", 27.0, 27.5, 26.8, 27.0, 1000, 1.0, None),
        ("33330", "2024-01-03", 38.0, 38.5, 37.8, 38.0, 1000, 1.0, None),
        ("7203", "2024-01-04", 11.0, 11.1, 10.6, 11.0, 1000, 1.0, None),
        ("11110", "2024-01-04", 18.0, 18.4, 17.8, 18.0, 1000, 1.0, None),
        ("22220", "2024-01-04", 26.0, 26.2, 25.5, 26.0, 1000, 1.0, None),
        ("33330", "2024-01-04", 38.0, 38.4, 37.5, 38.0, 1000, 1.0, None),
        ("7203", "2024-01-05", 11.2, 11.4, 10.8, 11.0, 1000, 1.0, None),
        ("11110", "2024-01-05", 19.0, 19.3, 18.8, 19.0, 1000, 1.0, None),
        ("22220", "2024-01-05", 26.5, 26.9, 26.2, 26.5, 1000, 1.0, None),
        ("33330", "2024-01-05", 39.0, 39.2, 38.7, 39.0, 1000, 1.0, None),
        ("7203", "2024-01-08", 11.0, 11.5, 10.9, 11.0, 1000, 1.0, None),
        ("11110", "2024-01-08", 20.0, 20.4, 19.8, 20.0, 1000, 1.0, None),
        ("22220", "2024-01-08", 27.0, 27.2, 26.8, 27.0, 1000, 1.0, None),
        ("33330", "2024-01-08", 40.0, 40.5, 39.7, 40.0, 1000, 1.0, None),
        ("7203", "2024-01-09", 12.0, 12.4, 11.7, 12.0, 1000, 1.0, None),
        ("11110", "2024-01-09", 21.0, 21.4, 20.7, 21.0, 1000, 1.0, None),
        ("22220", "2024-01-09", 26.0, 26.4, 25.8, 26.0, 1000, 1.0, None),
        ("33330", "2024-01-09", 39.0, 39.2, 38.5, 39.0, 1000, 1.0, None),
    ]
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.close()
    return str(db_path)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return _build_market_db(tmp_path / "market.duckdb")


def _summary_row(result, stock_group: str, bucket_key: str) -> pd.Series:
    row = result.summary_df[
        (result.summary_df["stock_group"] == stock_group)
        & (result.summary_df["close_bucket_key"] == bucket_key)
    ]
    assert len(row) == 1
    return row.iloc[0]


def _daily_group_row(result, stock_group: str, date: str) -> pd.Series:
    row = result.daily_group_returns_df[
        (result.daily_group_returns_df["stock_group"] == stock_group)
        & (result.daily_group_returns_df["date"] == date)
    ]
    assert len(row) == 1
    return row.iloc[0]


def test_day_counts_and_topix_exclusions_are_returned(analytics_db_path: str) -> None:
    result = run_topix_close_stock_overnight_distribution(
        analytics_db_path,
        sample_size=10,
    )

    day_counts = dict(
        zip(
            result.day_counts_df["close_bucket_key"],
            result.day_counts_df["day_count"],
            strict=True,
        )
    )
    assert day_counts == {
        "close_le_negative_threshold_2": 1,
        "close_negative_threshold_2_to_1": 1,
        "close_negative_threshold_1_to_threshold_1": 1,
        "close_threshold_1_to_2": 1,
        "close_ge_threshold_2": 1,
    }
    assert result.excluded_topix_days_without_prev_close == 1
    assert result.excluded_topix_days_without_next_session == 1
    assert result.available_start_date == "2024-01-01"
    assert result.available_end_date == "2024-01-09"
    assert result.analysis_start_date == "2024-01-02"
    assert result.analysis_end_date == "2024-01-08"


def test_group_classification_and_4digit_preference_are_applied(
    analytics_db_path: str,
) -> None:
    result = run_topix_close_stock_overnight_distribution(
        analytics_db_path,
        sample_size=10,
    )

    prime_large_up = _summary_row(result, "PRIME", "close_ge_threshold_2")
    prime_large_down = _summary_row(result, "PRIME", "close_le_negative_threshold_2")
    topix100_large_up = _summary_row(result, "TOPIX100", "close_ge_threshold_2")
    topix500_small_up = _summary_row(result, "TOPIX500", "close_threshold_1_to_2")
    prime_ex_mid_down = _summary_row(
        result,
        "PRIME ex TOPIX500",
        "close_negative_threshold_2_to_1",
    )

    assert prime_large_up["sample_count"] == 3
    assert prime_large_up["up_count"] == 1
    assert prime_large_up["down_count"] == 2
    assert prime_large_up["flat_count"] == 0
    assert prime_large_up["mean_overnight_return"] == pytest.approx(0.0)

    assert prime_large_down["sample_count"] == 3
    assert prime_large_down["up_count"] == 0
    assert prime_large_down["down_count"] == 2
    assert prime_large_down["flat_count"] == 1
    assert prime_large_down["mean_overnight_return"] == pytest.approx(
        (-1.0 / 12.0 + 0.0 - 1.0 / 27.0) / 3.0
    )

    assert topix100_large_up["sample_count"] == 1
    assert topix100_large_up["mean_overnight_return"] == pytest.approx(0.2)

    assert topix500_small_up["sample_count"] == 2
    assert topix500_small_up["up_count"] == 1
    assert topix500_small_up["down_count"] == 1
    assert topix500_small_up["mean_overnight_return"] == pytest.approx(
        (1.0 / 11.0 - 1.0 / 27.0) / 2.0
    )

    assert prime_ex_mid_down["sample_count"] == 1
    assert prime_ex_mid_down["mean_overnight_return"] == pytest.approx(1.0 / 18.0)

    sample_row = result.samples_df[
        (result.samples_df["stock_group"] == "TOPIX100")
        & (result.samples_df["close_bucket_key"] == "close_ge_threshold_2")
    ]
    assert len(sample_row) == 1
    assert sample_row.iloc[0]["code"] == "7203"
    assert sample_row.iloc[0]["overnight_return"] == pytest.approx(0.2)


def test_daily_group_returns_are_returned(analytics_db_path: str) -> None:
    result = run_topix_close_stock_overnight_distribution(
        analytics_db_path,
        selected_groups=["PRIME", "TOPIX500"],
        sample_size=0,
    )

    day_1_prime = _daily_group_row(result, "PRIME", "2024-01-02")
    day_2_topix500 = _daily_group_row(result, "TOPIX500", "2024-01-03")

    assert day_1_prime["close_bucket_key"] == "close_ge_threshold_2"
    assert day_1_prime["topix_close_return"] == pytest.approx(0.03)
    assert day_1_prime["day_mean_overnight_return"] == pytest.approx(0.0)
    assert day_1_prime["constituent_count"] == 3

    assert day_2_topix500["close_bucket_key"] == "close_le_negative_threshold_2"
    assert day_2_topix500["day_mean_overnight_return"] == pytest.approx(
        (-1.0 / 12.0 - 1.0 / 27.0) / 2.0
    )
    assert day_2_topix500["day_up_ratio"] == pytest.approx(0.0)


def test_selected_date_range_filters_results(analytics_db_path: str) -> None:
    result = run_topix_close_stock_overnight_distribution(
        analytics_db_path,
        start_date="2024-01-03",
        end_date="2024-01-08",
        selected_groups=["PRIME", "TOPIX100"],
        sample_size=10,
    )

    day_counts = dict(
        zip(
            result.day_counts_df["close_bucket_key"],
            result.day_counts_df["day_count"],
            strict=True,
        )
    )
    assert day_counts == {
        "close_le_negative_threshold_2": 1,
        "close_negative_threshold_2_to_1": 1,
        "close_negative_threshold_1_to_threshold_1": 1,
        "close_threshold_1_to_2": 1,
        "close_ge_threshold_2": 0,
    }
    assert result.analysis_start_date == "2024-01-03"
    assert result.analysis_end_date == "2024-01-08"
    assert tuple(result.summary_df["stock_group"].unique()) == ("PRIME", "TOPIX100")


def test_analysis_range_is_empty_when_no_rows_are_analyzable(analytics_db_path: str) -> None:
    result = run_topix_close_stock_overnight_distribution(
        analytics_db_path,
        start_date="2024-01-01",
        end_date="2024-01-01",
        selected_groups=["PRIME"],
        sample_size=10,
    )

    assert result.analysis_start_date is None
    assert result.analysis_end_date is None
    assert result.samples_df.empty
    assert result.summary_df["sample_count"].sum() == 0
    assert result.daily_group_returns_df.empty


def test_sampling_is_deterministic(analytics_db_path: str) -> None:
    first = run_topix_close_stock_overnight_distribution(
        analytics_db_path,
        selected_groups=["PRIME"],
        sample_size=1,
    )
    second = run_topix_close_stock_overnight_distribution(
        analytics_db_path,
        selected_groups=["PRIME"],
        sample_size=1,
    )

    pdt.assert_frame_equal(
        first.samples_df.reset_index(drop=True),
        second.samples_df.reset_index(drop=True),
    )


def test_clip_bounds_and_clipped_samples_are_returned(analytics_db_path: str) -> None:
    result = run_topix_close_stock_overnight_distribution(
        analytics_db_path,
        selected_groups=["PRIME"],
        sample_size=10,
        clip_percentiles=(25.0, 75.0),
    )

    assert not result.clip_bounds_df.empty
    assert len(result.clipped_samples_df) <= len(result.samples_df)
    assert {"clip_lower", "clip_upper"} <= set(result.clip_bounds_df.columns)


def test_get_topix_available_date_range_reads_metadata(analytics_db_path: str) -> None:
    assert get_topix_available_date_range(analytics_db_path) == (
        "2024-01-01",
        "2024-01-09",
    )


def test_get_topix_close_return_stats_returns_sigma_thresholds(
    analytics_db_path: str,
) -> None:
    stats = get_topix_close_return_stats(
        analytics_db_path,
        sigma_threshold_1=1.0,
        sigma_threshold_2=2.0,
    )

    assert stats is not None
    assert stats.sample_count == 5
    assert stats.mean_return == pytest.approx(-0.0026563740555811052)
    assert stats.std_return == pytest.approx(0.029862593175283923)
    assert stats.threshold_1 == pytest.approx(0.029862593175283923)
    assert stats.threshold_2 == pytest.approx(0.059725186350567845)
    assert stats.min_return == pytest.approx(-0.04854368932038835)
    assert stats.median_return == pytest.approx(0.004132231404958678)
    assert stats.max_return == pytest.approx(0.03)


def test_get_topix_close_return_stats_returns_none_when_no_rows(
    analytics_db_path: str,
) -> None:
    stats = get_topix_close_return_stats(
        analytics_db_path,
        start_date="2024-01-01",
        end_date="2024-01-01",
    )

    assert stats is None


def test_lock_contention_falls_back_to_snapshot(
    analytics_db_path: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_connect = analysis_module._connect_duckdb
    attempts = {"count": 0}
    wal_path = Path(f"{analytics_db_path}.wal")
    wal_path.write_text("wal", encoding="utf-8")

    def flaky_connect(db_path: str, *, read_only: bool = True):
        assert read_only is True
        if db_path == analytics_db_path and attempts["count"] == 0:
            attempts["count"] += 1
            raise duckdb.IOException(
                'IO Error: Could not set lock on file "market.duckdb": Conflicting lock is held'
            )
        if db_path != analytics_db_path:
            assert Path(f"{db_path}.wal").exists()
        return original_connect(db_path, read_only=read_only)

    monkeypatch.setattr(analysis_module, "_connect_duckdb", flaky_connect)

    result = run_topix_close_stock_overnight_distribution(
        analytics_db_path,
        selected_groups=["PRIME"],
        sample_size=2,
    )

    assert result.source_mode == "snapshot"
    assert "temporary snapshot copied from" in result.source_detail


def test_lock_contention_snapshot_fallback_works_without_wal(
    analytics_db_path: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_connect = analysis_module._connect_duckdb
    attempts = {"count": 0}

    def flaky_connect(db_path: str, *, read_only: bool = True):
        assert read_only is True
        if db_path == analytics_db_path and attempts["count"] == 0:
            attempts["count"] += 1
            raise duckdb.IOException(
                'IO Error: Could not set lock on file "market.duckdb": Conflicting lock is held'
            )
        if db_path != analytics_db_path:
            assert not Path(f"{db_path}.wal").exists()
        return original_connect(db_path, read_only=read_only)

    monkeypatch.setattr(analysis_module, "_connect_duckdb", flaky_connect)

    result = run_topix_close_stock_overnight_distribution(
        analytics_db_path,
        selected_groups=["PRIME"],
        sample_size=2,
    )

    assert result.source_mode == "snapshot"
    assert "temporary snapshot copied from" in result.source_detail


def test_non_lock_connection_error_is_propagated(
    analytics_db_path: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failing_connect(db_path: str, *, read_only: bool = True):
        raise RuntimeError("boom")

    monkeypatch.setattr(analysis_module, "_connect_duckdb", failing_connect)

    with pytest.raises(RuntimeError, match="boom"):
        get_topix_available_date_range(analytics_db_path)


def test_sample_size_zero_returns_empty_samples(analytics_db_path: str) -> None:
    result = run_topix_close_stock_overnight_distribution(
        analytics_db_path,
        selected_groups=["PRIME"],
        sample_size=0,
    )

    assert result.samples_df.empty
    assert result.clipped_samples_df.empty
    assert result.clip_bounds_df.empty
    assert "close_bucket_label" in result.samples_df.columns
    assert "overnight_return" in result.samples_df.columns


def test_selected_groups_are_deduplicated(analytics_db_path: str) -> None:
    result = run_topix_close_stock_overnight_distribution(
        analytics_db_path,
        selected_groups=["PRIME", "PRIME", "TOPIX100"],
        sample_size=1,
    )

    assert result.selected_groups == ("PRIME", "TOPIX100")


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"selected_groups": []}, "selected_groups must contain at least one supported group"),
        ({"selected_groups": ["INVALID"]}, "Unsupported stock group"),
        ({"close_threshold_1": 0.0}, "close_threshold_1 must be positive"),
        (
            {"close_threshold_1": 0.02, "close_threshold_2": 0.02},
            "close_threshold_2 must be greater than close_threshold_1",
        ),
        ({"sample_size": -1}, "sample_size must be non-negative"),
        (
            {"clip_percentiles": (99.0, 99.0)},
            "clip_percentiles must satisfy 0 <= lower < upper <= 100",
        ),
        ({"sigma_threshold_1": 0.0}, "sigma_threshold_1 must be positive"),
        (
            {"sigma_threshold_1": 2.0, "sigma_threshold_2": 2.0},
            "sigma_threshold_2 must be greater than sigma_threshold_1",
        ),
    ],
)
def test_invalid_inputs_raise(
    analytics_db_path: str,
    kwargs: dict[str, object],
    message: str,
) -> None:
    target = run_topix_close_stock_overnight_distribution
    if "sigma_threshold_1" in kwargs or "sigma_threshold_2" in kwargs:
        target = get_topix_close_return_stats

    with pytest.raises(ValueError, match=message):
        target(
            analytics_db_path,
            **cast(dict[str, Any], kwargs),
        )


def test_close_bucket_labels_support_fractional_thresholds() -> None:
    assert (
        format_close_bucket_label(
            "close_threshold_1_to_2",
            close_threshold_1=0.015,
            close_threshold_2=0.0275,
        )
        == "1.5% <= close < 2.75%"
    )
    assert (
        format_close_bucket_label(
            "close_negative_threshold_2_to_1",
            close_threshold_1=0.015,
            close_threshold_2=0.0275,
        )
        == "-2.75% < close <= -1.5%"
    )
