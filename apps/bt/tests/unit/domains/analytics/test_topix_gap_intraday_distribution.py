from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics import topix_gap_intraday_distribution as analysis_module
from src.domains.analytics.topix_gap_intraday_distribution import (
    format_gap_bucket_label,
    get_topix_available_date_range,
    run_topix_gap_intraday_distribution,
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
        ("72030", "Topix500 Prime", "TOPIX500 PRIME", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("11110", "Prime Ex", "PRIME EX", "0111", "プライム", "1", "A", "1", "A", "-", "2000-01-01", None, None),
        ("22220", "Standard", "STANDARD", "0112", "スタンダード", "1", "A", "1", "A", "-", "2000-01-01", None, None),
        ("33330", "Growth", "GROWTH", "0113", "グロース", "1", "A", "1", "A", "-", "2000-01-01", None, None),
    ]
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stocks,
    )

    topix_rows = [
        ("2024-01-01", 100.0, 101.0, 99.0, 100.0, None),
        ("2024-01-02", 101.0, 101.5, 99.5, 100.0, None),
        ("2024-01-03", 98.0, 99.0, 97.0, 97.0, None),
        ("2024-01-04", 97.5, 98.5, 97.0, 98.0, None),
    ]
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)", topix_rows)

    stock_rows = [
        ("7203", "2024-01-02", 10.0, 12.5, 9.5, 12.0, 1000, 1.0, None),
        ("72030", "2024-01-02", 10.0, 10.5, 4.5, 5.0, 1000, 1.0, None),
        ("11110", "2024-01-02", 20.0, 20.5, 17.5, 18.0, 1000, 1.0, None),
        ("22220", "2024-01-02", 30.0, 30.0, 30.0, 30.0, 1000, 1.0, None),
        ("33330", "2024-01-02", 40.0, 41.0, 34.0, 35.0, 1000, 1.0, None),
        ("7203", "2024-01-03", 12.0, 16.5, 11.5, 16.0, 1000, 1.0, None),
        ("11110", "2024-01-03", 18.0, 19.5, 17.5, 19.0, 1000, 1.0, None),
        ("22220", "2024-01-03", 30.0, 30.5, 28.5, 29.0, 1000, 1.0, None),
        ("33330", "2024-01-03", 35.0, 35.5, 34.5, 35.0, 1000, 1.0, None),
        ("7203", "2024-01-04", 16.0, 16.5, 14.5, 15.0, 1000, 1.0, None),
        ("11110", "2024-01-04", 19.0, 22.5, 18.5, 22.0, 1000, 1.0, None),
        ("22220", "2024-01-04", 29.0, 29.5, 26.5, 27.0, 1000, 1.0, None),
        ("33330", "2024-01-04", 35.0, 36.5, 34.5, 36.0, 1000, 1.0, None),
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
        & (result.summary_df["gap_bucket_key"] == bucket_key)
    ]
    assert len(row) == 1
    return row.iloc[0]


def test_gap_boundaries_and_missing_prev_close_are_handled(analytics_db_path: str) -> None:
    result = run_topix_gap_intraday_distribution(
        analytics_db_path,
        sample_size=10,
    )

    day_counts = dict(
        zip(result.day_counts_df["gap_bucket_key"], result.day_counts_df["day_count"], strict=True)
    )
    assert day_counts == {
        "gap_lt_threshold_1": 1,
        "gap_threshold_1_to_2": 1,
        "gap_ge_threshold_2": 1,
    }
    assert result.excluded_topix_days_without_prev_close == 1
    assert result.available_start_date == "2024-01-01"
    assert result.available_end_date == "2024-01-04"
    assert result.analysis_start_date == "2024-01-02"
    assert result.analysis_end_date == "2024-01-04"


def test_group_classification_and_4digit_preference(analytics_db_path: str) -> None:
    result = run_topix_gap_intraday_distribution(
        analytics_db_path,
        sample_size=10,
    )

    prime_mid = _summary_row(result, "PRIME", "gap_threshold_1_to_2")
    standard_mid = _summary_row(result, "STANDARD", "gap_threshold_1_to_2")
    growth_mid = _summary_row(result, "GROWTH", "gap_threshold_1_to_2")
    topix500_mid = _summary_row(result, "TOPIX500", "gap_threshold_1_to_2")
    prime_ex_mid = _summary_row(result, "PRIME ex TOPIX500", "gap_threshold_1_to_2")

    assert prime_mid["sample_count"] == 2
    assert prime_mid["up_count"] == 1
    assert prime_mid["down_count"] == 1
    assert prime_mid["flat_count"] == 0
    assert prime_mid["mean_intraday_diff"] == pytest.approx(0.0)
    assert prime_mid["median_intraday_diff"] == pytest.approx(0.0)
    assert prime_mid["p50_intraday_diff"] == pytest.approx(0.0)

    assert standard_mid["sample_count"] == 1
    assert standard_mid["flat_count"] == 1
    assert standard_mid["flat_ratio"] == pytest.approx(1.0)

    assert growth_mid["sample_count"] == 1
    assert growth_mid["down_ratio"] == pytest.approx(1.0)

    assert topix500_mid["sample_count"] == 1
    assert topix500_mid["up_count"] == 1
    assert topix500_mid["mean_intraday_diff"] == pytest.approx(2.0)

    assert prime_ex_mid["sample_count"] == 1
    assert prime_ex_mid["down_count"] == 1
    assert prime_ex_mid["mean_intraday_diff"] == pytest.approx(-2.0)


def test_selected_date_range_filters_results(analytics_db_path: str) -> None:
    result = run_topix_gap_intraday_distribution(
        analytics_db_path,
        start_date="2024-01-03",
        end_date="2024-01-04",
        selected_groups=["PRIME", "TOPIX500"],
        sample_size=10,
    )

    day_counts = dict(
        zip(result.day_counts_df["gap_bucket_key"], result.day_counts_df["day_count"], strict=True)
    )
    assert day_counts == {
        "gap_lt_threshold_1": 1,
        "gap_threshold_1_to_2": 0,
        "gap_ge_threshold_2": 1,
    }
    assert result.analysis_start_date == "2024-01-03"
    assert result.analysis_end_date == "2024-01-04"
    assert tuple(result.summary_df["stock_group"].unique()) == ("PRIME", "TOPIX500")


def test_null_topix_open_is_excluded_from_gap_buckets(analytics_db_path: str) -> None:
    conn = duckdb.connect(analytics_db_path)
    conn.execute("UPDATE topix_data SET open = NULL WHERE date = ?", ["2024-01-02"])
    conn.close()

    result = run_topix_gap_intraday_distribution(
        analytics_db_path,
        selected_groups=["PRIME"],
        sample_size=10,
    )

    day_counts = dict(
        zip(result.day_counts_df["gap_bucket_key"], result.day_counts_df["day_count"], strict=True)
    )
    assert day_counts == {
        "gap_lt_threshold_1": 1,
        "gap_threshold_1_to_2": 0,
        "gap_ge_threshold_2": 1,
    }
    assert result.excluded_topix_days_without_prev_close == 2
    assert result.analysis_start_date == "2024-01-03"
    assert result.analysis_end_date == "2024-01-04"


def test_analysis_range_is_empty_when_no_rows_are_analyzable(analytics_db_path: str) -> None:
    result = run_topix_gap_intraday_distribution(
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


def test_sampling_is_deterministic(analytics_db_path: str) -> None:
    first = run_topix_gap_intraday_distribution(
        analytics_db_path,
        selected_groups=["PRIME"],
        sample_size=1,
    )
    second = run_topix_gap_intraday_distribution(
        analytics_db_path,
        selected_groups=["PRIME"],
        sample_size=1,
    )

    pdt.assert_frame_equal(first.samples_df.reset_index(drop=True), second.samples_df.reset_index(drop=True))


def test_clip_bounds_and_clipped_samples_are_returned(analytics_db_path: str) -> None:
    result = run_topix_gap_intraday_distribution(
        analytics_db_path,
        selected_groups=["PRIME"],
        sample_size=10,
        clip_percentiles=(25.0, 75.0),
    )

    assert not result.clip_bounds_df.empty
    assert len(result.clipped_samples_df) <= len(result.samples_df)
    assert {"clip_lower", "clip_upper"} <= set(result.clip_bounds_df.columns)


def test_get_topix_available_date_range_reads_metadata(analytics_db_path: str) -> None:
    assert get_topix_available_date_range(analytics_db_path) == ("2024-01-01", "2024-01-04")


def test_lock_contention_falls_back_to_snapshot(analytics_db_path: str, monkeypatch: pytest.MonkeyPatch) -> None:
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

    result = run_topix_gap_intraday_distribution(
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
    result = run_topix_gap_intraday_distribution(
        analytics_db_path,
        selected_groups=["PRIME"],
        sample_size=0,
    )

    assert result.samples_df.empty
    assert result.clipped_samples_df.empty
    assert result.clip_bounds_df.empty
    assert "gap_bucket_label" in result.samples_df.columns


def test_selected_groups_are_deduplicated(analytics_db_path: str) -> None:
    result = run_topix_gap_intraday_distribution(
        analytics_db_path,
        selected_groups=["PRIME", "PRIME", "TOPIX500"],
        sample_size=1,
    )

    assert result.selected_groups == ("PRIME", "TOPIX500")


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"selected_groups": []}, "selected_groups must contain at least one supported group"),
        ({"selected_groups": ["INVALID"]}, "Unsupported stock group"),
        ({"gap_threshold_1": 0.0}, "gap_threshold_1 must be positive"),
        (
            {"gap_threshold_1": 0.02, "gap_threshold_2": 0.02},
            "gap_threshold_2 must be greater than gap_threshold_1",
        ),
        ({"sample_size": -1}, "sample_size must be non-negative"),
        (
            {"clip_percentiles": (99.0, 99.0)},
            "clip_percentiles must satisfy 0 <= lower < upper <= 100",
        ),
    ],
)
def test_invalid_inputs_raise(
    analytics_db_path: str,
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        run_topix_gap_intraday_distribution(analytics_db_path, **kwargs)


def test_gap_bucket_labels_support_fractional_thresholds() -> None:
    assert (
        format_gap_bucket_label(
            "gap_threshold_1_to_2",
            gap_threshold_1=0.015,
            gap_threshold_2=0.0275,
        )
        == "1.5% <= |gap| < 2.75%"
    )
