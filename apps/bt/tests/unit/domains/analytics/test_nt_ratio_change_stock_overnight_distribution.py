from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any, cast

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics import topix_close_stock_overnight_distribution as topix_analysis_module
from src.domains.analytics.nt_ratio_change_stock_overnight_distribution import (
    NT_RATIO_CHANGE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID,
    format_nt_ratio_bucket_label,
    get_nt_ratio_change_stock_overnight_distribution_bundle_path_for_run_id,
    get_nt_ratio_change_stock_overnight_distribution_latest_bundle_path,
    get_nt_ratio_available_date_range,
    load_nt_ratio_change_stock_overnight_distribution_research_bundle,
    run_nt_ratio_change_stock_overnight_distribution,
    write_nt_ratio_change_stock_overnight_distribution_research_bundle,
)


def _date_strings(count: int) -> list[str]:
    start = date(2024, 1, 1)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


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
    conn.execute(
        """
        CREATE TABLE indices_data (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            close DOUBLE,
            PRIMARY KEY (code, date)
        )
        """
    )

    stocks = [
        ("7203", "Topix100 Prime", "TOPIX100 PRIME", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("1111", "Prime Ex", "PRIME EX", "0111", "プライム", "1", "A", "1", "A", "-", "2000-01-01", None, None),
        ("2222", "Topix500 Mid400", "TOPIX500 MID400", "0111", "プライム", "1", "A", "1", "A", "TOPIX Mid400", "2000-01-01", None, None),
        ("3333", "Growth", "GROWTH", "0113", "グロース", "1", "A", "1", "A", "-", "2000-01-01", None, None),
    ]
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stocks,
    )

    date_values = _date_strings(15)
    nt_ratio_returns = [-0.12, -0.08, -0.02, -0.01, 0.0, 0.0, 0.0, 0.0, 0.0, 0.01, 0.02, 0.08, 0.12]
    nt_ratios = [10.0]
    for value in nt_ratio_returns:
        nt_ratios.append(nt_ratios[-1] * (1.0 + value))
    nt_ratios.append(nt_ratios[-1])

    topix_rows = [(day, 100.0, 100.0, 100.0, 100.0, None) for day in date_values]
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)", topix_rows)

    indices_rows = [
        ("N225_UNDERPX", day, ratio * 100.0)
        for day, ratio in zip(date_values, nt_ratios, strict=True)
    ]
    conn.executemany("INSERT INTO indices_data VALUES (?, ?, ?)", indices_rows)

    bucket_returns = {
        "return_le_mean_minus_2sd": {"7203": -0.04, "1111": -0.015, "2222": -0.025, "3333": -0.01},
        "return_mean_minus_2sd_to_minus_1sd": {"7203": -0.02, "1111": 0.005, "2222": -0.01, "3333": -0.005},
        "return_mean_minus_1sd_to_plus_1sd": {"7203": 0.01, "1111": 0.002, "2222": -0.005, "3333": 0.0},
        "return_mean_plus_1sd_to_plus_2sd": {"7203": 0.03, "1111": 0.008, "2222": 0.01, "3333": 0.005},
        "return_ge_mean_plus_2sd": {"7203": 0.05, "1111": 0.015, "2222": 0.02, "3333": 0.01},
    }
    event_bucket_by_date = {
        date_values[1]: "return_le_mean_minus_2sd",
        date_values[2]: "return_mean_minus_2sd_to_minus_1sd",
        date_values[3]: "return_mean_minus_1sd_to_plus_1sd",
        date_values[4]: "return_mean_minus_1sd_to_plus_1sd",
        date_values[5]: "return_mean_minus_1sd_to_plus_1sd",
        date_values[6]: "return_mean_minus_1sd_to_plus_1sd",
        date_values[7]: "return_mean_minus_1sd_to_plus_1sd",
        date_values[8]: "return_mean_minus_1sd_to_plus_1sd",
        date_values[9]: "return_mean_minus_1sd_to_plus_1sd",
        date_values[10]: "return_mean_minus_1sd_to_plus_1sd",
        date_values[11]: "return_mean_minus_1sd_to_plus_1sd",
        date_values[12]: "return_mean_plus_1sd_to_plus_2sd",
        date_values[13]: "return_ge_mean_plus_2sd",
    }

    stock_rows: list[tuple[Any, ...]] = []
    codes = ["7203", "1111", "2222", "3333"]
    for index, day in enumerate(date_values):
        for code in codes:
            next_open = 100.0
            if index > 0:
                previous_day = date_values[index - 1]
                previous_bucket = event_bucket_by_date.get(previous_day)
                if previous_bucket is not None:
                    next_open = 100.0 * (1.0 + bucket_returns[previous_bucket][code])
            stock_rows.append(
                (
                    code,
                    day,
                    next_open,
                    max(next_open, 100.0),
                    min(next_open, 100.0),
                    100.0,
                    1000,
                    1.0,
                    None,
                )
            )
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.close()
    return str(db_path)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return _build_market_db(tmp_path / "market.duckdb")


def _summary_row(result, stock_group: str, bucket_key: str) -> pd.Series:
    row = result.summary_df[
        (result.summary_df["stock_group"] == stock_group)
        & (result.summary_df["nt_ratio_bucket_key"] == bucket_key)
    ]
    assert len(row) == 1
    return row.iloc[0]


def _daily_group_row(result, stock_group: str, date_value: str) -> pd.Series:
    row = result.daily_group_returns_df[
        (result.daily_group_returns_df["stock_group"] == stock_group)
        & (result.daily_group_returns_df["date"] == date_value)
    ]
    assert len(row) == 1
    return row.iloc[0]


def test_nt_ratio_stats_and_day_counts_are_returned(analytics_db_path: str) -> None:
    result = run_nt_ratio_change_stock_overnight_distribution(
        analytics_db_path,
        sample_size=10,
    )

    assert result.nt_ratio_stats is not None
    assert result.nt_ratio_stats.sample_count == 13
    assert result.nt_ratio_stats.mean_return == pytest.approx(0.0)
    assert result.nt_ratio_stats.std_return == pytest.approx(0.05958187643906492)
    assert result.nt_ratio_stats.lower_threshold_2 == pytest.approx(-0.11916375287812984)
    assert result.nt_ratio_stats.upper_threshold_2 == pytest.approx(0.11916375287812984)

    day_counts = dict(
        zip(
            result.day_counts_df["nt_ratio_bucket_key"],
            result.day_counts_df["day_count"],
            strict=True,
        )
    )
    assert day_counts == {
        "return_le_mean_minus_2sd": 1,
        "return_mean_minus_2sd_to_minus_1sd": 1,
        "return_mean_minus_1sd_to_plus_1sd": 9,
        "return_mean_plus_1sd_to_plus_2sd": 1,
        "return_ge_mean_plus_2sd": 1,
    }
    assert result.excluded_nt_ratio_days_without_prev_ratio == 1
    assert result.excluded_nt_ratio_days_without_next_session == 1
    assert result.available_start_date == "2024-01-01"
    assert result.available_end_date == "2024-01-15"
    assert result.analysis_start_date == "2024-01-02"
    assert result.analysis_end_date == "2024-01-14"


def test_group_summary_and_daily_returns_are_returned(analytics_db_path: str) -> None:
    result = run_nt_ratio_change_stock_overnight_distribution(
        analytics_db_path,
        selected_groups=["PRIME", "TOPIX100", "TOPIX500"],
        sample_size=10,
    )

    topix100_extreme_positive = _summary_row(result, "TOPIX100", "return_ge_mean_plus_2sd")
    prime_central = _summary_row(result, "PRIME", "return_mean_minus_1sd_to_plus_1sd")
    topix500_negative = _summary_row(result, "TOPIX500", "return_mean_minus_2sd_to_minus_1sd")

    assert topix100_extreme_positive["sample_count"] == 1
    assert topix100_extreme_positive["mean_overnight_return"] == pytest.approx(0.05)
    assert prime_central["sample_count"] == 27
    assert prime_central["mean_overnight_return"] == pytest.approx((0.01 + 0.002 - 0.005) / 3.0)
    assert topix500_negative["mean_overnight_return"] == pytest.approx((-0.02 - 0.01) / 2.0)

    day_row = _daily_group_row(result, "TOPIX100", "2024-01-14")
    assert day_row["nt_ratio_bucket_key"] == "return_ge_mean_plus_2sd"
    assert day_row["nt_ratio_return"] == pytest.approx(0.12)
    assert day_row["day_mean_overnight_return"] == pytest.approx(0.05)
    assert day_row["constituent_count"] == 1


def test_sampling_is_deterministic(analytics_db_path: str) -> None:
    first = run_nt_ratio_change_stock_overnight_distribution(
        analytics_db_path,
        selected_groups=["PRIME"],
        sample_size=1,
    )
    second = run_nt_ratio_change_stock_overnight_distribution(
        analytics_db_path,
        selected_groups=["PRIME"],
        sample_size=1,
    )

    pdt.assert_frame_equal(
        first.samples_df.reset_index(drop=True),
        second.samples_df.reset_index(drop=True),
    )


def test_nt_ratio_bundle_roundtrip(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_nt_ratio_change_stock_overnight_distribution(
        analytics_db_path,
        selected_groups=["PRIME", "TOPIX100"],
        sample_size=5,
        clip_percentiles=(5.0, 95.0),
    )

    bundle = write_nt_ratio_change_stock_overnight_distribution_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260331_180000_testabcd",
    )
    reloaded = load_nt_ratio_change_stock_overnight_distribution_research_bundle(
        bundle.bundle_dir
    )

    assert bundle.experiment_id == NT_RATIO_CHANGE_STOCK_OVERNIGHT_RESEARCH_EXPERIMENT_ID
    assert bundle.summary_path.exists()
    assert (
        get_nt_ratio_change_stock_overnight_distribution_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_nt_ratio_change_stock_overnight_distribution_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert reloaded.sample_size == 5
    assert reloaded.clip_percentiles == (5.0, 95.0)
    assert reloaded.selected_groups == result.selected_groups
    assert reloaded.nt_ratio_stats == result.nt_ratio_stats
    pdt.assert_frame_equal(
        reloaded.summary_df,
        result.summary_df,
        check_dtype=False,
    )


def test_get_nt_ratio_available_date_range_reads_metadata(analytics_db_path: str) -> None:
    assert get_nt_ratio_available_date_range(analytics_db_path) == (
        "2024-01-01",
        "2024-01-15",
    )


def test_narrow_date_range_returns_empty_analysis(analytics_db_path: str) -> None:
    result = run_nt_ratio_change_stock_overnight_distribution(
        analytics_db_path,
        start_date="2024-01-01",
        end_date="2024-01-01",
        selected_groups=["PRIME"],
        sample_size=10,
    )

    assert result.nt_ratio_stats is None
    assert result.analysis_start_date is None
    assert result.analysis_end_date is None
    assert result.summary_df["sample_count"].sum() == 0
    assert result.samples_df.empty
    assert result.daily_group_returns_df.empty


def test_sample_size_zero_returns_empty_samples(analytics_db_path: str) -> None:
    result = run_nt_ratio_change_stock_overnight_distribution(
        analytics_db_path,
        selected_groups=["PRIME"],
        sample_size=0,
    )

    assert result.samples_df.empty
    assert result.clipped_samples_df.empty
    assert result.clip_bounds_df.empty
    assert "nt_ratio_bucket_label" in result.samples_df.columns
    assert "nt_ratio_return" in result.samples_df.columns


def test_selected_groups_are_deduplicated(analytics_db_path: str) -> None:
    result = run_nt_ratio_change_stock_overnight_distribution(
        analytics_db_path,
        selected_groups=["PRIME", "PRIME", "TOPIX100"],
        sample_size=1,
    )

    assert result.selected_groups == ("PRIME", "TOPIX100")


def test_lock_contention_falls_back_to_snapshot(
    analytics_db_path: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_connect = topix_analysis_module._connect_duckdb
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

    monkeypatch.setattr(topix_analysis_module, "_connect_duckdb", flaky_connect)

    result = run_nt_ratio_change_stock_overnight_distribution(
        analytics_db_path,
        selected_groups=["PRIME"],
        sample_size=2,
    )

    assert result.source_mode == "snapshot"
    assert "temporary snapshot copied from" in result.source_detail


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"selected_groups": []}, "selected_groups must contain at least one supported group"),
        ({"selected_groups": ["INVALID"]}, "Unsupported stock group"),
        ({"sigma_threshold_1": 0.0}, "sigma_threshold_1 must be positive"),
        (
            {"sigma_threshold_1": 1.5, "sigma_threshold_2": 1.5},
            "sigma_threshold_2 must be greater than sigma_threshold_1",
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
        run_nt_ratio_change_stock_overnight_distribution(
            analytics_db_path,
            **cast(dict[str, Any], kwargs),
        )


def test_bucket_labels_support_fractional_sigmas() -> None:
    assert (
        format_nt_ratio_bucket_label(
            "return_mean_plus_1sd_to_plus_2sd",
            sigma_threshold_1=1.5,
            sigma_threshold_2=2.25,
        )
        == "μ+1.5σ <= return < μ+2.25σ"
    )
    assert (
        format_nt_ratio_bucket_label(
            "return_mean_minus_2sd_to_minus_1sd",
            sigma_threshold_1=1.5,
            sigma_threshold_2=2.25,
        )
        == "μ-2.25σ < return <= μ-1.5σ"
    )
