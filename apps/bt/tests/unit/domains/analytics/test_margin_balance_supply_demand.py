from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.domains.analytics.margin_balance_supply_demand import (
    run_margin_balance_supply_demand_research,
)


def test_margin_balance_research_shifts_weekly_record_to_effective_entry(
    tmp_path: Path,
) -> None:
    db_path = _build_margin_research_db(tmp_path / "market.duckdb")

    result = run_margin_balance_supply_demand_research(
        db_path,
        horizons=(1,),
        adv_window=2,
        effective_lag_sessions=3,
        bucket_count=2,
        min_daily_observations=2,
        discovery_end_date="2024-01-10",
        percentile_window=2,
    )

    obs = result.observation_df.sort_values(["code", "margin_date"]).reset_index(drop=True)
    first_alpha = obs[(obs["code"] == "1111") & (obs["margin_date"] == "2024-01-03")].iloc[0]
    assert first_alpha["effective_date"] == "2024-01-08"
    assert first_alpha["margin_date"] < first_alpha["effective_date"]

    # ADV is previous sessions only: Jan 4 volume 300 and Jan 5 volume 400.
    assert first_alpha["adv20"] == pytest.approx(350.0)
    assert first_alpha["long_to_adv20"] == pytest.approx(1000.0 / 350.0)

    expected_return = (105.0 / 100.0) - 1.0
    assert first_alpha["return_open_to_close_1d"] == pytest.approx(expected_return)


def test_margin_balance_research_emits_bucket_and_pruning_summaries(
    tmp_path: Path,
) -> None:
    db_path = _build_margin_research_db(tmp_path / "market.duckdb")

    result = run_margin_balance_supply_demand_research(
        db_path,
        horizons=(1,),
        adv_window=2,
        effective_lag_sessions=3,
        bucket_count=2,
        min_daily_observations=2,
        discovery_end_date="2024-01-10",
        percentile_window=2,
    )

    bucket_summary = result.bucket_return_summary_df
    pruning_summary = result.pruning_summary_df

    assert not bucket_summary.empty
    assert {"long_to_adv20", "short_to_adv20"}.issubset(set(bucket_summary["feature"]))
    assert not pruning_summary.empty
    assert "exclude_high_long_to_adv20" in set(pruning_summary["candidate"])
    assert {"full", "discovery", "validation"}.issubset(set(pruning_summary["period"]))


def _build_margin_research_db(db_path: Path) -> Path:
    dates = [
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
        "2024-01-05",
        "2024-01-08",
        "2024-01-09",
        "2024-01-10",
        "2024-01-11",
        "2024-01-12",
        "2024-01-15",
        "2024-01-16",
        "2024-01-17",
    ]
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE stocks (
            code TEXT,
            company_name TEXT,
            market_code TEXT,
            market_name TEXT,
            scale_category TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_data (
            code TEXT,
            date TEXT,
            open DOUBLE,
            close DOUBLE,
            volume DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE margin_data (
            code TEXT,
            date TEXT,
            long_margin_volume DOUBLE,
            short_margin_volume DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?)",
        [
            ("1111", "Alpha", "0111", "Prime", "TOPIX Core30"),
            ("2222", "Beta", "0112", "Standard", None),
        ],
    )
    stock_rows = []
    for idx, date in enumerate(dates):
        stock_rows.append(("1111", date, 100.0, 101.0 + idx, 100.0 * (idx + 1)))
        stock_rows.append(("2222", date, 200.0, 199.0 - idx, 200.0 * (idx + 1)))
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?)", stock_rows)
    conn.executemany(
        "INSERT INTO margin_data VALUES (?, ?, ?, ?)",
        [
            ("1111", "2024-01-03", 1000.0, 100.0),
            ("2222", "2024-01-03", 300.0, 200.0),
            ("1111", "2024-01-05", 1100.0, 120.0),
            ("2222", "2024-01-05", 250.0, 210.0),
            ("1111", "2024-01-09", 900.0, 150.0),
            ("2222", "2024-01-09", 350.0, 180.0),
        ],
    )
    conn.close()
    return db_path
