from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.domains.analytics.daily_move_asymmetry import (
    DAILY_MOVE_ASYMMETRY_EXPERIMENT_ID,
    get_daily_move_asymmetry_bundle_path_for_run_id,
    get_daily_move_asymmetry_latest_bundle_path,
    load_daily_move_asymmetry_bundle,
    run_daily_move_asymmetry_research,
    write_daily_move_asymmetry_bundle,
)


def _build_market_db(db_path: Path) -> str:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE topix_data (
                date TEXT PRIMARY KEY,
                close DOUBLE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE stock_data (
                code TEXT NOT NULL,
                date TEXT NOT NULL,
                close DOUBLE,
                PRIMARY KEY (code, date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE stock_master_daily (
                code TEXT NOT NULL,
                date TEXT NOT NULL,
                company_name TEXT,
                market_code TEXT,
                market_name TEXT,
                sector_33_name TEXT,
                scale_category TEXT,
                PRIMARY KEY (code, date)
            )
            """
        )
        dates = pd.bdate_range("2024-01-02", periods=110)
        topix_rows: list[tuple[object, ...]] = []
        stock_rows: list[tuple[object, ...]] = []
        master_rows: list[tuple[object, ...]] = []
        for i, date in enumerate(dates):
            topix_close = 1000.0 + i * 1.0 + (15.0 if i == 65 else 0.0)
            topix_rows.append((date.strftime("%Y-%m-%d"), topix_close))
            values = {
                "1111": 100.0 + i * 0.8 + (8.0 if i == 66 else 0.0),
                "2222": 120.0 - i * 0.2 - (12.0 if i == 67 else 0.0),
                "3333": 80.0 + ((i % 10) - 5) * 0.7 + i * 0.1,
            }
            for code, close in values.items():
                stock_rows.append((code, date.strftime("%Y-%m-%d"), close))
                master_rows.append(
                    (
                        code,
                        date.strftime("%Y-%m-%d"),
                        f"{code} Co",
                        "0111",
                        "Prime",
                        "Sector",
                        "",
                    )
                )
        conn.executemany("INSERT INTO topix_data VALUES (?, ?)", topix_rows)
        conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?)", stock_rows)
        conn.executemany(
            "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?, ?)",
            master_rows,
        )
    finally:
        conn.close()
    return str(db_path)


def test_run_daily_move_asymmetry_builds_expected_tables(tmp_path: Path) -> None:
    db_path = _build_market_db(tmp_path / "market.duckdb")

    result = run_daily_move_asymmetry_research(
        db_path,
        horizons=(1, 5),
        rolling_vol_window=10,
        min_observations=1,
    )

    assert result.market_source == "stock_master_daily_exact_date"
    assert result.prime_code_count == 3
    assert result.topix_observation_count > 0
    assert result.prime_stock_observation_count > result.topix_observation_count
    assert set(result.prime_stock_event_summary_df["return_metric"]) == {
        "raw",
        "topix_excess",
        "beta_adjusted",
    }
    assert {"small", "medium", "large"} & set(result.paired_asymmetry_df["magnitude_bucket"])
    assert set(result.sign_persistence_df["scope"]) == {"topix", "prime_stock"}
    assert not result.streak_hazard_df.empty
    assert {"up_ratio_pct", "extreme_down_ratio_pct"}.issubset(result.prime_breadth_df.columns)
    assert not result.beta_diagnostics_df.empty
    assert not result.observation_sample_df.empty


def test_write_and_load_daily_move_asymmetry_bundle(tmp_path: Path) -> None:
    db_path = _build_market_db(tmp_path / "market.duckdb")
    result = run_daily_move_asymmetry_research(
        db_path,
        horizons=(1,),
        rolling_vol_window=10,
        min_observations=1,
    )

    bundle = write_daily_move_asymmetry_bundle(
        result,
        output_root=tmp_path,
        run_id="daily-asymmetry",
    )
    loaded = load_daily_move_asymmetry_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == DAILY_MOVE_ASYMMETRY_EXPERIMENT_ID
    assert (
        get_daily_move_asymmetry_bundle_path_for_run_id(
            "daily-asymmetry",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_daily_move_asymmetry_latest_bundle_path(output_root=tmp_path) == bundle.bundle_dir
    pd.testing.assert_frame_equal(
        loaded.paired_asymmetry_df.reset_index(drop=True),
        result.paired_asymmetry_df.reset_index(drop=True),
        check_dtype=False,
    )
