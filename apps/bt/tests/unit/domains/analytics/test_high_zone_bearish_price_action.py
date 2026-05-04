from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.domains.analytics.high_zone_bearish_price_action import (
    HIGH_ZONE_BEARISH_PRICE_ACTION_EXPERIMENT_ID,
    load_high_zone_bearish_price_action_research_bundle,
    run_high_zone_bearish_price_action_research,
    write_high_zone_bearish_price_action_research_bundle,
)


def _write_fixture_db(db_path: Path) -> None:
    dates = pd.bdate_range("2024-01-02", periods=55).strftime("%Y-%m-%d").tolist()
    stock_rows: list[tuple[str, str, float, float, float, float, int, float, str]] = []
    master_rows: list[tuple[str, str, str, str, str, str, str, str, str, str, str, str]] = []
    topix_rows: list[tuple[str, float, float, float, float, str]] = []
    for index, date in enumerate(dates):
        base = 100.0 + index
        topix_rows.append((date, 1000.0 + index, 1003.0 + index, 997.0 + index, 1001.0 + index, date))
        for code, market_code, company_name, offset in (
            ("1000", "0111", "Prime Test", 0.0),
            ("2000", "0112", "Standard Test", 10.0),
        ):
            open_price = base + offset
            close_price = open_price + 1.0
            high = close_price + 1.0
            low = open_price - 1.0
            volume = 1000 + index * 10
            if index == 29:
                open_price = 130.0 + offset
                close_price = 135.0 + offset
                high = 136.0 + offset
                low = 129.0 + offset
                volume = 1300
            if index == 30:
                open_price = 136.0 + offset
                close_price = 126.0 + offset
                high = 137.0 + offset
                low = 125.0 + offset
                volume = 4000
            if 31 <= index <= 35:
                open_price = 124.0 + offset - (index - 31)
                close_price = 122.0 + offset - (index - 31)
                high = open_price + 1.0
                low = close_price - 1.0
                volume = 2000
            stock_rows.append(
                (code, date, open_price, high, low, close_price, volume, 1.0, date)
            )
            master_rows.append(
                (
                    date,
                    code,
                    company_name,
                    company_name,
                    market_code,
                    "Market",
                    "17",
                    "Sector17",
                    "33",
                    "Sector33",
                    "TOPIX Small 1",
                    "2020-01-01",
                )
            )
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE stock_data (
            code VARCHAR,
            date VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            adjustment_factor DOUBLE,
            created_at VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE topix_data (
            date VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            created_at VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_master_daily (
            date VARCHAR,
            code VARCHAR,
            company_name VARCHAR,
            company_name_english VARCHAR,
            market_code VARCHAR,
            market_name VARCHAR,
            sector_17_code VARCHAR,
            sector_17_name VARCHAR,
            sector_33_code VARCHAR,
            sector_33_name VARCHAR,
            scale_category VARCHAR,
            listed_date VARCHAR
        )
        """
    )
    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)", topix_rows)
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        master_rows,
    )
    conn.close()


def test_run_high_zone_bearish_price_action_research_builds_pattern_tables(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "market.duckdb"
    _write_fixture_db(db_path)

    result = run_high_zone_bearish_price_action_research(
        str(db_path),
        start_date="2024-01-02",
        end_date="2024-03-15",
        horizons=(1, 5),
        min_events_for_selection=1,
        sample_event_size=2,
    )

    assert set(result.universe_summary_df["market_key"]) == {"prime", "standard"}
    assert "strict_bearish_engulfing" in set(result.pattern_summary_df["pattern_key"])
    strict_rows = result.pattern_summary_df.loc[
        result.pattern_summary_df["pattern_key"].eq("strict_bearish_engulfing")
        & result.pattern_summary_df["high_zone_key"].eq("any_high_zone")
        & result.pattern_summary_df["volume_key"].eq("all")
        & result.pattern_summary_df["horizon_days"].eq(5)
    ]
    assert not strict_rows.empty
    assert strict_rows["event_count"].min() >= 1
    assert not result.sampled_events_df.empty


def test_high_zone_bearish_price_action_bundle_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    output_root = tmp_path / "research"
    _write_fixture_db(db_path)
    result = run_high_zone_bearish_price_action_research(
        str(db_path),
        start_date="2024-01-02",
        end_date="2024-03-15",
        horizons=(5,),
        min_events_for_selection=1,
    )

    bundle = write_high_zone_bearish_price_action_research_bundle(
        result,
        output_root=output_root,
        run_id="20260101_000000_test",
    )
    loaded = load_high_zone_bearish_price_action_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == HIGH_ZONE_BEARISH_PRICE_ACTION_EXPERIMENT_ID
    assert loaded.horizons == (5,)
    assert loaded.pattern_summary_df.shape == result.pattern_summary_df.shape
