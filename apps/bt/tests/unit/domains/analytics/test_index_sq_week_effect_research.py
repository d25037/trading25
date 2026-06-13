from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

from src.domains.analytics.index_sq_week_effect_research import (
    INDEX_SQ_WEEK_EFFECT_RESEARCH_EXPERIMENT_ID,
    get_index_sq_week_effect_research_bundle_path_for_run_id,
    get_index_sq_week_effect_research_latest_bundle_path,
    load_index_sq_week_effect_research_bundle,
    run_index_sq_week_effect_research,
    write_index_sq_week_effect_research_bundle,
)


def _second_friday(value: date) -> date:
    first_day = value.replace(day=1)
    return first_day + timedelta(days=((4 - first_day.weekday()) % 7) + 7)


def _next_sq_date(value: date) -> date:
    current = _second_friday(value)
    if value <= current:
        return current
    if value.month == 12:
        return _second_friday(date(value.year + 1, 1, 1))
    return _second_friday(date(value.year, value.month + 1, 1))


def _build_sq_market_db(db_path: Path) -> str:
    conn = duckdb.connect(str(db_path))
    try:
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
                code TEXT,
                date TEXT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                sector_name TEXT,
                created_at TEXT,
                PRIMARY KEY (code, date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE options_225_data (
                code TEXT,
                date TEXT,
                whole_day_open DOUBLE,
                whole_day_high DOUBLE,
                whole_day_low DOUBLE,
                whole_day_close DOUBLE,
                night_session_open DOUBLE,
                night_session_high DOUBLE,
                night_session_low DOUBLE,
                night_session_close DOUBLE,
                day_session_open DOUBLE,
                day_session_high DOUBLE,
                day_session_low DOUBLE,
                day_session_close DOUBLE,
                volume DOUBLE,
                open_interest DOUBLE,
                turnover_value DOUBLE,
                contract_month TEXT,
                strike_price DOUBLE,
                only_auction_volume DOUBLE,
                emergency_margin_trigger_division TEXT,
                put_call_division TEXT,
                last_trading_day TEXT,
                special_quotation_day TEXT,
                settlement_price DOUBLE,
                theoretical_price DOUBLE,
                base_volatility DOUBLE,
                underlying_price DOUBLE,
                implied_volatility DOUBLE,
                interest_rate DOUBLE,
                created_at TEXT,
                PRIMARY KEY (code, date)
            )
            """
        )
        trading_dates = pd.bdate_range("2024-01-02", "2024-12-31")
        topix_rows: list[tuple[object, ...]] = []
        index_rows: list[tuple[object, ...]] = []
        option_rows: list[tuple[object, ...]] = []
        topix_close = 2500.0
        n225_close = 35000.0
        for i, timestamp in enumerate(trading_dates):
            current_date = timestamp.date()
            sq_date = _second_friday(current_date)
            week_start = current_date - timedelta(days=current_date.weekday())
            week_end = current_date + timedelta(days=4 - current_date.weekday())
            is_sq_week = week_start <= sq_date <= week_end
            topix_move = 8.0 if is_sq_week else 2.0
            n225_move = 120.0 if is_sq_week else 40.0
            topix_open = topix_close
            n225_open = n225_close
            topix_close = topix_open + topix_move
            n225_close = n225_open + n225_move
            date_text = current_date.isoformat()
            topix_rows.append(
                (
                    date_text,
                    topix_open,
                    topix_close + 3.0,
                    topix_open - 3.0,
                    topix_close,
                    None,
                )
            )
            index_rows.append(
                (
                    "N225_UNDERPX",
                    date_text,
                    n225_open,
                    n225_close + 30.0,
                    n225_open - 30.0,
                    n225_close,
                    "日経平均",
                    None,
                )
            )
            front_sq = _next_sq_date(current_date)
            for offset, put_call in enumerate(("1", "2")):
                strike = round(n225_close / 500.0) * 500.0 + offset * 500.0
                volume = 1000.0 if is_sq_week else 250.0
                open_interest = 5000.0 - i * 5.0
                option_rows.append(
                    (
                        f"OPT{i:04d}{put_call}",
                        date_text,
                        10.0,
                        12.0,
                        8.0,
                        11.0,
                        9.0,
                        11.0,
                        7.0,
                        10.0,
                        10.0,
                        12.0,
                        8.0,
                        11.0,
                        volume,
                        open_interest,
                        volume * 100.0,
                        f"{front_sq:%Y-%m}",
                        strike,
                        0.0,
                        None,
                        put_call,
                        (front_sq - timedelta(days=1)).isoformat(),
                        front_sq.isoformat(),
                        11.0,
                        10.5,
                        20.0,
                        n225_close,
                        24.0 if is_sq_week else 20.0,
                        0.5,
                        None,
                    )
                )
        conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)", topix_rows)
        conn.executemany("INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)", index_rows)
        conn.executemany(
            """
            INSERT INTO options_225_data VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            option_rows,
        )
    finally:
        conn.close()
    return str(db_path)


def test_run_index_sq_week_effect_research_builds_index_and_option_tables(
    tmp_path: Path,
) -> None:
    db_path = _build_sq_market_db(tmp_path / "market.duckdb")

    result = run_index_sq_week_effect_research(db_path, min_week_observations=2)

    assert result.index_week_count > 10
    assert result.sq_week_count >= 10
    assert set(result.index_daily_panel_df["index_key"]) == {"TOPIX", "N225_UNDERPX"}
    assert {"week_return_pct", "week_high_low_range_pct"}.issubset(
        result.index_weekly_panel_df.columns
    )
    assert set(result.index_weekly_summary_df["metric"]).issuperset(
        {"week_return_pct", "abs_week_return_pct"}
    )
    assert not result.index_daily_summary_df.empty
    assert not result.options_front_daily_df.empty
    assert {"front_volume", "atm_implied_volatility", "calendar_days_to_sq"}.issubset(
        result.options_front_daily_df.columns
    )
    assert set(result.options_weekly_summary_df["metric"]).issuperset(
        {"front_volume_sum", "atm_implied_volatility_mean"}
    )
    assert not result.options_days_to_sq_summary_df.empty


def test_write_and_load_index_sq_week_effect_research_bundle(tmp_path: Path) -> None:
    db_path = _build_sq_market_db(tmp_path / "market.duckdb")
    result = run_index_sq_week_effect_research(db_path, min_week_observations=2)

    bundle = write_index_sq_week_effect_research_bundle(
        result,
        output_root=tmp_path,
        run_id="index-sq-week-effect",
    )
    loaded = load_index_sq_week_effect_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == INDEX_SQ_WEEK_EFFECT_RESEARCH_EXPERIMENT_ID
    assert (
        get_index_sq_week_effect_research_bundle_path_for_run_id(
            "index-sq-week-effect",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_index_sq_week_effect_research_latest_bundle_path(output_root=tmp_path) == bundle.bundle_dir
    pd.testing.assert_frame_equal(
        loaded.index_weekly_summary_df.reset_index(drop=True),
        result.index_weekly_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        loaded.options_weekly_summary_df.reset_index(drop=True),
        result.options_weekly_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
