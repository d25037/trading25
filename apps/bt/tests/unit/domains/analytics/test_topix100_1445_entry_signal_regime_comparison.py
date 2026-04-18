from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.topix100_1445_entry_signal_regime_comparison import (
    TOPIX100_1445_ENTRY_SIGNAL_REGIME_COMPARISON_EXPERIMENT_ID,
    get_topix100_1445_entry_signal_regime_comparison_bundle_path_for_run_id,
    get_topix100_1445_entry_signal_regime_comparison_latest_bundle_path,
    load_topix100_1445_entry_signal_regime_comparison_research_bundle,
    run_topix100_1445_entry_signal_regime_comparison_research,
    write_topix100_1445_entry_signal_regime_comparison_research_bundle,
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
                "Alpha",
                "Alpha",
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
                "Beta",
                "Beta",
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
                "Gamma",
                "Gamma",
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
                "Delta",
                "Delta",
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


def _append_constant_rows(
    *,
    rows: list[tuple[str, str, str, float, float, float, float, int, float, None]],
    code: str,
    date: str,
    times: list[str],
    price: float,
    total_volume: int,
) -> None:
    per_minute_volume = total_volume // len(times)
    for time in times:
        rows.append(
            (
                code,
                date,
                time,
                price,
                price,
                price,
                price,
                per_minute_volume,
                float(per_minute_volume) * price,
                None,
            )
        )


def _append_price_row(
    *,
    rows: list[tuple[str, str, str, float, float, float, float, int, float, None]],
    code: str,
    date: str,
    time: str,
    price: float,
    volume: int = 100,
) -> None:
    rows.append(
        (
            code,
            date,
            time,
            price,
            price,
            price,
            price,
            volume,
            float(volume) * price,
            None,
        )
    )


def _build_market_db(db_path: Path) -> str:
    conn = duckdb.connect(str(db_path))
    _create_tables(conn)
    _insert_stocks(conn)

    opening_times = ["09:00", "09:01", "09:02", "09:03", "09:04"]
    closing_times = ["15:26", "15:27", "15:28", "15:29", "15:30"]
    dates = ["2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09", "2024-01-10"]
    profiles = {
        "1111": {
            "opening_volumes": [1600, 800, 400, 200, 100],
            "closing_volume": 100,
            "opens": [100.0, 99.0, 100.0, 102.0, 101.0],
            "at_1030": [100.0, 99.5, 100.0, 103.0, 101.5],
            "entry": [100.0, 95.0, 100.0, 102.0, 101.0],
        },
        "2222": {
            "opening_volumes": [800, 800, 800, 800, 800],
            "closing_volume": 200,
            "opens": [100.0, 100.0, 100.0, 103.0, 102.0],
            "at_1030": [100.0, 100.5, 100.0, 104.0, 102.5],
            "entry": [100.0, 96.0, 100.0, 103.0, 102.0],
        },
        "3333": {
            "opening_volumes": [200, 400, 800, 1600, 3200],
            "closing_volume": 400,
            "opens": [100.0, 101.0, 100.0, 104.0, 103.0],
            "at_1030": [100.0, 101.5, 100.0, 105.0, 103.5],
            "entry": [100.0, 97.0, 100.0, 104.0, 103.0],
        },
        "4444": {
            "opening_volumes": [100, 400, 1600, 6400, 25600],
            "closing_volume": 800,
            "opens": [100.0, 102.0, 100.0, 105.0, 104.0],
            "at_1030": [100.0, 102.5, 100.0, 106.0, 104.5],
            "entry": [100.0, 98.0, 100.0, 105.0, 104.0],
        },
    }

    rows: list[tuple[str, str, str, float, float, float, float, int, float, None]] = []
    for code, profile in profiles.items():
        for index, date in enumerate(dates):
            _append_constant_rows(
                rows=rows,
                code=code,
                date=date,
                times=opening_times,
                price=profile["opens"][index],
                total_volume=profile["opening_volumes"][index],
            )
            _append_price_row(
                rows=rows,
                code=code,
                date=date,
                time="10:30",
                price=profile["at_1030"][index],
            )
            _append_price_row(
                rows=rows,
                code=code,
                date=date,
                time="14:45",
                price=profile["entry"][index],
            )
            _append_constant_rows(
                rows=rows,
                code=code,
                date=date,
                times=closing_times,
                price=100.0,
                total_volume=profile["closing_volume"],
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


def test_run_research_builds_signal_regime_and_subgroup_tables(
    analytics_db_path: str,
) -> None:
    result = run_topix100_1445_entry_signal_regime_comparison_research(
        analytics_db_path,
        interval_minutes_list=[5],
        bucket_count=4,
        period_months=6,
        entry_time="14:45",
        next_session_exit_time="10:30",
        tail_fraction=0.10,
    )

    assert result.analysis_start_date == "2024-01-04"
    assert result.analysis_end_date == "2024-01-10"
    assert result.entry_time == "14:45"
    assert result.next_session_exit_time == "10:30"
    assert result.topix100_constituent_count == 4
    assert result.total_session_count == 20
    assert result.regime_day_count == 4
    assert set(result.market_regime_df["market_regime_bucket_key"]) == {
        "weak",
        "neutral",
        "strong",
    }
    assert set(result.signal_session_df["signal_family"]) == {
        "previous_open_vs_open",
        "previous_close_vs_open",
    }
    assert set(
        result.signal_session_df["expected_selected_bucket_label"]
    ) == {"Q1", "Q4"}
    assert result.selected_signal_session_count > 0
    assert set(result.selected_trade_level_df["exit_label"]) == {
        "next_open",
        "next_1030",
    }
    assert set(result.base_session_df["current_entry_bucket_key"]) == {
        "winners",
        "middle",
        "losers",
    }
    assert set(result.selected_trade_level_df["subgroup_key"]) <= {
        "all",
        "winners",
        "middle",
        "losers",
    }
    assert "all" in set(result.selected_trade_level_df["subgroup_key"])

    signal_summary = result.signal_summary_df.set_index(
        ["signal_family", "exit_label", "subgroup_key"]
    )
    assert signal_summary.loc[
        ("previous_open_vs_open", "next_open", "all"),
        "expected_selected_bucket_label",
    ] == "Q1"
    assert signal_summary.loc[
        ("previous_close_vs_open", "next_open", "all"),
        "expected_selected_bucket_label",
    ] == "Q4"
    assert signal_summary.loc[
        ("previous_open_vs_open", "next_open", "all"),
        "sample_share",
    ] == pytest.approx(1.0)
    assert signal_summary.loc[
        ("previous_open_vs_open", "next_open", "winners"),
        "sample_share",
    ] < 1.0

    intersection_summary = result.intersection_summary_df
    assert not intersection_summary.empty
    assert set(intersection_summary["market_regime_bucket_key"]) <= {
        "weak",
        "neutral",
        "strong",
    }


def test_research_bundle_roundtrip(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_topix100_1445_entry_signal_regime_comparison_research(
        analytics_db_path,
        interval_minutes_list=[5],
        bucket_count=4,
        period_months=6,
    )

    bundle = write_topix100_1445_entry_signal_regime_comparison_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260418_174500_testabcd",
    )
    reloaded = load_topix100_1445_entry_signal_regime_comparison_research_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == TOPIX100_1445_ENTRY_SIGNAL_REGIME_COMPARISON_EXPERIMENT_ID
    )
    assert bundle.run_id == "20260418_174500_testabcd"
    pd.testing.assert_frame_equal(
        reloaded.periods_df,
        result.periods_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.market_regime_df,
        result.market_regime_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.base_session_df,
        result.base_session_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.signal_session_df,
        result.signal_session_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.selected_trade_level_df,
        result.selected_trade_level_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.market_summary_df,
        result.market_summary_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.signal_summary_df,
        result.signal_summary_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.intersection_summary_df,
        result.intersection_summary_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.period_intersection_summary_df,
        result.period_intersection_summary_df,
        check_dtype=False,
    )
    assert (
        get_topix100_1445_entry_signal_regime_comparison_bundle_path_for_run_id(
            "20260418_174500_testabcd",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_1445_entry_signal_regime_comparison_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
