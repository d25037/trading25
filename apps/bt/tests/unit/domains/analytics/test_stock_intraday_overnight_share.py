from __future__ import annotations

import math
from pathlib import Path
from typing import Any, cast

import duckdb
import pandas as pd
import pytest

from src.domains.analytics import stock_intraday_overnight_share as analysis_module
from src.domains.analytics.stock_intraday_overnight_share import (
    STOCK_INTRADAY_OVERNIGHT_SHARE_RESEARCH_EXPERIMENT_ID,
    get_stock_intraday_overnight_share_bundle_path_for_run_id,
    get_stock_intraday_overnight_share_latest_bundle_path,
    get_stock_available_date_range,
    load_stock_intraday_overnight_share_research_bundle,
    run_stock_intraday_overnight_share_analysis,
    write_stock_intraday_overnight_share_research_bundle,
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

    stocks = [
        (
            "72030",
            "Topix100 Prime",
            "TOPIX100 PRIME",
            "0111",
            "プライム",
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
            "22220",
            "Topix500 Mid400",
            "TOPIX500 MID400",
            "0111",
            "プライム",
            "1",
            "A",
            "1",
            "A",
            "TOPIX Mid400",
            "2000-01-01",
            None,
            None,
        ),
        (
            "11110",
            "Prime Ex",
            "PRIME EX",
            "0111",
            "プライム",
            "1",
            "A",
            "1",
            "A",
            "-",
            "2000-01-01",
            None,
            None,
        ),
        (
            "33330",
            "Standard",
            "STANDARD",
            "0112",
            "スタンダード",
            "1",
            "A",
            "1",
            "A",
            "-",
            "2000-01-01",
            None,
            None,
        ),
        (
            "44440",
            "Growth",
            "GROWTH",
            "0113",
            "グロース",
            "1",
            "A",
            "1",
            "A",
            "-",
            "2000-01-01",
            None,
            None,
        ),
    ]
    conn.executemany("INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", stocks)

    stock_rows = [
        ("72030", "2024-01-02", 100.0, 110.0),
        ("72030", "2024-01-03", 121.0, 121.0),
        ("72030", "2024-01-04", 133.1, 133.1),
        ("72030", "2024-01-05", 146.41, 146.41),
        ("22220", "2024-01-02", 100.0, 100.0),
        ("22220", "2024-01-03", 80.0, 88.0),
        ("22220", "2024-01-04", 88.0, 96.8),
        ("22220", "2024-01-05", 96.8, 96.8),
        ("11110", "2024-01-02", 100.0, 120.0),
        ("11110", "2024-01-03", 120.0, 108.0),
        ("11110", "2024-01-04", 108.0, 108.0),
        ("11110", "2024-01-05", 108.0, 108.0),
        ("33330", "2024-01-02", 100.0, 100.0),
        ("33330", "2024-01-03", 110.0, 110.0),
        ("33330", "2024-01-04", 121.0, 121.0),
        ("33330", "2024-01-05", 133.1, 133.1),
        ("44440", "2024-01-02", 100.0, 110.0),
        ("44440", "2024-01-03", 100.0, 110.0),
        ("44440", "2024-01-04", 100.0, 110.0),
        ("44440", "2024-01-05", 100.0, 100.0),
    ]
    conn.executemany(
        """
        INSERT INTO stock_data (
            code,
            date,
            open,
            high,
            low,
            close,
            volume,
            adjustment_factor,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (code, date, open_, max(open_, close), min(open_, close), close, 1000, 1.0, None)
            for code, date, open_, close in stock_rows
        ],
    )
    conn.close()
    return str(db_path)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return _build_market_db(tmp_path / "market.duckdb")


def _stock_row(result: Any, group: str, code: str) -> Any:
    rows = result.stock_metrics_df[
        (result.stock_metrics_df["stock_group"] == group)
        & (result.stock_metrics_df["code"] == code)
    ]
    assert len(rows) == 1
    return rows.iloc[0]


def _group_row(result: Any, group: str) -> Any:
    rows = result.group_summary_df[result.group_summary_df["stock_group"] == group]
    assert len(rows) == 1
    return rows.iloc[0]


def _daily_group_row(result: Any, group: str, date: str) -> Any:
    rows = result.daily_group_shares_df[
        (result.daily_group_shares_df["stock_group"] == group)
        & (result.daily_group_shares_df["date"] == date)
    ]
    assert len(rows) == 1
    return rows.iloc[0]


def test_analysis_returns_expected_stock_shares(analytics_db_path: str) -> None:
    result = run_stock_intraday_overnight_share_analysis(
        analytics_db_path,
        min_session_count=1,
    )

    up_move = math.log(1.1)
    down_gap = abs(math.log(0.8))
    prime_ex_intraday = math.log(1.2) + abs(math.log(0.9))
    topix100 = _stock_row(result, "TOPIX100", "7203")
    topix500_topix100 = _stock_row(result, "TOPIX500", "7203")
    topix500_mid400 = _stock_row(result, "TOPIX500", "2222")
    prime_ex = _stock_row(result, "PRIME ex TOPIX500", "1111")
    standard = _stock_row(result, "STANDARD", "3333")
    growth = _stock_row(result, "GROWTH", "4444")

    assert result.analysis_start_date == "2024-01-02"
    assert result.analysis_end_date == "2024-01-04"
    assert topix100["session_count"] == 3
    assert topix100["intraday_share"] == pytest.approx(0.25)
    assert topix100["overnight_share"] == pytest.approx(0.75)
    assert topix500_topix100["overnight_share"] == pytest.approx(0.75)
    assert topix500_mid400["intraday_share"] == pytest.approx((2.0 * up_move) / (2.0 * up_move + down_gap))
    assert topix500_mid400["overnight_share"] == pytest.approx(down_gap / (2.0 * up_move + down_gap))
    assert prime_ex["intraday_abs_log_return_sum"] == pytest.approx(prime_ex_intraday)
    assert prime_ex["overnight_share"] == pytest.approx(0.0)
    assert standard["intraday_share"] == pytest.approx(0.0)
    assert standard["overnight_share"] == pytest.approx(1.0)
    assert growth["intraday_share"] == pytest.approx(0.5)
    assert growth["overnight_share"] == pytest.approx(0.5)


def test_group_summary_and_daily_group_shares_are_returned(analytics_db_path: str) -> None:
    result = run_stock_intraday_overnight_share_analysis(
        analytics_db_path,
        min_session_count=1,
    )

    topix500 = _group_row(result, "TOPIX500")
    standard = _group_row(result, "STANDARD")
    topix500_day_1 = _daily_group_row(result, "TOPIX500", "2024-01-02")

    up_move = math.log(1.1)
    down_gap = abs(math.log(0.8))

    assert topix500["stock_count"] == 2
    assert topix500["share_defined_stock_count"] == 2
    assert topix500["median_overnight_share"] == pytest.approx(
        (0.75 + down_gap / (2.0 * up_move + down_gap)) / 2.0
    )
    assert standard["median_intraday_share"] == pytest.approx(0.0)
    assert standard["median_overnight_share"] == pytest.approx(1.0)
    assert topix500_day_1["constituent_count"] == 2


def test_stock_intraday_bundle_roundtrip(
    analytics_db_path: str,
    tmp_path: Path,
) -> None:
    result = run_stock_intraday_overnight_share_analysis(
        analytics_db_path,
        selected_groups=["TOPIX100", "TOPIX500"],
        min_session_count=1,
    )

    bundle = write_stock_intraday_overnight_share_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260331_182000_testabcd",
    )
    reloaded = load_stock_intraday_overnight_share_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == STOCK_INTRADAY_OVERNIGHT_SHARE_RESEARCH_EXPERIMENT_ID
    assert bundle.summary_path.exists()
    assert (
        get_stock_intraday_overnight_share_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_stock_intraday_overnight_share_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert reloaded.selected_groups == result.selected_groups
    assert reloaded.min_session_count == result.min_session_count
    pd.testing.assert_frame_equal(
        reloaded.group_summary_df,
        result.group_summary_df,
        check_dtype=False,
    )


def test_min_session_count_filters_stocks(analytics_db_path: str) -> None:
    result = run_stock_intraday_overnight_share_analysis(
        analytics_db_path,
        min_session_count=4,
    )

    assert result.analysis_start_date == "2024-01-02"
    assert result.analysis_end_date == "2024-01-04"
    assert result.stock_metrics_df.empty
    assert result.daily_group_shares_df.empty
    assert list(result.group_summary_df["stock_count"]) == [0, 0, 0, 0, 0]


def test_selected_date_range_filters_stock_sessions(analytics_db_path: str) -> None:
    result = run_stock_intraday_overnight_share_analysis(
        analytics_db_path,
        start_date="2024-01-03",
        end_date="2024-01-03",
        min_session_count=1,
    )

    topix100 = _stock_row(result, "TOPIX100", "7203")
    prime_ex = _stock_row(result, "PRIME ex TOPIX500", "1111")

    assert result.analysis_start_date == "2024-01-03"
    assert result.analysis_end_date == "2024-01-03"
    assert topix100["session_count"] == 1
    assert topix100["intraday_share"] == pytest.approx(0.0)
    assert topix100["overnight_share"] == pytest.approx(1.0)
    assert prime_ex["session_count"] == 1
    assert prime_ex["intraday_share"] == pytest.approx(1.0)
    assert prime_ex["overnight_share"] == pytest.approx(0.0)


def test_selected_groups_are_deduplicated_and_filtered(analytics_db_path: str) -> None:
    result = run_stock_intraday_overnight_share_analysis(
        analytics_db_path,
        selected_groups=["TOPIX100", "TOPIX100", "STANDARD"],
        min_session_count=1,
    )

    assert result.selected_groups == ("TOPIX100", "STANDARD")
    assert tuple(result.group_summary_df["stock_group"]) == ("TOPIX100", "STANDARD")


def test_selected_groups_ignore_blank_entries(analytics_db_path: str) -> None:
    result = run_stock_intraday_overnight_share_analysis(
        analytics_db_path,
        selected_groups=[" ", "TOPIX100", "", "STANDARD"],
        min_session_count=1,
    )

    assert result.selected_groups == ("TOPIX100", "STANDARD")


def test_get_stock_available_date_range_reads_stock_data(analytics_db_path: str) -> None:
    assert get_stock_available_date_range(analytics_db_path) == (
        "2024-01-02",
        "2024-01-05",
    )


def test_lock_contention_falls_back_to_snapshot(
    analytics_db_path: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_connect = analysis_module._connect_duckdb
    calls = {"count": 0}

    def flaky_connect(db_path: str, *, read_only: bool = True) -> Any:
        if read_only and calls["count"] == 0:
            calls["count"] += 1
            raise duckdb.IOException(
                'IO Error: Could not set lock on file "market.duckdb": Conflicting lock is held'
            )
        return original_connect(db_path, read_only=read_only)

    monkeypatch.setattr(analysis_module, "_connect_duckdb", flaky_connect)

    result = run_stock_intraday_overnight_share_analysis(
        analytics_db_path,
        min_session_count=1,
    )

    assert result.source_mode == "snapshot"
    assert "temporary snapshot copied from" in result.source_detail


def test_non_lock_connection_error_is_propagated(
    analytics_db_path: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failing_connect(db_path: str, *, read_only: bool = True) -> Any:
        raise RuntimeError("boom")

    monkeypatch.setattr(analysis_module, "_connect_duckdb", failing_connect)

    with pytest.raises(RuntimeError, match="boom"):
        run_stock_intraday_overnight_share_analysis(
            analytics_db_path,
            min_session_count=1,
        )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"selected_groups": []}, "selected_groups must contain at least one supported group"),
        ({"selected_groups": ["INVALID"]}, "Unsupported stock group"),
        ({"min_session_count": -1}, "min_session_count must be non-negative"),
    ],
)
def test_invalid_inputs_raise(
    analytics_db_path: str,
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        run_stock_intraday_overnight_share_analysis(
            analytics_db_path,
            **cast(dict[str, Any], kwargs),
        )
