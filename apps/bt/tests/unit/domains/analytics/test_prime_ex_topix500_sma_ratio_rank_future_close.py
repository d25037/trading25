from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.prime_ex_topix500_sma_ratio_rank_future_close import (
    get_prime_ex_topix500_sma_ratio_rank_future_close_available_date_range,
    run_prime_ex_topix500_sma_ratio_rank_future_close_research,
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
            created_at TEXT
        )
        """
    )

    prime_ex_codes = [f"{1001 + idx}" for idx in range(12)]
    stocks: list[tuple[str, str, str, str, str, str, str, str, str, str, str, None, None]] = []
    for idx, code in enumerate(prime_ex_codes, start=1):
        stocks.append(
            (
                code,
                f"PrimeEx {idx}",
                f"PRIME EX {idx}",
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
            )
        )

    stocks.extend(
        [
            (
                "10010",
                "PrimeEx Duplicate",
                "PRIME EX DUP",
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
                "9001",
                "Topix500 Included Elsewhere",
                "TOPIX500",
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
                "9002",
                "Standard Outside",
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
        ]
    )
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stocks,
    )

    dates = pd.bdate_range("2023-01-02", periods=220)
    stock_rows: list[tuple[str, str, float, float, float, float, int, float, None]] = []
    for idx, code in enumerate(prime_ex_codes, start=1):
        base_close = 1200.0 - (idx * 60.0)
        close_growth = 0.004 - (idx * 0.0005)
        base_volume = 9000.0 - (idx * 300.0)
        volume_growth = 0.0025 - (idx * 0.00025)
        for day_idx, date in enumerate(dates):
            close = base_close * ((1.0 + close_growth) ** day_idx)
            volume = int(round(base_volume * ((1.0 + volume_growth) ** day_idx)))
            stock_rows.append(
                (
                    code,
                    date.strftime("%Y-%m-%d"),
                    close * 0.995,
                    close * 1.01,
                    close * 0.99,
                    close,
                    volume,
                    1.0,
                    None,
                )
            )

    duplicate_rows: list[tuple[str, str, float, float, float, float, int, float, None]] = []
    for day_idx, date in enumerate(dates):
        close = 300.0 * ((1.0 + 0.0002) ** day_idx)
        volume = int(round(1800.0 * ((1.0 + 0.0001) ** day_idx)))
        duplicate_rows.append(
            (
                "10010",
                date.strftime("%Y-%m-%d"),
                close * 0.995,
                close * 1.01,
                close * 0.99,
                close,
                volume,
                1.0,
                None,
            )
        )

    excluded_rows: list[tuple[str, str, float, float, float, float, int, float, None]] = []
    for code, base_close, base_volume in (("9001", 500.0, 3000.0), ("9002", 450.0, 2500.0)):
        for day_idx, date in enumerate(dates):
            close = base_close * ((1.0 + 0.0003) ** day_idx)
            volume = int(round(base_volume * ((1.0 + 0.0002) ** day_idx)))
            excluded_rows.append(
                (
                    code,
                    date.strftime("%Y-%m-%d"),
                    close * 0.995,
                    close * 1.01,
                    close * 0.99,
                    close,
                    volume,
                    1.0,
                    None,
                )
            )

    conn.executemany(
        "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stock_rows + duplicate_rows + excluded_rows,
    )
    conn.close()
    return str(db_path)


@pytest.fixture
def analytics_db_path(tmp_path: Path) -> str:
    return _build_market_db(tmp_path / "market.duckdb")


def test_prime_ex_topix500_available_range_and_universe_counts(
    analytics_db_path: str,
) -> None:
    available_start, available_end = (
        get_prime_ex_topix500_sma_ratio_rank_future_close_available_date_range(
            analytics_db_path
        )
    )

    assert available_start == "2023-01-02"
    assert available_end == "2023-11-03"

    result = run_prime_ex_topix500_sma_ratio_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=12,
    )

    assert result.universe_key == "prime_ex_topix500"
    assert result.universe_label == "PRIME ex TOPIX500"
    assert result.universe_constituent_count == 12
    assert result.topix100_constituent_count == 12
    assert result.analysis_start_date == "2023-07-28"
    assert result.analysis_end_date == "2023-11-03"
    assert result.valid_date_count == 71
    assert result.stock_day_count == 852
    assert result.ranked_event_count == 5112


def test_prime_ex_topix500_excludes_topix500_and_standard_codes(
    analytics_db_path: str,
) -> None:
    result = run_prime_ex_topix500_sma_ratio_rank_future_close_research(
        analytics_db_path,
        min_constituents_per_day=12,
    )

    assert sorted(result.event_panel_df["code"].unique().tolist()) == [
        "1001",
        "1002",
        "1003",
        "1004",
        "1005",
        "1006",
        "1007",
        "1008",
        "1009",
        "1010",
        "1011",
        "1012",
    ]
