from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


def _create_stock_tables(conn: duckdb.DuckDBPyConnection) -> None:
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


def _create_regime_tables(conn: duckdb.DuckDBPyConnection) -> None:
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


def build_topix100_research_market_db(
    db_path: Path,
    *,
    include_regimes: bool = False,
) -> str:
    conn = duckdb.connect(str(db_path))
    _create_stock_tables(conn)
    if include_regimes:
        _create_regime_tables(conn)

    stocks = [
        ("1111", "Alpha", "ALPHA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Core30", "2000-01-01", None, None),
        ("11110", "Alpha Duplicate", "ALPHA DUP", "0111", "プライム", "1", "A", "1", "A", "TOPIX Core30", "2000-01-01", None, None),
        ("2222", "Beta", "BETA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Core30", "2000-01-01", None, None),
        ("3333", "Gamma", "GAMMA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Core30", "2000-01-01", None, None),
        ("4444", "Delta", "DELTA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("5555", "Epsilon", "EPSILON", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("6666", "Zeta", "ZETA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("7777", "Eta", "ETA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("8888", "Theta", "THETA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("9999", "Iota", "IOTA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("1234", "Kappa", "KAPPA", "0111", "プライム", "1", "A", "1", "A", "TOPIX Large70", "2000-01-01", None, None),
        ("4321", "Outside", "OUTSIDE", "0111", "プライム", "1", "A", "1", "A", "-", "2000-01-01", None, None),
    ]
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stocks,
    )

    dates = pd.bdate_range("2023-01-02", periods=220)
    specs = {
        "1111": (1000.0, 0.0045, 10000.0, 0.0030),
        "2222": (900.0, 0.0035, 9000.0, 0.0025),
        "3333": (800.0, 0.0025, 8000.0, 0.0020),
        "4444": (700.0, 0.0015, 7000.0, 0.0015),
        "5555": (600.0, 0.0008, 6000.0, 0.0010),
        "6666": (500.0, -0.0002, 5000.0, 0.0002),
        "7777": (400.0, -0.0008, 4000.0, -0.0004),
        "8888": (300.0, -0.0015, 3000.0, -0.0010),
        "9999": (200.0, -0.0022, 2000.0, -0.0015),
        "1234": (100.0, -0.0030, 1000.0, -0.0020),
        "4321": (50.0, 0.0002, 1500.0, 0.0003),
    }

    stock_rows: list[tuple[str, str, float, float, float, float, int, float, None]] = []
    for code, (base_close, close_growth, base_volume, volume_growth) in specs.items():
        for index, date in enumerate(dates):
            close = base_close * ((1.0 + close_growth) ** index)
            volume = int(round(base_volume * ((1.0 + volume_growth) ** index)))
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
    for index, date in enumerate(dates):
        close = 200.0 * ((1.0 + 0.0002) ** index)
        volume = int(round(1000.0 * ((1.0 + 0.0001) ** index)))
        duplicate_rows.append(
            (
                "11110",
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
        stock_rows + duplicate_rows,
    )

    if include_regimes:
        topix_rows: list[tuple[str, float, float, float, float, None]] = []
        index_rows: list[tuple[str, str, float]] = []
        topix_close = 2000.0
        nt_ratio = 14.0
        topix_pattern = [-0.03, -0.015, 0.0, 0.015, 0.03]
        nt_pattern = [-0.04, -0.02, 0.0, 0.02, 0.04]
        for index, date in enumerate(dates):
            if index > 0:
                topix_close *= 1.0 + topix_pattern[index % len(topix_pattern)]
                nt_ratio *= 1.0 + nt_pattern[index % len(nt_pattern)]
            topix_rows.append(
                (
                    date.strftime("%Y-%m-%d"),
                    topix_close * 0.995,
                    topix_close * 1.01,
                    topix_close * 0.99,
                    topix_close,
                    None,
                )
            )
            index_rows.append(
                ("N225_UNDERPX", date.strftime("%Y-%m-%d"), topix_close * nt_ratio)
            )

        conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)", topix_rows)
        conn.executemany("INSERT INTO indices_data VALUES (?, ?, ?)", index_rows)

    conn.close()
    return str(db_path)


def build_prime_ex_topix500_research_market_db(db_path: Path) -> str:
    conn = duckdb.connect(str(db_path))
    _create_stock_tables(conn)

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
