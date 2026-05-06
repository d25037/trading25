from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.domains.analytics.annual_first_open_last_close_fundamental_panel import (
    ANNUAL_FIRST_OPEN_LAST_CLOSE_FUNDAMENTAL_PANEL_EXPERIMENT_ID,
)
from src.domains.analytics.annual_large_universe_value_profile import (
    ANNUAL_LARGE_UNIVERSE_VALUE_PROFILE_EXPERIMENT_ID,
    get_annual_large_universe_value_profile_bundle_path_for_run_id,
    get_annual_large_universe_value_profile_latest_bundle_path,
    load_annual_large_universe_value_profile_bundle,
    run_annual_large_universe_value_profile,
    write_annual_large_universe_value_profile_bundle,
)
from src.domains.analytics.research_bundle import write_research_bundle


_SCALE_CATEGORIES = (
    "TOPIX Core30",
    "TOPIX Large70",
    "TOPIX Mid400",
    "TOPIX Small 1",
)
_MARKET_NAMES = {"prime": "Prime", "standard": "Standard"}
_MARKET_CODES = {"prime": "0111", "standard": "0112"}


def _sample_event_ledger() -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for year_index, year in enumerate(("2021", "2022", "2023")):
        for scale_index, scale_category in enumerate(_SCALE_CATEGORIES):
            for rank in range(8):
                code = f"{scale_index + 1}{year_index}{rank:02d}"
                market = "prime" if scale_category != "TOPIX Mid400" or rank < 6 else "standard"
                value_strength = 7 - rank
                return_pct = (
                    -4.0
                    + value_strength * 1.8
                    + (1.2 if scale_category == "TOPIX Mid400" else 0.0)
                    + year_index * 0.3
                )
                entry_open = 100.0
                exit_close = entry_open * (1.0 + return_pct / 100.0)
                records.append(
                    {
                        "event_id": f"{code}:{year}",
                        "year": year,
                        "code": code,
                        "company_name": f"Stock {code}",
                        "market": market,
                        "market_code": _MARKET_CODES[market],
                        "sector_33_name": "Machinery" if rank % 2 == 0 else "Services",
                        "scale_category": scale_category,
                        "status": "realized",
                        "entry_date": f"{year}-01-04",
                        "exit_date": f"{year}-12-30",
                        "entry_open": entry_open,
                        "exit_close": exit_close,
                        "event_return_pct": return_pct,
                        "pbr": 0.45 + rank * 0.10 + scale_index * 0.03,
                        "forward_per": 5.0 + rank * 0.9 + scale_index * 0.2,
                        "market_cap_bil_jpy": 500.0 - scale_index * 80.0 + rank * 20.0,
                        "avg_trading_value_60d_mil_jpy": 300.0 - scale_index * 30.0 + rank * 5.0,
                    }
                )
    return pd.DataFrame(records)


def _build_market_db(db_path: Path, event_ledger_df: pd.DataFrame) -> str:
    conn = duckdb.connect(str(db_path))
    try:
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
        stock_rows = [
            (
                str(row["code"]),
                str(row["company_name"]),
                None,
                str(row["market_code"]),
                _MARKET_NAMES[str(row["market"])],
                "1",
                "A",
                "1",
                str(row["sector_33_name"]),
                str(row["scale_category"]),
                "2000-01-01",
                None,
                None,
            )
            for row in event_ledger_df.to_dict(orient="records")
        ]
        price_rows: list[tuple[object, ...]] = []
        for row in event_ledger_df.to_dict(orient="records"):
            code = str(row["code"])
            year = str(row["year"])
            entry_open = float(row["entry_open"])
            exit_close = float(row["exit_close"])
            mid_close = (entry_open + exit_close) / 2.0
            price_rows.extend(
                [
                    (
                        code,
                        f"{year}-01-04",
                        entry_open,
                        max(entry_open, mid_close),
                        min(entry_open, mid_close),
                        mid_close,
                        100_000,
                        1.0,
                        None,
                    ),
                    (
                        code,
                        f"{year}-12-30",
                        mid_close,
                        max(mid_close, exit_close),
                        min(mid_close, exit_close),
                        exit_close,
                        100_000,
                        1.0,
                        None,
                    ),
                ]
            )
        conn.executemany("INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", stock_rows)
        conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", price_rows)
    finally:
        conn.close()
    return str(db_path)


def _write_input_bundle(tmp_path: Path, db_path: str, event_ledger_df: pd.DataFrame) -> Path:
    bundle = write_research_bundle(
        experiment_id=ANNUAL_FIRST_OPEN_LAST_CLOSE_FUNDAMENTAL_PANEL_EXPERIMENT_ID,
        module="tests.fixture",
        function="build",
        params={},
        db_path=db_path,
        analysis_start_date="2021-01-04",
        analysis_end_date="2023-12-30",
        result_metadata={"db_path": db_path},
        result_tables={"event_ledger_df": event_ledger_df},
        summary_markdown="# fixture\n",
        output_root=tmp_path,
        run_id="input-panel",
    )
    return bundle.bundle_dir


def test_run_annual_large_universe_value_profile_builds_profile_tables(tmp_path: Path) -> None:
    event_ledger_df = _sample_event_ledger()
    db_path = _build_market_db(tmp_path / "market.duckdb", event_ledger_df)
    input_bundle = _write_input_bundle(tmp_path, db_path, event_ledger_df)

    result = run_annual_large_universe_value_profile(
        input_bundle,
        output_root=tmp_path,
        selection_fractions=(0.25,),
        min_observations=5,
    )

    assert result.input_run_id == "input-panel"
    assert set(result.large_universe_scored_panel_df["large_universe"].astype(str)) == {
        "topix100",
        "topix500",
    }
    assert "TOPIX Small 1" not in set(result.large_universe_scored_panel_df["scale_category"].astype(str))
    assert not result.factor_regression_df.empty
    assert not result.factor_bucket_summary_df.empty
    assert not result.selected_event_df.empty
    assert not result.portfolio_summary_df.empty
    assert not result.profile_summary_df.empty
    assert {"prime_size_tilt", "standard_pbr_tilt"}.issubset(
        set(result.profile_summary_df["score_method"].astype(str))
    )


def test_write_and_load_annual_large_universe_value_profile_bundle(tmp_path: Path) -> None:
    event_ledger_df = _sample_event_ledger()
    db_path = _build_market_db(tmp_path / "market.duckdb", event_ledger_df)
    input_bundle = _write_input_bundle(tmp_path, db_path, event_ledger_df)
    result = run_annual_large_universe_value_profile(
        input_bundle,
        output_root=tmp_path,
        selection_fractions=(0.25,),
        min_observations=5,
    )

    bundle = write_annual_large_universe_value_profile_bundle(
        result,
        output_root=tmp_path,
        run_id="large-universe-profile",
    )
    loaded = load_annual_large_universe_value_profile_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == ANNUAL_LARGE_UNIVERSE_VALUE_PROFILE_EXPERIMENT_ID
    assert (
        get_annual_large_universe_value_profile_bundle_path_for_run_id(
            "large-universe-profile",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_annual_large_universe_value_profile_latest_bundle_path(
        output_root=tmp_path,
    ) == bundle.bundle_dir
    pd.testing.assert_frame_equal(
        loaded.profile_summary_df.reset_index(drop=True),
        result.profile_summary_df.reset_index(drop=True),
        check_dtype=False,
    )
