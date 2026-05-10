from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.free_float_liquidity_gap import (
    FREE_FLOAT_LIQUIDITY_GAP_EXPERIMENT_ID,
    build_summary_markdown,
    get_free_float_liquidity_gap_bundle_path_for_run_id,
    get_free_float_liquidity_gap_latest_bundle_path,
    load_free_float_liquidity_gap_bundle,
    run_free_float_liquidity_gap_research,
    write_free_float_liquidity_gap_bundle,
)


def test_free_float_liquidity_gap_builds_regression_and_buckets(tmp_path: Path) -> None:
    db_path = _build_liquidity_gap_db(tmp_path / "market.duckdb")

    result = run_free_float_liquidity_gap_research(
        db_path,
        adv_windows=(3,),
        horizons=(2,),
        change_window=3,
        observation_stride_sessions=1,
        bucket_count=2,
        min_regression_observations=3,
    )

    assert result.market_source == "stock_master_daily_exact_date"
    assert not result.observation_df.empty
    assert {"prime", "standard"}.issubset(set(result.observation_df["market_scope"]))
    assert {"liquidity_residual_z", "liquidity_implied_ffcap_gap_pct"}.issubset(
        result.observation_df.columns
    )
    assert result.observation_df["liquidity_residual_z"].notna().any()
    assert result.observation_df["liquidity_residual_change"].notna().any()

    regression = result.market_regression_df
    assert {"all", "prime", "standard"}.issubset(set(regression["market_scope"]))
    assert (regression["beta_log_free_float_market_cap"].abs() > 0).all()

    residual_bucket = result.residual_bucket_forward_return_df
    change_bucket = result.residual_change_bucket_forward_return_df
    assert not residual_bucket.empty
    assert not change_bucket.empty
    assert {"low", "high"}.issubset(set(residual_bucket["residual_bucket"]))
    assert {"low", "high"}.issubset(set(change_bucket["residual_change_bucket"]))


def test_free_float_liquidity_gap_writes_and_loads_bundle(tmp_path: Path) -> None:
    db_path = _build_liquidity_gap_db(tmp_path / "market.duckdb")
    result = run_free_float_liquidity_gap_research(
        db_path,
        adv_windows=(3,),
        horizons=(2,),
        change_window=3,
        observation_stride_sessions=2,
        bucket_count=2,
        min_regression_observations=3,
    )

    summary = build_summary_markdown(result)
    assert "Market Regression" in summary
    assert "Residual Change Bucket Forward Returns" in summary

    bundle = write_free_float_liquidity_gap_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="liquidity-gap-test",
    )
    loaded = load_free_float_liquidity_gap_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == FREE_FLOAT_LIQUIDITY_GAP_EXPERIMENT_ID
    assert (
        get_free_float_liquidity_gap_bundle_path_for_run_id(
            "liquidity-gap-test",
            output_root=tmp_path / "research",
        )
        == bundle.bundle_dir
    )
    assert (
        get_free_float_liquidity_gap_latest_bundle_path(
            output_root=tmp_path / "research",
        )
        == bundle.bundle_dir
    )
    pd.testing.assert_frame_equal(
        loaded.market_regression_df.reset_index(drop=True),
        result.market_regression_df.reset_index(drop=True),
        check_dtype=False,
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"adv_windows": (1,)}, "adv_windows must contain integers greater than 1"),
        ({"horizons": (0,)}, "horizons must be positive"),
        ({"change_window": 0}, "change_window must be positive"),
        (
            {"observation_stride_sessions": 0},
            "observation_stride_sessions must be positive",
        ),
        ({"bucket_count": 1}, "bucket_count must be at least 2"),
        (
            {"min_regression_observations": 2},
            "min_regression_observations must be at least 3",
        ),
    ],
)
def test_free_float_liquidity_gap_rejects_invalid_params(
    tmp_path: Path,
    kwargs: dict[str, Any],
    message: str,
) -> None:
    db_path = _build_liquidity_gap_db(tmp_path / "market.duckdb")

    with pytest.raises(ValueError, match=message):
        run_free_float_liquidity_gap_research(db_path, **kwargs)


def test_free_float_liquidity_gap_requires_existing_db(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        run_free_float_liquidity_gap_research(tmp_path / "missing.duckdb")


def _build_liquidity_gap_db(db_path: Path) -> Path:
    dates = [f"2024-01-{day:02d}" for day in range(2, 31)]
    codes = [
        ("1111", "Prime A", "0111", "Prime", 1_000_000.0, 100_000.0, 100.0, 9_000.0),
        ("1112", "Prime B", "0111", "Prime", 2_000_000.0, 200_000.0, 200.0, 11_000.0),
        ("1113", "Prime C", "0111", "Prime", 4_000_000.0, 400_000.0, 300.0, 13_000.0),
        ("2221", "Standard A", "0112", "Standard", 800_000.0, 80_000.0, 80.0, 8_000.0),
        (
            "2222",
            "Standard B",
            "0112",
            "Standard",
            1_500_000.0,
            150_000.0,
            120.0,
            9_500.0,
        ),
        (
            "2223",
            "Standard C",
            "0112",
            "Standard",
            3_000_000.0,
            300_000.0,
            180.0,
            12_000.0,
        ),
    ]

    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE stock_data (
            code TEXT,
            date TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE topix_data (
            date TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_master_daily (
            date TEXT,
            code TEXT,
            company_name TEXT,
            market_code TEXT,
            market_name TEXT,
            sector_33_name TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE statements (
            code TEXT,
            disclosed_date TEXT,
            shares_outstanding DOUBLE,
            treasury_shares DOUBLE
        )
        """
    )

    stock_rows: list[tuple[str, str, float, float, float, float, float]] = []
    master_rows: list[tuple[str, str, str, str, str, str]] = []
    for day_index, date in enumerate(dates):
        for (
            code,
            name,
            market_code,
            market_name,
            _shares,
            _treasury,
            close_base,
            volume_base,
        ) in codes:
            close = close_base * (1.0 + day_index * 0.002)
            volume = volume_base
            if code in {"1111", "2221"} and day_index >= 15:
                volume *= 4.0
            if code in {"1113", "2223"} and day_index >= 15:
                volume *= 0.35
            stock_rows.append(
                (code, date, close - 1.0, close + 1.0, close - 2.0, close, volume)
            )
            master_rows.append((date, code, name, market_code, market_name, "Services"))

    topix_rows = []
    for day_index, date in enumerate(dates):
        close = 1000.0 * (1.0 + day_index * 0.001)
        topix_rows.append((date, close - 1.0, close + 1.0, close - 2.0, close))

    conn.executemany("INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)", stock_rows)
    conn.executemany("INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)", topix_rows)
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?)", master_rows
    )
    conn.executemany(
        "INSERT INTO statements VALUES (?, ?, ?, ?)",
        [
            (code, "2023-12-20", shares, treasury)
            for code, _, _, _, shares, treasury, _, _ in codes
        ],
    )
    conn.close()
    return db_path
