from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt

from src.domains.analytics.falling_knife_reversal_study import (
    FALLING_KNIFE_REVERSAL_STUDY_EXPERIMENT_ID,
    get_falling_knife_reversal_study_bundle_path_for_run_id,
    get_falling_knife_reversal_study_latest_bundle_path,
    load_falling_knife_reversal_study_bundle,
    run_falling_knife_reversal_study,
    write_falling_knife_reversal_study_bundle,
)


def _create_market_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE stocks (
            code TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            company_name_english TEXT,
            market_code TEXT,
            market_name TEXT,
            sector_17_code TEXT,
            sector_17_name TEXT,
            sector_33_code TEXT,
            sector_33_name TEXT,
            scale_category TEXT,
            listed_date TEXT,
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


def _price_path() -> list[float]:
    prices: list[float] = []
    price = 100.0
    for day in range(120):
        if day < 55:
            price *= 1.001
        elif day < 80:
            price *= 0.975
        elif day < 88:
            price *= 0.998
        else:
            price *= 1.012
        prices.append(price)
    return prices


def _build_test_market_db(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        _create_market_tables(conn)
        conn.executemany(
            """
            INSERT INTO stocks (
                code,
                company_name,
                company_name_english,
                market_code,
                market_name,
                sector_17_code,
                sector_17_name,
                sector_33_code,
                sector_33_name,
                scale_category,
                listed_date,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "1111",
                    "Prime Falling Knife",
                    "Prime Falling Knife",
                    "0111",
                    "プライム",
                    "1",
                    "A",
                    "1",
                    "A",
                    "TOPIX Small 1",
                    "2000-01-01",
                    None,
                    None,
                ),
                (
                    "2222",
                    "Prime Stable",
                    "Prime Stable",
                    "0111",
                    "プライム",
                    "1",
                    "A",
                    "1",
                    "A",
                    "TOPIX Small 1",
                    "2000-01-01",
                    None,
                    None,
                ),
            ],
        )
        dates = pd.bdate_range("2025-01-06", periods=120).strftime("%Y-%m-%d")
        falling_prices = _price_path()
        stable_prices = [100.0 * (1.0005**idx) for idx in range(120)]
        rows: list[tuple[object, ...]] = []
        for code, prices in (("1111", falling_prices), ("2222", stable_prices)):
            for date, close in zip(dates, prices, strict=True):
                open_price = close * 0.995
                rows.append(
                    (
                        code,
                        date,
                        open_price,
                        close * 1.01,
                        close * 0.99,
                        close,
                        100_000,
                        1.0,
                        None,
                    )
                )
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
            rows,
        )
    finally:
        conn.close()


def test_falling_knife_reversal_study_uses_daily_risk_adjusted_return(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "market.duckdb"
    _build_test_market_db(db_path)

    result = run_falling_knife_reversal_study(
        str(db_path),
        market_codes=("0111",),
        forward_horizons=(5, 20),
        risk_adjusted_lookback=10,
        condition_ratio_type="sortino",
        min_condition_count=2,
        max_wait_days=20,
        signal_cooldown_days=5,
    )

    assert result.event_count > 0
    assert result.wait_candidate_count > 0
    assert "risk_adjusted_return_10_sharpe" in result.event_df.columns
    assert "risk_adjusted_return_10_sortino" in result.event_df.columns
    assert bool(result.event_df["poor_risk_adjusted_return"].any())
    assert {"catch_next_open", "wait_for_stabilization"}.issubset(
        set(result.trade_summary_df["strategy_family"])
    )
    assert (result.trade_summary_df["horizon_days"] == 20).any()
    assert not result.condition_profile_df.empty


def test_falling_knife_reversal_study_bundle_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "market.duckdb"
    _build_test_market_db(db_path)
    result = run_falling_knife_reversal_study(
        str(db_path),
        market_codes=("0111",),
        forward_horizons=(5,),
        risk_adjusted_lookback=10,
        max_wait_days=20,
        signal_cooldown_days=5,
    )

    bundle = write_falling_knife_reversal_study_bundle(
        result,
        output_root=tmp_path,
        run_id="20260427_120000_testabcd",
    )
    reloaded = load_falling_knife_reversal_study_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == FALLING_KNIFE_REVERSAL_STUDY_EXPERIMENT_ID
    assert bundle.summary_path.exists()
    assert (
        get_falling_knife_reversal_study_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_falling_knife_reversal_study_latest_bundle_path(
        output_root=tmp_path,
    ) == bundle.bundle_dir
    assert reloaded.risk_adjusted_lookback == 10
    assert reloaded.signal_cooldown_days == 5
    pdt.assert_frame_equal(reloaded.event_df, result.event_df, check_dtype=False)
    pdt.assert_frame_equal(
        reloaded.trade_summary_df,
        result.trade_summary_df,
        check_dtype=False,
    )
