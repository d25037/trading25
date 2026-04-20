from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics.speculative_volume_surge_pullback_edge import (
    PULLBACK_BUCKET_SUMMARY_COLUMNS,
    TOP_EXAMPLES_COLUMNS,
    SPECULATIVE_VOLUME_SURGE_PULLBACK_EDGE_EXPERIMENT_ID,
    _bucket_pullback_position,
    get_speculative_volume_surge_pullback_edge_bundle_path_for_run_id,
    get_speculative_volume_surge_pullback_edge_latest_bundle_path,
    load_speculative_volume_surge_pullback_edge_research_bundle,
    run_speculative_volume_surge_pullback_edge_research,
    write_speculative_volume_surge_pullback_edge_research_bundle,
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

    stock_rows = [
        ("1000", "positive-edge", "positive-edge", "0112", "スタンダード", "1", "A", "1", "A", "-", "2010-01-01", None, None),
        ("2000", "negative-edge", "negative-edge", "0113", "グロース", "1", "A", "1", "A", "-", "2010-01-01", None, None),
        ("3000", "zero-ten", "zero-ten", "0111", "プライム", "1", "A", "1", "A", "TOPIX Mid400", "2010-01-01", None, None),
    ]
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stock_rows,
    )

    dates = pd.bdate_range("2023-01-02", periods=130)

    def make_row(
        code: str,
        date: pd.Timestamp,
        *,
        close: float,
        high: float,
        low: float,
        volume: int,
    ) -> tuple[object, ...]:
        open_value = close * 0.995
        return (
            code,
            date.strftime("%Y-%m-%d"),
            float(open_value),
            float(high),
            float(low),
            float(close),
            int(volume),
            1.0,
            None,
        )

    price_rows: list[tuple[object, ...]] = []
    for day_index, date in enumerate(dates):
        for code in ("1000", "2000", "3000"):
            close = 100.0
            high = 101.0
            low = 99.0
            volume = 1_000

            if day_index == 40:
                close = 112.0
                high = 140.0 if code != "3000" else 135.0
                low = 108.0
                volume = 20_000
            elif day_index == 41:
                close = 130.0 if code != "3000" else 124.0
                high = 150.0 if code != "3000" else 145.0
                low = 125.0 if code != "3000" else 120.0
                volume = 2_000
            elif day_index == 42:
                if code in {"1000", "2000"}:
                    close, high, low, volume = 118.0, 119.0, 116.0, 1_400
                else:
                    close, high, low, volume = 105.0, 106.0, 103.0, 1_300
            elif 43 <= day_index <= 54:
                if code == "1000":
                    close = 117.0 + (day_index - 43) * 0.3
                    high = 118.0 + (day_index - 43) * 0.6
                    low = 114.0
                    if day_index == 54:
                        high = 165.0
                        close = 150.0
                        low = 112.0
                    volume = 1_200
                elif code == "2000":
                    close = 116.0 - (day_index - 43) * 0.5
                    high = 118.0
                    low = 90.0 if day_index == 54 else 110.0 - (day_index - 43) * 0.7
                    if day_index == 54:
                        close = 90.0
                        high = 120.0
                    volume = 1_150
                else:
                    close = 104.0 + (day_index - 43) * 0.6
                    high = 107.0 + (day_index - 43) * 0.8
                    low = 102.0
                    if day_index == 54:
                        high = 125.0
                        close = 120.0
                    volume = 1_100
            elif day_index > 54:
                if code == "1000":
                    close, high, low, volume = 150.0, 152.0, 145.0, 1_050
                elif code == "2000":
                    close, high, low, volume = 92.0, 95.0, 88.0, 1_020
                else:
                    close, high, low, volume = 120.0, 122.0, 118.0, 1_010

            price_rows.append(
                make_row(
                    code,
                    date,
                    close=close,
                    high=high,
                    low=low,
                    volume=volume,
                )
            )

    conn.executemany(
        "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        price_rows,
    )
    conn.close()
    return str(db_path)


def test_speculative_volume_surge_pullback_edge_bundle_roundtrip(tmp_path: Path) -> None:
    db_path = _build_market_db(tmp_path / "market.duckdb")

    result = run_speculative_volume_surge_pullback_edge_research(
        db_path,
        lookback_years=1,
        future_horizons=(20, 40),
        primary_horizon=40,
        sample_size=2,
    )

    assert result.total_primary_episode_count == 3
    assert result.pullback_state_count >= 3
    assert set(result.pullback_state_df["pullback_bucket"]) >= {"10-20%", "0-10%"}
    bucket_row = result.pullback_bucket_summary_df.loc[
        (result.pullback_bucket_summary_df["future_horizon_days"] == 40)
        & (result.pullback_bucket_summary_df["pullback_bucket"] == "10-20%")
    ].iloc[0]
    assert int(bucket_row["observation_count"]) >= 2
    assert 0.0 <= float(bucket_row["peak_reclaim_rate"]) <= 1.0
    assert 0.0 <= float(bucket_row["upside_gt_downside_rate"]) <= 1.0
    zero_ten_row = result.pullback_bucket_summary_df.loc[
        (result.pullback_bucket_summary_df["future_horizon_days"] == 40)
        & (result.pullback_bucket_summary_df["pullback_bucket"] == "0-10%")
    ].iloc[0]
    assert int(zero_ten_row["observation_count"]) >= 1
    assert set(result.top_examples_df.columns) == set(TOP_EXAMPLES_COLUMNS)
    assert set(result.pullback_bucket_summary_df.columns) == set(PULLBACK_BUCKET_SUMMARY_COLUMNS)

    bundle = write_speculative_volume_surge_pullback_edge_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260420_140000_testabcd",
    )
    reloaded = load_speculative_volume_surge_pullback_edge_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == SPECULATIVE_VOLUME_SURGE_PULLBACK_EDGE_EXPERIMENT_ID
    assert (
        get_speculative_volume_surge_pullback_edge_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_speculative_volume_surge_pullback_edge_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.pullback_bucket_summary_df,
        result.pullback_bucket_summary_df,
        check_dtype=False,
    )


def test_speculative_volume_surge_pullback_edge_helpers() -> None:
    assert _bucket_pullback_position(None) == "missing"
    assert _bucket_pullback_position(-0.12) == "<-10%"
    assert _bucket_pullback_position(-0.02) == "-10% to 0%"
    assert _bucket_pullback_position(0.05) == "0-10%"
    assert _bucket_pullback_position(0.18) == "10-20%"
    assert _bucket_pullback_position(0.75) == "50%+"


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"lookback_years": 0}, "lookback_years must be positive"),
        ({"price_jump_threshold": 0.0}, "price_jump_threshold must be positive"),
        ({"volume_ratio_threshold": 0.0}, "volume_ratio_threshold must be positive"),
        ({"volume_window": 0}, "volume_window must be positive"),
        ({"adv_window": 0}, "adv_window must be positive"),
        ({"cooldown_sessions": -1}, "cooldown_sessions must be non-negative"),
        ({"initial_peak_window": 0}, "initial_peak_window must be positive"),
        (
            {"pullback_search_window": 5},
            "pullback_search_window must be greater than initial_peak_window",
        ),
        ({"sample_size": 0}, "sample_size must be positive"),
        (
            {"primary_horizon": 30},
            "primary_horizon must be included in future_horizons",
        ),
    ],
)
def test_speculative_volume_surge_pullback_edge_validation_errors(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        run_speculative_volume_surge_pullback_edge_research(
            "/tmp/unused.duckdb",
            future_horizons=(20, 40),
            **kwargs,
        )
