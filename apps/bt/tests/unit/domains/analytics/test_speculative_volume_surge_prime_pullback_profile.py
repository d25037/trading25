from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics.speculative_volume_surge_prime_pullback_profile import (
    ADV_BUCKET_SUMMARY_COLUMNS,
    PULLBACK_BUCKET_SUMMARY_COLUMNS,
    TOP_EXAMPLES_COLUMNS,
    SPECULATIVE_VOLUME_SURGE_PRIME_PULLBACK_PROFILE_EXPERIMENT_ID,
    _bucket_pullback_position,
    get_speculative_volume_surge_prime_pullback_profile_bundle_path_for_run_id,
    get_speculative_volume_surge_prime_pullback_profile_latest_bundle_path,
    load_speculative_volume_surge_prime_pullback_profile_research_bundle,
    run_speculative_volume_surge_prime_pullback_profile_research,
    write_speculative_volume_surge_prime_pullback_profile_research_bundle,
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
        ("1000", "prime-shallow", "prime-shallow", "0111", "プライム", "1", "A", "1", "A", "-", "2010-01-01", None, None),
        ("2000", "prime-deep", "prime-deep", "0111", "プライム", "1", "A", "1", "A", "-", "2010-01-01", None, None),
        ("3000", "prime-zero-ten", "prime-zero-ten", "0111", "プライム", "1", "A", "1", "A", "TOPIX Mid400", "2010-01-01", None, None),
        ("4000", "standard-ignore", "standard-ignore", "0112", "スタンダード", "1", "A", "1", "A", "-", "2010-01-01", None, None),
    ]
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stock_rows,
    )

    dates = pd.bdate_range("2023-01-02", periods=140)
    base_volume_map = {
        "1000": 100_000,   # <50m ADV
        "2000": 120_000,   # <50m ADV
        "3000": 1_000_000, # 50m-200m ADV
        "4000": 150_000,
    }

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
        for code in ("1000", "2000", "3000", "4000"):
            base_volume = base_volume_map[code]
            close = 100.0
            high = 101.0
            low = 99.0
            volume = base_volume

            if day_index == 40:
                close = 112.0
                high = 130.0
                low = 108.0
                volume = base_volume * 25
            elif day_index == 41:
                if code == "1000":
                    close, high, low, volume = 125.0, 140.0, 120.0, base_volume * 4
                elif code == "2000":
                    close, high, low, volume = 126.0, 142.0, 121.0, base_volume * 4
                elif code == "3000":
                    close, high, low, volume = 123.0, 138.0, 119.0, base_volume * 3
                else:
                    close, high, low, volume = 124.0, 139.0, 120.0, base_volume * 3
            elif 42 <= day_index <= 55:
                if code == "1000":
                    if day_index == 43:
                        close, high, low = 115.0, 117.0, 112.0
                    elif day_index < 55:
                        close = 116.0 + (day_index - 44) * 0.8
                        high = close + 2.0
                        low = close - 3.0
                    else:
                        close, high, low = 126.0, 132.0, 122.0
                    volume = base_volume * 2
                elif code == "2000":
                    if day_index == 44:
                        close, high, low = 97.0, 100.0, 94.0
                    elif day_index < 55:
                        close = 99.0 - (day_index - 45) * 0.3
                        high = close + 2.0
                        low = close - 4.0
                    else:
                        close, high, low = 95.0, 98.0, 91.0
                    volume = int(base_volume * 1.8)
                elif code == "3000":
                    if day_index == 42:
                        close, high, low = 107.0, 109.0, 104.0
                    elif day_index < 55:
                        close = 108.0 + (day_index - 43) * 0.5
                        high = close + 2.0
                        low = close - 2.5
                    else:
                        close, high, low = 114.0, 120.0, 111.0
                    volume = int(base_volume * 1.5)
                else:
                    close = 110.0 - (day_index - 42) * 0.8
                    high = close + 2.0
                    low = close - 3.0
                    volume = int(base_volume * 1.6)
            elif day_index > 55:
                if code == "1000":
                    close, high, low, volume = 126.0, 130.0, 122.0, base_volume
                elif code == "2000":
                    close, high, low, volume = 95.0, 97.0, 91.0, base_volume
                elif code == "3000":
                    close, high, low, volume = 114.0, 118.0, 111.0, base_volume
                else:
                    close, high, low, volume = 96.0, 98.0, 93.0, base_volume

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


def test_speculative_volume_surge_prime_pullback_profile_bundle_roundtrip(
    tmp_path: Path,
) -> None:
    db_path = _build_market_db(tmp_path / "market.duckdb")

    result = run_speculative_volume_surge_prime_pullback_profile_research(
        db_path,
        lookback_years=1,
        future_horizons=(20, 40),
        primary_horizon=20,
        sample_size=2,
    )

    assert result.market_name == "プライム"
    assert result.total_prime_episode_count == 3
    assert result.prime_pullback_profile_count == 3
    assert set(result.prime_pullback_profile_df["deepest_pullback_bucket"]) >= {
        "<0%",
        "0-10%",
        "10-20%",
    }
    shallow_row = result.pullback_bucket_summary_df.loc[
        (result.pullback_bucket_summary_df["future_horizon_days"] == 20)
        & (result.pullback_bucket_summary_df["pullback_bucket"] == "10-20%")
    ].iloc[0]
    deep_row = result.pullback_bucket_summary_df.loc[
        (result.pullback_bucket_summary_df["future_horizon_days"] == 20)
        & (result.pullback_bucket_summary_df["pullback_bucket"] == "<0%")
    ].iloc[0]
    assert float(shallow_row["median_future_close_return_pct"]) > 0.0
    assert float(shallow_row["median_future_close_return_pct"]) > float(
        deep_row["median_future_close_return_pct"]
    )
    assert float(shallow_row["median_asymmetry_pct"]) > float(
        deep_row["median_asymmetry_pct"]
    )
    assert set(result.adv_bucket_summary_df.columns) == set(ADV_BUCKET_SUMMARY_COLUMNS)
    assert set(result.pullback_bucket_summary_df.columns) == set(PULLBACK_BUCKET_SUMMARY_COLUMNS)
    assert set(result.top_examples_df.columns) == set(TOP_EXAMPLES_COLUMNS)

    bundle = write_speculative_volume_surge_prime_pullback_profile_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260420_180000_testabcd",
    )
    reloaded = load_speculative_volume_surge_prime_pullback_profile_research_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == SPECULATIVE_VOLUME_SURGE_PRIME_PULLBACK_PROFILE_EXPERIMENT_ID
    )
    assert (
        get_speculative_volume_surge_prime_pullback_profile_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_speculative_volume_surge_prime_pullback_profile_latest_bundle_path(
            output_root=tmp_path
        )
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.pullback_bucket_summary_df,
        result.pullback_bucket_summary_df,
        check_dtype=False,
    )


def test_speculative_volume_surge_prime_pullback_profile_helpers() -> None:
    assert _bucket_pullback_position(None) == "missing"
    assert _bucket_pullback_position(-0.02) == "<0%"
    assert _bucket_pullback_position(0.05) == "0-10%"
    assert _bucket_pullback_position(0.18) == "10-20%"
    assert _bucket_pullback_position(0.70) == "35%+"


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
def test_speculative_volume_surge_prime_pullback_profile_validation_errors(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        run_speculative_volume_surge_prime_pullback_profile_research(
            "/tmp/missing.duckdb",
            future_horizons=(20, 40),
            **kwargs,
        )
