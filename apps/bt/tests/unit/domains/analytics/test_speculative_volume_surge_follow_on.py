from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics.speculative_volume_surge_follow_on import (
    CONTROL_COHORT_SUMMARY_COLUMNS,
    FOLLOW_ON_SUMMARY_COLUMNS,
    SPECULATIVE_VOLUME_SURGE_FOLLOW_ON_EXPERIMENT_ID,
    SIZE_LIQUIDITY_SUMMARY_COLUMNS,
    TOP_EXAMPLES_COLUMNS,
    _bucket_adv20_jpy,
    _bucket_extension_pct,
    _bucket_price,
    _dedupe_candidate_events,
    _enrich_episode_df,
    get_speculative_volume_surge_follow_on_bundle_path_for_run_id,
    get_speculative_volume_surge_follow_on_latest_bundle_path,
    load_speculative_volume_surge_follow_on_research_bundle,
    run_speculative_volume_surge_follow_on_research,
    write_speculative_volume_surge_follow_on_research_bundle,
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
        ("1000", "follow-on", "follow-on", "0112", "スタンダード", "1", "A", "1", "A", "-", "2010-01-01", None, None),
        ("2000", "no-follow", "no-follow", "0113", "グロース", "1", "A", "1", "A", "-", "2010-01-01", None, None),
        ("3000", "price-only", "price-only", "0111", "プライム", "1", "A", "1", "A", "TOPIX Mid400", "2010-01-01", None, None),
        ("4000", "volume-only", "volume-only", "0112", "スタンダード", "1", "A", "1", "A", "-", "2010-01-01", None, None),
    ]
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stock_rows,
    )

    dates = pd.bdate_range("2023-01-02", periods=130)

    def row(
        code: str,
        date: pd.Timestamp,
        *,
        close: float,
        high: float,
        volume: int,
        open_price: float | None = None,
        low: float | None = None,
    ) -> tuple[object, ...]:
        open_value = open_price if open_price is not None else close * 0.995
        low_value = low if low is not None else min(open_value, close) * 0.99
        return (
            code,
            date.strftime("%Y-%m-%d"),
            float(open_value),
            float(high),
            float(low_value),
            float(close),
            int(volume),
            1.0,
            None,
        )

    price_rows: list[tuple[object, ...]] = []
    for date_index, date in enumerate(dates):
        for code in ("1000", "2000", "3000", "4000"):
            close = 100.0
            high = 101.0
            volume = 1_000

            if code == "1000" and date_index == 40:
                close, high, volume = 112.0, 130.0, 20_000
            elif code == "1000" and 41 <= date_index <= 54:
                close = 113.0 + (date_index - 41) * 0.4
                high = min(130.0, close * 1.02)
                volume = 1_200
            elif code == "1000" and date_index == 55:
                close, high, volume = 132.0, 150.0, 22_000
            elif code == "1000" and date_index > 55:
                close = 132.0 + (date_index - 55) * 0.1
                high = close * 1.02
                volume = 1_300
            elif code == "2000" and date_index == 40:
                close, high, volume = 112.0, 130.0, 20_000
            elif code == "2000" and date_index > 40:
                close = 110.0 + ((date_index - 41) % 5) * 0.8
                high = min(129.0, close * 1.03)
                volume = 1_150
            elif code == "3000" and date_index == 40:
                close, high, volume = 112.0, 130.0, 1_500
            elif code == "3000" and date_index > 40:
                close = 111.0 + ((date_index - 41) % 3) * 0.4
                high = min(128.0, close * 1.02)
                volume = 1_100
            elif code == "4000" and date_index == 40:
                close, high, volume = 104.0, 106.0, 20_000
            elif code == "4000" and date_index > 40:
                close = 103.0 + ((date_index - 41) % 4) * 0.25
                high = close * 1.01
                volume = 1_050

            price_rows.append(row(code, date, close=close, high=high, volume=volume))

    conn.executemany(
        "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        price_rows,
    )
    conn.close()
    return str(db_path)


def test_speculative_volume_surge_follow_on_research_bundle_roundtrip(
    tmp_path: Path,
) -> None:
    db_path = _build_market_db(tmp_path / "market.duckdb")

    result = run_speculative_volume_surge_follow_on_research(
        db_path,
        lookback_years=1,
        sample_size=2,
    )

    assert result.total_primary_candidate_count == 3
    assert result.total_primary_episode_count == 2
    assert set(result.event_ledger_df["code"]) == {"1000", "2000"}
    assert bool(
        result.event_ledger_df.loc[result.event_ledger_df["code"] == "1000", "breakout_follow_on_extension5d_gap10d_window40d"]
        .iloc[0]
    )
    assert not bool(
        result.event_ledger_df.loc[result.event_ledger_df["code"] == "2000", "breakout_follow_on_extension5d_gap10d_window40d"]
        .iloc[0]
    )
    assert bool(
        result.event_ledger_df.loc[result.event_ledger_df["code"] == "1000", "secondary_surge_gap10d_window40d"]
        .iloc[0]
    )
    assert set(result.control_cohort_summary_df["cohort_key"]) == {
        "surge_price_and_volume",
        "surge_price_only",
        "surge_volume_only",
    }
    assert set(result.top_examples_df["example_group"]) == {
        "follow_on_breakout",
        "no_follow_on_breakout",
    }
    assert set(result.follow_on_summary_df.columns) == set(FOLLOW_ON_SUMMARY_COLUMNS)
    assert set(result.size_liquidity_summary_df.columns) == set(SIZE_LIQUIDITY_SUMMARY_COLUMNS)
    assert set(result.control_cohort_summary_df.columns) == set(CONTROL_COHORT_SUMMARY_COLUMNS)
    assert set(result.top_examples_df.columns) == set(TOP_EXAMPLES_COLUMNS)

    bundle = write_speculative_volume_surge_follow_on_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260420_120000_testabcd",
    )
    reloaded = load_speculative_volume_surge_follow_on_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == SPECULATIVE_VOLUME_SURGE_FOLLOW_ON_EXPERIMENT_ID
    assert (
        get_speculative_volume_surge_follow_on_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_speculative_volume_surge_follow_on_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(reloaded.event_ledger_df, result.event_ledger_df, check_dtype=False)
    pdt.assert_frame_equal(
        reloaded.control_cohort_summary_df,
        result.control_cohort_summary_df,
        check_dtype=False,
    )


def test_speculative_volume_surge_helpers_and_empty_paths() -> None:
    assert _bucket_extension_pct(None) == "missing"
    assert _bucket_extension_pct(0.19) == "10-20%"
    assert _bucket_extension_pct(0.21) == "20-35%"
    assert _bucket_extension_pct(0.40) == "35-50%"
    assert _bucket_adv20_jpy(25_000_000) == "<50m"
    assert _bucket_adv20_jpy(500_000_000) == "200m-1000m"
    assert _bucket_price(80.0) == "<100"
    assert _bucket_price(400.0) == "300-1000"

    candidate_df = pd.DataFrame(
        {
            "code": ["1000", "1000", "1000"],
            "date": ["2024-01-01", "2024-01-05", "2024-02-20"],
            "session_index": [20, 23, 50],
        }
    )
    deduped = _dedupe_candidate_events(candidate_df, cooldown_sessions=20)
    assert deduped["date"].tolist() == ["2024-01-01", "2024-02-20"]

    empty_enriched = _enrich_episode_df(
        pd.DataFrame(),
        extension_windows=(0, 5),
        full_extension_windows=(20, 60),
        follow_on_gaps=(5, 10),
        follow_on_windows=(20, 40),
    )
    assert empty_enriched.empty
    assert "breakout_follow_on_extension5d_gap10d_window40d" in empty_enriched.columns


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"lookback_years": 0}, "lookback_years must be positive"),
        ({"price_jump_threshold": 0.0}, "price_jump_threshold must be positive"),
        ({"volume_ratio_threshold": 0.0}, "volume_ratio_threshold must be positive"),
        ({"volume_window": 0}, "volume_window must be positive"),
        ({"adv_window": 0}, "adv_window must be positive"),
        ({"cooldown_sessions": -1}, "cooldown_sessions must be non-negative"),
        ({"sample_size": 0}, "sample_size must be positive"),
        (
            {"primary_extension_window": 2},
            "primary_extension_window must be included in extension_windows",
        ),
    ],
)
def test_speculative_volume_surge_follow_on_validation_errors(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        run_speculative_volume_surge_follow_on_research("/tmp/unused.duckdb", **kwargs)
