from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

import src.domains.analytics.speculative_volume_surge_prime_pullback_tradeable as tradeable_module
from src.domains.analytics.speculative_volume_surge_prime_pullback_tradeable import (
    DEFAULT_ENTRY_BUCKETS,
    DEEPEST_ALIGNMENT_COLUMNS,
    ENTRY_BUCKET_SUMMARY_COLUMNS,
    ENTRY_TRADE_COLUMNS,
    ENTRY_SPEED_SUMMARY_COLUMNS,
    TOP_EXAMPLES_COLUMNS,
    SPECULATIVE_VOLUME_SURGE_PRIME_PULLBACK_TRADEABLE_EXPERIMENT_ID,
    _bool_rate,
    _bucket_speed,
    _build_deepest_alignment_df,
    _build_entry_bucket_summary_df,
    _build_entry_speed_summary_df,
    _build_research_bundle_summary_markdown,
    _build_top_examples_df,
    _build_trade_entry_df,
    _median,
    _normalize_entry_buckets,
    get_speculative_volume_surge_prime_pullback_tradeable_bundle_path_for_run_id,
    get_speculative_volume_surge_prime_pullback_tradeable_latest_bundle_path,
    load_speculative_volume_surge_prime_pullback_tradeable_research_bundle,
    run_speculative_volume_surge_prime_pullback_tradeable_research,
    write_speculative_volume_surge_prime_pullback_tradeable_research_bundle,
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
        ("1000", "prime-fast-reclaim", "prime-fast-reclaim", "0111", "プライム", "1", "A", "1", "A", "-", "2010-01-01", None, None),
        ("2000", "prime-deep-hold", "prime-deep-hold", "0111", "プライム", "1", "A", "1", "A", "-", "2010-01-01", None, None),
        ("3000", "prime-slow-reclaim", "prime-slow-reclaim", "0111", "プライム", "1", "A", "1", "A", "TOPIX Mid400", "2010-01-01", None, None),
        ("4000", "standard-ignore", "standard-ignore", "0112", "スタンダード", "1", "A", "1", "A", "-", "2010-01-01", None, None),
    ]
    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stock_rows,
    )

    dates = pd.bdate_range("2023-01-02", periods=140)
    base_volume_map = {
        "1000": 100_000,
        "2000": 120_000,
        "3000": 1_000_000,
        "4000": 150_000,
    }

    def make_row(
        code: str,
        date: pd.Timestamp,
        *,
        open_value: float,
        high: float,
        low: float,
        close: float,
        volume: int,
    ) -> tuple[object, ...]:
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

    def price_bar(
        close: float,
        *,
        high: float | None = None,
        low: float | None = None,
        open_value: float | None = None,
        volume: int,
    ) -> tuple[float, float, float, float, int]:
        resolved_open = open_value if open_value is not None else close * 0.995
        resolved_high = high if high is not None else close + 1.0
        resolved_low = low if low is not None else close - 1.0
        return (
            float(resolved_open),
            float(resolved_high),
            float(resolved_low),
            float(close),
            int(volume),
        )

    code_specific: dict[str, dict[int, tuple[float, float, float, float, int]]] = {
        "1000": {
            40: price_bar(112.0, high=130.0, low=108.0, volume=2_500_000),
            41: price_bar(125.0, high=140.0, low=122.0, volume=400_000),
            42: price_bar(114.0, high=116.0, low=111.0, volume=220_000),
            43: price_bar(118.0, high=120.0, low=113.0, volume=180_000),
            44: price_bar(123.0, high=139.0, low=120.0, volume=170_000),
            45: price_bar(125.0, high=137.0, low=122.0, volume=160_000),
            46: price_bar(127.0, high=146.0, low=124.0, volume=150_000),
        },
        "2000": {
            40: price_bar(112.0, high=130.0, low=108.0, volume=3_000_000),
            41: price_bar(126.0, high=142.0, low=123.0, volume=480_000),
            42: price_bar(116.0, high=118.0, low=113.0, volume=220_000),
            43: price_bar(108.0, high=110.0, low=105.0, volume=200_000),
            44: price_bar(97.0, high=99.0, low=94.0, volume=180_000),
            45: price_bar(96.0, high=99.0, low=93.0, volume=170_000),
            46: price_bar(98.0, high=100.0, low=95.0, volume=160_000),
        },
        "3000": {
            40: price_bar(112.0, high=128.0, low=108.0, volume=20_000_000),
            41: price_bar(126.0, high=133.0, low=123.0, volume=3_200_000),
            42: price_bar(129.0, high=138.0, low=125.0, volume=3_000_000),
            43: price_bar(121.0, high=123.0, low=118.0, volume=2_200_000),
            44: price_bar(114.0, high=116.0, low=111.0, volume=1_900_000),
            45: price_bar(108.0, high=110.0, low=105.0, volume=1_700_000),
            46: price_bar(110.0, high=112.0, low=107.0, volume=1_500_000),
            47: price_bar(116.0, high=118.0, low=112.0, volume=1_400_000),
            48: price_bar(120.0, high=139.0, low=118.0, volume=1_300_000),
        },
        "4000": {
            40: price_bar(112.0, high=130.0, low=108.0, volume=3_750_000),
            41: price_bar(126.0, high=142.0, low=123.0, volume=600_000),
            42: price_bar(116.0, high=118.0, low=113.0, volume=260_000),
            43: price_bar(105.0, high=108.0, low=102.0, volume=240_000),
        },
    }

    price_rows: list[tuple[object, ...]] = []
    for day_index, date in enumerate(dates):
        for code in ("1000", "2000", "3000", "4000"):
            base_volume = base_volume_map[code]
            default_bar = price_bar(
                100.0 if day_index < 40 else 99.0,
                high=101.0 if day_index < 40 else 100.5,
                low=99.0 if day_index < 40 else 97.5,
                volume=base_volume,
            )
            open_value, high, low, close, volume = code_specific.get(code, {}).get(
                day_index,
                default_bar,
            )
            price_rows.append(
                make_row(
                    code,
                    date,
                    open_value=open_value,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume,
                )
            )

    conn.executemany(
        "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        price_rows,
    )
    conn.close()
    return str(db_path)


def test_speculative_volume_surge_prime_pullback_tradeable_bundle_roundtrip(
    tmp_path: Path,
) -> None:
    db_path = _build_market_db(tmp_path / "market.duckdb")

    result = run_speculative_volume_surge_prime_pullback_tradeable_research(
        db_path,
        lookback_years=1,
        sample_size=2,
    )

    assert result.market_name == "プライム"
    assert result.total_prime_episode_count == 3
    assert result.total_deepest_profile_count == 3
    assert result.total_trade_entry_count == 5
    assert set(result.trade_entry_df["entry_bucket"]) == {"0-10%", "10-20%"}
    assert set(result.trade_entry_df["exit_reason"]) == {"hold_close", "peak_reclaim"}
    assert set(result.trade_entry_df["speed_bucket"]) >= {"1-2d", "3-5d"}
    assert set(result.entry_bucket_summary_df.columns) == set(ENTRY_BUCKET_SUMMARY_COLUMNS)
    assert set(result.entry_speed_summary_df.columns) == set(ENTRY_SPEED_SUMMARY_COLUMNS)
    assert set(result.deepest_alignment_df.columns) == set(DEEPEST_ALIGNMENT_COLUMNS)
    assert set(result.top_examples_df.columns) == set(TOP_EXAMPLES_COLUMNS)

    reclaim_entry = result.trade_entry_df.loc[
        (result.trade_entry_df["code"] == "1000")
        & (result.trade_entry_df["entry_bucket"] == "10-20%")
    ].iloc[0]
    assert bool(reclaim_entry["reclaim_hit"])
    assert float(reclaim_entry["trade_return_pct"]) > 0.15

    deep_hold_entry = result.trade_entry_df.loc[
        (result.trade_entry_df["code"] == "2000")
        & (result.trade_entry_df["entry_bucket"] == "10-20%")
    ].iloc[0]
    assert not bool(deep_hold_entry["reclaim_hit"])
    assert float(deep_hold_entry["trade_return_pct"]) < 0.0

    misaligned_row = result.deepest_alignment_df.loc[
        (result.deepest_alignment_df["entry_bucket"] == "10-20%")
        & (result.deepest_alignment_df["deepest_pullback_bucket"] == "<0%")
    ].iloc[0]
    assert int(misaligned_row["observation_count"]) == 1

    bundle = write_speculative_volume_surge_prime_pullback_tradeable_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260420_200000_testabcd",
    )
    reloaded = load_speculative_volume_surge_prime_pullback_tradeable_research_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == SPECULATIVE_VOLUME_SURGE_PRIME_PULLBACK_TRADEABLE_EXPERIMENT_ID
    )
    assert (
        get_speculative_volume_surge_prime_pullback_tradeable_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_speculative_volume_surge_prime_pullback_tradeable_latest_bundle_path(
            output_root=tmp_path
        )
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.entry_bucket_summary_df,
        result.entry_bucket_summary_df,
        check_dtype=False,
    )


def test_speculative_volume_surge_prime_pullback_tradeable_helper_edges() -> None:
    assert _normalize_entry_buckets(None) == DEFAULT_ENTRY_BUCKETS
    assert _normalize_entry_buckets(["10-20%", "0-10%", "10-20%"]) == (
        "0-10%",
        "10-20%",
    )
    with pytest.raises(
        ValueError,
        match="entry_buckets must contain at least one supported bucket",
    ):
        _normalize_entry_buckets(["20-35%"])

    assert _bucket_speed(None) == "missing"
    assert _bucket_speed(0) == "missing"
    assert _bucket_speed(1) == "1-2d"
    assert _bucket_speed(4) == "3-5d"
    assert _bucket_speed(8) == "6-10d"
    assert _bucket_speed(15) == "11-20d"

    assert _median(pd.Series([], dtype=float)) is None
    assert _bool_rate(pd.Series([], dtype=bool)) is None


def test_speculative_volume_surge_prime_pullback_tradeable_empty_helpers() -> None:
    empty_trade_df = pd.DataFrame(columns=list(ENTRY_TRADE_COLUMNS))

    pdt.assert_frame_equal(
        _build_trade_entry_df(
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pullback_search_window=20,
            entry_buckets=("0-10%",),
            holding_period_sessions=20,
        ),
        empty_trade_df,
    )
    assert _build_entry_bucket_summary_df(empty_trade_df).empty
    assert _build_entry_speed_summary_df(empty_trade_df).empty
    assert _build_deepest_alignment_df(empty_trade_df).empty
    assert _build_top_examples_df(empty_trade_df, sample_size=1).empty


def test_speculative_volume_surge_prime_pullback_tradeable_no_prime_episode_path(
    monkeypatch,
) -> None:
    empty_event_ledger_df = pd.DataFrame(
        columns=[
            "episode_id",
            "code",
            "company_name",
            "event_date",
            "base_close",
            "market_name",
            "scale_category",
            "adv20_bucket",
            "price_bucket",
        ]
    )
    fake_result = SimpleNamespace(
        analysis_start_date="2024-01-01",
        analysis_end_date="2024-12-31",
        event_ledger_df=empty_event_ledger_df,
    )
    monkeypatch.setattr(
        tradeable_module,
        "run_speculative_volume_surge_follow_on_research",
        lambda *args, **kwargs: fake_result,
    )

    result = run_speculative_volume_surge_prime_pullback_tradeable_research(
        "/tmp/missing.duckdb",
    )

    assert result.total_prime_episode_count == 0
    assert result.total_deepest_profile_count == 0
    assert result.total_trade_entry_count == 0
    assert result.trade_entry_df.empty
    assert "No trade entries were produced." in _build_research_bundle_summary_markdown(
        result
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"lookback_years": 0}, "lookback_years must be positive"),
        ({"price_jump_threshold": 0.0}, "price_jump_threshold must be positive"),
        (
            {"volume_ratio_threshold": 0.0},
            "volume_ratio_threshold must be positive",
        ),
        ({"volume_window": 0}, "volume_window must be positive"),
        ({"adv_window": 0}, "adv_window must be positive"),
        ({"cooldown_sessions": -1}, "cooldown_sessions must be non-negative"),
        ({"initial_peak_window": 0}, "initial_peak_window must be positive"),
        (
            {"pullback_search_window": 5},
            "pullback_search_window must be greater than initial_peak_window",
        ),
        ({"holding_period_sessions": 0}, "holding_period_sessions must be positive"),
        (
            {"entry_buckets": ("20-35%",)},
            "entry_buckets must contain at least one supported bucket",
        ),
        ({"sample_size": 0}, "sample_size must be positive"),
    ],
)
def test_speculative_volume_surge_prime_pullback_tradeable_validation_errors(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        run_speculative_volume_surge_prime_pullback_tradeable_research(
            "/tmp/missing.duckdb",
            **kwargs,
        )
