from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics.volume_trading_value_conditioning import (
    CONDITION_FAMILY_ORDER,
    SIGNAL_FAMILY_ORDER,
    UNIVERSE_ORDER,
    VOLUME_TRADING_VALUE_CONDITIONING_EXPERIMENT_ID,
    _assign_daily_quintile_labels,
    _build_signal_family_compare_table,
    _build_summary_markdown,
    _build_top_condition_buckets_table,
    _ensure_trading_value_ma_columns,
    _sort_conditioning_table,
    get_volume_trading_value_conditioning_bundle_path_for_run_id,
    get_volume_trading_value_conditioning_latest_bundle_path,
    load_volume_trading_value_conditioning_research_bundle,
    run_volume_trading_value_conditioning_research,
    write_volume_trading_value_conditioning_research_bundle,
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

    universe_specs = [
        ("topix500", "0111", "プライム", "TOPIX Mid400"),
        ("prime_ex_topix500", "0111", "プライム", "-"),
        ("standard", "0112", "スタンダード", "-"),
        ("growth", "0113", "グロース", "-"),
    ]
    dates = pd.bdate_range("2023-01-02", periods=220)

    stock_rows: list[tuple[object, ...]] = []
    price_rows: list[tuple[object, ...]] = []

    for universe_index, (universe_key, market_code, market_name, scale_category) in enumerate(
        universe_specs
    ):
        for code_index in range(12):
            code = f"{universe_index + 1}{code_index:03d}0"
            stock_rows.append(
                (
                    code,
                    f"{universe_key}-{code_index}",
                    f"{universe_key}-{code_index}",
                    market_code,
                    market_name,
                    "1",
                    "A",
                    "1",
                    "A",
                    scale_category,
                    "2010-01-01",
                    None,
                    None,
                )
            )

            base_price = 45.0 + universe_index * 10.0 + code_index * 1.1
            base_volume = 900.0 + universe_index * 180.0 + code_index * 30.0
            surge_slot = code_index % 4

            for day_index, date in enumerate(dates):
                base_trend = 0.05 + universe_index * 0.01
                seasonal = (day_index % 11) * 0.07
                close = base_price + day_index * base_trend + seasonal
                open_price = close * (0.995 + (code_index % 3) * 0.0005)
                volume = base_volume + (day_index % 17) * 20.0

                if day_index >= 160 and day_index % 12 == surge_slot:
                    if universe_key in {"standard", "growth"}:
                        volume *= 1.9 + 0.1 * (code_index % 2)
                        close *= 1.05 + 0.01 * (code_index % 3)
                    elif universe_key == "prime_ex_topix500":
                        volume *= 3.8
                        close *= 1.03
                    else:
                        volume *= 2.2
                        close *= 1.015
                    open_price = close * 0.991

                if universe_key == "growth" and day_index >= 150 and code_index < 4:
                    close *= 1.0 + (day_index % 5) * 0.003

                high = max(open_price, close) * 1.01
                low = min(open_price, close) * 0.99
                price_rows.append(
                    (
                        code,
                        date.strftime("%Y-%m-%d"),
                        float(open_price),
                        float(high),
                        float(low),
                        float(close),
                        int(volume),
                        1.0,
                        None,
                    )
                )

    conn.executemany(
        "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        stock_rows,
    )
    conn.executemany(
        "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        price_rows,
    )
    conn.close()
    return str(db_path)


def test_volume_trading_value_conditioning_research_bundle_roundtrip(
    tmp_path: Path,
) -> None:
    db_path = _build_market_db(tmp_path / "market.duckdb")

    result = run_volume_trading_value_conditioning_research(
        db_path,
        lookback_years=1,
        validation_ratio=0.25,
        analysis_use_sampled_codes=True,
        short_windows=(20,),
        long_windows=(60,),
        threshold_values=(1.2,),
        horizons=(5, 10),
        sample_seed=7,
        sample_size_per_universe=6,
        min_signal_events=1,
        min_unique_codes=1,
        top_k=2,
    )

    assert set(result.universe_summary_df["universe_key"]) == set(UNIVERSE_ORDER)
    assert all(
        count == 6
        for count in result.sampled_codes_df.groupby("universe_key").size().tolist()
    )
    assert set(result.overall_signal_summary_df["signal_family"]) == set(
        SIGNAL_FAMILY_ORDER
    )
    assert set(result.conditioned_signal_summary_df["condition_family"]) == set(
        CONDITION_FAMILY_ORDER
    )
    assert not result.signal_family_compare_df.empty
    assert set(result.signal_family_compare_df["winning_signal_family"]).issubset(
        {"volume_ratio", "trading_value_ratio", "tie"}
    )
    assert not result.top_condition_buckets_df.empty
    assert int(result.top_condition_buckets_df["selection_rank"].max()) <= 2

    bundle = write_volume_trading_value_conditioning_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260414_180000_testcond",
    )
    reloaded = load_volume_trading_value_conditioning_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == VOLUME_TRADING_VALUE_CONDITIONING_EXPERIMENT_ID
    assert bundle.summary_path.exists()
    assert (
        get_volume_trading_value_conditioning_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_volume_trading_value_conditioning_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.overall_signal_summary_df,
        result.overall_signal_summary_df,
        check_dtype=False,
    )
    pdt.assert_frame_equal(
        reloaded.signal_family_compare_df,
        result.signal_family_compare_df,
        check_dtype=False,
    )


def test_volume_trading_value_conditioning_helper_branches(tmp_path: Path) -> None:
    empty_df = pd.DataFrame()
    assert _ensure_trading_value_ma_columns(empty_df, windows=(20,)).empty
    assert _sort_conditioning_table(empty_df).empty
    assert _build_signal_family_compare_table(pd.DataFrame()).empty
    assert _build_top_condition_buckets_table(pd.DataFrame(), top_k=2).empty

    base_panel = pd.DataFrame(
        {
            "code": ["10000", "10001"],
            "date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
            "trading_value": [100.0, 200.0],
            "trading_value_ma_20": [100.0, 200.0],
            "adv_20": [100.0, None],
            "volatility_20": [0.1, 0.2],
        }
    )
    ensured = _ensure_trading_value_ma_columns(base_panel, windows=(20,))
    assert "trading_value_ma_20" in ensured.columns
    labeled = _assign_daily_quintile_labels(
        base_panel,
        source_column="adv_20",
        output_column="adv20_quintile",
    )
    assert labeled.loc[0, "adv20_quintile"] in {"q5", "q1"}
    assert pd.isna(labeled.loc[1, "adv20_quintile"])

    result = run_volume_trading_value_conditioning_research(
        _build_market_db(tmp_path / "market.duckdb"),
        lookback_years=1,
        validation_ratio=0.25,
        analysis_use_sampled_codes=True,
        short_windows=(20,),
        long_windows=(60,),
        threshold_values=(1.2,),
        horizons=(5, 10),
        sample_seed=7,
        sample_size_per_universe=6,
        min_signal_events=1,
        min_unique_codes=1,
        top_k=2,
    )
    empty_result = replace(
        result,
        overall_signal_summary_df=result.overall_signal_summary_df.iloc[0:0].copy(),
        conditioned_signal_summary_df=result.conditioned_signal_summary_df.iloc[0:0].copy(),
        signal_family_compare_df=result.signal_family_compare_df.iloc[0:0].copy(),
        top_condition_buckets_df=result.top_condition_buckets_df.iloc[0:0].copy(),
        analysis_use_sampled_codes=True,
        sample_size_per_universe=7,
    )
    summary = _build_summary_markdown(empty_result)
    assert "sampled 7 codes per universe" in summary
    assert "No overall rows were generated." in summary
    assert "No validation buckets satisfied the minimum-count gate." in summary
    assert "No family-comparison rows were generated." in summary

    partial_compare_df = result.conditioned_signal_summary_df.loc[
        result.conditioned_signal_summary_df["signal_family"] == "volume_ratio"
    ].copy()
    assert _build_signal_family_compare_table(partial_compare_df).empty

    no_meets_df = result.conditioned_signal_summary_df.copy()
    no_meets_df["meets_min_counts"] = False
    assert _build_top_condition_buckets_table(no_meets_df, top_k=2).empty


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"lookback_years": 0}, "lookback_years must be positive"),
        ({"validation_ratio": 1.0}, "validation_ratio must satisfy 0.0 <= ratio < 1.0"),
        (
            {"sample_size_per_universe": -1},
            "sample_size_per_universe must be non-negative",
        ),
        ({"min_signal_events": -1}, "min_signal_events must be non-negative"),
        ({"min_unique_codes": -1}, "min_unique_codes must be non-negative"),
        ({"top_k": 0}, "top_k must be positive"),
    ],
)
def test_volume_trading_value_conditioning_validation_errors(
    kwargs: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        run_volume_trading_value_conditioning_research("/tmp/unused.duckdb", **kwargs)
