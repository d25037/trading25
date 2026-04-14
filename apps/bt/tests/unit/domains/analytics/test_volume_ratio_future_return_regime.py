from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import duckdb
import pandas as pd
import pandas.testing as pdt
import pytest

from src.domains.analytics.volume_ratio_future_return_regime import (
    CONDITION_FAMILY_ORDER,
    UNIVERSE_ORDER,
    VOLUME_RATIO_FUTURE_RETURN_REGIME_EXPERIMENT_ID,
    _build_parameter_grid,
    _build_research_bundle_summary_markdown,
    _iter_split_frames,
    _normalize_float_sequence,
    _normalize_int_sequence,
    _prepare_universe_panel,
    _resolve_validation_split_dates,
    _safe_one_sample_t_test,
    _safe_optional_date,
    _safe_welch_t_test,
    _sample_reference_events_for_universe,
    get_volume_ratio_future_return_regime_bundle_path_for_run_id,
    get_volume_ratio_future_return_regime_latest_bundle_path,
    load_volume_ratio_future_return_regime_research_bundle,
    run_volume_ratio_future_return_regime_research,
    write_volume_ratio_future_return_regime_research_bundle,
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

    for universe_index, (universe_key, market_code, market_name, scale_category) in enumerate(universe_specs):
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

            base_price = 40.0 + universe_index * 8.0 + code_index * 1.2
            base_volume = 800.0 + universe_index * 150.0 + code_index * 25.0
            prime_boost = 1.0 if universe_key == "prime_ex_topix500" and code_index < 6 else 0.0
            other_boost = 1.0 if universe_key != "prime_ex_topix500" and code_index % 4 == 0 else 0.0

            for day_index, date in enumerate(dates):
                seasonal = (day_index % 11) * 0.08
                close = base_price + day_index * (0.07 + prime_boost * 0.02) + seasonal
                open_price = close * (0.995 + (code_index % 3) * 0.0005)
                volume = base_volume + (day_index % 17) * 18.0

                if day_index >= 160 and day_index % 20 == code_index % 5:
                    volume *= 5.0 if prime_boost else 2.2
                    close += 1.6 if prime_boost else 0.3 * other_boost
                    open_price = close * 0.992

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


def test_volume_ratio_future_return_regime_research_bundle_roundtrip(tmp_path: Path) -> None:
    db_path = _build_market_db(tmp_path / "market.duckdb")

    result = run_volume_ratio_future_return_regime_research(
        db_path,
        lookback_years=1,
        validation_ratio=0.25,
        short_windows=(5, 20, 50),
        long_windows=(20, 50, 150),
        threshold_values=(1.2, 1.7),
        horizons=(5, 10),
        sample_seed=7,
        sample_size_per_universe=3,
        sample_event_size_per_universe=2,
        min_signal_events=2,
        min_unique_codes=2,
        top_k=2,
    )

    assert set(result.universe_summary_df["universe_key"]) == set(UNIVERSE_ORDER)
    assert all(
        count == 3
        for count in result.sampled_codes_df.groupby("universe_key").size().tolist()
    )
    assert "reference_volume_ratio" in result.sampled_reference_events_df.columns
    assert not result.decile_summary_df.empty
    assert not result.decile_spread_summary_df.empty
    assert not result.threshold_grid_summary_df.empty
    assert not result.best_thresholds_df.empty
    assert set(result.reference_condition_summary_df["condition_family"]) == set(
        CONDITION_FAMILY_ORDER
    )

    bundle = write_volume_ratio_future_return_regime_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260414_120000_testabcd",
    )
    reloaded = load_volume_ratio_future_return_regime_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == VOLUME_RATIO_FUTURE_RETURN_REGIME_EXPERIMENT_ID
    assert bundle.summary_path.exists()
    assert (
        get_volume_ratio_future_return_regime_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_volume_ratio_future_return_regime_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.sampled_codes_df,
        result.sampled_codes_df,
        check_dtype=False,
    )
    pdt.assert_frame_equal(
        reloaded.best_thresholds_df,
        result.best_thresholds_df,
        check_dtype=False,
    )


def test_volume_ratio_helper_branches_and_validation_guards() -> None:
    assert _normalize_int_sequence(None, fallback=(5,), name="short_windows") == (5,)
    assert _normalize_float_sequence(None, fallback=(1.7,), name="threshold_values") == (1.7,)

    with pytest.raises(ValueError):
        _normalize_int_sequence((0, -1), fallback=(5,), name="short_windows")
    with pytest.raises(ValueError):
        _normalize_float_sequence((0.0, -1.0), fallback=(1.7,), name="threshold_values")
    with pytest.raises(ValueError):
        _build_parameter_grid(short_windows=(10,), long_windows=(5, 10))

    assert _safe_optional_date(None) is None
    assert _safe_optional_date(pd.NaT) is None

    split_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "value": [1, 2, 3],
        }
    )
    assert [key for key, _ in _iter_split_frames(split_df, validation_start_ts=None)] == [
        "full"
    ]
    assert [key for key, _ in _iter_split_frames(split_df, validation_start_ts=pd.Timestamp("2024-01-04"))] == [
        "full",
        "discovery",
    ]
    assert [key for key, _ in _iter_split_frames(split_df, validation_start_ts=pd.Timestamp("2024-01-01"))] == [
        "full",
        "validation",
    ]

    empty_dates_df = pd.DataFrame({"date": pd.Series([], dtype="datetime64[ns]")})
    assert _resolve_validation_split_dates(empty_dates_df, validation_ratio=0.3) == (
        None,
        None,
        None,
    )
    assert _resolve_validation_split_dates(split_df, validation_ratio=0.0) == (
        "2024-01-03",
        None,
        None,
    )

    assert _safe_welch_t_test(pd.Series([1.0]), pd.Series([2.0])) == (None, None)
    assert _safe_one_sample_t_test(pd.Series([1.0])) == (None, None)

    empty_panel = _prepare_universe_panel(
        pd.DataFrame(),
        analysis_start_date=None,
        analysis_end_date=None,
        unique_volume_windows=(5, 20),
        horizons=(5,),
    )
    assert empty_panel.empty

    dropped_panel = _prepare_universe_panel(
        pd.DataFrame(
            {
                "code": ["10000"],
                "date": ["2024-01-01"],
                "open": [None],
                "close": [10.0],
                "volume": [100.0],
            }
        ),
        analysis_start_date=None,
        analysis_end_date=None,
        unique_volume_windows=(5, 20),
        horizons=(5,),
    )
    assert dropped_panel.empty


def test_volume_ratio_sampling_and_summary_empty_paths(tmp_path: Path) -> None:
    db_path = _build_market_db(tmp_path / "market.duckdb")
    result = run_volume_ratio_future_return_regime_research(
        db_path,
        lookback_years=1,
        validation_ratio=0.25,
        short_windows=(5, 20, 50),
        long_windows=(20, 50, 150),
        threshold_values=(1.2, 1.7),
        horizons=(5, 10),
        sample_seed=7,
        sample_size_per_universe=3,
        sample_event_size_per_universe=2,
        min_signal_events=2,
        min_unique_codes=2,
        top_k=2,
    )

    empty_sampled = _sample_reference_events_for_universe(
        pd.DataFrame(),
        universe_key="topix500",
        sampled_codes_df=pd.DataFrame({"code": ["10000"]}),
        sample_seed=7,
        sample_event_size=2,
    )
    assert empty_sampled.empty

    no_signal_panel = pd.DataFrame(
        {
            "code": ["10000"],
            "company_name": ["sample"],
            "date": pd.to_datetime(["2024-01-01"]),
            "volume_ma_50": [1.0],
            "volume_ma_150": [1.0],
            "trend_state": ["above_sma150"],
            "momentum_state": ["positive_20d"],
            "liquidity_state": ["high_adv20"],
            "volatility_state": ["high_vol20"],
            "close_to_close_5d": [0.01],
            "next_open_to_close_5d": [0.01],
        }
    )
    assert _sample_reference_events_for_universe(
        no_signal_panel,
        universe_key="topix500",
        sampled_codes_df=pd.DataFrame({"code": ["10000"]}),
        sample_seed=7,
        sample_event_size=2,
    ).empty

    merge_empty_panel = no_signal_panel.copy()
    merge_empty_panel["volume_ma_50"] = 2.0
    merge_empty_panel["volume_ma_150"] = 1.0
    assert _sample_reference_events_for_universe(
        merge_empty_panel,
        universe_key="topix500",
        sampled_codes_df=pd.DataFrame({"code": ["99990"]}),
        sample_seed=7,
        sample_event_size=2,
    ).empty

    empty_result = replace(
        result,
        sampled_codes_df=result.sampled_codes_df.iloc[0:0].copy(),
        sampled_reference_events_df=result.sampled_reference_events_df.iloc[0:0].copy(),
        decile_summary_df=result.decile_summary_df.iloc[0:0].copy(),
        decile_spread_summary_df=result.decile_spread_summary_df.iloc[0:0].copy(),
        threshold_grid_summary_df=result.threshold_grid_summary_df.iloc[0:0].copy(),
        best_thresholds_df=result.best_thresholds_df.iloc[0:0].copy(),
        reference_condition_summary_df=result.reference_condition_summary_df.iloc[0:0].copy(),
        analysis_use_sampled_codes=True,
        sample_size_per_universe=7,
    )
    summary = _build_research_bundle_summary_markdown(empty_result)
    assert "sampled 7 codes per universe" in summary
    assert "No full-split reference rows were generated." in summary
    assert "No validated winners satisfied the stability gate." in summary
    assert "No trend-conditioned reference rows were generated." in summary


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"lookback_years": 0}, "lookback_years must be positive"),
        ({"validation_ratio": 1.0}, "validation_ratio must satisfy 0.0 <= ratio < 1.0"),
        (
            {"sample_size_per_universe": -1},
            "sample_size_per_universe must be non-negative",
        ),
        (
            {"sample_event_size_per_universe": -1},
            "sample_event_size_per_universe must be non-negative",
        ),
        ({"min_signal_events": -1}, "min_signal_events must be non-negative"),
        ({"min_unique_codes": -1}, "min_unique_codes must be non-negative"),
        ({"top_k": 0}, "top_k must be positive"),
    ],
)
def test_volume_ratio_research_validation_errors(kwargs: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        run_volume_ratio_future_return_regime_research("/tmp/unused.duckdb", **kwargs)
