from __future__ import annotations

from pathlib import Path

import pandas as pd
import pandas.testing as pdt

from src.domains.analytics.standard_value_pump_fade_decomposition import (
    STANDARD_VALUE_PUMP_FADE_DECOMPOSITION_EXPERIMENT_ID,
    get_standard_value_pump_fade_decomposition_bundle_path_for_run_id,
    get_standard_value_pump_fade_decomposition_latest_bundle_path,
    load_standard_value_pump_fade_decomposition_bundle,
    run_standard_value_pump_fade_decomposition_from_frames,
    write_standard_value_pump_fade_decomposition_bundle,
)


def _price_rows(code: str, *, pump: bool) -> list[dict[str, object]]:
    dates = pd.bdate_range("2023-01-02", periods=430)
    rows: list[dict[str, object]] = []
    for idx, date in enumerate(dates):
        close = 100.0 + idx * 0.02
        high = close * 1.01
        low = close * 0.99
        open_price = close * 0.995
        volume = 1000
        if pump and 180 <= idx <= 200:
            close = 100.0 + (idx - 180) * 5.0
            high = close * 1.15
            low = close * 0.90
            open_price = close * 0.85
            volume = 10000
        if pump and idx > 200:
            close = 135.0 - (idx - 200) * 0.25
            high = close * 1.03
            low = close * 0.98
            open_price = close * 1.01
        rows.append(
            {
                "code": code,
                "date": date.strftime("%Y-%m-%d"),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
        )
    return rows


def test_standard_value_pump_fade_decomposition_roundtrip(tmp_path: Path) -> None:
    snapshot = pd.DataFrame(
        [
            {
                "snapshot_date": "2024-05-01",
                "rank": 1,
                "code": "1000",
                "company_name": "Pump Fade",
                "score": 0.9,
                "score_before_boost": 0.85,
                "breakout_boost": 0.05,
                "pbr": 0.5,
                "forward_per": 6.0,
                "market_cap_bil_jpy": 2.0,
                "avg_trading_value_60d_mil_jpy": 12.0,
            },
            {
                "snapshot_date": "2024-05-01",
                "rank": 2,
                "code": "2000",
                "company_name": "Plain Value",
                "score": 0.8,
                "score_before_boost": 0.8,
                "breakout_boost": 0.0,
                "pbr": 0.7,
                "forward_per": 8.0,
                "market_cap_bil_jpy": 20.0,
                "avg_trading_value_60d_mil_jpy": 80.0,
            },
        ]
    )
    prices = pd.DataFrame([*_price_rows("1000", pump=True), *_price_rows("2000", pump=False)])

    result = run_standard_value_pump_fade_decomposition_from_frames(
        db_path="/tmp/market.duckdb",
        ranking_snapshot_df=snapshot,
        price_history_df=prices,
        forward_horizons=(20,),
    )

    assert result.candidate_count == 2
    pump_row = result.candidate_event_df.loc[result.candidate_event_df["code"] == "1000"].iloc[0]
    plain_row = result.candidate_event_df.loc[result.candidate_event_df["code"] == "2000"].iloc[0]
    assert bool(pump_row["large_month_candle"])
    assert pump_row["speculative_risk_score"] >= plain_row["speculative_risk_score"]
    assert set(result.pattern_summary_df["pattern_bucket"])

    bundle = write_standard_value_pump_fade_decomposition_bundle(
        result,
        output_root=tmp_path,
        run_id="20260508_test",
    )
    reloaded = load_standard_value_pump_fade_decomposition_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == STANDARD_VALUE_PUMP_FADE_DECOMPOSITION_EXPERIMENT_ID
    assert (
        get_standard_value_pump_fade_decomposition_bundle_path_for_run_id(
            "20260508_test",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_standard_value_pump_fade_decomposition_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.candidate_event_df,
        result.candidate_event_df,
        check_dtype=False,
    )
