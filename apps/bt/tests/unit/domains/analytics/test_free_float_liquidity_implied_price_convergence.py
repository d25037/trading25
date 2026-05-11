from __future__ import annotations

import pandas as pd

from src.domains.analytics.free_float_liquidity_implied_price_convergence import (
    _build_daily_panel,
    _build_observation_df,
)


def test_daily_implied_price_uses_same_date_cross_section_only() -> None:
    source = _source_frame(future_close_multiplier=1.0)
    changed_future = _source_frame(future_close_multiplier=3.0)

    panel, _diagnostics = _build_daily_panel(
        source,
        adv_windows=(60,),
        recent_return_windows=(20, 60),
        min_daily_regression_observations=3,
    )
    changed_panel, _changed_diagnostics = _build_daily_panel(
        changed_future,
        adv_windows=(60,),
        recent_return_windows=(20, 60),
        min_daily_regression_observations=3,
    )

    base_row = panel[(panel["date"] == "2024-01-02") & (panel["code"] == "1001")].iloc[
        0
    ]
    changed_row = changed_panel[
        (changed_panel["date"] == "2024-01-02") & (changed_panel["code"] == "1001")
    ].iloc[0]

    assert base_row["liquidity_implied_price"] == changed_row["liquidity_implied_price"]
    assert (
        base_row["liquidity_implied_price_gap_pct"]
        == changed_row["liquidity_implied_price_gap_pct"]
    )


def test_observation_forward_columns_are_outcomes_not_grouping_features() -> None:
    panel, _diagnostics = _build_daily_panel(
        _source_frame(future_close_multiplier=1.2),
        adv_windows=(60,),
        recent_return_windows=(20, 60),
        min_daily_regression_observations=3,
    )
    observations = _build_observation_df(
        panel,
        pd.DataFrame(
            {
                "date": ["2024-01-02", "2024-01-03", "2024-01-04"],
                "topix_close": [100.0, 101.0, 102.0],
            }
        ),
        horizons=(1,),
        observation_stride_sessions=1,
    )

    row = observations[
        (observations["date"] == "2024-01-02") & (observations["code"] == "1001")
    ].iloc[0]
    assert row["future_date_1d"] == "2024-01-03"
    assert row["future_return_1d_pct"] > 0
    assert row["fixed_target_closure_1d_ratio"] == row["fixed_target_closure_1d_ratio"]


def _source_frame(*, future_close_multiplier: float) -> pd.DataFrame:
    rows = []
    for date_index, date in enumerate(["2024-01-02", "2024-01-03", "2024-01-04"]):
        for code_index, code in enumerate(["1001", "1002", "1003"]):
            base_close = 100.0 + code_index * 50.0
            close = (
                base_close if date_index == 0 else base_close * future_close_multiplier
            )
            free_float_market_cap = (code_index + 1) * 10_000_000_000.0
            rows.append(
                {
                    "date": date,
                    "code": code,
                    "company_name": f"Company {code}",
                    "sector_33_name": "Sector",
                    "session_idx": date_index + 61,
                    "close": close,
                    "volume": 1_000_000 + code_index * 500_000,
                    "adv_60_jpy": [180_000_000.0, 210_000_000.0, 460_000_000.0][
                        code_index
                    ],
                    "adv_60_sessions": 60,
                    "free_float_market_cap_jpy": free_float_market_cap,
                    "shares_outstanding": free_float_market_cap / close,
                    "treasury_shares": 0.0,
                    "prior_close_20d": base_close * 0.95,
                    "prior_close_60d": base_close * 0.9,
                }
            )
    return pd.DataFrame(rows)
