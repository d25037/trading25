from __future__ import annotations

from src.domains.analytics.jpx_daily_price_limits import (
    JPX_DAILY_PRICE_LIMITS_REFERENCE_LABEL,
    build_standard_daily_limit_table_df,
    resolve_standard_daily_limit_width,
)


def test_resolve_standard_daily_limit_width_boundary_cases() -> None:
    assert resolve_standard_daily_limit_width(None) is None
    assert resolve_standard_daily_limit_width(0) is None
    assert resolve_standard_daily_limit_width(99) == 30
    assert resolve_standard_daily_limit_width(100) == 50
    assert resolve_standard_daily_limit_width(499) == 80
    assert resolve_standard_daily_limit_width(500) == 100
    assert resolve_standard_daily_limit_width(50_000_000) == 10_000_000


def test_build_standard_daily_limit_table_df_contains_first_and_last_band() -> None:
    table_df = build_standard_daily_limit_table_df()

    assert len(table_df) == 34
    assert JPX_DAILY_PRICE_LIMITS_REFERENCE_LABEL.endswith("Apr. 17, 2026)")
    assert table_df.iloc[0].to_dict() == {
        "base_price_rule": "base < 100",
        "daily_limit_width": 30,
    }
    assert table_df.iloc[-1].to_dict() == {
        "base_price_rule": "50,000,000 <= base",
        "daily_limit_width": 10_000_000,
    }
