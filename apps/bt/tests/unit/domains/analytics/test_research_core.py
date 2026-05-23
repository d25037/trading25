from __future__ import annotations

import pandas as pd
import pytest

from src.domains.analytics.research_core import (
    UNIVERSE_LABELS,
    build_event_portfolio_daily_df,
    build_market_universe_case_sql,
    normalize_positive_int_sequence,
    research_universe_market_codes,
    sort_research_table,
    sql_string_list,
    warmup_start_date,
)


def test_research_universe_sql_keeps_market_behavior_labels() -> None:
    case_sql = build_market_universe_case_sql(
        market_code_column="smd.market_code",
        scale_category_column="smd.scale_category",
    )

    assert "TOPIX Core30" in case_sql
    assert "prime_ex_topix500" in case_sql
    assert "standard" in case_sql
    assert "growth" in case_sql
    assert UNIVERSE_LABELS["topix500"] == "TOPIX500"
    assert "0111" in research_universe_market_codes()


def test_sql_string_list_escapes_static_values() -> None:
    assert sql_string_list(("0111", "O'Reilly")) == "'0111', 'O''Reilly'"


def test_normalize_positive_int_sequence_preserves_existing_modes() -> None:
    assert normalize_positive_int_sequence(
        [10, "5", 10],
        fallback=(20,),
        name="horizons",
    ) == (5, 10)
    assert normalize_positive_int_sequence(
        [0, 5, -1, 5],
        fallback=(20,),
        name="horizons",
        non_positive="filter",
    ) == (5,)

    with pytest.raises(ValueError, match="horizons values must be positive"):
        normalize_positive_int_sequence([0], fallback=(20,), name="horizons")


def test_warmup_start_date_clamps_to_available_start() -> None:
    assert (
        warmup_start_date(
            "2024-03-01",
            "2024-01-15",
            warmup_sessions=20,
            session_to_calendar_multiplier=2.1,
        )
        == "2024-01-15"
    )
    assert (
        warmup_start_date(
            "2024-03-01",
            "2024-02-01",
            warmup_sessions=20,
            session_to_calendar_multiplier=2.1,
        )
        == "2024-02-01"
    )


def test_sort_research_table_uses_universe_and_extra_order_without_temp_columns() -> None:
    frame = pd.DataFrame(
        [
            {"universe_key": "growth", "condition_key": "high", "date": "2024-01-02"},
            {"universe_key": "topix500", "condition_key": "low", "date": "2024-01-03"},
            {"universe_key": "topix500", "condition_key": "high", "date": "2024-01-01"},
        ]
    )

    sorted_frame = sort_research_table(
        frame,
        sort_columns=("date",),
        extra_order_columns={"condition_key": {"high": 0, "low": 1}},
    )

    assert sorted_frame["universe_key"].tolist() == ["topix500", "topix500", "growth"]
    assert sorted_frame["condition_key"].tolist() == ["high", "low", "high"]
    assert not any(column.startswith("_") for column in sorted_frame.columns)


def test_build_event_portfolio_daily_df_uses_grouped_price_paths() -> None:
    selected_event_df = pd.DataFrame(
        [
            {
                "market_scope": "standard",
                "score_method": "value",
                "code": "1000",
                "entry_date": "2024-01-02",
                "exit_date": "2024-01-04",
                "entry_open": 100.0,
            },
            {
                "market_scope": "standard",
                "score_method": "value",
                "code": "2000",
                "entry_date": "2024-01-03",
                "exit_date": "2024-01-05",
                "entry_open": 50.0,
            },
            {
                "market_scope": "growth",
                "score_method": "value",
                "code": "3000",
                "entry_date": "2024-01-02",
                "exit_date": "2024-01-04",
                "entry_open": 0.0,
            },
        ]
    )
    price_df = pd.DataFrame(
        [
            {"code": "1000", "date": "2024-01-02", "close": 110.0},
            {"code": "1000", "date": "2024-01-03", "close": 121.0},
            {"code": "1000", "date": "2024-01-04", "close": 108.9},
            {"code": "2000", "date": "2024-01-03", "close": 55.0},
            {"code": "2000", "date": "2024-01-04", "close": 60.5},
            {"code": "2000", "date": "2024-01-05", "close": 54.45},
            {"code": "3000", "date": "2024-01-02", "close": 10.0},
        ]
    )

    result = build_event_portfolio_daily_df(
        selected_event_df,
        price_df,
        group_columns=("market_scope", "score_method"),
    )

    standard = result[result["market_scope"].astype(str) == "standard"].reset_index(drop=True)
    assert standard["date"].tolist() == [
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
        "2024-01-05",
    ]
    assert standard["active_positions"].tolist() == [1, 2, 2, 1]
    assert standard["mean_daily_return"].round(6).tolist() == [0.1, 0.1, 0.0, -0.1]
    assert standard["portfolio_value"].round(6).tolist() == [1.1, 1.21, 1.21, 1.089]
    assert standard["drawdown_pct"].round(6).tolist() == [0.0, 0.0, 0.0, -10.0]


def test_build_event_portfolio_daily_df_returns_stable_empty_shape() -> None:
    result = build_event_portfolio_daily_df(
        pd.DataFrame(),
        pd.DataFrame(),
        group_columns=("market_scope", "score_method"),
    )

    assert result.columns.tolist() == [
        "market_scope",
        "score_method",
        "date",
        "active_positions",
        "mean_daily_return",
        "mean_daily_return_pct",
        "portfolio_value",
        "drawdown_pct",
    ]
    assert result.empty
