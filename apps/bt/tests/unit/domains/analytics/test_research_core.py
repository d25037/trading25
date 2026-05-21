from __future__ import annotations

import pandas as pd
import pytest

from src.domains.analytics.research_core import (
    UNIVERSE_LABELS,
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
