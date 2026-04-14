from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import pytest

from src.shared.utils.pit_guard import (
    assert_no_future_rows,
    filter_records_as_of,
    latest_rows_per_group_as_of,
    slice_frame_as_of,
)


@dataclass(frozen=True)
class _Record:
    code: str
    disclosed_date: str


def test_slice_frame_as_of_keeps_only_rows_on_or_before_cutoff() -> None:
    frame = pd.DataFrame.from_records(
        [
            {"date": "2026-01-05", "code": "1001"},
            {"date": "2026-01-06", "code": "1001"},
            {"date": "2026-01-07", "code": "1001"},
        ]
    )

    sliced = slice_frame_as_of(frame, as_of_date="2026-01-06")

    assert sliced["date"].tolist() == ["2026-01-05", "2026-01-06"]


def test_assert_no_future_rows_raises_on_future_data() -> None:
    frame = pd.DataFrame.from_records(
        [
            {"date": "2026-01-05", "code": "1001"},
            {"date": "2026-01-07", "code": "1001"},
        ]
    )

    with pytest.raises(ValueError, match="contains rows after as_of_date"):
        assert_no_future_rows(frame, as_of_date="2026-01-06", frame_name="history")


def test_latest_rows_per_group_as_of_ignores_future_rows() -> None:
    frame = pd.DataFrame.from_records(
        [
            {"date": "2026-01-05", "code": "1001", "value": 1},
            {"date": "2026-01-07", "code": "1001", "value": 2},
            {"date": "2026-01-04", "code": "1002", "value": 3},
            {"date": "2026-01-06", "code": "1002", "value": 4},
        ]
    )

    latest = latest_rows_per_group_as_of(
        frame,
        group_cols=["code"],
        as_of_date="2026-01-06",
    )

    assert latest.sort_values("code", kind="stable")["value"].tolist() == [1, 4]


def test_filter_records_as_of_keeps_only_eligible_disclosures() -> None:
    records = [
        _Record(code="1001", disclosed_date="2026-01-05"),
        _Record(code="1001", disclosed_date="2026-01-08"),
    ]

    filtered = filter_records_as_of(
        records,
        as_of_date="2026-01-06",
        date_getter=lambda record: record.disclosed_date,
    )

    assert filtered == [_Record(code="1001", disclosed_date="2026-01-05")]
