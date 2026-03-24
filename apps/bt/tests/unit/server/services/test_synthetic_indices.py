"""Tests for synthetic index helpers."""

from src.application.services.synthetic_indices import (
    NtRatioRow,
    get_nt_ratio_data_start_date,
    get_nt_ratio_rows,
)


class FakeReader:
    def __init__(self, *, query_one_result=None, query_result=None, fail_query_one=False, fail_query=False):
        self.query_one_result = query_one_result
        self.query_result = query_result if query_result is not None else []
        self.fail_query_one = fail_query_one
        self.fail_query = fail_query

    def query_one(self, sql, params=()):  # noqa: ANN001, ANN201
        del sql, params
        if self.fail_query_one:
            raise RuntimeError("boom")
        return self.query_one_result

    def query(self, sql, params=()):  # noqa: ANN001, ANN201
        del sql, params
        if self.fail_query:
            raise RuntimeError("boom")
        return self.query_result


def test_get_nt_ratio_data_start_date_returns_none_for_missing_reader() -> None:
    assert get_nt_ratio_data_start_date(None) is None


def test_get_nt_ratio_data_start_date_returns_none_on_query_error() -> None:
    assert get_nt_ratio_data_start_date(FakeReader(fail_query_one=True)) is None


def test_get_nt_ratio_data_start_date_returns_iso_date() -> None:
    reader = FakeReader(query_one_result={"data_start_date": "2026-02-06"})

    assert get_nt_ratio_data_start_date(reader) == "2026-02-06"


def test_get_nt_ratio_rows_returns_empty_for_missing_reader() -> None:
    assert get_nt_ratio_rows(None) == []


def test_get_nt_ratio_rows_returns_empty_on_query_error() -> None:
    assert get_nt_ratio_rows(FakeReader(fail_query=True)) == []


def test_get_nt_ratio_rows_filters_nulls_and_rounds_values() -> None:
    reader = FakeReader(
        query_result=[
            {"date": "2026-02-06", "value": 14.1234567},
            {"date": "2026-02-07", "value": None},
        ]
    )

    assert get_nt_ratio_rows(reader) == [NtRatioRow(date="2026-02-06", value=14.123457)]
