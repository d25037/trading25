"""signals/base.py のテスト"""

import pandas as pd
import pytest
from pydantic import ValidationError

from src.models.signals.base import (
    BaseSignalParams,
    Signals,
    _validate_condition_above_below,
    _validate_period_order,
)


def _make_signals(n=10, entry_mask=None, exit_mask=None):
    idx = pd.date_range("2025-01-01", periods=n, freq="D")
    entries = pd.Series(False, index=idx, dtype=bool)
    exits = pd.Series(False, index=idx, dtype=bool)
    if entry_mask is not None:
        entries.iloc[entry_mask] = True
    if exit_mask is not None:
        exits.iloc[exit_mask] = True
    return Signals(entries=entries, exits=exits)


class TestSignalsCreation:
    def test_basic(self):
        s = _make_signals()
        assert len(s) == 10
        assert not s.any_entries()
        assert not s.any_exits()

    def test_with_entries(self):
        s = _make_signals(entry_mask=[0, 5])
        assert s.any_entries()
        assert not s.any_exits()

    def test_with_exits(self):
        s = _make_signals(exit_mask=[3, 7])
        assert not s.any_entries()
        assert s.any_exits()

    def test_summary(self):
        s = _make_signals(entry_mask=[0, 1, 2], exit_mask=[5])
        summary = s.summary()
        assert summary["total_length"] == 10
        assert summary["entry_signals"] == 3
        assert summary["exit_signals"] == 1
        assert summary["has_entries"] is True
        assert summary["has_exits"] is True


class TestSignalsValidation:
    def test_non_series_entries(self):
        with pytest.raises(ValidationError, match="Series"):
            Signals(entries=[True, False], exits=pd.Series([True, False], dtype=bool))

    def test_non_boolean_entries(self):
        idx = pd.date_range("2025-01-01", periods=3, freq="D")
        with pytest.raises(ValidationError, match="boolean"):
            Signals(
                entries=pd.Series([1, 0, 1], index=idx),
                exits=pd.Series([False, False, False], index=idx, dtype=bool),
            )

    def test_index_mismatch(self):
        idx1 = pd.date_range("2025-01-01", periods=3, freq="D")
        idx2 = pd.date_range("2025-02-01", periods=3, freq="D")
        with pytest.raises(ValidationError, match="identical indices"):
            Signals(
                entries=pd.Series([True, False, True], index=idx1, dtype=bool),
                exits=pd.Series([False, True, False], index=idx2, dtype=bool),
            )

    def test_empty_series(self):
        idx = pd.DatetimeIndex([])
        s = Signals(
            entries=pd.Series([], index=idx, dtype=bool),
            exits=pd.Series([], index=idx, dtype=bool),
        )
        assert len(s) == 0
        assert not s.any_entries()


class TestValidatePeriodOrder:
    def test_valid_order(self):
        class FakeInfo:
            data = {"short_period": 10}

        assert _validate_period_order(20, FakeInfo(), "short_period", "error") == 20

    def test_equal_periods(self):
        class FakeInfo:
            data = {"short_period": 10}

        with pytest.raises(ValueError):
            _validate_period_order(10, FakeInfo(), "short_period", "error")

    def test_reversed_order(self):
        class FakeInfo:
            data = {"short_period": 20}

        with pytest.raises(ValueError):
            _validate_period_order(10, FakeInfo(), "short_period", "error")

    def test_missing_field(self):
        class FakeInfo:
            data = {}

        result = _validate_period_order(10, FakeInfo(), "short_period", "error")
        assert result == 10


class TestValidateConditionAboveBelow:
    def test_above(self):
        assert _validate_condition_above_below("above") == "above"

    def test_below(self):
        assert _validate_condition_above_below("below") == "below"

    def test_invalid(self):
        with pytest.raises(ValueError, match="condition"):
            _validate_condition_above_below("middle")


class TestBaseSignalParams:
    def test_default_disabled(self):
        p = BaseSignalParams()
        assert p.enabled is False

    def test_enabled(self):
        p = BaseSignalParams(enabled=True)
        assert p.enabled is True
