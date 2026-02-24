"""AllocationInfo モデルのテスト"""

import pytest
from pydantic import ValidationError

from src.shared.models.allocation import AllocationInfo


def _make_info(**kwargs) -> AllocationInfo:
    defaults = dict(
        allocation=0.25,
        win_rate=0.55,
        avg_win=0.03,
        avg_loss=0.02,
        total_trades=100,
        full_kelly=0.5,
        kelly_fraction=0.5,
    )
    defaults.update(kwargs)
    return AllocationInfo(**defaults)


class TestAllocationInfoCreation:
    def test_default_method(self):
        info = _make_info()
        assert info.method == "kelly"

    def test_custom_values(self):
        info = _make_info(allocation=0.8, win_rate=0.7, total_trades=500)
        assert info.allocation == 0.8
        assert info.win_rate == 0.7
        assert info.total_trades == 500

    def test_boundary_allocation_zero(self):
        info = _make_info(allocation=0.0)
        assert info.allocation == 0.0

    def test_boundary_allocation_one(self):
        info = _make_info(allocation=1.0)
        assert info.allocation == 1.0

    def test_boundary_win_rate_zero(self):
        info = _make_info(win_rate=0.0)
        assert info.win_rate == 0.0

    def test_boundary_win_rate_one(self):
        info = _make_info(win_rate=1.0)
        assert info.win_rate == 1.0


class TestAllocationInfoValidation:
    def test_allocation_below_zero(self):
        with pytest.raises(ValidationError):
            _make_info(allocation=-0.01)

    def test_allocation_above_one(self):
        with pytest.raises(ValidationError):
            _make_info(allocation=1.01)

    def test_win_rate_below_zero(self):
        with pytest.raises(ValidationError):
            _make_info(win_rate=-0.1)

    def test_win_rate_above_one(self):
        with pytest.raises(ValidationError):
            _make_info(win_rate=1.1)

    def test_avg_win_negative(self):
        with pytest.raises(ValidationError):
            _make_info(avg_win=-0.01)

    def test_avg_loss_negative(self):
        with pytest.raises(ValidationError):
            _make_info(avg_loss=-0.01)

    def test_total_trades_negative(self):
        with pytest.raises(ValidationError):
            _make_info(total_trades=-1)

    def test_kelly_fraction_zero(self):
        with pytest.raises(ValidationError):
            _make_info(kelly_fraction=0.0)

    def test_kelly_fraction_negative(self):
        with pytest.raises(ValidationError):
            _make_info(kelly_fraction=-0.5)


class TestGetKellyLabel:
    def test_half_kelly(self):
        assert _make_info(kelly_fraction=0.5).get_kelly_label() == "Half Kelly"

    def test_full_kelly(self):
        assert _make_info(kelly_fraction=1.0).get_kelly_label() == "Full Kelly"

    def test_2x_kelly(self):
        assert _make_info(kelly_fraction=2.0).get_kelly_label() == "2x Kelly"

    def test_custom_kelly(self):
        assert _make_info(kelly_fraction=0.75).get_kelly_label() == "Custom"

    def test_custom_kelly_small(self):
        assert _make_info(kelly_fraction=0.1).get_kelly_label() == "Custom"


class TestAllocationInfoDisplay:
    def test_str_contains_key_info(self):
        info = _make_info()
        result = str(info)
        assert "Kelly基準資金配分最適化" in result
        assert "Half Kelly" in result
        assert "55.0%" in result

    def test_repr_html_returns_html(self):
        info = _make_info()
        html = info._repr_html_()
        assert "<table" in html
        assert "Kelly基準資金配分最適化" in html
        assert "Half Kelly" in html
