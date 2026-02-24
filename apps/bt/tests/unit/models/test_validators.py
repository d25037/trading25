"""validators.py のテスト"""

import pytest

from src.shared.models.validators import (
    DIRECTION_CHOICES,
    INDICATOR_CHOICES,
    _format_choices_message,
    create_choice_validator,
    create_range_validator,
    validate_in_choices,
)


class TestFormatChoicesMessage:
    def test_single_choice(self):
        msg = _format_choices_message(["a"], "field")
        assert "field" in msg
        assert "'a'" in msg

    def test_multiple_choices(self):
        msg = _format_choices_message(["a", "b"], "field")
        assert "'a'" in msg
        assert "'b'" in msg
        assert "または" in msg


class TestCreateChoiceValidator:
    def test_valid_value(self):
        validator = create_choice_validator(["up", "down"], "direction")
        assert validator(None, "up") == "up"

    def test_invalid_value(self):
        validator = create_choice_validator(["up", "down"], "direction")
        with pytest.raises(ValueError, match="direction"):
            validator(None, "left")


class TestCreateRangeValidator:
    def test_value_in_range(self):
        validator = create_range_validator(0.0, 1.0, "ratio")
        assert validator(None, 0.5) == 0.5

    def test_value_at_min(self):
        validator = create_range_validator(0.0, 1.0, "ratio")
        assert validator(None, 0.0) == 0.0

    def test_value_at_max(self):
        validator = create_range_validator(0.0, 1.0, "ratio")
        assert validator(None, 1.0) == 1.0

    def test_value_below_min(self):
        validator = create_range_validator(0.0, 1.0, "ratio")
        with pytest.raises(ValueError, match="0.0以上"):
            validator(None, -0.1)

    def test_value_above_max(self):
        validator = create_range_validator(0.0, 1.0, "ratio")
        with pytest.raises(ValueError, match="1.0以下"):
            validator(None, 1.1)

    def test_no_min(self):
        validator = create_range_validator(max_val=10.0, field_name="x")
        assert validator(None, -100.0) == -100.0

    def test_no_max(self):
        validator = create_range_validator(min_val=0.0, field_name="x")
        assert validator(None, 999.0) == 999.0

    def test_no_bounds(self):
        validator = create_range_validator(field_name="x")
        assert validator(None, 42.0) == 42.0


class TestValidateInChoices:
    def test_valid(self):
        assert validate_in_choices("above", ["above", "below"], "dir") == "above"

    def test_invalid(self):
        with pytest.raises(ValueError, match="dir"):
            validate_in_choices("left", ["above", "below"], "dir")


class TestConstants:
    def test_direction_choices_keys(self):
        assert "above_below" in DIRECTION_CHOICES
        assert "surge_drop" in DIRECTION_CHOICES
        assert "golden_dead" in DIRECTION_CHOICES

    def test_indicator_choices_keys(self):
        assert "ma_type" in INDICATOR_CHOICES
        assert "ratio_type" in INDICATOR_CHOICES
        assert "crossover_type" in INDICATOR_CHOICES

    def test_direction_choices_values(self):
        assert DIRECTION_CHOICES["above_below"] == ["above", "below"]
        assert DIRECTION_CHOICES["surge_drop"] == ["surge", "drop"]
