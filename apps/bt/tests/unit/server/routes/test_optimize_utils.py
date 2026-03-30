"""Optimization YAML count helpers のテスト."""

from src.domains.optimization.grid_validation import validate_parameter_ranges
from src.domains.optimization.strategy_spec import parse_optimization_yaml


def _parse_optimization_counts(content: str) -> tuple[int, int]:
    parsed, errors = parse_optimization_yaml(content)
    if parsed is None or errors:
        return 0, 0

    parameter_ranges = parsed.get("parameter_ranges")
    if not isinstance(parameter_ranges, dict):
        return 0, 0

    validation = validate_parameter_ranges(parameter_ranges)
    return validation.param_count, validation.combinations


class TestParseGridYaml:
    def test_valid_yaml(self):
        content = """
parameter_ranges:
  entry_filter_params:
    breakout:
      period: [20, 40, 60]
      threshold: [1.0, 2.0]
"""
        param_count, combinations = _parse_optimization_counts(content)
        assert param_count == 2
        assert combinations == 6

    def test_nested_params(self):
        content = """
parameter_ranges:
  entry_filter_params:
    fundamental:
      per:
        threshold: [10, 20]
  exit_trigger_params:
    atr_stop:
      atr_multiplier: [1.0, 2.0, 3.0]
"""
        param_count, combinations = _parse_optimization_counts(content)
        assert param_count == 2
        assert combinations == 6

    def test_empty_data(self):
        content = ""
        param_count, combinations = _parse_optimization_counts(content)
        assert param_count == 0
        assert combinations == 0

    def test_missing_parameter_ranges_key(self):
        content = """
other_key:
  period: [1, 2, 3]
"""
        param_count, combinations = _parse_optimization_counts(content)
        assert param_count == 0
        assert combinations == 0

    def test_single_param(self):
        content = """
parameter_ranges:
  entry_filter_params:
    breakout:
      period: [10, 20, 30, 40, 50]
"""
        param_count, combinations = _parse_optimization_counts(content)
        assert param_count == 1
        assert combinations == 5

    def test_invalid_structure_returns_zero_counts(self):
        content = """
parameter_ranges:
  entry_filter_params:
    ratio_threshold: [1.0, 1.5, 2.0]
"""
        param_count, combinations = _parse_optimization_counts(content)
        assert param_count == 0
        assert combinations == 0
