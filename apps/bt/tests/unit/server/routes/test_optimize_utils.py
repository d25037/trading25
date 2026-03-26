"""server/routes/optimize.py - _parse_grid_yaml のテスト"""

from src.entrypoints.http.routes.optimize import _parse_grid_yaml


class TestParseGridYaml:
    def test_valid_yaml(self):
        content = """
parameter_ranges:
  entry_filter_params:
    breakout:
      period: [20, 40, 60]
      threshold: [1.0, 2.0]
"""
        param_count, combinations = _parse_grid_yaml(content)
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
        param_count, combinations = _parse_grid_yaml(content)
        assert param_count == 2
        assert combinations == 6

    def test_empty_data(self):
        content = ""
        param_count, combinations = _parse_grid_yaml(content)
        assert param_count == 0
        assert combinations == 0

    def test_missing_parameter_ranges_key(self):
        content = """
other_key:
  period: [1, 2, 3]
"""
        param_count, combinations = _parse_grid_yaml(content)
        assert param_count == 0
        assert combinations == 0

    def test_single_param(self):
        content = """
parameter_ranges:
  entry_filter_params:
    breakout:
      period: [10, 20, 30, 40, 50]
"""
        param_count, combinations = _parse_grid_yaml(content)
        assert param_count == 1
        assert combinations == 5

    def test_invalid_structure_returns_zero_counts(self):
        content = """
parameter_ranges:
  entry_filter_params:
    ratio_threshold: [1.0, 1.5, 2.0]
"""
        param_count, combinations = _parse_grid_yaml(content)
        assert param_count == 0
        assert combinations == 0
