from src.domains.optimization.grid_validation import validate_grid_yaml_content


def test_validate_grid_yaml_content_reports_signal_shape_error() -> None:
    result = validate_grid_yaml_content(
        """
parameter_ranges:
  entry_filter_params:
    ratio_threshold: [1.0, 1.5, 2.0]
"""
    )

    assert result.valid is False
    assert result.ready_to_run is False
    assert result.param_count == 0
    assert result.combinations == 0
    assert result.errors[0].path == "parameter_ranges.entry_filter_params.ratio_threshold"


def test_validate_grid_yaml_content_accepts_nested_signal_params() -> None:
    result = validate_grid_yaml_content(
        """
parameter_ranges:
  entry_filter_params:
    volume_ratio_above:
      ratio_threshold: [1.0, 1.5, 2.0]
  exit_trigger_params: {}
"""
    )

    assert result.valid is True
    assert result.ready_to_run is True
    assert result.param_count == 1
    assert result.combinations == 3
