from pydantic import BaseModel, Field

from src.domains.optimization.strategy_spec import (
    _clamp_float,
    _clamp_int,
    _dedupe_issues,
    _generate_numeric_candidates,
    analyze_strategy_optimization,
    calculate_total_combinations,
    dump_optimization_yaml,
    generate_strategy_optimization_draft,
    parse_optimization_yaml,
)
from src.domains.optimization.grid_validation import GridValidationIssue


def test_analyze_strategy_optimization_errors_for_disabled_signal() -> None:
    strategy_config = {
        "entry_filter_params": {
            "period_extrema_break": {
                "enabled": False,
                "period": 20,
            }
        },
        "exit_trigger_params": {},
    }
    optimization = {
        "parameter_ranges": {
            "entry_filter_params": {
                "period_extrema_break": {
                    "period": [10, 20, 30],
                }
            }
        }
    }

    analysis = analyze_strategy_optimization(strategy_config, optimization)

    assert analysis.valid is False
    assert any(
        issue.path == "optimization.parameter_ranges.entry_filter_params.period_extrema_break"
        and "disabled" in issue.message
        for issue in analysis.errors
    )


def test_analyze_strategy_optimization_supports_nested_fundamental_paths() -> None:
    strategy_config = {
        "entry_filter_params": {
            "fundamental": {
                "per": {
                    "enabled": True,
                    "threshold": 15.0,
                    "condition": "below",
                    "exclude_negative": True,
                }
            }
        },
        "exit_trigger_params": {},
    }
    optimization = {
        "parameter_ranges": {
            "entry_filter_params": {
                "fundamental": {
                    "per": {
                        "threshold": [10.0, 20.0],
                    }
                }
            }
        }
    }

    analysis = analyze_strategy_optimization(strategy_config, optimization)

    assert analysis.valid is True
    assert any(
        issue.path == "optimization.parameter_ranges.entry_filter_params.fundamental.per.threshold"
        and "Current strategy value 15.0" in issue.message
        for issue in analysis.drift
    )


def test_generate_strategy_optimization_draft_uses_enabled_numeric_leaf_params() -> None:
    strategy_config = {
        "entry_filter_params": {
            "period_extrema_break": {
                "enabled": True,
                "direction": "high",
                "period": 20,
                "lookback_days": 3,
            }
        },
        "exit_trigger_params": {},
    }

    analysis = generate_strategy_optimization_draft(strategy_config)

    entry_ranges = analysis.optimization["parameter_ranges"]["entry_filter_params"]["period_extrema_break"]
    assert analysis.valid is True
    assert "period" in entry_ranges
    assert "lookback_days" in entry_ranges
    assert "direction" not in entry_ranges


def test_parse_optimization_yaml_rejects_invalid_documents() -> None:
    parsed, errors = parse_optimization_yaml("[1, 2, 3]")

    assert parsed is None
    assert errors[0].message == "Optimization YAML root must be a mapping."

    parsed, errors = parse_optimization_yaml("")

    assert parsed is None
    assert errors[0].message == "Optimization YAML must be a mapping."

    parsed, errors = parse_optimization_yaml("parameter_ranges: [broken")

    assert parsed is None
    assert "YAML parse error" in errors[0].message


def test_dump_and_analyze_handle_empty_and_non_mapping_optimization() -> None:
    assert dump_optimization_yaml(None) == ""

    missing_analysis = analyze_strategy_optimization({}, None)
    assert missing_analysis.valid is True
    assert missing_analysis.ready_to_run is False
    assert missing_analysis.yaml_content == ""

    invalid_analysis = analyze_strategy_optimization({}, "invalid")
    assert invalid_analysis.valid is False
    assert invalid_analysis.errors[0].message == "Optimization block must be a mapping."


def test_analyze_strategy_optimization_reports_schema_validation_errors() -> None:
    analysis = analyze_strategy_optimization(
        {"entry_filter_params": {}, "exit_trigger_params": {}},
        {
            "description": "invalid",
            "parameter_ranges": {},
            "unexpected": True,
        },
    )

    assert analysis.valid is False
    assert any(issue.path == "optimization" for issue in analysis.errors)


def test_analyze_strategy_optimization_errors_for_invalid_paths() -> None:
    strategy_config = {
        "entry_filter_params": {
            "period_extrema_break": {
                "enabled": True,
                "period": 20,
            }
        },
        "exit_trigger_params": {
            "fundamental": {
                "per": {
                    "enabled": True,
                    "threshold": 15.0,
                    "condition": "below",
                    "exclude_negative": True,
                },
                "pbr": {
                    "enabled": False,
                    "threshold": 1.2,
                    "condition": "below",
                    "exclude_negative": True,
                },
            }
        },
    }
    optimization = {
        "parameter_ranges": {
            "invalid_section": {
                "period_extrema_break": {
                    "period": [10, 20, 30],
                }
            },
            "entry_filter_params": {
                "unknown_signal": {
                    "period": [10, 20, 30],
                },
                "period_extrema_break": {
                    "unknown_param": [10, 20, 30],
                    "period": {"nested": [10, 20, 30]},
                },
            },
            "exit_trigger_params": {
                "fundamental": {
                    "per": [10.0, 15.0],
                    "pbr": {
                        "threshold": [0.8, 1.2, 1.6],
                    },
                }
            },
        }
    }

    analysis = analyze_strategy_optimization(strategy_config, optimization)

    assert analysis.valid is False
    assert any("Section must be entry_filter_params or exit_trigger_params." in issue.message for issue in analysis.errors)
    assert any("does not exist in the current strategy model" in issue.message for issue in analysis.errors)
    assert any("does not exist on the current strategy signal" in issue.message for issue in analysis.errors)
    assert any("resolve to a nested object" in issue.message for issue in analysis.errors)
    assert any("resolve to a concrete leaf value" in issue.message for issue in analysis.errors)
    assert any("Nested signal is disabled" in issue.message for issue in analysis.errors)


def test_analyze_strategy_optimization_reports_missing_enabled_range_drift() -> None:
    strategy_config = {
        "entry_filter_params": {
            "period_extrema_break": {
                "enabled": True,
                "period": 20,
            },
            "fundamental": {
                "per": {
                    "enabled": True,
                    "threshold": 15.0,
                    "condition": "below",
                    "exclude_negative": True,
                }
            },
        },
        "exit_trigger_params": {},
    }
    optimization = {
        "parameter_ranges": {
            "entry_filter_params": {
                "period_extrema_break": {
                    "period": [10, 20, 30],
                }
            }
        }
    }

    analysis = analyze_strategy_optimization(strategy_config, optimization)

    assert analysis.valid is True
    assert any(
        issue.path == "optimization.parameter_ranges.entry_filter_params.fundamental.per"
        and "has no optimization ranges" in issue.message
        for issue in analysis.drift
    )


def test_generate_strategy_optimization_draft_warns_when_enabled_signal_has_no_numeric_candidates(
    monkeypatch,
) -> None:
    class _SignalOnlyToggle(BaseModel):
        enabled: bool = True
        condition: str = "above"

    class _SectionModel(BaseModel):
        toggle_only: _SignalOnlyToggle = Field(default_factory=_SignalOnlyToggle)

    monkeypatch.setattr(
        "src.domains.optimization.strategy_spec._build_section_signal_params",
        lambda _strategy_config: {
            "entry_filter_params": _SectionModel(),
            "exit_trigger_params": _SectionModel(toggle_only=_SignalOnlyToggle(enabled=False)),
        },
    )

    analysis = generate_strategy_optimization_draft({})

    assert analysis.optimization["parameter_ranges"] == {}
    assert any("No numeric optimization candidates" in issue.message for issue in analysis.warnings)


def test_generate_numeric_candidates_respects_schema_constraints() -> None:
    class _CandidateModel(BaseModel):
        int_value: int = Field(ge=5, le=7)
        float_value: float = Field(gt=0.0, lt=0.3)

    int_candidates = _generate_numeric_candidates(6, _CandidateModel, "int_value")
    float_candidates = _generate_numeric_candidates(0.25, _CandidateModel, "float_value")

    assert int_candidates == [5, 6, 7]
    assert float_candidates[0] > 0.0
    assert float_candidates[-1] < 0.3


def test_private_helpers_cover_scalar_guards_and_deduplication() -> None:
    class _CandidateModel(BaseModel):
        int_value: int = Field(ge=5, le=7)

    assert _generate_numeric_candidates(True, _CandidateModel, "int_value") == []
    assert _clamp_int(0, {"minimum": 5}) == 5
    assert _clamp_int(10, {"exclusiveMaximum": 10}) == 9
    assert _clamp_float(0.0, {"exclusiveMinimum": 0.0}) > 0.0
    assert _clamp_float(1.0, {"maximum": 0.3}) == 0.3
    assert _dedupe_issues(
        [
            GridValidationIssue(path="a", message="x"),
            GridValidationIssue(path="a", message="x"),
        ]
    ) == [GridValidationIssue(path="a", message="x")]


def test_calculate_total_combinations_counts_nested_ranges() -> None:
    assert (
        calculate_total_combinations(
            {
                "entry_filter_params": {
                    "period_extrema_break": {
                        "period": [10, 20],
                        "lookback_days": [1, 2, 3],
                    }
                }
            }
        )
        == 6
    )


def test_analyze_strategy_optimization_tolerates_non_mapping_signal_sections() -> None:
    strategy_config = {
        "entry_filter_params": {
            "period_extrema_break": {
                "enabled": True,
                "period": 20,
            },
            "fundamental": {
                "per": {
                    "enabled": True,
                    "threshold": 15.0,
                    "condition": "below",
                    "exclude_negative": True,
                }
            },
        },
        "exit_trigger_params": {},
    }
    optimization = {
        "parameter_ranges": {
            "entry_filter_params": "invalid-shape",
            "exit_trigger_params": {
                "fundamental": "invalid-shape",
            },
        }
    }

    analysis = analyze_strategy_optimization(strategy_config, optimization)

    assert analysis.valid is False
    assert any("valid dictionary" in issue.message for issue in analysis.errors)
    assert any("must be a mapping" in issue.message for issue in analysis.errors)
