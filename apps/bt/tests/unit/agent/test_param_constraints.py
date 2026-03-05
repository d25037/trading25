"""param_constraints.py のテスト"""

import math

from src.domains.lab_agent.param_constraints import apply_param_dependency_constraints


def test_long_period_depends_on_short_period() -> None:
    lower, upper = apply_param_dependency_constraints(
        key="long_period",
        min_val=50.0,
        max_val=300.0,
        param_type="int",
        sibling_params={"short_period": 120},
    )
    assert lower == 121.0
    assert upper == 300.0


def test_short_period_depends_on_long_period() -> None:
    lower, upper = apply_param_dependency_constraints(
        key="short_period",
        min_val=10.0,
        max_val=100.0,
        param_type="int",
        sibling_params={"long_period": 40},
    )
    assert lower == 10.0
    assert upper == 39.0


def test_slow_fast_constraints() -> None:
    slow_lower, slow_upper = apply_param_dependency_constraints(
        key="slow_period",
        min_val=10.0,
        max_val=100.0,
        param_type="int",
        sibling_params={"fast_period": 25},
    )
    fast_lower, fast_upper = apply_param_dependency_constraints(
        key="fast_period",
        min_val=5.0,
        max_val=80.0,
        param_type="int",
        sibling_params={"slow_period": 20},
    )

    assert slow_lower == 26.0
    assert slow_upper == 100.0
    assert fast_lower == 5.0
    assert fast_upper == 19.0


def test_threshold_constraints() -> None:
    max_lower, max_upper = apply_param_dependency_constraints(
        key="max_threshold",
        min_val=0.1,
        max_val=2.0,
        param_type="float",
        sibling_params={"min_threshold": 0.8},
    )
    min_lower, min_upper = apply_param_dependency_constraints(
        key="min_threshold",
        min_val=0.1,
        max_val=2.0,
        param_type="float",
        sibling_params={"max_threshold": 1.2},
    )

    assert max_lower > 0.8
    assert max_upper == 2.0
    assert min_lower == 0.1
    assert min_upper < 1.2


def test_beta_constraints() -> None:
    max_lower, max_upper = apply_param_dependency_constraints(
        key="max_beta",
        min_val=0.1,
        max_val=3.0,
        param_type="float",
        sibling_params={"min_beta": 1.1},
    )
    min_lower, min_upper = apply_param_dependency_constraints(
        key="min_beta",
        min_val=0.1,
        max_val=3.0,
        param_type="float",
        sibling_params={"max_beta": 1.4},
    )

    assert max_lower > 1.1
    assert max_upper == 3.0
    assert min_lower == 0.1
    assert min_upper < 1.4


def test_int_range_collapses_when_constraints_invert_bounds() -> None:
    lower, upper = apply_param_dependency_constraints(
        key="short_period",
        min_val=10.0,
        max_val=100.0,
        param_type="int",
        sibling_params={"long_period": 10},
    )
    assert lower == upper


def test_float_range_uses_nextafter_when_constraints_invert_bounds() -> None:
    lower, upper = apply_param_dependency_constraints(
        key="min_threshold",
        min_val=0.1,
        max_val=2.0,
        param_type="float",
        sibling_params={"max_threshold": 0.1},
    )

    assert lower < upper
    assert math.isclose(lower, 0.1)
