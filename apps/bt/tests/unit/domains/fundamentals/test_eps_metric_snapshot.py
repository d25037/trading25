from __future__ import annotations

import math

import pytest

from src.domains.fundamentals.eps_metric_snapshot import build_eps_metric_snapshot


def test_build_eps_metric_snapshot_keeps_ratio_and_change_rate_consistent() -> None:
    snapshot = build_eps_metric_snapshot(actual_eps=0.77, forecast_eps=80.78)

    assert snapshot.actual_eps == pytest.approx(0.77)
    assert snapshot.forecast_eps == pytest.approx(80.78)
    assert snapshot.forecast_to_actual_ratio == pytest.approx(104.9091)
    assert snapshot.forecast_eps_change_rate == pytest.approx(10390.9091)


@pytest.mark.parametrize("actual_eps", [0.0, -1.0, math.nan])
def test_build_eps_metric_snapshot_rejects_undefined_ratio(actual_eps: float) -> None:
    snapshot = build_eps_metric_snapshot(actual_eps=actual_eps, forecast_eps=80.78)

    assert snapshot.forecast_to_actual_ratio is None
