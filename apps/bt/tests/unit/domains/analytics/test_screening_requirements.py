from __future__ import annotations

from types import SimpleNamespace

from src.domains.analytics.screening_requirements import (
    build_strategy_data_requirements,
    needs_data_requirement,
    resolve_period_type,
    should_include_forecast_revision,
)
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams


def _shared_config(**overrides: object) -> SharedConfig:
    payload: dict[str, object] = {"universe_preset": "primeExTopix500"}
    payload.update(overrides)
    return SharedConfig.model_validate(payload, context={"resolve_stock_codes": False})


def test_needs_data_requirement_matches_prefix_and_enabled_checker() -> None:
    entry = SignalParams()
    exit_ = SignalParams()
    registry = [
        SimpleNamespace(data_requirements=["benchmark_close"], enabled_checker=lambda _params: True),
        SimpleNamespace(data_requirements=["margin"], enabled_checker=lambda _params: False),
    ]

    assert needs_data_requirement(entry, exit_, "benchmark", signal_registry=registry)
    assert not needs_data_requirement(entry, exit_, "margin", signal_registry=registry)


def test_resolve_period_type_defaults_to_fy() -> None:
    entry = SignalParams()
    exit_ = SignalParams()
    entry.fundamental = entry.fundamental.model_copy(update={"period_type": ""})
    exit_.fundamental = exit_.fundamental.model_copy(update={"period_type": ""})

    assert resolve_period_type(entry, exit_) == "FY"


def test_resolve_period_type_prefers_explicit_fundamental_setting() -> None:
    entry = SignalParams()
    exit_ = SignalParams()
    entry.fundamental = entry.fundamental.model_copy(update={"period_type": ""})
    exit_.fundamental.period_type = "2Q"

    assert resolve_period_type(entry, exit_) == "2Q"


def test_should_include_forecast_revision_when_forecast_signals_are_enabled() -> None:
    entry = SignalParams()
    exit_ = SignalParams()

    entry.fundamental.enabled = True
    entry.fundamental.forward_eps_growth.enabled = True
    assert should_include_forecast_revision(entry, exit_)

    entry.fundamental.forward_eps_growth.enabled = False
    entry.fundamental.forecast_eps_above_recent_fy_actuals.enabled = True
    assert should_include_forecast_revision(entry, exit_)


def test_build_strategy_data_requirements_resolves_keys_and_flags() -> None:
    entry = SignalParams()
    exit_ = SignalParams()
    entry.fundamental.enabled = True
    entry.fundamental.forward_payout_ratio.enabled = True

    registry = [
        SimpleNamespace(data_requirements=["margin"], enabled_checker=lambda _params: True),
        SimpleNamespace(data_requirements=["benchmark_close"], enabled_checker=lambda _params: True),
        SimpleNamespace(data_requirements=["sector"], enabled_checker=lambda _params: False),
    ]

    requirements = build_strategy_data_requirements(
        shared_config=_shared_config(include_margin_data=True, include_statements_data=True),
        entry_params=entry,
        exit_params=exit_,
        stock_codes=("7203", "6758"),
        start_date="2026-01-01",
        end_date="2026-02-01",
        signal_registry=registry,
    )

    assert requirements.include_margin_data is True
    assert requirements.include_statements_data is False
    assert requirements.needs_benchmark is True
    assert requirements.needs_sector is False
    assert requirements.multi_data_key.stock_codes == ("7203", "6758")
    assert requirements.multi_data_key.include_forecast_revision is True
    assert requirements.benchmark_data_key is not None
    assert requirements.sector_data_key is None
    assert requirements.sector_mapping_key is None
