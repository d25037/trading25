"""signal_param_factory.py の分岐テスト。"""

from __future__ import annotations

import random

from src.agent import signal_param_factory


def test_randomize_params_keeps_values_when_no_range_defined() -> None:
    rng = random.Random(0)
    params = {"enabled": True}

    randomized = signal_param_factory.randomize_params("buy_and_hold", params, rng)

    assert randomized == params
    assert randomized is not params


def test_get_default_params_unknown_signal_returns_empty() -> None:
    rng = random.Random(0)
    assert signal_param_factory.get_default_params("unknown_signal", "entry", rng) == {}


def test_get_signal_model_defaults_handles_none_dict_and_plain_values(
    monkeypatch,
) -> None:
    class DummyField:
        def __init__(self, default):
            self._default = default

        def get_default(self, call_default_factory: bool = True):  # noqa: ARG002
            return self._default

    class DummySignalParams:
        model_fields = {
            "none_sig": DummyField(None),
            "dict_sig": DummyField({"foo": 1}),
            "plain_sig": DummyField(123),
        }

    monkeypatch.setattr(signal_param_factory, "SignalParams", DummySignalParams)

    assert signal_param_factory._get_signal_model_defaults("none_sig") == {}
    assert signal_param_factory._get_signal_model_defaults("dict_sig") == {"foo": 1}
    assert signal_param_factory._get_signal_model_defaults("plain_sig") == {}


def test_get_default_fundamental_params_returns_empty_when_model_default_missing(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        signal_param_factory,
        "_get_signal_model_defaults",
        lambda signal_name: {},  # noqa: ARG005
    )

    params = signal_param_factory._get_default_fundamental_params(
        "entry",
        random.Random(0),
    )

    assert params == {}


def test_get_default_fundamental_params_exit_keeps_children_disabled() -> None:
    params = signal_param_factory._get_default_fundamental_params("exit", random.Random(0))

    child_enabled = [
        value.get("enabled")
        for value in params.values()
        if isinstance(value, dict) and "enabled" in value
    ]
    assert child_enabled
    assert all(enabled is False for enabled in child_enabled)
