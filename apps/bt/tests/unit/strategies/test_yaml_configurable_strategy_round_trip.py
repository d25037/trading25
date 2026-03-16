"""YamlConfigurableStrategy の round-trip 関連分岐テスト."""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.domains.strategy.core.yaml_configurable_strategy import YamlConfigurableStrategy
from src.domains.strategy.runtime.compiler import CompiledStrategyIR
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams, Signals


def _shared_config(**overrides: Any) -> SharedConfig:
    payload = {
        "dataset": "sample",
        "stock_codes": ["1111"],
        "printlog": False,
    }
    payload.update(overrides)
    return SharedConfig.model_validate(payload, context={"resolve_stock_codes": False})


def _ohlcv(periods: int = 4) -> pd.DataFrame:
    index = pd.date_range("2024-01-01", periods=periods, freq="D")
    return pd.DataFrame(
        {
            "Open": [10.0, 11.0, 12.0, 13.0],
            "High": [11.0, 12.0, 13.0, 14.0],
            "Low": [9.0, 10.0, 11.0, 12.0],
            "Close": [10.5, 11.5, 12.5, 13.5],
            "Volume": [100, 200, 300, 400],
        },
        index=index,
    )


def _signals(index: pd.DatetimeIndex) -> Signals:
    return Signals(
        entries=pd.Series([True, False, False, False], index=index, dtype=bool),
        exits=pd.Series([False, False, False, False], index=index, dtype=bool),
    )


class TestYamlConfigurableStrategyRoundTrip:
    @pytest.mark.parametrize(
        ("level", "expected_method"),
        [
            ("debug", "debug"),
            ("warning", "warning"),
            ("error", "error"),
            ("critical", "critical"),
            ("info", "info"),
        ],
    )
    def test_log_routes_to_expected_logger_method(
        self,
        level: str,
        expected_method: str,
    ) -> None:
        strategy = YamlConfigurableStrategy(shared_config=_shared_config(printlog=True))
        strategy.logger = MagicMock()

        strategy._log("message", level)

        getattr(strategy.logger, expected_method).assert_called_once_with("message")

    def test_log_suppresses_non_error_messages_when_printlog_is_disabled(self) -> None:
        strategy = YamlConfigurableStrategy(shared_config=_shared_config(printlog=False))
        strategy.logger = MagicMock()

        strategy._log("message", "info")
        strategy._log("error-message", "error")

        strategy.logger.info.assert_not_called()
        strategy.logger.error.assert_called_once_with("error-message")

    def test_generate_signals_forces_last_day_exit_in_standard_mode(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        strategy = YamlConfigurableStrategy(shared_config=_shared_config())
        data = _ohlcv()
        monkeypatch.setattr(
            strategy.signal_processor,
            "generate_signals",
            lambda **kwargs: _signals(cast(pd.DatetimeIndex, data.index)),
        )

        result = strategy.generate_signals(data)

        assert bool(result.exits.iloc[-1]) is True

    def test_generate_signals_skips_forced_exit_in_next_session_round_trip(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        strategy = YamlConfigurableStrategy(
            shared_config=_shared_config(next_session_round_trip=True)
        )
        data = _ohlcv()
        monkeypatch.setattr(
            strategy.signal_processor,
            "generate_signals",
            lambda **kwargs: _signals(cast(pd.DatetimeIndex, data.index)),
        )

        result = strategy.generate_signals(data)

        assert bool(result.exits.iloc[-1]) is False

    def test_generate_signals_skips_forced_exit_in_current_session_round_trip_oracle(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        strategy = YamlConfigurableStrategy(
            shared_config=_shared_config(current_session_round_trip_oracle=True)
        )
        data = _ohlcv()
        monkeypatch.setattr(
            strategy.signal_processor,
            "generate_signals",
            lambda **kwargs: _signals(cast(pd.DatetimeIndex, data.index)),
        )

        result = strategy.generate_signals(data)

        assert bool(result.exits.iloc[-1]) is False

    def test_generate_signals_passes_compiled_strategy_to_processor(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        strategy = YamlConfigurableStrategy(
            shared_config=_shared_config(current_session_round_trip_oracle=True)
        )
        data = _ohlcv()
        captured: dict[str, Any] = {}

        def _fake_generate_signals(**kwargs: Any) -> Signals:
            captured.update(kwargs)
            return _signals(cast(pd.DatetimeIndex, data.index))

        monkeypatch.setattr(
            strategy.signal_processor,
            "generate_signals",
            _fake_generate_signals,
        )

        strategy.generate_signals(data)

        assert isinstance(captured["compiled_strategy"], CompiledStrategyIR)
        assert captured["compiled_strategy"].execution_semantics == "current_session_round_trip_oracle"
        assert "current_session_round_trip_oracle" not in captured

    def test_generate_signals_handles_missing_last_valid_close_without_forced_exit(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        strategy = YamlConfigurableStrategy(shared_config=_shared_config())
        data = _ohlcv()
        data["Close"] = float("nan")
        monkeypatch.setattr(
            strategy.signal_processor,
            "generate_signals",
            lambda **kwargs: _signals(cast(pd.DatetimeIndex, data.index)),
        )

        result = strategy.generate_signals(data)

        assert bool(result.exits.any()) is False

    def test_generate_signals_fails_closed_when_required_fundamental_data_is_missing(
        self,
    ) -> None:
        params = SignalParams()
        params.fundamental.enabled = True
        params.fundamental.forward_eps_growth.enabled = True

        strategy = YamlConfigurableStrategy(
            shared_config=_shared_config(),
            entry_filter_params=params,
        )
        data = _ohlcv()
        statements = pd.DataFrame(
            {
                "EPS": [10.0, 11.0, 12.0, 13.0],
            },
            index=data.index,
        )

        result = strategy.generate_signals(data, statements_data=statements)

        expected_entries = pd.Series(False, index=data.index, dtype=bool)
        pd.testing.assert_series_equal(result.entries, expected_entries)

    def test_generate_multi_signals_restores_stock_code_on_success(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        strategy = YamlConfigurableStrategy(shared_config=_shared_config())
        original_stock_code = strategy.stock_code
        data = _ohlcv()

        def _fake_generate_signals(*args: Any, **kwargs: Any) -> Signals:
            assert strategy.stock_code == "2222"
            return _signals(cast(pd.DatetimeIndex, data.index))

        monkeypatch.setattr(strategy, "generate_signals", _fake_generate_signals)

        result = strategy.generate_multi_signals("2222", data)

        assert isinstance(result, Signals)
        assert strategy.stock_code == original_stock_code

    def test_generate_multi_signals_restores_stock_code_on_failure(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        strategy = YamlConfigurableStrategy(shared_config=_shared_config())
        original_stock_code = strategy.stock_code
        data = _ohlcv()

        def _raise_generate_signals(*args: Any, **kwargs: Any) -> Signals:
            raise RuntimeError("boom")

        monkeypatch.setattr(strategy, "generate_signals", _raise_generate_signals)

        with pytest.raises(RuntimeError, match="boom"):
            strategy.generate_multi_signals("2222", data)

        assert strategy.stock_code == original_stock_code

    def test_build_relative_status_covers_both_branches(self) -> None:
        standard_strategy = YamlConfigurableStrategy(shared_config=_shared_config())
        relative_strategy = YamlConfigurableStrategy(
            shared_config=_shared_config(relative_mode=True)
        )

        assert standard_strategy._build_relative_status() == ""
        assert relative_strategy._build_relative_status() == " (Relative Mode)"

    def test_initialization_copies_round_trip_flag(self) -> None:
        strategy = YamlConfigurableStrategy(
            shared_config=_shared_config(next_session_round_trip=True)
        )

        assert strategy.next_session_round_trip is True
        assert strategy.exit_trigger_params is None

    def test_initialization_copies_current_session_round_trip_oracle_flag(self) -> None:
        strategy = YamlConfigurableStrategy(
            shared_config=_shared_config(current_session_round_trip_oracle=True)
        )

        assert strategy.current_session_round_trip_oracle is True
        assert strategy.exit_trigger_params is None
        assert strategy.compiled_strategy.execution_semantics == "current_session_round_trip_oracle"

    def test_initialization_compiles_next_session_round_trip_semantics(self) -> None:
        strategy = YamlConfigurableStrategy(
            shared_config=_shared_config(next_session_round_trip=True)
        )

        assert strategy.compiled_strategy.execution_semantics == "next_session_round_trip"

    def test_initialization_keeps_oracle_flag_off_when_only_same_day_oracle_signal_is_enabled(self) -> None:
        strategy = YamlConfigurableStrategy(
            shared_config=_shared_config(),
            entry_filter_params=SignalParams.model_validate(
                {"oracle_index_open_gap_regime": {"enabled": True}}
            ),
        )

        assert strategy.current_session_round_trip_oracle is False
        assert strategy.compiled_strategy.execution_semantics == "standard"
