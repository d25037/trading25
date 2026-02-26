"""DataManagerMixin の実行経路テスト."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.domains.strategy.core.mixins.data_manager_mixin import DataManagerMixin
from src.shared.models.signals import SignalParams


def _ohlcv_df(start: str = "2020-01-01", periods: int = 4) -> pd.DataFrame:
    idx = pd.date_range(start, periods=periods, freq="D")
    return pd.DataFrame(
        {
            "Open": [10.0, 11.0, 12.0, 13.0],
            "High": [11.0, 12.0, 13.0, 14.0],
            "Low": [9.0, 10.0, 11.0, 12.0],
            "Close": [10.5, 11.5, 12.5, 13.5],
            "Volume": [100.0, 200.0, 300.0, 400.0],
        },
        index=idx,
    )


class _DataManagerStrategy(DataManagerMixin):
    def __init__(self) -> None:
        self.dataset = "primeExTopix500"
        self.stock_codes = ["1111"]
        self.start_date = None
        self.end_date = None
        self.timeframe = "daily"
        self.include_margin_data = True
        self.include_statements_data = True
        self.multi_data_dict = None
        self.relative_data_dict = None
        self.execution_data_dict = None
        self.benchmark_data = None
        self.benchmark_table = "topix"
        self.entry_filter_params = None
        self.exit_trigger_params = None
        self.logs: list[tuple[str, str]] = []
        self._should_load_margin_data = lambda: False
        self._should_load_statements_data = lambda: False

    def _log(self, message: str, level: str = "info") -> None:
        self.logs.append((level, message))


class TestDataManagerMixinPaths:
    def test_load_benchmark_data_success_and_cached(self, monkeypatch) -> None:
        strategy = _DataManagerStrategy()
        calls = {"count": 0}

        def fake_load_topix(_dataset, _start, _end):
            calls["count"] += 1
            return _ohlcv_df()[["Open", "High", "Low", "Close"]]

        monkeypatch.setattr(
            "src.domains.strategy.core.mixins.data_manager_mixin.load_topix_data",
            fake_load_topix,
        )

        first = strategy.load_benchmark_data()
        second = strategy.load_benchmark_data()
        assert not first.empty
        assert first.equals(second)
        assert calls["count"] == 1

    def test_load_benchmark_data_error(self, monkeypatch) -> None:
        strategy = _DataManagerStrategy()

        def raise_error(*_args, **_kwargs):
            raise RuntimeError("load fail")

        monkeypatch.setattr(
            "src.domains.strategy.core.mixins.data_manager_mixin.load_topix_data",
            raise_error,
        )

        with pytest.raises(ValueError):
            strategy.load_benchmark_data()

    def test_load_relative_data_success_and_fallback(self, monkeypatch) -> None:
        strategy = _DataManagerStrategy()
        strategy.stock_codes = ["1111"]
        daily = _ohlcv_df()
        weekly = _ohlcv_df("2020-01-01", 4)

        strategy.load_benchmark_data = MagicMock(
            return_value=_ohlcv_df()[["Open", "High", "Low", "Close"]]
        )
        strategy.load_multi_data = MagicMock(
            return_value={"1111": {"daily": daily, "weekly": weekly}}
        )

        call_state = {"count": 0}

        def fake_relative(stock_df, _benchmark):
            call_state["count"] += 1
            if call_state["count"] == 2:
                raise RuntimeError("relative fail")
            out = stock_df.copy()
            out["Close"] = out["Close"] / 2
            return out

        monkeypatch.setattr(
            "src.domains.strategy.core.mixins.data_manager_mixin.create_relative_ohlc_data",
            fake_relative,
        )

        relative_data, execution_data = strategy.load_relative_data()
        assert "1111" in relative_data
        assert "1111" in execution_data
        assert relative_data["1111"]["daily"]["Close"].iloc[0] == pytest.approx(
            daily["Close"].iloc[0] / 2
        )
        assert relative_data["1111"]["weekly"].equals(weekly)

    def test_load_relative_data_uses_cache(self) -> None:
        strategy = _DataManagerStrategy()
        cached = {"1111": {"daily": _ohlcv_df()}}
        strategy.relative_data_dict = cached
        strategy.execution_data_dict = cached

        strategy.load_benchmark_data = MagicMock(side_effect=AssertionError("should not call"))
        strategy.load_multi_data = MagicMock(side_effect=AssertionError("should not call"))

        relative_data, execution_data = strategy.load_relative_data()
        assert relative_data is cached
        assert execution_data is cached

    def test_load_multi_data_warning_when_required_but_disabled(self, monkeypatch) -> None:
        strategy = _DataManagerStrategy()
        strategy.include_margin_data = False
        strategy.include_statements_data = False
        strategy._should_load_margin_data = lambda: True
        strategy._should_load_statements_data = lambda: True

        captured: dict[str, object] = {}

        def fake_prepare_multi_data(**kwargs):
            captured.update(kwargs)
            return {"1111": {"daily": _ohlcv_df()}}

        monkeypatch.setattr(
            "src.domains.strategy.core.mixins.data_manager_mixin.prepare_multi_data",
            fake_prepare_multi_data,
        )

        got = strategy.load_multi_data()
        assert "1111" in got
        assert captured["include_margin_data"] is False
        assert captured["include_statements_data"] is False
        assert any("include_margin_data=false" in msg for _lvl, msg in strategy.logs)
        assert any("include_statements_data=false" in msg for _lvl, msg in strategy.logs)

    def test_load_multi_data_enables_forecast_revision_for_forecast_vs_actual_signal(
        self, monkeypatch
    ) -> None:
        strategy = _DataManagerStrategy()
        strategy._should_load_margin_data = lambda: False
        strategy._should_load_statements_data = lambda: True
        strategy.entry_filter_params = SignalParams()
        strategy.entry_filter_params.fundamental.enabled = True
        strategy.entry_filter_params.fundamental.forecast_eps_above_recent_fy_actuals.enabled = True

        captured: dict[str, object] = {}

        def fake_prepare_multi_data(**kwargs):
            captured.update(kwargs)
            return {"1111": {"daily": _ohlcv_df()}}

        monkeypatch.setattr(
            "src.domains.strategy.core.mixins.data_manager_mixin.prepare_multi_data",
            fake_prepare_multi_data,
        )

        strategy.load_multi_data()
        assert captured["include_forecast_revision"] is True
