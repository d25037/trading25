"""
Screening service helper tests.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams, Signals
from src.entrypoints.http.schemas.screening import MatchedStrategyItem, ScreeningResultItem
from src.application.services.screening_service import (
    MultiDataRequirementKey,
    ScreeningService,
    ScreeningRequestCache,
    SectorDataRequirementKey,
    StockUniverseItem,
    TopixDataRequirementKey,
    StrategyDataBundle,
    StrategyExecutionInput,
    StrategyRuntime,
    _format_date,
)


class DummyReader:
    def query(self, sql, params=()):  # noqa: ANN001, ANN201
        return []


def _runtime(
    name: str = "test_strategy",
    *,
    shared_overrides: dict[str, object] | None = None,
    entry_params: SignalParams | None = None,
    exit_params: SignalParams | None = None,
) -> StrategyRuntime:
    shared_payload = {"dataset": "primeExTopix500"}
    if shared_overrides:
        shared_payload.update(shared_overrides)

    return StrategyRuntime(
        name=f"production/{name}",
        response_name=name,
        basename=name,
        entry_params=entry_params or SignalParams(),
        exit_params=exit_params or SignalParams(),
        shared_config=SharedConfig.model_validate(
            shared_payload,
            context={"resolve_stock_codes": False},
        ),
    )


class TestSignalMatchingHelpers:
    def test_find_recent_match_date_uses_entry_true_and_exit_false(self):
        service = ScreeningService(DummyReader())
        index = pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"])

        signals = Signals(
            entries=pd.Series([True, True, True], index=index),
            exits=pd.Series([False, True, False], index=index),
        )

        matched = service._find_recent_match_date(signals, recent_days=3)  # noqa: SLF001
        assert matched == "2026-01-03"

    def test_find_recent_match_date_returns_none_when_all_recent_entries_are_exited(self):
        service = ScreeningService(DummyReader())
        index = pd.to_datetime(["2026-01-01", "2026-01-02"])

        signals = Signals(
            entries=pd.Series([True, True], index=index),
            exits=pd.Series([True, True], index=index),
        )

        matched = service._find_recent_match_date(signals, recent_days=2)  # noqa: SLF001
        assert matched is None

    def test_format_date_accepts_string_values(self):
        assert _format_date("2026-02-17T15:30:00") == "2026-02-17"


class TestSortHelpers:
    def test_sort_best_strategy_score_keeps_null_last_for_both_orders(self):
        service = ScreeningService(DummyReader())

        scored = ScreeningResultItem(
            stockCode="1001",
            companyName="A",
            matchedDate="2026-01-03",
            bestStrategyName="s1",
            bestStrategyScore=1.2,
            matchStrategyCount=1,
            matchedStrategies=[
                MatchedStrategyItem(
                    strategyName="s1",
                    matchedDate="2026-01-03",
                    strategyScore=1.2,
                )
            ],
        )
        missing = ScreeningResultItem(
            stockCode="1002",
            companyName="B",
            matchedDate="2026-01-04",
            bestStrategyName="s2",
            bestStrategyScore=None,
            matchStrategyCount=1,
            matchedStrategies=[
                MatchedStrategyItem(
                    strategyName="s2",
                    matchedDate="2026-01-04",
                    strategyScore=None,
                )
            ],
        )

        desc = service._sort_results([missing, scored], "bestStrategyScore", "desc")  # noqa: SLF001
        asc = service._sort_results([missing, scored], "bestStrategyScore", "asc")  # noqa: SLF001

        assert [r.stockCode for r in desc] == ["1001", "1002"]
        assert [r.stockCode for r in asc] == ["1001", "1002"]

    def test_sort_helpers_cover_non_score_sorts_and_fallback(self):
        service = ScreeningService(DummyReader())

        row1 = ScreeningResultItem(
            stockCode="1001",
            companyName="A",
            matchedDate="2026-01-03",
            bestStrategyName="s1",
            bestStrategyScore=1.0,
            matchStrategyCount=2,
            matchedStrategies=[],
        )
        row2 = ScreeningResultItem(
            stockCode="1002",
            companyName="B",
            matchedDate="2026-01-01",
            bestStrategyName="s2",
            bestStrategyScore=2.0,
            matchStrategyCount=1,
            matchedStrategies=[],
        )

        by_date = service._sort_results([row1, row2], "matchedDate", "asc")  # noqa: SLF001
        by_code = service._sort_results([row2, row1], "stockCode", "desc")  # noqa: SLF001
        by_count = service._sort_results([row2, row1], "matchStrategyCount", "desc")  # noqa: SLF001
        passthrough = service._sort_results([row1], "unknown", "asc")  # type: ignore[arg-type]  # noqa: SLF001

        assert [r.stockCode for r in by_date] == ["1002", "1001"]
        assert [r.stockCode for r in by_code] == ["1002", "1001"]
        assert [r.stockCode for r in by_count] == ["1001", "1002"]
        assert passthrough == [row1]


class TestDataLoadingHelpers:
    def test_load_stock_universe_returns_empty_when_no_market_codes(self):
        service = ScreeningService(DummyReader())
        assert service._load_stock_universe([]) == []  # noqa: SLF001

    def test_load_stock_universe_deduplicates_normalized_codes(self):
        reader = DummyReader()
        reader.query = lambda _sql, _params=(): [  # type: ignore[assignment]
            {
                "code": "10010",
                "company_name": "A",
                "scale_category": "Small",
                "sector_33_name": "情報・通信業",
            },
            {
                "code": "1001",
                "company_name": "A duplicate",
                "scale_category": "Small",
                "sector_33_name": "情報・通信業",
            },
        ]
        service = ScreeningService(reader)

        universe = service._load_stock_universe(["prime"])  # noqa: SLF001
        assert len(universe) == 1
        assert universe[0].code == "1001"

    def test_load_latest_metric_handles_missing_invalid_and_non_numeric(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ):
        service = ScreeningService(DummyReader())
        monkeypatch.setattr(
            "src.application.services.screening_service.get_backtest_results_dir",
            lambda basename: tmp_path / basename,
        )

        # missing dir
        score, warning = service._load_latest_metric("missing")  # noqa: SLF001
        assert score is None
        assert warning is None

        # empty dir
        (tmp_path / "empty").mkdir(parents=True)
        score, warning = service._load_latest_metric("empty")  # noqa: SLF001
        assert score is None
        assert warning is None

        # invalid json
        invalid_dir = tmp_path / "invalid"
        invalid_dir.mkdir(parents=True)
        (invalid_dir / "x.metrics.json").write_text("{invalid", encoding="utf-8")
        score, warning = service._load_latest_metric("invalid")  # noqa: SLF001
        assert score is None
        assert warning and "failed to read metrics" in warning

        # missing metric key
        missing_key_dir = tmp_path / "missing-key"
        missing_key_dir.mkdir(parents=True)
        (missing_key_dir / "x.metrics.json").write_text('{"total_return": 1.2}', encoding="utf-8")
        score, warning = service._load_latest_metric("missing-key")  # noqa: SLF001
        assert score is None
        assert warning is None

        # string metric should be parsed
        string_metric_dir = tmp_path / "string-metric"
        string_metric_dir.mkdir(parents=True)
        (string_metric_dir / "x.metrics.json").write_text(
            '{"sharpe_ratio": "1.23"}',
            encoding="utf-8",
        )
        score, warning = service._load_latest_metric("string-metric")  # noqa: SLF001
        assert score == pytest.approx(1.23)
        assert warning is None

        # non-numeric string metric
        bad_metric_dir = tmp_path / "bad-metric"
        bad_metric_dir.mkdir(parents=True)
        (bad_metric_dir / "x.metrics.json").write_text(
            '{"sharpe_ratio": "NaN-value"}',
            encoding="utf-8",
        )
        score, warning = service._load_latest_metric("bad-metric")  # noqa: SLF001
        assert score is None
        assert warning and "is not numeric" in warning

    def test_load_strategy_scores_collects_missing_and_warning(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())
        r1 = _runtime("r1")
        r2 = _runtime("r2")

        def _fake_load_latest_metric(basename: str):
            if basename == "r1":
                return 1.5, None
            return None, "broken"

        monkeypatch.setattr(service, "_load_latest_metric", _fake_load_latest_metric)
        scores, missing, warnings = service._load_strategy_scores([r1, r2])  # noqa: SLF001

        assert scores == {"r1": 1.5, "r2": None}
        assert missing == ["r2"]
        assert warnings == ["r2: broken"]


class TestStrategyResolutionHelpers:
    def test_resolve_strategy_token_paths(self):
        service = ScreeningService(DummyReader())
        metadata_by_name = {
            "production/range_break_v15": object(),
            "production/forward_eps_driven": object(),
        }
        basename_map = {
            "range_break_v15": ["production/range_break_v15"],
            "dupe": ["production/dupe_a", "production/dupe_b"],
        }

        assert (
            service._resolve_strategy_token("production/range_break_v15", metadata_by_name, basename_map)  # noqa: SLF001
            == "production/range_break_v15"
        )
        assert (
            service._resolve_strategy_token("range_break_v15", metadata_by_name, basename_map)  # noqa: SLF001
            == "production/range_break_v15"
        )
        assert service._resolve_strategy_token("dupe", metadata_by_name, basename_map) is None  # noqa: SLF001
        assert service._resolve_strategy_token("missing", metadata_by_name, basename_map) is None  # noqa: SLF001

    def test_resolve_strategies_raises_when_no_production(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())
        monkeypatch.setattr(service._config_loader, "get_strategy_metadata", lambda: [])
        with pytest.raises(ValueError, match="No production strategies found"):
            service._resolve_strategies(None)  # noqa: SLF001

    def test_resolve_strategies_raises_when_requested_list_is_empty(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ):
        from src.shared.paths.resolver import StrategyMetadata

        service = ScreeningService(DummyReader())
        meta = StrategyMetadata(
            name="production/range_break_v15",
            category="production",
            path=tmp_path / "production/range_break_v15.yaml",
            mtime=datetime.now(),
        )

        monkeypatch.setattr(service._config_loader, "get_strategy_metadata", lambda: [meta])
        monkeypatch.setattr(
            service._config_loader,
            "load_strategy_config",
            lambda _name: {"entry_filter_params": {}, "exit_trigger_params": {}},
        )
        monkeypatch.setattr(
            service._config_loader,
            "merge_shared_config",
            lambda _config: {"dataset": "primeExTopix500"},
        )

        with pytest.raises(ValueError, match="No valid production strategies selected"):
            service._resolve_strategies(",")  # noqa: SLF001


class TestRuntimeEvaluationHelpers:
    def test_resolve_date_range_uses_reference_date_as_end(self):
        service = ScreeningService(DummyReader())
        shared = SharedConfig.model_validate(
            {
                "dataset": "primeExTopix500",
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
            },
            context={"resolve_stock_codes": False},
        )

        start, end = service._resolve_date_range(shared, "2024-06-30", 10)  # noqa: SLF001
        assert start == "2024-01-01"
        assert end == "2024-06-30"

        start, end = service._resolve_date_range(shared, "2025-01-01", 10)  # noqa: SLF001
        assert start == "2024-01-01"
        assert end == "2025-01-01"

    def test_resolve_date_range_defaults_to_latest_market_date_with_history_window(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())
        shared = SharedConfig.model_validate(
            {"dataset": "primeExTopix500"},
            context={"resolve_stock_codes": False},
        )

        monkeypatch.setattr(service, "_get_latest_market_date", lambda: "2026-02-17")
        monkeypatch.setattr(service, "_resolve_history_trading_days", lambda _recent_days: 20)
        monkeypatch.setattr(service, "_get_trading_date_before", lambda _date, _offset: "2026-01-20")

        start, end = service._resolve_date_range(shared, None, 10)  # noqa: SLF001

        assert start == "2026-01-20"
        assert end == "2026-02-17"

    def test_load_multi_data_uses_market_loader(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())
        key = MultiDataRequirementKey(
            stock_codes=("7203",),
            start_date="2026-01-01",
            end_date="2026-02-17",
            include_margin_data=False,
            include_statements_data=True,
            timeframe="daily",
            period_type="FY",
            include_forecast_revision=True,
        )

        index = pd.to_datetime(["2026-02-17"])
        market_data = {
            "7203": {
                "daily": pd.DataFrame(
                    {"Open": [1.0], "High": [1.0], "Low": [1.0], "Close": [1.0], "Volume": [10]},
                    index=index,
                )
            }
        }

        monkeypatch.setattr(
            "src.application.services.screening_service.load_market_multi_data",
            lambda *_args, **_kwargs: (market_data, []),
        )

        loaded = service._load_multi_data(key)  # noqa: SLF001
        assert loaded == market_data

    def test_load_multi_data_returns_empty_for_weekly_timeframe(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())
        key = MultiDataRequirementKey(
            stock_codes=("7203",),
            start_date="2026-01-01",
            end_date="2026-02-17",
            include_margin_data=False,
            include_statements_data=False,
            timeframe="weekly",
            period_type="FY",
            include_forecast_revision=False,
        )

        monkeypatch.setattr(
            "src.application.services.screening_service.load_market_multi_data",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not be called")),
        )

        assert service._load_multi_data(key) == {}  # noqa: SLF001

    def test_load_multi_data_margin_and_loader_warnings_are_logged(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())
        key = MultiDataRequirementKey(
            stock_codes=("7203",),
            start_date="2026-01-01",
            end_date="2026-02-17",
            include_margin_data=True,
            include_statements_data=False,
            timeframe="daily",
            period_type="FY",
            include_forecast_revision=False,
        )
        logged: list[str] = []

        def _warn(message: str, *args, **_kwargs):  # noqa: ANN001
            text = message.format(*args) if args else message
            logged.append(text)

        monkeypatch.setattr("src.application.services.screening_service.logger.warning", _warn)
        monkeypatch.setattr(
            "src.application.services.screening_service.load_market_multi_data",
            lambda *_args, **_kwargs: ({}, ["loader warning"]),
        )

        assert service._load_multi_data(key) == {}  # noqa: SLF001
        assert any("does not provide margin data" in message for message in logged)
        assert any("screening market loader warning: loader warning" in message for message in logged)

    def test_market_loader_wrappers_for_benchmark_sector_and_mapping(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())
        benchmark = pd.DataFrame({"Close": [1.0]}, index=pd.to_datetime(["2026-02-17"]))
        sector = {"情報・通信業": benchmark}
        mapping = {"7203": "輸送用機器"}

        monkeypatch.setattr(
            "src.application.services.screening_service.load_market_topix_data",
            lambda *_args, **_kwargs: benchmark,
        )
        monkeypatch.setattr(
            "src.application.services.screening_service.load_market_sector_indices",
            lambda *_args, **_kwargs: sector,
        )
        monkeypatch.setattr(
            "src.application.services.screening_service.load_market_stock_sector_mapping",
            lambda *_args, **_kwargs: mapping,
        )

        assert (
            service._load_benchmark_data(  # noqa: SLF001
                key=SimpleNamespace(start_date="2026-01-01", end_date="2026-02-17")
            )
            is benchmark
        )
        assert (
            service._load_sector_data(  # noqa: SLF001
                key=SimpleNamespace(start_date="2026-01-01", end_date="2026-02-17")
            )
            is sector
        )
        assert service._load_sector_mapping() is mapping  # noqa: SLF001

    def test_resolve_history_trading_days_respects_env_and_recent_days(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())

        monkeypatch.delenv("BT_SCREENING_HISTORY_TRADING_DAYS", raising=False)
        assert service._resolve_history_trading_days(10) == 520  # noqa: SLF001

        monkeypatch.setenv("BT_SCREENING_HISTORY_TRADING_DAYS", "30")
        assert service._resolve_history_trading_days(10) == 30  # noqa: SLF001

        monkeypatch.setenv("BT_SCREENING_HISTORY_TRADING_DAYS", "3")
        assert service._resolve_history_trading_days(10) == 10  # noqa: SLF001

        monkeypatch.setenv("BT_SCREENING_HISTORY_TRADING_DAYS", "invalid")
        assert service._resolve_history_trading_days(10) == 520  # noqa: SLF001

    def test_get_latest_market_date_handles_none_and_error(self):
        class _ReaderWithDate(DummyReader):
            def query_one(self, _sql, _params=()):  # noqa: ANN001, ANN201
                return {"max_date": "2026-02-17"}

        class _ReaderWithNone(DummyReader):
            def query_one(self, _sql, _params=()):  # noqa: ANN001, ANN201
                return None

        class _ReaderWithError(DummyReader):
            def query_one(self, _sql, _params=()):  # noqa: ANN001, ANN201
                raise RuntimeError("db error")

        assert ScreeningService(_ReaderWithDate())._get_latest_market_date() == "2026-02-17"  # noqa: SLF001
        assert ScreeningService(_ReaderWithNone())._get_latest_market_date() is None  # noqa: SLF001
        assert ScreeningService(_ReaderWithError())._get_latest_market_date() is None  # noqa: SLF001

    def test_get_trading_date_before_handles_branches(self):
        class _Reader(DummyReader):
            def __init__(self, responses):  # noqa: ANN001
                self._responses = iter(responses)

            def query_one(self, _sql, _params=()):  # noqa: ANN001, ANN201
                result = next(self._responses)
                if isinstance(result, Exception):
                    raise result
                return result

        service = ScreeningService(_Reader([]))
        assert service._get_trading_date_before("2026-02-17", -1) == "2026-02-17"  # noqa: SLF001

        direct = ScreeningService(_Reader([{"date": "2026-02-10"}]))
        assert direct._get_trading_date_before("2026-02-17", 2) == "2026-02-10"  # noqa: SLF001

        fallback = ScreeningService(_Reader([None, {"min_date": "2020-01-01"}]))
        assert fallback._get_trading_date_before("2026-02-17", 2000) == "2020-01-01"  # noqa: SLF001

        no_oldest = ScreeningService(_Reader([None, None]))
        assert no_oldest._get_trading_date_before("2026-02-17", 2000) is None  # noqa: SLF001

        query_error = ScreeningService(_Reader([RuntimeError("boom")]))
        assert query_error._get_trading_date_before("2026-02-17", 2) is None  # noqa: SLF001

        oldest_error = ScreeningService(_Reader([None, RuntimeError("boom")]))
        assert oldest_error._get_trading_date_before("2026-02-17", 2000) is None  # noqa: SLF001

    def test_needs_data_requirement_with_custom_registry(self, monkeypatch: pytest.MonkeyPatch):
        service = ScreeningService(DummyReader())
        entry_params = SignalParams()
        exit_params = SignalParams()

        registry = [
            SimpleNamespace(
                data_requirements=["benchmark:topix"],
                enabled_checker=lambda _params: True,
            ),
            SimpleNamespace(
                data_requirements=["sector"],
                enabled_checker=lambda _params: False,
            ),
        ]
        monkeypatch.setattr("src.application.services.screening_service.SIGNAL_REGISTRY", registry)

        assert service._needs_data_requirement(entry_params, exit_params, "benchmark")  # noqa: SLF001
        assert not service._needs_data_requirement(entry_params, exit_params, "margin")  # noqa: SLF001

    def test_resolve_period_type_and_forecast_revision_flags(self):
        service = ScreeningService(DummyReader())

        entry = SignalParams()
        exit_ = SignalParams()
        entry.fundamental.period_type = ""  # type: ignore[assignment]
        exit_.fundamental.period_type = ""  # type: ignore[assignment]
        assert service._resolve_period_type(entry, exit_) == "FY"  # noqa: SLF001

        entry.fundamental.enabled = True
        entry.fundamental.forward_eps_growth.enabled = True
        assert service._should_include_forecast_revision(entry, exit_)  # noqa: SLF001

        entry.fundamental.forward_eps_growth.enabled = False
        entry.fundamental.peg_ratio.enabled = True
        assert service._should_include_forecast_revision(entry, exit_)  # noqa: SLF001

        entry.fundamental.peg_ratio.enabled = False
        entry.fundamental.forward_dividend_growth.enabled = True
        assert service._should_include_forecast_revision(entry, exit_)  # noqa: SLF001

        entry.fundamental.forward_dividend_growth.enabled = False
        entry.fundamental.forward_payout_ratio.enabled = True
        assert service._should_include_forecast_revision(entry, exit_)  # noqa: SLF001

        entry.fundamental.forward_payout_ratio.enabled = False
        entry.fundamental.forecast_eps_above_all_actuals.enabled = True
        assert service._should_include_forecast_revision(entry, exit_)  # noqa: SLF001

        entry.fundamental.enabled = False
        entry.fundamental.forecast_eps_above_all_actuals.enabled = False
        assert not service._should_include_forecast_revision(entry, exit_)  # noqa: SLF001

    def test_build_result_item_and_best_strategy_helpers(self):
        service = ScreeningService(DummyReader())
        stock = StockUniverseItem(
            code="1001",
            company_name="A",
            scale_category="Large",
            sector_33_name="情報・通信業",
        )
        aggregated = {
            "stock": stock,
            "matchedDate": "2026-01-10",
            "matchedStrategies": [
                MatchedStrategyItem(strategyName="b", matchedDate="2026-01-08", strategyScore=None),
                MatchedStrategyItem(strategyName="a", matchedDate="2026-01-09", strategyScore=1.5),
            ],
        }
        row = service._build_result_item(aggregated)  # noqa: SLF001
        assert row.bestStrategyName == "a"
        assert row.matchedStrategies[0].strategyName == "a"

        null_only = [
            MatchedStrategyItem(strategyName="x", matchedDate="2026-01-01", strategyScore=None),
            MatchedStrategyItem(strategyName="y", matchedDate="2026-01-02", strategyScore=None),
        ]
        assert service._pick_best_strategy(null_only).strategyName == "y"  # noqa: SLF001
        with pytest.raises(ValueError, match="matched_strategies is empty"):
            service._pick_best_strategy([])  # noqa: SLF001

    def test_dedupe_warnings_truncates_and_keeps_order(self):
        service = ScreeningService(DummyReader())
        service._WARNING_LIMIT = 3  # noqa: SLF001
        warnings = ["a", "a", "b", "c", "d"]
        deduped = service._dedupe_warnings(warnings)  # noqa: SLF001
        assert deduped == ["a", "b", "c", "additional warnings were truncated"]

    def test_evaluate_strategy_handles_signal_failures(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())
        strategy = _runtime(
            "range_break_v15",
            shared_overrides={"include_margin_data": True, "include_statements_data": True},
        )
        universe = [
            StockUniverseItem(code="1001", company_name="A", scale_category=None, sector_33_name=None),
            StockUniverseItem(code="1002", company_name="B", scale_category=None, sector_33_name=None),
            StockUniverseItem(code="1003", company_name="C", scale_category=None, sector_33_name=None),
        ]

        index = pd.to_datetime(["2026-01-01", "2026-01-02"])
        daily = pd.DataFrame({"close": [1.0, 1.1]}, index=index)

        data_bundle = StrategyDataBundle(
            multi_data={
                "1001": {"daily": daily, "margin_daily": "m1", "statements_daily": "s1"},
                "1002": {},
                "1003": {"daily": daily, "margin_daily": "m3", "statements_daily": "s3"},
            },
            benchmark_data=None,
            sector_data=None,
            stock_sector_mapping={},
        )

        calls: list[dict[str, object]] = []

        def _generate_signals(**kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                return Signals(
                    entries=pd.Series([True, True], index=index),
                    exits=pd.Series([False, False], index=index),
                )
            raise RuntimeError("signal failed")

        monkeypatch.setattr(service._signal_processor, "generate_signals", _generate_signals)

        matches, processed, warnings = service._evaluate_strategy(  # noqa: SLF001
            strategy=strategy,
            stock_universe=universe,
            recent_days=2,
            data_bundle=data_bundle,
        )

        assert len(matches) == 1
        assert matches[0][0].code == "1001"
        assert processed == {"1001", "1003"}
        assert any("signal generation failed" in warning for warning in warnings)
        assert calls[0]["margin_data"] == "m1"
        assert calls[0]["statements_data"] == "s1"
        assert calls[0]["screening_recent_days"] == 2
        assert calls[0]["skip_exit_when_no_recent_entry"] is True

    def test_evaluate_strategy_short_circuit_when_universe_empty(self):
        service = ScreeningService(DummyReader())
        strategy = _runtime("any")
        matches, processed, warnings = service._evaluate_strategy(  # noqa: SLF001
            strategy=strategy,
            stock_universe=[],
            recent_days=10,
            data_bundle=StrategyDataBundle(multi_data={}),
        )
        assert matches == []
        assert processed == set()
        assert warnings == []

    def test_evaluate_stock_reuses_per_stock_signal_cache(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())
        s1 = _runtime("s1")
        s2 = _runtime("s2")
        stock = StockUniverseItem(
            code="1001",
            company_name="A",
            scale_category=None,
            sector_33_name=None,
        )
        index = pd.to_datetime(["2026-01-01", "2026-01-02"])
        daily = pd.DataFrame(
            {"Close": [1.0, 1.1], "Volume": [100.0, 120.0]},
            index=index,
        )
        shared_bundle = StrategyDataBundle(
            multi_data={
                "1001": {
                    "daily": daily,
                    "margin_daily": None,
                    "statements_daily": None,
                }
            }
        )
        strategy_inputs = [
            StrategyExecutionInput(strategy=s1, data_bundle=shared_bundle, load_warnings=[]),
            StrategyExecutionInput(strategy=s2, data_bundle=shared_bundle, load_warnings=[]),
        ]

        call_count = {"count": 0}

        def _generate_signals(**_kwargs):
            call_count["count"] += 1
            return Signals(
                entries=pd.Series([True, True], index=index),
                exits=pd.Series([False, False], index=index),
            )

        monkeypatch.setattr(service._signal_processor, "generate_signals", _generate_signals)

        strategy_cache_tokens = {
            s1.response_name: service._build_strategy_signal_cache_token(s1),  # noqa: SLF001
            s2.response_name: service._build_strategy_signal_cache_token(s2),  # noqa: SLF001
        }
        outcome = service._evaluate_stock(  # noqa: SLF001
            stock=stock,
            strategy_inputs=strategy_inputs,
            recent_days=2,
            strategy_cache_tokens=strategy_cache_tokens,
        )

        assert call_count["count"] == 1
        assert outcome.processed_strategy_names == {s1.response_name, s2.response_name}
        assert outcome.matched_dates_by_strategy == {
            s1.response_name: "2026-01-02",
            s2.response_name: "2026-01-02",
        }

    def test_evaluate_stock_continues_when_single_strategy_hits_unexpected_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())
        s1 = _runtime("s1")
        s2 = _runtime("s2")
        stock = StockUniverseItem(
            code="1001",
            company_name="A",
            scale_category=None,
            sector_33_name=None,
        )
        index = pd.to_datetime(["2026-01-01", "2026-01-02"])
        daily = pd.DataFrame(
            {"Close": [1.0, 1.1], "Volume": [100.0, 120.0]},
            index=index,
        )
        shared_bundle = StrategyDataBundle(
            multi_data={"1001": {"daily": daily}},
        )
        strategy_inputs = [
            StrategyExecutionInput(strategy=s1, data_bundle=shared_bundle, load_warnings=[]),
            StrategyExecutionInput(strategy=s2, data_bundle=shared_bundle, load_warnings=[]),
        ]

        monkeypatch.setattr(
            service._signal_processor,  # noqa: SLF001
            "generate_signals",
            lambda **_kwargs: Signals(
                entries=pd.Series([True, True], index=index),
                exits=pd.Series([False, False], index=index),
            ),
        )

        calls = {"count": 0}

        def _find_recent_match_date(_signals, _recent_days):  # noqa: ANN001
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("unexpected matcher error")
            return "2026-01-02"

        monkeypatch.setattr(service, "_find_recent_match_date", _find_recent_match_date)

        strategy_cache_tokens = {
            s1.response_name: service._build_strategy_signal_cache_token(s1),  # noqa: SLF001
            s2.response_name: service._build_strategy_signal_cache_token(s2),  # noqa: SLF001
        }
        outcome = service._evaluate_stock(  # noqa: SLF001
            stock=stock,
            strategy_inputs=strategy_inputs,
            recent_days=2,
            strategy_cache_tokens=strategy_cache_tokens,
        )

        assert outcome.processed_strategy_names == {s1.response_name, s2.response_name}
        assert outcome.matched_dates_by_strategy == {s2.response_name: "2026-01-02"}
        assert outcome.warning_by_strategy == [
            (s1.response_name, "1001 evaluation failed (unexpected matcher error)")
        ]

    def test_evaluate_strategies_keeps_load_warnings_when_universe_empty(self):
        service = ScreeningService(DummyReader())
        strategy = _runtime("s1")
        strategy_inputs = [
            StrategyExecutionInput(
                strategy=strategy,
                data_bundle=StrategyDataBundle(multi_data={}),
                load_warnings=["multi data load failed (boom)"],
            )
        ]

        results, warnings, worker_count = service._evaluate_strategies(  # noqa: SLF001
            strategy_inputs=strategy_inputs,
            stock_universe=[],
            recent_days=10,
            progress_callback=None,
        )

        assert warnings == []
        assert worker_count == 1
        assert len(results) == 1
        assert results[0].warnings == ["multi data load failed (boom)"]


class TestRequestCacheHelpers:
    def test_multi_data_cache_caches_success_and_error(self):
        cache = ScreeningRequestCache()
        success_key = MultiDataRequirementKey(
            stock_codes=("7203",),
            start_date="2026-01-01",
            end_date="2026-02-17",
            include_margin_data=False,
            include_statements_data=False,
            timeframe="daily",
            period_type="FY",
            include_forecast_revision=False,
        )

        success_calls = {"count": 0}

        def _load_success():
            success_calls["count"] += 1
            return {"7203": {"daily": pd.DataFrame()}}

        first = cache.get_multi_data(success_key, _load_success)
        second = cache.get_multi_data(success_key, _load_success)
        assert first == second
        assert success_calls["count"] == 1

        error_key = MultiDataRequirementKey(
            stock_codes=("6758",),
            start_date="2026-01-01",
            end_date="2026-02-17",
            include_margin_data=False,
            include_statements_data=False,
            timeframe="daily",
            period_type="FY",
            include_forecast_revision=False,
        )

        error_calls = {"count": 0}

        def _load_error():
            error_calls["count"] += 1
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            cache.get_multi_data(error_key, _load_error)
        with pytest.raises(RuntimeError, match="boom"):
            cache.get_multi_data(error_key, _load_error)
        assert error_calls["count"] == 1

    def test_optional_data_cache_records_hits_and_warnings(self):
        cache = ScreeningRequestCache()

        benchmark_key = TopixDataRequirementKey(start_date="2026-01-01", end_date="2026-02-17")
        benchmark_first = cache.get_benchmark_data(
            benchmark_key,
            loader=lambda: (_ for _ in ()).throw(RuntimeError("benchmark failed")),
        )
        benchmark_second = cache.get_benchmark_data(
            benchmark_key,
            loader=lambda: pd.DataFrame({"Close": [1.0]}),
        )
        assert benchmark_first.warning == "benchmark failed"
        assert benchmark_second.warning == "benchmark failed"

        sector_key = SectorDataRequirementKey(start_date="2026-01-01", end_date="2026-02-17")
        sector_first = cache.get_sector_data(
            sector_key,
            loader=lambda: (_ for _ in ()).throw(RuntimeError("sector failed")),
        )
        sector_second = cache.get_sector_data(
            sector_key,
            loader=lambda: {"情報・通信業": pd.DataFrame({"Close": [1.0]})},
        )
        assert sector_first.warning == "sector failed"
        assert sector_second.warning == "sector failed"

        mapping_first = cache.get_sector_mapping(
            "market_stocks",
            loader=lambda: (_ for _ in ()).throw(RuntimeError("mapping failed")),
        )
        mapping_second = cache.get_sector_mapping(
            "market_stocks",
            loader=lambda: {"7203": "輸送用機器"},
        )
        assert mapping_first.warning == "mapping failed"
        assert mapping_second.warning == "mapping failed"
        assert cache.stats.hits >= 3
