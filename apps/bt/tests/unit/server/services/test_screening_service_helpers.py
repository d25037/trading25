"""
Screening service helper tests.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from src.models.config import SharedConfig
from src.models.signals import SignalParams, Signals
from src.server.schemas.screening import MatchedStrategyItem, ScreeningResultItem
from src.server.services.screening_service import (
    ScreeningService,
    StockUniverseItem,
    StrategyDataBundle,
    StrategyRuntime,
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
            "src.server.services.screening_service.get_backtest_results_dir",
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
        from src.paths.resolver import StrategyMetadata

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

    def test_resolve_strategies_success_with_duplicate_basename_uses_full_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ):
        from src.paths.resolver import StrategyMetadata

        service = ScreeningService(DummyReader())
        meta_a = StrategyMetadata(
            name="production/group_a/same",
            category="production",
            path=tmp_path / "production/group_a/same.yaml",
            mtime=datetime.now(),
        )
        meta_b = StrategyMetadata(
            name="production/group_b/same",
            category="production",
            path=tmp_path / "production/group_b/same.yaml",
            mtime=datetime.now(),
        )

        monkeypatch.setattr(service._config_loader, "get_strategy_metadata", lambda: [meta_a, meta_b])
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

        runtimes = service._resolve_strategies(None)  # noqa: SLF001

        assert {runtime.name for runtime in runtimes} == {
            "production/group_a/same",
            "production/group_b/same",
        }
        assert {runtime.response_name for runtime in runtimes} == {
            "production/group_a/same",
            "production/group_b/same",
        }

    def test_resolve_strategies_rejects_invalid_requested_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ):
        from src.paths.resolver import StrategyMetadata

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

        with pytest.raises(ValueError, match="Invalid strategies"):
            service._resolve_strategies("range_break_v15,unknown")  # noqa: SLF001


class TestRuntimeEvaluationHelpers:
    def test_run_screening_aggregates_results_and_handles_strategy_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())
        s1 = _runtime("s1")
        s2 = _runtime("s2")
        stock_a = StockUniverseItem(code="1001", company_name="A", scale_category=None, sector_33_name=None)
        stock_b = StockUniverseItem(code="1002", company_name="B", scale_category=None, sector_33_name=None)

        monkeypatch.setattr(service, "_load_stock_universe", lambda _codes: [stock_a, stock_b])
        monkeypatch.setattr(service, "_resolve_strategies", lambda _strategies: [s1, s2])
        monkeypatch.setattr(
            service,
            "_load_strategy_scores",
            lambda _strategies, _metric: (
                {"s1": 1.5, "s2": None},
                ["s2"],
                ["metrics warning"],
            ),
        )

        def _evaluate(strategy, _stock_universe, _recent_days, _reference_date, data_source="dataset"):
            assert data_source == "market"
            if strategy.response_name == "s1":
                return [(stock_a, "2026-01-10"), (stock_b, "2026-01-09")], {"1001", "1002"}, ["strategy warning"]
            raise RuntimeError("strategy boom")

        monkeypatch.setattr(service, "_evaluate_strategy", _evaluate)

        response = service.run_screening(
            markets="prime,standard",
            data_source="market",
            limit=1,
        )

        assert response.summary.matchCount == 2
        assert len(response.results) == 1
        assert response.summary.byStrategy == {"s1": 2, "s2": 0}
        assert "dataSource=market" in response.summary.warnings
        assert "metrics warning" in response.summary.warnings
        assert any("evaluation failed" in warning for warning in response.summary.warnings)

    def test_run_screening_updates_matched_date_when_same_stock_matches_multiple_strategies(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())
        s1 = _runtime("s1")
        s2 = _runtime("s2")
        stock = StockUniverseItem(code="1001", company_name="A", scale_category=None, sector_33_name=None)

        monkeypatch.setattr(service, "_load_stock_universe", lambda _codes: [stock])
        monkeypatch.setattr(service, "_resolve_strategies", lambda _strategies: [s1, s2])
        monkeypatch.setattr(
            service,
            "_load_strategy_scores",
            lambda _strategies, _metric: ({"s1": 2.0, "s2": 1.0}, [], []),
        )
        monkeypatch.setattr(
            service,
            "_evaluate_strategy",
            lambda strategy, _stock_universe, _recent_days, _reference_date, data_source="dataset": (
                [(stock, "2026-01-05")],
                {"1001"},
                [],
            ) if strategy.response_name == "s1" else ([(stock, "2026-01-10")], {"1001"}, []),
        )

        response = service.run_screening(data_source="dataset")

        assert response.summary.matchCount == 1
        assert response.results[0].stockCode == "1001"
        assert response.results[0].matchedDate == "2026-01-10"
        assert response.results[0].matchStrategyCount == 2
        assert response.results[0].bestStrategyName == "s1"

    def test_resolve_date_range_prefers_reference_date_when_earlier(self):
        service = ScreeningService(DummyReader())
        shared = SharedConfig.model_validate(
            {
                "dataset": "primeExTopix500",
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
            },
            context={"resolve_stock_codes": False},
        )

        start, end = service._resolve_date_range(shared, "2024-06-30")  # noqa: SLF001
        assert start == "2024-01-01"
        assert end == "2024-06-30"

        start, end = service._resolve_date_range(shared, "2025-01-01")  # noqa: SLF001
        assert start == "2024-01-01"
        assert end == "2024-12-31"

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
        monkeypatch.setattr("src.server.services.screening_service.SIGNAL_REGISTRY", registry)

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

        entry.fundamental.enabled = False
        entry.fundamental.peg_ratio.enabled = False
        assert not service._should_include_forecast_revision(entry, exit_)  # noqa: SLF001

    @pytest.mark.parametrize("period_type", ["all", "1Q", "2Q", "3Q", "FY"])
    def test_resolve_period_type_variants(self, period_type: str):
        service = ScreeningService(DummyReader())
        entry = SignalParams()
        exit_ = SignalParams()
        entry.fundamental.period_type = period_type  # type: ignore[assignment]
        assert service._resolve_period_type(entry, exit_) == period_type  # noqa: SLF001

    def test_load_multi_data_market_collects_benchmark_and_sector_warnings(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())
        index = pd.to_datetime(["2026-01-01"])
        daily = pd.DataFrame(
            {
                "Close": [1.0],
                "Open": [1.0],
                "High": [1.1],
                "Low": [0.9],
                "Volume": [100],
            },
            index=index,
        )

        monkeypatch.setattr(
            "src.server.services.screening_service.load_market_multi_data",
            lambda *_args, **_kwargs: ({"1001": {"daily": daily}}, ["base warning"]),
        )
        monkeypatch.setattr(
            "src.server.services.screening_service.load_market_topix_data",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("topix unavailable")),
        )
        monkeypatch.setattr(
            "src.server.services.screening_service.load_market_sector_indices",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("sector unavailable")),
        )

        _multi_data, benchmark_data, sector_data, _mapping, warnings = service._load_multi_data_market(  # noqa: SLF001
            stock_codes=["1001"],
            start_date="2026-01-01",
            end_date="2026-01-31",
            include_statements=False,
            period_type="FY",
            include_forecast_revision=False,
            needs_benchmark=True,
            needs_sector=True,
        )

        assert benchmark_data is None
        assert sector_data is None
        assert "base warning" in warnings
        assert any("benchmark load failed" in warning for warning in warnings)
        assert any("sector data load failed" in warning for warning in warnings)

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

    def test_evaluate_strategy_market_mode_uses_market_loader(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())
        strategy = _runtime("forward_eps_driven", shared_overrides={"include_statements_data": True})
        universe = [StockUniverseItem(code="1001", company_name="A", scale_category=None, sector_33_name=None)]

        index = pd.to_datetime(["2026-01-01", "2026-01-02"])
        daily = pd.DataFrame({"Close": [1.0, 1.1], "Open": [1.0, 1.0], "High": [1.1, 1.2], "Low": [0.9, 1.0], "Volume": [100, 200]}, index=index)

        monkeypatch.setattr(
            "src.server.services.screening_service.load_market_multi_data",
            lambda *_args, **_kwargs: (
                {"1001": {"daily": daily, "statements_daily": "stmt"}},
                ["market loader warning"],
            ),
        )
        monkeypatch.setattr(
            "src.server.services.screening_service.load_market_topix_data",
            lambda *_args, **_kwargs: pd.DataFrame(),
        )
        monkeypatch.setattr(
            "src.server.services.screening_service.load_market_sector_indices",
            lambda *_args, **_kwargs: {},
        )
        monkeypatch.setattr(
            "src.server.services.screening_service.load_market_stock_sector_mapping",
            lambda *_args, **_kwargs: {},
        )
        monkeypatch.setattr(service, "_needs_data_requirement", lambda *_args, **_kwargs: False)
        monkeypatch.setattr(
            service._signal_processor,
            "generate_signals",
            lambda **_kwargs: Signals(
                entries=pd.Series([True, True], index=index),
                exits=pd.Series([False, False], index=index),
            ),
        )

        matches, processed, warnings = service._evaluate_strategy(  # noqa: SLF001
            strategy=strategy,
            stock_universe=universe,
            recent_days=2,
            reference_date=None,
            data_source="market",
        )

        assert len(matches) == 1
        assert processed == {"1001"}
        assert any("market loader warning" in warning for warning in warnings)

    def test_evaluate_strategy_market_mode_fallbacks_to_dataset(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())
        strategy = _runtime("range_break_v15")
        universe = [StockUniverseItem(code="1001", company_name="A", scale_category=None, sector_33_name=None)]

        index = pd.to_datetime(["2026-01-01", "2026-01-02"])
        daily = pd.DataFrame({"Close": [1.0, 1.1], "Open": [1.0, 1.0], "High": [1.1, 1.2], "Low": [0.9, 1.0], "Volume": [100, 200]}, index=index)

        monkeypatch.setattr(
            "src.server.services.screening_service.load_market_multi_data",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("market down")),
        )
        monkeypatch.setattr("src.server.services.screening_service.data_access_mode_context", lambda _mode: nullcontext())
        monkeypatch.setattr(
            "src.server.services.screening_service.prepare_multi_data",
            lambda **_kwargs: {"1001": {"daily": daily}},
        )
        monkeypatch.setattr(service, "_needs_data_requirement", lambda *_args, **_kwargs: False)
        monkeypatch.setattr(
            service._signal_processor,
            "generate_signals",
            lambda **_kwargs: Signals(
                entries=pd.Series([True, True], index=index),
                exits=pd.Series([False, False], index=index),
            ),
        )

        matches, _processed, warnings = service._evaluate_strategy(  # noqa: SLF001
            strategy=strategy,
            stock_universe=universe,
            recent_days=2,
            reference_date=None,
            data_source="market",
        )

        assert len(matches) == 1
        assert any("fallback=dataset" in warning for warning in warnings)

    def test_evaluate_strategy_market_mode_fallbacks_to_dataset_when_margin_required(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        service = ScreeningService(DummyReader())
        strategy = _runtime(
            "margin_based",
            shared_overrides={"include_margin_data": True},
        )
        universe = [StockUniverseItem(code="1001", company_name="A", scale_category=None, sector_33_name=None)]

        index = pd.to_datetime(["2026-01-01", "2026-01-02"])
        daily = pd.DataFrame({
            "Close": [1.0, 1.1],
            "Open": [1.0, 1.0],
            "High": [1.1, 1.2],
            "Low": [0.9, 1.0],
            "Volume": [100, 200],
        }, index=index)
        margin_daily = pd.DataFrame({"margin_balance": [10.0, 11.0]}, index=index)

        monkeypatch.setattr("src.server.services.screening_service.data_access_mode_context", lambda _mode: nullcontext())
        monkeypatch.setattr(
            "src.server.services.screening_service.prepare_multi_data",
            lambda **_kwargs: {"1001": {"daily": daily, "margin_daily": margin_daily}},
        )

        def _market_loader_should_not_be_called(*_args, **_kwargs):
            raise AssertionError("market loader should not be used when margin is required")

        monkeypatch.setattr(
            "src.server.services.screening_service.load_market_multi_data",
            _market_loader_should_not_be_called,
        )
        monkeypatch.setattr(
            service,
            "_needs_data_requirement",
            lambda _entry, _exit, requirement: requirement == "margin",
        )

        captured: dict[str, object] = {}

        def _generate_signals(**kwargs):
            captured["margin_data"] = kwargs.get("margin_data")
            return Signals(
                entries=pd.Series([True, True], index=index),
                exits=pd.Series([False, False], index=index),
            )

        monkeypatch.setattr(service._signal_processor, "generate_signals", _generate_signals)

        matches, processed, warnings = service._evaluate_strategy(  # noqa: SLF001
            strategy=strategy,
            stock_universe=universe,
            recent_days=2,
            reference_date=None,
            data_source="market",
        )

        assert len(matches) == 1
        assert processed == {"1001"}
        assert any("fallback=dataset" in warning for warning in warnings)
        assert captured["margin_data"] is margin_daily
