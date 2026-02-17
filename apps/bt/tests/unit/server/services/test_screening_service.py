"""
Screening Service Unit Tests
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from src.lib.market_db.market_reader import MarketDbReader
from src.models.config import SharedConfig
from src.models.signals import SignalParams
from src.paths.resolver import StrategyMetadata
from src.server.services.screening_service import ScreeningService, StrategyRuntime


@pytest.fixture
def screening_db(tmp_path):
    db_path = str(tmp_path / "screening.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    conn.execute(
        """
        CREATE TABLE stocks (
            code TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            market_code TEXT NOT NULL,
            scale_category TEXT,
            sector_33_name TEXT
        )
        """
    )

    stocks = [
        ("10010", "Numeric Prime", "0111"),
        ("10020", "Legacy Prime", "prime"),
        ("10030", "Numeric Standard", "0112"),
        ("10040", "Legacy Standard", "standard"),
    ]
    for code, company_name, market_code in stocks:
        conn.execute(
            """
            INSERT INTO stocks (code, company_name, market_code, scale_category, sector_33_name)
            VALUES (?, ?, ?, ?, ?)
            """,
            (code, company_name, market_code, "TOPIX Small 1", "情報・通信業"),
        )

    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def service(screening_db):
    reader = MarketDbReader(screening_db)
    yield ScreeningService(reader)
    reader.close()


def _runtime(name: str) -> StrategyRuntime:
    return StrategyRuntime(
        name=f"production/{name}",
        response_name=name,
        basename=name,
        entry_params=SignalParams(),
        exit_params=SignalParams(),
        shared_config=SharedConfig.model_validate(
            {"dataset": "primeExTopix500"},
            context={"resolve_stock_codes": False},
        ),
    )


class TestMarketCodeCompatibility:
    def test_prime_and_numeric_prime_screen_same_universe(self, service, monkeypatch):
        runtime = _runtime("range_break_v15")

        monkeypatch.setattr(service, "_resolve_strategies", lambda _: [runtime])
        monkeypatch.setattr(
            service,
            "_load_strategy_scores",
            lambda strategies, metric: ({runtime.response_name: 1.0}, [], []),
        )
        monkeypatch.setattr(
            service,
            "_evaluate_strategy",
            lambda strategy, universe, recent_days, reference_date: (
                [],
                {s.code for s in universe},
                [],
            ),
        )

        prime_result = service.run_screening(markets="prime")
        numeric_result = service.run_screening(markets="0111")

        assert prime_result.summary.totalStocksScreened == 2
        assert numeric_result.summary.totalStocksScreened == 2


class TestStrategyResolution:
    def test_resolves_production_only_and_supports_basename_and_fullname(
        self,
        service,
        monkeypatch,
        tmp_path,
    ):
        production_1 = StrategyMetadata(
            name="production/range_break_v15",
            category="production",
            path=Path(tmp_path / "production/range_break_v15.yaml"),
            mtime=datetime.now(),
        )
        production_2 = StrategyMetadata(
            name="production/forward_eps_driven",
            category="production",
            path=Path(tmp_path / "production/forward_eps_driven.yaml"),
            mtime=datetime.now(),
        )
        experimental = StrategyMetadata(
            name="experimental/test_strategy",
            category="experimental",
            path=Path(tmp_path / "experimental/test_strategy.yaml"),
            mtime=datetime.now(),
        )

        monkeypatch.setattr(
            service._config_loader,
            "get_strategy_metadata",
            lambda: [production_1, production_2, experimental],
        )
        monkeypatch.setattr(
            service._config_loader,
            "load_strategy_config",
            lambda _name: {
                "entry_filter_params": {},
                "exit_trigger_params": {},
            },
        )
        monkeypatch.setattr(
            service._config_loader,
            "merge_shared_config",
            lambda _config: {"dataset": "primeExTopix500"},
        )

        auto_resolved = service._resolve_strategies(None)
        assert [s.name for s in auto_resolved] == [
            "production/forward_eps_driven",
            "production/range_break_v15",
        ]

        specified = service._resolve_strategies(
            "range_break_v15,production/forward_eps_driven"
        )
        assert [s.name for s in specified] == [
            "production/range_break_v15",
            "production/forward_eps_driven",
        ]

        with pytest.raises(ValueError, match="Invalid strategies"):
            service._resolve_strategies("experimental/test_strategy")

    def test_uses_full_name_when_selected_basenames_collide(
        self,
        service,
        monkeypatch,
        tmp_path,
    ):
        production_a = StrategyMetadata(
            name="production/group_a/shared_name",
            category="production",
            path=Path(tmp_path / "production/group_a/shared_name.yaml"),
            mtime=datetime.now(),
        )
        production_b = StrategyMetadata(
            name="production/group_b/shared_name",
            category="production",
            path=Path(tmp_path / "production/group_b/shared_name.yaml"),
            mtime=datetime.now(),
        )

        monkeypatch.setattr(
            service._config_loader,
            "get_strategy_metadata",
            lambda: [production_a, production_b],
        )
        monkeypatch.setattr(
            service._config_loader,
            "load_strategy_config",
            lambda _name: {
                "entry_filter_params": {},
                "exit_trigger_params": {},
            },
        )
        monkeypatch.setattr(
            service._config_loader,
            "merge_shared_config",
            lambda _config: {"dataset": "primeExTopix500"},
        )

        resolved = service._resolve_strategies(
            "production/group_a/shared_name,production/group_b/shared_name"
        )
        assert [s.response_name for s in resolved] == [
            "production/group_a/shared_name",
            "production/group_b/shared_name",
        ]


class TestAggregationAndSorting:
    def test_aggregates_by_stock_and_picks_best_strategy(self, service, monkeypatch):
        s1 = _runtime("range_break_v15")
        s2 = _runtime("forward_eps_driven")

        monkeypatch.setattr(service, "_resolve_strategies", lambda _: [s1, s2])
        monkeypatch.setattr(
            service,
            "_load_strategy_scores",
            lambda strategies, metric: (
                {s1.response_name: 0.8, s2.response_name: 1.2},
                [],
                [],
            ),
        )

        def _fake_evaluate(strategy, universe, _recent_days, _reference_date):
            by_code = {s.code: s for s in universe}
            if strategy.response_name == "range_break_v15":
                return [(by_code["1001"], "2026-01-05")], {s.code for s in universe}, []
            return [(by_code["1001"], "2026-01-06")], {s.code for s in universe}, []

        monkeypatch.setattr(service, "_evaluate_strategy", _fake_evaluate)

        result = service.run_screening(markets="prime")

        assert result.summary.matchCount == 1
        row = result.results[0]
        assert row.stockCode == "1001"
        assert row.matchStrategyCount == 2
        assert row.bestStrategyName == "forward_eps_driven"
        assert row.bestStrategyScore == pytest.approx(1.2)

    def test_missing_metrics_strategy_gets_null_score_and_sorted_last(
        self,
        service,
        monkeypatch,
    ):
        scored = _runtime("range_break_v15")
        missing = _runtime("forward_eps_driven")

        monkeypatch.setattr(service, "_resolve_strategies", lambda _: [scored, missing])
        monkeypatch.setattr(
            service,
            "_load_strategy_scores",
            lambda strategies, metric: (
                {scored.response_name: 1.0, missing.response_name: None},
                [missing.response_name],
                [],
            ),
        )

        def _fake_evaluate(strategy, universe, _recent_days, _reference_date):
            by_code = {s.code: s for s in universe}
            if strategy.response_name == "range_break_v15":
                return [(by_code["1001"], "2026-01-05")], {s.code for s in universe}, []
            return [(by_code["1002"], "2026-01-06")], {s.code for s in universe}, []

        monkeypatch.setattr(service, "_evaluate_strategy", _fake_evaluate)

        result_desc = service.run_screening(
            markets="prime",
            sort_by="bestStrategyScore",
            order="desc",
        )
        assert [r.stockCode for r in result_desc.results] == ["1001", "1002"]

        result_asc = service.run_screening(
            markets="prime",
            sort_by="bestStrategyScore",
            order="asc",
        )
        assert [r.stockCode for r in result_asc.results] == ["1001", "1002"]
        assert result_asc.summary.strategiesWithoutBacktestMetrics == ["forward_eps_driven"]

    def test_backtest_metric_switch_sort_order_and_limit(self, service, monkeypatch):
        s1 = _runtime("range_break_v15")
        s2 = _runtime("forward_eps_driven")

        captured_metric: dict[str, str] = {}

        monkeypatch.setattr(service, "_resolve_strategies", lambda _: [s1, s2])

        def _fake_scores(_strategies, metric):
            captured_metric["value"] = metric
            return {s1.response_name: 0.2, s2.response_name: 0.9}, [], []

        monkeypatch.setattr(service, "_load_strategy_scores", _fake_scores)

        def _fake_evaluate(strategy, universe, _recent_days, _reference_date):
            by_code = {s.code: s for s in universe}
            if strategy.response_name == "range_break_v15":
                return [(by_code["1001"], "2026-01-07")], {s.code for s in universe}, []
            return [(by_code["1002"], "2026-01-06")], {s.code for s in universe}, []

        monkeypatch.setattr(service, "_evaluate_strategy", _fake_evaluate)

        result = service.run_screening(
            markets="prime",
            backtest_metric="calmar_ratio",
            sort_by="matchedDate",
            order="asc",
            limit=1,
        )

        assert captured_metric["value"] == "calmar_ratio"
        assert len(result.results) == 1
        assert result.results[0].stockCode == "1002"
