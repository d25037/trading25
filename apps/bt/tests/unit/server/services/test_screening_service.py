"""
Screening Service Unit Tests
"""

from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import pytest

from src.lib.market_db.market_reader import MarketDbReader
from src.models.config import SharedConfig
from src.models.signals import SignalParams
from src.paths.resolver import StrategyMetadata
from src.server.services.screening_service import (
    RequestCacheStats,
    ScreeningService,
    StockUniverseItem,
    StrategyDataBundle,
    StrategyEvaluationResult,
    StrategyExecutionInput,
    StrategyRuntime,
)


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


def _runtime(name: str, *, shared_overrides: dict[str, object] | None = None) -> StrategyRuntime:
    shared_payload: dict[str, object] = {"dataset": "primeExTopix500"}
    if shared_overrides:
        shared_payload.update(shared_overrides)

    return StrategyRuntime(
        name=f"production/{name}",
        response_name=name,
        basename=name,
        entry_params=SignalParams(),
        exit_params=SignalParams(),
        shared_config=SharedConfig.model_validate(
            shared_payload,
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
            lambda strategies: ({runtime.response_name: 1.0}, [], []),
        )

        def _prepare(*, strategy_runtimes, stock_universe, reference_date):
            return (
                [
                    StrategyExecutionInput(
                        strategy=runtime,
                        data_bundle=StrategyDataBundle(multi_data={}),
                        load_warnings=[],
                    )
                ],
                RequestCacheStats(hits=0, misses=0),
            )

        monkeypatch.setattr(service, "_prepare_strategy_inputs", _prepare)

        def _evaluate(strategy_inputs, stock_universe, recent_days, progress_callback):
            return (
                [
                    StrategyEvaluationResult(
                        strategy=runtime,
                        matched_rows=[],
                        processed_codes={s.code for s in stock_universe},
                        warnings=[],
                    )
                ],
                [],
                1,
            )

        monkeypatch.setattr(service, "_evaluate_strategies", _evaluate)

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


class TestAggregationAndSorting:
    def test_aggregates_by_stock_and_picks_best_strategy(self, service, monkeypatch):
        s1 = _runtime("range_break_v15")
        s2 = _runtime("forward_eps_driven")

        monkeypatch.setattr(service, "_resolve_strategies", lambda _: [s1, s2])
        monkeypatch.setattr(
            service,
            "_load_strategy_scores",
            lambda strategies: (
                {s1.response_name: 0.8, s2.response_name: 1.2},
                [],
                [],
            ),
        )

        def _prepare(*, strategy_runtimes, stock_universe, reference_date):
            inputs = [
                StrategyExecutionInput(
                    strategy=s1,
                    data_bundle=StrategyDataBundle(multi_data={}),
                    load_warnings=[],
                ),
                StrategyExecutionInput(
                    strategy=s2,
                    data_bundle=StrategyDataBundle(multi_data={}),
                    load_warnings=[],
                ),
            ]
            return inputs, RequestCacheStats(hits=0, misses=0)

        monkeypatch.setattr(service, "_prepare_strategy_inputs", _prepare)

        def _evaluate(strategy_inputs, stock_universe, recent_days, progress_callback):
            by_code = {s.code: s for s in stock_universe}
            return (
                [
                    StrategyEvaluationResult(
                        strategy=s1,
                        matched_rows=[(by_code["1001"], "2026-01-05")],
                        processed_codes={s.code for s in stock_universe},
                        warnings=[],
                    ),
                    StrategyEvaluationResult(
                        strategy=s2,
                        matched_rows=[(by_code["1001"], "2026-01-06")],
                        processed_codes={s.code for s in stock_universe},
                        warnings=[],
                    ),
                ],
                [],
                2,
            )

        monkeypatch.setattr(service, "_evaluate_strategies", _evaluate)

        result = service.run_screening(markets="prime")

        assert result.summary.matchCount == 1
        row = result.results[0]
        assert row.stockCode == "1001"
        assert row.matchStrategyCount == 2
        assert row.bestStrategyName == "forward_eps_driven"
        assert row.bestStrategyScore == pytest.approx(1.2)

    def test_default_sort_is_matched_date_desc_and_limit(self, service, monkeypatch):
        s1 = _runtime("range_break_v15")
        s2 = _runtime("forward_eps_driven")

        monkeypatch.setattr(service, "_resolve_strategies", lambda _: [s1, s2])
        monkeypatch.setattr(
            service,
            "_load_strategy_scores",
            lambda strategies: (
                {s1.response_name: 0.2, s2.response_name: 0.9},
                [],
                [],
            ),
        )

        def _prepare(*, strategy_runtimes, stock_universe, reference_date):
            inputs = [
                StrategyExecutionInput(
                    strategy=s1,
                    data_bundle=StrategyDataBundle(multi_data={}),
                    load_warnings=[],
                ),
                StrategyExecutionInput(
                    strategy=s2,
                    data_bundle=StrategyDataBundle(multi_data={}),
                    load_warnings=[],
                ),
            ]
            return inputs, RequestCacheStats(hits=0, misses=0)

        monkeypatch.setattr(service, "_prepare_strategy_inputs", _prepare)

        def _evaluate(strategy_inputs, stock_universe, recent_days, progress_callback):
            by_code = {s.code: s for s in stock_universe}
            return (
                [
                    StrategyEvaluationResult(
                        strategy=s1,
                        matched_rows=[(by_code["1001"], "2026-01-07")],
                        processed_codes={s.code for s in stock_universe},
                        warnings=[],
                    ),
                    StrategyEvaluationResult(
                        strategy=s2,
                        matched_rows=[(by_code["1002"], "2026-01-06")],
                        processed_codes={s.code for s in stock_universe},
                        warnings=[],
                    ),
                ],
                [],
                2,
            )

        monkeypatch.setattr(service, "_evaluate_strategies", _evaluate)

        result = service.run_screening(markets="prime", limit=1)

        assert result.sortBy == "matchedDate"
        assert result.order == "desc"
        assert len(result.results) == 1
        assert result.results[0].stockCode == "1001"


class TestRequestMemoizationAndParallelization:
    def test_prepare_strategy_inputs_memoizes_same_requirements(self, service, monkeypatch):
        s1 = _runtime("s1")
        s2 = _runtime("s2")
        stock_universe = [
            StockUniverseItem(
                code="1001",
                company_name="A",
                scale_category=None,
                sector_33_name=None,
            )
        ]

        call_count = {"multi": 0}

        def _load_multi(_key):
            call_count["multi"] += 1
            return {"1001": {}}

        monkeypatch.setattr(service, "_load_multi_data", _load_multi)

        inputs, stats = service._prepare_strategy_inputs(  # noqa: SLF001
            strategy_runtimes=[s1, s2],
            stock_universe=stock_universe,
            reference_date=None,
        )

        assert len(inputs) == 2
        assert call_count["multi"] == 1
        assert stats.hits == 1
        assert stats.misses == 1

    def test_prepare_strategy_inputs_separates_different_requirements(self, service, monkeypatch):
        s1 = _runtime("s1", shared_overrides={"dataset": "primeExTopix500"})
        s2 = _runtime("s2", shared_overrides={"dataset": "topix100"})
        stock_universe = [
            StockUniverseItem(
                code="1001",
                company_name="A",
                scale_category=None,
                sector_33_name=None,
            )
        ]

        call_count = {"multi": 0}

        def _load_multi(_key):
            call_count["multi"] += 1
            return {"1001": {}}

        monkeypatch.setattr(service, "_load_multi_data", _load_multi)

        inputs, stats = service._prepare_strategy_inputs(  # noqa: SLF001
            strategy_runtimes=[s1, s2],
            stock_universe=stock_universe,
            reference_date=None,
        )

        assert len(inputs) == 2
        assert call_count["multi"] == 2
        assert stats.hits == 0
        assert stats.misses == 2

    def test_evaluate_strategies_parallel_returns_all_results(self, service, monkeypatch):
        s1 = _runtime("s1")
        s2 = _runtime("s2")
        s3 = _runtime("s3")

        strategy_inputs = [
            StrategyExecutionInput(strategy=s1, data_bundle=StrategyDataBundle(multi_data={}), load_warnings=[]),
            StrategyExecutionInput(strategy=s2, data_bundle=StrategyDataBundle(multi_data={}), load_warnings=[]),
            StrategyExecutionInput(strategy=s3, data_bundle=StrategyDataBundle(multi_data={}), load_warnings=[]),
        ]

        def _evaluate_input(strategy_input, _stock_universe, _recent_days):
            time.sleep(0.01)
            return StrategyEvaluationResult(
                strategy=strategy_input.strategy,
                matched_rows=[],
                processed_codes=set(),
                warnings=[],
            )

        monkeypatch.setattr(service, "_evaluate_strategy_input", _evaluate_input)
        monkeypatch.setenv("BT_SCREENING_MAX_STRATEGY_WORKERS", "3")

        progresses: list[tuple[int, int]] = []
        results, warnings, worker_count = service._evaluate_strategies(  # noqa: SLF001
            strategy_inputs=strategy_inputs,
            stock_universe=[],
            recent_days=10,
            progress_callback=lambda completed, total: progresses.append((completed, total)),
        )
        expected_workers = min(3, os.cpu_count() or 1)

        assert len(results) == 3
        assert warnings == []
        assert worker_count == expected_workers
        assert progresses[0] == (0, 3)
        assert progresses[-1] == (3, 3)
