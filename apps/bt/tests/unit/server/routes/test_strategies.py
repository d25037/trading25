"""server/routes/strategies.py のテスト"""

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from ruamel.yaml.comments import CommentedMap

import src.entrypoints.http.routes.strategies as strategies_mod
from src.entrypoints.http.routes.strategies import router


@pytest.fixture(autouse=True)
def mock_config_loader():
    """ConfigLoaderのmock（module属性差し替え）"""
    original = strategies_mod._config_loader
    mock_loader = MagicMock()
    strategies_mod._config_loader = mock_loader
    yield mock_loader
    strategies_mod._config_loader = original


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


class TestGetStrategyDetail:
    def test_success(self, client, mock_config_loader):
        mock_config_loader.load_strategy_config.return_value = {
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
            "shared_config": {"universe_preset": "primeExTopix500"},
        }
        with patch("src.domains.backtest.core.runner.BacktestRunner") as mock_runner_cls:
            mock_runner = MagicMock()
            mock_runner.get_execution_info.return_value = {"status": "ok"}
            mock_runner_cls.return_value = mock_runner
            resp = client.get("/api/strategies/test_strategy")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "test_strategy"

    def test_not_found(self, client, mock_config_loader):
        mock_config_loader.load_strategy_config.side_effect = FileNotFoundError("not found")
        resp = client.get("/api/strategies/missing")
        assert resp.status_code == 404


class TestStrategyEditorReference:
    def test_success(self, client):
        resp = client.get("/api/strategies/editor/reference")

        assert resp.status_code == 200
        data = resp.json()
        assert data["capabilities"]["visual_editor"] is True
        assert any(field["path"] == "data_source" for field in data["shared_config_fields"])
        assert any(field["path"] == "universe_preset" for field in data["shared_config_fields"])
        assert not any(field["path"] == "dataset" for field in data["shared_config_fields"])
        assert any(group["key"] == "data" for group in data["shared_config_groups"])

    def test_error_returns_500(self, client):
        with patch.object(
            strategies_mod,
            "build_strategy_editor_reference",
            side_effect=RuntimeError("boom"),
        ):
            resp = client.get("/api/strategies/editor/reference")

        assert resp.status_code == 500
        assert "boom" in resp.json()["detail"]


class TestStrategyEditorContext:
    def test_success(self, client, mock_config_loader):
        mock_config_loader.load_strategy_config.return_value = {
            "shared_config": {"universe_preset": "primeExTopix500"},
            "execution": {"output_directory": "/tmp/custom"},
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
            "extra_block": {"keep": True},
        }
        mock_config_loader.resolve_strategy_category.return_value = "experimental"
        mock_config_loader.default_config = {
            "execution": {"output_directory": None, "create_output_dir": True},
            "parameters": {
                "shared_config": {
                    "universe_preset": "standard",
                    "benchmark_table": "topix",
                    "execution_policy": {"mode": "standard"},
                    "stock_codes": ["all"],
                }
            },
        }
        mock_config_loader.merge_shared_config.return_value = {
            "universe_preset": "primeExTopix500",
            "benchmark_table": "topix",
            "execution_policy": {"mode": "standard"},
            "stock_codes": ["all"],
        }
        mock_config_loader.get_execution_config.return_value = {
            "output_directory": "/tmp/custom",
            "create_output_dir": True,
        }

        resp = client.get("/api/strategies/experimental/sample/editor-context")

        assert resp.status_code == 200
        data = resp.json()
        assert data["strategy_name"] == "experimental/sample"
        assert data["default_shared_config"]["universe_preset"] == "standard"
        assert data["effective_shared_config"]["universe_preset"] == "primeExTopix500"
        assert data["unknown_top_level_keys"] == ["extra_block"]
        universe_provenance = next(
            item for item in data["shared_config_provenance"] if item["path"] == "universe_preset"
        )
        assert universe_provenance == {
            "path": "universe_preset",
            "source": "strategy",
            "overridden": True,
        }

    def test_not_found_returns_404(self, client, mock_config_loader):
        mock_config_loader.load_strategy_config.side_effect = FileNotFoundError("missing")

        resp = client.get("/api/strategies/experimental/missing/editor-context")

        assert resp.status_code == 404


class TestDefaultStructuredHelpers:
    def test_ensure_commented_map_reuses_existing_and_creates_missing(self):
        parent = CommentedMap({"existing": CommentedMap({"value": 1})})

        existing = strategies_mod._ensure_commented_map(parent, "existing")
        created = strategies_mod._ensure_commented_map(parent, "created")

        assert existing is parent["existing"]
        assert isinstance(created, CommentedMap)
        assert parent["created"] == created

    def test_patch_mapping_updates_nested_and_prunes_removed_keys(self):
        target = CommentedMap(
            {
                "output_directory": "/tmp/out",
                "create_output_dir": True,
                "nested": CommentedMap({"keep": 1, "drop": 2}),
            }
        )

        strategies_mod._patch_mapping(
            target,
            {
                "output_directory": "/tmp/new",
                "nested": {"keep": 3, "added": 4},
            },
        )

        assert "create_output_dir" not in target
        assert target["output_directory"] == "/tmp/new"
        assert target["nested"] == {"keep": 3, "added": 4}

    def test_validate_default_structured_request_rejects_invalid_shared_config(self):
        request = strategies_mod.DefaultConfigStructuredUpdateRequest(
            execution={"output_directory": "/tmp/out"},
            shared_config={"initial_cash": "not-a-number"},
        )

        with pytest.raises(strategies_mod.HTTPException) as exc_info:
            strategies_mod._validate_default_structured_request(request)

        assert exc_info.value.status_code == 400


class TestValidateStrategy:
    def test_valid_config(self, client, mock_config_loader):
        mock_config_loader.load_strategy_config.return_value = {
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
        }
        mock_config_loader.merge_shared_config.return_value = {
            "universe_preset": "primeExTopix500",
            "direction": "longonly",
            "timeframe": "daily",
        }
        with patch("src.domains.backtest.core.runner.BacktestRunner") as mock_runner_cls:
            mock_runner = MagicMock()
            mock_runner.get_execution_info.return_value = {}
            mock_runner_cls.return_value = mock_runner
            resp = client.post("/api/strategies/test/validate")
        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is True
        assert data["compiled_strategy"]["signal_ids"] == ["entry.volume_ratio_above"]
        assert data["compiled_strategy"]["required_data_domains"] == ["market"]

    def test_missing_params_warning(self, client, mock_config_loader):
        mock_config_loader.load_strategy_config.return_value = {}
        mock_config_loader.merge_shared_config.return_value = {
            "universe_preset": "primeExTopix500",
            "direction": "longonly",
            "timeframe": "daily",
        }
        with patch("src.domains.backtest.core.runner.BacktestRunner") as mock_runner_cls:
            mock_runner = MagicMock()
            mock_runner.get_execution_info.return_value = {}
            mock_runner_cls.return_value = mock_runner
            resp = client.post("/api/strategies/test/validate")
        data = resp.json()
        assert len(data["warnings"]) > 0

    def test_invalid_kelly_fraction(self, client, mock_config_loader):
        mock_config_loader.load_strategy_config.return_value = {
            "shared_config": {"kelly_fraction": 5.0},
        }
        mock_config_loader.merge_shared_config.return_value = {
            "universe_preset": "primeExTopix500",
            "direction": "longonly",
            "timeframe": "daily",
            "kelly_fraction": 5.0,
        }
        with patch("src.domains.backtest.core.runner.BacktestRunner") as mock_runner_cls:
            mock_runner = MagicMock()
            mock_runner.get_execution_info.return_value = {}
            mock_runner_cls.return_value = mock_runner
            resp = client.post("/api/strategies/test/validate")
        data = resp.json()
        assert data["valid"] is False
        assert any("kelly_fraction" in e for e in data["errors"])

    def test_strict_nested_typo(self, client, mock_config_loader):
        mock_config_loader.merge_shared_config.return_value = {
            "universe_preset": "primeExTopix500",
            "direction": "longonly",
            "timeframe": "daily",
        }
        with patch("src.domains.backtest.core.runner.BacktestRunner") as mock_runner_cls:
            mock_runner = MagicMock()
            mock_runner.get_execution_info.return_value = {}
            mock_runner_cls.return_value = mock_runner
            resp = client.post(
                "/api/strategies/test/validate",
                json={
                    "config": {
                        "entry_filter_params": {
                            "fundamental": {
                                "foward_eps_growth": {
                                    "enabled": True,
                                    "threshold": 0.2,
                                    "condition": "above",
                                }
                            }
                        }
                    }
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is False
        assert any(
            "entry_filter_params.fundamental.foward_eps_growth" in e
            for e in data["errors"]
        )

    def test_next_session_round_trip_rejects_exit_triggers(self, client, mock_config_loader):
        mock_config_loader.merge_shared_config.return_value = {
            "universe_preset": "primeExTopix500",
            "direction": "longonly",
            "timeframe": "daily",
            "execution_policy": {"mode": "next_session_round_trip"},
        }
        with patch("src.domains.backtest.core.runner.BacktestRunner") as mock_runner_cls:
            mock_runner = MagicMock()
            mock_runner.get_execution_info.return_value = {}
            mock_runner_cls.return_value = mock_runner
            resp = client.post(
                "/api/strategies/test/validate",
                json={
                    "config": {
                        "shared_config": {
                            "execution_policy": {"mode": "next_session_round_trip"}
                        },
                        "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
                        "exit_trigger_params": {"rsi_threshold": {"enabled": True}},
                    }
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is False
        assert any("exit_trigger_params" in e for e in data["errors"])

    def test_current_session_round_trip_compiles_allowlisted_signal(self, client, mock_config_loader):
        mock_config_loader.merge_shared_config.return_value = {
            "universe_preset": "primeExTopix500",
            "direction": "longonly",
            "timeframe": "daily",
            "execution_policy": {"mode": "current_session_round_trip"},
        }
        with patch("src.domains.backtest.core.runner.BacktestRunner") as mock_runner_cls:
            mock_runner = MagicMock()
            mock_runner.get_execution_info.return_value = {}
            mock_runner_cls.return_value = mock_runner
            resp = client.post(
                "/api/strategies/test/validate",
                json={
                    "config": {
                        "shared_config": {
                            "execution_policy": {"mode": "current_session_round_trip"}
                        },
                        "entry_filter_params": {
                            "index_open_gap_regime": {"enabled": True},
                            "volume_ratio_above": {"enabled": True},
                        },
                    }
                },
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is True
        by_signal = {
            item["signal_id"]: item
            for item in data["compiled_strategy"]["signals"]
        }
        assert (
            by_signal["entry.index_open_gap_regime"]["availability"]["observation_time"]
            == "current_session_open"
        )
        assert (
            by_signal["entry.volume_ratio_above"]["availability"]["observation_time"]
            == "prior_session_close"
        )

    def test_validate_request_config_does_not_depend_on_saved_strategy_execution_info(
        self,
        client,
        mock_config_loader,
    ):
        mock_config_loader.merge_shared_config.return_value = {
            "universe_preset": "primeExTopix500",
            "direction": "longonly",
            "timeframe": "daily",
        }

        with patch("src.domains.backtest.core.runner.BacktestRunner") as mock_runner_cls:
            mock_runner = MagicMock()
            mock_runner.get_execution_info.return_value = {
                "error": "saved strategy missing",
            }
            mock_runner_cls.return_value = mock_runner

            resp = client.post(
                "/api/strategies/experimental/draft/validate",
                json={
                    "config": {
                        "shared_config": {"universe_preset": "primeExTopix500"},
                        "entry_filter_params": {
                            "volume_ratio_above": {"enabled": True},
                        },
                    }
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is True
        assert data["errors"] == []
        mock_runner.get_execution_info.assert_not_called()

    def test_production_requires_explicit_universe_preset_in_raw_yaml(self, client, mock_config_loader):
        mock_config_loader.resolve_strategy_category.return_value = "production"
        mock_config_loader.merge_shared_config.return_value = {
            "universe_preset": "primeExTopix500",
            "direction": "longonly",
            "timeframe": "daily",
        }

        resp = client.post(
            "/api/strategies/production/range_break_v16/validate",
            json={
                "config": {
                    "entry_filter_params": {
                        "volume_ratio_above": {"enabled": True},
                    }
                }
            },
        )

        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is False
        assert any("shared_config.universe_preset explicitly" in error for error in data["errors"])


class TestUpdateStrategy:
    def test_success(self, client, mock_config_loader):
        mock_config_loader.is_updatable_category.return_value = True
        mock_config_loader.save_strategy_config.return_value = Path("/saved/path.yaml")
        resp = client.put(
            "/api/strategies/test",
            json={"config": {"entry_filter_params": {}}},
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        mock_config_loader.save_strategy_config.assert_called_once_with(
            "test",
            {"entry_filter_params": {}},
            force=True,
            allow_production=True,
        )

    def test_production_success(self, client, mock_config_loader):
        mock_config_loader.is_updatable_category.return_value = True
        mock_config_loader.load_strategy_config.return_value = {
            "entry_filter_params": {}
        }
        mock_config_loader.save_strategy_config.return_value = Path(
            "/saved/production/path.yaml"
        )
        resp = client.put(
            "/api/strategies/production/range_break_v16",
            json={"config": {"entry_filter_params": {}}},
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        mock_config_loader.save_strategy_config.assert_called_once_with(
            "production/range_break_v16",
            {"entry_filter_params": {}},
            force=True,
            allow_production=True,
        )

    def test_production_not_found_404(self, client, mock_config_loader):
        mock_config_loader.is_updatable_category.return_value = True
        mock_config_loader.load_strategy_config.side_effect = FileNotFoundError(
            "not found"
        )
        resp = client.put(
            "/api/strategies/production/missing_strategy",
            json={"config": {"entry_filter_params": {}}},
        )
        assert resp.status_code == 404

    def test_non_editable_403(self, client, mock_config_loader):
        mock_config_loader.is_updatable_category.return_value = False
        resp = client.put(
            "/api/strategies/production_test",
            json={"config": {}},
        )
        assert resp.status_code == 403


class TestDeleteStrategy:
    def test_success(self, client, mock_config_loader):
        mock_config_loader.is_editable_category.return_value = True
        resp = client.delete("/api/strategies/test")
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_non_editable_403(self, client, mock_config_loader):
        mock_config_loader.is_editable_category.return_value = False
        resp = client.delete("/api/strategies/production_test")
        assert resp.status_code == 403

    def test_not_found_404(self, client, mock_config_loader):
        mock_config_loader.is_editable_category.return_value = True
        mock_config_loader.delete_strategy.side_effect = FileNotFoundError("not found")
        resp = client.delete("/api/strategies/missing")
        assert resp.status_code == 404


class TestDuplicateStrategy:
    def test_success(self, client, mock_config_loader):
        mock_config_loader.duplicate_strategy.return_value = Path("/dup/path.yaml")
        resp = client.post(
            "/api/strategies/test/duplicate",
            json={"new_name": "test_copy"},
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_not_found_404(self, client, mock_config_loader):
        mock_config_loader.duplicate_strategy.side_effect = FileNotFoundError("not found")
        resp = client.post(
            "/api/strategies/missing/duplicate",
            json={"new_name": "copy"},
        )
        assert resp.status_code == 404

    def test_conflict_409(self, client, mock_config_loader):
        mock_config_loader.duplicate_strategy.side_effect = FileExistsError("exists")
        resp = client.post(
            "/api/strategies/test/duplicate",
            json={"new_name": "existing"},
        )
        assert resp.status_code == 409


class TestRenameStrategy:
    def test_success(self, client, mock_config_loader):
        mock_config_loader.is_editable_category.return_value = True
        mock_config_loader.rename_strategy.return_value = Path("/new/path.yaml")
        resp = client.post(
            "/api/strategies/test/rename",
            json={"new_name": "test_renamed"},
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_not_found_404(self, client, mock_config_loader):
        mock_config_loader.is_editable_category.return_value = True
        mock_config_loader.rename_strategy.side_effect = FileNotFoundError("not found")
        resp = client.post(
            "/api/strategies/missing/rename",
            json={"new_name": "new_name"},
        )
        assert resp.status_code == 404

    def test_conflict_409(self, client, mock_config_loader):
        mock_config_loader.is_editable_category.return_value = True
        mock_config_loader.rename_strategy.side_effect = FileExistsError("exists")
        resp = client.post(
            "/api/strategies/test/rename",
            json={"new_name": "existing"},
        )
        assert resp.status_code == 409

    def test_non_editable_403(self, client, mock_config_loader):
        mock_config_loader.is_editable_category.return_value = False
        resp = client.post(
            "/api/strategies/production_test/rename",
            json={"new_name": "new"},
        )
        assert resp.status_code == 403


class TestMoveStrategy:
    def test_success(self, client, mock_config_loader):
        mock_config_loader.move_strategy.return_value = (
            "production/test",
            Path("/new/path.yaml"),
        )
        resp = client.post(
            "/api/strategies/experimental/test/move",
            json={"target_category": "production"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["old_strategy_name"] == "experimental/test"
        assert data["new_strategy_name"] == "production/test"

    def test_conflict_409(self, client, mock_config_loader):
        mock_config_loader.move_strategy.side_effect = FileExistsError("exists")
        resp = client.post(
            "/api/strategies/experimental/test/move",
            json={"target_category": "production"},
        )
        assert resp.status_code == 409

    def test_not_found_404(self, client, mock_config_loader):
        mock_config_loader.move_strategy.side_effect = FileNotFoundError("not found")
        resp = client.post(
            "/api/strategies/experimental/missing/move",
            json={"target_category": "production"},
        )
        assert resp.status_code == 404

    def test_bad_request_400(self, client, mock_config_loader):
        mock_config_loader.move_strategy.side_effect = ValueError("invalid request")
        resp = client.post(
            "/api/strategies/experimental/test/move",
            json={"target_category": "production"},
        )
        assert resp.status_code == 400

    def test_internal_error_500(self, client, mock_config_loader):
        mock_config_loader.move_strategy.side_effect = RuntimeError("unexpected")
        resp = client.post(
            "/api/strategies/experimental/test/move",
            json={"target_category": "production"},
        )
        assert resp.status_code == 500

    def test_invalid_category_422(self, client, mock_config_loader):  # noqa: ARG002
        resp = client.post(
            "/api/strategies/experimental/test/move",
            json={"target_category": "reference"},
        )
        assert resp.status_code == 422


class TestListStrategies:
    def test_success(self, client, mock_config_loader):
        mock_config_loader.get_strategy_metadata.return_value = [
            SimpleNamespace(
                name="production/range_break_v16",
                category="production",
                mtime=datetime(2026, 2, 17),
            ),
            SimpleNamespace(
                name="experimental/demo",
                category="experimental",
                mtime=datetime(2026, 2, 16),
            ),
        ]
        mock_config_loader.load_strategy_config.side_effect = [
            {"entry_filter_params": {}, "exit_trigger_params": {}},
            {
                "shared_config": {
                    "execution_policy": {"mode": "current_session_round_trip"}
                },
                "entry_filter_params": {
                    "index_open_gap_regime": {"enabled": True},
                },
                "exit_trigger_params": {},
            },
        ]
        mock_config_loader.merge_shared_config.side_effect = [
            {"universe_preset": "primeExTopix500"},
            {"execution_policy": {"mode": "current_session_round_trip"}},
        ]
        with patch.object(
            strategies_mod,
            "resolve_strategy_dataset_metadata",
            side_effect=[
                strategies_mod.StrategyDatasetMetadata(
                    dataset_name="primeExTopix500_20260316",
                    dataset_preset="primeExTopix500",
                    screening_default_markets=["prime", "standard"],
                ),
                strategies_mod.StrategyDatasetMetadata(
                    dataset_name=None,
                    dataset_preset=None,
                    screening_default_markets=None,
                ),
            ],
        ):
            resp = client.get("/api/strategies")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert data["strategies"][0]["name"] == "production/range_break_v16"
        assert data["strategies"][0]["screening_support"] == "supported"
        assert data["strategies"][0]["entry_decidability"] == "pre_open_decidable"
        assert data["strategies"][0]["screening_error"] is None
        assert data["strategies"][0]["dataset_name"] == "primeExTopix500_20260316"
        assert data["strategies"][0]["dataset_preset"] == "primeExTopix500"
        assert data["strategies"][0]["screening_default_markets"] == ["prime", "standard"]
        assert data["strategies"][1]["screening_support"] == "supported"
        assert data["strategies"][1]["entry_decidability"] == "requires_same_session_observation"
        assert data["strategies"][1]["screening_error"] is None

    def test_exit_only_same_day_filter_stays_standard(self, client, mock_config_loader):
        mock_config_loader.get_strategy_metadata.return_value = [
            SimpleNamespace(
                name="production/exit_only_same_day",
                category="production",
                mtime=datetime(2026, 2, 17),
            ),
        ]
        mock_config_loader.load_strategy_config.return_value = {
            "entry_filter_params": {"volume_ratio_above": {"enabled": True}},
            "exit_trigger_params": {
                "index_open_gap_regime": {"enabled": True},
            },
        }
        mock_config_loader.merge_shared_config.return_value = {"universe_preset": "primeExTopix500"}
        with patch.object(
            strategies_mod,
            "resolve_strategy_dataset_metadata",
            return_value=strategies_mod.StrategyDatasetMetadata(
                dataset_name="primeExTopix500_20260316",
                dataset_preset="primeExTopix500",
                screening_default_markets=["prime"],
            ),
        ):
            resp = client.get("/api/strategies")

        assert resp.status_code == 200
        data = resp.json()
        assert data["strategies"][0]["screening_support"] == "supported"
        assert data["strategies"][0]["entry_decidability"] == "pre_open_decidable"
        assert data["strategies"][0]["screening_error"] is None

    def test_broken_strategy_returns_screening_error(self, client, mock_config_loader):
        mock_config_loader.get_strategy_metadata.return_value = [
            SimpleNamespace(
                name="production/broken",
                category="production",
                mtime=datetime(2026, 2, 17),
            ),
        ]
        mock_config_loader.load_strategy_config.side_effect = RuntimeError("bad yaml")

        resp = client.get("/api/strategies")

        assert resp.status_code == 200
        data = resp.json()
        assert data["strategies"][0]["screening_support"] == "unsupported"
        assert data["strategies"][0]["entry_decidability"] is None
        assert data["strategies"][0]["screening_error"] == "bad yaml"

    def test_dataset_metadata_failure_does_not_drop_screening_support(
        self,
        client,
        mock_config_loader,
    ):
        mock_config_loader.get_strategy_metadata.return_value = [
            SimpleNamespace(
                name="production/range_break_v16",
                category="production",
                mtime=datetime(2026, 2, 17),
            ),
        ]
        mock_config_loader.load_strategy_config.return_value = {
            "shared_config": {"universe_preset": "primeExTopix500"},
            "entry_filter_params": {},
            "exit_trigger_params": {},
        }
        mock_config_loader.merge_shared_config.return_value = {"universe_preset": "primeExTopix500"}

        with patch.object(
            strategies_mod,
            "resolve_strategy_dataset_metadata",
            side_effect=FileNotFoundError("manifest missing"),
        ), patch.object(strategies_mod.logger, "warning") as warning_mock:
            resp = client.get("/api/strategies")

        assert resp.status_code == 200
        data = resp.json()
        assert data["strategies"][0]["screening_support"] == "supported"
        assert data["strategies"][0]["entry_decidability"] == "pre_open_decidable"
        assert data["strategies"][0]["screening_error"] == "manifest missing"
        assert data["strategies"][0]["dataset_name"] is None
        assert data["strategies"][0]["dataset_preset"] is None
        assert data["strategies"][0]["screening_default_markets"] is None
        warning_mock.assert_not_called()

    def test_error_500(self, client, mock_config_loader):
        mock_config_loader.get_strategy_metadata.side_effect = RuntimeError("boom")

        resp = client.get("/api/strategies")
        assert resp.status_code == 500


class TestDefaultConfig:
    def test_get_success(self, client, mock_config_loader, tmp_path):
        default_path = tmp_path / "default.yaml"
        default_path.write_text("default:\n  parameters:\n    shared_config:\n      universe_preset: prime\n", encoding="utf-8")
        mock_config_loader.get_default_config_path.return_value = default_path

        resp = client.get("/api/config/default")
        assert resp.status_code == 200
        assert "default:" in resp.json()["content"]

    def test_get_not_found(self, client, mock_config_loader, tmp_path):
        mock_config_loader.get_default_config_path.return_value = tmp_path / "default.yaml"

        resp = client.get("/api/config/default")
        assert resp.status_code == 404

    def test_get_error_500(self, client, mock_config_loader):  # noqa: ARG002
        mock_config_loader.get_default_config_path.return_value = None

        resp = client.get("/api/config/default")
        assert resp.status_code == 500

    def test_editor_context_success(self, client, mock_config_loader, tmp_path):
        default_path = tmp_path / "default.yaml"
        default_path.write_text(
            (
                "default:\n"
                "  extra_note: keep me\n"
                "  execution:\n"
                "    output_directory: /tmp/backtest\n"
                "  parameters:\n"
                "    shared_config:\n"
                "      universe_preset: primeExTopix500\n"
                "      benchmark_table: topix\n"
            ),
            encoding="utf-8",
        )
        mock_config_loader.get_default_config_path.return_value = default_path

        resp = client.get("/api/config/default/editor-context")

        assert resp.status_code == 200
        data = resp.json()
        assert data["raw_execution"]["output_directory"] == "/tmp/backtest"
        assert data["raw_shared_config"]["universe_preset"] == "primeExTopix500"
        assert data["advanced_only_paths"] == ["default.extra_note"]

    def test_update_success(self, client, mock_config_loader, tmp_path):
        write_path = tmp_path / "xdg" / "default.yaml"
        mock_config_loader.get_default_config_write_path.return_value = write_path

        resp = client.put(
            "/api/config/default",
            json={"content": "default:\n  parameters:\n    shared_config:\n      universe_preset: prime\n"},
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        assert write_path.exists()
        mock_config_loader.reload_default_config.assert_called_once()

    def test_update_yaml_syntax_error_400(self, client, mock_config_loader, tmp_path):
        mock_config_loader.get_default_config_write_path.return_value = tmp_path / "default.yaml"

        resp = client.put(
            "/api/config/default",
            json={"content": "default:\n  - [invalid"},
        )
        assert resp.status_code == 400

    def test_update_non_object_400(self, client, mock_config_loader, tmp_path):
        mock_config_loader.get_default_config_write_path.return_value = tmp_path / "default.yaml"

        resp = client.put(
            "/api/config/default",
            json={"content": "- one\n- two\n"},
        )
        assert resp.status_code == 400
        assert "オブジェクト" in resp.json()["detail"]

    def test_update_missing_default_key_400(self, client, mock_config_loader, tmp_path):
        mock_config_loader.get_default_config_write_path.return_value = tmp_path / "default.yaml"

        resp = client.put(
            "/api/config/default",
            json={"content": "foo:\n  bar: 1\n"},
        )
        assert resp.status_code == 400
        assert "default" in resp.json()["detail"]

    def test_update_default_not_object_400(self, client, mock_config_loader, tmp_path):
        mock_config_loader.get_default_config_write_path.return_value = tmp_path / "default.yaml"

        resp = client.put(
            "/api/config/default",
            json={"content": "default: 1\n"},
        )
        assert resp.status_code == 400

    def test_update_creates_missing_xdg_parent(self, client, mock_config_loader, tmp_path):
        write_path = tmp_path / "missing_parent" / "default.yaml"
        mock_config_loader.get_default_config_write_path.return_value = write_path

        resp = client.put(
            "/api/config/default",
            json={"content": "default:\n  parameters:\n    shared_config:\n      universe_preset: prime\n"},
        )
        assert resp.status_code == 200
        assert write_path.exists()

    def test_structured_update_preserves_comments(self, client, mock_config_loader, tmp_path):
        default_path = tmp_path / "default.yaml"
        default_path.write_text(
            (
                "# top comment\n"
                "default:\n"
                "  # keep note\n"
                "  extra_note: keep me\n"
                "  execution:\n"
                "    output_directory: /tmp/old  # exec comment\n"
                "  parameters:\n"
                "    shared_config:\n"
                "      universe_preset: old-preset  # universe comment\n"
                "      benchmark_table: topix\n"
            ),
            encoding="utf-8",
        )
        mock_config_loader.get_default_config_path.return_value = default_path
        mock_config_loader.get_default_config_write_path.return_value = default_path

        resp = client.put(
            "/api/config/default/structured",
            json={
                "execution": {"output_directory": "/tmp/new"},
                "shared_config": {
                    "universe_preset": "standard",
                    "benchmark_table": "topix",
                },
            },
        )

        assert resp.status_code == 200
        assert resp.json()["success"] is True
        saved = default_path.read_text(encoding="utf-8")
        assert "# top comment" in saved
        assert "# keep note" in saved
        assert "extra_note: keep me" in saved
        assert "output_directory: /tmp/new" in saved
        assert "universe_preset: standard" in saved
        mock_config_loader.reload_default_config.assert_called_once()

    def test_structured_update_reads_baseline_and_writes_override(self, client, mock_config_loader, tmp_path):
        baseline_path = tmp_path / "repo" / "default.yaml"
        baseline_path.parent.mkdir(parents=True)
        baseline_path.write_text(
            (
                "default:\n"
                "  execution:\n"
                "    output_directory: /tmp/old\n"
                "  parameters:\n"
                "    shared_config:\n"
                "      universe_preset: prime\n"
            ),
            encoding="utf-8",
        )
        override_path = tmp_path / "xdg" / "default.yaml"
        mock_config_loader.get_default_config_path.return_value = baseline_path
        mock_config_loader.get_default_config_write_path.return_value = override_path

        resp = client.put(
            "/api/config/default/structured",
            json={
                "execution": {"output_directory": "/tmp/override"},
                "shared_config": {"universe_preset": "growth"},
            },
        )

        assert resp.status_code == 200
        assert "universe_preset: prime" in baseline_path.read_text(encoding="utf-8")
        saved = override_path.read_text(encoding="utf-8")
        assert "output_directory: /tmp/override" in saved
        assert "universe_preset: growth" in saved

    def test_structured_update_rejects_unknown_execution_field(
        self,
        client,
        mock_config_loader,
        tmp_path,
    ):
        default_path = tmp_path / "default.yaml"
        default_path.write_text("default:\n  execution: {}\n", encoding="utf-8")
        mock_config_loader.get_default_config_path.return_value = default_path
        mock_config_loader.get_default_config_write_path.return_value = default_path

        resp = client.put(
            "/api/config/default/structured",
            json={
                "execution": {"unknown_field": "value"},
                "shared_config": {},
            },
        )

        assert resp.status_code == 400
        assert "Unknown execution field" in resp.json()["detail"]
