"""server/routes/strategies.py のテスト"""

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

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
            "entry_filter_params": {"volume": {"enabled": True}},
            "shared_config": {"dataset": "test"},
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


class TestValidateStrategy:
    def test_valid_config(self, client, mock_config_loader):
        mock_config_loader.load_strategy_config.return_value = {
            "entry_filter_params": {"volume": {"enabled": True}},
        }
        with patch("src.domains.backtest.core.runner.BacktestRunner") as mock_runner_cls:
            mock_runner = MagicMock()
            mock_runner.get_execution_info.return_value = {}
            mock_runner_cls.return_value = mock_runner
            resp = client.post("/api/strategies/test/validate")
        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is True

    def test_missing_params_warning(self, client, mock_config_loader):
        mock_config_loader.load_strategy_config.return_value = {}
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
        with patch("src.domains.backtest.core.runner.BacktestRunner") as mock_runner_cls:
            mock_runner = MagicMock()
            mock_runner.get_execution_info.return_value = {}
            mock_runner_cls.return_value = mock_runner
            resp = client.post("/api/strategies/test/validate")
        data = resp.json()
        assert data["valid"] is False
        assert any("kelly_fraction" in e for e in data["errors"])

    def test_strict_nested_typo(self, client, mock_config_loader):
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

        resp = client.get("/api/strategies")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert data["strategies"][0]["name"] == "production/range_break_v16"

    def test_error_500(self, client, mock_config_loader):
        mock_config_loader.get_strategy_metadata.side_effect = RuntimeError("boom")

        resp = client.get("/api/strategies")
        assert resp.status_code == 500


class TestDefaultConfig:
    def test_get_success(self, client, mock_config_loader, tmp_path):
        default_path = tmp_path / "default.yaml"
        default_path.write_text("default:\n  dataset: test\n", encoding="utf-8")
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

    def test_update_success(self, client, mock_config_loader, tmp_path):
        mock_config_loader.get_default_config_path.return_value = tmp_path / "default.yaml"

        resp = client.put(
            "/api/config/default",
            json={"content": "default:\n  dataset: test\n"},
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        assert (tmp_path / "default.yaml").exists()
        mock_config_loader.reload_default_config.assert_called_once()

    def test_update_yaml_syntax_error_400(self, client, mock_config_loader, tmp_path):
        mock_config_loader.get_default_config_path.return_value = tmp_path / "default.yaml"

        resp = client.put(
            "/api/config/default",
            json={"content": "default:\n  - [invalid"},
        )
        assert resp.status_code == 400

    def test_update_non_object_400(self, client, mock_config_loader, tmp_path):
        mock_config_loader.get_default_config_path.return_value = tmp_path / "default.yaml"

        resp = client.put(
            "/api/config/default",
            json={"content": "- one\n- two\n"},
        )
        assert resp.status_code == 400
        assert "オブジェクト" in resp.json()["detail"]

    def test_update_missing_default_key_400(self, client, mock_config_loader, tmp_path):
        mock_config_loader.get_default_config_path.return_value = tmp_path / "default.yaml"

        resp = client.put(
            "/api/config/default",
            json={"content": "foo:\n  bar: 1\n"},
        )
        assert resp.status_code == 400
        assert "default" in resp.json()["detail"]

    def test_update_default_not_object_400(self, client, mock_config_loader, tmp_path):
        mock_config_loader.get_default_config_path.return_value = tmp_path / "default.yaml"

        resp = client.put(
            "/api/config/default",
            json={"content": "default: 1\n"},
        )
        assert resp.status_code == 400

    def test_update_write_error_500(self, client, mock_config_loader, tmp_path):
        mock_config_loader.get_default_config_path.return_value = (
            tmp_path / "missing_parent" / "default.yaml"
        )

        resp = client.put(
            "/api/config/default",
            json={"content": "default:\n  dataset: test\n"},
        )
        assert resp.status_code == 500
