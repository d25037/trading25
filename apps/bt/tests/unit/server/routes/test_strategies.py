"""server/routes/strategies.py のテスト"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.server.routes.strategies as strategies_mod
from src.server.routes.strategies import router


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
        with patch("src.lib.backtest_core.runner.BacktestRunner") as mock_runner_cls:
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
        with patch("src.lib.backtest_core.runner.BacktestRunner") as mock_runner_cls:
            mock_runner = MagicMock()
            mock_runner.get_execution_info.return_value = {}
            mock_runner_cls.return_value = mock_runner
            resp = client.post("/api/strategies/test/validate")
        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is True

    def test_missing_params_warning(self, client, mock_config_loader):
        mock_config_loader.load_strategy_config.return_value = {}
        with patch("src.lib.backtest_core.runner.BacktestRunner") as mock_runner_cls:
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
        with patch("src.lib.backtest_core.runner.BacktestRunner") as mock_runner_cls:
            mock_runner = MagicMock()
            mock_runner.get_execution_info.return_value = {}
            mock_runner_cls.return_value = mock_runner
            resp = client.post("/api/strategies/test/validate")
        data = resp.json()
        assert data["valid"] is False
        assert any("kelly_fraction" in e for e in data["errors"])


class TestUpdateStrategy:
    def test_success(self, client, mock_config_loader):
        mock_config_loader.is_editable_category.return_value = True
        mock_config_loader.save_strategy_config.return_value = Path("/saved/path.yaml")
        resp = client.put(
            "/api/strategies/test",
            json={"config": {"entry_filter_params": {}}},
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_non_editable_403(self, client, mock_config_loader):
        mock_config_loader.is_editable_category.return_value = False
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
