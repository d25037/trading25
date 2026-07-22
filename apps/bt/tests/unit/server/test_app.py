"""app.py のテスト"""

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from fastapi.testclient import TestClient

import src.entrypoints.http.app as app_module
from src.entrypoints.http.app import _periodic_cleanup, create_app, lifespan
from src.infrastructure.db.market.market_operation_lease import MarketOperationLease
from src.shared.config.settings import Settings
from tests.unit.server.db.market_writer_test_support import (
    connect_market_duckdb_for_test,
    open_market_db,
)


class TestCreateApp:
    def test_returns_fastapi_instance(self) -> None:
        app = create_app()
        assert app.title == "Trading25 API"

    def test_has_routes(self) -> None:
        app = create_app()
        paths = app.openapi()["paths"]
        assert "/api/health" in paths
        assert "/api/strategies" in paths

    def test_cors_middleware_added(self) -> None:
        app = create_app()
        middleware_classes = [m.cls.__name__ for m in app.user_middleware]
        assert "CORSMiddleware" in middleware_classes

    def test_health_endpoint(self) -> None:
        app = create_app()
        client = TestClient(app)
        resp = client.get("/api/health")
        assert resp.status_code == 200

class TestPeriodicCleanup:
    @pytest.mark.asyncio
    async def test_cleanup_runs_and_logs(self) -> None:
        with patch("src.entrypoints.http.app.job_manager") as mock_jm:
            mock_jm.cleanup_old_jobs.return_value = 3
            with patch(
                "src.entrypoints.http.app.asyncio.sleep",
                side_effect=asyncio.CancelledError,
            ):
                with pytest.raises(asyncio.CancelledError):
                    await _periodic_cleanup(interval_seconds=1)

    @pytest.mark.asyncio
    async def test_cleanup_handles_exception(self) -> None:
        with patch("src.entrypoints.http.app.job_manager") as mock_jm:
            mock_jm.cleanup_old_jobs.side_effect = RuntimeError("test error")
            with patch(
                "src.entrypoints.http.app.asyncio.sleep",
                side_effect=[None, asyncio.CancelledError],
            ):
                with pytest.raises(asyncio.CancelledError):
                    await _periodic_cleanup(interval_seconds=1)


class TestLifespan:
    @staticmethod
    def _lease_settings(root: Path) -> SimpleNamespace:
        market = root / "market-timeseries"
        market.mkdir(parents=True)
        return SimpleNamespace(
            market_timeseries_dir=str(market),
            jquants_api_key="",
            jquants_plan="free",
        )

    @pytest.mark.asyncio
    async def test_normal_lifespan_does_not_hold_operation_lease(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        settings = self._lease_settings(tmp_path / "root")
        monkeypatch.delenv("TRADING25_MARKET_OPERATION_LOCK_FD", raising=False)
        monkeypatch.setattr("src.entrypoints.http.app.get_settings", lambda: settings)
        app = create_app()

        class StartupMarker(RuntimeError):
            pass

        monkeypatch.setattr(
            "src.entrypoints.http.app.JQuantsAsyncClient",
            lambda **_kwargs: (_ for _ in ()).throw(StartupMarker()),
        )
        with MarketOperationLease.acquire(tmp_path / "root", exclusive=True):
            with pytest.raises(StartupMarker):
                async with lifespan(app):
                    pass
            assert app.state.market_operation_lease is None

    @pytest.mark.asyncio
    async def test_normal_lifespan_ignores_removed_cutover_capability(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        settings = self._lease_settings(tmp_path / "root")
        monkeypatch.setenv("TRADING25_RUNTIME_CAPABILITY", "retained_market_smoke")
        monkeypatch.delenv("TRADING25_MARKET_OPERATION_LOCK_FD", raising=False)
        monkeypatch.delenv("TRADING25_DATA_ROOT_FD", raising=False)
        monkeypatch.setattr("src.entrypoints.http.app.get_settings", lambda: settings)

        class StartupMarker(RuntimeError):
            pass

        monkeypatch.setattr(
            "src.entrypoints.http.app.JQuantsAsyncClient",
            lambda **_kwargs: (_ for _ in ()).throw(StartupMarker()),
        )

        with pytest.raises(StartupMarker):
            async with lifespan(create_app()):
                pass

    @pytest.mark.asyncio
    async def test_lifespan_releases_shared_lease_on_startup_exception(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        root = tmp_path / "root"
        settings = self._lease_settings(root)
        monkeypatch.delenv("TRADING25_MARKET_OPERATION_LOCK_FD", raising=False)
        monkeypatch.setattr("src.entrypoints.http.app.get_settings", lambda: settings)

        class StartupMarker(RuntimeError):
            pass

        monkeypatch.setattr(
            "src.entrypoints.http.app.JQuantsAsyncClient",
            lambda **_kwargs: (_ for _ in ()).throw(StartupMarker()),
        )
        app = create_app()
        with pytest.raises(StartupMarker):
            async with lifespan(app):
                pass

        assert app.state.market_operation_lease is None
        with MarketOperationLease.acquire(root, exclusive=True):
            pass

    @pytest.mark.asyncio
    async def test_lifespan_rejects_legacy_bigint_adjusted_volume_with_reset_guidance(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        market_root = tmp_path / "market-timeseries"
        db_path = market_root / "market.duckdb"
        seeded = open_market_db(str(db_path), read_only=False)
        seeded.close()
        with connect_market_duckdb_for_test(str(db_path)) as connection:
            connection.execute(
                "ALTER TABLE stock_data_raw ALTER adjusted_volume TYPE BIGINT"
            )
            connection.execute("ALTER TABLE stock_data ALTER volume TYPE BIGINT")

        settings = Settings(
            MARKET_TIMESERIES_DIR=str(market_root),
            PORTFOLIO_DB_PATH=str(tmp_path / "portfolio.db"),
            DATASET_BASE_PATH=str(tmp_path / "datasets"),
        )
        monkeypatch.setattr(app_module, "get_settings", lambda: settings)
        reader_factory = MagicMock()
        monkeypatch.setattr(app_module, "MarketDbReader", reader_factory)
        monkeypatch.setattr(app_module, "MarketDb", MagicMock(return_value=MagicMock()))
        store_factory = MagicMock(return_value=MagicMock())
        monkeypatch.setattr(
            app_module,
            "create_time_series_store",
            store_factory,
        )
        monkeypatch.setattr(app_module, "PortfolioDb", MagicMock(return_value=MagicMock()))
        warning = MagicMock()
        monkeypatch.setattr(app_module.logger, "warning", warning)

        class StartupMarker(RuntimeError):
            pass

        monkeypatch.setattr(
            app_module.job_manager,
            "reconcile_orphaned_jobs",
            AsyncMock(side_effect=StartupMarker("stop after Market startup")),
        )
        app = create_app()

        with pytest.raises(StartupMarker):
            async with lifespan(app):
                pass

        reader_factory.assert_not_called()
        store_factory.assert_not_called()
        assert app.state.market_reader is None
        assert app.state.market_time_series_store is None
        assert any(
            "RESET initial" in str(call_args)
            for call_args in warning.call_args_list
        )

    @pytest.mark.asyncio
    async def test_lifespan_startup_shutdown(self) -> None:
        app = create_app()

        # Create a real cancelled task to mimic the actual behavior
        async def _fake_cleanup() -> None:
            await asyncio.sleep(3600)

        with (
            patch(
                "src.entrypoints.http.app._periodic_cleanup", side_effect=_fake_cleanup
            ),
            patch("src.entrypoints.http.app.backtest_service") as mock_bt,
            patch("src.entrypoints.http.app.optimization_service") as mock_opt,
            patch("src.entrypoints.http.app.lab_service") as mock_lab,
            patch(
                "src.entrypoints.http.app.close_all_cached_data_access_clients"
            ) as mock_close_clients,
            patch("src.entrypoints.http.app.job_manager") as mock_job_manager,
            patch(
                "src.entrypoints.http.app.screening_job_manager"
            ) as mock_screening_job_manager,
            patch(
                "src.entrypoints.http.app.screening_job_service"
            ) as mock_screening_job_service,
            patch(
                "src.application.services.sync_service.sync_job_manager"
            ) as mock_sync_job_manager,
            patch(
                "src.application.services.dataset_builder_service.dataset_job_manager"
            ) as mock_dataset_job_manager,
        ):
            # Mock executors に _broken / _shutdown を明示セット
            for mock_svc in (mock_bt, mock_opt, mock_lab):
                mock_svc._executor._broken = False
                mock_svc._executor._shutdown = False
            mock_job_manager.reconcile_orphaned_jobs = AsyncMock(return_value=["job-1"])
            mock_job_manager.shutdown = AsyncMock(return_value=1)
            mock_screening_job_manager.reconcile_orphaned_jobs = AsyncMock(
                return_value=["job-2"]
            )
            mock_screening_job_manager.shutdown = AsyncMock(return_value=1)
            mock_screening_job_service.shutdown = AsyncMock()
            mock_sync_job_manager.shutdown = AsyncMock()
            mock_dataset_job_manager.shutdown = AsyncMock()

            replacement_reader = MagicMock()
            replacement_store = MagicMock()
            replacement_db = MagicMock()
            async with lifespan(app):
                app.state.market_reader = replacement_reader
                app.state.market_time_series_store = replacement_store
                app.state.market_db = replacement_db

            # shutdown phase
            mock_job_manager.reconcile_orphaned_jobs.assert_awaited_once()
            mock_screening_job_manager.reconcile_orphaned_jobs.assert_awaited_once()
            mock_job_manager.shutdown.assert_awaited_once()
            mock_screening_job_manager.shutdown.assert_awaited_once()
            mock_screening_job_service.shutdown.assert_awaited_once()
            mock_sync_job_manager.shutdown.assert_awaited_once()
            mock_dataset_job_manager.shutdown.assert_awaited_once()
            assert mock_job_manager.set_portfolio_db.call_args_list[-1] == call(None)
            assert mock_screening_job_manager.set_portfolio_db.call_args_list[
                -1
            ] == call(None)
            mock_bt._executor.shutdown.assert_called_once_with(wait=True)
            mock_opt._executor.shutdown.assert_called_once_with(wait=True)
            mock_close_clients.assert_called_once_with()
            replacement_reader.close.assert_called_once_with()
            replacement_store.close.assert_called_once_with()
            replacement_db.close.assert_called_once_with()
