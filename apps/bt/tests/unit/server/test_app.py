"""app.py のテスト"""

import asyncio
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, call, patch

import pytest
from fastapi.testclient import TestClient

from src.entrypoints.http.app import _periodic_cleanup, create_app, lifespan
from src.application.services.market_v4_cutover import (
    CutoverSafetyError,
    MarketOperationLease,
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
            with patch("src.entrypoints.http.app.asyncio.sleep", side_effect=asyncio.CancelledError):
                with pytest.raises(asyncio.CancelledError):
                    await _periodic_cleanup(interval_seconds=1)

    @pytest.mark.asyncio
    async def test_cleanup_handles_exception(self) -> None:
        with patch("src.entrypoints.http.app.job_manager") as mock_jm:
            mock_jm.cleanup_old_jobs.side_effect = RuntimeError("test error")
            with patch("src.entrypoints.http.app.asyncio.sleep", side_effect=[None, asyncio.CancelledError]):
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
    async def test_normal_lifespan_shared_lease_rejects_active_exclusive(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        settings = self._lease_settings(tmp_path / "root")
        monkeypatch.delenv("TRADING25_MARKET_OPERATION_LOCK_FD", raising=False)
        monkeypatch.setattr("src.entrypoints.http.app.get_settings", lambda: settings)
        with MarketOperationLease.acquire(tmp_path / "root", exclusive=True):
            with pytest.raises(CutoverSafetyError, match="operation lease"):
                async with lifespan(create_app()):
                    pass

    @pytest.mark.asyncio
    async def test_owned_lifespan_adopts_exact_fd_without_releasing_parent_exclusive(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        root = tmp_path / "root"
        settings = self._lease_settings(root)
        parent = MarketOperationLease.acquire(root, exclusive=True)
        inherited_fd = os.dup(parent.fd)
        monkeypatch.setenv("TRADING25_MARKET_OPERATION_LOCK_FD", str(inherited_fd))
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
        app.state.market_operation_lease.release()
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            MarketOperationLease.acquire(root, exclusive=False)
        parent.release()
        with MarketOperationLease.acquire(root, exclusive=False):
            pass

    @pytest.mark.asyncio
    async def test_owned_lifespan_rejects_wrong_fd_inode_or_root(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        root = tmp_path / "root"
        other = tmp_path / "other"
        settings = self._lease_settings(root)
        self._lease_settings(other)
        monkeypatch.setattr("src.entrypoints.http.app.get_settings", lambda: settings)

        wrong_path = tmp_path / "wrong.lock"
        wrong_fd = os.open(wrong_path, os.O_CREAT | os.O_RDWR, 0o600)
        monkeypatch.setenv("TRADING25_MARKET_OPERATION_LOCK_FD", str(wrong_fd))
        try:
            with pytest.raises(CutoverSafetyError, match="invalid|identity mismatch"):
                async with lifespan(create_app()):
                    pass
        finally:
            os.close(wrong_fd)

        other_lease = MarketOperationLease.acquire(other, exclusive=True)
        inherited_fd = os.dup(other_lease.fd)
        monkeypatch.setenv("TRADING25_MARKET_OPERATION_LOCK_FD", str(inherited_fd))
        try:
            with pytest.raises(CutoverSafetyError, match="invalid|identity mismatch"):
                async with lifespan(create_app()):
                    pass
        finally:
            os.close(inherited_fd)
            other_lease.release()

    @pytest.mark.asyncio
    async def test_lifespan_startup_shutdown(self) -> None:
        app = create_app()

        # Create a real cancelled task to mimic the actual behavior
        async def _fake_cleanup() -> None:
            await asyncio.sleep(3600)

        with (
            patch("src.entrypoints.http.app._periodic_cleanup", side_effect=_fake_cleanup),
            patch("src.entrypoints.http.app.backtest_service") as mock_bt,
            patch("src.entrypoints.http.app.optimization_service") as mock_opt,
            patch("src.entrypoints.http.app.lab_service") as mock_lab,
            patch("src.entrypoints.http.app.close_all_cached_data_access_clients") as mock_close_clients,
            patch("src.entrypoints.http.app.job_manager") as mock_job_manager,
            patch("src.entrypoints.http.app.screening_job_manager") as mock_screening_job_manager,
            patch("src.entrypoints.http.app.screening_job_service") as mock_screening_job_service,
            patch("src.application.services.sync_service.sync_job_manager") as mock_sync_job_manager,
            patch("src.application.services.dataset_builder_service.dataset_job_manager") as mock_dataset_job_manager,
        ):
            # Mock executors に _broken / _shutdown を明示セット
            for mock_svc in (mock_bt, mock_opt, mock_lab):
                mock_svc._executor._broken = False
                mock_svc._executor._shutdown = False
            mock_job_manager.reconcile_orphaned_jobs = AsyncMock(return_value=["job-1"])
            mock_job_manager.shutdown = AsyncMock(return_value=1)
            mock_screening_job_manager.reconcile_orphaned_jobs = AsyncMock(return_value=["job-2"])
            mock_screening_job_manager.shutdown = AsyncMock(return_value=1)
            mock_screening_job_service.shutdown = AsyncMock()
            mock_sync_job_manager.shutdown = AsyncMock()
            mock_dataset_job_manager.shutdown = AsyncMock()

            async with lifespan(app):
                pass  # startup phase

            # shutdown phase
            mock_job_manager.reconcile_orphaned_jobs.assert_awaited_once()
            mock_screening_job_manager.reconcile_orphaned_jobs.assert_awaited_once()
            mock_job_manager.shutdown.assert_awaited_once()
            mock_screening_job_manager.shutdown.assert_awaited_once()
            mock_screening_job_service.shutdown.assert_awaited_once()
            mock_sync_job_manager.shutdown.assert_awaited_once()
            mock_dataset_job_manager.shutdown.assert_awaited_once()
            assert mock_job_manager.set_portfolio_db.call_args_list[-1] == call(None)
            assert mock_screening_job_manager.set_portfolio_db.call_args_list[-1] == call(None)
            mock_bt._executor.shutdown.assert_called_once_with(wait=True)
            mock_opt._executor.shutdown.assert_called_once_with(wait=True)
            mock_close_clients.assert_called_once_with()
