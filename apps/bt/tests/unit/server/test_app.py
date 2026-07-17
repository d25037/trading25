"""app.py のテスト"""

import asyncio
import hashlib
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from fastapi.testclient import TestClient

from src.entrypoints.http.app import _periodic_cleanup, create_app, lifespan
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market.market_operation_lease import MarketOperationLease
from tests.unit.server.db.market_writer_test_support import open_market_db


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

    @pytest.mark.parametrize(
        ("method", "path", "payload"),
        [
            ("POST", "/api/db/sync", {"mode": "initial", "resetBeforeSync": True}),
            ("POST", "/api/db/adjusted-metrics/materialize", {}),
            ("POST", "/api/db/intraday/sync", {}),
            ("POST", "/api/db/stocks/refresh", {"codes": ["7203"]}),
            ("DELETE", "/api/db/sync/jobs/job-1", None),
        ],
    )
    def test_retained_runtime_rejects_market_mutations_with_unified_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        method: str,
        path: str,
        payload: dict[str, object] | None,
    ) -> None:
        market = tmp_path / "market-timeseries"
        (market / "parquet").mkdir(parents=True)
        market_db = market / "market.duckdb"
        parquet_file = market / "parquet/part.parquet"
        market_db.write_bytes(b"retained-market")
        parquet_file.write_bytes(b"retained-parquet")

        def identity() -> tuple[tuple[int, int, int, str], ...]:
            return tuple(
                (
                    path.stat().st_dev,
                    path.stat().st_ino,
                    path.stat().st_size,
                    hashlib.sha256(path.read_bytes()).hexdigest(),
                )
                for path in (market_db, parquet_file)
            )

        before = identity()
        monkeypatch.setenv(
            "TRADING25_RUNTIME_CAPABILITY",
            "retained_market_smoke",
        )
        client = TestClient(create_app(), raise_server_exceptions=False)

        response = client.request(
            method,
            path,
            json=payload,
            headers={"x-correlation-id": "retained-guard-test"},
        )

        assert response.status_code == 403
        assert response.headers["x-correlation-id"] == "retained-guard-test"
        body = response.json()
        assert body["status"] == "error"
        assert body["error"] == "Forbidden"
        assert "retained_market_smoke" in body["message"]
        assert identity() == before


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

    @pytest.mark.darwin_capability
    @pytest.mark.skipif(
        sys.platform != "darwin",
        reason="requires Darwin F_GETPATH inherited-root resolution",
    )
    @pytest.mark.asyncio
    async def test_owned_lifespan_adopts_exact_fd_without_releasing_parent_exclusive(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        root = tmp_path / "root"
        settings = self._lease_settings(root)
        parent = MarketOperationLease.acquire(root, exclusive=True)
        inherited_fd = os.dup(parent.fd)
        inherited_root_fd = os.dup(parent.root_fd)
        monkeypatch.setenv("TRADING25_MARKET_OPERATION_LOCK_FD", str(inherited_fd))
        monkeypatch.setenv("TRADING25_DATA_ROOT_FD", str(inherited_root_fd))
        monkeypatch.setenv("TRADING25_DATA_DIR", str(root))
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
        assert app.state.market_operation_lease.fd == -1
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            MarketOperationLease.acquire(root, exclusive=False)
        parent.release()
        with MarketOperationLease.acquire(root, exclusive=False):
            pass

    @pytest.mark.darwin_capability
    @pytest.mark.skipif(
        sys.platform != "darwin",
        reason="requires Darwin F_GETPATH inherited-root resolution",
    )
    @pytest.mark.asyncio
    async def test_owned_lifespan_adopts_inherited_root_without_path_reopen(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        root = tmp_path / "root"
        settings = self._lease_settings(root)
        parent = MarketOperationLease.acquire(root, exclusive=True)
        inherited_fd = os.dup(parent.fd)
        inherited_root_fd = os.dup(parent.root_fd)
        monkeypatch.setenv("TRADING25_MARKET_OPERATION_LOCK_FD", str(inherited_fd))
        monkeypatch.setenv("TRADING25_DATA_ROOT_FD", str(inherited_root_fd))
        monkeypatch.setenv("TRADING25_DATA_DIR", str(root))
        monkeypatch.setattr("src.entrypoints.http.app.get_settings", lambda: settings)
        monkeypatch.setattr(
            "src.entrypoints.http.app.prepare_market_managed_root",
            lambda *_args: (_ for _ in ()).throw(
                AssertionError("owned server must not reopen its lexical root")
            ),
        )

        class StartupMarker(RuntimeError):
            pass

        monkeypatch.setattr(
            "src.entrypoints.http.app.JQuantsAsyncClient",
            lambda **_kwargs: (_ for _ in ()).throw(StartupMarker()),
        )
        try:
            with pytest.raises(StartupMarker):
                async with lifespan(create_app()):
                    pass
        finally:
            parent.release()

    @pytest.mark.darwin_capability
    @pytest.mark.skipif(
        sys.platform != "darwin",
        reason="requires Darwin F_GETPATH inherited-root resolution",
    )
    @pytest.mark.asyncio
    async def test_retained_lifespan_uses_only_read_only_market_startup(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from src.infrastructure.db.market.market_db import MarketDb as RealMarketDb

        root = tmp_path / "retained"
        market = root / "market-timeseries"
        parquet = market / "parquet"
        parquet.mkdir(parents=True)
        db_path = market / "market.duckdb"
        open_market_db(str(db_path)).close()
        parquet_file = parquet / "part.parquet"
        parquet_file.write_bytes(b"retained")
        (root / "datasets").mkdir()
        settings = SimpleNamespace(
            market_timeseries_dir=str(market),
            jquants_api_key="",
            jquants_plan="free",
            moomoo_opend_host="127.0.0.1",
            moomoo_opend_port=11111,
            moomoo_opend_is_encrypt=False,
            moomoo_opend_enabled=False,
            moomoo_opend_max_history_rows=100,
            portfolio_db_path=None,
            dataset_base_path=str(root / "datasets"),
        )
        parent = MarketOperationLease.acquire(root, exclusive=True)
        inherited_fd = os.dup(parent.fd)
        inherited_root_fd = os.dup(parent.root_fd)
        monkeypatch.setenv("TRADING25_RUNTIME_CAPABILITY", "retained_market_smoke")
        monkeypatch.setenv("TRADING25_MARKET_OPERATION_LOCK_FD", str(inherited_fd))
        monkeypatch.setenv("TRADING25_DATA_ROOT_FD", str(inherited_root_fd))
        monkeypatch.setenv("TRADING25_DATA_DIR", str(root))
        monkeypatch.setattr("src.entrypoints.http.app.get_settings", lambda: settings)
        modes: list[bool] = []

        def observed_market_db(path: str, *, read_only: bool = False):
            modes.append(read_only)
            if not read_only:
                raise AssertionError("retained lifespan constructed writable MarketDb")
            return RealMarketDb(path, read_only=True)

        def identity() -> tuple[tuple[int, int, int, str], ...]:
            return tuple(
                (
                    path.stat().st_dev,
                    path.stat().st_ino,
                    path.stat().st_size,
                    hashlib.sha256(path.read_bytes()).hexdigest(),
                )
                for path in (db_path, parquet_file)
            )

        before = identity()
        app = create_app()
        with (
            patch("src.entrypoints.http.app.MarketDb", side_effect=observed_market_db),
            patch(
                "src.entrypoints.http.app.JQuantsAsyncClient",
                side_effect=AssertionError(
                    "retained startup must not create J-Quants client"
                ),
            ),
            patch(
                "src.entrypoints.http.app.MoomooQuoteClient",
                side_effect=AssertionError(
                    "retained startup must not create moomoo client"
                ),
            ),
            patch(
                "src.entrypoints.http.app.PortfolioDb",
                side_effect=AssertionError(
                    "retained startup must not open PortfolioDb"
                ),
            ),
            patch(
                "src.entrypoints.http.app._periodic_cleanup",
                side_effect=AssertionError(
                    "retained startup must not schedule cleanup"
                ),
            ),
            patch("src.entrypoints.http.app.job_manager") as primary_jobs,
            patch("src.entrypoints.http.app.screening_job_manager") as screening_jobs,
            patch(
                "src.application.services.sync_service.sync_job_manager"
            ) as sync_jobs,
            patch(
                "src.application.services.sync_service.adjusted_metrics_materialize_job_manager"
            ) as materialize_jobs,
            patch(
                "src.application.services.dataset_builder_service.dataset_job_manager"
            ) as dataset_jobs,
            patch(
                "src.entrypoints.http.app.screening_job_service"
            ) as screening_service,
        ):
            primary_jobs.reconcile_orphaned_jobs = AsyncMock(
                side_effect=AssertionError("retained startup must not reconcile jobs")
            )
            screening_jobs.reconcile_orphaned_jobs = AsyncMock(
                side_effect=AssertionError("retained startup must not reconcile jobs")
            )
            for manager in (
                primary_jobs,
                screening_jobs,
                sync_jobs,
                materialize_jobs,
                dataset_jobs,
                screening_service,
            ):
                manager.shutdown = AsyncMock()
            screening_service._executor._broken = True
            try:
                async with lifespan(app):
                    assert identity() == before
            finally:
                parent.release()

        assert modes and all(modes)
        assert identity() == before
        primary_jobs.reconcile_orphaned_jobs.assert_not_awaited()
        screening_jobs.reconcile_orphaned_jobs.assert_not_awaited()

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

    @pytest.mark.darwin_capability
    @pytest.mark.skipif(
        sys.platform != "darwin",
        reason="requires Darwin F_GETPATH inherited-root resolution",
    )
    @pytest.mark.asyncio
    async def test_owned_lifespan_rejects_wrong_fd_inode_or_root(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        root = tmp_path / "root"
        other = tmp_path / "other"
        settings = self._lease_settings(root)
        self._lease_settings(other)
        monkeypatch.setattr("src.entrypoints.http.app.get_settings", lambda: settings)
        monkeypatch.setenv("TRADING25_DATA_DIR", str(root))

        wrong_path = tmp_path / "wrong.lock"
        wrong_fd = os.open(wrong_path, os.O_CREAT | os.O_RDWR, 0o600)
        monkeypatch.setenv("TRADING25_MARKET_OPERATION_LOCK_FD", str(wrong_fd))
        root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        monkeypatch.setenv("TRADING25_DATA_ROOT_FD", str(root_fd))
        try:
            with pytest.raises(CutoverSafetyError, match="invalid|identity mismatch"):
                async with lifespan(create_app()):
                    pass
        finally:
            os.close(wrong_fd)

        other_lease = MarketOperationLease.acquire(other, exclusive=True)
        inherited_fd = os.dup(other_lease.fd)
        monkeypatch.setenv("TRADING25_MARKET_OPERATION_LOCK_FD", str(inherited_fd))
        root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        monkeypatch.setenv("TRADING25_DATA_ROOT_FD", str(root_fd))
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
