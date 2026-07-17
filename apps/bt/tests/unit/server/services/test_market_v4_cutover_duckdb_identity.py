"""Market v4 cutover duckdb identity tests."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

import src.application.services.market_v4_cutover.duckdb_identity as cutover_module
from src.application.services.market_v4_cutover.contracts import (
    MarketSourceMetadata,
)
from src.application.services.market_v4_cutover.duckdb_identity import (
    DefaultDuckDbAdapter,
)
from src.application.services.market_v4_cutover.errors import (
    WorkerShutdownError,
)
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from tests.unit.server.db.market_writer_test_support import open_market_db
from tests.unit.server.services.market_v4_cutover_test_support import (
    FakeRuntime,
    FakeApi,
    _market_root,
    _retained_source,
    guard_lease_fd as guard_lease_fd,
)


def test_default_duckdb_adapter_checkpoints_and_reads_raw_metadata(
    tmp_path: Path,
    guard_lease_fd: int,
) -> None:
    import duckdb

    db_path = tmp_path / "market.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute("CREATE TABLE market_schema_version(version INTEGER)")
    connection.execute("INSERT INTO market_schema_version VALUES (4)")
    connection.execute("CREATE TABLE sync_metadata(key VARCHAR, value VARCHAR)")
    connection.execute(
        "INSERT INTO sync_metadata VALUES "
        "('stock_price_adjustment_mode', 'local_projection_v2_event_time')"
    )
    connection.close()

    adapter = DefaultDuckDbAdapter()
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        assert adapter.checkpoint_exclusive(
            directory_fd,
            "market.duckdb",
            guard_lease_fd=guard_lease_fd,
        ) == MarketSourceMetadata(4, "local_projection_v2_event_time", False)
        assert adapter.inspect(
            directory_fd,
            "market.duckdb",
            guard_lease_fd=guard_lease_fd,
        ) == MarketSourceMetadata(4, "local_projection_v2_event_time", False)
    finally:
        os.close(directory_fd)


def test_directory_bound_worker_disables_progress_output_before_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guard_lease_fd: int,
) -> None:
    events: list[str] = []

    class FakeConnection:
        def execute(self, sql: str) -> FakeConnection:
            events.append(sql)
            return self

        def close(self) -> None:
            events.append("close")

    class FakeDuckDb:
        @staticmethod
        def connect(filename: str, *, read_only: bool) -> FakeConnection:
            assert filename == "market.duckdb"
            assert read_only is True
            return FakeConnection()

    monkeypatch.setitem(cutover_module.sys.modules, "duckdb", FakeDuckDb)
    monkeypatch.setattr(
        DefaultDuckDbAdapter,
        "_metadata",
        staticmethod(
            lambda _connection: (
                events.append("metadata")
                or MarketSourceMetadata(4, "local_projection_v2_event_time", True)
            )
        ),
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    original_cwd_fd = os.open(".", os.O_RDONLY | os.O_DIRECTORY)
    monkeypatch.setattr(
        cutover_module.sys,
        "argv",
        ["worker", "inspect", str(directory_fd), str(guard_lease_fd), "market.duckdb"],
    )
    try:
        cutover_module.directory_bound_duckdb_worker()
    finally:
        os.fchdir(original_cwd_fd)
        os.close(original_cwd_fd)
        os.close(directory_fd)

    assert events[:2] == ["PRAGMA disable_progress_bar", "metadata"]


def test_rehearse_retained_rejects_real_duckdb_inexact_lineage_before_runtime(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    db_path = retained_root / "market-timeseries/market.duckdb"
    db_path.unlink()
    market_db = open_market_db(str(db_path))
    try:
        market_db._execute(
            """
            INSERT INTO statements (
                code, disclosed_date, earnings_per_share, type_of_current_period
            ) VALUES ('7203', '2024-05-10', 100, 'FY')
            """
        )
        market_db._execute(
            """
            INSERT INTO stock_adjustment_bases (
                code, basis_id, valid_from, valid_to_exclusive,
                adjustment_through_date, source_fingerprint,
                materialized_through_date, status
            ) VALUES (
                '7203', 'ready-7203', '2024-01-01', NULL,
                '2024-12-30', 'fp', '2024-12-30', 'ready'
            )
            """
        )
        market_db._execute(
            """
            INSERT INTO statement_metrics_adjusted (
                code, disclosed_date, period_end, period_type, price_basis_date,
                raw_eps, basis_version
            ) VALUES (
                '7203', '2024-05-10', '2024-05-10', 'FY', '2024-12-30',
                999, 'ready-7203'
            )
            """
        )
        market_db._execute(
            """
            INSERT INTO stock_data_raw (
                code, date, open, high, low, close, volume, adjustment_factor
            ) VALUES ('7203', '2024-06-03', 100, 110, 90, 105, 1000, 1)
            """
        )
        market_db._execute(
            """
            INSERT INTO stock_adjustment_basis_segments (
                code, basis_id, source_date_from, source_date_to_exclusive,
                cumulative_factor
            ) VALUES ('7203', 'ready-7203', '2024-01-01', NULL, 1)
            """
        )
        market_db._execute(
            """
            INSERT INTO daily_valuation (
                code, date, close, price_basis_date, basis_version
            ) VALUES ('7203', '2024-06-03', 105, '2024-12-30', 'ready-7203')
            """
        )
    finally:
        market_db.close()

    runtime = FakeRuntime(apis=[FakeApi()])
    service._workspace.duckdb = DefaultDuckDbAdapter()
    service._workspace.runtime = runtime

    with pytest.raises(CutoverSafetyError, match="rehearsal failed") as exc_info:
        service.rehearse_retained(
            "retained-real-inexact-lineage",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    assert "lineage is not ready" in str(exc_info.value.__cause__)
    assert runtime.start_calls == 0
    assert not (
        retained_root
        / "market-timeseries/.cutover-runtime-retained-real-inexact-lineage"
    ).exists()


def test_directory_bound_adapter_keeps_real_duckdb_bound_after_parent_swap(
    tmp_path: Path,
    guard_lease_fd: int,
) -> None:
    import duckdb

    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    market_root.mkdir(parents=True)
    db_path = market_root / "market.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute("CREATE TABLE market_schema_version(version INTEGER)")
    connection.execute("INSERT INTO market_schema_version VALUES (4)")
    connection.execute("CREATE TABLE sync_metadata(key VARCHAR, value VARCHAR)")
    connection.execute(
        "INSERT INTO sync_metadata VALUES "
        "('stock_price_adjustment_mode', 'local_projection_v2_event_time')"
    )
    connection.close()

    external = tmp_path / "external"
    external.mkdir()
    external_db = external / "market.duckdb"
    external_db.write_bytes(b"external-must-not-change")
    external_before = external_db.read_bytes()
    retained_fd = os.open(market_root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        detached = data_root / "market-timeseries.detached"
        market_root.rename(detached)
        market_root.symlink_to(external, target_is_directory=True)

        adapter = DefaultDuckDbAdapter()
        assert adapter.checkpoint_exclusive(
            retained_fd,
            "market.duckdb",
            guard_lease_fd=guard_lease_fd,
        ) == MarketSourceMetadata(4, "local_projection_v2_event_time", False)
        assert adapter.inspect(
            retained_fd,
            "market.duckdb",
            guard_lease_fd=guard_lease_fd,
        ) == MarketSourceMetadata(4, "local_projection_v2_event_time", False)
    finally:
        os.close(retained_fd)

    assert external_db.read_bytes() == external_before


def test_directory_bound_checkpoint_snapshot_holds_worker_until_release(
    tmp_path: Path,
    guard_lease_fd: int,
) -> None:
    import duckdb

    db_path = tmp_path / "market.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute("CREATE TABLE market_schema_version(version INTEGER)")
    connection.execute("INSERT INTO market_schema_version VALUES (4)")
    connection.close()
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    adapter = DefaultDuckDbAdapter()
    writer_probe = [
        cutover_module.sys.executable,
        "-c",
        (
            "import duckdb,sys;"
            "connection=duckdb.connect(sys.argv[1],read_only=False);"
            "connection.close()"
        ),
        str(db_path),
    ]
    try:
        with adapter.checkpoint_snapshot(
            directory_fd,
            "market.duckdb",
            guard_lease_fd=guard_lease_fd,
        ):
            locked = cutover_module.subprocess.run(
                writer_probe,
                capture_output=True,
                check=False,
            )
            assert locked.returncode != 0
        released = cutover_module.subprocess.run(
            writer_probe,
            capture_output=True,
            check=False,
        )
        assert released.returncode == 0
    finally:
        os.close(directory_fd)


def test_checkpoint_worker_timeout_is_killed_without_masking_body_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guard_lease_fd: int,
) -> None:
    class Pipe:
        closed = False

        def write(self, _payload: bytes) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    class HungProcess:
        stdin = Pipe()
        stderr = Pipe()
        terminated = False
        killed = False
        communicated = False

        def wait(self, timeout: float) -> int:
            if self.killed:
                return -9
            raise cutover_module.subprocess.TimeoutExpired("worker", timeout)

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True

        def communicate(self, timeout: float) -> tuple[bytes, bytes]:
            del timeout
            self.communicated = True
            return b"", b""

    class BodyError(RuntimeError):
        pass

    process = HungProcess()
    release_pipe = process.stdin
    adapter = DefaultDuckDbAdapter()
    monkeypatch.setattr(adapter, "_start_worker", lambda *_args, **_kwargs: process)
    monkeypatch.setattr(
        adapter,
        "_read_metadata",
        lambda _process: MarketSourceMetadata(4, "local_projection_v2_event_time"),
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(BodyError):
            with adapter.checkpoint_snapshot(
                directory_fd,
                "market.duckdb",
                guard_lease_fd=guard_lease_fd,
            ):
                raise BodyError("original body failure")
    finally:
        os.close(directory_fd)

    assert process.terminated is True
    assert process.killed is True
    assert process.communicated is True
    assert release_pipe.closed is True


def test_checkpoint_worker_broken_release_is_reaped_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guard_lease_fd: int,
) -> None:
    class BrokenPipe:
        closed = False

        def write(self, _payload: bytes) -> None:
            raise BrokenPipeError("worker closed release pipe")

        def close(self) -> None:
            self.closed = True

    class ExitedProcess:
        stdin = BrokenPipe()
        stderr = BrokenPipe()
        communicated = False

        def wait(self, timeout: float) -> int:
            del timeout
            return 0

        def communicate(self, timeout: float) -> tuple[bytes, bytes]:
            del timeout
            self.communicated = True
            return b"", b""

    process = ExitedProcess()
    release_pipe = process.stdin
    adapter = DefaultDuckDbAdapter()
    monkeypatch.setattr(adapter, "_start_worker", lambda *_args, **_kwargs: process)
    monkeypatch.setattr(
        adapter,
        "_read_metadata",
        lambda _process: MarketSourceMetadata(4, "local_projection_v2_event_time"),
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(CutoverSafetyError, match="release"):
            with adapter.checkpoint_snapshot(
                directory_fd,
                "market.duckdb",
                guard_lease_fd=guard_lease_fd,
            ):
                pass
    finally:
        os.close(directory_fd)

    assert process.communicated is True
    assert release_pipe.closed is True


def test_inspect_worker_timeout_is_killed_reaped_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guard_lease_fd: int,
) -> None:
    class Pipe:
        closed = False

        def close(self) -> None:
            self.closed = True

    class HungProcess:
        stdin = Pipe()
        terminated = False
        killed = False
        communicated = False

        def wait(self, timeout: float) -> int:
            if self.killed:
                return -9
            raise cutover_module.subprocess.TimeoutExpired("worker", timeout)

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True

        def communicate(self, timeout: float) -> tuple[bytes, bytes]:
            del timeout
            self.communicated = True
            return b"", b""

    process = HungProcess()
    stdin_pipe = process.stdin
    adapter = DefaultDuckDbAdapter()
    monkeypatch.setattr(adapter, "_start_worker", lambda *_args, **_kwargs: process)
    monkeypatch.setattr(
        adapter,
        "_read_metadata",
        lambda _process: MarketSourceMetadata(4, "local_projection_v2_event_time"),
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(CutoverSafetyError, match="timed out"):
            adapter.inspect(
                directory_fd,
                "market.duckdb",
                guard_lease_fd=guard_lease_fd,
            )
    finally:
        os.close(directory_fd)

    assert process.terminated is True
    assert process.killed is True
    assert process.communicated is True
    assert stdin_pipe.closed is True


def test_inspect_worker_pre_metadata_hang_is_bounded_and_reaped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guard_lease_fd: int,
) -> None:
    adapter = DefaultDuckDbAdapter()
    monkeypatch.setattr(
        DefaultDuckDbAdapter,
        "_WORKER_EXIT_TIMEOUT_SECONDS",
        0.05,
    )
    monkeypatch.setattr(
        DefaultDuckDbAdapter,
        "_WORKER_STOP_TIMEOUT_SECONDS",
        1.0,
    )
    process = cutover_module.subprocess.Popen(
        [cutover_module.sys.executable, "-c", "import time; time.sleep(60)"],
        stdin=cutover_module.subprocess.PIPE,
        stdout=cutover_module.subprocess.PIPE,
        stderr=cutover_module.subprocess.PIPE,
    )
    monkeypatch.setattr(adapter, "_start_worker", lambda *_args, **_kwargs: process)
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(CutoverSafetyError, match="metadata timed out"):
            adapter.inspect(
                directory_fd,
                "market.duckdb",
                guard_lease_fd=guard_lease_fd,
            )
    finally:
        os.close(directory_fd)

    assert process.poll() is not None


def test_inspect_worker_partial_metadata_hang_is_bounded_and_reaped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guard_lease_fd: int,
) -> None:
    adapter = DefaultDuckDbAdapter()
    monkeypatch.setattr(
        DefaultDuckDbAdapter,
        "_WORKER_EXIT_TIMEOUT_SECONDS",
        0.05,
    )
    monkeypatch.setattr(
        DefaultDuckDbAdapter,
        "_WORKER_STOP_TIMEOUT_SECONDS",
        1.0,
    )
    process = cutover_module.subprocess.Popen(
        [
            cutover_module.sys.executable,
            "-c",
            "import sys,time; sys.stdout.write('{'); sys.stdout.flush(); time.sleep(60)",
        ],
        stdin=cutover_module.subprocess.PIPE,
        stdout=cutover_module.subprocess.PIPE,
        stderr=cutover_module.subprocess.PIPE,
    )
    monkeypatch.setattr(adapter, "_start_worker", lambda *_args, **_kwargs: process)
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(CutoverSafetyError, match="metadata timed out"):
            adapter.inspect(
                directory_fd,
                "market.duckdb",
                guard_lease_fd=guard_lease_fd,
            )
    finally:
        os.close(directory_fd)

    assert process.poll() is not None


def test_inspect_unkillable_worker_cleanup_remains_bounded_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guard_lease_fd: int,
) -> None:
    class Pipe:
        closed = False

        def close(self) -> None:
            self.closed = True

    class UnkillableProcess:
        stdin = Pipe()
        terminate_calls = 0
        kill_calls = 0
        communicate_calls = 0

        def wait(self, timeout: float) -> int:
            raise cutover_module.subprocess.TimeoutExpired("worker", timeout)

        def terminate(self) -> None:
            self.terminate_calls += 1

        def kill(self) -> None:
            self.kill_calls += 1

        def communicate(self, timeout: float) -> tuple[bytes, bytes]:
            self.communicate_calls += 1
            raise cutover_module.subprocess.TimeoutExpired("worker", timeout)

    process = UnkillableProcess()
    adapter = DefaultDuckDbAdapter()
    monkeypatch.setattr(adapter, "_start_worker", lambda *_args, **_kwargs: process)
    monkeypatch.setattr(
        adapter,
        "_read_metadata",
        lambda _process: MarketSourceMetadata(4, "local_projection_v2_event_time"),
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(CutoverSafetyError, match="shutdown failed"):
            adapter.inspect(
                directory_fd,
                "market.duckdb",
                guard_lease_fd=guard_lease_fd,
            )
    finally:
        os.close(directory_fd)

    assert process.terminate_calls == 1
    assert process.kill_calls == 2
    assert process.communicate_calls == 2


@pytest.mark.parametrize("denied_signal", ["terminate", "kill"])
def test_inspect_worker_signal_errors_return_explicit_unjoined_verdict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    denied_signal: str,
    guard_lease_fd: int,
) -> None:
    class Pipe:
        def close(self) -> None:
            pass

    class SignalDeniedProcess:
        stdin = Pipe()

        def wait(self, timeout: float) -> int:
            raise cutover_module.subprocess.TimeoutExpired("worker", timeout)

        def terminate(self) -> None:
            if denied_signal == "terminate":
                raise PermissionError("terminate denied")

        def kill(self) -> None:
            if denied_signal == "kill":
                raise PermissionError("kill denied")

        def communicate(self, timeout: float) -> tuple[bytes, bytes]:
            raise cutover_module.subprocess.TimeoutExpired("worker", timeout)

    process = SignalDeniedProcess()
    adapter = DefaultDuckDbAdapter()
    monkeypatch.setattr(adapter, "_start_worker", lambda *_args, **_kwargs: process)
    monkeypatch.setattr(
        adapter,
        "_read_metadata",
        lambda _process: MarketSourceMetadata(4, "local_projection_v2_event_time"),
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(WorkerShutdownError) as captured:
            adapter.inspect(
                directory_fd,
                "market.duckdb",
                guard_lease_fd=guard_lease_fd,
            )
    finally:
        os.close(directory_fd)

    assert captured.value.process_joined is False
    assert isinstance(captured.value, CutoverSafetyError)
