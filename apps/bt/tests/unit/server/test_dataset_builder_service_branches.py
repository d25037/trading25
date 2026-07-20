from __future__ import annotations

import asyncio
import importlib
from pathlib import Path
import threading
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

import src.application.services.dataset_builder_service as dataset_builder_service
from src.application.contracts.jobs import JobStatus
from src.application.services.dataset_builder_copy_stages import (
    preflight_provider_snapshot_source,
)
from src.application.services.dataset_builder_service import (
    DatasetJobData,
    DatasetResult,
    _build_dataset,
    _convert_stocks,
    start_dataset_build,
)
from src.application.services.dataset_presets import PresetConfig
from src.application.services.generic_job_manager import GenericJobManager
from src.infrastructure.db.dataset_io.dataset_writer import (
    DatasetSnapshotError,
    DatasetWriter,
)
from src.infrastructure.db.market.dataset_snapshot_reader import validate_dataset_snapshot
from src.infrastructure.db.market.market_reader import MarketDbReader
from tests.unit.server.db.market_writer_test_support import open_market_db
from tests.unit.server.db.test_dataset_event_time_basis_snapshot import (
    _build_v5_provider_market,
)


@pytest.fixture
def isolated_dataset_manager(monkeypatch: pytest.MonkeyPatch) -> GenericJobManager:
    manager: GenericJobManager = GenericJobManager()
    monkeypatch.setattr(dataset_builder_service, "dataset_job_manager", manager)
    return manager


async def _create_job(
    manager: GenericJobManager,
    *,
    name: str = "sample",
    preset: str = "quickTesting",
    overwrite: bool = False,
):
    job = await manager.create_job(
        DatasetJobData(name=name, preset=preset, overwrite=overwrite)
    )
    assert job is not None
    return job


def test_provider_source_preflight_accepts_only_market_v5_metadata(
    tmp_path: Path,
) -> None:
    source = tmp_path / "market-v5.duckdb"
    db = open_market_db(str(source))
    db.close()
    conn = importlib.import_module("duckdb").connect(str(source))
    try:
        conn.execute(
            """
            INSERT INTO sync_metadata (key, value) VALUES ('provider_plan', 'premium')
            ON CONFLICT (key) DO UPDATE SET value = excluded.value
            """
        )
    finally:
        conn.close()
    preflight_provider_snapshot_source(str(source))

    conn = importlib.import_module("duckdb").connect(str(source))
    try:
        conn.execute("UPDATE market_schema_version SET version = 4")
    finally:
        conn.close()
    with pytest.raises(DatasetSnapshotError, match="Market schema version 5"):
        preflight_provider_snapshot_source(str(source))


@pytest.mark.asyncio
async def test_dataset_selection_uses_provider_windows_as_global_cutoff(
    tmp_path: Path,
) -> None:
    source = _build_v5_provider_market(tmp_path)
    reader = MarketDbReader(str(source))
    try:
        assert await dataset_builder_service._load_market_global_cutoff(
            reader, ["7203"]
        ) == "2024-01-05"
        assert await dataset_builder_service._load_market_stock_date_range(
            reader, ["7203"], "2024-01-05"
        ) == ("2024-01-04", "2024-01-05")
    finally:
        reader.close()


@pytest.mark.asyncio
async def test_dataset_selection_rejects_missing_provider_window(
    tmp_path: Path,
) -> None:
    source = _build_v5_provider_market(tmp_path)
    conn = importlib.import_module("duckdb").connect(str(source))
    try:
        conn.execute("DELETE FROM stock_provider_windows")
    finally:
        conn.close()
    reader = MarketDbReader(str(source))
    try:
        with pytest.raises(DatasetSnapshotError, match="provider vintage is missing"):
            await dataset_builder_service._load_market_global_cutoff(reader, ["7203"])
    finally:
        reader.close()


@pytest.mark.asyncio
async def test_cutoff_master_requires_exact_coverage_end_rows(tmp_path: Path) -> None:
    source = _build_v5_provider_market(tmp_path)
    conn = importlib.import_module("duckdb").connect(str(source))
    try:
        conn.execute("DELETE FROM stock_master_daily WHERE date = '2024-01-05'")
    finally:
        conn.close()
    reader = MarketDbReader(str(source))
    try:
        with pytest.raises(DatasetSnapshotError, match="exact stock_master_daily"):
            await dataset_builder_service._load_market_stock_master(reader, "2024-01-05")
    finally:
        reader.close()


@pytest.mark.asyncio
async def test_cutoff_master_prefers_whole_canonical_alias_and_normalizes_null_date(
    tmp_path: Path,
) -> None:
    source = _build_v5_provider_market(tmp_path)
    conn = importlib.import_module("duckdb").connect(str(source))
    try:
        conn.execute(
            "UPDATE stock_master_daily SET listed_date = NULL "
            "WHERE code = '7203' AND date = '2024-01-05'"
        )
        conn.execute(
            """
            INSERT INTO stock_master_daily
            SELECT date, '72030', 'Alias Wrong', company_name_english,
                   '0113', 'Growth', sector_17_code, sector_17_name,
                   sector_33_code, sector_33_name, scale_category,
                   '2099-01-01', created_at
            FROM stock_master_daily
            WHERE code = '7203' AND date = '2024-01-05'
            """
        )
    finally:
        conn.close()
    reader = MarketDbReader(str(source))
    try:
        rows = await dataset_builder_service._load_market_stock_master(
            reader, "2024-01-05"
        )
    finally:
        reader.close()
    assert rows == [
        {
            "Code": "72030",
            "CoName": "Toyota",
            "CoNameEn": None,
            "Mkt": "0111",
            "MktNm": "Prime",
            "S17": "7",
            "S17Nm": "Transport",
            "S33": "3050",
            "S33Nm": "Auto",
            "ScaleCat": None,
            "Date": "",
        }
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("listed_date", ["1949-5-16", "2025-01-01"])
async def test_cutoff_master_rejects_malformed_or_future_listed_date(
    tmp_path: Path,
    listed_date: str,
) -> None:
    source = _build_v5_provider_market(tmp_path)
    conn = importlib.import_module("duckdb").connect(str(source))
    try:
        conn.execute(
            "UPDATE stock_master_daily SET listed_date = ? "
            "WHERE code = '7203' AND date = '2024-01-05'",
            (listed_date,),
        )
    finally:
        conn.close()
    reader = MarketDbReader(str(source))
    try:
        with pytest.raises(DatasetSnapshotError, match="listed_date"):
            await dataset_builder_service._load_market_stock_master(
                reader, "2024-01-05"
            )
    finally:
        reader.close()


@pytest.mark.asyncio
async def test_selected_price_range_requires_current_basis_state(tmp_path: Path) -> None:
    source = _build_v5_provider_market(tmp_path)
    conn = importlib.import_module("duckdb").connect(str(source))
    try:
        conn.execute("DELETE FROM current_basis_fundamentals_state")
    finally:
        conn.close()
    reader = MarketDbReader(str(source))
    try:
        with pytest.raises(DatasetSnapshotError, match="7203"):
            await dataset_builder_service._load_market_stock_date_range(
                reader, ["7203"], "2024-01-05"
            )
    finally:
        reader.close()


def test_convert_stocks_maps_jquants_fields() -> None:
    assert _convert_stocks(
        [{
            "Code": "72030", "CoName": "Toyota", "CoNameEn": "Toyota Motor",
            "Mkt": "0111", "MktNm": "Prime", "S17": "7",
            "S17Nm": "Transport", "S33": "3050", "S33Nm": "Auto",
            "ScaleCat": "TOPIX Core30", "Date": "1949-05-16",
        }]
    )[0]["code"] == "7203"


@pytest.mark.asyncio
async def test_build_dataset_returns_error_for_unknown_preset(
    isolated_dataset_manager: GenericJobManager,
) -> None:
    job = await _create_job(isolated_dataset_manager, preset="unknown")
    result = await _build_dataset(
        job,
        MagicMock(),
        MagicMock(),
        source_duckdb_path="/unused-for-unknown-preset",
    )
    assert result.success is False
    assert result.errors == ["Unknown preset: unknown"]


@pytest.mark.asyncio
async def test_build_dataset_rejects_missing_source_before_destination_access(
    isolated_dataset_manager: GenericJobManager,
) -> None:
    job = await _create_job(isolated_dataset_manager)
    resolver = MagicMock()
    with pytest.raises(ValueError, match="source_duckdb_path is required"):
        await _build_dataset(
            job,
            resolver,
            MagicMock(),
            source_duckdb_path=cast(Any, None),
        )
    resolver.get_dataset_path.assert_not_called()


@pytest.mark.asyncio
async def test_build_dataset_direct_copy_generates_valid_v4_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    isolated_dataset_manager: GenericJobManager,
    tmp_path: Path,
) -> None:
    job = await _create_job(
        isolated_dataset_manager,
        name="direct-copy",
        preset="quickTesting",
    )
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = str(tmp_path / "direct-copy")
    source = _build_v5_provider_market(tmp_path)
    monkeypatch.setattr(
        dataset_builder_service,
        "get_preset",
        lambda _name: PresetConfig(
            markets=["prime"],
            include_topix=True,
            include_margin=True,
            include_sector_indices=True,
        ),
    )
    reader = MarketDbReader(str(source))
    try:
        result = await _build_dataset(
            job,
            resolver,
            reader,
            source_duckdb_path=str(source),
        )
    finally:
        reader.close()

    assert result.success is True
    assert result.totalStocks == 1
    manifest = validate_dataset_snapshot(tmp_path / "direct-copy")
    assert manifest.schemaVersion == 4
    assert manifest.source.marketSchemaVersion == 5
    assert manifest.source.stockPriceAdjustmentMode == "provider_adjusted_v1"
    assert manifest.source.providerCoverageStart == "2024-01-04"
    assert manifest.source.providerCoverageEnd == "2024-01-05"
    assert not (tmp_path / "direct-copy" / "parquet" / "stock_adjustment_bases.parquet").exists()


@pytest.mark.asyncio
async def test_overwrite_preflight_failure_preserves_existing_target(
    monkeypatch: pytest.MonkeyPatch,
    isolated_dataset_manager: GenericJobManager,
    tmp_path: Path,
) -> None:
    job = await _create_job(
        isolated_dataset_manager,
        name="preserve",
        preset="quickTesting",
        overwrite=True,
    )
    target = tmp_path / "preserve"
    target.mkdir()
    sentinel = target / "sentinel"
    sentinel.write_text("unchanged", encoding="utf-8")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = str(target)
    resolver.get_artifact_paths.return_value = [str(target)]
    source = _build_v5_provider_market(tmp_path)
    conn = importlib.import_module("duckdb").connect(str(source))
    try:
        conn.execute("UPDATE market_schema_version SET version = 4")
    finally:
        conn.close()
    monkeypatch.setattr(
        dataset_builder_service,
        "get_preset",
        lambda _name: PresetConfig(markets=["prime"]),
    )
    reader = MarketDbReader(str(source))
    try:
        with pytest.raises(DatasetSnapshotError, match="Market schema version 5"):
            await _build_dataset(job, resolver, reader, source_duckdb_path=str(source))
    finally:
        reader.close()
    assert sentinel.read_text(encoding="utf-8") == "unchanged"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected_status", "expected_error"),
    [
        (asyncio.TimeoutError(), JobStatus.FAILED, "Dataset build timed out after 35 minutes"),
        (RuntimeError("dataset exploded"), JobStatus.FAILED, "dataset exploded"),
        (asyncio.CancelledError(), JobStatus.PENDING, None),
    ],
)
async def test_start_dataset_build_handles_terminal_failures(
    monkeypatch: pytest.MonkeyPatch,
    isolated_dataset_manager: GenericJobManager,
    tmp_path: Path,
    failure: BaseException,
    expected_status: JobStatus,
    expected_error: str | None,
) -> None:
    source = tmp_path / "market.duckdb"
    source.touch()

    async def fake_wait_for(coro: Any, timeout: int):
        assert timeout == 35 * 60
        coro.close()
        raise failure

    monkeypatch.setattr(dataset_builder_service.asyncio, "wait_for", fake_wait_for)
    reader = MagicMock()
    reader.db_path = str(source)
    job = await start_dataset_build(
        DatasetJobData(name="terminal", preset="quickTesting"),
        MagicMock(),
        reader,
        str(source),
    )
    assert job is not None and job.task is not None
    await job.task
    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == expected_status
    assert stored.error == expected_error


@pytest.mark.asyncio
async def test_start_dataset_build_completes_when_not_cancelled(
    monkeypatch: pytest.MonkeyPatch,
    isolated_dataset_manager: GenericJobManager,
    tmp_path: Path,
) -> None:
    source = tmp_path / "market.duckdb"
    source.touch()

    async def fake_build(*_args: Any, **_kwargs: Any) -> DatasetResult:
        return DatasetResult(
            success=True,
            totalStocks=1,
            processedStocks=1,
            outputPath="/tmp/completed",
        )

    monkeypatch.setattr(dataset_builder_service, "_build_dataset", fake_build)
    reader = MagicMock()
    reader.db_path = str(source)
    job = await start_dataset_build(
        DatasetJobData(name="completed", preset="quickTesting"),
        MagicMock(),
        reader,
        str(source),
    )
    assert job is not None and job.task is not None
    await job.task
    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None and stored.status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_manifest_hash_cancellation_joins_worker_and_removes_staging(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    started = threading.Event()
    release = threading.Event()
    staged = tmp_path / ".manifest.v2.json.job.tmp"

    def blocking_manifest_writer(**_kwargs: Any) -> str:
        started.set()
        assert release.wait(timeout=5)
        staged.write_text("complete", encoding="utf-8")
        return str(staged)

    monkeypatch.setattr(
        dataset_builder_service,
        "_write_dataset_manifest",
        blocking_manifest_writer,
    )
    task = asyncio.create_task(
        dataset_builder_service._write_staged_manifest_off_thread(
            snapshot_path=str(tmp_path / "dataset"),
            dataset_name="sample",
            preset_name="quickTesting",
            staged_manifest_path=staged,
        )
    )
    assert await asyncio.to_thread(started.wait, 5)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not staged.exists()


@pytest.mark.asyncio
async def test_cancellation_waits_for_immutable_source_snapshot_worker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    pinned = tmp_path / "pinned.duckdb"

    def blocking_snapshot(_source: str, target: Path) -> None:
        entered.set()
        assert release.wait(timeout=5)
        target.write_text("joined", encoding="utf-8")
        finished.set()

    monkeypatch.setattr(
        dataset_builder_service,
        "_create_immutable_market_snapshot",
        blocking_snapshot,
    )
    task = asyncio.create_task(
        dataset_builder_service._create_immutable_market_snapshot_off_thread(
            str(tmp_path / "source.duckdb"),
            pinned,
        )
    )
    assert await asyncio.to_thread(entered.wait, 5)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()
    assert pinned.read_text(encoding="utf-8") == "joined"


@pytest.mark.asyncio
async def test_dataset_writer_worker_close_without_open_is_idempotent(
    tmp_path: Path,
) -> None:
    worker = dataset_builder_service._DatasetWriterWorker(
        str(tmp_path / "never-opened")
    )
    await worker.close()
    assert not (tmp_path / "never-opened").exists()


@pytest.mark.asyncio
async def test_build_uses_immutable_source_when_live_market_mutates(
    monkeypatch: pytest.MonkeyPatch,
    isolated_dataset_manager: GenericJobManager,
    tmp_path: Path,
) -> None:
    source = _build_v5_provider_market(tmp_path)
    job = await _create_job(isolated_dataset_manager, name="immutable-source")

    async def inspect_pinned(
        _job: Any,
        _resolver: Any,
        pinned_reader: MarketDbReader,
        *,
        source_duckdb_path: str,
    ) -> DatasetResult:
        live = importlib.import_module("duckdb").connect(str(source))
        try:
            live.execute(
                "UPDATE stock_data_raw SET adjusted_close = 999 "
                "WHERE code = '7203' AND date = '2024-01-04'"
            )
        finally:
            live.close()
        assert Path(source_duckdb_path) != source
        pinned = pinned_reader.query(
            "SELECT adjusted_close FROM stock_data_raw "
            "WHERE code = '7203' AND date = '2024-01-04'"
        )
        assert float(pinned[0]["adjusted_close"]) == 200.0
        return DatasetResult(success=True)

    monkeypatch.setattr(
        dataset_builder_service,
        "_build_dataset_from_pinned_source",
        inspect_pinned,
    )
    reader = MarketDbReader(str(source))
    try:
        result = await _build_dataset(
            job,
            MagicMock(),
            reader,
            source_duckdb_path=str(source),
        )
    finally:
        reader.close()
    assert result.success is True


def _provider_build_preset() -> PresetConfig:
    return PresetConfig(
        markets=["prime"],
        include_topix=True,
        include_margin=False,
        include_sector_indices=False,
    )


@pytest.mark.asyncio
async def test_real_cancel_joins_blocked_manifest_worker_before_terminal_cancel(
    monkeypatch: pytest.MonkeyPatch,
    isolated_dataset_manager: GenericJobManager,
    tmp_path: Path,
) -> None:
    resolver = MagicMock()
    snapshot_dir = tmp_path / "cancel-checksum-worker"
    resolver.get_dataset_path.return_value = str(snapshot_dir)
    source = _build_v5_provider_market(tmp_path)
    monkeypatch.setattr(
        dataset_builder_service,
        "get_preset",
        lambda _name: _provider_build_preset(),
    )
    original_write = dataset_builder_service._write_dataset_manifest
    entered = threading.Event()
    release = threading.Event()

    def blocked_checksum(**kwargs: Any) -> str:
        entered.set()
        assert release.wait(timeout=5)
        return original_write(**kwargs)

    monkeypatch.setattr(
        dataset_builder_service,
        "_write_dataset_manifest",
        blocked_checksum,
    )
    reader = MarketDbReader(str(source))
    job = await start_dataset_build(
        DatasetJobData(name="cancel-checksum-worker", preset="quickTesting"),
        resolver,
        reader,
        str(source),
    )
    assert job is not None and job.task is not None
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        cancel_task = asyncio.create_task(
            isolated_dataset_manager.cancel_job(job.job_id)
        )
        while not job.cancelled.is_set():
            await asyncio.sleep(0)
        assert not cancel_task.done()
        assert job.status != JobStatus.CANCELLED
        release.set()
        assert await cancel_task is True
        await job.task
    finally:
        release.set()
        reader.close()
    assert job.status == JobStatus.CANCELLED
    assert job.result is None
    assert not (snapshot_dir / "manifest.v2.json").exists()
    assert not list(snapshot_dir.glob(".manifest.v2.json.*.tmp"))


@pytest.mark.asyncio
async def test_cancel_waiting_during_manifest_replace_observes_completed_bundle(
    monkeypatch: pytest.MonkeyPatch,
    isolated_dataset_manager: GenericJobManager,
    tmp_path: Path,
) -> None:
    resolver = MagicMock()
    snapshot_dir = tmp_path / "cancel-during-replace"
    resolver.get_dataset_path.return_value = str(snapshot_dir)
    source = _build_v5_provider_market(tmp_path)
    monkeypatch.setattr(
        dataset_builder_service,
        "get_preset",
        lambda _name: _provider_build_preset(),
    )
    loop = asyncio.get_running_loop()
    original_replace = Path.replace
    cancel_result: list[bool] = []
    cancel_threads: list[threading.Thread] = []

    def racing_replace(path: Path, target: Path) -> Path:
        if path.name.startswith(".manifest.v2.json"):
            def request_cancel() -> None:
                future = asyncio.run_coroutine_threadsafe(
                    isolated_dataset_manager.cancel_job(job.job_id, wait=False),
                    loop,
                )
                cancel_result.append(future.result(timeout=5))

            thread = threading.Thread(target=request_cancel)
            cancel_threads.append(thread)
            thread.start()
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", racing_replace)
    reader = MarketDbReader(str(source))
    job = await start_dataset_build(
        DatasetJobData(name="cancel-during-replace", preset="quickTesting"),
        resolver,
        reader,
        str(source),
    )
    assert job is not None and job.task is not None
    try:
        await job.task
        for thread in cancel_threads:
            await asyncio.to_thread(thread.join, 5)
    finally:
        reader.close()
    assert cancel_result == [False]
    assert job.status == JobStatus.COMPLETED
    assert job.result is not None and job.result.success is True
    assert validate_dataset_snapshot(snapshot_dir).schemaVersion == 4
    assert not list(snapshot_dir.glob(".manifest.v2.json.*.tmp"))


@pytest.mark.asyncio
async def test_cancel_immediately_after_publication_lock_sees_completed_bundle(
    monkeypatch: pytest.MonkeyPatch,
    isolated_dataset_manager: GenericJobManager,
    tmp_path: Path,
) -> None:
    resolver = MagicMock()
    snapshot_dir = tmp_path / "cancel-after-publish"
    resolver.get_dataset_path.return_value = str(snapshot_dir)
    source = _build_v5_provider_market(tmp_path)
    monkeypatch.setattr(
        dataset_builder_service,
        "get_preset",
        lambda _name: _provider_build_preset(),
    )
    original_complete = isolated_dataset_manager.complete_job_with_publication
    published = asyncio.Event()
    allow_builder_return = asyncio.Event()

    async def gated_complete(*args: Any, **kwargs: Any) -> bool:
        result = await original_complete(*args, **kwargs)
        published.set()
        await allow_builder_return.wait()
        return result

    monkeypatch.setattr(
        isolated_dataset_manager,
        "complete_job_with_publication",
        gated_complete,
    )
    reader = MarketDbReader(str(source))
    job = await start_dataset_build(
        DatasetJobData(name="cancel-after-publish", preset="quickTesting"),
        resolver,
        reader,
        str(source),
    )
    assert job is not None and job.task is not None
    try:
        await published.wait()
        cancel_won = await isolated_dataset_manager.cancel_job(
            job.job_id, wait=False
        )
        allow_builder_return.set()
        await job.task
    finally:
        allow_builder_return.set()
        reader.close()
    assert cancel_won is False
    assert job.status == JobStatus.COMPLETED
    assert job.result is not None and job.result.success is True
    assert validate_dataset_snapshot(snapshot_dir).schemaVersion == 4
    assert not list(snapshot_dir.glob(".manifest.v2.json.*.tmp"))


@pytest.mark.asyncio
async def test_cancellation_after_provider_copy_closes_partial_without_manifest(
    monkeypatch: pytest.MonkeyPatch,
    isolated_dataset_manager: GenericJobManager,
    tmp_path: Path,
) -> None:
    job = await _create_job(
        isolated_dataset_manager,
        name="cancel-provider-copy",
        preset="quickTesting",
    )
    resolver = MagicMock()
    snapshot_dir = tmp_path / "cancel-provider-copy"
    resolver.get_dataset_path.return_value = str(snapshot_dir)
    source = _build_v5_provider_market(tmp_path)
    monkeypatch.setattr(
        dataset_builder_service,
        "get_preset",
        lambda _name: _provider_build_preset(),
    )

    class CancellingWriter(DatasetWriter):
        instances: list["CancellingWriter"] = []

        def __init__(self, path: str) -> None:
            super().__init__(path)
            self.closed_for_test = False
            self.__class__.instances.append(self)

        def copy_provider_snapshot_from_source(self, **kwargs: Any):
            result = super().copy_provider_snapshot_from_source(**kwargs)
            job.cancelled.set()
            return result

        def close(self) -> None:
            super().close()
            self.closed_for_test = True

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", CancellingWriter)
    reader = MarketDbReader(str(source))
    try:
        result = await _build_dataset(
            job,
            resolver,
            reader,
            source_duckdb_path=str(source),
        )
    finally:
        reader.close()
    assert result.success is False
    assert result.errors == ["Cancelled"]
    assert CancellingWriter.instances
    assert all(writer.closed_for_test for writer in CancellingWriter.instances)
    assert not (snapshot_dir / "manifest.v2.json").exists()


@pytest.mark.asyncio
async def test_overwrite_selection_failure_preserves_existing_target(
    monkeypatch: pytest.MonkeyPatch,
    isolated_dataset_manager: GenericJobManager,
    tmp_path: Path,
) -> None:
    job = await _create_job(
        isolated_dataset_manager,
        name="overwrite-selection-failure",
        preset="quickTesting",
        overwrite=True,
    )
    target = tmp_path / "overwrite-selection-failure"
    target.mkdir()
    sentinel = target / "sentinel"
    sentinel.write_text("unchanged", encoding="utf-8")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = str(target)
    resolver.get_artifact_paths.return_value = [str(target)]
    source = _build_v5_provider_market(tmp_path)
    conn = importlib.import_module("duckdb").connect(str(source))
    try:
        conn.execute("DELETE FROM current_basis_fundamentals_state")
    finally:
        conn.close()
    monkeypatch.setattr(
        dataset_builder_service,
        "get_preset",
        lambda _name: _provider_build_preset(),
    )
    reader = MarketDbReader(str(source))
    try:
        with pytest.raises(DatasetSnapshotError, match="current-basis coverage"):
            await _build_dataset(
                job,
                resolver,
                reader,
                source_duckdb_path=str(source),
            )
    finally:
        reader.close()
    assert sentinel.read_text(encoding="utf-8") == "unchanged"


@pytest.mark.asyncio
async def test_successful_overwrite_replaces_target_with_complete_bundle(
    monkeypatch: pytest.MonkeyPatch,
    isolated_dataset_manager: GenericJobManager,
    tmp_path: Path,
) -> None:
    job = await _create_job(
        isolated_dataset_manager,
        name="overwrite-success",
        preset="quickTesting",
        overwrite=True,
    )
    target = tmp_path / "overwrite-success"
    target.mkdir()
    sentinel = target / "sentinel"
    sentinel.write_text("stale", encoding="utf-8")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = str(target)
    resolver.get_artifact_paths.return_value = [str(target)]
    resolver.evict.return_value = None
    source = _build_v5_provider_market(tmp_path)
    monkeypatch.setattr(
        dataset_builder_service,
        "get_preset",
        lambda _name: _provider_build_preset(),
    )
    reader = MarketDbReader(str(source))
    try:
        result = await _build_dataset(
            job,
            resolver,
            reader,
            source_duckdb_path=str(source),
        )
    finally:
        reader.close()
    assert result.success is True
    assert not sentinel.exists()
    assert validate_dataset_snapshot(target).schemaVersion == 4
