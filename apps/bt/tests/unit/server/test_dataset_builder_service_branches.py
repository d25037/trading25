from __future__ import annotations

import asyncio
import importlib
from pathlib import Path
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
from src.infrastructure.db.dataset_io.dataset_writer import DatasetSnapshotError
from src.infrastructure.db.market.dataset_snapshot_reader import validate_dataset_snapshot
from src.infrastructure.db.market.market_reader import MarketDbReader
from tests.unit.server.db.market_writer_test_support import open_market_db
from tests.unit.server.db.test_dataset_event_time_basis_snapshot import (
    _build_v4_market_with_two_regimes,
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
    source = _build_v4_market_with_two_regimes(tmp_path)
    reader = MarketDbReader(str(source))
    try:
        assert await dataset_builder_service._load_market_global_cutoff(reader) == "2024-01-05"
        assert await dataset_builder_service._load_market_stock_date_range(
            reader, ["7203"], "2024-01-05"
        ) == ("2024-01-04", "2024-01-05")
    finally:
        reader.close()


@pytest.mark.asyncio
async def test_dataset_selection_rejects_missing_provider_window(
    tmp_path: Path,
) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
    conn = importlib.import_module("duckdb").connect(str(source))
    try:
        conn.execute("DELETE FROM stock_provider_windows")
    finally:
        conn.close()
    reader = MarketDbReader(str(source))
    try:
        with pytest.raises(DatasetSnapshotError, match="provider vintage is missing"):
            await dataset_builder_service._load_market_global_cutoff(reader)
    finally:
        reader.close()


@pytest.mark.asyncio
async def test_cutoff_master_requires_exact_coverage_end_rows(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
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
async def test_selected_price_range_requires_current_basis_state(tmp_path: Path) -> None:
    source = _build_v4_market_with_two_regimes(tmp_path)
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
    source = _build_v4_market_with_two_regimes(tmp_path)
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
    source = _build_v4_market_with_two_regimes(tmp_path)
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
